"""Инфраструктурные адаптеры NL2SQL для Vanna и MCP."""

from __future__ import annotations

import os
from pathlib import Path
from threading import Lock
from typing import Any

from backend.app.infrastructure.config.env_config import load_repo_env
from backend.app.infrastructure.mcp.query_server import execute_safe_query
from backend.app.infrastructure.training.train_vanna_pipeline import (
    RunConfig,
    attach_sql_runner,
    create_trainer,
    load_and_validate_assets,
    train_vanna_assets,
)


class McpQueryExecutor:
    """Адаптер исполнения SQL через существующий MCP-инструмент."""

    def execute(self, sql_text: str) -> dict[str, Any]:
        """Передает SQL в `execute_safe_query` и возвращает ответ инструмента."""

        return execute_safe_query(sql_text)


class VannaRuntime:
    """Ленивая инициализация обученного Vanna-объекта для runtime-запросов."""

    def __init__(self) -> None:
        """Создает runtime и подготавливает lock для безопасного lazy-init."""

        self._lock = Lock()
        self._vn: Any | None = None

    def get_vn(self) -> Any:
        """Возвращает готовый Vanna-инстанс, обучая его при первом обращении."""

        if self._vn is not None:
            return self._vn

        with self._lock:
            if self._vn is not None:
                return self._vn

            model = _resolve_runtime_model()
            trainer = create_trainer(os.getenv("OPENAI_API_KEY", ""), model)
            assets = load_and_validate_assets(_default_run_config())
            train_vanna_assets(trainer, assets, _noop_logger, request_id="req_phase4_boot")
            attach_sql_runner(trainer)
            self._vn = trainer
            return self._vn


class VannaSqlGenerator:
    """Генерация SQL по вопросу через Vanna legacy API."""

    def __init__(self, runtime: VannaRuntime) -> None:
        """Инициализирует генератор с общим runtime-кэшем."""

        self._runtime = runtime

    def generate_sql(self, question: str) -> str:
        """Генерирует SQL с помощью `vn.generate_sql(...)`."""

        vn = self._runtime.get_vn()
        return vn.generate_sql(question=question)

    def regenerate_sql(
        self,
        question: str,
        previous_output: str,
        error_message: str,
    ) -> str:
        """Перегенерирует SQL в строгом формате после неудачной первичной попытки."""

        vn = self._runtime.get_vn()
        prompt = [
            vn.system_message(
                "Ты PostgreSQL-генератор SQL для аналитики. "
                "Верни строго один валидный read-only SQL-запрос (SELECT или WITH ... SELECT) "
                "без пояснений, markdown и комментариев."
            ),
            vn.user_message(
                "Вопрос пользователя:\n"
                f"{question}\n\n"
                "Предыдущий неудачный ответ:\n"
                f"{previous_output}\n\n"
                "Ошибка:\n"
                f"{error_message}\n\n"
                "Доступные поля таблицы orders: city_id, status_tender, order_timestamp, status_order, "
                "price_order_local, distance_in_meters, duration_in_seconds.\n"
                "Сейчас верни только SQL."
            ),
        ]
        return vn.submit_prompt(prompt)


class VannaSqlExplainer:
    """Генерация текстового Explain-блока через LLM-канал Vanna."""

    def __init__(self, runtime: VannaRuntime) -> None:
        """Инициализирует explain-генератор с общим runtime-кэшем."""

        self._runtime = runtime

    def explain(self, question: str, sql_text: str) -> str:
        """Строит prompt и возвращает русскоязычное объяснение SQL."""

        vn = self._runtime.get_vn()
        prompt = [
            vn.system_message(
                "Ты аналитик данных. Кратко и понятно объясни, что делает SQL-запрос."
            ),
            vn.user_message(
                "Вопрос пользователя:\n"
                f"{question}\n\n"
                "SQL-запрос:\n"
                f"{sql_text}\n\n"
                "Дай объяснение на русском языке в 2-4 предложениях, без markdown."
            ),
        ]
        return vn.submit_prompt(prompt)


class VannaIntentClassifier:
    """Fallback-classifier неоднозначных intent-запросов через LLM-канал Vanna."""

    def __init__(self, runtime: VannaRuntime) -> None:
        """Инициализирует classifier с общим runtime-кэшем."""

        self._runtime = runtime

    def classify(self, question: str) -> dict[str, Any] | str | None:
        """Просит LLM вернуть только JSON для неоднозначных intent-кейсов."""

        vn = self._runtime.get_vn()
        prompt = [
            vn.system_message(
                "Ты классификатор неоднозначных аналитических вопросов. "
                "Ты работаешь только в домене таблицы orders. "
                "В MVP `регионы` и `города` уже считаются агрегированным полем city_id, "
                "поэтому никогда не проси выбрать северный, южный, западный или восточный регион. "
                "Разрешены только два вида уточнения: metric и period. "
                "Верни только JSON. "
                'Если уточнение не нужно, верни {"needs_clarification": false}. '
                'Если уточнение нужно, верни JSON с полями needs_clarification, confidence и clarification.'
            ),
            vn.user_message(
                "Вопрос пользователя:\n"
                f"{question}\n\n"
                "Формат ответа при уточнении:\n"
                '{'
                '"needs_clarification": true, '
                '"confidence": {"score": 0.42, "level": "low", "reason": "..."}, '
                '"clarification": {'
                '"kind": "metric", '
                '"reason": "...", '
                '"question": "...", '
                '"options": ['
                '{"label": "...", "value": "...", "description": "..."}'
                "]"
                "}"
                "}\n"
                "Поле clarification.kind может быть только metric или period.\n"
                "Если вопрос достаточно точный, верни только JSON с needs_clarification=false."
            ),
        ]
        return vn.submit_prompt(prompt)


def build_intent_classifier(runtime: VannaRuntime) -> VannaIntentClassifier | None:
    """Включает LLM-classifier только при явном env-флаге."""

    flag = os.getenv("ENABLE_LLM_INTENT_CLASSIFIER", "").strip().lower()
    if flag in {"1", "true", "yes", "on"}:
        return VannaIntentClassifier(runtime)
    return None


def _default_run_config() -> RunConfig:
    """Создает базовый конфиг загрузки обучающих артефактов Vanna."""

    project_root = Path(__file__).resolve().parents[2]
    return RunConfig(
        metrics_path=project_root / "semantic/metrics.yaml",
        dimensions_path=project_root / "semantic/dimensions.yaml",
        few_shot_path=project_root / "training/few_shot_pairs.yaml",
        rules_path=project_root / "training/business_rules.md",
        cube_output_path=project_root / "semantic/cubes/orders_cube.yaml",
        init_sql_path=project_root / "init.sql",
        model=_resolve_runtime_model(),
        verify_db=False,
        dry_run=False,
    )


def _resolve_runtime_model() -> str:
    """Загружает `.env` и возвращает модель LLM для runtime-запросов Vanna."""

    load_repo_env()
    raw_model = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip() or "gpt-4o-mini"
    return _normalize_openrouter_model_id(raw_model)


def _normalize_openrouter_model_id(model: str) -> str:
    """Снимает ошибочный префикс `openrouter/`, если он попал в `.env`."""

    prefix = "openrouter/"
    if model.lower().startswith(prefix):
        return model[len(prefix):]
    return model


def _noop_logger(*_args: Any, **_kwargs: Any) -> None:
    """Пустой логгер для runtime-инициализации без лишнего stdout-шума."""
