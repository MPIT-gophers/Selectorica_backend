"""Скрипт обучения Vanna для закрытия Phase 2 (speed-first).

Сценарий:
1. Загружает и валидирует metrics/dimensions/few-shot/rules.
2. Собирает dotML-куб `orders`.
3. Проверяет доступ к PostgreSQL (опционально).
4. Обучает Vanna через `vn.train(...)` на DDL, документации и few-shot примерах.
"""

from __future__ import annotations

import argparse
import json
import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Protocol, Tuple

import pandas as pd
import sqlglot
import yaml
from sqlalchemy import create_engine, text

from backend.app.infrastructure.config.env_config import get_db_config, load_repo_env


REQUIRED_METRICS = {"revenue_local", "cancelled_orders", "completed_trips"}
REQUIRED_DIMENSIONS = {"city_id", "channel", "order_date"}
MIN_FEW_SHOT_COUNT = 8
SQL_TABLE_PLACEHOLDER = "${table}"
SQL_VALIDATION_TABLE = "orders"
PROJECT_ROOT = Path(__file__).resolve().parents[3]


class AppError(Exception):
    """Доменно-осмысленная ошибка с кодом для стабильного CLI-ответа."""

    def __init__(self, error_code: str, message: str):
        super().__init__(message)
        self.error_code = error_code
        self.message = message


@dataclass
class RunConfig:
    """Конфиг запуска пайплайна обучения."""

    metrics_path: Path
    dimensions_path: Path
    few_shot_path: Path
    rules_path: Path
    cube_output_path: Path
    init_sql_path: Path
    model: str
    verify_db: bool
    dry_run: bool


@dataclass
class Assets:
    """Подготовленные входные артефакты обучения."""

    metrics: List[Dict[str, Any]]
    dimensions: List[Dict[str, Any]]
    few_shot_pairs: List[Dict[str, Any]]
    rules_text: str
    cube_config: Dict[str, Any]
    init_sql_text: str


class Trainable(Protocol):
    """Минимальный протокол объекта, пригодного для обучения."""

    def train(self, **kwargs: Any) -> Any:
        """Добавляет обучающий элемент в backend-хранилище."""


def utc_now_iso() -> str:
    """Возвращает текущее время в ISO UTC-формате для логов."""

    return datetime.now(tz=timezone.utc).isoformat()


def generate_request_id() -> str:
    """Генерирует `request_id` для сквозной трассировки логов."""

    return f"req_{uuid.uuid4().hex[:12]}"


def mask_sensitive(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Маскирует чувствительные значения перед выводом в лог."""

    masked = {}
    sensitive_tokens = ("password", "token", "secret", "api_key", "key")

    for key, value in payload.items():
        lower_key = key.lower()
        if any(token in lower_key for token in sensitive_tokens):
            masked[key] = "***"
        else:
            masked[key] = value
    return masked


def log_event(level: str, event: str, request_id: str, **payload: Any) -> None:
    """Пишет структурированный JSON-лог с `request_id`."""

    record = {
        "ts": utc_now_iso(),
        "level": level.upper(),
        "event": event,
        "request_id": request_id,
        **mask_sensitive(payload),
    }
    print(json.dumps(record, ensure_ascii=True))


def read_yaml(path: Path, expected_root_key: str) -> Dict[str, Any]:
    """Читает YAML и проверяет наличие ожидаемого корневого ключа."""

    if not path.exists():
        raise AppError("FILE_NOT_FOUND", f"Файл не найден: {path}")

    with path.open("r", encoding="utf-8") as file:
        content = yaml.safe_load(file) or {}

    if not isinstance(content, dict) or expected_root_key not in content:
        raise AppError(
            "INVALID_YAML_SCHEMA",
            f"В файле {path} отсутствует ключ `{expected_root_key}`.",
        )
    return content


def read_text(path: Path) -> str:
    """Читает текстовый файл и возвращает его содержимое."""

    if not path.exists():
        raise AppError("FILE_NOT_FOUND", f"Файл не найден: {path}")
    return path.read_text(encoding="utf-8")


def validate_metric_sql(sql_expression: str) -> None:
    """Проверяет синтаксис SQL-выражения метрики через sqlglot."""

    normalized_expression = sql_expression.replace(
        SQL_TABLE_PLACEHOLDER, SQL_VALIDATION_TABLE
    )
    probe_query = f"SELECT {normalized_expression} AS metric_value FROM orders"
    try:
        sqlglot.parse_one(probe_query, dialect="postgres")
    except Exception as error:
        raise AppError("INVALID_METRIC_SQL", f"Некорректный SQL метрики: {error}") from error


def validate_dimension_sql(sql_expression: str) -> None:
    """Проверяет синтаксис SQL-выражения измерения через sqlglot."""

    normalized_expression = sql_expression.replace(
        SQL_TABLE_PLACEHOLDER, SQL_VALIDATION_TABLE
    )
    probe_query = f"SELECT {normalized_expression} AS dim_value FROM orders"
    try:
        sqlglot.parse_one(probe_query, dialect="postgres")
    except Exception as error:
        raise AppError(
            "INVALID_DIMENSION_SQL", f"Некорректный SQL измерения: {error}"
        ) from error


def validate_query_sql(query_sql: str) -> None:
    """Проверяет, что few-shot SQL является валидным read-only SELECT."""

    normalized = query_sql.strip().lower()
    if not normalized.startswith("select"):
        raise AppError("INVALID_FEWSHOT_SQL", "Few-shot SQL должен начинаться с SELECT.")

    forbidden = ("update ", "delete ", "insert ", "drop ", "truncate ", "alter ")
    if any(token in normalized for token in forbidden):
        raise AppError("INVALID_FEWSHOT_SQL", "Few-shot SQL содержит мутационную операцию.")

    try:
        sqlglot.parse_one(query_sql, dialect="postgres")
    except Exception as error:
        raise AppError(
            "INVALID_FEWSHOT_SQL", f"Некорректный few-shot SQL: {error}"
        ) from error


def validate_metrics(metrics: List[Dict[str, Any]]) -> None:
    """Валидирует структуру и минимальный набор метрик Phase 2."""

    if not metrics:
        raise AppError("INVALID_METRICS", "Список metrics пуст.")

    names = set()
    for metric in metrics:
        name = metric.get("name")
        sql_expression = metric.get("sql")
        if not name or not sql_expression:
            raise AppError("INVALID_METRICS", "Каждая метрика обязана иметь name и sql.")
        names.add(name)
        validate_metric_sql(sql_expression)

    missing = REQUIRED_METRICS - names
    if missing:
        raise AppError("INVALID_METRICS", f"Отсутствуют обязательные метрики: {sorted(missing)}")


def validate_dimensions(dimensions: List[Dict[str, Any]]) -> None:
    """Валидирует структуру и минимальный набор измерений Phase 2."""

    if not dimensions:
        raise AppError("INVALID_DIMENSIONS", "Список dimensions пуст.")

    names = set()
    for dimension in dimensions:
        name = dimension.get("name")
        sql_expression = dimension.get("sql")
        if not name or not sql_expression:
            raise AppError(
                "INVALID_DIMENSIONS", "Каждое измерение обязано иметь name и sql."
            )
        names.add(name)
        validate_dimension_sql(sql_expression)

    missing = REQUIRED_DIMENSIONS - names
    if missing:
        raise AppError(
            "INVALID_DIMENSIONS", f"Отсутствуют обязательные измерения: {sorted(missing)}"
        )


def validate_few_shot_pairs(pairs: List[Dict[str, Any]]) -> None:
    """Валидирует few-shot примеры для обучения Vanna."""

    if len(pairs) < MIN_FEW_SHOT_COUNT:
        raise AppError(
            "INVALID_FEWSHOT_COUNT",
            f"Ожидалось минимум {MIN_FEW_SHOT_COUNT} пар, получено {len(pairs)}.",
        )

    seen_ids = set()
    for pair in pairs:
        pair_id = pair.get("id")
        question = pair.get("question_ru")
        query_sql = pair.get("sql")

        if not pair_id or not question or not query_sql:
            raise AppError(
                "INVALID_FEWSHOT_SCHEMA",
                "Каждая пара обязана иметь id, question_ru и sql.",
            )
        if pair_id in seen_ids:
            raise AppError("INVALID_FEWSHOT_SCHEMA", f"Дублирующийся id: {pair_id}")
        seen_ids.add(pair_id)
        validate_query_sql(query_sql)


def build_orders_cube(
    metrics: List[Dict[str, Any]], dimensions: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Собирает итоговый dotML-куб `orders` из metrics/dimensions."""

    return {
        "version": 1,
        "cubes": [
            {
                "name": "orders",
                "table": "orders",
                "dimensions": dimensions,
                "metrics": metrics,
            }
        ],
    }


def save_yaml(path: Path, payload: Dict[str, Any]) -> None:
    """Сохраняет YAML-файл в UTF-8."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        yaml.safe_dump(payload, file, sort_keys=False, allow_unicode=True)


def create_trainer(api_key: str, model: str) -> Trainable:
    """Создает экземпляр Vanna-тренера для legacy `vn.train(...)`."""

    resolved_api_key, base_url = _resolve_llm_credentials(api_key)

    if not resolved_api_key:
        raise AppError(
            "MISSING_API_KEY",
            "Не найден OPENAI_API_KEY/OPENROUTER_API_KEY. Добавьте ключ в окружение перед запуском.",
        )
    try:
        from openai import OpenAI
        from vanna.legacy.openai import OpenAI_Chat
    except Exception as error:
        raise AppError(
            "MISSING_OPENAI_DEPENDENCY",
            "Для реального train отсутствует пакет `openai`. "
            "Установите зависимость или используйте --dry-run.",
        ) from error

    openai_kwargs: Dict[str, Any] = {"api_key": resolved_api_key}
    if base_url:
        openai_kwargs["base_url"] = base_url

    openrouter_headers = _build_openrouter_headers(base_url)
    if openrouter_headers:
        openai_kwargs["default_headers"] = openrouter_headers

    openai_client = OpenAI(**openai_kwargs)

    class _OpenAIInMemoryTrainer(OpenAI_Chat):
        """Тренер Vanna с OpenAI-чатом и локальным in-memory retrieval."""

        def __init__(self, config: Dict[str, Any]):
            OpenAI_Chat.__init__(self, client=openai_client, config=config)
            self._ddl_store: List[str] = []
            self._doc_store: List[str] = []
            self._question_sql_store: List[Dict[str, str]] = []

        def generate_embedding(self, data: str, **kwargs: Any) -> List[float]:
            """Возвращает детерминированный embedding-заглушку для in-memory режима."""

            seed = float(len(data or ""))
            return [seed, 1.0, 2.0, 3.0, 4.0]

        def add_ddl(self, ddl: str, **kwargs: Any) -> str:
            """Сохраняет DDL-описание в локальное хранилище контекста."""

            self._ddl_store.append(ddl)
            return str(len(self._ddl_store))

        def add_documentation(self, documentation: str, **kwargs: Any) -> str:
            """Сохраняет документацию в локальное хранилище контекста."""

            self._doc_store.append(documentation)
            return str(len(self._doc_store))

        def add_question_sql(self, question: str, sql: str, **kwargs: Any) -> str:
            """Сохраняет few-shot пару вопрос/SQL для последующего retrieval."""

            self._question_sql_store.append({"question": question, "sql": sql})
            return str(len(self._question_sql_store))

        def get_related_ddl(self, question: str, **kwargs: Any) -> list:
            """Возвращает релевантные DDL-фрагменты для текущего вопроса."""

            return _rank_by_overlap(question, self._ddl_store, limit=5)

        def get_related_documentation(self, question: str, **kwargs: Any) -> list:
            """Возвращает релевантную документацию для текущего вопроса."""

            return _rank_by_overlap(question, self._doc_store, limit=5)

        def get_similar_question_sql(self, question: str, **kwargs: Any) -> list:
            """Возвращает few-shot пары, наиболее похожие на текущий вопрос."""

            ranked_pairs = sorted(
                self._question_sql_store,
                key=lambda pair: _overlap_score(question, pair.get("question", "")),
                reverse=True,
            )
            return ranked_pairs[:5]

        def get_training_data(self, **kwargs: Any) -> pd.DataFrame:
            """Возвращает объединенную витрину in-memory обучающих данных."""

            rows: List[Dict[str, Any]] = []
            for idx, ddl in enumerate(self._ddl_store):
                rows.append(
                    {
                        "id": f"ddl_{idx}",
                        "training_data_type": "ddl",
                        "question": None,
                        "content": ddl,
                    }
                )
            for idx, doc in enumerate(self._doc_store):
                rows.append(
                    {
                        "id": f"doc_{idx}",
                        "training_data_type": "documentation",
                        "question": None,
                        "content": doc,
                    }
                )
            for idx, pair in enumerate(self._question_sql_store):
                rows.append(
                    {
                        "id": f"sql_{idx}",
                        "training_data_type": "sql",
                        "question": pair.get("question"),
                        "content": pair.get("sql"),
                    }
                )
            return pd.DataFrame(rows)

        def remove_training_data(self, id: str, **kwargs: Any) -> bool:
            """Удаление в in-memory режиме не используется, возвращаем `False`."""

            return False

    return _OpenAIInMemoryTrainer(config={"api_key": resolved_api_key, "model": model})


def _overlap_score(query: str, text: str) -> int:
    """Считает простую релевантность по пересечению токенов запроса и текста."""

    query_tokens = {token for token in query.lower().split() if token}
    text_tokens = {token for token in text.lower().split() if token}
    return len(query_tokens.intersection(text_tokens))


def _rank_by_overlap(query: str, items: List[str], limit: int) -> List[str]:
    """Ранжирует список текстов по overlap-score и возвращает top-k."""

    ranked = sorted(items, key=lambda value: _overlap_score(query, value), reverse=True)
    return ranked[:limit]


def _resolve_llm_credentials(api_key: str) -> Tuple[str, Optional[str]]:
    """Определяет итоговые `api_key` и `base_url` для OpenAI-совместимого клиента."""

    # Гарантируем загрузку .env перед чтением ключей LLM.
    load_repo_env()

    if api_key.strip():
        return api_key.strip(), os.getenv("OPENAI_BASE_URL")

    openrouter_key = os.getenv("OPENROUTER_API_KEY", "").strip()
    if openrouter_key:
        base_url = os.getenv("OPENAI_BASE_URL") or "https://openrouter.ai/api/v1"
        return openrouter_key, base_url

    return "", os.getenv("OPENAI_BASE_URL")


def _build_openrouter_headers(base_url: Optional[str]) -> Dict[str, str]:
    """Возвращает рекомендованные заголовки для OpenRouter (если применимо)."""

    if not base_url or "openrouter.ai" not in base_url.lower():
        return {}

    headers: Dict[str, str] = {}
    http_referer = os.getenv("OPENROUTER_HTTP_REFERER", "").strip()
    app_title = os.getenv("OPENROUTER_APP_TITLE", "Drivee NL2SQL").strip()

    if http_referer:
        headers["HTTP-Referer"] = http_referer
    if app_title:
        headers["X-Title"] = app_title

    return headers


def verify_db_connection() -> None:
    """Проверяет доступность PostgreSQL через `SELECT 1`."""

    db = get_db_config()
    db_url = (
        f"postgresql+pg8000://{db.user}:{db.password}"
        f"@{db.host}:{db.port}/{db.database}"
    )
    engine = create_engine(db_url)
    with engine.connect() as connection:
        probe = connection.execute(text("SELECT 1")).scalar_one()
    if int(probe) != 1:
        raise AppError("DB_PROBE_FAILED", f"Неверный результат DB probe: {probe}")


def attach_sql_runner(vn: Any) -> None:
    """Подключает read-only `run_sql` на pg8000 для будущих NL->SQL вызовов."""

    db = get_db_config()
    db_url = (
        f"postgresql+pg8000://{db.user}:{db.password}"
        f"@{db.host}:{db.port}/{db.database}"
    )
    engine = create_engine(db_url)

    def run_sql(query: str) -> pd.DataFrame:
        return pd.read_sql_query(query, engine)

    vn.run_sql = run_sql
    vn.run_sql_is_set = True
    vn.dialect = "PostgreSQL"


def load_and_validate_assets(config: RunConfig) -> Assets:
    """Загружает и валидирует все артефакты Phase 2."""

    metrics_yaml = read_yaml(config.metrics_path, "metrics")
    dimensions_yaml = read_yaml(config.dimensions_path, "dimensions")
    few_shot_yaml = read_yaml(config.few_shot_path, "pairs")
    rules_text = read_text(config.rules_path)
    init_sql_text = read_text(config.init_sql_path)

    metrics = metrics_yaml["metrics"]
    dimensions = dimensions_yaml["dimensions"]
    pairs = few_shot_yaml["pairs"]

    validate_metrics(metrics)
    validate_dimensions(dimensions)
    validate_few_shot_pairs(pairs)

    cube_config = build_orders_cube(metrics=metrics, dimensions=dimensions)
    return Assets(
        metrics=metrics,
        dimensions=dimensions,
        few_shot_pairs=pairs,
        rules_text=rules_text,
        cube_config=cube_config,
        init_sql_text=init_sql_text,
    )


def train_vanna_assets(
    vn: Trainable, assets: Assets, log: Callable[..., None], request_id: str
) -> Dict[str, Any]:
    """Выполняет обучение Vanna на DDL, документации и few-shot парах."""

    trained_items = 0
    documentation_items = 0
    few_shot_items = 0

    log("INFO", "train_start", request_id, phase="phase2")

    vn.train(ddl=assets.init_sql_text)
    trained_items += 1
    log("INFO", "train_ddl_done", request_id)

    vn.train(documentation=assets.rules_text)
    trained_items += 1
    documentation_items += 1
    log("INFO", "train_rules_done", request_id)

    cube_doc = yaml.safe_dump(assets.cube_config, sort_keys=False, allow_unicode=True)
    vn.train(documentation=f"dotML cube configuration:\n{cube_doc}")
    trained_items += 1
    documentation_items += 1
    log("INFO", "train_cube_doc_done", request_id)

    for pair in assets.few_shot_pairs:
        vn.train(question=pair["question_ru"], sql=pair["sql"])
        trained_items += 1
        few_shot_items += 1

    log("INFO", "train_few_shot_done", request_id, few_shot_count=few_shot_items)

    return {
        "trained_items_total": trained_items,
        "documentation_items": documentation_items,
        "few_shot_items": few_shot_items,
    }


def run_pipeline(
    config: RunConfig,
    request_id: Optional[str] = None,
    trainer: Optional[Trainable] = None,
) -> Dict[str, Any]:
    """Запускает полный пайплайн подготовки и обучения Phase 2."""

    req_id = request_id or generate_request_id()
    log_event("INFO", "pipeline_start", req_id, dry_run=config.dry_run)

    assets = load_and_validate_assets(config)
    save_yaml(config.cube_output_path, assets.cube_config)
    log_event(
        "INFO",
        "cube_written",
        req_id,
        cube_path=str(config.cube_output_path),
        metrics_count=len(assets.metrics),
        dimensions_count=len(assets.dimensions),
    )

    if config.dry_run:
        log_event("INFO", "pipeline_dry_run_done", req_id)
        return {
            "status": "ok",
            "request_id": req_id,
            "dry_run": True,
            "few_shot_items": len(assets.few_shot_pairs),
        }

    if config.verify_db:
        verify_db_connection()
        log_event("INFO", "db_probe_ok", req_id)

    vn = trainer or create_trainer(os.getenv("OPENAI_API_KEY", ""), config.model)
    attach_sql_runner(vn)
    summary = train_vanna_assets(vn, assets, log_event, req_id)

    result = {
        "status": "ok",
        "request_id": req_id,
        "dry_run": False,
        **summary,
    }
    log_event("INFO", "pipeline_done", req_id, **summary)
    return result


def build_parser() -> argparse.ArgumentParser:
    """Создает CLI-парсер аргументов скрипта обучения."""

    parser = argparse.ArgumentParser(description="Train Vanna for Phase 2 assets.")
    parser.add_argument("--metrics-path", default=str(PROJECT_ROOT / "semantic/metrics.yaml"))
    parser.add_argument(
        "--dimensions-path", default=str(PROJECT_ROOT / "semantic/dimensions.yaml")
    )
    parser.add_argument(
        "--few-shot-path", default=str(PROJECT_ROOT / "training/few_shot_pairs.yaml")
    )
    parser.add_argument("--rules-path", default=str(PROJECT_ROOT / "training/business_rules.md"))
    parser.add_argument(
        "--cube-output-path", default=str(PROJECT_ROOT / "semantic/cubes/orders_cube.yaml")
    )
    parser.add_argument("--init-sql-path", default=str(PROJECT_ROOT / "init.sql"))
    parser.add_argument("--model", default="gpt-4o-mini")
    parser.add_argument("--request-id", default=None)
    parser.add_argument(
        "--skip-db-check",
        action="store_true",
        help="Пропустить проверку PostgreSQL перед обучением.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Только валидация и сборка артефактов без вызова vn.train().",
    )
    return parser


def cli_config_from_args(args: argparse.Namespace) -> RunConfig:
    """Преобразует CLI-аргументы в типизированный RunConfig."""

    return RunConfig(
        metrics_path=Path(args.metrics_path),
        dimensions_path=Path(args.dimensions_path),
        few_shot_path=Path(args.few_shot_path),
        rules_path=Path(args.rules_path),
        cube_output_path=Path(args.cube_output_path),
        init_sql_path=Path(args.init_sql_path),
        model=args.model,
        verify_db=not args.skip_db_check,
        dry_run=args.dry_run,
    )


def main() -> int:
    """CLI-точка входа с единым форматом ошибок."""

    parser = build_parser()
    args = parser.parse_args()
    config = cli_config_from_args(args)
    request_id = args.request_id or generate_request_id()

    try:
        result = run_pipeline(config=config, request_id=request_id)
        print(json.dumps(result, ensure_ascii=True))
        return 0
    except AppError as error:
        error_response = {
            "status": "error",
            "error_code": error.error_code,
            "message": error.message,
            "request_id": request_id,
        }
        print(json.dumps(error_response, ensure_ascii=True))
        return 1
    except Exception as error:
        error_response = {
            "status": "error",
            "error_code": "UNEXPECTED_ERROR",
            "message": repr(error),
            "request_id": request_id,
        }
        print(json.dumps(error_response, ensure_ascii=True))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
