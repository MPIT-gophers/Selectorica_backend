"""Unit-тесты для пайплайна Phase 2."""

from __future__ import annotations

import os
import unittest
from unittest import mock
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
TMP_ROOT = REPO_ROOT / ".tmp_test"

from backend.app.infrastructure.training.train_vanna_pipeline import (
    AppError,
    RunConfig,
    _resolve_llm_credentials,
    build_parser,
    cli_config_from_args,
    create_trainer,
    load_and_validate_assets,
    run_pipeline,
    validate_few_shot_pairs,
)
from backend.app.infrastructure import nl2sql_adapters


class FakeTrainer:
    """Тестовый тренер, который записывает вызовы train()."""

    def __init__(self) -> None:
        self.calls = []

    def train(self, **kwargs):
        """Сохраняет входные данные каждого вызова для проверок."""

        self.calls.append(kwargs)
        return f"id_{len(self.calls)}"


class TestPhase2TrainingPipeline(unittest.TestCase):
    """Проверяет unit-сценарии подготовки и обучения Phase 2."""

    def test_load_and_validate_assets_success(self):
        """Артефакты репозитория должны успешно валидироваться."""

        config = RunConfig(
            metrics_path=REPO_ROOT / "semantic/metrics.yaml",
            dimensions_path=REPO_ROOT / "semantic/dimensions.yaml",
            few_shot_path=REPO_ROOT / "training/few_shot_pairs.yaml",
            rules_path=REPO_ROOT / "training/business_rules.md",
            cube_output_path=REPO_ROOT / "semantic/cubes/orders_cube.yaml",
            init_sql_path=REPO_ROOT / "init.sql",
            model="gpt-4o-mini",
            verify_db=False,
            dry_run=True,
        )

        assets = load_and_validate_assets(config)
        metric_names = {metric["name"] for metric in assets.metrics}
        dimension_names = {dimension["name"] for dimension in assets.dimensions}

        self.assertGreaterEqual(len(assets.metrics), 3)
        self.assertGreaterEqual(len(assets.dimensions), 3)
        self.assertGreaterEqual(len(assets.few_shot_pairs), 8)
        self.assertIn("declined_tenders", metric_names)
        self.assertIn("avg_order_value_local", metric_names)
        self.assertIn("avg_accept_time_seconds", metric_names)
        self.assertIn("order_hour", dimension_names)
        self.assertIn("tender_status", dimension_names)

    def test_cli_default_paths_point_to_backend_root(self):
        """CLI-дефолты должны искать semantic/training артефакты от корня backend."""

        parser = build_parser()
        args = parser.parse_args(["--dry-run"])
        config = cli_config_from_args(args)

        self.assertEqual(config.metrics_path, REPO_ROOT / "semantic/metrics.yaml")
        self.assertEqual(config.dimensions_path, REPO_ROOT / "semantic/dimensions.yaml")
        self.assertEqual(config.few_shot_path, REPO_ROOT / "training/few_shot_pairs.yaml")
        self.assertTrue(config.metrics_path.exists())

    def test_validate_few_shot_pairs_duplicate_id(self):
        """Дубли id в few-shot должны приводить к AppError."""

        bad_pairs = [
            {"id": "same", "question_ru": "Q1", "sql": "SELECT 1", "notes": ""},
            {"id": "same", "question_ru": "Q2", "sql": "SELECT 2", "notes": ""},
        ] + [
            {"id": f"ok_{i}", "question_ru": f"Q{i}", "sql": "SELECT 1", "notes": ""}
            for i in range(6)
        ]

        with self.assertRaises(AppError) as ctx:
            validate_few_shot_pairs(bad_pairs)
        self.assertEqual(ctx.exception.error_code, "INVALID_FEWSHOT_SCHEMA")

    def test_validate_few_shot_pairs_allows_curated_expansion(self):
        """Few-shot база должна разрешать добавлять новые demo-примеры сверх MVP-минимума."""

        config = RunConfig(
            metrics_path=REPO_ROOT / "semantic/metrics.yaml",
            dimensions_path=REPO_ROOT / "semantic/dimensions.yaml",
            few_shot_path=REPO_ROOT / "training/few_shot_pairs.yaml",
            rules_path=REPO_ROOT / "training/business_rules.md",
            cube_output_path=REPO_ROOT / "semantic/cubes/orders_cube.yaml",
            init_sql_path=REPO_ROOT / "init.sql",
            model="gpt-4o-mini",
            verify_db=False,
            dry_run=True,
        )
        assets = load_and_validate_assets(config)
        expanded_pairs = list(assets.few_shot_pairs) + [
            {
                "id": "fs_extra_demo",
                "question_ru": "Какой средний чек по часам?",
                "sql": (
                    "SELECT EXTRACT(HOUR FROM order_timestamp) AS order_hour, "
                    "AVG(price_order_local) AS avg_order_value_local "
                    "FROM orders GROUP BY 1;"
                ),
                "notes": "Дополнительный demo-пример для расширения semantic coverage.",
            }
        ]

        validate_few_shot_pairs(expanded_pairs)

    def test_demo_terms_are_present_in_semantic_training_context(self):
        """Training context должен покрывать ключевые поля и термины текущего demo-set."""

        config = RunConfig(
            metrics_path=REPO_ROOT / "semantic/metrics.yaml",
            dimensions_path=REPO_ROOT / "semantic/dimensions.yaml",
            few_shot_path=REPO_ROOT / "training/few_shot_pairs.yaml",
            rules_path=REPO_ROOT / "training/business_rules.md",
            cube_output_path=REPO_ROOT / "semantic/cubes/orders_cube.yaml",
            init_sql_path=REPO_ROOT / "init.sql",
            model="gpt-4o-mini",
            verify_db=False,
            dry_run=True,
        )
        assets = load_and_validate_assets(config)
        training_context = self._semantic_training_context(assets)

        expected_terms = [
            "decline",
            "driveraccept_timestamp",
            "duration_in_seconds",
            "distance_in_meters",
            "price_start_local",
            "price_tender_local",
            "средний чек",
        ]
        for term in expected_terms:
            self.assertIn(term, training_context)

    def test_run_pipeline_training_order(self):
        """Пайплайн должен вызывать train в ожидаемом порядке и количестве."""

        fake = FakeTrainer()
        TMP_ROOT.mkdir(exist_ok=True)
        cube_path = TMP_ROOT / "orders_cube_unit.yaml"
        if cube_path.exists():
            cube_path.unlink()
        config = RunConfig(
            metrics_path=REPO_ROOT / "semantic/metrics.yaml",
            dimensions_path=REPO_ROOT / "semantic/dimensions.yaml",
            few_shot_path=REPO_ROOT / "training/few_shot_pairs.yaml",
            rules_path=REPO_ROOT / "training/business_rules.md",
            cube_output_path=cube_path,
            init_sql_path=REPO_ROOT / "init.sql",
            model="gpt-4o-mini",
            verify_db=False,
            dry_run=False,
        )

        result = run_pipeline(config=config, request_id="req_unit_test", trainer=fake)

        self.assertEqual(result["status"], "ok")
        self.assertGreaterEqual(result["few_shot_items"], 8)
        self.assertEqual(result["trained_items_total"], result["few_shot_items"] + 3)
        self.assertEqual(len(fake.calls), result["trained_items_total"])

        self.assertIn("ddl", fake.calls[0])
        self.assertIn("documentation", fake.calls[1])
        self.assertIn("documentation", fake.calls[2])
        self.assertIn("question", fake.calls[3])
        self.assertIn("sql", fake.calls[3])

        with cube_path.open("r", encoding="utf-8") as file:
            written_cube = yaml.safe_load(file)
        self.assertIn("cubes", written_cube)
        self.assertEqual(written_cube["cubes"][0]["name"], "orders")

    def test_resolve_llm_credentials_prefers_explicit_openai_key(self):
        """Явно переданный api_key должен иметь приоритет над OPENROUTER_API_KEY."""

        with mock.patch.dict(
            "os.environ",
            {
                "OPENROUTER_API_KEY": "or-key",
                "OPENAI_BASE_URL": "https://openrouter.ai/api/v1",
            },
            clear=False,
        ):
            api_key, base_url = _resolve_llm_credentials("openai-key")

        self.assertEqual(api_key, "openai-key")
        self.assertEqual(base_url, "https://openrouter.ai/api/v1")

    def test_resolve_llm_credentials_fallback_to_openrouter(self):
        """При пустом OPENAI_API_KEY должен использоваться OPENROUTER_API_KEY."""

        with mock.patch.dict(
            "os.environ",
            {"OPENROUTER_API_KEY": "or-key"},
            clear=False,
        ):
            api_key, base_url = _resolve_llm_credentials("")

        self.assertEqual(api_key, "or-key")
        self.assertEqual(base_url, "https://openrouter.ai/api/v1")

    def test_create_trainer_is_not_abstract_after_openrouter_changes(self):
        """create_trainer должен возвращать не-абстрактный тренер с embedding-методом."""

        trainer = create_trainer(api_key="test-key", model="openai/gpt-4o-mini")
        embedding = trainer.generate_embedding("abc")
        self.assertEqual(len(embedding), 5)
        self.assertEqual(embedding[0], 3.0)

    def test_create_trainer_persists_retrieval_context(self):
        """Тренер должен хранить и отдавать контекст для generate_sql retrieval-этапа."""

        trainer = create_trainer(api_key="test-key", model="openai/gpt-4o-mini")
        trainer.train(ddl="CREATE TABLE orders(id INT);")
        trainer.train(documentation="Таблица orders хранит поездки.")
        trainer.train(question="Сколько поездок?", sql="SELECT COUNT(*) FROM orders;")

        ddl = trainer.get_related_ddl("orders")
        docs = trainer.get_related_documentation("поездки")
        pairs = trainer.get_similar_question_sql("сколько поездок")

        self.assertTrue(len(ddl) > 0)
        self.assertTrue(len(docs) > 0)
        self.assertTrue(len(pairs) > 0)

    def test_runtime_model_is_loaded_from_repo_env_before_vanna_init(self):
        """Runtime должен читать OPENAI_MODEL из `.env` до инициализации Vanna."""

        def fake_load_repo_env() -> None:
            os.environ["OPENAI_MODEL"] = "openrouter/moonshotai/kimi-k2.6"

        with mock.patch.dict("os.environ", {}, clear=True):
            with mock.patch(
                "backend.app.infrastructure.nl2sql_adapters.load_repo_env",
                side_effect=fake_load_repo_env,
            ):
                model = nl2sql_adapters._resolve_runtime_model()

        self.assertEqual(model, "moonshotai/kimi-k2.6")

    def _semantic_training_context(self, assets: Any) -> str:
        """Собирает текстовый semantic/training context без DDL, чтобы проверять бизнес-покрытие."""

        context_parts = [
            yaml.safe_dump(assets.metrics, allow_unicode=True),
            yaml.safe_dump(assets.dimensions, allow_unicode=True),
            assets.rules_text,
            yaml.safe_dump(assets.few_shot_pairs, allow_unicode=True),
        ]
        return "\n".join(context_parts).lower()


if __name__ == "__main__":
    unittest.main()
