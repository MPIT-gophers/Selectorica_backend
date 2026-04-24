"""Integration-тесты для реального запуска train-пайплайна."""

from __future__ import annotations

import os
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
TMP_ROOT = REPO_ROOT / ".tmp_test"

from backend.scripts.train_vanna import RunConfig, run_pipeline


@unittest.skipUnless(
    os.getenv("RUN_INTEGRATION") == "1" and bool(os.getenv("OPENAI_API_KEY")),
    "Integration-тесты запускаются только при RUN_INTEGRATION=1 и OPENAI_API_KEY.",
)
class TestTrainVannaIntegration(unittest.TestCase):
    """Проверяет end-to-end запуск реального train-пайплайна."""

    def test_run_pipeline_real_trainer(self):
        """Pipeline должен завершиться успешно и обучить 8 few-shot примеров."""

        TMP_ROOT.mkdir(exist_ok=True)
        cube_path = TMP_ROOT / "orders_cube_integration.yaml"
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

        result = run_pipeline(config=config, request_id="req_it_train_vanna")
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["few_shot_items"], 8)
        self.assertTrue(cube_path.exists())


if __name__ == "__main__":
    unittest.main()
