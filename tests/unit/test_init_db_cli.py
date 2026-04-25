"""Unit-тесты CLI-загрузчика демо-среза `train.csv`."""

from __future__ import annotations

import unittest

from backend.app.interfaces.cli.init_db_cli import get_default_csv_path


class TestInitDbCli(unittest.TestCase):
    """Проверяет путь к CSV для локальной загрузки демо-данных."""

    def test_default_csv_path_points_to_repo_root_train_csv(self) -> None:
        """`init_db_cli` должен искать `train.csv` в корне проекта."""

        csv_path = get_default_csv_path()

        self.assertEqual(csv_path.name, "train.csv")
        self.assertTrue(csv_path.exists())
        self.assertTrue(csv_path.parent.name == "drivee_mpit")


if __name__ == "__main__":
    unittest.main()
