"""Совместимость: legacy-entrypoint загрузки данных в БД."""

from backend.app.interfaces.cli.init_db_cli import *  # noqa: F403


if __name__ == "__main__":
    load_data()
