"""Unit-тесты production-загрузчика заказов из CSV."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

from backend.app.interfaces.cli.load_orders_cli import load_orders_csv


TEST_TMP_DIR = Path(__file__).resolve().parents[2] / "tmp"


def _write_test_csv(name: str, content: str) -> Path:
    """Создает локальный временный CSV внутри ignored backend/tmp."""

    TEST_TMP_DIR.mkdir(exist_ok=True)
    csv_path = TEST_TMP_DIR / name
    csv_path.write_text(content, encoding="utf-8")
    return csv_path


def test_load_orders_csv_writes_chunks_and_normalizes_timestamps() -> None:
    """CSV должен загружаться чанками, а timestamp-поля нормализоваться до datetime."""

    csv_path = _write_test_csv(
        "orders_loader_chunks.csv",
        "\n".join(
            [
                "city_id,order_id,order_timestamp,price_order_local",
                "1,o1,2026-04-24 10:00:00,100.5",
                "2,o2,bad-date,200.0",
                "3,o3,2026-04-25 11:00:00,300.0",
            ]
        ),
    )
    engine = Mock()

    with patch.object(pd.DataFrame, "to_sql", autospec=True) as to_sql:
        inserted_rows = load_orders_csv(
            csv_path=csv_path,
            engine=engine,
            chunk_size=2,
            truncate=False,
        )

    assert inserted_rows == 3
    assert to_sql.call_count == 2
    first_chunk = to_sql.call_args_list[0].args[0]
    assert str(first_chunk["order_timestamp"].dtype).startswith("datetime64")
    assert pd.isna(first_chunk["order_timestamp"].iloc[1])


def test_load_orders_csv_truncates_before_insert() -> None:
    """Флаг truncate должен очищать таблицу перед первой вставкой."""

    csv_path = _write_test_csv("orders_loader_truncate.csv", "city_id,order_id\n1,o1\n")

    connection = Mock()
    context = Mock()
    context.__enter__ = Mock(return_value=connection)
    context.__exit__ = Mock(return_value=False)
    engine = Mock()
    engine.begin.return_value = context

    with patch.object(pd.DataFrame, "to_sql", autospec=True):
        load_orders_csv(
            csv_path=csv_path,
            engine=engine,
            chunk_size=100,
            truncate=True,
        )

    connection.execute.assert_called_once()
