"""Production-загрузчик CSV-данных заказов в PostgreSQL."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from backend.app.infrastructure.config.env_config import get_db_config


TIMESTAMP_COLUMNS = (
    "order_timestamp",
    "tender_timestamp",
    "driveraccept_timestamp",
    "driverarrived_timestamp",
    "driverstarttheride_timestamp",
    "driverdone_timestamp",
    "clientcancel_timestamp",
    "drivercancel_timestamp",
    "order_modified_local",
    "cancel_before_accept_local",
)


def build_engine() -> Engine:
    """Создает SQLAlchemy engine для admin-загрузки данных из CSV."""

    config = get_db_config()
    db_url = (
        f"postgresql+pg8000://{config.user}:{config.password}"
        f"@{config.host}:{config.port}/{config.database}"
    )
    return create_engine(db_url)


def load_orders_csv(
    csv_path: Path,
    engine: Engine,
    chunk_size: int,
    truncate: bool,
) -> int:
    """Загружает CSV в `orders` чанками и возвращает число вставленных строк."""

    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero")
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    if truncate:
        with engine.begin() as connection:
            connection.execute(text("TRUNCATE TABLE orders"))

    inserted_rows = 0
    for chunk in _read_csv_chunks(csv_path=csv_path, chunk_size=chunk_size):
        normalized_chunk = normalize_orders_chunk(chunk)
        normalized_chunk.to_sql("orders", engine, if_exists="append", index=False)
        inserted_rows += len(normalized_chunk)
        print(f"Inserted {inserted_rows} rows...")

    return inserted_rows


def normalize_orders_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
    """Нормализует timestamp-колонки CSV-чанка перед вставкой в PostgreSQL."""

    normalized = chunk.copy()
    for column in TIMESTAMP_COLUMNS:
        if column in normalized.columns:
            normalized[column] = pd.to_datetime(normalized[column], errors="coerce")
    return normalized


def _read_csv_chunks(csv_path: Path, chunk_size: int) -> Iterable[pd.DataFrame]:
    """Читает CSV лениво, чтобы большой файл не загружался в память целиком."""

    return pd.read_csv(csv_path, chunksize=chunk_size)


def parse_args() -> argparse.Namespace:
    """Читает CLI-аргументы production-загрузчика заказов."""

    parser = argparse.ArgumentParser(description="Load full orders CSV into PostgreSQL.")
    parser.add_argument("--csv", type=Path, required=True, help="Path to source CSV file.")
    parser.add_argument("--chunk-size", type=int, default=50_000)
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate orders table before loading CSV.",
    )
    return parser.parse_args()


def main() -> int:
    """Точка входа production-загрузки CSV."""

    args = parse_args()
    engine = build_engine()
    try:
        inserted_rows = load_orders_csv(
            csv_path=args.csv,
            engine=engine,
            chunk_size=args.chunk_size,
            truncate=args.truncate,
        )
    finally:
        engine.dispose()

    print(f"Done. Inserted {inserted_rows} rows into orders.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
