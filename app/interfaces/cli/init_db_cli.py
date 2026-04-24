"""Загружает обучающий срез из train.csv в таблицу PostgreSQL `orders`."""

from __future__ import annotations

import os

import pandas as pd
from sqlalchemy import create_engine

from backend.app.infrastructure.config.env_config import get_db_config


def load_data() -> None:
    """Загружает 10k строк в PostgreSQL-таблицу `orders`.

    Returns:
        None: записывает данные в настроенную PostgreSQL-базу.
    """
    # Используем единый конфиг из .env, чтобы не было расхождения
    # между docker-compose, init_db.py и init_vanna.py.
    config = get_db_config()
    db_url = (
        f"postgresql+pg8000://{config.user}:{config.password}"
        f"@{config.host}:{config.port}/{config.database}"
    )
    engine = create_engine(db_url)

    csv_path = os.path.join(os.path.dirname(__file__), "..", "train.csv")
    print("Loading first 10,000 rows from train.csv...")
    df = pd.read_csv(csv_path, nrows=10000)

    timestamp_columns = [
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
    ]

    for column in timestamp_columns:
        if column in df.columns:
            # Ошибочные/битые даты не роняют загрузку, а превращаются в NaT.
            df[column] = pd.to_datetime(df[column], errors="coerce")

    print("Writing rows to PostgreSQL table 'orders'...")
    # append: добавляем в существующую таблицу, не пересоздаем ее схему.
    df.to_sql("orders", engine, if_exists="append", index=False)
    print(f"Inserted {len(df)} rows.")


if __name__ == "__main__":
    load_data()
