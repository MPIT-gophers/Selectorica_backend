"""Инициализация Vanna и проверка подключения к PostgreSQL.

Скрипт использует актуальный API Vanna (`PostgresRunner`) и выполняет
smoke-test запросом `SELECT 1`.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import uuid

from sqlalchemy import create_engine, text

from backend.app.infrastructure.config.env_config import DbConfig, get_db_config
from vanna.capabilities.sql_runner.models import RunSqlToolArgs
from vanna.core.tool.models import ToolContext
from vanna.core.user.models import User
from vanna.integrations.local.agent_memory.in_memory import DemoAgentMemory
from vanna.integrations.postgres import PostgresRunner

def parse_args() -> DbConfig:
    """Читает CLI-аргументы и возвращает итоговый конфиг БД.

    Returns:
        DbConfig: финальные параметры после `.env` и возможных CLI-переопределений.
    """
    parser = argparse.ArgumentParser(
        description="Initialize Vanna PostgresRunner and verify DB connectivity."
    )
    # Сначала читаем единые настройки из .env, потом при необходимости
    # локально переопределяем через аргументы командной строки.
    defaults = get_db_config()
    parser.add_argument("--host", default=defaults.host)
    parser.add_argument("--port", type=int, default=defaults.port)
    parser.add_argument("--database", default=defaults.database)
    parser.add_argument("--user", default=defaults.user)
    parser.add_argument("--password", default=defaults.password)
    args = parser.parse_args()
    return DbConfig(
        host=args.host,
        port=args.port,
        database=args.database,
        user=args.user,
        password=args.password,
    )


def make_tool_context() -> ToolContext:
    """Создает минимальный `ToolContext`, нужный Vanna для `run_sql`.

    Returns:
        ToolContext: локальный контекст с техническими ID запроса.
    """
    # Vanna ожидает ToolContext даже для простого run_sql, поэтому создаем
    # минимальный локальный контекст без внешних зависимостей.
    return ToolContext(
        user=User(id="local-dev"),
        conversation_id=str(uuid.uuid4()),
        request_id=str(uuid.uuid4()),
        agent_memory=DemoAgentMemory(),
    )


async def check_vanna_runner(config: DbConfig) -> None:
    """Выполняет основную проверку подключения через `Vanna PostgresRunner`.

    Args:
        config: параметры подключения к PostgreSQL.

    Returns:
        None: выбрасывает исключение при ошибке коннекта или неожиданном ответе.
    """
    # Основная проверка: используем нативный раннер Vanna для Postgres.
    runner = PostgresRunner(
        host=config.host,
        port=config.port,
        database=config.database,
        user=config.user,
        password=config.password,
    )
    context = make_tool_context()
    df = await runner.run_sql(RunSqlToolArgs(sql="SELECT 1 AS ok"), context)
    value = int(df.iloc[0]["ok"])
    if value != 1:
        raise RuntimeError(f"Unexpected probe result from Vanna runner: {value}")


def check_pg8000_fallback(config: DbConfig) -> None:
    """Выполняет резервную проверку через `pg8000`, если Vanna-путь упал.

    Args:
        config: параметры подключения к PostgreSQL.

    Returns:
        None: выбрасывает исключение, если fallback-подключение недоступно.
    """
    # Резервная проверка чисто для диагностики среды:
    # если Vanna-путь падает из-за psycopg2/encoding, проверяем,
    # доступна ли сама БД через pg8000.
    url = (
        f"postgresql+pg8000://{config.user}:{config.password}"
        f"@{config.host}:{config.port}/{config.database}"
    )
    engine = create_engine(url)
    with engine.connect() as connection:
        probe = connection.execute(text("SELECT 1")).scalar_one()
    if int(probe) != 1:
        raise RuntimeError(f"Unexpected probe result from pg8000 fallback: {probe}")


def main() -> int:
    """Точка входа скрипта инициализации и проверки Vanna-подключения.

    Returns:
        int: код завершения (`0` — успех, `1` — ошибка).
    """
    config = parse_args()

    print("Initializing Vanna PostgresRunner...")
    print(
        f"Using connection target: {config.host}:{config.port}/{config.database} "
        f"(user={config.user})"
    )
    try:
        asyncio.run(check_vanna_runner(config))
        print("OK: Vanna PostgresRunner connected successfully.")
        return 0
    except UnicodeDecodeError as err:
        # Известный кейс на Windows: psycopg2 может упасть при декодировании
        # сообщения об ошибке от сервера, поэтому отдельно проверяем fallback.
        print("WARN: Vanna PostgresRunner failed with UnicodeDecodeError.")
        print(f"WARN: {safe_error_text(err)}")
        print("Trying pg8000 connectivity fallback...")
        try:
            check_pg8000_fallback(config)
            print("OK: pg8000 fallback connected successfully.")
            print(
                "INFO: Database is reachable, but Vanna's psycopg2 path needs a"
                " Windows encoding workaround."
            )
            return 0
        except Exception as fallback_err:
            print(f"ERROR: pg8000 fallback also failed: {safe_error_text(fallback_err)}")
            return 1
    except Exception as err:
        print(f"ERROR: Vanna PostgresRunner connectivity check failed: {safe_error_text(err)}")
        return 1


def safe_error_text(err: Exception) -> str:
    """Безопасно форматирует исключение для не-UTF8 консоли Windows.

    Args:
        err: исходное исключение.

    Returns:
        str: ASCII-совместимая строка, печатаемая в cp1251/cp866 консоли.
    """
    # Не все консоли на Windows дружат с Unicode-символами в traceback.
    # backslashreplace гарантирует, что лог точно напечатается.
    rendered = repr(err)
    return rendered.encode("ascii", "backslashreplace").decode("ascii")


if __name__ == "__main__":
    sys.exit(main())
