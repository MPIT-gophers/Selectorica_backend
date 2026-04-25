"""SQLite-репозиторий истории отчетов фазы 4."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List


@dataclass(frozen=True)
class ReportRecord:
    """Структура одной сохраненной записи отчета."""

    question: str
    sql_text: str
    asked_at: str
    refinement_trace: list[dict[str, str]]
    explain_text: str = ""
    confidence: dict[str, Any] = None
    recommended_actions: list[str] = None
    assumptions: list[str] = None
    resolved_params: dict[str, Any] = None
    decision_events: list[dict[str, Any]] = None


class ReportHistoryRepository:
    """Репозиторий для сохранения и чтения истории отчетов в SQLite."""

    def __init__(self, db_path: Path) -> None:
        """Создает репозиторий и гарантирует наличие схемы таблиц."""

        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def save_report(self, record: ReportRecord) -> None:
        """Сохраняет запись отчета в таблицу `reports`."""

        with closing(sqlite3.connect(self._db_path)) as connection:
            connection.execute(
                """
                INSERT INTO reports (
                    question,
                    sql_text,
                    asked_at,
                    refinement_trace_json,
                    explain_text,
                    confidence_json,
                    recommended_actions_json,
                    assumptions_json,
                    resolved_params_json,
                    decision_events_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.question,
                    record.sql_text,
                    record.asked_at,
                    json.dumps(record.refinement_trace, ensure_ascii=False),
                    record.explain_text,
                    json.dumps(record.confidence or {}, ensure_ascii=False),
                    json.dumps(record.recommended_actions or [], ensure_ascii=False),
                    json.dumps(record.assumptions or [], ensure_ascii=False),
                    json.dumps(record.resolved_params or {}, ensure_ascii=False),
                    json.dumps(record.decision_events or [], ensure_ascii=False),
                ),
            )
            connection.commit()

    def list_reports(self, limit: int = 50) -> List[ReportRecord]:
        """Возвращает последние записи отчетов (новые сверху)."""

        with closing(sqlite3.connect(self._db_path)) as connection:
            rows = connection.execute(
                """
                SELECT
                    question,
                    sql_text,
                    asked_at,
                    refinement_trace_json,
                    explain_text,
                    confidence_json,
                    recommended_actions_json,
                    assumptions_json,
                    resolved_params_json,
                    decision_events_json
                FROM reports
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

        return [
            ReportRecord(
                question=row[0],
                sql_text=row[1],
                asked_at=row[2],
                refinement_trace=self._parse_refinement_trace(row[3]),
                explain_text=row[4] or "",
                confidence=self._parse_confidence(row[5]),
                recommended_actions=self._parse_recommended_actions(row[6]),
                assumptions=self._parse_string_list(row[7]),
                resolved_params=self._parse_dict(row[8]),
                decision_events=self._parse_event_list(row[9]),
            )
            for row in rows
        ]

    def _init_schema(self) -> None:
        """Создает таблицу истории, если она отсутствует."""

        with closing(sqlite3.connect(self._db_path)) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    question TEXT NOT NULL,
                    sql_text TEXT NOT NULL,
                    asked_at TEXT NOT NULL,
                    refinement_trace_json TEXT NOT NULL DEFAULT '[]',
                    explain_text TEXT NOT NULL DEFAULT '',
                    confidence_json TEXT NOT NULL DEFAULT '{}',
                    recommended_actions_json TEXT NOT NULL DEFAULT '[]',
                    assumptions_json TEXT NOT NULL DEFAULT '[]',
                    resolved_params_json TEXT NOT NULL DEFAULT '{}',
                    decision_events_json TEXT NOT NULL DEFAULT '[]'
                )
                """
            )
            columns = {
                row[1]
                for row in connection.execute("PRAGMA table_info(reports)").fetchall()
            }
            if "refinement_trace_json" not in columns:
                connection.execute(
                    """
                    ALTER TABLE reports
                    ADD COLUMN refinement_trace_json TEXT NOT NULL DEFAULT '[]'
                    """
                )
            if "explain_text" not in columns:
                connection.execute(
                    """
                    ALTER TABLE reports
                    ADD COLUMN explain_text TEXT NOT NULL DEFAULT ''
                    """
                )
            if "confidence_json" not in columns:
                connection.execute(
                    """
                    ALTER TABLE reports
                    ADD COLUMN confidence_json TEXT NOT NULL DEFAULT '{}'
                    """
                )
            if "recommended_actions_json" not in columns:
                connection.execute(
                    """
                    ALTER TABLE reports
                    ADD COLUMN recommended_actions_json TEXT NOT NULL DEFAULT '[]'
                    """
                )
            if "assumptions_json" not in columns:
                connection.execute(
                    """
                    ALTER TABLE reports
                    ADD COLUMN assumptions_json TEXT NOT NULL DEFAULT '[]'
                    """
                )
            if "resolved_params_json" not in columns:
                connection.execute(
                    """
                    ALTER TABLE reports
                    ADD COLUMN resolved_params_json TEXT NOT NULL DEFAULT '{}'
                    """
                )
            if "decision_events_json" not in columns:
                connection.execute(
                    """
                    ALTER TABLE reports
                    ADD COLUMN decision_events_json TEXT NOT NULL DEFAULT '[]'
                    """
                )
            connection.commit()

    def _parse_refinement_trace(self, raw_value: str | None) -> list[dict[str, str]]:
        """Преобразует JSON-строку refinement trace в Python-список."""

        if not raw_value:
            return []

        try:
            parsed = json.loads(raw_value)
        except json.JSONDecodeError:
            return []

        if not isinstance(parsed, list):
            return []

        normalized: list[dict[str, str]] = []
        for item in parsed:
            if not isinstance(item, dict):
                continue
            normalized.append(
                {
                    "question": str(item.get("question", "")),
                    "selected_label": str(item.get("selected_label", "")),
                    "selected_value": str(item.get("selected_value", "")),
                }
            )
        return normalized

    def _parse_confidence(self, raw_value: str | None) -> dict[str, Any]:
        """Преобразует confidence JSON в словарь с безопасным fallback."""

        if not raw_value:
            return {}
        try:
            parsed = json.loads(raw_value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _parse_recommended_actions(self, raw_value: str | None) -> list[str]:
        """Преобразует JSON-массив action-подсказок в список строк."""

        return self._parse_string_list(raw_value)

    def _parse_string_list(self, raw_value: str | None) -> list[str]:
        """Преобразует JSON-массив строк в безопасный список строк."""

        if not raw_value:
            return []
        try:
            parsed = json.loads(raw_value)
        except json.JSONDecodeError:
            return []
        if not isinstance(parsed, list):
            return []
        return [str(item) for item in parsed]

    def _parse_dict(self, raw_value: str | None) -> dict[str, Any]:
        """Преобразует JSON-объект в словарь с безопасным fallback."""

        if not raw_value:
            return {}
        try:
            parsed = json.loads(raw_value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _parse_event_list(self, raw_value: str | None) -> list[dict[str, Any]]:
        """Преобразует JSON-массив событий decision engine в список словарей."""

        if not raw_value:
            return []
        try:
            parsed = json.loads(raw_value)
        except json.JSONDecodeError:
            return []
        if not isinstance(parsed, list):
            return []
        return [item for item in parsed if isinstance(item, dict)]
