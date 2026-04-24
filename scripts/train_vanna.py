"""Совместимость: legacy-entrypoint тренировки Vanna."""

from backend.app.infrastructure.training.train_vanna_pipeline import *  # noqa: F403
from backend.app.infrastructure.training.train_vanna_pipeline import _resolve_llm_credentials


if __name__ == "__main__":
    raise SystemExit(main())
