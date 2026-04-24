"""Совместимость: legacy-entrypoint инициализации Vanna."""

from backend.app.interfaces.cli.init_vanna_cli import *  # noqa: F403


if __name__ == "__main__":
    raise SystemExit(main())
