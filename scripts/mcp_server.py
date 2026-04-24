"""Совместимость: legacy-импорты MCP query server из scripts."""

from backend.app.infrastructure.mcp.query_server import *  # noqa: F403


if __name__ == "__main__":
    mcp.run()
