"""Apply numbered plain-SQL migrations with asyncpg."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol


class MigrationConnection(Protocol):
    async def execute(self, query: str, *args: object) -> str: ...

    async def fetch(self, query: str, *args: object): ...

    def transaction(self): ...


MIGRATIONS = Path(__file__).with_name("migrations")


async def apply_migrations(connection: MigrationConnection) -> None:
    """Apply each migration once, atomically with its schema version."""
    await connection.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations ("
        "version integer PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
    )
    rows = await connection.fetch("SELECT version FROM schema_migrations ORDER BY version")
    applied = {row["version"] for row in rows}
    for path in sorted(MIGRATIONS.glob("[0-9][0-9][0-9]_*.sql")):
        version = int(path.name[:3])
        if version in applied:
            continue
        sql = path.read_text(encoding="utf-8")
        async with connection.transaction():
            await connection.execute(sql)
            await connection.execute(
                "INSERT INTO schema_migrations (version) VALUES ($1)",
                version,
            )
