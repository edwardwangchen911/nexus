"""Exercise a historical migration's own fence without bypassing the current head."""

import importlib.util
from pathlib import Path

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


async def historical_downgrade_guard(database_url, revision):
    path = Path(__file__).parents[2] / "alembic" / "versions" / f"{revision}.py"
    spec = importlib.util.spec_from_file_location(f"guard_{revision}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:

            async def fingerprint():
                tables = await connection.scalars(
                    text(
                        "SELECT tablename FROM pg_tables WHERE schemaname='public' AND "
                        "(tablename LIKE 'research_stage_%' "
                        "OR tablename LIKE 'research_monitor_%') "
                        "ORDER BY tablename"
                    )
                )
                return {
                    table: await connection.scalar(
                        text(
                            "SELECT md5(COALESCE(string_agg(to_jsonb(r)::text, "
                            f"'' ORDER BY to_jsonb(r)::text), '')) FROM {table} r"
                        )
                    )
                    for table in tables
                }

            before = await connection.scalar(text("SELECT version_num FROM alembic_version"))
            facts_before = await fingerprint()
            await connection.rollback()
            transaction = await connection.begin()
            try:

                def downgrade(sync_connection):
                    module.op = Operations(MigrationContext.configure(sync_connection))
                    module.downgrade()

                await connection.run_sync(downgrade)
            finally:
                # Even an unexpectedly permissive historical implementation cannot
                # remove current stage facts from this isolated fixture.
                await transaction.rollback()
                assert (
                    await connection.scalar(text("SELECT version_num FROM alembic_version"))
                    == before
                )
                assert await fingerprint() == facts_before
    finally:
        await engine.dispose()
