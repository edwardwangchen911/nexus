import asyncio
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


async def _policy_count(database_url: str) -> int:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            return int(
                await connection.scalar(
                    text(
                        "SELECT count(*) FROM source_policies "
                        "WHERE key='intraday_monitor' "
                        "AND version='intraday_monitor_policy_v5'"
                    )
                )
                or 0
            )
    finally:
        await engine.dispose()


def test_0054_round_trips_without_schwab_facts(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))

    command.upgrade(config, "0053_alpaca_snapshot_policy")
    assert asyncio.run(_policy_count(temporary_database_url)) == 0
    command.upgrade(config, "0054_schwab_stream_policy")
    assert asyncio.run(_policy_count(temporary_database_url)) == 1
    command.downgrade(config, "0053_alpaca_snapshot_policy")
    assert asyncio.run(_policy_count(temporary_database_url)) == 0
    command.upgrade(config, "0054_schwab_stream_policy")
    assert asyncio.run(_policy_count(temporary_database_url)) == 1
