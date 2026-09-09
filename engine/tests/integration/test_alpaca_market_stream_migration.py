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
                        "AND version='intraday_monitor_policy_v3'"
                    )
                )
                or 0
            )
    finally:
        await engine.dispose()


def test_0047_round_trips_without_stream_facts(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))

    command.upgrade(config, "0046_fosun_push_quote_authority")
    assert asyncio.run(_policy_count(temporary_database_url)) == 0
    command.upgrade(config, "0047_alpaca_market_stream")
    assert asyncio.run(_policy_count(temporary_database_url)) == 1
    command.downgrade(config, "0046_fosun_push_quote_authority")
    assert asyncio.run(_policy_count(temporary_database_url)) == 0
    command.upgrade(config, "0047_alpaca_market_stream")
    assert asyncio.run(_policy_count(temporary_database_url)) == 1
