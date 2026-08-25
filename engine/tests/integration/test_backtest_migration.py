"""Release migration checks for immutable strategy and backtest storage."""

import asyncio
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from helpers.migration_helpers import _table_names
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

BACKTEST_TABLES = {
    "backtest_authority_publication_marks",
    "exchange_calendar_authorities",
    "exchange_calendar_sessions",
    "backtest_authority_collections",
    "backtest_authority_bundles",
    "backtest_universe_authority_members",
    "backtest_sector_authority_members",
    "backtest_corporate_action_authority_members",
    "backtest_price_authority_members",
    "backtest_instrument_authority_members",
    "strategy_definitions",
    "backtest_runs",
    "backtest_windows",
    "backtest_nav_points",
    "backtest_fills",
    "backtest_order_outcomes",
    "backtest_metrics",
}


async def _seed_same_session_price_rows(database_url: str, *, interval: str) -> None:
    engine = create_async_engine(database_url)
    try:
        artifact_id = uuid4()
        manifest_id = uuid4()
        instrument_id = uuid4()
        first_at = datetime(2026, 8, 24, 20, tzinfo=UTC)
        second_at = datetime(2026, 8, 25, 20, tzinfo=UTC)
        second_observed_at = (
            datetime(2026, 8, 24, 20, 1, tzinfo=UTC) if interval == "1Min" else first_at
        )
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO data_artifacts "
                    "(id, uri, sha256, byte_size, mime_type, schema_version, source, "
                    "observed_at, available_at, received_at) VALUES "
                    "(:id, :uri, :digest, 100, 'application/x-parquet', '1', 'fixture', "
                    "'2026-08-24T20:00:00Z', '2026-08-24T20:00:00Z', "
                    "'2026-08-24T20:00:00Z')"
                ),
                {"id": artifact_id, "uri": f"memory://{artifact_id}", "digest": "a" * 64},
            )
            await connection.execute(
                text(
                    "INSERT INTO price_series_manifests "
                    "(id, data_artifact_id, instrument, source, feed, delivery_mode, "
                    "adjustment, interval, start_at, end_at, row_count) VALUES "
                    "(:id, :artifact, :instrument, 'fixture', 'test', 'fixture', "
                    "'raw', :interval, '2026-08-24T20:00:00Z', "
                    "'2026-08-25T20:00:00Z', 2)"
                ),
                {
                    "id": manifest_id,
                    "artifact": artifact_id,
                    "instrument": str(instrument_id),
                    "interval": interval,
                },
            )
            await connection.execute(
                text(
                    "INSERT INTO price_bar_records "
                    "(id, price_series_manifest_id, instrument_id, session, open, high, "
                    "low, close, volume, observed_at, available_at, received_at, revision, "
                    "quality_flags) VALUES "
                    "(:id, :manifest, :instrument, '2026-08-24', 10, 10, 10, 10, 100, "
                    ":observed, :available, :available, :revision, '[]'::jsonb)"
                ),
                (
                    {
                        "id": uuid4(),
                        "manifest": manifest_id,
                        "instrument": instrument_id,
                        "observed": first_at,
                        "available": first_at,
                        "revision": "1",
                    },
                    {
                        "id": uuid4(),
                        "manifest": manifest_id,
                        "instrument": instrument_id,
                        "observed": second_observed_at,
                        "available": second_at if interval == "1Day" else second_observed_at,
                        "revision": "2" if interval == "1Day" else "1",
                    },
                ),
            )
    finally:
        await engine.dispose()


def test_0012_round_trips_from_0011(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    """The backtest release must be one reversible successor of Agent authority."""
    engine_root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(engine_root / "alembic.ini"))

    command.upgrade(config, "0011_agents")
    assert BACKTEST_TABLES.isdisjoint(_table_names(temporary_database_url))
    command.upgrade(config, "0012_backtests")
    assert BACKTEST_TABLES.issubset(_table_names(temporary_database_url))
    command.downgrade(config, "0011_agents")
    assert BACKTEST_TABLES.isdisjoint(_table_names(temporary_database_url))
    command.upgrade(config, "0012_backtests")
    assert BACKTEST_TABLES.issubset(_table_names(temporary_database_url))


def test_published_strategy_definition_is_immutable_and_hash_bounded(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    """Published strategy source must not be mutable or accept an ambiguous digest."""
    engine_root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    command.upgrade(Config(str(engine_root / "alembic.ini")), "0012_backtests")

    async def exercise() -> None:
        engine = create_async_engine(temporary_database_url)
        try:
            strategy_id = uuid4()
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "INSERT INTO strategy_definitions "
                        "(id, strategy_version, schema_version, expression_hash, document, "
                        "lifecycle, published_at) VALUES "
                        "(:id, 'baseline_v1', '1', :digest, CAST(:document AS jsonb), "
                        "'champion', '2026-08-26T00:00:00Z')"
                    ),
                    {"id": strategy_id, "digest": "a" * 64, "document": '{"version":"1"}'},
                )

            async with engine.begin() as connection:
                with pytest.raises(DBAPIError, match="backtest record is immutable"):
                    await connection.execute(
                        text(
                            "UPDATE strategy_definitions SET lifecycle = 'retired' WHERE id = :id"
                        ),
                        {"id": strategy_id},
                    )

            async with engine.begin() as connection:
                with pytest.raises(DBAPIError, match="expression_hash_canonical"):
                    await connection.execute(
                        text(
                            "INSERT INTO strategy_definitions "
                            "(id, strategy_version, schema_version, expression_hash, document, "
                            "lifecycle, published_at) VALUES "
                            "(:id, 'bad_v1', '1', 'ABC', '{}'::jsonb, 'challenger', "
                            "'2026-08-26T00:00:00Z')"
                        ),
                        {"id": uuid4()},
                    )
        finally:
            await engine.dispose()

    asyncio.run(exercise())


def test_0012_downgrade_fails_closed_when_a_daily_manifest_contains_revisions(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    """Task 16 must never discard immutable same-session revision evidence."""
    engine_root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(engine_root / "alembic.ini"))
    command.upgrade(config, "0012_backtests")

    asyncio.run(_seed_same_session_price_rows(temporary_database_url, interval="1Day"))

    with pytest.raises(DBAPIError, match="revision-aware price manifests require Task 16"):
        command.downgrade(config, "0011_agents")
    assert BACKTEST_TABLES.issubset(_table_names(temporary_database_url))


def test_0012_downgrade_allows_multiple_intraday_bars_in_one_session(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    """Normal intraday bars are not Task 16 daily revision evidence."""
    engine_root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(engine_root / "alembic.ini"))
    command.upgrade(config, "0012_backtests")
    asyncio.run(_seed_same_session_price_rows(temporary_database_url, interval="1Min"))

    command.downgrade(config, "0011_agents")

    assert BACKTEST_TABLES.isdisjoint(_table_names(temporary_database_url))
