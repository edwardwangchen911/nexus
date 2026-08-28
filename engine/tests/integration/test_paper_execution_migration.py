"""Task 19 paper execution authority migration contract."""

import asyncio
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

ALEMBIC_CONFIG = Path(__file__).parents[2] / "alembic.ini"
EXECUTION_TABLES = {
    "paper_execution_models",
    "paper_execution_window_authorities",
    "paper_execution_window_members",
    "paper_request_idempotency",
    "paper_performance_attributions",
}
FILL_COLUMNS = {
    "execution_model_id",
    "execution_window_authority_id",
    "ledger_transaction_id",
    "window_vwap",
    "half_spread",
    "slippage_bps",
    "slippage_amount",
    "commission",
    "regulatory_fee",
    "evaluated_at",
}


def _config() -> Config:
    return Config(str(ALEMBIC_CONFIG))


async def _rows(database_url: str, statement: str) -> list[tuple[object, ...]]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            return list((await connection.execute(text(statement))).tuples())
    finally:
        await engine.dispose()


async def _insert_fixture_calendar(database_url: str) -> tuple[object, object]:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO exchange_calendar_authorities "
                    "(id, exchange, version, source, range_start, range_end, "
                    "query_parameters, completeness_status, content_hash, published_at) "
                    "VALUES ('62000000-0000-0000-0000-000000000001', 'XNYS', "
                    "'task19-calendar', 'fixture', '2026-08-27', '2026-08-27', "
                    "'{}'::jsonb, 'complete', :hash, '2026-08-26T20:00:00Z')"
                ),
                {"hash": "6" * 64},
            )
            await connection.execute(
                text(
                    "INSERT INTO exchange_calendar_sessions "
                    "(calendar_authority_id, session, decision_at) VALUES "
                    "('62000000-0000-0000-0000-000000000001', '2026-08-27', "
                    "'2026-08-27T20:00:00Z')"
                )
            )
        rows = await _rows(
            database_url,
            "SELECT market_open_at, market_close_at FROM exchange_calendar_sessions "
            "WHERE calendar_authority_id = "
            "'62000000-0000-0000-0000-000000000001'",
        )
        return rows[0]
    finally:
        await engine.dispose()


async def _mutation_is_rejected(database_url: str) -> bool:
    engine = create_async_engine(database_url)
    try:
        with pytest.raises(DBAPIError):
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "UPDATE paper_execution_models SET version = 'rewritten' "
                        "WHERE version = 'execution_model_v1'"
                    )
                )
        return True
    finally:
        await engine.dispose()


def test_0020_is_the_single_head_after_task19() -> None:
    scripts = ScriptDirectory.from_config(_config())

    assert scripts.get_heads() == ["0024_broker_simulation"]
    revision = scripts.get_revision("0015_paper_execution")
    assert revision is not None
    assert revision.down_revision == "0014_paper"
    atomic_revision = scripts.get_revision("0016_paper_execution_atomic")
    assert atomic_revision is not None
    assert atomic_revision.down_revision == "0015_paper_execution"
    time_order_revision = scripts.get_revision("0017_paper_execution_time_order")
    assert time_order_revision is not None
    assert time_order_revision.down_revision == "0016_paper_execution_atomic"
    performance_revision = scripts.get_revision("0018_paper_performance_chain")
    assert performance_revision is not None
    assert performance_revision.down_revision == "0017_paper_execution_time_order"
    performance_root = scripts.get_revision("0019_paper_performance_root")
    assert performance_root is not None
    assert performance_root.down_revision == "0018_paper_performance_chain"
    automation = scripts.get_revision("0020_resilient_automation_audit")
    retry_allowance = scripts.get_revision("0021_manual_job_retry_allowance")
    assert automation is not None
    assert automation.down_revision == "0019_paper_performance_root"
    assert retry_allowance is not None
    assert retry_allowance.down_revision == "0020_resilient_automation_audit"


def test_execution_migration_seeds_authority_backfills_calendar_and_round_trips(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = _config()

    command.upgrade(config, "head")
    tables = {
        str(row[0])
        for row in asyncio.run(
            _rows(
                temporary_database_url,
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
            )
        )
    }
    assert EXECUTION_TABLES <= tables
    fill_columns = {
        str(row[0])
        for row in asyncio.run(
            _rows(
                temporary_database_url,
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = 'paper_fills'",
            )
        )
    }
    assert FILL_COLUMNS <= fill_columns
    order_event_columns = {
        str(row[0])
        for row in asyncio.run(
            _rows(
                temporary_database_url,
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = 'paper_order_events'",
            )
        )
    }
    assert "source_fill_id" in order_event_columns
    atomic_constraints = {
        str(row[0])
        for row in asyncio.run(
            _rows(
                temporary_database_url,
                "SELECT conname FROM pg_constraint WHERE conname IN ("
                "'uq_paper_order_events_source_fill',"
                "'fk_paper_order_events_source_fill',"
                "'ck_paper_order_events_fill_binding',"
                "'uq_paper_position_adjustments_source_fill')",
            )
        )
    }
    assert atomic_constraints == {
        "uq_paper_order_events_source_fill",
        "fk_paper_order_events_source_fill",
        "ck_paper_order_events_fill_binding",
        "uq_paper_position_adjustments_source_fill",
    }
    window_chain_trigger = asyncio.run(
        _rows(
            temporary_database_url,
            "SELECT tgname FROM pg_trigger WHERE tgname = "
            "'trg_paper_execution_window_authorities_chain' AND NOT tgisinternal",
        )
    )
    assert window_chain_trigger == [
        ("trg_paper_execution_window_authorities_chain",)
    ]
    performance_chain_trigger = asyncio.run(
        _rows(
            temporary_database_url,
            "SELECT tgname FROM pg_trigger WHERE tgname = "
            "'trg_paper_performance_snapshots_chain' AND NOT tgisinternal",
        )
    )
    assert performance_chain_trigger == [("trg_paper_performance_snapshots_chain",)]
    performance_root_triggers = {
        str(row[0])
        for row in asyncio.run(
            _rows(
                temporary_database_url,
                "SELECT tgname FROM pg_trigger WHERE tgname LIKE "
                "'trg_paper_%_performance_root' AND NOT tgisinternal",
            )
        )
    }
    assert performance_root_triggers == {
        "trg_paper_performance_snapshots_performance_root",
        "trg_paper_position_snapshots_performance_root",
        "trg_paper_performance_attributions_performance_root",
    }
    calendar_columns = {
        str(row[0]): str(row[1])
        for row in asyncio.run(
            _rows(
                temporary_database_url,
                "SELECT column_name, is_nullable FROM information_schema.columns "
                "WHERE table_schema = 'public' "
                "AND table_name = 'exchange_calendar_sessions' "
                "AND column_name IN ('market_open_at','market_close_at')",
            )
        )
    }
    assert calendar_columns == {
        "market_close_at": "NO",
        "market_open_at": "NO",
    }
    model = asyncio.run(
        _rows(
            temporary_database_url,
            "SELECT version, schema_version, content_hash, "
            "document ->> 'price_method' FROM paper_execution_models",
        )
    )
    assert model == [
        (
            "execution_model_v1",
            "paper_execution_model_v1",
            "18a4f8c7b9111e668baf011f33210df3f24ffc02ae15a448891b91898e169433",
            "close_volume_vwap_v1",
        )
    ]
    market_open, market_close = asyncio.run(_insert_fixture_calendar(temporary_database_url))
    assert market_open.isoformat() == "2026-08-27T13:30:00+00:00"
    assert market_close.isoformat() == "2026-08-27T20:00:00+00:00"
    assert asyncio.run(_mutation_is_rejected(temporary_database_url))

    command.downgrade(config, "0014_paper")
    downgraded_tables = {
        str(row[0])
        for row in asyncio.run(
            _rows(
                temporary_database_url,
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
            )
        )
    }
    assert EXECUTION_TABLES.isdisjoint(downgraded_tables)
    downgraded_fill_columns = {
        str(row[0])
        for row in asyncio.run(
            _rows(
                temporary_database_url,
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = 'paper_fills'",
            )
        )
    }
    assert FILL_COLUMNS.isdisjoint(downgraded_fill_columns)
    downgraded_order_event_columns = {
        str(row[0])
        for row in asyncio.run(
            _rows(
                temporary_database_url,
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = 'paper_order_events'",
            )
        )
    }
    assert "source_fill_id" not in downgraded_order_event_columns

    command.upgrade(config, "head")
    replayed = asyncio.run(
        _rows(
            temporary_database_url,
            "SELECT version, content_hash FROM paper_execution_models",
        )
    )
    assert replayed == [
        (
            "execution_model_v1",
            "18a4f8c7b9111e668baf011f33210df3f24ffc02ae15a448891b91898e169433",
        )
    ]
