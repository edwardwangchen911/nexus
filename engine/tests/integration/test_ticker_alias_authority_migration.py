"""PostgreSQL authority for point-in-time ticker-to-instrument identity."""

from __future__ import annotations

import asyncio
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import UUID

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from nexus.jobs.daily_pipeline import assert_daily_pipeline_schema_ready
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

_ENGINE_ROOT = Path(__file__).resolve().parents[2]
_REVISION = "0032_ticker_alias_authority"
_FIRST_INSTRUMENT = UUID("0198d500-1000-7000-8000-000000003201")
_SECOND_INSTRUMENT = UUID("0198d500-1000-7000-8000-000000003202")


def _config() -> Config:
    return Config(str(_ENGINE_ROOT / "alembic.ini"))


def test_0032_precedes_cash_flow_business_date_head() -> None:
    scripts = ScriptDirectory.from_config(_config())

    assert scripts.get_heads() == ["0033_cash_flow_business_date"]
    assert scripts.get_revision("0033_cash_flow_business_date").down_revision == _REVISION
    revision = scripts.get_revision(_REVISION)
    assert revision is not None
    assert revision.down_revision == "0031_intraday_schema_admission"


def test_0032_downgrade_fences_workers_and_jobs_before_alias_inspection() -> None:
    source = (_ENGINE_ROOT / "alembic/versions/0032_ticker_alias_authority.py").read_text()
    downgrade = source.split("def downgrade() -> None:", maxsplit=1)[1]

    advisory = downgrade.index("nexus:intraday-schema-admission:v1")
    heartbeats = downgrade.index(
        "LOCK TABLE public.worker_heartbeats IN ACCESS EXCLUSIVE MODE"
    )
    jobs = downgrade.index("LOCK TABLE public.jobs IN ACCESS EXCLUSIVE MODE")
    aliases = downgrade.index(
        "LOCK TABLE public.instrument_aliases IN ACCESS EXCLUSIVE MODE"
    )
    inspection = downgrade.index("DO $$ BEGIN")
    assert advisory < heartbeats < jobs < aliases < inspection
    assert "queue IN ('general', 'provider', 'broker')" in downgrade
    assert "'broker.simulation.dispatch'" in downgrade


def test_0032_rejects_preexisting_ambiguous_ticker_periods(
    temporary_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    command.upgrade(_config(), "0031_intraday_schema_admission")
    asyncio.run(_seed_ambiguous_symbol(temporary_database_url))

    with pytest.raises(DBAPIError, match="ambiguous ticker authority prevents upgrade") as error:
        command.upgrade(_config(), "head")

    assert error.value.orig.sqlstate == "P3D01"


def test_0032_blocks_overlapping_ticker_for_either_side_of_the_mapping(
    migrated_database_url: str,
) -> None:
    outcomes = asyncio.run(_attempt_sequential_alias_conflicts(migrated_database_url))

    assert outcomes == ("instrument_blocked", "symbol_blocked")


def test_0032_serializes_concurrent_ticker_symbol_claims(
    migrated_database_url: str,
) -> None:
    outcomes = asyncio.run(_attempt_concurrent_symbol_claims(migrated_database_url))

    assert len([outcome for outcome in outcomes if outcome == "committed"]) == 1, outcomes
    failures = [outcome for outcome in outcomes if isinstance(outcome, DBAPIError)]
    assert len(failures) == 1
    # PostgreSQL may resolve concurrent GiST exclusion claims either after the
    # winner commits (23P01) or by aborting one deadlock victim (40P01). Both
    # outcomes are fail-closed and leave exactly one authoritative mapping.
    assert failures[0].orig.sqlstate in {"23P01", "40P01"}


@pytest.mark.asyncio
async def test_daily_pipeline_schema_guard_holds_shared_downgrade_fence(
    migrated_database_url: str,
) -> None:
    engine = create_async_engine(migrated_database_url, pool_size=2, max_overflow=0)
    try:
        async with engine.connect() as worker:
            worker_transaction = await worker.begin()
            try:
                async with AsyncSession(bind=worker) as session:
                    await assert_daily_pipeline_schema_ready(session)
                async with engine.connect() as downgrade:
                    downgrade_transaction = await downgrade.begin()
                    try:
                        await downgrade.execute(text("SET LOCAL lock_timeout='50ms'"))
                        with pytest.raises(DBAPIError) as error:
                            await downgrade.execute(
                                text(
                                    "SELECT pg_catalog.pg_advisory_xact_lock("
                                    "pg_catalog.hashtextextended("
                                    "'nexus:intraday-schema-admission:v1', 0))"
                                )
                            )
                        assert error.value.orig.sqlstate == "55P03"
                    finally:
                        await downgrade_transaction.rollback()
            finally:
                await worker_transaction.rollback()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_daily_pipeline_schema_guard_rejects_noncurrent_head(
    migrated_database_url: str,
) -> None:
    engine = create_async_engine(migrated_database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "UPDATE public.alembic_version "
                    "SET version_num='0031_intraday_schema_admission'"
                )
            )
        async with AsyncSession(engine) as session, session.begin():
            with pytest.raises(RuntimeError, match="DAILY_PIPELINE_DATABASE_HEAD_MISMATCH"):
                await assert_daily_pipeline_schema_ready(session)
    finally:
        await engine.dispose()


def test_0032_downgrade_refuses_a_live_current_head_provider(
    temporary_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    command.upgrade(_config(), "head")
    asyncio.run(_seed_live_provider_heartbeat(temporary_database_url))

    with pytest.raises(DBAPIError, match="ticker alias authority prevents downgrade") as error:
        command.downgrade(_config(), "0031_intraday_schema_admission")

    assert error.value.orig.sqlstate == "P3D01"


def test_0032_downgrade_refuses_an_actionable_daily_root(
    temporary_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    command.upgrade(_config(), "head")
    asyncio.run(_seed_actionable_daily_root(temporary_database_url))

    with pytest.raises(DBAPIError, match="ticker alias authority prevents downgrade") as error:
        command.downgrade(_config(), "0031_intraday_schema_admission")

    assert error.value.orig.sqlstate == "P3D01"


def test_0032_downgrade_refuses_a_live_current_head_broker(
    temporary_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    command.upgrade(_config(), "head")
    asyncio.run(_seed_live_broker_heartbeat(temporary_database_url))

    with pytest.raises(DBAPIError, match="ticker alias authority prevents downgrade") as error:
        command.downgrade(_config(), "0031_intraday_schema_admission")

    assert error.value.orig.sqlstate == "P3D01"


def test_0032_downgrade_refuses_an_actionable_broker_dispatch(
    temporary_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    command.upgrade(_config(), "head")
    asyncio.run(_seed_actionable_broker_dispatch(temporary_database_url))

    with pytest.raises(DBAPIError, match="ticker alias authority prevents downgrade") as error:
        command.downgrade(_config(), "0031_intraday_schema_admission")

    assert error.value.orig.sqlstate == "P3D01"


@pytest.mark.asyncio
async def test_broker_schema_guard_holds_shared_downgrade_fence(
    migrated_database_url: str,
) -> None:
    from nexus.broker_worker import assert_broker_worker_schema_ready

    engine = create_async_engine(migrated_database_url, pool_size=2, max_overflow=0)
    try:
        async with engine.connect() as worker:
            worker_transaction = await worker.begin()
            try:
                async with AsyncSession(bind=worker) as session:
                    await assert_broker_worker_schema_ready(session)
                async with engine.connect() as downgrade:
                    downgrade_transaction = await downgrade.begin()
                    try:
                        await downgrade.execute(text("SET LOCAL lock_timeout='50ms'"))
                        with pytest.raises(DBAPIError) as error:
                            await downgrade.execute(
                                text(
                                    "SELECT pg_catalog.pg_advisory_xact_lock("
                                    "pg_catalog.hashtextextended("
                                    "'nexus:intraday-schema-admission:v1', 0))"
                                )
                            )
                        assert error.value.orig.sqlstate == "55P03"
                    finally:
                        await downgrade_transaction.rollback()
            finally:
                await worker_transaction.rollback()
    finally:
        await engine.dispose()


async def _seed_instruments(database_url: str) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO instruments "
                    "(id,asset_type,exchange,currency,tick_size,risk_bucket) VALUES "
                    "(:first,'equity','XNYS','USD',0.01,'equity'),"
                    "(:second,'equity','XNYS','USD',0.01,'equity')"
                ),
                {"first": _FIRST_INSTRUMENT, "second": _SECOND_INSTRUMENT},
            )
    finally:
        await engine.dispose()


async def _seed_ambiguous_symbol(database_url: str) -> None:
    await _seed_instruments(database_url)
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO instrument_aliases "
                    "(id,instrument_id,symbol,alias_type,valid_from,valid_to) VALUES "
                    "(:first_alias,:first,'AAPL','ticker','2020-01-01',NULL),"
                    "(:second_alias,:second,'aapl','ticker','2021-01-01',NULL)"
                ),
                {
                    "first_alias": UUID("0198d500-1000-7000-8000-000000003211"),
                    "second_alias": UUID("0198d500-1000-7000-8000-000000003212"),
                    "first": _FIRST_INSTRUMENT,
                    "second": _SECOND_INSTRUMENT,
                },
            )
    finally:
        await engine.dispose()


async def _attempt_sequential_alias_conflicts(database_url: str) -> tuple[str, str]:
    await _seed_instruments(database_url)
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO instrument_aliases "
                    "(id,instrument_id,symbol,alias_type,valid_from,valid_to) VALUES "
                    "(:id,:instrument,'AAPL','ticker','2020-01-01',NULL)"
                ),
                {
                    "id": UUID("0198d500-1000-7000-8000-000000003221"),
                    "instrument": _FIRST_INSTRUMENT,
                },
            )

        instrument_outcome = "unexpected"
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "INSERT INTO instrument_aliases "
                        "(id,instrument_id,symbol,alias_type,valid_from,valid_to) VALUES "
                        "(:id,:instrument,'MSFT','ticker','2021-01-01',NULL)"
                    ),
                    {
                        "id": UUID("0198d500-1000-7000-8000-000000003222"),
                        "instrument": _FIRST_INSTRUMENT,
                    },
                )
        except DBAPIError as error:
            assert error.orig.sqlstate == "23P01"
            instrument_outcome = "instrument_blocked"

        symbol_outcome = "unexpected"
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "INSERT INTO instrument_aliases "
                        "(id,instrument_id,symbol,alias_type,valid_from,valid_to) VALUES "
                        "(:id,:instrument,'aapl','ticker','2022-01-01',NULL)"
                    ),
                    {
                        "id": UUID("0198d500-1000-7000-8000-000000003223"),
                        "instrument": _SECOND_INSTRUMENT,
                    },
                )
        except DBAPIError as error:
            assert error.orig.sqlstate == "23P01"
            symbol_outcome = "symbol_blocked"
        return instrument_outcome, symbol_outcome
    finally:
        await engine.dispose()


async def _attempt_concurrent_symbol_claims(
    database_url: str,
) -> tuple[str | DBAPIError, str | DBAPIError]:
    await _seed_instruments(database_url)
    engine = create_async_engine(database_url, pool_size=2, max_overflow=0)

    async def claim(
        alias_id: UUID,
        instrument_id: UUID,
        symbol: str,
        valid_from: date,
    ) -> str:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO instrument_aliases "
                    "(id,instrument_id,symbol,alias_type,valid_from,valid_to) VALUES "
                    "(:id,:instrument,:symbol,'ticker',:valid_from,NULL)"
                ),
                {
                    "id": alias_id,
                    "instrument": instrument_id,
                    "symbol": symbol,
                    "valid_from": valid_from,
                },
            )
        return "committed"

    try:
        results = await asyncio.wait_for(
            asyncio.gather(
                claim(
                    UUID("0198d500-1000-7000-8000-000000003231"),
                    _FIRST_INSTRUMENT,
                    "AAPL",
                    date(2020, 1, 1),
                ),
                claim(
                    UUID("0198d500-1000-7000-8000-000000003232"),
                    _SECOND_INSTRUMENT,
                    "aapl",
                    date(2021, 1, 1),
                ),
                return_exceptions=True,
            ),
            timeout=5,
        )
        return results[0], results[1]
    finally:
        await engine.dispose()


async def _seed_live_provider_heartbeat(database_url: str) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            now = datetime.now(UTC)
            await connection.execute(
                text(
                    "INSERT INTO worker_heartbeats "
                    "(id,queue,worker_id,process_version,migration_head,started_at,seen_at) "
                    "VALUES (:id,'provider','ticker-authority-test','test',:head,:now,:now)"
                ),
                {
                    "id": UUID("0198d500-1000-7000-8000-000000003241"),
                    "head": _REVISION,
                    "now": now,
                },
            )
    finally:
        await engine.dispose()


async def _seed_live_broker_heartbeat(database_url: str) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            now = datetime.now(UTC)
            await connection.execute(
                text(
                    "INSERT INTO worker_heartbeats "
                    "(id,queue,worker_id,process_version,migration_head,started_at,seen_at) "
                    "VALUES (:id,'broker','ticker-broker-test','test',:head,:now,:now)"
                ),
                {
                    "id": UUID("0198d500-1000-7000-8000-000000003243"),
                    "head": _REVISION,
                    "now": now,
                },
            )
    finally:
        await engine.dispose()


async def _seed_actionable_daily_root(database_url: str) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            now = datetime.now(UTC)
            await connection.execute(
                text(
                    "INSERT INTO jobs "
                    "(id,job_key,input_hash,status,schema_version,payload,job_type,queue,"
                    "scope_kind,scope_id,effective_at,priority,retry_policy,max_attempts,"
                    "attempt_count,manual_retry_count,available_at,started_at,finished_at,"
                    "last_error_code,result_code,version) VALUES "
                    "(:id,'ticker-authority-daily-root',:hash,'queued','daily_pipeline_v1',"
                    "CAST('{}' AS jsonb),'daily.pipeline','general','global','XNYS.2026-08-28',"
                    ":now,280,'automatic_safe',3,0,0,:now,NULL,NULL,NULL,NULL,1)"
                ),
                {
                    "id": UUID("0198d500-1000-7000-8000-000000003242"),
                    "hash": "a" * 64,
                    "now": now,
                },
            )
    finally:
        await engine.dispose()


async def _seed_actionable_broker_dispatch(database_url: str) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            now = datetime.now(UTC)
            await connection.execute(
                text(
                    "INSERT INTO jobs "
                    "(id,job_key,input_hash,status,schema_version,payload,job_type,queue,"
                    "scope_kind,scope_id,effective_at,priority,retry_policy,max_attempts,"
                    "attempt_count,manual_retry_count,available_at,started_at,finished_at,"
                    "last_error_code,result_code,version) VALUES "
                    "(:id,'ticker-authority-broker-dispatch',:hash,'queued',"
                    "'broker_sim_dispatch_v1',CAST(:payload AS jsonb),"
                    "'broker.simulation.dispatch','broker','provider','simulation-fixture',"
                    ":now,300,'never',1,0,0,:now,NULL,NULL,NULL,NULL,1)"
                ),
                {
                    "id": UUID("0198d500-1000-7000-8000-000000003244"),
                    "hash": "b" * 64,
                    "payload": '{"intent_id":"0198d500-1000-7000-8000-000000003245"}',
                    "now": now,
                },
            )
    finally:
        await engine.dispose()
