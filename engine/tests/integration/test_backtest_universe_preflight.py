"""The future internal-universe preflight reads existing master data and writes nothing."""

import asyncio
import hashlib
import json
import os
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, cast
from uuid import uuid4

import exchange_calendars
import pytest
from nexus.backtest.universe import load_us_liquid_v1_definition, preflight_us_liquid_v1
from nexus.persistence.models.backtest import BacktestAuthorityPublicationMarkRecord
from nexus.persistence.models.market import (
    Instrument,
    InstrumentAlias,
    UniverseMember,
    UniverseSnapshot,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession


async def _database_clock(database_url: str) -> datetime:
    factory = session_factory(EngineSettings(database_url=database_url))
    async with factory() as session:
        cutoff = await session.scalar(select(text("clock_timestamp()")))
    assert isinstance(cutoff, datetime)
    return cutoff


async def _future_session(database_url: str) -> date:
    cutoff = await _database_clock(database_url)
    official = exchange_calendars.get_calendar("XNYS")
    sessions = official.sessions_in_range(cutoff.date(), cutoff.date() + timedelta(days=14))
    return next(
        session.date()
        for session in sessions
        if official.session_open(session).to_pydatetime() > cutoff
    )


async def _past_session(database_url: str) -> date:
    cutoff = await _database_clock(database_url)
    official = exchange_calendars.get_calendar("XNYS")
    sessions = official.sessions_in_range(cutoff.date() - timedelta(days=14), cutoff.date())
    return next(
        session.date()
        for session in reversed(sessions)
        if official.session_open(session).to_pydatetime() <= cutoff
    )


async def _seed_master(database_url: str, effective_session: date, problem: str | None) -> None:
    factory = session_factory(EngineSettings(database_url=database_url))
    definition = load_us_liquid_v1_definition(effective_session)
    database_clock = await _database_clock(database_url)
    created_at = database_clock - timedelta(days=1)
    async with factory.begin() as session:
        if problem in {"ambiguous_alias", "ambiguous_instrument_alias"}:
            # Simulate a corrupt/legacy store despite the current database exclusion guard.
            constraint = (
                "ex_instrument_aliases_ticker_symbol_period"
                if problem == "ambiguous_alias"
                else "ex_instrument_aliases_ticker_instrument_period"
            )
            await session.execute(
                text("ALTER TABLE instrument_aliases DROP CONSTRAINT " + constraint)
            )
        for index, member in enumerate(definition.members):
            missing_instrument = problem == "missing_instrument" and index == 0
            if not missing_instrument:
                instrument_created_at = (
                    database_clock + timedelta(days=1)
                    if problem == "late_instrument" and index == 0
                    else created_at
                )
                session.add(
                    Instrument(
                        id=member.instrument_id,
                        asset_type=member.asset_type,
                        exchange="XNAS",
                        currency=(
                            "EUR" if problem == "invalid_instrument" and index == 0 else "USD"
                        ),
                        tick_size=Decimal("0.01"),
                        risk_bucket=member.risk_bucket,
                        created_at=instrument_created_at,
                    )
                )
            if missing_instrument or (problem == "missing_alias" and index == 0):
                continue
            alias_instrument_id = member.instrument_id
            if problem == "mismatch_alias" and index == 0:
                alias_instrument_id = uuid4()
                session.add(
                    Instrument(
                        id=alias_instrument_id,
                        asset_type=member.asset_type,
                        exchange="XNAS",
                        currency="USD",
                        tick_size=Decimal("0.01"),
                        risk_bucket=member.risk_bucket,
                        created_at=created_at,
                    )
                )
            session.add(
                InstrumentAlias(
                    instrument_id=alias_instrument_id,
                    symbol=member.symbol,
                    alias_type="ticker",
                    valid_from=date(2020, 1, 1),
                    valid_to=None,
                    created_at=(
                        database_clock + timedelta(days=1)
                        if problem == "late_alias" and index == 0
                        else created_at
                    ),
                )
            )
            if problem == "ambiguous_alias" and index == 0:
                other_instrument_id = uuid4()
                session.add(
                    Instrument(
                        id=other_instrument_id,
                        asset_type=member.asset_type,
                        exchange="XNAS",
                        currency="USD",
                        tick_size=Decimal("0.01"),
                        risk_bucket=member.risk_bucket,
                        created_at=created_at,
                    )
                )
                session.add(
                    InstrumentAlias(
                        instrument_id=other_instrument_id,
                        symbol=member.symbol,
                        alias_type="ticker",
                        valid_from=date(2021, 1, 1),
                        valid_to=None,
                        created_at=created_at,
                    )
                )
            if problem == "ambiguous_instrument_alias" and index == 0:
                session.add(
                    InstrumentAlias(
                        instrument_id=member.instrument_id,
                        symbol=f"{member.symbol}OLD",
                        alias_type="ticker",
                        valid_from=date(2021, 1, 1),
                        valid_to=None,
                        created_at=created_at,
                    )
                )


def _canonical_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _canonical_value(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_canonical_value(item) for item in value]
    if isinstance(value, date | datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if value is None or isinstance(value, bool | int | float | str):
        return value
    return str(value)


async def _database_snapshot(database_url: str) -> dict[str, tuple[int, str]]:
    factory = session_factory(EngineSettings(database_url=database_url))
    models = (
        Instrument,
        InstrumentAlias,
        UniverseSnapshot,
        UniverseMember,
        BacktestAuthorityPublicationMarkRecord,
    )
    async with factory() as session:
        snapshot: dict[str, tuple[int, str]] = {}
        for model in models:
            table = model.__table__
            rows = (
                await session.execute(select(*table.c).order_by(*table.primary_key.columns))
            ).mappings()
            payload = [
                {key: _canonical_value(value) for key, value in sorted(row.items())} for row in rows
            ]
            digest = hashlib.sha256(
                json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
            snapshot[table.name] = (len(payload), digest)
        return snapshot


class _ClockGatedSession:
    def __init__(
        self,
        session: AsyncSession,
        cutoff_read: asyncio.Event,
        writer_committed: asyncio.Event,
    ) -> None:
        self._session = session
        self._cutoff_read = cutoff_read
        self._writer_committed = writer_committed
        self.cutoff: datetime | None = None

    async def execute(self, statement):
        return await self._session.execute(statement)

    async def scalar(self, statement):
        value = await self._session.scalar(statement)
        if "clock_timestamp" in str(statement):
            assert isinstance(value, datetime)
            self.cutoff = value
            self._cutoff_read.set()
            await self._writer_committed.wait()
        return value

    async def scalars(self, statement):
        return await self._session.scalars(statement)


async def _cli(database_url: str, effective_session: date):
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "nexus.ops.backtest_once",
        "--preflight-universe",
        "--effective-session",
        effective_session.isoformat(),
        env={**os.environ, "DATABASE_URL": database_url},
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=30)
    return process.returncode, stdout, stderr


@pytest.mark.asyncio
async def test_normal_cli_preflight_reads_exact_master_and_changes_no_database_rows(
    migrated_database_url,
) -> None:
    effective_session = await _future_session(migrated_database_url)
    await _seed_master(migrated_database_url, effective_session, None)
    before = await _database_snapshot(migrated_database_url)

    code, stdout, stderr = await _cli(migrated_database_url, effective_session)

    assert code == 0, stderr.decode()
    assert stderr == b""
    assert json.loads(stdout) == {
        "gap_categories": [],
        "member_count": 49,
        "status": "BACKTEST_UNIVERSE_PREFLIGHT_READY",
    }
    assert await _database_snapshot(migrated_database_url) == before
    assert before[UniverseSnapshot.__tablename__][0] == 0
    assert before[UniverseMember.__tablename__][0] == 0
    definition = load_us_liquid_v1_definition(effective_session)
    assert b'"AAPL"' not in stdout
    assert str(definition.members[0].instrument_id).encode() not in stdout
    previous = await _past_session(migrated_database_url)
    old_code, old_stdout, old_stderr = await _cli(migrated_database_url, previous)
    assert old_code == 1 and old_stdout == b""
    assert json.loads(old_stderr) == {
        "status": "failed",
        "code": "BACKTEST_UNIVERSE_SESSION_NOT_FUTURE",
    }
    assert await _database_snapshot(migrated_database_url) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "problem,expected",
    [
        ("missing_instrument", ("INSTRUMENT_MISSING", "TICKER_ALIAS_MISSING")),
        ("late_instrument", ("INSTRUMENT_LATE",)),
        ("invalid_instrument", ("INSTRUMENT_IDENTITY_INVALID",)),
        ("missing_alias", ("TICKER_ALIAS_MISSING",)),
        ("late_alias", ("TICKER_ALIAS_LATE",)),
        ("ambiguous_alias", ("TICKER_ALIAS_AMBIGUOUS",)),
        ("ambiguous_instrument_alias", ("TICKER_ALIAS_AMBIGUOUS",)),
        ("mismatch_alias", ("TICKER_ALIAS_MISMATCH",)),
    ],
)
async def test_master_gaps_are_classified_without_identity_output_or_writes(
    problem: str, expected: tuple[str, ...], migrated_database_url
) -> None:
    effective_session = await _future_session(migrated_database_url)
    await _seed_master(migrated_database_url, effective_session, problem)
    before = await _database_snapshot(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))

    async with factory() as session, session.begin():
        result = await preflight_us_liquid_v1(session, effective_session)
        assert await session.scalar(text("SHOW transaction_read_only")) == "on"
        assert await session.scalar(text("SHOW transaction_isolation")) == "repeatable read"

    assert result.status == "BACKTEST_UNIVERSE_PREFLIGHT_BLOCKED"
    assert result.member_count == 49
    assert result.gap_categories == expected
    assert await _database_snapshot(migrated_database_url) == before
    if problem == "missing_alias":
        code, stdout, stderr = await _cli(migrated_database_url, effective_session)
        assert code == 0 and stderr == b""
        assert json.loads(stdout) == {
            "gap_categories": ["TICKER_ALIAS_MISSING"],
            "member_count": 49,
            "status": "BACKTEST_UNIVERSE_PREFLIGHT_BLOCKED",
        }
        assert b'"AAPL"' not in stdout
        assert await _database_snapshot(migrated_database_url) == before


@pytest.mark.asyncio
async def test_preflight_does_not_mix_a_backfilled_master_commit_after_its_cutoff(
    migrated_database_url,
) -> None:
    effective_session = await _future_session(migrated_database_url)
    definition = load_us_liquid_v1_definition(effective_session)
    missing = definition.members[0]
    await _seed_master(migrated_database_url, effective_session, "missing_instrument")
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    cutoff_read = asyncio.Event()
    writer_committed = asyncio.Event()

    async with factory() as reader, reader.begin():
        gated = _ClockGatedSession(reader, cutoff_read, writer_committed)

        async def backfill_after_cutoff() -> None:
            await cutoff_read.wait()
            assert gated.cutoff is not None
            backfilled_at = gated.cutoff - timedelta(days=1)
            async with factory.begin() as writer:
                writer.add(
                    Instrument(
                        id=missing.instrument_id,
                        asset_type=missing.asset_type,
                        exchange="XNAS",
                        currency="USD",
                        tick_size=Decimal("0.01"),
                        risk_bucket=missing.risk_bucket,
                        created_at=backfilled_at,
                    )
                )
                writer.add(
                    InstrumentAlias(
                        instrument_id=missing.instrument_id,
                        symbol=missing.symbol,
                        alias_type="ticker",
                        valid_from=date(2020, 1, 1),
                        valid_to=None,
                        created_at=backfilled_at,
                    )
                )
            writer_committed.set()

        writer_task = asyncio.create_task(backfill_after_cutoff())
        result = await preflight_us_liquid_v1(cast(AsyncSession, gated), effective_session)
        await writer_task

        assert result.status == "BACKTEST_UNIVERSE_PREFLIGHT_BLOCKED"
        assert result.gap_categories == ("INSTRUMENT_MISSING", "TICKER_ALIAS_MISSING")
        assert await reader.scalar(text("SHOW transaction_read_only")) == "on"
        assert await reader.scalar(text("SHOW transaction_isolation")) == "repeatable read"
