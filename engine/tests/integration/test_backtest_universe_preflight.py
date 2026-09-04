"""The future internal-universe preflight reads existing master data and writes nothing."""

import asyncio
import json
import os
import sys
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
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
from sqlalchemy import func, select, text


def _future_session() -> date:
    today = datetime.now(UTC).date()
    official = exchange_calendars.get_calendar("XNYS")
    sessions = official.sessions_in_range(today + timedelta(days=1), today + timedelta(days=14))
    return sessions[0].date()


async def _seed_master(database_url: str, effective_session: date, problem: str | None) -> None:
    factory = session_factory(EngineSettings(database_url=database_url))
    definition = load_us_liquid_v1_definition(effective_session)
    created_at = datetime.now(UTC) - timedelta(days=1)
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
                    datetime.now(UTC) + timedelta(days=1)
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
                        datetime.now(UTC) + timedelta(days=1)
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


async def _counts(database_url: str) -> tuple[int, ...]:
    factory = session_factory(EngineSettings(database_url=database_url))
    models = (
        Instrument,
        InstrumentAlias,
        UniverseSnapshot,
        UniverseMember,
        BacktestAuthorityPublicationMarkRecord,
    )
    async with factory() as session:
        counts: list[int] = []
        for model in models:
            counts.append(int(await session.scalar(select(func.count()).select_from(model)) or 0))
        return tuple(counts)


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
    effective_session = _future_session()
    await _seed_master(migrated_database_url, effective_session, None)
    before = await _counts(migrated_database_url)

    code, stdout, stderr = await _cli(migrated_database_url, effective_session)

    assert code == 0, stderr.decode()
    assert stderr == b""
    assert json.loads(stdout) == {
        "gap_categories": [],
        "member_count": 49,
        "status": "BACKTEST_UNIVERSE_PREFLIGHT_READY",
    }
    assert await _counts(migrated_database_url) == before
    assert before[2:4] == (0, 0)
    definition = load_us_liquid_v1_definition(effective_session)
    assert b'"AAPL"' not in stdout
    assert str(definition.members[0].instrument_id).encode() not in stdout
    previous = exchange_calendars.get_calendar("XNYS").previous_session(effective_session).date()
    old_code, old_stdout, old_stderr = await _cli(migrated_database_url, previous)
    assert old_code == 1 and old_stdout == b""
    assert json.loads(old_stderr) == {
        "status": "failed",
        "code": "BACKTEST_UNIVERSE_SESSION_NOT_FUTURE",
    }
    assert await _counts(migrated_database_url) == before


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
    effective_session = _future_session()
    await _seed_master(migrated_database_url, effective_session, problem)
    before = await _counts(migrated_database_url)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))

    async with factory() as session, session.begin():
        result = await preflight_us_liquid_v1(session, effective_session)

    assert result.status == "BACKTEST_UNIVERSE_PREFLIGHT_BLOCKED"
    assert result.member_count == 49
    assert result.gap_categories == expected
    assert await _counts(migrated_database_url) == before
    if problem == "missing_alias":
        code, stdout, stderr = await _cli(migrated_database_url, effective_session)
        assert code == 0 and stderr == b""
        assert json.loads(stdout) == {
            "gap_categories": ["TICKER_ALIAS_MISSING"],
            "member_count": 49,
            "status": "BACKTEST_UNIVERSE_PREFLIGHT_BLOCKED",
        }
        assert b'"AAPL"' not in stdout
        assert await _counts(migrated_database_url) == before
