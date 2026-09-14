import asyncio
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest
from nexus.ops import agent_risk_authority as entry
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.market import (
    Instrument,
    InstrumentAlias,
    UniverseMember,
    UniverseSnapshot,
)
from nexus.persistence.models.paper import (
    PAPER_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
    RiskQueryAuthorityRecord,
)
from nexus.risk.authority import canonical_query_authority_hash
from nexus.risk.query_authority_producer import (
    RiskSourceProof,
    inspect_query_authority_scope,
    persist_query_authorities,
)
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

INSTRUMENT_ID = UUID("51000000-0000-0000-0000-000000000001")
EXTRA_INSTRUMENT_ID = UUID("51000000-0000-0000-0000-000000000005")
UNIVERSE_ID = UUID("51000000-0000-0000-0000-000000000002")
SESSION = date(2026, 9, 14)
AVAILABLE_AT = datetime(2026, 9, 14, 14, 30, tzinfo=UTC)
DECISION_TIME = datetime(2026, 9, 14, 20, 30, tzinfo=UTC)
CALENDAR_ID = UUID("51000000-0000-0000-0000-000000000003")
NEWER_CALENDAR_ID = UUID("51000000-0000-0000-0000-000000000004")


def _proofs(*, halt: bool = False) -> tuple[RiskSourceProof, ...]:
    return (
        RiskSourceProof("corporate_action", False, "a" * 64, AVAILABLE_AT),
        RiskSourceProof("suspension", halt, "b" * 64, AVAILABLE_AT),
        RiskSourceProof("delisting", False, "c" * 64, AVAILABLE_AT),
    )


async def _seed(
    url: str, *, exchange: str = "XNAS", extra_member: bool = False
) -> None:
    engine = create_async_engine(url)
    try:
        async with async_sessionmaker(engine).begin() as session:
            session.add(
                Instrument(
                    id=INSTRUMENT_ID,
                    asset_type="common_stock",
                    exchange=exchange,
                    currency="USD",
                    tick_size=Decimal("0.01"),
                    risk_bucket="technology",
                    created_at=datetime(2026, 9, 1, tzinfo=UTC),
                )
            )
            session.add(
                InstrumentAlias(
                    instrument_id=INSTRUMENT_ID,
                    symbol="AAPL",
                    alias_type="ticker",
                    valid_from=date(2020, 1, 1),
                    valid_to=None,
                    created_at=datetime(2026, 9, 1, tzinfo=UTC),
                )
            )
            if extra_member:
                session.add(
                    Instrument(
                        id=EXTRA_INSTRUMENT_ID,
                        asset_type="common_stock",
                        exchange="XNAS",
                        currency="USD",
                        tick_size=Decimal("0.01"),
                        risk_bucket="technology",
                        created_at=datetime(2026, 9, 1, tzinfo=UTC),
                    )
                )
            session.add(
                UniverseSnapshot(
                    id=UNIVERSE_ID,
                    key="us_liquid",
                    version="us_liquid_v1",
                    effective_from=date(2026, 9, 1),
                    source="fosun_local_read_only",
                    content_hash="d" * 64,
                    published_at=None,
                    created_at=datetime(2026, 9, 1, tzinfo=UTC),
                )
            )
            await session.flush()
            session.add(
                ExchangeCalendarAuthorityRecord(
                    id=CALENDAR_ID,
                    exchange="XNYS",
                    version="producer-test",
                    source="fixture",
                    range_start=SESSION,
                    range_end=SESSION,
                    query_parameters={},
                    completeness_status="complete",
                    content_hash="e" * 64,
                    published_at=datetime(2026, 9, 1, tzinfo=UTC),
                    created_at=datetime(2026, 9, 1, tzinfo=UTC),
                )
            )
            await session.flush()
            session.add(
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=CALENDAR_ID,
                    session=SESSION,
                    decision_at=DECISION_TIME,
                    created_at=datetime(2026, 9, 1, tzinfo=UTC),
                )
            )
            session.add(
                UniverseMember(
                    universe_snapshot_id=UNIVERSE_ID,
                    instrument_id=INSTRUMENT_ID,
                    inclusion_reason="TEST",
                    created_at=datetime(2026, 9, 1, tzinfo=UTC),
                )
            )
            if extra_member:
                session.add(
                    UniverseMember(
                        universe_snapshot_id=UNIVERSE_ID,
                        instrument_id=EXTRA_INSTRUMENT_ID,
                        inclusion_reason="TEST_EXTRA",
                        created_at=datetime(2026, 9, 1, tzinfo=UTC),
                    )
                )
            await session.flush()
            await session.execute(
                text("UPDATE universe_snapshots SET published_at = :at WHERE id = :id"),
                {"at": datetime(2026, 9, 1, tzinfo=UTC), "id": UNIVERSE_ID},
            )
    finally:
        await engine.dispose()


async def _persist_twice(url: str):
    engine = create_async_engine(url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory.begin() as session:
            first = await persist_query_authorities(
                session,
                universe_snapshot_id=UNIVERSE_ID,
                decision_session=SESSION,
                symbol="AAPL",
                proofs=_proofs(halt=True),
            )
        async with factory.begin() as session:
            second = await persist_query_authorities(
                session,
                universe_snapshot_id=UNIVERSE_ID,
                decision_session=SESSION,
                symbol="AAPL",
                proofs=_proofs(halt=True),
            )
            count = await session.scalar(select(func.count()).select_from(RiskQueryAuthorityRecord))
        return first, second, count
    finally:
        await engine.dispose()


async def _seed_newer_overlapping_calendar(url: str) -> None:
    engine = create_async_engine(url)
    try:
        async with async_sessionmaker(engine).begin() as session:
            session.add(
                ExchangeCalendarAuthorityRecord(
                    id=NEWER_CALENDAR_ID,
                    exchange="XNYS",
                    version="producer-test-newer",
                    source="fixture",
                    range_start=SESSION,
                    range_end=SESSION,
                    query_parameters={},
                    completeness_status="complete",
                    content_hash="f" * 64,
                    published_at=datetime(2026, 9, 2, tzinfo=UTC),
                    created_at=datetime(2026, 9, 2, tzinfo=UTC),
                )
            )
            await session.flush()
            session.add(
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=NEWER_CALENDAR_ID,
                    session=SESSION,
                    decision_at=DECISION_TIME,
                    created_at=datetime(2026, 9, 2, tzinfo=UTC),
                )
            )
    finally:
        await engine.dispose()


def test_normal_entry_selects_latest_overlapping_calendar_before_window_gate(
    migrated_database_url, tmp_path
) -> None:
    asyncio.run(_seed(migrated_database_url))
    asyncio.run(_seed_newer_overlapping_calendar(migrated_database_url))
    settings = entry.AgentRiskSettings.model_validate(
        {
            "DATABASE_URL": migrated_database_url,
            "ALPACA_API_KEY_ID": "synthetic-key",
            "ALPACA_API_SECRET_KEY": "synthetic-secret",
        }
    )

    with pytest.raises(ValueError, match="AGENT_RISK_COLLECTION_WINDOW_INVALID"):
        asyncio.run(
            entry.run_once(
                settings,
                decision_session=SESSION,
                decision_time=DECISION_TIME,
                output_dir=tmp_path / "risk",
                now=DECISION_TIME - timedelta(minutes=3),
            )
        )


def test_normal_entry_accepts_canonical_us_equity_exchange_before_window_gate(
    migrated_database_url, tmp_path
) -> None:
    asyncio.run(_seed(migrated_database_url, exchange="XNYS"))
    settings = entry.AgentRiskSettings.model_validate(
        {
            "DATABASE_URL": migrated_database_url,
            "ALPACA_API_KEY_ID": "synthetic-key",
            "ALPACA_API_SECRET_KEY": "synthetic-secret",
        }
    )

    with pytest.raises(ValueError, match="AGENT_RISK_COLLECTION_WINDOW_INVALID"):
        asyncio.run(
            entry.run_once(
                settings,
                decision_session=SESSION,
                decision_time=DECISION_TIME,
                output_dir=tmp_path / "risk",
                now=DECISION_TIME - timedelta(minutes=3),
            )
        )


def test_schwab_entry_accepts_intraday_time_without_matching_calendar_decision_at(
    migrated_database_url, tmp_path
) -> None:
    asyncio.run(_seed(migrated_database_url, exchange="XNYS"))
    settings = entry.AgentRiskSettings.model_validate(
        {
            "DATABASE_URL": migrated_database_url,
            "ALPACA_API_KEY_ID": "synthetic-key",
            "ALPACA_API_SECRET_KEY": "synthetic-secret",
        }
    )
    intraday_decision = datetime(2026, 9, 14, 19, 30, tzinfo=UTC)

    with pytest.raises(ValueError, match="AGENT_RISK_COLLECTION_WINDOW_INVALID"):
        asyncio.run(
            entry.run_once(
                settings,
                decision_session=SESSION,
                decision_time=intraday_decision,
                output_dir=tmp_path / "risk",
                now=intraday_decision - timedelta(minutes=3),
                source_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
            )
        )


def test_persists_exact_three_proofs_and_replay_returns_existing_rows(
    migrated_database_url,
) -> None:
    asyncio.run(_seed(migrated_database_url))

    first, second, count = asyncio.run(_persist_twice(migrated_database_url))

    assert count == 3
    assert [row.id for row in first] == [row.id for row in second]
    assert {row.kind for row in first} == {"corporate_action", "suspension", "delisting"}
    assert next(row for row in first if row.kind == "suspension").blocked_instrument_ids == [
        INSTRUMENT_ID
    ]
    for row in first:
        assert row.source_policy_id == PAPER_DECISION_SOURCE_POLICY_ID
        assert row.required_instrument_ids == [INSTRUMENT_ID]
        assert row.complete is True
        assert row.content_hash == canonical_query_authority_hash(
            source_policy_id=PAPER_DECISION_SOURCE_POLICY_ID,
            universe_snapshot_id=UNIVERSE_ID,
            decision_session=SESSION,
            kind=row.kind,
            required_instrument_ids=[INSTRUMENT_ID],
            blocked_instrument_ids=row.blocked_instrument_ids,
            complete=True,
            source_watermark_hash=row.source_watermark_hash,
            available_at=AVAILABLE_AT,
        )


def test_persists_a_newer_complete_proof_set_in_the_same_session(
    migrated_database_url,
) -> None:
    asyncio.run(_seed(migrated_database_url))

    async def persist_versions():
        engine = create_async_engine(migrated_database_url)
        try:
            factory = async_sessionmaker(engine, expire_on_commit=False)
            async with factory.begin() as session:
                older = await persist_query_authorities(
                    session,
                    universe_snapshot_id=UNIVERSE_ID,
                    decision_session=SESSION,
                    symbol="AAPL",
                    proofs=_proofs(),
                )
            newer_at = AVAILABLE_AT + timedelta(minutes=5)
            newer_proofs = tuple(
                RiskSourceProof(kind, False, character * 64, newer_at)
                for kind, character in (
                    ("corporate_action", "d"),
                    ("suspension", "e"),
                    ("delisting", "f"),
                )
            )
            async with factory.begin() as session:
                newer = await persist_query_authorities(
                    session,
                    universe_snapshot_id=UNIVERSE_ID,
                    decision_session=SESSION,
                    symbol="AAPL",
                    proofs=newer_proofs,
                )
                count = await session.scalar(
                    select(func.count()).select_from(RiskQueryAuthorityRecord)
                )
            return older, newer, count
        finally:
            await engine.dispose()

    older, newer, count = asyncio.run(persist_versions())

    assert count == 6
    assert {row.id for row in older}.isdisjoint(row.id for row in newer)


def test_scope_selects_latest_complete_eligible_proof_set(migrated_database_url) -> None:
    asyncio.run(_seed(migrated_database_url))

    async def persist_and_inspect():
        engine = create_async_engine(migrated_database_url)
        try:
            factory = async_sessionmaker(engine, expire_on_commit=False)
            async with factory.begin() as session:
                older = await persist_query_authorities(
                    session,
                    universe_snapshot_id=UNIVERSE_ID,
                    decision_session=SESSION,
                    symbol="AAPL",
                    proofs=_proofs(),
                )
                newer_at = AVAILABLE_AT + timedelta(minutes=1)
                newer = await persist_query_authorities(
                    session,
                    universe_snapshot_id=UNIVERSE_ID,
                    decision_session=SESSION,
                    symbol="AAPL",
                    proofs=tuple(
                        RiskSourceProof(kind, False, character * 64, newer_at)
                        for kind, character in (
                            ("corporate_action", "d"),
                            ("suspension", "e"),
                            ("delisting", "f"),
                        )
                    ),
                )
            async with factory() as session:
                older_scope = await inspect_query_authority_scope(
                    session,
                    decision_session=SESSION,
                    symbol="AAPL",
                    as_of=AVAILABLE_AT + timedelta(seconds=30),
                    universe_snapshot_id=UNIVERSE_ID,
                )
                newer_scope = await inspect_query_authority_scope(
                    session,
                    decision_session=SESSION,
                    symbol="AAPL",
                    as_of=AVAILABLE_AT + timedelta(minutes=1, seconds=30),
                    universe_snapshot_id=UNIVERSE_ID,
                )
            return older, newer, older_scope, newer_scope
        finally:
            await engine.dispose()

    older, newer, older_scope, newer_scope = asyncio.run(persist_and_inspect())

    assert {row.id for row in older_scope.existing} == {row.id for row in older}
    assert {row.id for row in newer_scope.existing} == {row.id for row in newer}


def test_persists_exact_three_proofs_for_schwab_decision_policy(
    migrated_database_url,
) -> None:
    asyncio.run(_seed(migrated_database_url, extra_member=True))

    async def persist() -> tuple[RiskQueryAuthorityRecord, ...]:
        engine = create_async_engine(migrated_database_url)
        try:
            async with async_sessionmaker(engine, expire_on_commit=False).begin() as session:
                return await persist_query_authorities(
                    session,
                    universe_snapshot_id=UNIVERSE_ID,
                    decision_session=SESSION,
                    symbol="AAPL",
                    proofs=_proofs(),
                    source_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
                )
        finally:
            await engine.dispose()

    records = asyncio.run(persist())

    assert len(records) == 3
    assert {row.source_policy_id for row in records} == {
        PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID
    }


async def _conflicting_replay(url: str) -> None:
    engine = create_async_engine(url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory.begin() as session:
            await persist_query_authorities(
                session,
                universe_snapshot_id=UNIVERSE_ID,
                decision_session=SESSION,
                symbol="AAPL",
                proofs=_proofs(),
            )
        async with factory.begin() as session:
            await persist_query_authorities(
                session,
                universe_snapshot_id=UNIVERSE_ID,
                decision_session=SESSION,
                symbol="AAPL",
                proofs=_proofs(halt=True),
            )
    finally:
        await engine.dispose()


def test_rejects_a_changed_replay_instead_of_rewriting_frozen_authority(
    migrated_database_url,
) -> None:
    asyncio.run(_seed(migrated_database_url))

    with pytest.raises(ValueError, match="RISK_QUERY_AUTHORITY_CONFLICT"):
        asyncio.run(_conflicting_replay(migrated_database_url))


async def _run_entry_twice(
    url: str, output: Path, monkeypatch
) -> tuple[dict[str, object], dict[str, object], int]:
    calls = 0

    async def collect(**kwargs):
        nonlocal calls
        calls += 1
        assert kwargs["symbol"] == "AAPL"
        return (
            RiskSourceProof("corporate_action", False, "a" * 64, DECISION_TIME),
            RiskSourceProof("delisting", False, "c" * 64, DECISION_TIME),
            RiskSourceProof("suspension", False, "b" * 64, DECISION_TIME),
        )

    monkeypatch.setattr(entry, "collect_source_proofs", collect)
    settings = entry.AgentRiskSettings.model_validate(
        {
            "DATABASE_URL": url,
            "ALPACA_API_KEY_ID": "synthetic-key",
            "ALPACA_API_SECRET_KEY": "synthetic-secret",
        }
    )
    first = await entry.run_once(
        settings,
        decision_session=SESSION,
        decision_time=DECISION_TIME,
        output_dir=output,
        now=DECISION_TIME - timedelta(minutes=1),
    )
    second = await entry.run_once(
        settings,
        decision_session=SESSION,
        decision_time=DECISION_TIME,
        output_dir=output,
        now=DECISION_TIME + timedelta(minutes=1),
    )
    return first, second, calls


def test_normal_entry_persists_once_then_reuses_without_source_calls(
    migrated_database_url, tmp_path, monkeypatch
) -> None:
    asyncio.run(_seed(migrated_database_url))

    first, second, calls = asyncio.run(
        _run_entry_twice(migrated_database_url, tmp_path / "risk", monkeypatch)
    )

    assert (
        first
        == second
        == {
            "status": "verified",
            "authority_count": 3,
            "blocked_kinds": [],
            "execution_locked": True,
            "model_called": False,
        }
    )
    assert calls == 1


async def _run_with_stale_existing(url: str, output: Path) -> None:
    engine = create_async_engine(url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        stale = DECISION_TIME - timedelta(minutes=2, seconds=1)
        proofs = tuple(
            RiskSourceProof(kind, False, character * 64, stale)
            for kind, character in (
                ("corporate_action", "a"),
                ("suspension", "b"),
                ("delisting", "c"),
            )
        )
        async with factory.begin() as session:
            await persist_query_authorities(
                session,
                universe_snapshot_id=UNIVERSE_ID,
                decision_session=SESSION,
                symbol="AAPL",
                proofs=proofs,
            )
    finally:
        await engine.dispose()
    settings = entry.AgentRiskSettings.model_validate(
        {
            "DATABASE_URL": url,
            "ALPACA_API_KEY_ID": "synthetic-key",
            "ALPACA_API_SECRET_KEY": "synthetic-secret",
        }
    )
    await entry.run_once(
        settings,
        decision_session=SESSION,
        decision_time=DECISION_TIME,
        output_dir=output,
        now=DECISION_TIME + timedelta(minutes=1),
    )


def test_normal_entry_does_not_reuse_stale_halt_proof(
    migrated_database_url, tmp_path
) -> None:
    asyncio.run(_seed(migrated_database_url))

    with pytest.raises(ValueError, match="AGENT_RISK_COLLECTION_WINDOW_INVALID"):
        asyncio.run(_run_with_stale_existing(migrated_database_url, tmp_path / "risk"))


async def _failed_entry_count(url: str, output: Path, monkeypatch) -> int:
    async def fail(**kwargs):
        raise ValueError("AGENT_RISK_SOURCE_COLLECTION_FAILED")

    monkeypatch.setattr(entry, "collect_source_proofs", fail)
    settings = entry.AgentRiskSettings.model_validate(
        {
            "DATABASE_URL": url,
            "ALPACA_API_KEY_ID": "synthetic-key",
            "ALPACA_API_SECRET_KEY": "synthetic-secret",
        }
    )
    with pytest.raises(ValueError, match="AGENT_RISK_SOURCE_COLLECTION_FAILED"):
        await entry.run_once(
            settings,
            decision_session=SESSION,
            decision_time=DECISION_TIME,
            output_dir=output,
            now=DECISION_TIME - timedelta(minutes=1),
        )
    engine = create_async_engine(url)
    try:
        async with async_sessionmaker(engine)() as session:
            count = await session.scalar(select(func.count()).select_from(RiskQueryAuthorityRecord))
            return int(count or 0)
    finally:
        await engine.dispose()


def test_normal_entry_source_failure_writes_no_authority(
    migrated_database_url, tmp_path, monkeypatch
) -> None:
    asyncio.run(_seed(migrated_database_url))

    count = asyncio.run(_failed_entry_count(migrated_database_url, tmp_path / "risk", monkeypatch))

    assert count == 0
