import asyncio
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

import pytest
from helpers.portfolio_api import (
    INSTRUMENT_ID,
    PORTFOLIOS,
    seed_portfolio_list,
    settings,
)
from nexus.diagnostics.assembler import (
    DiagnosticEvidenceCapabilityUnavailable,
    assemble_diagnostic_evidence,
)
from nexus.diagnostics.calendar import completed_xnys_calendar
from nexus.diagnostics.repository import (
    load_frozen_snapshot,
    load_instrument_identities,
)
from nexus.diagnostics.service import diagnose_and_persist
from nexus.domain.evidence import EvidenceRef
from nexus.market.ingestion import serialize_price_bars
from nexus.market.universe import stable_instrument_id
from nexus.persistence.models.data import DataArtifact, PriceSeriesManifest
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.session import session_factory
from nexus.providers.base import PriceBar
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

SNAPSHOT_ID = UUID("0198d500-1000-7000-8000-000000000041")
SNAPSHOT_AT = datetime(2026, 8, 24, 3, 59, tzinfo=UTC)
SPY_ID = UUID(str(stable_instrument_id("SPY")))
HOLIDAY = date(2026, 7, 3)


class FixtureArtifactReader:
    def __init__(self, payloads: dict[str, bytes]) -> None:
        self.payloads = payloads

    async def __call__(self, uri: str) -> bytes:
        return self.payloads[uri]


@pytest.mark.asyncio
async def test_production_service_assembles_real_diagnostics_from_hashed_parquet(
    migrated_database_url: str,
) -> None:
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    payloads: dict[str, bytes] = {}
    async with factory() as session, session.begin():
        await _seed_spy(session)
        await _add_series(session, payloads, INSTRUMENT_ID, "asset")
        await _add_series(session, payloads, SPY_ID, "benchmark")
        snapshot = await load_frozen_snapshot(
            session, portfolio_id=PORTFOLIOS[0], snapshot_id=SNAPSHOT_ID
        )
        identities = await load_instrument_identities(session, snapshot=snapshot)
        assembly = await assemble_diagnostic_evidence(
            session,
            snapshot=snapshot,
            identities=identities,
            artifact_reader=FixtureArtifactReader(payloads),
        )

        report = await diagnose_and_persist(
            session,
            portfolio_id=PORTFOLIOS[0],
            snapshot_id=SNAPSHOT_ID,
            artifact_reader=FixtureArtifactReader(payloads),
        )
        payloads["memory://asset"] += b"tampered-after-persist"
        with pytest.raises(ValueError, match="artifact hash"):
            await diagnose_and_persist(
                session,
                portfolio_id=PORTFOLIOS[0],
                snapshot_id=SNAPSHOT_ID,
                artifact_reader=FixtureArtifactReader(payloads),
            )

        stored = await session.scalar(
            select(PortfolioDiagnosticReportRecord).where(
                PortfolioDiagnosticReportRecord.portfolio_snapshot_id == SNAPSHOT_ID
            )
        )

    assert stored is not None
    assert await _report_count_from_url(migrated_database_url) == 1
    assert len(stored.diagnostic_input_hash) == 64
    assert report.snapshot_as_of == SNAPSHOT_AT
    assert assembly.market.trading_sessions[-1] == date(2026, 8, 21)
    assert HOLIDAY not in assembly.market.trading_sessions
    assert all(day.weekday() < 5 for day in assembly.market.trading_sessions)
    assert assembly.market.evidence.trading_calendar is not None
    assert assembly.market.evidence.trading_calendar.refs[0].source == "exchange_calendars"
    assert all(item.unknown_reason != "MARKET_EVIDENCE_UNAVAILABLE" for item in report.items)
    liquidity = next(
        item for item in report.items if item.diagnostic_code.startswith("LIQUIDITY_DAYS_TO_EXIT")
    )
    assert liquidity.metric_value is not None
    assert any(
        ref.object_id.startswith("price-series-manifest:") for ref in liquidity.evidence_refs
    )


@pytest.mark.asyncio
async def test_missing_reader_or_tampered_artifact_never_consumes_report_slot(
    migrated_database_url: str,
) -> None:
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    payloads: dict[str, bytes] = {}
    async with factory() as session, session.begin():
        await _seed_spy(session)
        await _add_series(session, payloads, INSTRUMENT_ID, "asset")
        await _add_series(session, payloads, SPY_ID, "benchmark")

        with pytest.raises(DiagnosticEvidenceCapabilityUnavailable):
            await diagnose_and_persist(
                session,
                portfolio_id=PORTFOLIOS[0],
                snapshot_id=SNAPSHOT_ID,
            )
        assert await _report_count(session) == 0

        payloads["memory://asset"] += b"tampered"
        with pytest.raises(ValueError, match="artifact hash"):
            await diagnose_and_persist(
                session,
                portfolio_id=PORTFOLIOS[0],
                snapshot_id=SNAPSHOT_ID,
                artifact_reader=FixtureArtifactReader(payloads),
            )
        assert await _report_count(session) == 0


@pytest.mark.asyncio
async def test_authoritative_service_serializes_same_input_replay(
    migrated_database_url: str,
) -> None:
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    payloads: dict[str, bytes] = {}
    async with factory() as session, session.begin():
        await _seed_spy(session)
        await _add_series(session, payloads, INSTRUMENT_ID, "asset")
        await _add_series(session, payloads, SPY_ID, "benchmark")

    async def diagnose():
        async with factory() as session, session.begin():
            return await diagnose_and_persist(
                session,
                portfolio_id=PORTFOLIOS[0],
                snapshot_id=SNAPSHOT_ID,
                artifact_reader=FixtureArtifactReader(payloads),
            )

    reports = await asyncio.gather(diagnose(), diagnose())

    assert reports[0] == reports[1]
    assert await _report_count_from_url(migrated_database_url) == 1


@pytest.mark.asyncio
async def test_assembler_executes_source_policy_and_excludes_future_revision(
    migrated_database_url: str,
) -> None:
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    payloads: dict[str, bytes] = {}
    async with factory() as session, session.begin():
        await _seed_spy(session)
        await _add_series(session, payloads, INSTRUMENT_ID, "asset")
        await _add_series(session, payloads, SPY_ID, "benchmark")
        await _add_series(
            session,
            payloads,
            INSTRUMENT_ID,
            "newer-ineligible-alpaca",
            source="alpaca",
            feed="iex",
            delivery_mode="delayed",
        )
        await _add_series(
            session,
            payloads,
            INSTRUMENT_ID,
            "future-fixture-revision",
            available_at=SNAPSHOT_AT + timedelta(seconds=1),
        )
        await _add_series(
            session,
            payloads,
            INSTRUMENT_ID,
            "future-created-artifact",
            source="alpaca",
            feed="sip",
            delivery_mode="delayed",
            artifact_created_at=SNAPSHOT_AT + timedelta(seconds=1),
        )
        await _add_series(
            session,
            payloads,
            INSTRUMENT_ID,
            "future-created-manifest",
            source="alpaca",
            feed="sip",
            delivery_mode="delayed",
            manifest_created_at=SNAPSHOT_AT + timedelta(seconds=1),
        )
        snapshot = await load_frozen_snapshot(
            session, portfolio_id=PORTFOLIOS[0], snapshot_id=SNAPSHOT_ID
        )
        identities = await load_instrument_identities(session, snapshot=snapshot)

        assembly = await assemble_diagnostic_evidence(
            session,
            snapshot=snapshot,
            identities=identities,
            artifact_reader=FixtureArtifactReader(payloads),
        )

    assert assembly.market.quality == "PASS"
    selected = assembly.market.evidence.price_series[INSTRUMENT_ID].refs[0]
    assert selected.source == "fixture"
    assert selected.content_hash == sha256(payloads["memory://asset"]).hexdigest()


@pytest.mark.asyncio
async def test_hash_valid_holiday_benchmark_bar_is_blocked_with_artifact_evidence(
    migrated_database_url: str,
) -> None:
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    payloads: dict[str, bytes] = {}
    async with factory() as session, session.begin():
        await _seed_spy(session)
        await _add_series(session, payloads, INSTRUMENT_ID, "asset")
        await _add_series(
            session,
            payloads,
            SPY_ID,
            "benchmark",
            invalid_session=HOLIDAY,
        )

        report = await diagnose_and_persist(
            session,
            portfolio_id=PORTFOLIOS[0],
            snapshot_id=SNAPSHOT_ID,
            artifact_reader=FixtureArtifactReader(payloads),
        )

    assert report.optimization_need == "BLOCKED"
    assert len(report.items) == 1
    assert report.items[0].unknown_reason == "SOURCE_CONFLICT"
    assert report.items[0].metric_value is None
    assert any(
        ref.content_hash == sha256(payloads["memory://benchmark"]).hexdigest()
        for ref in report.items[0].evidence_refs
    )


async def _seed_spy(session: AsyncSession) -> None:
    session.add(
        Instrument(
            id=SPY_ID,
            asset_type="equity_etf",
            exchange="XNYS",
            currency="USD",
            tick_size=Decimal("0.01"),
            risk_bucket="us_equity",
        )
    )
    session.add(
        InstrumentAlias(
            instrument_id=SPY_ID,
            symbol="SPY",
            alias_type="ticker",
            valid_from=datetime(2020, 1, 1, tzinfo=UTC).date(),
            valid_to=None,
        )
    )
    await session.flush()


async def _add_series(
    session: AsyncSession,
    payloads: dict[str, bytes],
    instrument_id: UUID,
    name: str,
    *,
    source: str = "fixture",
    feed: str = "fixture",
    delivery_mode: str = "fixture",
    available_at: datetime | None = None,
    invalid_session: date | None = None,
    artifact_created_at: datetime | None = None,
    manifest_created_at: datetime | None = None,
) -> None:
    bars = _bars(
        instrument_id,
        available_at=available_at,
        revision=f"{name}-r1",
        invalid_session=invalid_session,
    )
    payload = serialize_price_bars(bars)
    uri = f"memory://{name}"
    payloads[uri] = payload
    artifact = DataArtifact(
        uri=uri,
        sha256=sha256(payload).hexdigest(),
        byte_size=len(payload),
        mime_type="application/vnd.apache.parquet",
        schema_version="price_bars_v1",
        source=source,
        observed_at=SNAPSHOT_AT - timedelta(minutes=2),
        available_at=max(bar.available_at for bar in bars),
        received_at=max(bar.received_at for bar in bars if bar.received_at is not None),
        created_at=artifact_created_at,
    )
    session.add(artifact)
    await session.flush()
    session.add(
        PriceSeriesManifest(
            data_artifact_id=artifact.id,
            instrument=str(instrument_id),
            source=source,
            feed=feed,
            delivery_mode=delivery_mode,
            adjustment="all",
            start_at=bars[0].observed_at,
            end_at=bars[-1].observed_at,
            row_count=len(bars),
            created_at=manifest_created_at,
        )
    )
    await session.flush()


def _bars(
    instrument_id: UUID,
    *,
    available_at: datetime | None = None,
    revision: str = "fixture-r1",
    invalid_session: date | None = None,
) -> tuple[PriceBar, ...]:
    sessions = _published_sessions(253)
    bars = tuple(
        PriceBar(
            instrument_id=instrument_id,
            session=published_session,
            open=Decimal("100") + Decimal(index % 5),
            high=Decimal("101") + Decimal(index % 5),
            low=Decimal("99") + Decimal(index % 5),
            close=Decimal("100") + Decimal(index % 5),
            volume=Decimal("1000") + Decimal(index),
            observed_at=SNAPSHOT_AT - timedelta(minutes=2),
            available_at=available_at or SNAPSHOT_AT - timedelta(minutes=1),
            received_at=available_at or SNAPSHOT_AT - timedelta(minutes=1),
            revision=revision,
        )
        for index, published_session in enumerate(sessions)
    )
    if invalid_session is None:
        return bars
    invalid = bars[-1].model_copy(
        update={"session": invalid_session, "revision": f"{revision}-invalid-session"}
    )
    return tuple(sorted((*bars, invalid), key=lambda bar: bar.session))


def _published_sessions(count: int) -> tuple[date, ...]:
    policy_ref = EvidenceRef(
        source="nexus_db",
        object_id="source-policy:fixture:v1",
        available_at=SNAPSHOT_AT - timedelta(days=365),
        location="db://source-policies/fixture",
        content_hash="a" * 64,
    )
    return completed_xnys_calendar(SNAPSHOT_AT, policy_ref, count=count).sessions


async def _report_count(session: AsyncSession) -> int:
    return int(
        await session.scalar(select(func.count()).select_from(PortfolioDiagnosticReportRecord))
    )


async def _report_count_from_url(database_url: str) -> int:
    factory = session_factory(settings(database_url))
    async with factory() as session:
        return await _report_count(session)
