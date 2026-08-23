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
from nexus.diagnostics.repository import (
    load_frozen_snapshot,
    load_instrument_identities,
)
from nexus.diagnostics.service import diagnose_and_persist
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

        stored = await session.scalar(
            select(PortfolioDiagnosticReportRecord).where(
                PortfolioDiagnosticReportRecord.portfolio_snapshot_id == SNAPSHOT_ID
            )
        )

    assert stored is not None
    assert len(stored.diagnostic_input_hash) == 64
    assert report.snapshot_as_of == SNAPSHOT_AT
    assert HOLIDAY not in assembly.market.trading_sessions
    assert all(day.weekday() < 5 for day in assembly.market.trading_sessions)
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
) -> None:
    bars = _bars(instrument_id)
    payload = serialize_price_bars(bars)
    uri = f"memory://{name}"
    payloads[uri] = payload
    artifact = DataArtifact(
        uri=uri,
        sha256=sha256(payload).hexdigest(),
        byte_size=len(payload),
        mime_type="application/vnd.apache.parquet",
        schema_version="price_bars_v1",
        source="fixture",
        observed_at=SNAPSHOT_AT - timedelta(minutes=2),
        available_at=max(bar.available_at for bar in bars),
        received_at=max(bar.received_at for bar in bars if bar.received_at is not None),
    )
    session.add(artifact)
    await session.flush()
    session.add(
        PriceSeriesManifest(
            data_artifact_id=artifact.id,
            instrument=str(instrument_id),
            source="fixture",
            feed="fixture",
            delivery_mode="fixture",
            adjustment="all",
            start_at=bars[0].observed_at,
            end_at=bars[-1].observed_at,
            row_count=len(bars),
        )
    )
    await session.flush()


def _bars(instrument_id: UUID) -> tuple[PriceBar, ...]:
    sessions = _published_sessions(253)
    return tuple(
        PriceBar(
            instrument_id=instrument_id,
            session=published_session,
            open=Decimal("100") + Decimal(index % 5),
            high=Decimal("101") + Decimal(index % 5),
            low=Decimal("99") + Decimal(index % 5),
            close=Decimal("100") + Decimal(index % 5),
            volume=Decimal("1000") + Decimal(index),
            observed_at=SNAPSHOT_AT - timedelta(minutes=2),
            available_at=SNAPSHOT_AT - timedelta(minutes=1),
            received_at=SNAPSHOT_AT - timedelta(minutes=1),
            revision="fixture-r1",
        )
        for index, published_session in enumerate(sessions)
    )


def _published_sessions(count: int) -> tuple[date, ...]:
    result: list[date] = []
    candidate = SNAPSHOT_AT.date()
    while len(result) < count:
        if candidate.weekday() < 5 and candidate != HOLIDAY:
            result.append(candidate)
        candidate -= timedelta(days=1)
    return tuple(reversed(result))


async def _report_count(session: AsyncSession) -> int:
    return int(
        await session.scalar(select(func.count()).select_from(PortfolioDiagnosticReportRecord))
    )
