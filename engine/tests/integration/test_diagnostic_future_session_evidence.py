from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

import pytest
from helpers.diagnostic_artifacts import (
    FixtureArtifactReader,
    fixture_xnys_calendar_adoption,
    publish_diagnostic_v2_after,
)
from helpers.portfolio_api import INSTRUMENT_ID, PORTFOLIOS, seed_portfolio_list, settings
from nexus.diagnostics.calendar import completed_xnys_calendar
from nexus.diagnostics.service import diagnose_and_persist
from nexus.market.ingestion import serialize_price_bars
from nexus.market.universe import stable_instrument_id
from nexus.persistence.models.data import DataArtifact, PriceSeriesManifest
from nexus.persistence.models.diagnostics import (
    PortfolioDiagnosticEvidenceRefRecord,
    PortfolioDiagnosticItemRecord,
    PortfolioDiagnosticReportRecord,
)
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.session import session_factory
from nexus.providers.base import PriceBar
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

SNAPSHOT_ID = UUID("0198d500-1000-7000-8000-000000000041")
SNAPSHOT_AT = datetime(2026, 8, 24, 3, 59, tzinfo=UTC)
SPY_ID = UUID(str(stable_instrument_id("SPY")))
FUTURE_SESSION = date(2026, 8, 25)


@pytest.mark.asyncio
async def test_future_position_session_persists_selected_artifact_conflict_evidence(
    migrated_database_url: str,
) -> None:
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    payloads: dict[str, bytes] = {}
    async with factory() as session, session.begin():
        await _seed_spy(session)
        manifest_id, artifact_id, digest = await _add_series(
            session,
            payloads,
            INSTRUMENT_ID,
            "future-position",
            future_session=FUTURE_SESSION,
        )
        await _add_series(session, payloads, SPY_ID, "benchmark")

        report = await diagnose_and_persist(
            session,
            portfolio_id=PORTFOLIOS[0],
            snapshot_id=SNAPSHOT_ID,
            artifact_reader=FixtureArtifactReader(payloads),
        )
        stored_item = await session.scalar(
            select(PortfolioDiagnosticItemRecord)
            .join(
                PortfolioDiagnosticReportRecord,
                PortfolioDiagnosticReportRecord.id == PortfolioDiagnosticItemRecord.report_id,
            )
            .where(
                PortfolioDiagnosticReportRecord.portfolio_snapshot_id == SNAPSHOT_ID,
                PortfolioDiagnosticItemRecord.diagnostic_code == "SNAPSHOT_ADMISSION",
            )
        )
        assert stored_item is not None
        stored_refs = tuple(
            await session.scalars(
                select(PortfolioDiagnosticEvidenceRefRecord).where(
                    PortfolioDiagnosticEvidenceRefRecord.item_id == stored_item.id
                )
            )
        )

    expected_ref = (
        f"price-series-manifest:{manifest_id}",
        f"db://data-artifacts/{artifact_id}",
        digest,
    )
    memory_refs = {
        (ref.object_id, ref.location, ref.content_hash) for ref in report.items[0].evidence_refs
    }
    persisted_refs = {
        (ref.object_id, ref.location, ref.content_hash) for ref in stored_refs
    }
    object_ids = {ref.object_id for ref in report.items[0].evidence_refs}

    assert report.optimization_need == "BLOCKED"
    assert len(report.items) == 1
    assert report.items[0].unknown_reason == "SOURCE_CONFLICT"
    assert report.items[0].metric_value is None
    assert expected_ref in memory_refs
    assert expected_ref in persisted_refs
    assert persisted_refs == memory_refs
    assert any(value.startswith(f"instrument:{INSTRUMENT_ID}") for value in object_ids)
    assert any(value.startswith("instrument-alias:") for value in object_ids)
    assert any(value.startswith("source-policy:") for value in object_ids)
    assert any(value.startswith("calendar:XNYS:") for value in object_ids)
    assert any(value.startswith("calendar-adoption:") for value in object_ids)
    assert report.items[0].evidence_refs
    assert stored_refs


async def _seed_spy(session: AsyncSession) -> None:
    await publish_diagnostic_v2_after(session, SNAPSHOT_AT)
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
            valid_from=date(2020, 1, 1),
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
    future_session: date | None = None,
) -> tuple[UUID, UUID, str]:
    bars = _bars(instrument_id, name, future_session=future_session)
    payload = serialize_price_bars(bars)
    digest = sha256(payload).hexdigest()
    uri = f"memory://{name}"
    payloads[uri] = payload
    artifact = DataArtifact(
        uri=uri,
        sha256=digest,
        byte_size=len(payload),
        mime_type="application/vnd.apache.parquet",
        schema_version="price_bars_v1",
        source="fixture",
        observed_at=SNAPSHOT_AT - timedelta(minutes=2),
        available_at=SNAPSHOT_AT - timedelta(minutes=1),
        received_at=SNAPSHOT_AT - timedelta(minutes=1),
    )
    session.add(artifact)
    await session.flush()
    manifest = PriceSeriesManifest(
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
    session.add(manifest)
    await session.flush()
    return manifest.id, artifact.id, digest


def _bars(
    instrument_id: UUID,
    revision: str,
    *,
    future_session: date | None,
) -> tuple[PriceBar, ...]:
    sessions = completed_xnys_calendar(
        SNAPSHOT_AT, fixture_xnys_calendar_adoption()
    ).sessions
    if future_session is not None:
        sessions = (*sessions, future_session)
    return tuple(
        PriceBar(
            instrument_id=instrument_id,
            session=published_session,
            open=Decimal("100"),
            high=Decimal("101"),
            low=Decimal("99"),
            close=Decimal("100") + Decimal(index % 5),
            volume=Decimal("1000") + Decimal(index),
            observed_at=SNAPSHOT_AT - timedelta(minutes=2),
            available_at=SNAPSHOT_AT - timedelta(minutes=1),
            received_at=SNAPSHOT_AT - timedelta(minutes=1),
            revision=f"{revision}-r1",
        )
        for index, published_session in enumerate(sessions)
    )
