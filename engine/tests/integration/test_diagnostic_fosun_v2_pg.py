from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import UUID, uuid4

import exchange_calendars as xcals
import pytest
from helpers.diagnostic_artifacts import FixtureArtifactReader
from helpers.fosun_lineage import seed_fosun_daily_lineage
from helpers.portfolio_api import settings
from nexus.diagnostics.assembler import _adv20, load_diagnostic_source_policy
from nexus.diagnostics.series_authority import select_authoritative_series
from nexus.market.ingestion import serialize_price_bars
from nexus.market.price_bar_authority import finalize_price_bar_set
from nexus.persistence.models.data import (
    DataArtifact,
    PriceBarRecord,
    PriceSeriesManifest,
    ProviderCapability,
)
from nexus.persistence.models.market import Instrument
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.persistence.models.runtime import IntegrationStatus
from nexus.persistence.session import session_factory
from nexus.providers.base import InstrumentId, PriceBar
from sqlalchemy import update


@pytest.mark.asyncio
@pytest.mark.parametrize("exchange", ("ARCA", "NYSEARCA"))
async def test_current_diagnostic_series_uses_exact_fosun_v2_and_replays_status_change(
    migrated_database_url: str,
    exchange: str,
) -> None:
    factory = session_factory(settings(migrated_database_url))
    instrument_id = UUID("019d0000-0000-7000-8000-0000000001a1")
    capability_id = UUID("019d0000-0000-7000-8000-0000000001a2")
    status_id = UUID("019d0000-0000-7000-8000-0000000001a3")
    contract_hash = "a" * 64
    as_of = datetime.now(UTC)
    authorized_at = as_of - timedelta(minutes=1)
    observed_at = authorized_at - timedelta(minutes=3)
    available_at = authorized_at - timedelta(minutes=2)
    manifest_id = uuid4()
    raw_payload = f'{{"manifest_id":"{manifest_id}"}}'.encode()
    raw_digest = sha256(raw_payload).hexdigest()
    bar = PriceBar(
        instrument_id=InstrumentId(instrument_id),
        session=as_of.date(),
        open=Decimal("100"),
        high=Decimal("102"),
        low=Decimal("99"),
        close=Decimal("101"),
        volume=Decimal("1000"),
        observed_at=observed_at,
        available_at=available_at,
        received_at=available_at,
        revision=raw_digest,
    )
    calendar = xcals.get_calendar("XNYS")
    sessions = tuple(
        label.date()
        for label in calendar.sessions_in_range(
            (as_of - timedelta(days=60)).date().isoformat(), as_of.date().isoformat()
        )
        if calendar.session_close(label).to_pydatetime() <= observed_at
    )[-20:]
    bars = tuple(
        bar.model_copy(update={
            "session": day,
            "observed_at": datetime.combine(day, datetime.min.time(), tzinfo=UTC),
        })
        for day in sessions
    )
    assert len(bars) == 20
    payload = serialize_price_bars(bars)
    digest = sha256(payload).hexdigest()
    uri = f"memory://sha256/{digest}"
    async with factory() as session, session.begin():
        session.add_all(
            (
                Instrument(
                    id=instrument_id,
                    asset_type="common_stock",
                    exchange=exchange,
                    currency="USD",
                    tick_size=Decimal("0.01"),
                    risk_bucket="technology",
                ),
                ProviderCapability(
                    id=capability_id,
                    provider="fosun",
                    capability="READ_ONLY",
                    contract_hash=contract_hash,
                    expires_at=as_of + timedelta(hours=1),
                    schema_version="1",
                    payload={
                        "operation": "fosun_get_bars",
                        "market_scope": "US",
                        "delivery_mode": "real_time",
                    },
                    created_at=as_of - timedelta(hours=1),
                ),
                IntegrationStatus(
                    id=status_id,
                    provider="fosun",
                    status="read_only",
                    last_success_at=authorized_at,
                    version=1,
                    mode_changed_at=as_of - timedelta(hours=1),
                    capability_invalidated_at=None,
                    admitted_contract_hash=contract_hash,
                    created_at=as_of - timedelta(hours=1),
                ),
            )
        )
        artifact = DataArtifact(
            id=uuid4(),
            uri=uri,
            sha256=digest,
            byte_size=len(payload),
            mime_type="application/vnd.apache.parquet",
            schema_version="price_bars_v1",
            source="fosun",
            observed_at=bars[0].observed_at,
            available_at=available_at,
            received_at=available_at,
        )
        session.add(artifact)
        await session.flush()
        lineage = await seed_fosun_daily_lineage(
            session,
            manifest_id=manifest_id,
            provider_authorized_at=authorized_at,
            sessions=sessions,
            raw_payload=raw_payload,
            received_at=available_at,
            calendar_range_end=(as_of + timedelta(hours=2)).date(),
        )
        artifact_evidence = ResearchEvidenceRef(
            source="fosun",
            provider_object_id=f"price-series-manifest:{manifest_id}",
            revision=digest,
            available_at=available_at,
            location=uri,
            content_hash=digest,
            byte_length=len(payload),
        )
        session.add(artifact_evidence)
        await session.flush()
        manifest = PriceSeriesManifest(
            id=manifest_id,
            evidence_ref_id=artifact_evidence.id,
            data_artifact_id=artifact.id,
            instrument=str(instrument_id),
            source="fosun",
            feed="l1",
            delivery_mode="real_time",
            adjustment="raw",
            interval="1Day",
            start_at=bars[0].observed_at,
            end_at=bars[-1].observed_at,
            row_count=len(bars),
            provider_capability_id=capability_id,
            provider_status_id=status_id,
            provider_status_version=1,
            provider_contract_hash=contract_hash,
            provider_authorized_at=authorized_at,
            gateway_transport_artifact_id=lineage.raw_artifact.id,
            daily_calendar_authority_id=lineage.calendar_authority_id,
            gateway_request_start_at=lineage.request_start_at,
            gateway_request_end_at=lineage.request_end_at,
        )
        session.add(manifest)
        await session.flush()
        session.add_all(
            [
                PriceBarRecord(
                    price_series_manifest_id=manifest.id,
                    instrument_id=instrument_id,
                    session=bar.session,
                    open=bar.open,
                    high=bar.high,
                    low=bar.low,
                    close=bar.close,
                    volume=bar.volume,
                    observed_at=bar.observed_at,
                    available_at=available_at,
                    received_at=available_at,
                    revision=bar.revision,
                    quality_flags=[],
                )
                for bar in bars
            ]
        )
        await session.flush()
        assert await finalize_price_bar_set(session, manifest) is not None
        policy = await load_diagnostic_source_policy(session, as_of)
        assert policy.version == "source_policy_v2"
        first = await select_authoritative_series(
            session,
            instrument_id,
            as_of,
            FixtureArtifactReader({uri: payload}),
            policy,
        )
        await session.execute(
            update(IntegrationStatus)
            .where(IntegrationStatus.id == status_id)
            .values(
                status="disabled",
                version=2,
                mode_changed_at=as_of + timedelta(minutes=1),
            )
        )
        second = await select_authoritative_series(
            session,
            instrument_id,
            as_of,
            FixtureArtifactReader({uri: payload}),
            policy,
        )
        expired_capability_replay = await select_authoritative_series(
            session,
            instrument_id,
            as_of + timedelta(hours=2),
            FixtureArtifactReader({uri: payload}),
            policy,
        )

    assert first.selected is not None
    assert first.selected[0].source == "fosun"
    assert first.selected[0].contract_hash == contract_hash
    assert len(first.selected[0].bars) == 20
    assert first.selected[0].bars[-1].close == Decimal("101")
    assert _adv20(first.selected[0], sessions) is None
    assert second.selected == first.selected
    assert second.canonical_input == first.canonical_input
    assert expired_capability_replay.selected is not None
    assert expired_capability_replay.selected[0].source == "fosun"
    assert expired_capability_replay.selected[0].contract_hash == contract_hash
    candidate = first.canonical_input["candidates"][0]  # type: ignore[index]
    assert candidate["manifest"]["provider_contract_hash"] == contract_hash
    assert candidate["price_bar_set_authority"]["policy_version"] == (
        "agent_price_authority_v2"
    )
    object_ids = {ref.object_id for ref in first.relevant_refs}
    assert f"price-series-manifest:{manifest_id}" in object_ids
    assert f"price-bar-set:{manifest_id}" in object_ids
