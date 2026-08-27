import asyncio
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.market.price_bar_authority import SOURCE_POLICY_ID, finalize_price_bar_set
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import (
    DataArtifact,
    DataSnapshot,
    PriceBarRecord,
    PriceSeriesManifest,
    QualityReport,
)
from nexus.persistence.models.market import (
    Instrument,
    SectorExposure,
    UniverseMember,
    UniverseSnapshot,
)
from nexus.persistence.models.paper import RiskQueryAuthorityRecord
from nexus.risk.authority import (
    build_risk_market_snapshot,
    canonical_query_authority_hash,
    load_risk_market_snapshot,
)
from nexus.risk.canonical import canonical_forward_strategy_decision_hash
from nexus.risk.models import AuthorityQueryProof, RiskPrice, RiskSectorExposure
from nexus.strategy.forward import ForwardStrategyDecision, ForwardStrategyTarget
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

PAPER_ACCOUNT_ID = UUID("d31672c0-3922-5ebd-8fdb-cd3174266731")
PORTFOLIO_ID = UUID("ee2ff673-eaf6-5076-bbab-f4c9cedfc74e")
INSTRUMENT_ID = UUID("40000000-0000-0000-0000-000000000001")
CALENDAR_ID = UUID("40000000-0000-0000-0000-000000000002")
UNIVERSE_ID = UUID("40000000-0000-0000-0000-000000000003")
SNAPSHOT_ID = UUID("40000000-0000-0000-0000-000000000004")
QUALITY_ID = UUID("40000000-0000-0000-0000-000000000005")
STRATEGY_DEFINITION_ID = UUID("40000000-0000-0000-0000-00000000000d")
REGISTRY_EVENT_ID = UUID("40000000-0000-0000-0000-00000000000e")
ARTIFACT_ID = UUID("40000000-0000-0000-0000-000000000006")
MANIFEST_ID = UUID("40000000-0000-0000-0000-000000000007")
BAR_ID = UUID("40000000-0000-0000-0000-000000000008")
SECTOR_ID = UUID("40000000-0000-0000-0000-000000000009")
QUERY_IDS = {
    "corporate_action": UUID("40000000-0000-0000-0000-00000000000a"),
    "suspension": UUID("40000000-0000-0000-0000-00000000000b"),
    "delisting": UUID("40000000-0000-0000-0000-00000000000c"),
}
DECISION_SESSION = date(2026, 8, 21)
EXECUTION_SESSION = date(2026, 8, 24)
DECISION_TIME = datetime(2026, 8, 21, 21, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 8, 21, 20, tzinfo=UTC)


def _decision(authority_hash: str) -> ForwardStrategyDecision:
    draft = ForwardStrategyDecision(
        account_id=PAPER_ACCOUNT_ID,
        portfolio_id=PORTFOLIO_ID,
        strategy_id="baseline",
        strategy_version="baseline_v1",
        strategy_definition_id=STRATEGY_DEFINITION_ID,
        registry_event_id=REGISTRY_EVENT_ID,
        champion_generation=1,
        decision_session=DECISION_SESSION,
        decision_time=DECISION_TIME,
        execution_session=EXECUTION_SESSION,
        source_policy_id=SOURCE_POLICY_ID,
        calendar_authority_id=CALENDAR_ID,
        universe_snapshot_id=UNIVERSE_ID,
        quality_report_id=QUALITY_ID,
        portfolio_projection_hash="a" * 64,
        authority_hash=authority_hash,
        strategy_parameters_hash="b" * 64,
        decision_hash="0" * 64,
        cash_weight=Decimal("0.875"),
        reason="WEEKLY_REBALANCE",
        targets=(
            ForwardStrategyTarget(
                instrument_id=INSTRUMENT_ID,
                target_weight=Decimal("0.125"),
                exit=False,
                strategy_priority=2,
                reason="BASELINE_NEW_TARGET",
            ),
        ),
    )
    return draft.model_copy(
        update={"decision_hash": canonical_forward_strategy_decision_hash(draft)}
    )


async def _seed_authority(
    database_url: str,
    *,
    adjustment: str = "raw",
    query_kinds: tuple[str, ...] = ("corporate_action", "suspension", "delisting"),
) -> ForwardStrategyDecision:
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            session.add(
                Instrument(
                    id=INSTRUMENT_ID,
                    asset_type="common_stock",
                    exchange="NASDAQ",
                    currency="USD",
                    tick_size=Decimal("0.01"),
                    risk_bucket="technology",
                )
            )
            session.add(
                ExchangeCalendarAuthorityRecord(
                    id=CALENDAR_ID,
                    exchange="XNYS",
                    version="paper-risk-v1",
                    source="fixture",
                    range_start=date(2026, 8, 1),
                    range_end=date(2026, 8, 31),
                    query_parameters={},
                    completeness_status="complete",
                    content_hash="c" * 64,
                    published_at=AVAILABLE_AT,
                )
            )
            session.add_all(
                (
                    ExchangeCalendarSessionRecord(
                        calendar_authority_id=CALENDAR_ID,
                        session=DECISION_SESSION,
                        decision_at=DECISION_TIME,
                    ),
                    ExchangeCalendarSessionRecord(
                        calendar_authority_id=CALENDAR_ID,
                        session=EXECUTION_SESSION,
                        decision_at=datetime(2026, 8, 24, 21, tzinfo=UTC),
                    ),
                )
            )
            universe = UniverseSnapshot(
                id=UNIVERSE_ID,
                key="paper-risk",
                version="v1",
                effective_from=date(2026, 8, 1),
                source="fixture",
                content_hash="d" * 64,
                published_at=None,
            )
            session.add(universe)
            session.add(
                DataSnapshot(
                    id=SNAPSHOT_ID,
                    source="fixture",
                    provider_object_id="paper-risk-quality",
                    revision="v1",
                    observed_at=AVAILABLE_AT,
                    available_at=AVAILABLE_AT,
                    received_at=AVAILABLE_AT,
                    schema_version="v1",
                    payload={},
                )
            )
            session.add(
                DataArtifact(
                    id=ARTIFACT_ID,
                    uri="memory://paper-risk-bar",
                    sha256="e" * 64,
                    byte_size=100,
                    mime_type="application/x-parquet",
                    schema_version="v1",
                    source="fixture",
                    observed_at=AVAILABLE_AT,
                    available_at=AVAILABLE_AT,
                    received_at=AVAILABLE_AT,
                )
            )
            await session.flush()
            session.add(
                UniverseMember(
                    id=UUID(int=500),
                    universe_snapshot_id=UNIVERSE_ID,
                    instrument_id=INSTRUMENT_ID,
                    inclusion_reason="TEST",
                )
            )
            await session.flush()
            await session.execute(
                text("UPDATE universe_snapshots SET published_at = :published_at WHERE id = :id"),
                {"id": UNIVERSE_ID, "published_at": AVAILABLE_AT},
            )
            session.add(
                QualityReport(
                    id=QUALITY_ID,
                    data_snapshot_id=SNAPSHOT_ID,
                    status="PASS",
                    schema_version="v1",
                    payload={
                        "required_instrument_ids": [str(INSTRUMENT_ID)],
                        "decision_session": DECISION_SESSION.isoformat(),
                    },
                )
            )
            manifest = PriceSeriesManifest(
                id=MANIFEST_ID,
                evidence_ref_id=None,
                data_artifact_id=ARTIFACT_ID,
                instrument=str(INSTRUMENT_ID),
                source="fixture",
                feed="fixture",
                delivery_mode="fixture",
                adjustment=adjustment,
                interval="1Day",
                start_at=AVAILABLE_AT,
                end_at=AVAILABLE_AT,
                row_count=1,
            )
            session.add(manifest)
            await session.flush()
            session.add(
                PriceBarRecord(
                    id=BAR_ID,
                    price_series_manifest_id=MANIFEST_ID,
                    instrument_id=INSTRUMENT_ID,
                    session=DECISION_SESSION,
                    open=Decimal("99"),
                    high=Decimal("101"),
                    low=Decimal("98"),
                    close=Decimal("100"),
                    volume=Decimal("1000000"),
                    observed_at=AVAILABLE_AT,
                    available_at=AVAILABLE_AT,
                    received_at=AVAILABLE_AT,
                    revision="v1",
                    quality_flags=[],
                )
            )
            session.add(
                SectorExposure(
                    id=SECTOR_ID,
                    instrument_id=INSTRUMENT_ID,
                    sector_code="technology",
                    weight=Decimal(1),
                    as_of=DECISION_SESSION,
                    source_url="memory://paper-risk-sector",
                    content_hash="f" * 64,
                    synthetic=False,
                    reviewed=True,
                )
            )
            await session.flush()
            finalized = await finalize_price_bar_set(session, manifest)
            assert finalized is not None
            price_authority, _ = finalized

            proofs: list[AuthorityQueryProof] = []
            for index, kind in enumerate(query_kinds, start=1):
                content_hash = canonical_query_authority_hash(
                    source_policy_id=SOURCE_POLICY_ID,
                    universe_snapshot_id=UNIVERSE_ID,
                    decision_session=DECISION_SESSION,
                    kind=kind,
                    required_instrument_ids=(INSTRUMENT_ID,),
                    blocked_instrument_ids=(),
                    complete=True,
                    source_watermark_hash=f"{index}" * 64,
                    available_at=AVAILABLE_AT,
                )
                query_id = QUERY_IDS[kind]
                session.add(
                    RiskQueryAuthorityRecord(
                        id=query_id,
                        source_policy_id=SOURCE_POLICY_ID,
                        universe_snapshot_id=UNIVERSE_ID,
                        decision_session=DECISION_SESSION,
                        kind=kind,
                        required_instrument_ids=[INSTRUMENT_ID],
                        blocked_instrument_ids=[],
                        complete=True,
                        source_watermark_hash=f"{index}" * 64,
                        available_at=AVAILABLE_AT,
                        content_hash=content_hash,
                    )
                )
                proofs.append(
                    AuthorityQueryProof(
                        kind=kind,
                        authority_id=query_id,
                        complete=True,
                        blocked=False,
                        available_at=AVAILABLE_AT,
                        content_hash=content_hash,
                    )
                )

            provisional = _decision("0" * 64)
            if len(proofs) != 3:
                return provisional
            market = build_risk_market_snapshot(
                decision=provisional,
                required_instrument_ids=(INSTRUMENT_ID,),
                whitelisted_instrument_ids=(INSTRUMENT_ID,),
                prices=(
                    RiskPrice(
                        instrument_id=INSTRUMENT_ID,
                        authority_id=price_authority.id,
                        market="US",
                        currency="USD",
                        asset_type="common_stock",
                        close=Decimal("100"),
                        source="fixture",
                        delivery_mode="fixture",
                        available_at=AVAILABLE_AT,
                        content_hash=price_authority.content_hash,
                    ),
                ),
                sector_exposures=(
                    RiskSectorExposure(
                        instrument_id=INSTRUMENT_ID,
                        authority_id=SECTOR_ID,
                        sector="technology",
                        weight=Decimal(1),
                        content_hash="f" * 64,
                    ),
                ),
                query_proofs=tuple(proofs),
            )
            return _decision(market.authority_hash)
    finally:
        await engine.dispose()


async def _load(database_url: str, decision: ForwardStrategyDecision):
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            return await load_risk_market_snapshot(
                session,
                decision=decision,
                required_instrument_ids=(INSTRUMENT_ID,),
            )
    finally:
        await engine.dispose()


def test_pg_loader_reconstructs_the_exact_bound_authority(
    migrated_database_url: str,
) -> None:
    decision = asyncio.run(_seed_authority(migrated_database_url))
    market = asyncio.run(_load(migrated_database_url, decision))

    assert market.authority_hash == decision.authority_hash
    assert market.prices[0].source == "fixture"
    assert market.prices[0].delivery_mode == "fixture"
    assert market.whitelisted_instrument_ids == (INSTRUMENT_ID,)


@pytest.mark.parametrize(
    ("seed_kwargs", "message"),
    (
        ({"adjustment": "split_adjusted"}, "price authority is invalid"),
        (
            {"query_kinds": ("corporate_action", "suspension")},
            "query proofs are incomplete",
        ),
    ),
)
def test_pg_loader_rejects_adjusted_prices_and_incomplete_query_proofs(
    migrated_database_url: str,
    seed_kwargs: dict[str, object],
    message: str,
) -> None:
    decision = asyncio.run(_seed_authority(migrated_database_url, **seed_kwargs))
    with pytest.raises(ValueError, match=message):
        asyncio.run(_load(migrated_database_url, decision))
