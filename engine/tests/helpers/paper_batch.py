from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

from nexus.market.price_bar_authority import SOURCE_POLICY_ID, finalize_price_bar_set
from nexus.paper.projection import load_paper_projection
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
)
from nexus.risk.canonical import canonical_forward_strategy_decision_hash
from nexus.risk.models import AuthorityQueryProof, RiskPrice, RiskSectorExposure
from nexus.strategy.forward import (
    ForwardStrategyDecision,
    ForwardStrategyTarget,
    persist_forward_strategy_decision,
)
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

PAPER_ACCOUNT_ID = UUID("d31672c0-3922-5ebd-8fdb-cd3174266731")
PORTFOLIO_ID = UUID("ee2ff673-eaf6-5076-bbab-f4c9cedfc74e")
DECISION_SESSION = date(2026, 8, 26)
EXECUTION_SESSION = date(2026, 8, 27)
DECISION_TIME = datetime(2026, 8, 26, 21, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 8, 26, 20, tzinfo=UTC)
REQUEST_TIME = datetime(2026, 8, 26, 21, 0, 1, tzinfo=UTC)
CALENDAR_ID = UUID("51000000-0000-0000-0000-000000000001")
UNIVERSE_ID = UUID("51000000-0000-0000-0000-000000000002")
DATA_SNAPSHOT_ID = UUID("51000000-0000-0000-0000-000000000003")
QUALITY_REPORT_ID = UUID("51000000-0000-0000-0000-000000000004")
INSTRUMENTS = (
    (
        UUID("51000000-0000-0000-0000-000000000010"),
        Decimal("100"),
        Decimal("0.20"),
        "health_care",
    ),
    (
        UUID("51000000-0000-0000-0000-000000000011"),
        Decimal("100"),
        Decimal("0.10"),
        "technology",
    ),
    (
        UUID("51000000-0000-0000-0000-000000000012"),
        Decimal("60"),
        Decimal("0.01"),
        "utilities",
    ),
)
NO_CHANGE_INSTRUMENT = (
    UUID("51000000-0000-0000-0000-000000000013"),
    Decimal("50"),
    Decimal("0"),
    "industrials",
)


@dataclass(frozen=True)
class PaperBatchSeed:
    account_id: UUID
    portfolio_id: UUID
    decision_id: UUID
    decision_time: datetime
    request_time: datetime
    expected_outcomes: dict[UUID, str]


async def seed_paper_batch(
    database_url: str,
    *,
    omit_persisted_query_kind: str | None = None,
    include_no_change: bool = False,
) -> PaperBatchSeed:
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            session.add(
                ExchangeCalendarAuthorityRecord(
                    id=CALENDAR_ID,
                    exchange="XNYS",
                    version="paper-batch-v1",
                    source="fixture",
                    range_start=date(2026, 8, 1),
                    range_end=date(2026, 8, 31),
                    query_parameters={},
                    completeness_status="complete",
                    content_hash="a" * 64,
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
                        decision_at=datetime(2026, 8, 27, 21, tzinfo=UTC),
                    ),
                )
            )
            session.add(
                UniverseSnapshot(
                    id=UNIVERSE_ID,
                    key="paper-batch",
                    version="v1",
                    effective_from=date(2026, 8, 1),
                    source="fixture",
                    content_hash="b" * 64,
                    published_at=None,
                )
            )
            session.add(
                DataSnapshot(
                    id=DATA_SNAPSHOT_ID,
                    source="fixture",
                    provider_object_id="paper-batch-quality",
                    revision="v1",
                    observed_at=AVAILABLE_AT,
                    available_at=AVAILABLE_AT,
                    received_at=AVAILABLE_AT,
                    schema_version="v1",
                    payload={},
                )
            )
            await session.flush()

            price_inputs: list[RiskPrice] = []
            sector_inputs: list[RiskSectorExposure] = []
            instruments = (
                INSTRUMENTS + (NO_CHANGE_INSTRUMENT,)
                if include_no_change
                else INSTRUMENTS
            )
            for index, (instrument_id, close, _, sector) in enumerate(instruments, start=1):
                artifact_id = UUID(int=0x51000000000000000000000000000100 + index)
                manifest_id = UUID(int=0x51000000000000000000000000000200 + index)
                bar_id = UUID(int=0x51000000000000000000000000000300 + index)
                sector_id = UUID(int=0x51000000000000000000000000000400 + index)
                session.add(
                    Instrument(
                        id=instrument_id,
                        asset_type="common_stock",
                        exchange="NASDAQ",
                        currency="USD",
                        tick_size=Decimal("0.01"),
                        risk_bucket=sector,
                    )
                )
                session.add(
                    UniverseMember(
                        id=UUID(int=0x51000000000000000000000000000500 + index),
                        universe_snapshot_id=UNIVERSE_ID,
                        instrument_id=instrument_id,
                        inclusion_reason="PAPER_BATCH_TEST",
                    )
                )
                session.add(
                    DataArtifact(
                        id=artifact_id,
                        uri=f"memory://paper-batch/{index}",
                        sha256=f"{index}" * 64,
                        byte_size=100,
                        mime_type="application/x-parquet",
                        schema_version="v1",
                        source="fixture",
                        observed_at=AVAILABLE_AT,
                        available_at=AVAILABLE_AT,
                        received_at=AVAILABLE_AT,
                    )
                )
                manifest = PriceSeriesManifest(
                    id=manifest_id,
                    evidence_ref_id=None,
                    data_artifact_id=artifact_id,
                    instrument=str(instrument_id),
                    source="fixture",
                    feed="fixture",
                    delivery_mode="fixture",
                    adjustment="raw",
                    interval="1Day",
                    start_at=AVAILABLE_AT,
                    end_at=AVAILABLE_AT,
                    row_count=1,
                )
                session.add(manifest)
                await session.flush()
                session.add(
                    PriceBarRecord(
                        id=bar_id,
                        price_series_manifest_id=manifest_id,
                        instrument_id=instrument_id,
                        session=DECISION_SESSION,
                        open=close,
                        high=close,
                        low=close,
                        close=close,
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
                        id=sector_id,
                        instrument_id=instrument_id,
                        sector_code=sector,
                        weight=Decimal(1),
                        as_of=DECISION_SESSION,
                        source_url=f"memory://paper-batch/sector/{index}",
                        content_hash=f"{index + 3}" * 64,
                        synthetic=False,
                        reviewed=True,
                    )
                )
                await session.flush()
                finalized = await finalize_price_bar_set(session, manifest)
                assert finalized is not None
                authority, _ = finalized
                price_inputs.append(
                    RiskPrice(
                        instrument_id=instrument_id,
                        authority_id=authority.id,
                        market="US",
                        currency="USD",
                        asset_type="common_stock",
                        close=close,
                        source="fixture",
                        delivery_mode="fixture",
                        available_at=AVAILABLE_AT,
                        content_hash=authority.content_hash,
                    )
                )
                sector_inputs.append(
                    RiskSectorExposure(
                        instrument_id=instrument_id,
                        authority_id=sector_id,
                        sector=sector,
                        weight=Decimal(1),
                        content_hash=f"{index + 3}" * 64,
                    )
                )

            await session.execute(
                text("UPDATE universe_snapshots SET published_at = :at WHERE id = :id"),
                {"at": AVAILABLE_AT, "id": UNIVERSE_ID},
            )
            required_ids = tuple(
                sorted((item[0] for item in instruments), key=lambda value: value.int)
            )
            session.add(
                QualityReport(
                    id=QUALITY_REPORT_ID,
                    data_snapshot_id=DATA_SNAPSHOT_ID,
                    status="PASS",
                    schema_version="v1",
                    payload={
                        "required_instrument_ids": [str(value) for value in required_ids],
                        "decision_session": DECISION_SESSION.isoformat(),
                    },
                )
            )
            proofs: list[AuthorityQueryProof] = []
            for index, kind in enumerate(
                ("corporate_action", "suspension", "delisting"), start=1
            ):
                authority_id = UUID(int=0x51000000000000000000000000000600 + index)
                content_hash = canonical_query_authority_hash(
                    source_policy_id=SOURCE_POLICY_ID,
                    universe_snapshot_id=UNIVERSE_ID,
                    decision_session=DECISION_SESSION,
                    kind=kind,
                    required_instrument_ids=required_ids,
                    blocked_instrument_ids=(),
                    complete=True,
                    source_watermark_hash=f"{index + 6}" * 64,
                    available_at=AVAILABLE_AT,
                )
                if kind != omit_persisted_query_kind:
                    session.add(
                        RiskQueryAuthorityRecord(
                            id=authority_id,
                            source_policy_id=SOURCE_POLICY_ID,
                            universe_snapshot_id=UNIVERSE_ID,
                            decision_session=DECISION_SESSION,
                            kind=kind,
                            required_instrument_ids=list(required_ids),
                            blocked_instrument_ids=[],
                            complete=True,
                            source_watermark_hash=f"{index + 6}" * 64,
                            available_at=AVAILABLE_AT,
                            content_hash=content_hash,
                        )
                    )
                proofs.append(
                    AuthorityQueryProof(
                        kind=kind,
                        authority_id=authority_id,
                        complete=True,
                        blocked=False,
                        available_at=AVAILABLE_AT,
                        content_hash=content_hash,
                    )
                )

            projection = await load_paper_projection(
                session,
                account_id=PAPER_ACCOUNT_ID,
                as_of=DECISION_TIME,
            )
            targets = tuple(
                ForwardStrategyTarget(
                    instrument_id=instrument_id,
                    target_weight=weight,
                    exit=False,
                    strategy_priority=2,
                    reason="BASELINE_NEW_TARGET",
                )
                for instrument_id, _, weight, _ in instruments
            )
            provisional = ForwardStrategyDecision(
                account_id=PAPER_ACCOUNT_ID,
                portfolio_id=PORTFOLIO_ID,
                strategy_id="baseline",
                strategy_version="baseline_v1",
                decision_session=DECISION_SESSION,
                decision_time=DECISION_TIME,
                execution_session=EXECUTION_SESSION,
                source_policy_id=SOURCE_POLICY_ID,
                calendar_authority_id=CALENDAR_ID,
                universe_snapshot_id=UNIVERSE_ID,
                quality_report_id=QUALITY_REPORT_ID,
                portfolio_projection_hash=projection.projection_hash,
                authority_hash="0" * 64,
                strategy_parameters_hash="c" * 64,
                decision_hash="0" * 64,
                cash_weight=Decimal("0.69"),
                reason="WEEKLY_REBALANCE",
                targets=targets,
            )
            market = build_risk_market_snapshot(
                decision=provisional,
                required_instrument_ids=required_ids,
                whitelisted_instrument_ids=required_ids,
                prices=price_inputs,
                sector_exposures=sector_inputs,
                query_proofs=proofs,
            )
            decision = provisional.model_copy(update={"authority_hash": market.authority_hash})
            decision = decision.model_copy(
                update={"decision_hash": canonical_forward_strategy_decision_hash(decision)}
            )
            persisted = await persist_forward_strategy_decision(session, decision)
        return PaperBatchSeed(
            account_id=PAPER_ACCOUNT_ID,
            portfolio_id=PORTFOLIO_ID,
            decision_id=persisted.decision_id,
            decision_time=DECISION_TIME,
            request_time=REQUEST_TIME,
            expected_outcomes={
                INSTRUMENTS[0][0]: "reduced",
                INSTRUMENTS[1][0]: "approved",
                INSTRUMENTS[2][0]: "rejected",
            },
        )
    finally:
        await engine.dispose()
