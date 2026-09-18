"""Exact-set assembly for deterministic paper risk market authority."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Literal, cast
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, field_validator
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.market.price_bar_authority import (
    HASH_VERSION,
    HASH_VERSION_V2,
    POLICY_VERSION,
    POLICY_VERSION_V2,
    SOURCE_POLICY_ID,
    SOURCE_POLICY_V2_ID,
    prepare_price_bar_set,
)
from nexus.market.scope import US_EQUITY_EXCHANGES
from nexus.paper.agent_cycle_models import CandidateManifestEntry, candidate_manifest_hash
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import (
    DataSnapshot,
    PriceBarRecord,
    PriceBarSetAuthority,
    PriceSeriesManifest,
    ProviderCapability,
    QualityReport,
    SourcePolicy,
)
from nexus.persistence.models.market import (
    Instrument,
    SectorExposure,
    UniverseMember,
    UniverseSnapshot,
)
from nexus.persistence.models.paper import (
    PAPER_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
    AgentPaperCycleRecord,
    RiskQueryAuthorityRecord,
)
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.quality.source_policy import (
    SOURCE_POLICY_KEY,
    SOURCE_POLICY_PAYLOAD,
    SOURCE_POLICY_V2_PAYLOAD,
    SOURCE_POLICY_V2_VERSION,
    SOURCE_POLICY_VERSION,
)
from nexus.risk.canonical import canonical_risk_market_authority_hash
from nexus.risk.models import (
    AuthorityQueryProof,
    PaperDecisionAuthority,
    RiskMarketSnapshot,
    RiskPrice,
    RiskSectorExposure,
)

_QUERY_KINDS = frozenset(("corporate_action", "suspension", "delisting"))


class RiskAuthorityInput(BaseModel):
    """Frozen identity and cutoff for first risk evidence preparation, without a decision."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    decision_session: date
    execution_session: date
    decision_time: datetime
    source_policy_id: UUID
    calendar_authority_id: UUID
    universe_snapshot_id: UUID
    quality_report_id: UUID | None

    @field_validator("decision_time")
    @classmethod
    def _utc_cutoff(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
            raise ValueError("decision time must use UTC")
        return value


def _valid_source_policy(policy: SourcePolicy) -> bool:
    if policy.id == PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID:
        return (
            policy.key == "paper_decision_price"
            and policy.version == "paper_schwab_l1_decision_v1"
            and policy.schema_version == "1"
            and policy.payload.get("market_context_source") == "schwab"
            and policy.payload.get("fallback") == "disabled"
            and policy.payload.get("content_hash")
            == "fcbb81143639771e8c9228d768707cb11b720a7e69fcd0598aef2af958dba885"
        )
    if policy.id == PAPER_DECISION_SOURCE_POLICY_ID:
        return (
            policy.key == "paper_decision_price"
            and policy.version == "paper_decision_price_policy_v1"
            and policy.schema_version == "1"
            and policy.payload.get("selected_source") == "alpaca"
            and policy.payload.get("fallback") == "disabled"
            and policy.payload.get("content_hash")
            == "70737603eb58331d60668bb6a039e4910d856a2c992e972fc8af0adcf05db579"
        )
    if policy.id == SOURCE_POLICY_V2_ID:
        return (
            policy.key == SOURCE_POLICY_KEY
            and policy.version == SOURCE_POLICY_V2_VERSION
            and policy.schema_version == "2"
            and policy.payload == SOURCE_POLICY_V2_PAYLOAD
        )
    if policy.id == SOURCE_POLICY_ID:
        return (
            policy.key == SOURCE_POLICY_KEY
            and policy.version == SOURCE_POLICY_VERSION
            and policy.schema_version == "1"
            and policy.payload == SOURCE_POLICY_PAYLOAD
        )
    return False


def _valid_price_protocol(
    authority: PriceBarSetAuthority,
    manifest: PriceSeriesManifest,
    requested_source_policy_id: UUID,
) -> bool:
    if requested_source_policy_id == PAPER_DECISION_SOURCE_POLICY_ID:
        return (
            authority.source_policy_id == PAPER_DECISION_SOURCE_POLICY_ID
            and authority.hash_version == HASH_VERSION
            and authority.policy_version == POLICY_VERSION
            and authority.source == manifest.source == "alpaca"
            and manifest.feed == "sip"
            and manifest.delivery_mode == "delayed"
            and manifest.adjustment == "all"
            and manifest.interval == "1Day"
        )
    if authority.source_policy_id == SOURCE_POLICY_V2_ID:
        return (
            authority.hash_version == HASH_VERSION_V2
            and authority.policy_version == POLICY_VERSION_V2
            and authority.source == manifest.source == "fosun"
            and manifest.feed == "l1"
            and manifest.delivery_mode in {"real_time", "delayed"}
            and manifest.adjustment == "raw"
            and manifest.interval == "1Day"
            and all(
                value is not None
                for value in (
                    manifest.provider_capability_id,
                    manifest.provider_status_id,
                    manifest.provider_status_version,
                    manifest.provider_contract_hash,
                    manifest.provider_authorized_at,
                )
            )
        )
    if authority.source_policy_id == SOURCE_POLICY_ID:
        legacy_source = (
            manifest.source == "alpaca"
            and manifest.feed == "sip"
            and manifest.delivery_mode == "delayed"
        ) or (
            manifest.source == "fixture"
            and bool(manifest.feed)
            and manifest.delivery_mode == "fixture"
        )
        return (
            authority.hash_version == HASH_VERSION
            and authority.policy_version == POLICY_VERSION
            and authority.source == manifest.source
            and legacy_source
            and manifest.adjustment == "raw"
            and manifest.interval == "1Day"
        )
    return False


def _valid_v2_provider_authority(
    manifest: PriceSeriesManifest,
    capability: ProviderCapability | None,
    decision_time: datetime,
) -> bool:
    if manifest.source != "fosun":
        return capability is None
    authorized_at = manifest.provider_authorized_at
    payload = {} if capability is None else capability.payload
    return (
        capability is not None
        and manifest.provider_capability_id == capability.id
        and manifest.provider_status_id is not None
        and manifest.provider_status_version is not None
        and manifest.provider_status_version > 0
        and manifest.provider_contract_hash == capability.contract_hash
        and authorized_at is not None
        and capability.schema_version == "1"
        and capability.provider == "fosun"
        and capability.capability == "READ_ONLY"
        and payload.get("operation") == "fosun_get_bars"
        and payload.get("market_scope") == "US"
        and payload.get("delivery_mode") == manifest.delivery_mode
        and capability.created_at <= authorized_at < capability.expires_at
        and authorized_at <= decision_time
    )


def _valid_price_evidence(
    authority: PriceBarSetAuthority,
    manifest: PriceSeriesManifest,
    evidence: ResearchEvidenceRef,
) -> bool:
    return (
        authority.source == manifest.source == evidence.source
        and authority.provider_object_id
        == evidence.provider_object_id
        == f"price-bar-set:{manifest.id}"
        and authority.revision == evidence.revision == authority.content_hash
        and authority.content_hash == evidence.content_hash
        and authority.byte_length == evidence.byte_length
        and authority.available_at == evidence.available_at
    )


def canonical_query_authority_hash(
    *,
    source_policy_id: UUID,
    universe_snapshot_id: UUID,
    decision_session: date,
    kind: str,
    required_instrument_ids: Sequence[UUID],
    blocked_instrument_ids: Sequence[UUID],
    complete: bool,
    source_watermark_hash: str,
    available_at: datetime,
) -> str:
    """Hash the exact no-event/event query scope independently of row identity."""
    if (
        available_at.tzinfo is None
        or available_at.utcoffset() is None
        or available_at.utcoffset() != UTC.utcoffset(available_at)
    ):
        raise ValueError("query authority available_at must use UTC")
    payload = {
        "schema": "risk_query_authority_sha256_v1",
        "source_policy_id": str(source_policy_id).lower(),
        "universe_snapshot_id": str(universe_snapshot_id).lower(),
        "decision_session": decision_session.isoformat(),
        "kind": kind,
        "required_instrument_ids": sorted(str(value).lower() for value in required_instrument_ids),
        "blocked_instrument_ids": sorted(str(value).lower() for value in blocked_instrument_ids),
        "complete": complete,
        "source_watermark_hash": source_watermark_hash,
        "available_at": available_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def build_risk_market_snapshot(
    *,
    decision: PaperDecisionAuthority | RiskAuthorityInput,
    required_instrument_ids: Sequence[UUID],
    whitelisted_instrument_ids: Sequence[UUID],
    prices: Sequence[RiskPrice],
    sector_exposures: Sequence[RiskSectorExposure],
    query_proofs: Sequence[AuthorityQueryProof],
) -> RiskMarketSnapshot:
    """Validate one complete point-in-time authority set and compute its digest."""
    draft = RiskMarketSnapshot(
        decision_session=decision.decision_session,
        execution_session=decision.execution_session,
        decision_time=decision.decision_time,
        source_policy_id=decision.source_policy_id,
        calendar_authority_id=decision.calendar_authority_id,
        universe_snapshot_id=decision.universe_snapshot_id,
        quality_report_id=decision.quality_report_id,
        required_instrument_ids=tuple(sorted(required_instrument_ids, key=lambda value: value.int)),
        whitelisted_instrument_ids=tuple(
            sorted(whitelisted_instrument_ids, key=lambda value: value.int)
        ),
        prices=tuple(sorted(prices, key=lambda item: item.instrument_id.int)),
        sector_exposures=tuple(
            sorted(
                sector_exposures,
                key=lambda item: (
                    item.instrument_id.int,
                    item.sector,
                    item.authority_id.int,
                ),
            )
        ),
        query_proofs=tuple(
            sorted(
                query_proofs,
                key=lambda item: (
                    tuple(value.int for value in item.required_instrument_ids or ()),
                    item.kind,
                ),
            )
        ),
        authority_hash="0" * 64,
    )
    return draft.model_copy(update={"authority_hash": canonical_risk_market_authority_hash(draft)})


def require_decision_authority_match(
    decision: PaperDecisionAuthority,
    market: RiskMarketSnapshot,
) -> None:
    """Bind a reloaded authority set to the immutable forward decision."""
    recomputed = canonical_risk_market_authority_hash(market)
    if recomputed != market.authority_hash or decision.authority_hash != market.authority_hash:
        raise ValueError("risk authority hash does not match forward decision")


async def _validate_root_authorities(
    session: AsyncSession,
    decision: PaperDecisionAuthority | RiskAuthorityInput,
    required: set[UUID],
) -> tuple[UUID, ...]:
    agent_paper = decision.source_policy_id in {
        PAPER_DECISION_SOURCE_POLICY_ID,
        PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
    }
    source_policy = await session.get(SourcePolicy, decision.source_policy_id)
    calendar = await session.get(ExchangeCalendarAuthorityRecord, decision.calendar_authority_id)
    universe = await session.get(UniverseSnapshot, decision.universe_snapshot_id)
    quality = (
        None
        if decision.quality_report_id is None
        else await session.get(QualityReport, decision.quality_report_id)
    )
    if (
        source_policy is None
        or not _valid_source_policy(source_policy)
        or calendar is None
        or universe is None
        or agent_paper != (decision.quality_report_id is None)
        or (not agent_paper and quality is None)
    ):
        raise ValueError("risk authority roots are incomplete")
    quality_snapshot = (
        None if quality is None else await session.get(DataSnapshot, quality.data_snapshot_id)
    )
    if not agent_paper and (
        quality_snapshot is None or quality is None or quality.status != "PASS"
    ):
        raise ValueError("risk quality report is incomplete")
    available_times: tuple[datetime | None, ...] = (
        source_policy.created_at,
        calendar.published_at,
        universe.published_at,
    )
    if quality is not None and quality_snapshot is not None:
        available_times += (quality.created_at, quality_snapshot.available_at)
    if any(
        available_at is None or available_at > decision.decision_time
        for available_at in available_times
    ):
        raise ValueError("risk authority root was available after decision time")

    sessions = tuple(
        (
            await session.scalars(
                select(ExchangeCalendarSessionRecord)
                .where(
                    ExchangeCalendarSessionRecord.calendar_authority_id
                    == decision.calendar_authority_id
                )
                .order_by(ExchangeCalendarSessionRecord.session)
            )
        ).all()
    )
    decision_indexes = [
        index for index, row in enumerate(sessions) if row.session == decision.decision_session
    ]
    if len(decision_indexes) != 1:
        raise ValueError("risk calendar authority is incomplete")
    index = decision_indexes[0]
    if decision.source_policy_id == PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID:
        local_time = decision.decision_time.astimezone(ZoneInfo("America/New_York"))
        if (
            decision.execution_session != decision.decision_session
            or local_time.date() != decision.decision_session
            or not 7 <= local_time.hour < 16
        ):
            raise ValueError("risk execution session is not the decision session")
    elif (
        sessions[index].decision_at != decision.decision_time
        or index + 1 >= len(sessions)
        or sessions[index + 1].session != decision.execution_session
    ):
        raise ValueError("risk execution session is not the next calendar session")

    whitelist = tuple(
        (
            await session.scalars(
                select(UniverseMember.instrument_id).where(
                    UniverseMember.universe_snapshot_id == decision.universe_snapshot_id,
                    UniverseMember.instrument_id.in_(required),
                )
            )
        ).all()
    )
    if set(whitelist) != required or len(whitelist) != len(required):
        raise ValueError("risk universe does not exactly cover required instruments")
    if quality is not None:
        expected_quality_ids = sorted(str(value).lower() for value in required)
        if (
            quality.payload.get("required_instrument_ids") != expected_quality_ids
            or quality.payload.get("decision_session") != decision.decision_session.isoformat()
        ):
            raise ValueError("risk quality report does not cover the exact authority set")
    return tuple(sorted(whitelist, key=lambda value: value.int))


async def _load_prices(
    session: AsyncSession,
    decision: PaperDecisionAuthority | RiskAuthorityInput,
    required: set[UUID],
) -> tuple[RiskPrice, ...]:
    price_authority_policy_id = decision.source_policy_id
    query = (
        select(
            PriceBarSetAuthority,
            PriceSeriesManifest,
            PriceBarRecord,
            Instrument,
            ResearchEvidenceRef,
            ProviderCapability,
        )
        .join(
            PriceSeriesManifest,
            PriceSeriesManifest.id == PriceBarSetAuthority.price_series_manifest_id,
        )
        .join(
            PriceBarRecord,
            PriceBarRecord.price_series_manifest_id == PriceSeriesManifest.id,
        )
        .join(Instrument, Instrument.id == PriceBarRecord.instrument_id)
        .join(
            ResearchEvidenceRef,
            ResearchEvidenceRef.id == PriceBarSetAuthority.evidence_ref_id,
        )
        .outerjoin(
            ProviderCapability,
            ProviderCapability.id == PriceSeriesManifest.provider_capability_id,
        )
        .where(
            PriceBarSetAuthority.source_policy_id == price_authority_policy_id,
            PriceBarRecord.instrument_id.in_(required),
            PriceBarRecord.session == decision.decision_session,
        )
    )
    if decision.source_policy_id == PAPER_DECISION_SOURCE_POLICY_ID:
        query = query.where(
            PriceSeriesManifest.source == "alpaca",
            PriceSeriesManifest.feed == "sip",
            PriceSeriesManifest.delivery_mode == "delayed",
            PriceSeriesManifest.adjustment == "all",
            PriceSeriesManifest.interval == "1Day",
        )
    rows = (await session.execute(query)).all()
    if len(rows) != len(required):
        raise ValueError("risk prices do not exactly cover required instruments")
    prices: list[RiskPrice] = []
    observed_ids: set[UUID] = set()
    for authority, manifest, bar, instrument, evidence, capability in rows:
        if bar.instrument_id in observed_ids:
            raise ValueError("risk prices contain duplicate instruments")
        observed_ids.add(bar.instrument_id)
        prepared = await prepare_price_bar_set(
            session,
            manifest.id,
            as_of=decision.decision_time,
            source_policy_id=(
                PAPER_DECISION_SOURCE_POLICY_ID
                if decision.source_policy_id == PAPER_DECISION_SOURCE_POLICY_ID
                else None
            ),
        )
        if (
            prepared is None
            or prepared.content_hash != authority.content_hash
            or prepared.byte_length != authority.byte_length
            or prepared.bar_count != authority.bar_count
            or prepared.first_session != authority.first_session
            or prepared.last_session != authority.last_session
            or prepared.available_at != authority.available_at
            or not _valid_price_protocol(authority, manifest, decision.source_policy_id)
            or not _valid_price_evidence(authority, manifest, evidence)
            or not _valid_v2_provider_authority(
                manifest,
                capability,
                decision.decision_time,
            )
            or instrument.currency != "USD"
            or instrument.asset_type not in {"ETF", "common_stock"}
            or instrument.exchange not in US_EQUITY_EXCHANGES
            or authority.available_at > decision.decision_time
            or bar.available_at > decision.decision_time
        ):
            raise ValueError("risk price authority is invalid")
        prices.append(
            RiskPrice(
                instrument_id=bar.instrument_id,
                authority_id=authority.id,
                market="US",
                currency="USD",
                asset_type=cast(Literal["ETF", "common_stock"], instrument.asset_type),
                close=bar.close,
                source=manifest.source,
                delivery_mode=manifest.delivery_mode,
                available_at=max(authority.available_at, bar.available_at),
                content_hash=authority.content_hash,
            )
        )
    if observed_ids != required:
        raise ValueError("risk prices do not exactly cover required instruments")
    return tuple(sorted(prices, key=lambda item: item.instrument_id.int))


async def _load_sectors(
    session: AsyncSession,
    decision: PaperDecisionAuthority | RiskAuthorityInput,
    required: set[UUID],
) -> tuple[RiskSectorExposure, ...]:
    latest = (
        select(
            SectorExposure.instrument_id,
            func.max(SectorExposure.as_of).label("as_of"),
        )
        .where(
            SectorExposure.instrument_id.in_(required),
            SectorExposure.as_of <= decision.decision_session,
            SectorExposure.created_at <= decision.decision_time,
            SectorExposure.reviewed.is_(True),
            SectorExposure.synthetic.is_(False),
        )
        .group_by(SectorExposure.instrument_id)
        .subquery()
    )
    records = tuple(
        (
            await session.scalars(
                select(SectorExposure)
                .join(
                    latest,
                    (SectorExposure.instrument_id == latest.c.instrument_id)
                    & (SectorExposure.as_of == latest.c.as_of),
                )
                .where(
                    SectorExposure.created_at <= decision.decision_time,
                    SectorExposure.reviewed.is_(True),
                    SectorExposure.synthetic.is_(False),
                )
                .order_by(SectorExposure.instrument_id, SectorExposure.sector_code)
            )
        ).all()
    )
    if {record.instrument_id for record in records} != required:
        raise ValueError("risk sectors do not exactly cover required instruments")
    if any(record.created_at > decision.decision_time or not record.reviewed for record in records):
        raise ValueError("risk sector authority is invalid")
    return tuple(
        RiskSectorExposure(
            instrument_id=record.instrument_id,
            authority_id=record.id,
            sector=record.sector_code,
            weight=record.weight,
            content_hash=record.content_hash,
        )
        for record in sorted(
            records,
            key=lambda item: (item.instrument_id.int, item.sector_code, item.id.int),
        )
    )


async def _load_query_proofs(
    session: AsyncSession,
    decision: PaperDecisionAuthority | RiskAuthorityInput,
    required: set[UUID],
    authority_ids: Sequence[UUID] | None = None,
) -> tuple[AuthorityQueryProof, ...]:
    query = select(RiskQueryAuthorityRecord).where(
        RiskQueryAuthorityRecord.source_policy_id == decision.source_policy_id,
        RiskQueryAuthorityRecord.universe_snapshot_id == decision.universe_snapshot_id,
        RiskQueryAuthorityRecord.decision_session == decision.decision_session,
    )
    if authority_ids is not None:
        query = query.where(RiskQueryAuthorityRecord.id.in_(tuple(authority_ids)))
    else:
        query = query.where(
            RiskQueryAuthorityRecord.available_at <= decision.decision_time
        ).order_by(
            RiskQueryAuthorityRecord.kind,
            RiskQueryAuthorityRecord.available_at.desc(),
            RiskQueryAuthorityRecord.id.desc(),
        )
    records = tuple((await session.scalars(query)).all())
    if authority_ids is None:
        latest: dict[str, RiskQueryAuthorityRecord] = {}
        for record in records:
            latest.setdefault(record.kind, record)
        records = tuple(latest[kind] for kind in sorted(latest))
    if (
        {record.kind for record in records} != _QUERY_KINDS
        or len(records) != 3
        or (authority_ids is not None and {record.id for record in records} != set(authority_ids))
    ):
        raise ValueError("risk query proofs are incomplete")
    proofs: list[AuthorityQueryProof] = []
    for record in records:
        if (
            set(record.required_instrument_ids) != required
            or len(record.required_instrument_ids) != len(required)
            or not set(record.blocked_instrument_ids) <= required
            or not record.complete
            or record.available_at > decision.decision_time
            or (
                decision.source_policy_id == PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID
                and record.kind == "suspension"
                and decision.decision_time - record.available_at > timedelta(minutes=2)
            )
            or canonical_query_authority_hash(
                source_policy_id=record.source_policy_id,
                universe_snapshot_id=record.universe_snapshot_id,
                decision_session=record.decision_session,
                kind=record.kind,
                required_instrument_ids=record.required_instrument_ids,
                blocked_instrument_ids=record.blocked_instrument_ids,
                complete=record.complete,
                source_watermark_hash=record.source_watermark_hash,
                available_at=record.available_at,
            )
            != record.content_hash
        ):
            raise ValueError("risk query authority is invalid")
        proofs.append(
            AuthorityQueryProof(
                kind=cast(
                    Literal["corporate_action", "suspension", "delisting"],
                    record.kind,
                ),
                authority_id=record.id,
                complete=record.complete,
                blocked=bool(record.blocked_instrument_ids),
                available_at=record.available_at,
                content_hash=record.content_hash,
            )
        )
    return tuple(sorted(proofs, key=lambda item: item.kind))


async def _load_frozen_prior_daily_price(
    session: AsyncSession,
    *,
    authority: RiskAuthorityInput,
    candidate: CandidateManifestEntry,
) -> RiskPrice:
    previous_session = await session.scalar(
        select(ExchangeCalendarSessionRecord.session)
        .where(
            ExchangeCalendarSessionRecord.calendar_authority_id == authority.calendar_authority_id,
            ExchangeCalendarSessionRecord.session < authority.decision_session,
        )
        .order_by(ExchangeCalendarSessionRecord.session.desc())
        .limit(1)
    )
    row = (
        await session.execute(
            select(
                PriceBarSetAuthority,
                PriceSeriesManifest,
                PriceBarRecord,
                Instrument,
                ResearchEvidenceRef,
            )
            .join(
                PriceSeriesManifest,
                PriceSeriesManifest.id == PriceBarSetAuthority.price_series_manifest_id,
            )
            .join(
                PriceBarRecord,
                PriceBarRecord.price_series_manifest_id == PriceSeriesManifest.id,
            )
            .join(Instrument, Instrument.id == PriceBarRecord.instrument_id)
            .join(
                ResearchEvidenceRef,
                ResearchEvidenceRef.id == PriceBarSetAuthority.evidence_ref_id,
            )
            .where(
                PriceBarSetAuthority.id == candidate.prior_daily_price_authority_id,
                PriceBarRecord.instrument_id == candidate.instrument_id,
            )
        )
    ).one_or_none()
    if row is None or previous_session is None:
        raise ValueError("risk price authority is invalid")
    price_authority, manifest, bar, instrument, evidence = row
    prepared = await prepare_price_bar_set(
        session,
        manifest.id,
        as_of=authority.decision_time,
        source_policy_id=PAPER_DECISION_SOURCE_POLICY_ID,
    )
    if (
        bar.session != previous_session
        or price_authority.first_session != previous_session
        or price_authority.last_session != previous_session
        or prepared is None
        or prepared.content_hash != price_authority.content_hash
        or prepared.byte_length != price_authority.byte_length
        or prepared.bar_count != price_authority.bar_count
        or prepared.first_session != price_authority.first_session
        or prepared.last_session != price_authority.last_session
        or prepared.available_at != price_authority.available_at
        or not _valid_price_protocol(
            price_authority,
            manifest,
            PAPER_DECISION_SOURCE_POLICY_ID,
        )
        or not _valid_price_evidence(price_authority, manifest, evidence)
        or instrument.currency != "USD"
        or instrument.asset_type not in {"ETF", "common_stock"}
        or instrument.exchange not in US_EQUITY_EXCHANGES
        or price_authority.available_at > authority.decision_time
        or bar.available_at > authority.decision_time
    ):
        raise ValueError("risk price authority is invalid")
    return RiskPrice(
        instrument_id=bar.instrument_id,
        authority_id=price_authority.id,
        market="US",
        currency="USD",
        asset_type=cast(Literal["ETF", "common_stock"], instrument.asset_type),
        close=bar.close,
        source=manifest.source,
        delivery_mode=manifest.delivery_mode,
        available_at=max(price_authority.available_at, bar.available_at),
        content_hash=price_authority.content_hash,
    )


async def prepare_schwab_realtime_risk_snapshot(
    session: AsyncSession, *, cycle: AgentPaperCycleRecord
) -> RiskMarketSnapshot:
    """Build the same-session risk snapshot only from one cycle's frozen IDs."""
    if (
        cycle.decision_price_policy_id != PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID
        or cycle.execution_price_policy_id != PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID
        or cycle.execution_session != cycle.decision_session
        or cycle.quality_report_id is not None
    ):
        raise ValueError("risk Schwab cycle authority is invalid")
    manifest = tuple(CandidateManifestEntry.model_validate(row) for row in cycle.candidate_manifest)
    required = {candidate.instrument_id for candidate in manifest}
    if (
        not 1 <= len(manifest) <= 5
        or len(required) != len(manifest)
        or candidate_manifest_hash(manifest) != cycle.candidate_hash
    ):
        raise ValueError("risk Schwab candidate authority is invalid")
    calendar = await session.scalar(
        select(ExchangeCalendarAuthorityRecord)
        .join(
            ExchangeCalendarSessionRecord,
            ExchangeCalendarSessionRecord.calendar_authority_id
            == ExchangeCalendarAuthorityRecord.id,
        )
        .where(
            ExchangeCalendarAuthorityRecord.exchange == "XNYS",
            ExchangeCalendarAuthorityRecord.completeness_status == "complete",
            ExchangeCalendarAuthorityRecord.range_start <= cycle.decision_session,
            ExchangeCalendarAuthorityRecord.range_end >= cycle.decision_session,
            ExchangeCalendarAuthorityRecord.published_at <= cycle.decision_time,
            ExchangeCalendarAuthorityRecord.created_at <= cycle.decision_time,
            ExchangeCalendarSessionRecord.session == cycle.decision_session,
        )
        .order_by(
            ExchangeCalendarAuthorityRecord.published_at.desc(),
            ExchangeCalendarAuthorityRecord.id.desc(),
        )
        .limit(1)
    )
    if calendar is None:
        raise ValueError("risk calendar authority is incomplete")
    authority = RiskAuthorityInput(
        decision_session=cycle.decision_session,
        execution_session=cycle.execution_session,
        decision_time=cycle.decision_time,
        source_policy_id=cycle.decision_price_policy_id,
        calendar_authority_id=calendar.id,
        universe_snapshot_id=cycle.universe_snapshot_id,
        quality_report_id=cycle.quality_report_id,
    )
    whitelist = await _validate_root_authorities(session, authority, required)
    prices: list[RiskPrice] = []
    proofs: list[AuthorityQueryProof] = []
    for candidate in sorted(manifest, key=lambda item: item.instrument_id.int):
        prices.append(
            await _load_frozen_prior_daily_price(
                session,
                authority=authority,
                candidate=candidate,
            )
        )
        candidate_proofs = await _load_query_proofs(
            session,
            authority,
            {candidate.instrument_id},
            candidate.risk_query_authority_ids,
        )
        proofs.extend(
            proof.model_copy(update={"required_instrument_ids": (candidate.instrument_id,)})
            if len(manifest) > 1
            else proof
            for proof in candidate_proofs
        )
    return build_risk_market_snapshot(
        decision=authority,
        required_instrument_ids=tuple(required),
        whitelisted_instrument_ids=whitelist,
        prices=prices,
        sector_exposures=await _load_sectors(session, authority, required),
        query_proofs=proofs,
    )


async def load_risk_market_snapshot(
    session: AsyncSession,
    *,
    decision: PaperDecisionAuthority,
    required_instrument_ids: Sequence[UUID],
) -> RiskMarketSnapshot:
    """Load all risk authority facts from one caller-owned transaction snapshot."""
    required_values = tuple(required_instrument_ids)
    required = set(required_values)
    if not required or len(required) != len(required_values):
        raise ValueError("required risk instruments must be nonempty and unique")
    decision_instruments = {target.instrument_id for target in decision.targets}
    if not decision_instruments <= required:
        raise ValueError("required risk instruments omit decision targets")
    market = await prepare_risk_market_snapshot(
        session,
        authority=RiskAuthorityInput(
            decision_session=decision.decision_session,
            execution_session=decision.execution_session,
            decision_time=decision.decision_time,
            source_policy_id=decision.source_policy_id,
            calendar_authority_id=decision.calendar_authority_id,
            universe_snapshot_id=decision.universe_snapshot_id,
            quality_report_id=decision.quality_report_id,
        ),
        required_instrument_ids=required_values,
    )
    require_decision_authority_match(decision, market)
    return market


async def prepare_risk_market_snapshot(
    session: AsyncSession,
    *,
    authority: RiskAuthorityInput,
    required_instrument_ids: Sequence[UUID],
) -> RiskMarketSnapshot:
    """Prepare first evidence from one caller-owned snapshot; never persist facts.

    This computes evidence identity only, not order eligibility or execution permission.
    Saved decisions must still use load_risk_market_snapshot to verify their bound hash.
    """
    required_values = tuple(required_instrument_ids)
    required = set(required_values)
    if not required or len(required) != len(required_values):
        raise ValueError("required risk instruments must be nonempty and unique")
    whitelist = await _validate_root_authorities(session, authority, required)
    market = build_risk_market_snapshot(
        decision=authority,
        required_instrument_ids=required_values,
        whitelisted_instrument_ids=whitelist,
        prices=await _load_prices(session, authority, required),
        sector_exposures=await _load_sectors(session, authority, required),
        query_proofs=await _load_query_proofs(session, authority, required),
    )
    return market
