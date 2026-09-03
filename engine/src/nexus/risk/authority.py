"""Exact-set assembly for deterministic paper risk market authority."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from datetime import UTC, date, datetime
from typing import Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_validator
from sqlalchemy import select
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
from nexus.persistence.models.paper import RiskQueryAuthorityRecord
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
    RiskMarketSnapshot,
    RiskPrice,
    RiskSectorExposure,
)
from nexus.strategy.forward import ForwardStrategyDecision

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
    quality_report_id: UUID

    @field_validator("decision_time")
    @classmethod
    def _utc_cutoff(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
            raise ValueError("decision time must use UTC")
        return value


def _valid_source_policy(policy: SourcePolicy) -> bool:
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
) -> bool:
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
    decision: ForwardStrategyDecision | RiskAuthorityInput,
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
        query_proofs=tuple(sorted(query_proofs, key=lambda item: item.kind)),
        authority_hash="0" * 64,
    )
    return draft.model_copy(update={"authority_hash": canonical_risk_market_authority_hash(draft)})


def require_decision_authority_match(
    decision: ForwardStrategyDecision,
    market: RiskMarketSnapshot,
) -> None:
    """Bind a reloaded authority set to the immutable forward decision."""
    recomputed = canonical_risk_market_authority_hash(market)
    if recomputed != market.authority_hash or decision.authority_hash != market.authority_hash:
        raise ValueError("risk authority hash does not match forward decision")


async def _validate_root_authorities(
    session: AsyncSession,
    decision: ForwardStrategyDecision | RiskAuthorityInput,
    required: set[UUID],
) -> tuple[UUID, ...]:
    source_policy = await session.get(SourcePolicy, decision.source_policy_id)
    calendar = await session.get(ExchangeCalendarAuthorityRecord, decision.calendar_authority_id)
    universe = await session.get(UniverseSnapshot, decision.universe_snapshot_id)
    quality = await session.get(QualityReport, decision.quality_report_id)
    if (
        source_policy is None
        or not _valid_source_policy(source_policy)
        or calendar is None
        or universe is None
        or quality is None
    ):
        raise ValueError("risk authority roots are incomplete")
    quality_snapshot = await session.get(DataSnapshot, quality.data_snapshot_id)
    if quality_snapshot is None or quality.status != "PASS":
        raise ValueError("risk quality report is incomplete")
    available_times = (
        source_policy.created_at,
        calendar.published_at,
        universe.published_at,
        quality.created_at,
        quality_snapshot.available_at,
    )
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
    if (
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
    expected_quality_ids = sorted(str(value).lower() for value in required)
    if (
        quality.payload.get("required_instrument_ids") != expected_quality_ids
        or quality.payload.get("decision_session") != decision.decision_session.isoformat()
    ):
        raise ValueError("risk quality report does not cover the exact authority set")
    return tuple(sorted(whitelist, key=lambda value: value.int))


async def _load_prices(
    session: AsyncSession,
    decision: ForwardStrategyDecision | RiskAuthorityInput,
    required: set[UUID],
) -> tuple[RiskPrice, ...]:
    rows = (
        await session.execute(
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
                PriceBarSetAuthority.source_policy_id == decision.source_policy_id,
                PriceBarRecord.instrument_id.in_(required),
                PriceBarRecord.session == decision.decision_session,
            )
        )
    ).all()
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
        )
        if (
            prepared is None
            or prepared.content_hash != authority.content_hash
            or prepared.byte_length != authority.byte_length
            or prepared.bar_count != authority.bar_count
            or prepared.first_session != authority.first_session
            or prepared.last_session != authority.last_session
            or prepared.available_at != authority.available_at
            or not _valid_price_protocol(authority, manifest)
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
    decision: ForwardStrategyDecision | RiskAuthorityInput,
    required: set[UUID],
) -> tuple[RiskSectorExposure, ...]:
    records = tuple(
        (
            await session.scalars(
                select(SectorExposure).where(
                    SectorExposure.instrument_id.in_(required),
                    SectorExposure.as_of == decision.decision_session,
                )
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
    decision: ForwardStrategyDecision | RiskAuthorityInput,
    required: set[UUID],
) -> tuple[AuthorityQueryProof, ...]:
    records = tuple(
        (
            await session.scalars(
                select(RiskQueryAuthorityRecord).where(
                    RiskQueryAuthorityRecord.source_policy_id == decision.source_policy_id,
                    RiskQueryAuthorityRecord.universe_snapshot_id == decision.universe_snapshot_id,
                    RiskQueryAuthorityRecord.decision_session == decision.decision_session,
                )
            )
        ).all()
    )
    if {record.kind for record in records} != _QUERY_KINDS or len(records) != 3:
        raise ValueError("risk query proofs are incomplete")
    proofs: list[AuthorityQueryProof] = []
    for record in records:
        if (
            set(record.required_instrument_ids) != required
            or len(record.required_instrument_ids) != len(required)
            or not set(record.blocked_instrument_ids) <= required
            or not record.complete
            or record.available_at > decision.decision_time
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


async def load_risk_market_snapshot(
    session: AsyncSession,
    *,
    decision: ForwardStrategyDecision,
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
