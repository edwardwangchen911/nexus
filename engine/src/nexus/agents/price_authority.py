"""Point-in-time selection of one database-resident price authority."""

from datetime import datetime
from enum import StrEnum
from uuid import UUID

from sqlalchemy import and_, case, or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select

from nexus.market.price_bar_authority import (
    HASH_VERSION,
    HASH_VERSION_V2,
    POLICY_VERSION,
    POLICY_VERSION_V2,
    SOURCE_POLICY_ID,
    prepare_price_bar_set,
)
from nexus.persistence.models.agents import (
    AgentDataSnapshotEvidenceRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import (
    DataSnapshot,
    PriceBarSetAuthority,
    PriceSeriesManifest,
)
from nexus.persistence.models.data import (
    SourcePolicy as SourcePolicyRecord,
)
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.quality.source_policy import (
    SOURCE_POLICY_KEY,
    SOURCE_POLICY_PAYLOAD,
    SOURCE_POLICY_V2_ID,
    SOURCE_POLICY_V2_PAYLOAD,
    SOURCE_POLICY_V2_VERSION,
    SOURCE_POLICY_VERSION,
)


def _runtime_eligible(manifest: PriceSeriesManifest, *, allow_fixture: bool) -> bool:
    if (
        manifest.source == "fosun"
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
                manifest.gateway_transport_artifact_id,
                manifest.daily_calendar_authority_id,
                manifest.gateway_request_start_at,
                manifest.gateway_request_end_at,
            )
        )
    ):
        return True
    return (
        allow_fixture
        and manifest.source == "fixture"
        and bool(manifest.feed)
        and manifest.delivery_mode == "fixture"
        and manifest.interval == "1Day"
    )


def _authority_statement(
    *,
    instrument_id: UUID,
    selected_at: datetime,
    allow_fixture: bool | None,
) -> Select[
    tuple[
        PriceBarSetAuthority,
        ResearchEvidenceRef,
        PriceSeriesManifest,
        SourcePolicyRecord,
    ]
]:
    fosun = and_(
        PriceSeriesManifest.source == "fosun",
        PriceSeriesManifest.feed == "l1",
        PriceSeriesManifest.delivery_mode.in_(("real_time", "delayed")),
        PriceSeriesManifest.adjustment == "raw",
        PriceSeriesManifest.interval == "1Day",
        PriceSeriesManifest.provider_capability_id.is_not(None),
        PriceSeriesManifest.provider_status_id.is_not(None),
        PriceSeriesManifest.provider_status_version.is_not(None),
        PriceSeriesManifest.provider_contract_hash.is_not(None),
        PriceSeriesManifest.provider_authorized_at.is_not(None),
        PriceSeriesManifest.gateway_transport_artifact_id.is_not(None),
        PriceSeriesManifest.daily_calendar_authority_id.is_not(None),
        PriceSeriesManifest.gateway_request_start_at.is_not(None),
        PriceSeriesManifest.gateway_request_end_at.is_not(None),
    )
    historical_alpaca = and_(
        PriceSeriesManifest.source == "alpaca",
        PriceSeriesManifest.feed == "sip",
        PriceSeriesManifest.delivery_mode == "delayed",
        PriceSeriesManifest.interval == "1Day",
    )
    fixture = and_(
        PriceSeriesManifest.source == "fixture",
        PriceSeriesManifest.feed != "",
        PriceSeriesManifest.delivery_mode == "fixture",
        PriceSeriesManifest.interval == "1Day",
    )
    runtime_source = fosun if allow_fixture is False else or_(fosun, fixture)
    if allow_fixture is None:
        runtime_source = or_(fosun, historical_alpaca, fixture)
    v1_protocol = and_(
        PriceBarSetAuthority.source_policy_id == SOURCE_POLICY_ID,
        PriceBarSetAuthority.hash_version == HASH_VERSION,
        PriceBarSetAuthority.policy_version == POLICY_VERSION,
        SourcePolicyRecord.id == SOURCE_POLICY_ID,
        SourcePolicyRecord.key == SOURCE_POLICY_KEY,
        SourcePolicyRecord.version == SOURCE_POLICY_VERSION,
        SourcePolicyRecord.schema_version == "1",
        SourcePolicyRecord.payload == SOURCE_POLICY_PAYLOAD,
        or_(historical_alpaca, fixture),
    )
    v2_protocol = and_(
        PriceBarSetAuthority.source_policy_id == SOURCE_POLICY_V2_ID,
        PriceBarSetAuthority.hash_version == HASH_VERSION_V2,
        PriceBarSetAuthority.policy_version == POLICY_VERSION_V2,
        SourcePolicyRecord.id == SOURCE_POLICY_V2_ID,
        SourcePolicyRecord.key == SOURCE_POLICY_KEY,
        SourcePolicyRecord.version == SOURCE_POLICY_V2_VERSION,
        SourcePolicyRecord.schema_version == "2",
        SourcePolicyRecord.payload == SOURCE_POLICY_V2_PAYLOAD,
        fosun,
    )
    return (
        select(
            PriceBarSetAuthority,
            ResearchEvidenceRef,
            PriceSeriesManifest,
            SourcePolicyRecord,
        )
        .join(
            ResearchEvidenceRef,
            ResearchEvidenceRef.id == PriceBarSetAuthority.evidence_ref_id,
        )
        .join(
            PriceSeriesManifest,
            PriceSeriesManifest.id == PriceBarSetAuthority.price_series_manifest_id,
        )
        .join(
            SourcePolicyRecord,
            SourcePolicyRecord.id == PriceBarSetAuthority.source_policy_id,
        )
        .where(
            or_(v1_protocol, v2_protocol),
            PriceSeriesManifest.instrument == str(instrument_id),
            PriceBarSetAuthority.available_at <= selected_at,
            runtime_source,
        )
        .order_by(
            case(
                (PriceSeriesManifest.source == "fosun", 0),
                (PriceSeriesManifest.source == "alpaca", 1),
                else_=2,
            ),
            PriceBarSetAuthority.available_at.desc(),
            PriceSeriesManifest.id.desc(),
        )
    )


def _valid_owner(
    authority: PriceBarSetAuthority,
    evidence: ResearchEvidenceRef,
    manifest: PriceSeriesManifest,
    policy: SourcePolicyRecord,
    *,
    instrument_id: UUID,
    selected_at: datetime,
    allow_fixture: bool,
) -> bool:
    historical_alpaca_v1 = (
        manifest.source == "alpaca"
        and manifest.feed == "sip"
        and manifest.delivery_mode == "delayed"
        and manifest.interval == "1Day"
    )
    fixture_v1 = (
        allow_fixture
        and manifest.source == "fixture"
        and bool(manifest.feed)
        and manifest.delivery_mode == "fixture"
        and manifest.interval == "1Day"
    )
    v1_protocol = (
        authority.source_policy_id == policy.id == SOURCE_POLICY_ID
        and authority.hash_version == HASH_VERSION
        and authority.policy_version == POLICY_VERSION
        and policy.key == SOURCE_POLICY_KEY
        and policy.version == SOURCE_POLICY_VERSION
        and policy.schema_version == "1"
        and policy.payload == SOURCE_POLICY_PAYLOAD
        and (historical_alpaca_v1 or fixture_v1)
    )
    v2_protocol = (
        authority.source_policy_id == policy.id == SOURCE_POLICY_V2_ID
        and authority.hash_version == HASH_VERSION_V2
        and authority.policy_version == POLICY_VERSION_V2
        and policy.key == SOURCE_POLICY_KEY
        and policy.version == SOURCE_POLICY_V2_VERSION
        and policy.schema_version == "2"
        and policy.payload == SOURCE_POLICY_V2_PAYLOAD
        and _runtime_eligible(manifest, allow_fixture=False)
    )
    return (
        authority.price_series_manifest_id == manifest.id
        and authority.evidence_ref_id == evidence.id
        and (v1_protocol or v2_protocol)
        and authority.source == manifest.source == evidence.source
        and authority.provider_object_id
        == evidence.provider_object_id
        == f"price-bar-set:{manifest.id}"
        and authority.revision == evidence.revision == authority.content_hash
        and authority.available_at == evidence.available_at
        and authority.available_at <= selected_at
        and authority.content_hash == evidence.content_hash
        and authority.byte_length == evidence.byte_length
        and evidence.location
        == f"db://price-bar-set/{manifest.id}@sha256:{authority.content_hash}"
        and manifest.instrument == str(instrument_id)
    )


def _valid_existing_link(
    record: AgentDataSnapshotEvidenceRecord,
    evidence: ResearchEvidenceRef,
    *,
    snapshot: DataSnapshot,
) -> bool:
    return (
        record.selected_at is not None
        and record.selected_at <= snapshot.available_at
        and record.evidence_ref_id == evidence.id
        and record.source == evidence.source
        and record.provider_object_id == evidence.provider_object_id
        and record.revision == evidence.revision
        and record.available_at == evidence.available_at
        and record.available_at <= record.selected_at
        and record.location == evidence.location
        and record.content_hash == evidence.content_hash
    )


class FrozenPriceAuthorityStatus(StrEnum):
    """Current database verdict for the price authority frozen into one run."""

    MISSING = "missing"
    VALID = "valid"
    INVALID = "invalid"


async def revalidate_frozen_price_authority(
    session: AsyncSession,
    *,
    run: AgentResearchRunRecord,
    evidence_records: tuple[ResearchEvidenceRef, ...],
    allow_fixture: bool,
) -> FrozenPriceAuthorityStatus:
    """Revalidate one frozen BarSet owner and its current canonical database rows."""
    persisted_run_identity = (
        await session.execute(
            select(
                AgentResearchRunRecord.id,
                AgentResearchRunRecord.data_snapshot_id,
                AgentResearchRunRecord.instrument_id,
                AgentResearchRunRecord.decision_time,
            ).where(AgentResearchRunRecord.id == run.id)
        )
    ).one_or_none()
    if persisted_run_identity is None:
        return FrozenPriceAuthorityStatus.INVALID
    current_run_id, current_snapshot_id, current_instrument_id, current_decision_time = (
        persisted_run_identity
    )
    if (
        current_run_id,
        current_snapshot_id,
        current_instrument_id,
        current_decision_time,
    ) != (run.id, run.data_snapshot_id, run.instrument_id, run.decision_time):
        return FrozenPriceAuthorityStatus.INVALID
    shaped_evidence = tuple(
        evidence
        for evidence in evidence_records
        if evidence.provider_object_id.startswith("price-bar-set:")
        or evidence.location.startswith("db://price-bar-set/")
    )
    if not shaped_evidence:
        return FrozenPriceAuthorityStatus.MISSING
    if len(shaped_evidence) != 1:
        return FrozenPriceAuthorityStatus.INVALID
    evidence = shaped_evidence[0]
    if not (
        evidence.provider_object_id.startswith("price-bar-set:")
        and evidence.location.startswith("db://price-bar-set/")
    ):
        return FrozenPriceAuthorityStatus.INVALID
    snapshot = await session.get(
        DataSnapshot,
        run.data_snapshot_id,
        populate_existing=True,
    )
    binding = await session.scalar(
        select(AgentDataSnapshotEvidenceRecord).where(
            AgentDataSnapshotEvidenceRecord.data_snapshot_id == run.data_snapshot_id,
            AgentDataSnapshotEvidenceRecord.instrument_id == run.instrument_id,
            AgentDataSnapshotEvidenceRecord.evidence_ref_id == evidence.id,
            AgentDataSnapshotEvidenceRecord.authority_kind == "price",
            AgentDataSnapshotEvidenceRecord.selected_at.is_not(None),
            AgentDataSnapshotEvidenceRecord.selected_at <= run.decision_time,
        ).execution_options(populate_existing=True)
    )
    if (
        snapshot is None
        or snapshot.available_at > current_decision_time
        or snapshot.received_at > current_decision_time
        or binding is None
        or binding.authority_kind != "price"
        or binding.selected_at is None
        or binding.selected_at > run.decision_time
    ):
        return FrozenPriceAuthorityStatus.INVALID
    loaded = tuple(
        (
            await session.execute(
                _authority_statement(
                    instrument_id=run.instrument_id,
                    selected_at=binding.selected_at,
                    allow_fixture=None,
                )
                .where(PriceBarSetAuthority.evidence_ref_id == evidence.id)
                .limit(2)
                .execution_options(populate_existing=True)
            )
        ).all()
    )
    if len(loaded) != 1:
        return FrozenPriceAuthorityStatus.INVALID
    authority, current_evidence, manifest, policy = loaded[0]
    if not (
        _valid_owner(
            authority,
            current_evidence,
            manifest,
            policy,
            instrument_id=run.instrument_id,
            selected_at=binding.selected_at,
            allow_fixture=allow_fixture,
        )
        and _valid_existing_link(binding, current_evidence, snapshot=snapshot)
        and evidence.id == current_evidence.id
        and evidence.source == current_evidence.source
        and evidence.provider_object_id == current_evidence.provider_object_id
        and evidence.revision == current_evidence.revision
        and evidence.available_at == current_evidence.available_at
        and evidence.location == current_evidence.location
        and evidence.content_hash == current_evidence.content_hash
    ):
        return FrozenPriceAuthorityStatus.INVALID
    if not await _prepared_owner_matches(
        session,
        authority=authority,
        manifest=manifest,
        selected_at=binding.selected_at,
    ):
        return FrozenPriceAuthorityStatus.INVALID
    return FrozenPriceAuthorityStatus.VALID


async def freeze_price_authority(
    session: AsyncSession,
    *,
    snapshot: DataSnapshot,
    instrument_id: UUID,
    decision_time: datetime,
    allow_fixture: bool = False,
) -> None:
    """Attach one runtime-eligible finalized Bar-set authority, if available."""
    current_snapshot = await session.get(
        DataSnapshot,
        snapshot.id,
        populate_existing=True,
    )
    snapshot_is_current = (
        current_snapshot is not None
        and current_snapshot.available_at <= decision_time
        and current_snapshot.received_at <= decision_time
    )
    existing_records = tuple(
        await session.scalars(
            select(AgentDataSnapshotEvidenceRecord)
            .outerjoin(
                PriceBarSetAuthority,
                PriceBarSetAuthority.evidence_ref_id
                == AgentDataSnapshotEvidenceRecord.evidence_ref_id,
            )
            .where(
                AgentDataSnapshotEvidenceRecord.data_snapshot_id == snapshot.id,
                AgentDataSnapshotEvidenceRecord.instrument_id == instrument_id,
                or_(
                    AgentDataSnapshotEvidenceRecord.authority_kind == "price",
                    PriceBarSetAuthority.evidence_ref_id.is_not(None),
                ),
            )
            .limit(2)
            .execution_options(populate_existing=True)
        )
    )
    if existing_records:
        if len(existing_records) != 1:
            raise ValueError("existing price authority is invalid")
        existing = existing_records[0]
        if (
            not snapshot_is_current
            or existing.authority_kind != "price"
            or existing.selected_at is None
        ):
            raise ValueError("existing price authority is invalid")
        loaded = tuple(
            (
                await session.execute(
                    _authority_statement(
                        instrument_id=instrument_id,
                        selected_at=existing.selected_at,
                        allow_fixture=None,
                    )
                    .where(
                        PriceBarSetAuthority.evidence_ref_id
                        == existing.evidence_ref_id
                    )
                    .limit(2)
                    .execution_options(populate_existing=True)
                )
            ).all()
        )
        if len(loaded) != 1 or current_snapshot is None:
            raise ValueError("existing price authority is invalid")
        authority, evidence, manifest, policy = loaded[0]
        if not (
            _valid_owner(
                authority,
                evidence,
                manifest,
                policy,
                instrument_id=instrument_id,
                selected_at=existing.selected_at,
                allow_fixture=allow_fixture,
            )
            and _valid_existing_link(
                existing,
                evidence,
                snapshot=current_snapshot,
            )
        ):
            raise ValueError("existing price authority is invalid")
        if not await _prepared_owner_matches(
            session,
            authority=authority,
            manifest=manifest,
            selected_at=existing.selected_at,
        ):
            raise ValueError("existing price authority is invalid")
        return
    if not snapshot_is_current or current_snapshot is None:
        return
    selected_at = current_snapshot.available_at
    selected = (
        await session.execute(
            _authority_statement(
                instrument_id=instrument_id,
                selected_at=selected_at,
                allow_fixture=allow_fixture,
            )
            .limit(1)
        )
    ).one_or_none()
    if selected is None and not allow_fixture:
        blocked_fixture = (
            await session.execute(
                _authority_statement(
                    instrument_id=instrument_id,
                    selected_at=selected_at,
                    allow_fixture=None,
                )
                .where(PriceSeriesManifest.source == "fixture")
                .limit(1)
            )
        ).one_or_none()
        if blocked_fixture is not None:
            authority, evidence, manifest, policy = blocked_fixture
            if not _valid_owner(
                authority,
                evidence,
                manifest,
                policy,
                instrument_id=instrument_id,
                selected_at=selected_at,
                allow_fixture=True,
            ):
                raise ValueError("price authority is invalid")
            raise ValueError("price authority is not permitted in this runtime")
    if selected is None:
        return
    authority, evidence, manifest, policy = selected
    if not _valid_owner(
        authority,
        evidence,
        manifest,
        policy,
        instrument_id=instrument_id,
        selected_at=selected_at,
        allow_fixture=allow_fixture,
    ):
        raise ValueError("price authority is invalid")
    if not await _prepared_owner_matches(
        session,
        authority=authority,
        manifest=manifest,
        selected_at=selected_at,
    ):
        return
    await session.execute(
        pg_insert(AgentDataSnapshotEvidenceRecord)
        .values(
            data_snapshot_id=current_snapshot.id,
            instrument_id=instrument_id,
            evidence_ref_id=evidence.id,
            authority_kind="price",
            selected_at=selected_at,
            source=evidence.source,
            provider_object_id=evidence.provider_object_id,
            revision=evidence.revision,
            available_at=evidence.available_at,
            location=evidence.location,
            content_hash=evidence.content_hash,
        )
        .on_conflict_do_nothing()
    )


async def _prepared_owner_matches(
    session: AsyncSession,
    *,
    authority: PriceBarSetAuthority,
    manifest: PriceSeriesManifest,
    selected_at: datetime,
) -> bool:
    prepared = await prepare_price_bar_set(
        session,
        manifest.id,
        as_of=selected_at,
    )
    return prepared is not None and (
        prepared.content_hash,
        prepared.byte_length,
        prepared.bar_count,
        prepared.first_session,
        prepared.last_session,
        prepared.available_at,
    ) == (
        authority.content_hash,
        authority.byte_length,
        authority.bar_count,
        authority.first_session,
        authority.last_session,
        authority.available_at,
    )
