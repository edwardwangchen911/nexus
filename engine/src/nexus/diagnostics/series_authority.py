"""Point-in-time source-policy selection with complete safe provenance."""

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Literal, cast
from uuid import UUID

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from nexus.diagnostics.artifacts import ArtifactByteReader, parse_price_series_artifact
from nexus.diagnostics.evidence import combine_refs
from nexus.diagnostics.models import EvidenceBinding
from nexus.domain.evidence import EvidenceRef
from nexus.market.price_bar_authority import (
    HASH_VERSION_V2,
    POLICY_VERSION_V2,
    SOURCE_POLICY_V2_ID,
    PriceBarSetPreparation,
    prepare_price_bar_set,
)
from nexus.persistence.models.data import (
    DataArtifact,
    PriceBarSetAuthority,
    PriceSeriesManifest,
    ProviderCapability,
)
from nexus.persistence.models.market import Instrument
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.providers.base import PriceSeries
from nexus.quality.source_policy import (
    SourcePolicy,
    SourcePolicyV2,
    apply_source_policy_v1,
)

_US_EXCHANGES = frozenset(("AMEX", "ARCX", "NASDAQ", "NYSE", "NYSE_ARCA", "XNAS", "XNYS"))

type CandidateDisposition = Literal[
    "selected", "superseded", "superseded_revision", "ineligible", "conflicting"
]


@dataclass(frozen=True)
class SeriesAuthority:
    selected: tuple[PriceSeries, EvidenceBinding] | None
    quality: Literal["PASS", "PARTIAL", "FAIL"]
    reasons: tuple[str, ...]
    source_conflict: bool
    relevant_refs: tuple[EvidenceRef, ...]
    canonical_input: dict[str, object]


@dataclass(frozen=True)
class DiagnosticSourcePolicyAuthority:
    """Typed policy route; callers never infer authority from an evidence string."""

    version: Literal["source_policy_v1", "source_policy_v2"]
    policy: SourcePolicy | SourcePolicyV2
    ref: EvidenceRef


async def select_authoritative_series(
    session: AsyncSession,
    instrument_id: UUID,
    as_of: datetime,
    reader: ArtifactByteReader,
    policy_authority: DiagnosticSourcePolicyAuthority,
) -> SeriesAuthority:
    policy_ref = policy_authority.ref
    instrument = await session.scalar(
        select(Instrument).where(
            Instrument.id == instrument_id,
            Instrument.created_at <= as_of,
        )
    )
    if instrument is None:
        return SeriesAuthority(
            selected=None,
            quality="FAIL",
            reasons=("INSTRUMENT_AUTHORITY_UNAVAILABLE",),
            source_conflict=False,
            relevant_refs=(policy_ref,),
            canonical_input={
                "instrument_id": str(instrument_id),
                "selector": _selector_input(None, "FAIL", ("INSTRUMENT_AUTHORITY_UNAVAILABLE",)),
                "candidates": (),
            },
        )
    if policy_authority.version == "source_policy_v2":
        if instrument.currency != "USD" or instrument.exchange not in _US_EXCHANGES:
            return _unavailable_series(
                instrument_id,
                policy_ref,
                "INSTRUMENT_MARKET_SCOPE_MISMATCH",
            )
        return await _select_v2_series(
            session,
            instrument_id,
            as_of,
            reader,
            policy_authority,
        )
    rows = (
        await session.execute(
            select(PriceSeriesManifest, DataArtifact)
            .join(DataArtifact, DataArtifact.id == PriceSeriesManifest.data_artifact_id)
            .where(
                PriceSeriesManifest.instrument == str(instrument_id),
                PriceSeriesManifest.end_at <= as_of,
                PriceSeriesManifest.created_at <= as_of,
                DataArtifact.created_at <= as_of,
                DataArtifact.available_at <= as_of,
                DataArtifact.received_at <= as_of,
            )
            .order_by(
                desc(DataArtifact.available_at),
                desc(DataArtifact.received_at),
                desc(PriceSeriesManifest.id),
            )
        )
    ).all()
    candidates: list[PriceSeries] = []
    candidate_inputs: list[dict[str, object]] = []
    bindings: dict[tuple[str, str, str, str], EvidenceBinding] = {}
    primary_indexes: dict[tuple[str, str, str, str], int] = {}
    individual_eligible: dict[tuple[str, str, str, str], bool] = {}
    artifact_refs: list[EvidenceRef] = []
    for manifest, artifact in rows:
        artifact_ref = _artifact_ref(manifest, artifact)
        artifact_refs.append(artifact_ref)
        key = (manifest.source, manifest.feed, manifest.adjustment, manifest.delivery_mode)
        candidate_input = _candidate_input(manifest, artifact)
        if key in bindings:
            candidate_input["disposition"] = "superseded_revision"
            candidate_inputs.append(candidate_input)
            continue
        payload = await reader(artifact.uri)
        series, parsed_ref = parse_price_series_artifact(payload, artifact, manifest, instrument_id)
        binding = EvidenceBinding(
            available_at=max(artifact.available_at, policy_ref.available_at),
            as_of=max(bar.observed_at or bar.available_at for bar in series.bars),
            session=max(bar.session for bar in series.bars),
            refs=(parsed_ref, policy_ref),
        )
        individual = apply_source_policy_v1(
            (series,),
            cast(SourcePolicy, policy_authority.policy),
            as_of,
            None,
        )
        candidate_input["parsed"] = True
        candidate_input["individual_selector"] = _selector_input(
            individual.selected,
            individual.quality,
            individual.reasons,
        )
        primary_indexes[key] = len(candidate_inputs)
        bindings[key] = binding
        individual_eligible[key] = individual.selected is not None
        candidates.append(series)
        candidate_inputs.append(candidate_input)
    selection = apply_source_policy_v1(
        candidates,
        cast(SourcePolicy, policy_authority.policy),
        as_of,
        None,
    )
    selected_key = None if selection.selected is None else _series_key(selection.selected)
    conflict = "SOURCE_CONFLICT" in selection.reasons
    for key, index in primary_indexes.items():
        disposition: CandidateDisposition
        if conflict and individual_eligible[key]:
            disposition = "conflicting"
        elif key == selected_key:
            disposition = "selected"
        elif not individual_eligible[key]:
            disposition = "ineligible"
        else:
            disposition = "superseded"
        candidate_inputs[index]["disposition"] = disposition
    selected = (
        None
        if selection.selected is None or selection.quality != "PASS" or selected_key is None
        else (selection.selected, bindings[selected_key])
    )
    relevant_refs = combine_refs(tuple(artifact_refs), (policy_ref,))
    return SeriesAuthority(
        selected=selected,
        quality=selection.quality,
        reasons=selection.reasons,
        source_conflict=conflict,
        relevant_refs=relevant_refs,
        canonical_input={
            "instrument_id": str(instrument_id),
            "selector": _selector_input(selection.selected, selection.quality, selection.reasons),
            "selected_manifest_id": (
                None
                if selected_key is None
                else candidate_inputs[primary_indexes[selected_key]]["manifest_id"]
            ),
            "candidates": tuple(candidate_inputs),
        },
    )


async def _select_v2_series(
    session: AsyncSession,
    instrument_id: UUID,
    as_of: datetime,
    reader: ArtifactByteReader,
    policy_authority: DiagnosticSourcePolicyAuthority,
) -> SeriesAuthority:
    artifact_evidence = aliased(ResearchEvidenceRef)
    authority_evidence = aliased(ResearchEvidenceRef)
    rows = (
        await session.execute(
            select(
                PriceSeriesManifest,
                DataArtifact,
                PriceBarSetAuthority,
                artifact_evidence,
                authority_evidence,
                ProviderCapability,
            )
            .join(DataArtifact, DataArtifact.id == PriceSeriesManifest.data_artifact_id)
            .join(
                artifact_evidence,
                artifact_evidence.id == PriceSeriesManifest.evidence_ref_id,
            )
            .join(
                PriceBarSetAuthority,
                PriceBarSetAuthority.price_series_manifest_id == PriceSeriesManifest.id,
            )
            .join(
                authority_evidence,
                authority_evidence.id == PriceBarSetAuthority.evidence_ref_id,
            )
            .join(
                ProviderCapability,
                ProviderCapability.id == PriceSeriesManifest.provider_capability_id,
            )
            .where(
                PriceSeriesManifest.instrument == str(instrument_id),
                PriceSeriesManifest.source == "fosun",
                PriceSeriesManifest.feed == "l1",
                PriceSeriesManifest.delivery_mode.in_(("real_time", "delayed")),
                PriceSeriesManifest.adjustment == "raw",
                PriceSeriesManifest.interval == "1Day",
                PriceSeriesManifest.end_at <= as_of,
                PriceSeriesManifest.created_at <= as_of,
                PriceSeriesManifest.provider_authorized_at <= as_of,
                DataArtifact.source == "fosun",
                DataArtifact.schema_version == "price_bars_v1",
                DataArtifact.created_at <= as_of,
                DataArtifact.available_at <= as_of,
                DataArtifact.received_at <= as_of,
                PriceBarSetAuthority.source_policy_id == SOURCE_POLICY_V2_ID,
                PriceBarSetAuthority.hash_version == HASH_VERSION_V2,
                PriceBarSetAuthority.policy_version == POLICY_VERSION_V2,
                PriceBarSetAuthority.available_at <= as_of,
                artifact_evidence.available_at <= as_of,
                authority_evidence.available_at <= as_of,
                ProviderCapability.provider == "fosun",
                ProviderCapability.capability == "READ_ONLY",
                ProviderCapability.schema_version == "1",
                ProviderCapability.contract_hash
                == PriceSeriesManifest.provider_contract_hash,
                ProviderCapability.created_at
                <= PriceSeriesManifest.provider_authorized_at,
                ProviderCapability.expires_at > as_of,
                ProviderCapability.payload["operation"].as_string()
                == "fosun_get_bars",
                ProviderCapability.payload["market_scope"].as_string() == "US",
                ProviderCapability.payload["delivery_mode"].as_string()
                == PriceSeriesManifest.delivery_mode,
            )
            .order_by(
                desc(PriceBarSetAuthority.available_at),
                desc(DataArtifact.available_at),
                desc(PriceSeriesManifest.id),
            )
        )
    ).all()
    candidate_inputs: list[dict[str, object]] = []
    relevant_refs: list[EvidenceRef] = [policy_authority.ref]
    selected: tuple[PriceSeries, EvidenceBinding] | None = None
    selected_manifest_id: str | None = None
    selected_key: tuple[str, str, str, str] | None = None
    seen_keys: set[tuple[str, str, str, str]] = set()
    for manifest, artifact, authority, artifact_ref, authority_ref, capability in rows:
        candidate_input = _candidate_input_v2(
            manifest,
            artifact,
            authority,
            capability,
        )
        parsed_artifact_ref = _research_ref(artifact_ref)
        parsed_authority_ref = _research_ref(authority_ref)
        relevant_refs.extend((parsed_artifact_ref, parsed_authority_ref))
        prepared = await prepare_price_bar_set(session, manifest.id)
        if (
            prepared is None
            or not _valid_v2_final_authority(
                manifest,
                artifact,
                authority,
                artifact_ref,
                authority_ref,
                capability,
                prepared,
                as_of,
            )
        ):
            candidate_input["disposition"] = "ineligible"
            candidate_inputs.append(candidate_input)
            continue
        key = (manifest.source, manifest.feed, manifest.adjustment, manifest.delivery_mode)
        if key in seen_keys:
            candidate_input["disposition"] = "superseded_revision"
            candidate_inputs.append(candidate_input)
            continue
        seen_keys.add(key)
        payload = await reader(artifact.uri)
        series, parsed_ref = parse_price_series_artifact(
            payload,
            artifact,
            manifest,
            instrument_id,
        )
        if any(bar.available_at > as_of for bar in series.bars):
            candidate_input["disposition"] = "ineligible"
            candidate_inputs.append(candidate_input)
            continue
        binding = EvidenceBinding(
            available_at=max(
                artifact.available_at,
                policy_authority.ref.available_at,
                cast(datetime, manifest.provider_authorized_at),
                authority.available_at,
                authority_ref.available_at,
            ),
            as_of=max(bar.observed_at or bar.available_at for bar in series.bars),
            session=max(bar.session for bar in series.bars),
            refs=combine_refs(
                (parsed_ref, parsed_artifact_ref, parsed_authority_ref),
                (policy_authority.ref,),
            ),
        )
        candidate_input["parsed"] = True
        if selected is None:
            candidate_input["disposition"] = "selected"
            selected = (series, binding)
            selected_manifest_id = str(manifest.id)
            selected_key = key
        else:
            candidate_input["disposition"] = "superseded"
        candidate_inputs.append(candidate_input)

    if selected is None:
        return SeriesAuthority(
            selected=None,
            quality="FAIL",
            reasons=("NO_ELIGIBLE_SOURCE",),
            source_conflict=False,
            relevant_refs=combine_refs(tuple(relevant_refs)),
            canonical_input={
                "instrument_id": str(instrument_id),
                "selector": _selector_input(None, "FAIL", ("NO_ELIGIBLE_SOURCE",)),
                "selected_manifest_id": None,
                "candidates": tuple(candidate_inputs),
            },
        )
    return SeriesAuthority(
        selected=selected,
        quality="PASS",
        reasons=(),
        source_conflict=False,
        relevant_refs=combine_refs(tuple(relevant_refs)),
        canonical_input={
            "instrument_id": str(instrument_id),
            "selector": {
                "selected_identity": selected_key,
                "quality": "PASS",
                "reasons": (),
                "source_conflict": False,
                "policy_version": policy_authority.version,
            },
            "selected_manifest_id": selected_manifest_id,
            "candidates": tuple(candidate_inputs),
        },
    )
def _candidate_input(manifest: PriceSeriesManifest, artifact: DataArtifact) -> dict[str, object]:
    return {
        "manifest_id": str(manifest.id),
        "data_artifact_id": str(artifact.id),
        "manifest": {
            "instrument": manifest.instrument,
            "source": manifest.source,
            "feed": manifest.feed,
            "delivery_mode": manifest.delivery_mode,
            "adjustment": manifest.adjustment,
            "start_at": _canonical(manifest.start_at),
            "end_at": _canonical(manifest.end_at),
            "row_count": manifest.row_count,
            "created_at": _canonical(manifest.created_at),
        },
        "artifact": {
            "id": str(artifact.id),
            "sha256": artifact.sha256,
            "byte_size": artifact.byte_size,
            "mime_type": artifact.mime_type,
            "schema_version": artifact.schema_version,
            "source": artifact.source,
            "observed_at": _canonical(artifact.observed_at),
            "available_at": _canonical(artifact.available_at),
            "received_at": _canonical(artifact.received_at),
            "created_at": _canonical(artifact.created_at),
        },
        "parsed": False,
    }


def _candidate_input_v2(
    manifest: PriceSeriesManifest,
    artifact: DataArtifact,
    authority: PriceBarSetAuthority,
    capability: ProviderCapability,
) -> dict[str, object]:
    value = _candidate_input(manifest, artifact)
    value["manifest"] = {
        **cast(dict[str, object], value["manifest"]),
        "interval": manifest.interval,
        "provider_capability_id": str(manifest.provider_capability_id),
        "provider_status_id": str(manifest.provider_status_id),
        "provider_status_version": manifest.provider_status_version,
        "provider_contract_hash": manifest.provider_contract_hash,
        "provider_authorized_at": _canonical(cast(datetime, manifest.provider_authorized_at)),
    }
    value["price_bar_set_authority"] = {
        "id": str(authority.id),
        "source_policy_id": str(authority.source_policy_id),
        "hash_version": authority.hash_version,
        "policy_version": authority.policy_version,
        "content_hash": authority.content_hash,
        "available_at": _canonical(authority.available_at),
    }
    value["provider_capability"] = {
        "id": str(capability.id),
        "schema_version": capability.schema_version,
        "contract_hash": capability.contract_hash,
        "expires_at": _canonical(capability.expires_at),
    }
    return value


def _valid_v2_final_authority(
    manifest: PriceSeriesManifest,
    artifact: DataArtifact,
    authority: PriceBarSetAuthority,
    artifact_evidence: ResearchEvidenceRef,
    authority_evidence: ResearchEvidenceRef,
    capability: ProviderCapability,
    prepared: PriceBarSetPreparation,
    as_of: datetime,
) -> bool:
    authorized_at = manifest.provider_authorized_at
    return (
        authorized_at is not None
        and manifest.provider_capability_id == capability.id
        and manifest.provider_status_id is not None
        and manifest.provider_status_version is not None
        and manifest.provider_status_version > 0
        and manifest.provider_contract_hash == capability.contract_hash
        and capability.created_at <= authorized_at <= as_of
        and capability.expires_at > as_of
        and artifact_evidence.id == manifest.evidence_ref_id
        and artifact_evidence.source == artifact.source == manifest.source
        and artifact_evidence.provider_object_id == f"price-series-manifest:{manifest.id}"
        and artifact_evidence.revision == artifact.sha256
        and artifact_evidence.content_hash == artifact.sha256
        and artifact_evidence.byte_length == artifact.byte_size
        and artifact_evidence.available_at == artifact.available_at
        and authority.source_policy_id == SOURCE_POLICY_V2_ID
        and authority.hash_version == HASH_VERSION_V2
        and authority.policy_version == POLICY_VERSION_V2
        and authority.source == manifest.source == authority_evidence.source
        and authority.provider_object_id
        == authority_evidence.provider_object_id
        == f"price-bar-set:{manifest.id}"
        and authority.revision
        == authority_evidence.revision
        == prepared.content_hash
        and authority.content_hash
        == authority_evidence.content_hash
        == prepared.content_hash
        and authority.byte_length == authority_evidence.byte_length == prepared.byte_length
        and authority.bar_count == prepared.bar_count
        and authority.first_session == prepared.first_session
        and authority.last_session == prepared.last_session
        and authority.available_at
        == authority_evidence.available_at
        == prepared.available_at
    )


def _research_ref(record: ResearchEvidenceRef) -> EvidenceRef:
    return EvidenceRef(
        source=record.source,
        object_id=record.provider_object_id,
        available_at=record.available_at,
        location=record.location,
        content_hash=record.content_hash,
    )


def _unavailable_series(
    instrument_id: UUID,
    policy_ref: EvidenceRef,
    reason: str,
) -> SeriesAuthority:
    reasons = (reason,)
    return SeriesAuthority(
        selected=None,
        quality="FAIL",
        reasons=reasons,
        source_conflict=False,
        relevant_refs=(policy_ref,),
        canonical_input={
            "instrument_id": str(instrument_id),
            "selector": _selector_input(None, "FAIL", reasons),
            "selected_manifest_id": None,
            "candidates": (),
        },
    )


def _selector_input(
    selected: PriceSeries | None,
    quality: str,
    reasons: tuple[str, ...],
) -> dict[str, object]:
    return {
        "selected_identity": None if selected is None else _series_key(selected),
        "quality": quality,
        "reasons": reasons,
        "source_conflict": "SOURCE_CONFLICT" in reasons,
    }


def _artifact_ref(manifest: PriceSeriesManifest, artifact: DataArtifact) -> EvidenceRef:
    return EvidenceRef(
        source=artifact.source,
        object_id=f"price-series-manifest:{manifest.id}",
        available_at=artifact.available_at,
        location=f"db://data-artifacts/{artifact.id}",
        content_hash=artifact.sha256,
    )


def _series_key(series: PriceSeries) -> tuple[str, str, str, str]:
    return (series.source, series.feed, series.adjustment, series.delivery_mode)


def _canonical(value: datetime | date | Decimal | UUID) -> str:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)
