"""Point-in-time source-policy selection with complete safe provenance."""

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.diagnostics.artifacts import ArtifactByteReader, parse_price_series_artifact
from nexus.diagnostics.evidence import combine_refs
from nexus.diagnostics.models import EvidenceBinding
from nexus.domain.evidence import EvidenceRef
from nexus.persistence.models.data import DataArtifact, PriceSeriesManifest
from nexus.persistence.models.market import Instrument
from nexus.providers.base import PriceSeries
from nexus.quality.source_policy import select_price_series

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


async def select_authoritative_series(
    session: AsyncSession,
    instrument_id: UUID,
    as_of: datetime,
    reader: ArtifactByteReader,
    policy_ref: EvidenceRef,
) -> SeriesAuthority:
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
        individual = await select_price_series(session, (series,), instrument.tick_size, as_of)
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
    selection = await select_price_series(session, candidates, instrument.tick_size, as_of)
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
