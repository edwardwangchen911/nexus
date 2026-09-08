"""One public DEXHKUS vintage for read-only HKD cash scenario valuation."""

import csv
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from importlib.metadata import version
from io import StringIO
from urllib.parse import parse_qs, urlsplit
from uuid import UUID

import exchange_calendars as xcals  # type: ignore[import-untyped]
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from nexus.diagnostics.calendar_policy import load_xnys_calendar_adoption
from nexus.domain.evidence import EvidenceRef
from nexus.domain.time import require_utc
from nexus.jobs.provider_collection import RawEvidenceStore
from nexus.persistence.models.data import DataArtifact, DataSnapshot, MacroObservation
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.providers.fred import normalize_alfred_observation

_SCHEMA = "dexhkus_vintage_csv_v1"
_UNIT = "HKD per USD"


@dataclass(frozen=True)
class FxValuation:
    observation_id: UUID
    evidence_ref_id: UUID
    rate_hkd_per_usd: Decimal
    observed_on: date
    vintage: date
    available_at: datetime
    received_at: datetime
    raw_sha256: str
    evidence_refs: tuple[EvidenceRef, ...]

    def input_identity(self) -> dict[str, str]:
        return {
            "series": "DEXHKUS",
            "unit": _UNIT,
            "observation_id": str(self.observation_id),
            "evidence_ref_id": str(self.evidence_ref_id),
            "rate": str(self.rate_hkd_per_usd),
            "observed_on": self.observed_on.isoformat(),
            "vintage": self.vintage.isoformat(),
            "available_at": self.available_at.isoformat(),
            "received_at": self.received_at.isoformat(),
            "raw_sha256": self.raw_sha256,
        }


def parse_dexhkus_vintage(
    payload: bytes, url: str
) -> tuple[date, tuple[tuple[date, Decimal], ...]]:
    """Validate the publisher's vintage column and reject transformed/foreign series."""
    parsed = urlsplit(url)
    query = parse_qs(parsed.query, strict_parsing=True)
    if (
        parsed.scheme != "https"
        or parsed.netloc != "alfred.stlouisfed.org"
        or parsed.path != "/graph/alfredgraph.csv"
        or parsed.fragment
        or set(query) != {"id", "cosd", "coed", "vintage_date"}
        or any(len(values) != 1 for values in query.values())
        or query["id"] != ["DEXHKUS"]
    ):
        raise ValueError("unsupported FX source URL")
    vintage = date.fromisoformat(query["vintage_date"][0])
    start, end = date.fromisoformat(query["cosd"][0]), date.fromisoformat(query["coed"][0])
    if not start <= end <= vintage or (end - start).days > 31 or len(payload) > 65536:
        raise ValueError("FX vintage range is invalid")
    reader = csv.DictReader(StringIO(payload.decode("utf-8-sig")))
    column = f"DEXHKUS_{vintage:%Y%m%d}"
    if reader.fieldnames != ["observation_date", column]:
        raise ValueError("FX vintage column mismatch")
    observations: list[tuple[date, Decimal]] = []
    for row in reader:
        if set(row) != {"observation_date", column}:
            raise ValueError("FX CSV row is invalid")
        observed = date.fromisoformat(row["observation_date"])
        if not start <= observed <= end or any(prior == observed for prior, _ in observations):
            raise ValueError("FX observation date is invalid")
        try:
            value = Decimal(row[column])
        except (InvalidOperation, TypeError) as error:
            raise ValueError("FX rate must be a positive Decimal") from error
        if not value.is_finite() or value <= 0:
            raise ValueError("FX rate must be a positive Decimal")
        observations.append((observed, value))
    if not observations:
        raise ValueError("FX vintage is empty")
    return vintage, tuple(sorted(observations))


async def import_dexhkus_vintage(
    session: AsyncSession,
    *,
    payload: bytes,
    url: str,
    received_at: datetime,
    store: RawEvidenceStore,
) -> tuple[UUID, ...]:
    """Import already-fetched public bytes; no network fetch, Job, or order is created."""
    received_at = require_utc(received_at)
    vintage, observations = parse_dexhkus_vintage(payload, url)
    if vintage > received_at.date():
        raise ValueError("FX vintage is future-dated")
    adoption = await load_xnys_calendar_adoption(session, received_at)
    if version("exchange-calendars") != adoption.payload.package_version:
        raise ValueError("FX calendar runtime differs from adopted version")
    calendar = xcals.get_calendar("XNYS")
    next_day = calendar.date_to_session((vintage + timedelta(days=1)).isoformat(), direction="next")
    next_open = calendar.session_open(next_day).to_pydatetime().astimezone(UTC)
    source_available = max(next_open, adoption.created_at)
    if source_available > received_at:
        raise ValueError("FX vintage is not yet available")
    digest = sha256(payload).hexdigest()
    for observed, _ in observations:
        revision = f"public-{vintage}:{observed}"
        existing = await session.scalar(
            select(ResearchEvidenceRef).where(
                ResearchEvidenceRef.source == "fred",
                ResearchEvidenceRef.provider_object_id == f"DEXHKUS:{revision}",
                ResearchEvidenceRef.revision == revision,
            )
        )
        if existing is not None and existing.content_hash != digest:
            raise ValueError("FX vintage evidence conflict")
    location = await store.put_if_absent(key=f"evidence/fred/sha256/{digest}.csv", content=payload)
    calendar_payload = json.dumps(
        {
            "adoption": adoption.canonical_input(),
            "vintage": vintage.isoformat(),
            "next_session_open": next_open.isoformat(),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    calendar_hash = sha256(calendar_payload).hexdigest()
    calendar_location = await store.put_if_absent(
        key=f"evidence/calendar/sha256/{calendar_hash}.json",
        content=calendar_payload,
    )
    calendar_object = f"calendar:XNYS:{adoption.row_id}:{vintage}"
    calendar_ref = await session.scalar(
        select(ResearchEvidenceRef).where(
            ResearchEvidenceRef.source == "exchange_calendars",
            ResearchEvidenceRef.provider_object_id == calendar_object,
            ResearchEvidenceRef.revision == calendar_hash,
        )
    )
    if calendar_ref is None:
        calendar_ref = ResearchEvidenceRef(
            source="exchange_calendars",
            provider_object_id=calendar_object,
            revision=calendar_hash,
            available_at=adoption.created_at,
            location=calendar_location,
            content_hash=calendar_hash,
            byte_length=len(calendar_payload),
        )
        session.add(calendar_ref)
        await session.flush()
    artifact = await session.scalar(select(DataArtifact).where(DataArtifact.sha256 == digest))
    if artifact is None:
        artifact = DataArtifact(
            uri=location,
            sha256=digest,
            byte_size=len(payload),
            mime_type="text/csv",
            schema_version=_SCHEMA,
            source="fred",
            observed_at=datetime.combine(observations[-1][0], datetime.min.time(), UTC),
            available_at=source_available,
            received_at=received_at,
        )
        session.add(artifact)
        await session.flush()
    ids = []
    for observed, value in observations:
        revision = f"public-{vintage}:{observed}"
        object_id = f"DEXHKUS:{revision}"
        existing_row = await session.scalar(
            select(MacroObservation).where(
                MacroObservation.source == "fred",
                MacroObservation.provider_object_id == object_id,
                MacroObservation.revision == revision,
            )
        )
        if existing_row is not None:
            ids.append(existing_row.id)
            continue
        normalized = normalize_alfred_observation(
            {
                "date": observed.isoformat(),
                "realtime_start": vintage.isoformat(),
                "realtime_end": vintage.isoformat(),
                "value": str(value),
                "revision": revision,
            },
            series="DEXHKUS",
            unit=_UNIT,
            evidence_ref=EvidenceRef(
                source="fred",
                object_id=object_id,
                available_at=source_available,
                location=location,
                content_hash=digest,
            ),
            calendar_evidence_ref=EvidenceRef(
                source="exchange_calendars",
                object_id=calendar_object,
                available_at=adoption.created_at,
                location=calendar_location,
                content_hash=calendar_hash,
            ),
            next_session_open=lambda _: next_open,
        )
        evidence = ResearchEvidenceRef(
            source="fred",
            provider_object_id=object_id,
            revision=revision,
            available_at=source_available,
            location=location,
            content_hash=digest,
            byte_length=len(payload),
        )
        session.add(evidence)
        await session.flush()
        observation = MacroObservation(
            source="fred",
            series="DEXHKUS",
            observed_at=datetime.combine(observed, datetime.min.time(), UTC),
            available_at=source_available,
            revision=revision,
            value=normalized.value,
            unit=_UNIT,
            provider_object_id=object_id,
            authority_version=normalized.authority_version,
            real_time_start=vintage,
            real_time_end=vintage,
            evidence_ref_id=evidence.id,
            calendar_evidence_ref_id=calendar_ref.id,
            calendar_source=calendar_ref.source,
            calendar_provider_object_id=calendar_ref.provider_object_id,
            calendar_revision=calendar_ref.revision,
            display_sanity_only=True,
        )
        session.add(observation)
        await session.flush()
        ids.append(observation.id)
    receipt = await session.scalar(
        select(DataSnapshot).where(
            DataSnapshot.source == "fred",
            DataSnapshot.provider_object_id == f"public-fx:{digest}",
            DataSnapshot.revision == digest,
        )
    )
    if receipt is None:
        session.add(
            DataSnapshot(
                source="fred",
                provider_object_id=f"public-fx:{digest}",
                revision=digest,
                observed_at=artifact.observed_at,
                available_at=received_at,
                received_at=received_at,
                schema_version=_SCHEMA,
                payload={
                    "url": url,
                    "artifact_id": str(artifact.id),
                    "vintage": vintage.isoformat(),
                    "observation_ids": [str(i) for i in ids],
                },
            )
        )
    await session.flush()
    return tuple(ids)


async def load_dexhkus_valuation(
    session: AsyncSession,
    *,
    valuation_time: datetime,
    known_at: datetime,
) -> FxValuation | None:
    """A vintage may price an older snapshot only once this analysis has received it."""
    valuation_time, known_at = require_utc(valuation_time), require_utc(known_at)
    if known_at < valuation_time:
        return None
    calendar_ref = aliased(ResearchEvidenceRef)
    rows = (
        await session.execute(
            select(MacroObservation, ResearchEvidenceRef, calendar_ref, DataArtifact)
            .join(ResearchEvidenceRef, ResearchEvidenceRef.id == MacroObservation.evidence_ref_id)
            .join(calendar_ref, calendar_ref.id == MacroObservation.calendar_evidence_ref_id)
            .join(DataArtifact, DataArtifact.sha256 == ResearchEvidenceRef.content_hash)
            .where(
                MacroObservation.source == "fred",
                MacroObservation.series == "DEXHKUS",
                MacroObservation.authority_version == "research_macro_v1",
                MacroObservation.display_sanity_only.is_(True),
                MacroObservation.unit == _UNIT,
                MacroObservation.value > 0,
                MacroObservation.real_time_start <= valuation_time.date(),
                MacroObservation.observed_at <= valuation_time,
                MacroObservation.available_at <= valuation_time,
                MacroObservation.created_at <= known_at,
                ResearchEvidenceRef.available_at <= valuation_time,
                ResearchEvidenceRef.created_at <= known_at,
                calendar_ref.available_at <= valuation_time,
                calendar_ref.created_at <= known_at,
                DataArtifact.source == "fred",
                DataArtifact.schema_version == _SCHEMA,
                DataArtifact.available_at <= valuation_time,
                DataArtifact.received_at <= known_at,
                DataArtifact.created_at <= known_at,
                DataArtifact.uri == ResearchEvidenceRef.location,
                DataArtifact.byte_size == ResearchEvidenceRef.byte_length,
            )
            .order_by(MacroObservation.observed_at.desc(), MacroObservation.real_time_start.desc())
        )
    ).all()
    if not rows:
        return None
    row, evidence, calendar_evidence, artifact = rows[0]
    if row.real_time_start is None or any(
        other.observed_at == row.observed_at and other.real_time_start == row.real_time_start
        for other, _, _, _ in rows[1:]
    ):
        return None
    return FxValuation(
        row.id,
        evidence.id,
        row.value,
        row.observed_at.date(),
        row.real_time_start,
        row.available_at,
        artifact.received_at,
        artifact.sha256,
        (
            EvidenceRef(
                source=evidence.source,
                object_id=evidence.provider_object_id,
                available_at=evidence.available_at,
                location=evidence.location,
                content_hash=evidence.content_hash,
            ),
            EvidenceRef(
                source=calendar_evidence.source,
                object_id=calendar_evidence.provider_object_id,
                available_at=calendar_evidence.available_at,
                location=calendar_evidence.location,
                content_hash=calendar_evidence.content_hash,
            ),
        ),
    )
