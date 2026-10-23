"""Append-only local operator designation and narrowly consumable review evidence."""

import json
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.research.accounting_adoption import (
    DESIGNATION_SCHEMA,
    DESIGNATION_SOURCE,
    SCHEMA,
    SOURCE,
    validate_designation,
)
from nexus.research.accounting_review import (
    ReviewMaterial,
    ReviewSubmission,
    validate_submission,
    verify_xbrl_contexts,
)
from nexus.research.accounting_review_storage import read_receipt, verify_database_scope


def payload_bytes(payload: dict[str, object]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


async def _clock_and_lock(session: AsyncSession, key: str) -> datetime:
    lock = int.from_bytes(sha256(key.encode()).digest()[:8], signed=True)
    await session.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": lock})
    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime):
        raise ValueError("ACCOUNTING_RECEIPT_TIME_INVALID")
    return now


async def register_designation(
    session: AsyncSession, submission_id: UUID, data: object
) -> DataSnapshot:
    """Trusted local operator attests the separately observed owner and report origin.

    Not exposed to research Agents/API and never inferred from opinion fields.
    The operator must have verified the owner instruction; bytes provide audit
    provenance, not authentication of arbitrary uploaded files.
    """
    receipt = await read_receipt(session, submission_id)
    if receipt is None:
        raise ValueError("ACCOUNTING_DESIGNATION_RECEIPT_MISSING")
    material = ReviewMaterial.model_validate(receipt.payload["material"])
    opinion = ReviewSubmission.model_validate(receipt.payload["submission"])
    designation = validate_designation(data, material=material, opinion=opinion)
    now = await _clock_and_lock(session, f"designation:{designation.designation_id}")
    payload: dict[str, object] = {
        "designation": designation.model_dump(mode="json"),
        "receipt_id": str(receipt.id),
        "owner_message_sha256": sha256(designation.owner_message_text.encode()).hexdigest(),
        "report_sha256": sha256(designation.report_text.encode()).hexdigest(),
    }
    digest = sha256(payload_bytes(payload)).hexdigest()
    existing = await session.scalar(
        select(DataSnapshot).where(
            DataSnapshot.source == DESIGNATION_SOURCE,
            DataSnapshot.provider_object_id == str(designation.designation_id),
        )
    )
    if existing is not None:
        if existing.payload != payload or existing.revision != digest:
            raise ValueError("ACCOUNTING_DESIGNATION_CONFLICT")
        return existing
    record = DataSnapshot(
        source=DESIGNATION_SOURCE,
        provider_object_id=str(designation.designation_id),
        revision=digest,
        schema_version=DESIGNATION_SCHEMA,
        payload=payload,
        observed_at=now,
        received_at=now,
        available_at=now,
        created_at=now,
    )
    session.add(record)
    await session.flush()
    return record


async def _designation(
    session: AsyncSession, designation_id: UUID, receipt: DataSnapshot, now: datetime
) -> DataSnapshot:
    record = await session.scalar(
        select(DataSnapshot).where(
            DataSnapshot.source == DESIGNATION_SOURCE,
            DataSnapshot.provider_object_id == str(designation_id),
            DataSnapshot.schema_version == DESIGNATION_SCHEMA,
        )
    )
    if record is None:
        raise ValueError("ACCOUNTING_DESIGNATION_MISSING")
    material = ReviewMaterial.model_validate(receipt.payload["material"])
    opinion = ReviewSubmission.model_validate(receipt.payload["submission"])
    designation = validate_designation(
        record.payload.get("designation"), material=material, opinion=opinion
    )
    if (
        designation.designation_id != designation_id
        or record.payload.get("receipt_id") != str(receipt.id)
        or record.revision != sha256(payload_bytes(record.payload)).hexdigest()
        or record.payload.get("owner_message_sha256")
        != sha256(designation.owner_message_text.encode()).hexdigest()
        or record.payload.get("report_sha256")
        != sha256(designation.report_text.encode()).hexdigest()
        or not receipt.created_at
        <= record.observed_at
        == record.received_at
        == record.available_at
        == record.created_at
        <= now
    ):
        raise ValueError("ACCOUNTING_DESIGNATION_INVALID")
    return record


async def adopt_review(
    session: AsyncSession, submission_id: UUID, designation_id: UUID, *, source_root: Path
) -> ResearchEvidenceRef:
    now = await _clock_and_lock(session, f"adoption:{submission_id}:{designation_id}")
    receipt = await read_receipt(session, submission_id)
    if receipt is None:
        raise ValueError("ACCOUNTING_DESIGNATION_RECEIPT_MISSING")
    authorization = await _designation(session, designation_id, receipt, now)
    material = ReviewMaterial.model_validate(receipt.payload["material"])
    validate_submission(
        receipt.payload["submission"], package=material, source_root=source_root, now=now
    )
    if verify_xbrl_contexts(material, source_root=source_root, now=now) != len(material.facts):
        raise ValueError("ACCOUNTING_CONTEXT_INCOMPLETE")
    await verify_database_scope(session, material)
    from nexus.research.accounting_source_proof import verify_adoption_sources

    await verify_adoption_sources(session, material, source_root=source_root, now=now)
    # No stronger conclusion is minted: unknown/adverse findings are also retained
    # as visible evidence, so a later favorable report cannot hide a conflict.
    payload: dict[str, object] = {
        "instrument_id": str(material.instrument_id),
        "receipt_id": str(receipt.id),
        "submission_id": str(submission_id),
        "designation_id": str(designation_id),
        "designation_sha256": authorization.revision,
        "material": receipt.payload["material"],
        "submission": receipt.payload["submission"],
        "contract": "as_disclosed_eps_v1",
    }
    raw = payload_bytes(payload)
    digest = sha256(raw).hexdigest()
    object_id = f"{submission_id}:{designation_id}"
    existing = await session.scalar(
        select(ResearchEvidenceRef).where(
            ResearchEvidenceRef.source == SOURCE,
            ResearchEvidenceRef.provider_object_id == object_id,
        )
    )
    if existing is not None:
        found = await read_adoptions(
            session, instrument_id=material.instrument_id, as_of=now, evidence_ids=(existing.id,)
        )
        if existing.content_hash != digest or found[existing.id] != payload:
            raise ValueError("ACCOUNTING_ADOPTION_CONFLICT")
        return existing
    # Registration is not available while original verification is in flight.
    # Take the DB clock after validation, never backdate to lock acquisition.
    registered_at = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(registered_at, datetime) or registered_at < now:
        raise ValueError("ACCOUNTING_RECEIPT_TIME_INVALID")
    await _designation(session, designation_id, receipt, registered_at)
    now = registered_at
    ref = ResearchEvidenceRef(
        source=SOURCE,
        provider_object_id=object_id,
        revision=digest,
        content_hash=digest,
        byte_length=len(raw),
        location=f"memory://sha256/{digest}",
        available_at=now,
        created_at=now,
    )
    session.add(ref)
    session.add(
        DataSnapshot(
            source=SOURCE,
            provider_object_id=object_id,
            revision=digest,
            schema_version=SCHEMA,
            payload=payload,
            observed_at=now,
            received_at=now,
            available_at=now,
            created_at=now,
        )
    )
    await session.flush()
    return ref


async def read_adoptions(
    session: AsyncSession,
    *,
    instrument_id: UUID,
    as_of: datetime,
    evidence_ids: tuple[UUID, ...] | None = None,
) -> dict[UUID, dict[str, object]]:
    query = (
        select(ResearchEvidenceRef, DataSnapshot)
        .join(
            DataSnapshot,
            (DataSnapshot.source == ResearchEvidenceRef.source)
            & (DataSnapshot.provider_object_id == ResearchEvidenceRef.provider_object_id)
            & (DataSnapshot.revision == ResearchEvidenceRef.revision),
        )
        .where(
            ResearchEvidenceRef.source == SOURCE,
            DataSnapshot.schema_version == SCHEMA,
            ResearchEvidenceRef.available_at <= as_of,
            ResearchEvidenceRef.created_at <= as_of,
            DataSnapshot.created_at <= as_of,
            DataSnapshot.available_at <= as_of,
        )
    )
    if evidence_ids is not None:
        if not evidence_ids:
            return {}
        query = query.where(ResearchEvidenceRef.id.in_(evidence_ids))
    else:
        query = query.where(DataSnapshot.payload["instrument_id"].astext == str(instrument_id))
    result: dict[UUID, dict[str, object]] = {}
    for ref, record in await session.execute(query.order_by(ResearchEvidenceRef.id)):
        try:
            material = ReviewMaterial.model_validate(record.payload["material"])
            opinion = ReviewSubmission.model_validate(record.payload["submission"])
            receipt = await read_receipt(session, opinion.submission_id)
            if receipt is None:
                raise ValueError("missing receipt")
            authorization = await _designation(
                session, UUID(str(record.payload["designation_id"])), receipt, as_of
            )
            raw = payload_bytes(record.payload)
            digest = sha256(raw).hexdigest()
            if (
                material.instrument_id != instrument_id
                or record.payload.get("instrument_id") != str(instrument_id)
                or record.payload.get("contract") != "as_disclosed_eps_v1"
                or record.payload.get("receipt_id") != str(receipt.id)
                or record.payload.get("submission_id") != str(opinion.submission_id)
                or record.payload.get("designation_sha256") != authorization.revision
                or record.payload["material"] != receipt.payload["material"]
                or record.payload["submission"] != receipt.payload["submission"]
                or ref.provider_object_id
                != f"{opinion.submission_id}:{authorization.provider_object_id}"
                or ref.content_hash != digest
                or ref.revision != digest
                or ref.byte_length != len(raw)
                or ref.location != f"memory://sha256/{digest}"
                or not authorization.created_at
                <= record.observed_at
                == record.received_at
                == record.available_at
                == record.created_at
                == ref.available_at
                == ref.created_at
                <= as_of
            ):
                raise ValueError("invalid binding")
            await verify_database_scope(session, material)
            result[ref.id] = record.payload
        except (ValueError, KeyError, TypeError) as exc:
            raise ValueError("ACCOUNTING_ADOPTION_INVALID") from exc
    if evidence_ids is not None and set(result) != set(evidence_ids):
        raise ValueError("ACCOUNTING_ADOPTION_INVALID")
    return result
