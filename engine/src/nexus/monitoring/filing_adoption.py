"""One owner-authorized graphic judgment, recorded by the trusted local operator.

The assistant acting on an observed user instruction is the operator. This is
not cryptographic human authentication or isolation from database administrators.
No collector, investment Agent, BFF, or HTTP route can designate a reviewer.
"""

import json
from datetime import datetime
from hashlib import sha256
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.domain.time import require_utc
from nexus.monitoring.filing_material import rebuild_material
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.runtime import AuditEvent
from nexus.persistence.models.sec_monitor import SecMonitorRule

SOURCE = "nexus_sec_graphic_adoption"
SCHEMA = "sec_graphic_adoption_v1"
ACTION = "research.sec.graphic_adopted"
ACTOR = "local-owner-operator"
ISSUER_CIK = "0001045810"
ACCESSION = "0001045810-26-000088"
GRAPHIC_FILENAME = "nvdalogoa19.jpg"
OWNER_SHA256 = "0dd184b199415fe361af5ab2823baaccdd35359fba34b5aacf8667a3a1af48a3"
OBJECT_ID = "0001045810:0001045810-26-000088:nvdalogoa19.jpg"
MATERIAL_SHA256 = "1ee097fa875484f5a51b8df1740c4c3aa83a521b46fd3bfcd84be9bfc91d32c3"
REPORT_SHA256 = "37992ce996ac13cdbd2dec11fe852fdd11f04b6b670282d8d7396b8b414840c6"
GRAPHIC_SHA256 = "578e5181e5d55b715e64c2e3e53fed5b6cbd2c512f6422105769f0f06056ed32"
ERROR = "SEC_GRAPHIC_ADOPTION_INVALID"


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


class GraphicDesignation(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)
    material: dict[str, object]
    report_text: str = Field(min_length=20, max_length=200000)
    report_source: str = Field(min_length=8, max_length=512)
    owner_message_text: str = Field(min_length=8, max_length=20000)
    owner_message_source: str = Field(min_length=8, max_length=512)
    purpose: Literal["nvda_000088_header_graphic_only"]
    graphic_filename: str
    graphic_sha256: str
    research_qualified: Literal[False] = False


def validate_designation(value: object) -> GraphicDesignation:
    result = GraphicDesignation.model_validate(value)
    entry = result.material.get("entry")
    if (
        sha256(canonical(result.material)).hexdigest() != MATERIAL_SHA256
        or sha256(result.report_text.encode()).hexdigest() != REPORT_SHA256
        or sha256(result.owner_message_text.encode()).hexdigest() != OWNER_SHA256
        or result.graphic_filename != GRAPHIC_FILENAME
        or result.graphic_sha256 != GRAPHIC_SHA256
        or result.material.get("issuer_cik") != ISSUER_CIK
        or not isinstance(entry, dict)
        or entry.get("accession_number") != ACCESSION
    ):
        raise ValueError(ERROR)
    return result


async def verify_material(
    db: AsyncSession, designation: GraphicDesignation, *, account_id: UUID, as_of: datetime
) -> UUID:
    material = designation.material
    prepared = require_utc(datetime.fromisoformat(str(material["prepared_at"])))
    if prepared > as_of or material.get("database_schema") not in {
        "0134_sec_foundation",
        "0135_routine_budget",
        "0136_sec_adopted_content",
        "0137_linear_json_canonical",
    }:
        raise ValueError(ERROR)
    snapshot_id = UUID(str(material["original_snapshot_id"]))
    rebuilt, _ = await rebuild_material(
        db, snapshot_id=snapshot_id, prepared_at=prepared, schema=str(material["database_schema"])
    )
    if canonical(rebuilt) != canonical(material):
        raise ValueError(ERROR)
    from nexus.monitoring.filing_originals import read_originals

    original = await read_originals(db, snapshot_id=snapshot_id, as_of=as_of)
    if (
        await db.scalar(
            select(SecMonitorRule.id).where(
                SecMonitorRule.account_id == account_id,
                SecMonitorRule.instrument_id == original.instrument_id,
                SecMonitorRule.issuer_cik == original.issuer_cik,
            )
        )
        is None
    ):
        raise ValueError("SEC_DURABLE_SCOPE_INVALID")
    return original.instrument_id


async def read_adoption(
    db: AsyncSession, *, snapshot_id: UUID, account_id: UUID, as_of: datetime
) -> GraphicDesignation:
    result = await read_adoption_record(db, snapshot_id=snapshot_id, as_of=as_of)
    await verify_material(db, result, account_id=account_id, as_of=as_of)
    return result


async def read_adoption_record(
    db: AsyncSession, *, snapshot_id: UUID, as_of: datetime
) -> GraphicDesignation:
    """Verify the authority fact; consumers must additionally prove all material.

    This is not sufficient content eligibility and is not an operator endpoint.
    """
    as_of = require_utc(as_of)
    row = await db.get(DataSnapshot, snapshot_id)
    if (
        row is None
        or row.source != SOURCE
        or row.schema_version != SCHEMA
        or row.provider_object_id != OBJECT_ID
        or not row.observed_at == row.received_at == row.available_at == row.created_at <= as_of
        or row.revision != sha256(canonical(row.payload)).hexdigest()
    ):
        raise ValueError(ERROR)
    result = validate_designation(row.payload)
    anchors = list(
        (
            await db.scalars(
                select(AuditEvent).where(
                    AuditEvent.action == ACTION,
                    AuditEvent.payload["snapshot_id"].astext == str(row.id),
                )
            )
        ).all()
    )
    if len(anchors) != 1:
        raise ValueError(ERROR)
    anchor = anchors[0]
    if (
        anchor.actor_id != ACTOR
        or anchor.schema_version != SCHEMA
        or anchor.occurred_at != row.created_at
        or anchor.created_at > as_of
        or anchor.payload
        != {"snapshot_id": str(row.id), "canonical_hash": row.revision, "purpose": result.purpose}
    ):
        raise ValueError(ERROR)
    return result


async def register_adoption(
    db: AsyncSession, value: object, *, account_id: UUID
) -> tuple[DataSnapshot, bool]:
    result = validate_designation(value)
    await db.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key,0))"),
        {"key": "sec-graphic-adoption:" + OBJECT_ID},
    )
    now = require_utc(await db.scalar(text("SELECT clock_timestamp()")))
    await verify_material(db, result, account_id=account_id, as_of=now)
    payload = result.model_dump(mode="json")
    digest = sha256(canonical(payload)).hexdigest()
    existing = list(
        (
            await db.scalars(
                select(DataSnapshot).where(
                    DataSnapshot.source == SOURCE, DataSnapshot.provider_object_id == OBJECT_ID
                )
            )
        ).all()
    )
    if existing:
        if len(existing) != 1 or existing[0].revision != digest or existing[0].payload != payload:
            raise ValueError("SEC_GRAPHIC_ADOPTION_CONFLICT")
        await read_adoption(db, snapshot_id=existing[0].id, account_id=account_id, as_of=now)
        return existing[0], False
    # Availability begins after complete verification, never at source/export time.
    now = require_utc(await db.scalar(text("SELECT clock_timestamp()")))
    row = DataSnapshot(
        source=SOURCE,
        provider_object_id=OBJECT_ID,
        revision=digest,
        schema_version=SCHEMA,
        payload=payload,
        observed_at=now,
        received_at=now,
        available_at=now,
        created_at=now,
    )
    db.add(row)
    await db.flush()
    await append_audit_event(
        db,
        actor_id=ACTOR,
        action=ACTION,
        schema_version=SCHEMA,
        occurred_at=now,
        payload={"snapshot_id": str(row.id), "canonical_hash": digest, "purpose": result.purpose},
    )
    checked_at = require_utc(await db.scalar(text("SELECT clock_timestamp()")))
    await read_adoption(db, snapshot_id=row.id, account_id=account_id, as_of=checked_at)
    return row, True
