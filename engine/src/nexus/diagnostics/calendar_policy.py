"""Immutable database adoption of the XNYS calendar ruleset."""

import json
from datetime import UTC, datetime
from hashlib import sha256
from typing import Literal, cast
from uuid import UUID

from pydantic import ConfigDict, ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.diagnostics.models import _DiagnosticModel
from nexus.domain.evidence import EvidenceRef
from nexus.domain.time import require_utc
from nexus.persistence.models.diagnostics import DiagnosticPolicy

XNYS_CALENDAR_ADOPTION_ID = UUID("4f717aca-5b62-5468-a53c-6dd93763434c")
XNYS_CALENDAR_ADOPTION_KEY = "xnys_calendar_adoption"
XNYS_CALENDAR_ADOPTION_VERSION = "xnys_exchange_calendars_v1"
XNYS_CALENDAR_PACKAGE_VERSION = "4.13.2"
XNYS_CALENDAR_ADOPTION_PAYLOAD: dict[str, str] = {
    "calendar": "XNYS",
    "provider": "exchange-calendars",
    "package": "exchange-calendars",
    "package_version": XNYS_CALENDAR_PACKAGE_VERSION,
    "ruleset_version": "xnys_ruleset_v1",
    "effective_from": "2026-01-01T00:00:00Z",
}


class XnysCalendarAdoptionPayload(_DiagnosticModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    calendar: Literal["XNYS"]
    provider: Literal["exchange-calendars"]
    package: Literal["exchange-calendars"]
    package_version: Literal["4.13.2"]
    ruleset_version: Literal["xnys_ruleset_v1"]
    effective_from: datetime


class XnysCalendarAdoption(_DiagnosticModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    row_id: UUID
    key: Literal["xnys_calendar_adoption"]
    version: Literal["xnys_exchange_calendars_v1"]
    schema_version: Literal["1"]
    payload: XnysCalendarAdoptionPayload
    created_at: datetime
    content_hash: str

    @property
    def ref(self) -> EvidenceRef:
        return EvidenceRef(
            source="nexus_db",
            object_id=f"calendar-adoption:{self.row_id}:{self.version}",
            available_at=self.created_at,
            location=f"db://diagnostic-policies/{self.row_id}",
            content_hash=self.content_hash,
        )

    def canonical_input(self) -> dict[str, object]:
        return {
            "row_id": str(self.row_id),
            "key": self.key,
            "version": self.version,
            "schema_version": self.schema_version,
            "payload": self.payload.model_dump(mode="json"),
            "created_at": _utc_string(self.created_at),
            "content_hash": self.content_hash,
        }


async def load_xnys_calendar_adoption(
    session: AsyncSession, snapshot_as_of: datetime
) -> XnysCalendarAdoption:
    record = await session.scalar(
        select(DiagnosticPolicy).where(
            DiagnosticPolicy.key == XNYS_CALENDAR_ADOPTION_KEY,
            DiagnosticPolicy.version == XNYS_CALENDAR_ADOPTION_VERSION,
        )
    )
    return validate_xnys_calendar_adoption(record, snapshot_as_of)


def validate_xnys_calendar_adoption(
    record: DiagnosticPolicy | None, snapshot_as_of: datetime
) -> XnysCalendarAdoption:
    snapshot_utc = require_utc(snapshot_as_of)
    if record is None:
        raise ValueError("published XNYS calendar adoption is missing")
    if record.created_at > snapshot_utc:
        raise ValueError("published XNYS calendar adoption is future-created")
    if (
        record.id != XNYS_CALENDAR_ADOPTION_ID
        or record.key != XNYS_CALENDAR_ADOPTION_KEY
        or record.version != XNYS_CALENDAR_ADOPTION_VERSION
        or record.schema_version != "1"
        or record.payload != XNYS_CALENDAR_ADOPTION_PAYLOAD
    ):
        raise ValueError("published XNYS calendar adoption is invalid")
    try:
        payload = XnysCalendarAdoptionPayload.model_validate(record.payload)
    except ValidationError as error:
        raise ValueError("published XNYS calendar adoption is invalid") from error
    if payload.effective_from > snapshot_utc:
        raise ValueError("published XNYS calendar adoption is not effective")
    canonical = {
        "row_id": str(record.id),
        "key": record.key,
        "version": record.version,
        "schema_version": record.schema_version,
        "payload": record.payload,
        "created_at": _utc_string(record.created_at),
    }
    content_hash = sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return XnysCalendarAdoption(
        row_id=record.id,
        key=cast(Literal["xnys_calendar_adoption"], record.key),
        version=cast(Literal["xnys_exchange_calendars_v1"], record.version),
        schema_version=cast(Literal["1"], record.schema_version),
        payload=payload,
        created_at=record.created_at,
        content_hash=content_hash,
    )


def _utc_string(value: datetime) -> str:
    return require_utc(value).astimezone(UTC).isoformat().replace("+00:00", "Z")
