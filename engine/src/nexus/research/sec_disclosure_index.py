"""Receipt-bound SEC filing metadata; no judgment about financial effects or significance."""

import json
import re
from datetime import UTC, date, datetime, time
from hashlib import sha256
from typing import Any, Literal, Self
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

MAX_RAW_BYTES = 8 * 1024 * 1024
MAX_RECENT_ROWS = 10000
MAX_ENTRIES = 1000
_FIELDS = ("accessionNumber", "form", "acceptanceDateTime", "filingDate", "primaryDocument")
_RECEIPT_KEYS = {"source_url", "received_at", "sha256", "byte_length", "http_status"}
_ACCESSION = re.compile(r"[0-9]{10}-[0-9]{2}-[0-9]{6}")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("SEC_INDEX_TIME_INVALID")
    return value.astimezone(UTC)


class SecIndexEntry(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    accession_number: str = Field(pattern=r"^[0-9]{10}-[0-9]{2}-[0-9]{6}$")
    form: Literal["8-K", "8-K/A"]
    accepted_at: AwareDatetime
    filed_at: AwareDatetime
    primary_document: str = Field(
        min_length=5, max_length=255, pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*\.html?$"
    )

    @field_validator("accepted_at", "filed_at")
    @classmethod
    def _timestamps(cls, value: datetime) -> datetime:
        return _utc(value)

    @field_validator("filed_at")
    @classmethod
    def _midnight(cls, value: datetime) -> datetime:
        if value.time() != time.min:
            raise ValueError("SEC_INDEX_FILED_DATE_INVALID")
        return value


class SecDisclosureIndex(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    instrument_id: UUID
    identity_snapshot_id: UUID
    issuer_cik: str = Field(pattern=r"^[0-9]{10}$")
    received_at: AwareDatetime
    source_url: str = Field(max_length=128)
    raw_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    entries: tuple[SecIndexEntry, ...] = Field(max_length=MAX_ENTRIES)
    coverage_start_at: AwareDatetime | None
    history_complete: bool = Field(strict=True)

    @field_validator("received_at", "coverage_start_at")
    @classmethod
    def _timestamps(cls, value: datetime | None) -> datetime | None:
        return None if value is None else _utc(value)

    @model_validator(mode="after")
    def _bindings(self) -> Self:
        if (
            int(self.issuer_cik) == 0
            or self.source_url != f"https://data.sec.gov/submissions/CIK{self.issuer_cik}.json"
            or self.received_at > datetime.now(UTC)
            or len({item.accession_number for item in self.entries}) != len(self.entries)
            or (self.entries and self.coverage_start_at is None)
            or (self.coverage_start_at is not None and self.coverage_start_at > self.received_at)
            or any(
                max(item.accepted_at, item.filed_at) > self.received_at
                or (
                    self.coverage_start_at is not None and item.accepted_at < self.coverage_start_at
                )
                for item in self.entries
            )
        ):
            raise ValueError("SEC_INDEX_BINDING_INVALID")
        return self

    def canonical_bytes(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()


def _object(raw: bytes) -> dict[str, Any]:
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("SEC_INDEX_JSON_DUPLICATE_KEY")
            result[key] = value
        return result

    def nonfinite(value: str) -> None:
        raise ValueError("SEC_INDEX_JSON_INVALID")

    try:
        document = json.loads(raw, object_pairs_hook=unique, parse_constant=nonfinite)
    except (ValueError, UnicodeError, RecursionError):
        raise ValueError("SEC_INDEX_JSON_INVALID") from None
    if not isinstance(document, dict):
        raise ValueError("SEC_INDEX_JSON_INVALID")
    return document


def _timestamp(value: object) -> datetime:
    if not isinstance(value, str) or len(value) > 40 or "T" not in value:
        raise ValueError("SEC_INDEX_TIME_INVALID")
    try:
        return _utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError:
        raise ValueError("SEC_INDEX_TIME_INVALID") from None


def build_disclosure_index(
    raw: bytes,
    receipt: bytes,
    *,
    instrument_id: UUID,
    identity_snapshot_id: UUID,
    issuer_cik: str,
) -> SecDisclosureIndex:
    """Validate exact source rows and local receipt without collecting or writing anything."""
    if not 0 < len(raw) <= MAX_RAW_BYTES or not 0 < len(receipt) <= 8192:
        raise ValueError("SEC_INDEX_ORIGINAL_SIZE_INVALID")
    if not isinstance(issuer_cik, str) or not re.fullmatch(r"[0-9]{10}", issuer_cik):
        raise ValueError("SEC_INDEX_ISSUER_INVALID")
    proof = _object(receipt)
    if set(proof) not in (_RECEIPT_KEYS, _RECEIPT_KEYS | {"schema"}) or (
        "schema" in proof and proof["schema"] != "sec_identity_source_receipt_v1"
    ):
        raise ValueError("SEC_INDEX_RECEIPT_INVALID")
    url = f"https://data.sec.gov/submissions/CIK{issuer_cik}.json"
    digest = sha256(raw).hexdigest()
    if (
        proof["source_url"] != url
        or proof["sha256"] != digest
        or type(proof["byte_length"]) is not int
        or proof["byte_length"] != len(raw)
        or type(proof["http_status"]) is not int
        or proof["http_status"] != 200
    ):
        raise ValueError("SEC_INDEX_RECEIPT_INVALID")
    received_at = _timestamp(proof["received_at"])
    if received_at > datetime.now(UTC):
        raise ValueError("SEC_INDEX_RECEIPT_TIME_INVALID")
    document = _object(raw)
    cik = document.get("cik")
    if (
        type(cik) not in (str, int)
        or not re.fullmatch(r"[0-9]{1,10}", str(cik))
        or str(cik).zfill(10) != issuer_cik
        or int(issuer_cik) == 0
    ):
        raise ValueError("SEC_INDEX_ISSUER_INVALID")
    filings = document.get("filings")
    recent = filings.get("recent") if isinstance(filings, dict) else None
    if not isinstance(recent, dict) or any(
        not isinstance(recent.get(key), list) for key in _FIELDS
    ):
        raise ValueError("SEC_INDEX_RECENT_INVALID")
    arrays = [recent[key] for key in _FIELDS]
    if len({len(array) for array in arrays}) != 1 or len(arrays[0]) > MAX_RECENT_ROWS:
        raise ValueError("SEC_INDEX_RECENT_INVALID")
    assert isinstance(filings, dict)
    files = filings.get("files")
    if "files" in filings and not isinstance(files, list):
        raise ValueError("SEC_INDEX_HISTORY_INVALID")
    seen: dict[str, tuple[str, ...]] = {}
    entries: list[SecIndexEntry] = []
    accepted_times: list[datetime] = []
    for row in zip(*arrays, strict=True):
        if not all(isinstance(value, str) and 0 < len(value) <= 255 for value in row):
            raise ValueError("SEC_INDEX_ROW_INVALID")
        accession, form, accepted, filed, primary = row
        if not _ACCESSION.fullmatch(accession) or not re.fullmatch(
            r"[0-9]{4}-[0-9]{2}-[0-9]{2}", filed
        ):
            raise ValueError("SEC_INDEX_ROW_INVALID")
        accepted_at = _timestamp(accepted)
        try:
            filed_at = datetime.combine(date.fromisoformat(filed), time.min, tzinfo=UTC)
        except ValueError:
            raise ValueError("SEC_INDEX_FILED_DATE_INVALID") from None
        if max(accepted_at, filed_at) > received_at:
            raise ValueError("SEC_INDEX_ROW_TIME_INVALID")
        if accession in seen:
            if seen[accession] != row:
                raise ValueError("SEC_INDEX_ACCESSION_CONFLICT")
            continue
        seen[accession] = row
        accepted_times.append(accepted_at)
        if form in {"8-K", "8-K/A"}:
            entries.append(
                SecIndexEntry(
                    accession_number=accession,
                    form=form,
                    accepted_at=accepted_at,
                    filed_at=filed_at,
                    primary_document=primary,
                )
            )
    return SecDisclosureIndex(
        instrument_id=instrument_id,
        identity_snapshot_id=identity_snapshot_id,
        issuer_cik=issuer_cik,
        received_at=received_at,
        source_url=url,
        raw_content_hash=digest,
        entries=tuple(entries),
        coverage_start_at=min(accepted_times) if accepted_times else None,
        history_complete=files == [],
    )
