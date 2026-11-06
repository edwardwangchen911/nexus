"""Versioned listed-history metadata, never authority for document interpretation."""

import re
from datetime import UTC, date, datetime, time
from hashlib import sha256
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from nexus.research.sec_disclosure_index import (
    _ACCESSION,
    _FIELDS,
    MAX_ENTRIES,
    MAX_RAW_BYTES,
    MAX_RECENT_ROWS,
    SecDisclosureIndex,
    SecIndexEntry,
    _object,
    _timestamp,
)
from nexus.research.sec_index_storage import _receipt

PARSER: Literal["sec_listed_history_v2"] = "sec_listed_history_v2"
MAX_HISTORY_FILES = 1


class ListedFile(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True)
    name: str
    filingCount: int = Field(ge=0, le=MAX_RECENT_ROWS)
    filingFrom: str
    filingTo: str


class SecMetadataEntry(SecIndexEntry):
    # A TXT identifier is evidence of a filing, not permission to use the HTML reader.
    primary_document: str = Field(
        min_length=5,
        max_length=255,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*\.(?:html?|txt)$",
    )


class ListedHistoryIndex(SecDisclosureIndex):
    parser_version: Literal["sec_listed_history_v2"] = PARSER
    entries: tuple[SecMetadataEntry, ...] = Field(max_length=MAX_ENTRIES)
    recent_accessions: tuple[str, ...]
    listed_files: tuple[ListedFile, ...]
    coverage_received_at: datetime
    closing_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


def listed_files(raw: bytes, *, issuer_cik: str) -> tuple[ListedFile, ...] | None:
    if not 0 < len(raw) <= MAX_RAW_BYTES or not re.fullmatch(r"[0-9]{10}", issuer_cik):
        raise ValueError("SEC_INDEX_HISTORY_INVALID")
    doc = _object(raw)
    if type(doc.get("cik")) not in (int, str) or str(doc["cik"]).zfill(10) != issuer_cik:
        raise ValueError("SEC_INDEX_ISSUER_INVALID")
    filings = doc.get("filings")
    if not isinstance(filings, dict):
        raise ValueError("SEC_INDEX_HISTORY_INVALID")
    if "files" not in filings:
        return None
    values = filings["files"]
    if not isinstance(values, list) or len(values) > MAX_HISTORY_FILES:
        raise ValueError("SEC_INDEX_HISTORY_LIMIT")
    files = tuple(ListedFile.model_validate(x) for x in values)
    for item in files:
        if not re.fullmatch(rf"CIK{issuer_cik}-submissions-[0-9]{{3}}\.json", item.name):
            raise ValueError("SEC_INDEX_HISTORY_SOURCE_INVALID")
        try:
            first, last = date.fromisoformat(item.filingFrom), date.fromisoformat(item.filingTo)
        except ValueError:
            raise ValueError("SEC_INDEX_HISTORY_RANGE_INVALID") from None
        if (
            first > last
            or first.isoformat() != item.filingFrom
            or last.isoformat() != item.filingTo
        ):
            raise ValueError("SEC_INDEX_HISTORY_RANGE_INVALID")
    if len({x.name for x in files}) != len(files):
        raise ValueError("SEC_INDEX_HISTORY_SOURCE_INVALID")
    return files


def build_listed_index(
    *,
    parent: tuple[bytes, bytes],
    history: tuple[tuple[str, bytes, bytes], ...],
    closing: tuple[bytes, bytes],
    instrument_id: UUID,
    identity_snapshot_id: UUID,
    issuer_cik: str,
) -> ListedHistoryIndex:
    """Validate all original bytes; missing pieces remain partial, never truncated."""
    declarations = listed_files(parent[0], issuer_cik=issuer_cik)
    if (
        len(history) > MAX_HISTORY_FILES
        or sum(len(x) for pair in (parent, closing) for x in pair)
        + sum(len(raw) + len(receipt) for _, raw, receipt in history)
        > MAX_RAW_BYTES
    ):
        raise ValueError("SEC_INDEX_HISTORY_LIMIT")
    now = datetime.now(UTC)
    url = f"https://data.sec.gov/submissions/CIK{issuer_cik}.json"
    first = _receipt(*parent, source_url=url, now=now)
    last = _receipt(*closing, source_url=url, now=now)
    if last < first or parent[0] != closing[0]:
        raise ValueError("SEC_INDEX_HISTORY_PARENT_CHANGED")
    files = {x.name: x for x in declarations or ()}
    if len({x[0] for x in history}) != len(history) or any(x[0] not in files for x in history):
        raise ValueError("SEC_INDEX_HISTORY_SOURCE_INVALID")
    recent = _object(parent[0])["filings"].get("recent")
    collections: list[tuple[Any, datetime, ListedFile | None]] = [(recent, first, None)]
    for name, raw, proof in history:
        received = _receipt(
            raw, proof, source_url=f"https://data.sec.gov/submissions/{name}", now=now
        )
        if not first <= received <= last:
            raise ValueError("SEC_INDEX_HISTORY_RECEIPT_ORDER")
        collections.append((_object(raw), received, files[name]))
    seen: dict[str, tuple[str, ...]] = {}
    entries: list[SecMetadataEntry] = []
    accepted_times: list[datetime] = []
    recent_accessions: list[str] = []
    total = 0
    for group, received, declaration in collections:
        if not isinstance(group, dict) or any(not isinstance(group.get(k), list) for k in _FIELDS):
            raise ValueError("SEC_INDEX_RECENT_INVALID")
        arrays = [group[k] for k in _FIELDS]
        total += len(arrays[0])
        if len({len(a) for a in arrays}) != 1 or total > MAX_RECENT_ROWS:
            raise ValueError("SEC_INDEX_RECENT_INVALID")
        if declaration and len(arrays[0]) != declaration.filingCount:
            raise ValueError("SEC_INDEX_HISTORY_COUNT_INVALID")
        for row in zip(*arrays, strict=True):
            if not all(isinstance(v, str) and 0 < len(v) <= 255 for v in row[:4]) or not (
                isinstance(row[4], str) and len(row[4]) <= 255
            ):
                raise ValueError("SEC_INDEX_ROW_INVALID")
            accession, form, accepted, filed, primary = row
            if not _ACCESSION.fullmatch(accession):
                raise ValueError("SEC_INDEX_ROW_INVALID")
            accepted_at = _timestamp(accepted)
            filed_date = date.fromisoformat(filed)
            if filed_date.isoformat() != filed:
                raise ValueError("SEC_INDEX_FILED_DATE_INVALID")
            filed_at = datetime.combine(filed_date, time.min, tzinfo=UTC)
            if max(accepted_at, filed_at) > received:
                raise ValueError("SEC_INDEX_ROW_TIME_INVALID")
            if declaration and not declaration.filingFrom <= filed <= declaration.filingTo:
                raise ValueError("SEC_INDEX_HISTORY_RANGE_INVALID")
            if accession in seen:
                if seen[accession] != row:
                    raise ValueError("SEC_INDEX_ACCESSION_CONFLICT")
                continue
            seen[accession] = row
            accepted_times.append(accepted_at)
            if form in {"8-K", "8-K/A"}:
                entries.append(
                    SecMetadataEntry(
                        accession_number=accession,
                        form=form,
                        accepted_at=accepted_at,
                        filed_at=filed_at,
                        primary_document=primary,
                    )
                )
                if declaration is None:
                    recent_accessions.append(accession)
    return ListedHistoryIndex(
        instrument_id=instrument_id,
        identity_snapshot_id=identity_snapshot_id,
        issuer_cik=issuer_cik,
        received_at=last,
        source_url=url,
        raw_content_hash=sha256(parent[0]).hexdigest(),
        entries=tuple(sorted(entries, key=lambda x: (x.accepted_at, x.accession_number))),
        coverage_start_at=min(accepted_times, default=None),
        history_complete=declarations is not None and set(files) == {x[0] for x in history},
        recent_accessions=tuple(sorted(recent_accessions)),
        listed_files=declarations or (),
        coverage_received_at=first,
        closing_content_hash=sha256(closing[0]).hexdigest(),
    )
