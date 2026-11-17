"""Receipt-bound metadata with isolated row defects, never a rewrite of old indexes.

The normal durable importer must still verify the instrument/issuer identity and
archive originals. This parser cannot by itself grant notification or research.
"""

import re
from datetime import UTC, date, datetime, time
from hashlib import sha256
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

from nexus.research.sec_disclosure_index import (
    MAX_ENTRIES,
    MAX_RAW_BYTES,
    MAX_RECENT_ROWS,
    SecDisclosureIndex,
    SecIndexEntry,
    _object,
    _timestamp,
)
from nexus.research.sec_index_storage import _receipt

_FIELDS = ("accessionNumber", "form", "acceptanceDateTime", "filingDate", "primaryDocument")


class SecMetadataFact(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    accession_number: str = Field(pattern=r"^[0-9]{10}-[0-9]{2}-[0-9]{6}$")
    form: str = Field(min_length=1, max_length=32)
    accepted_at: AwareDatetime
    filed_at: AwareDatetime
    primary_document: str = Field(
        min_length=1, max_length=255, pattern=r"^[A-Za-z0-9][A-Za-z0-9._/-]*$"
    )

    @field_validator("primary_document")
    @classmethod
    def relative_document(cls, value: str) -> str:
        # Metadata for non-target SEC forms may use xsl/... paths. Preserve
        # those exact facts without granting a filing fetch or HTML-reader right.
        if any(part in ("", ".", "..") for part in value.split("/")):
            raise ValueError("SEC_DURABLE_ROW_INVALID")
        return value


class SecRowIssue(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    row_number: int = Field(ge=0, lt=MAX_RECENT_ROWS)
    accession_number: str | None
    reason: Literal["SEC_DURABLE_ROW_INVALID", "SEC_DURABLE_ROW_DUPLICATE"]


class DurableIndexEvidence(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    index: SecDisclosureIndex
    metadata_entries: tuple[SecMetadataFact, ...]
    issues: tuple[SecRowIssue, ...]
    raw_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


def coverage_details(raw: bytes, evidence: DurableIndexEvidence) -> dict[str, object]:
    """Explain this verified capture only; never infer completeness or eligibility."""
    if sha256(raw).hexdigest() != evidence.raw_content_hash:
        raise ValueError("SEC_DURABLE_SOURCE_INVALID")
    files = _object(raw).get("filings", {}).get("files")
    return {
        "unread_history_files": len(files) if isinstance(files, list) else None,
        "invalid_rows": len(evidence.issues),
        "coverage_start_known": evidence.index.coverage_start_at is not None,
    }


def build_durable_index(
    raw: bytes,
    receipt: bytes,
    *,
    instrument_id: UUID,
    identity_snapshot_id: UUID,
    issuer_cik: str,
    now: datetime,
) -> DurableIndexEvidence:
    if not 0 < len(raw) <= MAX_RAW_BYTES or not re.fullmatch(r"[0-9]{10}", issuer_cik):
        raise ValueError("SEC_DURABLE_SOURCE_INVALID")
    received = _receipt(
        raw, receipt, source_url=f"https://data.sec.gov/submissions/CIK{issuer_cik}.json", now=now
    )
    document = _object(raw)
    if type(document.get("cik")) not in (str, int) or str(document["cik"]).zfill(10) != issuer_cik:
        raise ValueError("SEC_DURABLE_ISSUER_INVALID")
    filings = document.get("filings")
    if not isinstance(filings, dict) or not isinstance(filings.get("recent"), dict):
        raise ValueError("SEC_DURABLE_SOURCE_INVALID")
    recent = filings["recent"]
    if any(not isinstance(recent.get(key), list) for key in _FIELDS):
        raise ValueError("SEC_DURABLE_SOURCE_INVALID")
    lengths = {len(recent[key]) for key in _FIELDS}
    if len(lengths) != 1 or next(iter(lengths)) > MAX_RECENT_ROWS:
        raise ValueError("SEC_DURABLE_SOURCE_INVALID")
    facts: dict[str, SecMetadataFact] = {}
    ambiguous: set[str] = set()
    issues: list[SecRowIssue] = []
    for number in range(next(iter(lengths))):
        accession = recent["accessionNumber"][number]
        key = (
            accession
            if isinstance(accession, str)
            and re.fullmatch(r"[0-9]{10}-[0-9]{2}-[0-9]{6}", accession)
            else None
        )
        try:
            accepted_raw = recent["acceptanceDateTime"][number]
            # Require second precision; a truncated minute/day cannot establish
            # which side of the rule's activation boundary a disclosure belongs to.
            if not isinstance(accepted_raw, str) or not re.fullmatch(
                r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?(?:Z|[+-][0-9]{2}:[0-9]{2})",
                accepted_raw,
            ):
                raise ValueError("SEC_DURABLE_ROW_INVALID")
            fact = SecMetadataFact(
                accession_number=accession,
                form=recent["form"][number],
                accepted_at=_timestamp(recent["acceptanceDateTime"][number]),
                filed_at=datetime.combine(
                    date.fromisoformat(recent["filingDate"][number]), time.min, UTC
                ),
                primary_document=recent["primaryDocument"][number],
            )
            if max(fact.accepted_at, fact.filed_at) > received:
                raise ValueError("SEC_DURABLE_ROW_INVALID")
        except (ValueError, TypeError):
            issues.append(
                SecRowIssue(
                    row_number=number, accession_number=key, reason="SEC_DURABLE_ROW_INVALID"
                )
            )
            if key is not None:
                ambiguous.add(key)
            continue
        if fact.accession_number in facts:
            ambiguous.add(fact.accession_number)
            issues.append(
                SecRowIssue(
                    row_number=number, accession_number=key, reason="SEC_DURABLE_ROW_DUPLICATE"
                )
            )
        else:
            facts[fact.accession_number] = fact
    metadata = tuple(value for key, value in facts.items() if key not in ambiguous)
    entries: list[SecIndexEntry] = []
    for fact in metadata:
        if fact.form not in ("8-K", "8-K/A"):
            continue
        try:
            entries.append(SecIndexEntry.model_validate(fact.model_dump()))
        except ValueError:
            issues.append(
                SecRowIssue(
                    row_number=list(facts).index(fact.accession_number),
                    accession_number=fact.accession_number,
                    reason="SEC_DURABLE_ROW_INVALID",
                )
            )
    if len(entries) > MAX_ENTRIES:
        raise ValueError("SEC_DURABLE_CAPACITY_UNAVAILABLE")
    content_hash = sha256(raw).hexdigest()
    index = SecDisclosureIndex(
        instrument_id=instrument_id,
        identity_snapshot_id=identity_snapshot_id,
        issuer_cik=issuer_cik,
        received_at=received,
        source_url=f"https://data.sec.gov/submissions/CIK{issuer_cik}.json",
        raw_content_hash=content_hash,
        entries=tuple(entries),
        coverage_start_at=min((e.accepted_at for e in entries), default=None),
        history_complete=filings.get("files") == [] and not issues,
    )
    return DurableIndexEvidence(
        index=index, metadata_entries=metadata, issues=tuple(issues), raw_content_hash=content_hash
    )
