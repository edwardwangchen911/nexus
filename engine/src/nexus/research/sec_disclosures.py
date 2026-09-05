"""Bounded SEC 8-K/6-K/497K text; never synthesize accounting facts from prose."""

import hashlib
import json
import re
from datetime import datetime
from html.parser import HTMLParser
from typing import Literal, Self
from uuid import UUID

from botocore.exceptions import BotoCoreError, ClientError  # type: ignore[import-untyped]
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError, model_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.research import Filing, ResearchEvidenceRef

MAX_EXCERPT = 12000
SCHEMA_VERSION = "sec_disclosure_v1"
SEC_RESEARCH_FORMS = ("10-K", "10-Q", "8-K", "6-K", "497K")


class SecFundIdentity(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    cik: str = Field(pattern=r"^[0-9]{10}$")
    series_id: str = Field(pattern=r"^S[0-9]{9}$")
    class_id: str = Field(pattern=r"^C[0-9]{9}$")
    symbol: str = Field(pattern=r"^[A-Z][A-Z0-9.-]{0,14}$")
    filing_index_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class SecDisclosure(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    instrument_id: UUID
    form: Literal["8-K", "6-K", "497K"]
    accession_number: str = Field(pattern=r"^[0-9]{10}-[0-9]{2}-[0-9]{6}$")
    source_url: str = Field(
        max_length=512,
        pattern=r"^https://www\.sec\.gov/Archives/edgar/data/[0-9]+/[0-9]{18}/[A-Za-z0-9][A-Za-z0-9._-]*\.htm[l]?$",
    )
    filed_at: AwareDatetime
    accepted_at: AwareDatetime
    raw_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    excerpt: str = Field(min_length=20, max_length=MAX_EXCERPT)
    truncated: bool
    fund_identity: SecFundIdentity | None = None
    issuer_cik: str | None = Field(default=None, pattern=r"^[0-9]{10}$")

    @model_validator(mode="after")
    def check_binding(self) -> Self:
        if self.source_url.split("/")[-2] != self.accession_number.replace("-", ""):
            raise ValueError("SEC_DISCLOSURE_ACCESSION_MISMATCH")
        if self.form == "8-K" and self.issuer_cik is None:
            raise ValueError("SEC_DISCLOSURE_ISSUER_REQUIRED")
        if self.issuer_cik is not None and (
            self.form != "8-K" or self.source_url.split("/")[-3] != str(int(self.issuer_cik))
        ):
            raise ValueError("SEC_DISCLOSURE_ISSUER_MISMATCH")
        if (self.form == "497K") != (self.fund_identity is not None):
            raise ValueError("SEC_DISCLOSURE_FUND_IDENTITY_REQUIRED")
        if self.fund_identity and self.source_url.split("/")[-3] != str(
            int(self.fund_identity.cik)
        ):
            raise ValueError("SEC_DISCLOSURE_FUND_CIK_MISMATCH")
        return self

    def canonical_bytes(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json", exclude_none=True), sort_keys=True, separators=(",", ":")
        ).encode()


class _VisibleText(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.stack: list[tuple[str, bool]] = []

    @property
    def hidden(self) -> bool:
        return bool(self.stack and self.stack[-1][1])

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = dict(attrs)
        hidden = self.hidden or (
            tag in {"head", "script", "style", "ix:header"}
            or "hidden" in attributes
            or attributes.get("aria-hidden") == "true"
            or bool(
                re.search(
                    r"display\s*:\s*none|visibility\s*:\s*hidden",
                    attributes.get("style") or "",
                    re.I,
                )
            )
        )
        if tag not in {
            "area",
            "base",
            "br",
            "col",
            "embed",
            "hr",
            "img",
            "input",
            "link",
            "meta",
            "param",
            "source",
            "track",
            "wbr",
        }:
            self.stack.append((tag, hidden))
        if not hidden and tag in {"p", "div", "tr", "br", "h1", "h2", "h3"}:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        for index in range(len(self.stack) - 1, -1, -1):
            if self.stack[index][0] == tag:
                del self.stack[index:]
                break
        if not self.hidden and tag in {"p", "div", "tr", "h1", "h2", "h3"}:
            self.parts.append("\n")

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        depth = len(self.stack)
        self.handle_starttag(tag, attrs)
        del self.stack[depth:]

    def handle_data(self, data: str) -> None:
        if not self.hidden:
            self.parts.append(data + " ")


def disclosure_excerpt(raw: bytes) -> tuple[str, bool]:
    if not raw or len(raw) > 8 * 1024 * 1024:
        raise ValueError("SEC_DISCLOSURE_SIZE_INVALID")
    parser = _VisibleText()
    parser.feed(raw.decode("utf-8-sig"))
    parser.close()
    text = "\n".join(
        line for part in "".join(parser.parts).splitlines() if (line := " ".join(part.split()))
    )
    if len(text) < 20:
        raise ValueError("SEC_DISCLOSURE_TEXT_UNAVAILABLE")
    return text[:MAX_EXCERPT], len(text) > MAX_EXCERPT


async def persist_disclosure(
    session: AsyncSession,
    document: SecDisclosure,
    *,
    location: str,
    received_at: datetime,
) -> bool:
    if received_at.tzinfo is None or max(document.accepted_at, document.filed_at) > received_at:
        raise ValueError("SEC_DISCLOSURE_RECEIPT_TIME_INVALID")
    content = document.canonical_bytes()
    digest = hashlib.sha256(content).hexdigest()
    existing = await session.scalar(
        select(ResearchEvidenceRef).where(
            ResearchEvidenceRef.source == "sec",
            ResearchEvidenceRef.provider_object_id == document.accession_number,
            ResearchEvidenceRef.revision == digest,
        )
    )
    if existing is not None:
        if existing.content_hash != digest:
            raise ValueError("SEC_DISCLOSURE_HASH_CONFLICT")
        return False
    # SEC acceptance is not first public availability. New 8-K evidence is usable
    # only from actual receipt; legacy 6-K/497K bindings remain byte-compatible.
    available_at = received_at if document.form == "8-K" else document.accepted_at
    evidence = ResearchEvidenceRef(
        source="sec",
        provider_object_id=document.accession_number,
        revision=digest,
        available_at=available_at,
        location=location,
        content_hash=digest,
        byte_length=len(content),
    )
    session.add(evidence)
    await session.flush()
    session.add(
        Filing(
            instrument_id=document.instrument_id,
            evidence_ref_id=evidence.id,
            source="sec",
            provider_object_id=document.accession_number,
            revision=digest,
            accession_number=document.accession_number,
            form=document.form,
            filed_at=document.filed_at,
            accepted_at=document.accepted_at,
        )
    )
    session.add(
        DataSnapshot(
            source="sec",
            provider_object_id=document.accession_number,
            revision=digest,
            observed_at=document.accepted_at,
            available_at=available_at,
            received_at=received_at,
            schema_version=SCHEMA_VERSION,
            payload=document.model_dump(mode="json"),
        )
    )
    await session.flush()
    return True


async def read_disclosures(
    session: AsyncSession,
    *,
    instrument_id: UUID,
    evidence_ids: tuple[UUID, ...],
    as_of: datetime,
) -> dict[UUID, SecDisclosure]:
    """Only exact frozen evidence with matching content, owner and receipt time."""
    if not evidence_ids:
        return {}
    rows = await session.execute(
        select(DataSnapshot, ResearchEvidenceRef, Filing)
        .join(
            ResearchEvidenceRef,
            (ResearchEvidenceRef.source == DataSnapshot.source)
            & (ResearchEvidenceRef.provider_object_id == DataSnapshot.provider_object_id)
            & (ResearchEvidenceRef.revision == DataSnapshot.revision),
        )
        .join(Filing, Filing.evidence_ref_id == ResearchEvidenceRef.id)
        .where(
            ResearchEvidenceRef.id.in_(evidence_ids),
            ResearchEvidenceRef.source == "sec",
            Filing.instrument_id == instrument_id,
            Filing.created_at <= as_of,
            ResearchEvidenceRef.created_at <= as_of,
            DataSnapshot.schema_version.in_((SCHEMA_VERSION, "sec_8k_exhibit_v2")),
            DataSnapshot.created_at <= as_of,
            DataSnapshot.received_at <= as_of,
            DataSnapshot.available_at <= as_of,
        )
        .limit(20)
    )
    result: dict[UUID, SecDisclosure] = {}
    for snapshot, evidence, filing in rows:
        try:
            document: SecDisclosure
            if snapshot.schema_version == "sec_8k_exhibit_v2":
                from nexus.research.sec_exhibit_storage import (
                    configured_exhibit_store,
                    verify_exhibit_originals,
                )
                from nexus.research.sec_exhibits import SecExhibitDisclosure

                document = SecExhibitDisclosure.model_validate(snapshot.payload)
                store = configured_exhibit_store(session)
                await verify_exhibit_originals(session, store, document, as_of=as_of)
                canonical = await store.read_evidence(
                    f"evidence/sec/{document.accession_number}/{evidence.content_hash}.json",
                    uri=evidence.location,
                )
                if canonical != document.canonical_bytes():
                    continue
            else:
                document = SecDisclosure.model_validate(snapshot.payload)
        except (
            ValidationError, ValueError, TypeError, KeyError, OSError, BotoCoreError, ClientError,
        ):
            continue
        content = document.canonical_bytes()
        digest = hashlib.sha256(content).hexdigest()
        available_at = snapshot.received_at if document.form == "8-K" else document.accepted_at
        if (
            document.instrument_id == instrument_id
            and document.accession_number == filing.accession_number == evidence.provider_object_id
            and document.form == filing.form
            and document.accepted_at == filing.accepted_at == snapshot.observed_at
            and available_at == evidence.available_at == snapshot.available_at
            and max(document.accepted_at, document.filed_at) <= snapshot.received_at
            and document.filed_at == filing.filed_at <= as_of
            and document.accepted_at <= as_of
            and digest == evidence.content_hash == evidence.revision == snapshot.revision
            and len(content) == evidence.byte_length
        ):
            result[evidence.id] = document
    return result
