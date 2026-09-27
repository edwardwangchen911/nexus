"""Independent SEC amendment text; no inferred parent or accounting replacement.

This schema deliberately cannot claim a verified parent, complete attachment set,
or accounting correction. Those claims require source proofs in a separate
contract. Legacy disclosure schemas retain their original canonical bytes.
"""

import json
from typing import Literal, Self
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from nexus.research.sec_disclosures import MAX_EXCERPT, SecDisclosure

SCHEMA_VERSION: Literal["sec_amendment_v1"] = "sec_amendment_v1"


class SecAmendmentDisclosure(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal["sec_amendment_v1"] = SCHEMA_VERSION
    instrument_id: UUID
    form: Literal["8-K/A"]
    issuer_cik: str = Field(pattern=r"^[0-9]{10}$")
    accession_number: str = Field(pattern=r"^[0-9]{10}-[0-9]{2}-[0-9]{6}$")
    source_url: str = Field(max_length=512)
    filed_at: AwareDatetime
    accepted_at: AwareDatetime
    received_at: AwareDatetime
    raw_byte_length: int = Field(gt=0, le=8 * 1024 * 1024)
    http_status: Literal[200]
    raw_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    excerpt: str = Field(min_length=20, max_length=MAX_EXCERPT)
    truncated: bool
    relation_status: Literal["unknown"] = "unknown"
    original_accession_number: None = None
    relation_evidence_ids: tuple[()] = ()
    content_coverage: Literal["primary_document_only"] = "primary_document_only"
    attachments_status: Literal["not_collected"] = "not_collected"
    accounting_revision_status: Literal["unverified"] = "unverified"

    @model_validator(mode="after")
    def check_issuer_and_accession(self) -> Self:
        if max(self.accepted_at, self.filed_at) > self.received_at:
            raise ValueError("SEC_AMENDMENT_RECEIPT_TIME_INVALID")
        # Reuse the existing issuer/path rules without extending or reserializing
        # the legacy schema. This check never returns a normal 8-K as evidence.
        SecDisclosure(
            instrument_id=self.instrument_id,
            form="8-K",
            issuer_cik=self.issuer_cik,
            accession_number=self.accession_number,
            source_url=self.source_url,
            filed_at=self.filed_at,
            accepted_at=self.accepted_at,
            raw_content_hash=self.raw_content_hash,
            excerpt=self.excerpt,
            truncated=self.truncated,
        )
        return self

    def canonical_bytes(self) -> bytes:
        # Keep unknown relation explicit, including the null parent accession.
        return json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()
