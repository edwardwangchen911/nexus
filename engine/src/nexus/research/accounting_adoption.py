"""Owner-operated adoption of narrowly scoped as-disclosed opinions.

The operator, not the opinion author or an Agent tool, registers the independently
observed owner instruction and report provenance. This is an auditable local trust
boundary, not a claim of cryptographic human authentication or audit assurance.
"""

import json
from datetime import datetime
from hashlib import sha256
from typing import Literal
from uuid import UUID

from pydantic import Field

from nexus.research.accounting_review import (
    Digest,
    Nonempty,
    ReviewMaterial,
    ReviewModel,
    ReviewSubmission,
    canonical_bytes,
)

SOURCE = "nexus_accounting_adoption"
SCHEMA = "accounting_adoption_v1"
DESIGNATION_SOURCE = "nexus_accounting_designation"
DESIGNATION_SCHEMA = "accounting_designation_v1"
LIMITATIONS = (
    "Owner-designated AI review limited to exact historical EPS and amendment pairs. "
    "Not audit assurance or exclusion of all retrospective accounting effects; "
    "source acceptance-time conflicts remain unreconciled."
)


class OwnerDesignation(ReviewModel):
    source_receipts: tuple[Nonempty, ...] = Field(min_length=1, max_length=50)
    designation_id: UUID
    material_sha256: Digest
    submission_sha256: Digest
    reviewer_name: Nonempty
    owner_message_source: Nonempty
    owner_message_text: Nonempty
    report_source: Nonempty
    report_text: Nonempty
    purpose: Literal["owner_designated_ai_as_disclosed_eps"]


def validate_designation(
    data: object, *, material: ReviewMaterial, opinion: ReviewSubmission
) -> OwnerDesignation:
    designation = OwnerDesignation.model_validate(data)
    if (
        designation.material_sha256 != sha256(canonical_bytes(material)).hexdigest()
        or designation.submission_sha256 != sha256(canonical_bytes(opinion)).hexdigest()
        or designation.reviewer_name != opinion.reviewer_name
    ):
        raise ValueError("ACCOUNTING_DESIGNATION_SCOPE_INVALID")
    receipts = [json.loads(raw) for raw in designation.source_receipts]
    expected = {src.sha256: src for src in material.sources}
    if (
        len(receipts) != len(expected)
        or not all(isinstance(receipt, dict) for receipt in receipts)
        or {receipt.get("sha256") for receipt in receipts} != set(expected)
    ):
        raise ValueError("ACCOUNTING_SOURCE_RECEIPT_INVALID")
    base_keys = {"source_url", "received_at", "sha256", "byte_length"}
    for receipt in receipts:
        source = expected[receipt["sha256"]]
        # The original primary-document receipt predates HTTP status recording.
        # Recognize its exact four-field shape, never infer a missing status or
        # accept added failure/unknown metadata merely because the URL matches.
        direct = receipt.get("source_url") == source.source_url and (
            set(receipt) == base_keys
            or (set(receipt) == base_keys | {"http_status"} and receipt["http_status"] == 200)
        )
        archived = (
            "source_url" not in receipt
            and set(receipt)
            == (base_keys - {"source_url"})
            | {"status", "stage", "http_status", "documents", "requests"}
            and receipt["status"] == "ARCHIVED"
            and receipt["stage"] == "submission_get"
            and receipt["http_status"] == 200
            and isinstance(receipt["documents"], list)
            and bool(receipt["documents"])
            and type(receipt["requests"]) is int
            and receipt["requests"] > 0
        )
        if (
            not (direct or archived)
            or receipt.get("byte_length") != source.byte_length
            or datetime.fromisoformat(str(receipt.get("received_at"))) != source.received_at
        ):
            raise ValueError("ACCOUNTING_SOURCE_RECEIPT_INVALID")
    return designation


def fact_review_fields(
    fact_id: UUID, amendment_ids: tuple[UUID, ...], adoptions: dict[UUID, dict[str, object]]
) -> dict[str, object]:
    """Require every frozen amendment; never let a later opinion hide disagreement."""
    conclusions: dict[UUID, set[str]] = {item: set() for item in amendment_ids}
    used: set[UUID] = set()
    for ref_id, payload in adoptions.items():
        opinion = ReviewSubmission.model_validate(payload["submission"])
        for finding in opinion.findings:
            if finding.fact_id == fact_id and finding.amendment_evidence_ref_id in conclusions:
                conclusions[finding.amendment_evidence_ref_id].add(finding.conclusion)
                used.add(ref_id)
    status = "unverified"
    if any(len(values) > 1 for values in conclusions.values()):
        status = "conflict"
    elif conclusions and all(
        values == {"as_disclosed_not_replaced"} for values in conclusions.values()
    ):
        status = "as_disclosed_not_replaced"
    return {
        "amendment_review_status": status,
        "accounting_review_evidence_ref_ids": sorted(str(item) for item in used),
        "accounting_limitations": LIMITATIONS,
    }
