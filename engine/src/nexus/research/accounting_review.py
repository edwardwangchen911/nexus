"""Bounded review materials and untrusted opinions, never accounting authority.

Receipt validation proves bytes and scope only. It does not decide whether an
amendment revises a financial fact, authenticate a reviewer, or change old runs.
"""

import json
import xml.etree.ElementTree as ET
from datetime import datetime
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from pathlib import Path, PurePosixPath
from typing import Annotated, Literal
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Nonempty = Annotated[str, Field(min_length=1, max_length=12000)]


class ReviewModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class MaterialSource(ReviewModel):
    relative_path: str = Field(min_length=1, max_length=512)
    sha256: Digest
    byte_length: int = Field(gt=0, le=8 * 1024 * 1024)
    source_url: str = Field(min_length=1, max_length=1024)
    received_at: AwareDatetime


class FactScope(ReviewModel):
    fact_id: UUID
    binding: dict[str, str | None] = Field(min_length=1, max_length=30)
    original_source_sha256: Digest
    context_locator: Nonempty
    context_sha256: Digest


class AmendmentScope(ReviewModel):
    evidence_ref_id: UUID
    source_sha256: Digest
    accession_number: str = Field(pattern=r"^[0-9]{10}-[0-9]{2}-[0-9]{6}$")


class ReviewMaterial(ReviewModel):
    schema_version: Literal["accounting_material_v1"] = "accounting_material_v1"
    instrument_id: UUID
    issuer_cik: str = Field(pattern=r"^[0-9]{10}$")
    facts: tuple[FactScope, ...] = Field(min_length=1, max_length=50)
    amendments: tuple[AmendmentScope, ...] = Field(min_length=1, max_length=20)
    sources: tuple[MaterialSource, ...] = Field(min_length=1, max_length=50)
    unresolved_questions: tuple[Nonempty, ...] = Field(min_length=1, max_length=50)
    accounting_authority: Literal[False] = False


class ClauseCitation(ReviewModel):
    source_sha256: Digest
    locator: Nonempty
    quote: str = Field(min_length=1, max_length=2000)


class ReviewFinding(ReviewModel):
    fact_id: UUID
    amendment_evidence_ref_id: UUID
    conclusion: Literal["as_disclosed_not_replaced", "requires_reconciliation", "unknown"]
    reasoning: str = Field(min_length=20, max_length=12000)
    citations: tuple[ClauseCitation, ...] = Field(min_length=1, max_length=20)
    timing_conflict_assessment: Nonempty


class ReviewSubmission(ReviewModel):
    schema_version: Literal["accounting_submission_v1"] = "accounting_submission_v1"
    submission_id: UUID
    material_sha256: Digest
    reviewer_name: str = Field(min_length=1, max_length=200)
    reviewer_capacity: Nonempty
    reviewed_at: AwareDatetime
    findings: tuple[ReviewFinding, ...] = Field(min_length=1, max_length=1000)
    authority_status: Literal["unverified"] = "unverified"


def canonical_bytes(value: BaseModel) -> bytes:
    return json.dumps(value.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode()


def material_digest(material: ReviewMaterial) -> str:
    return sha256(canonical_bytes(material)).hexdigest()


def read_sources(material: ReviewMaterial, source_root: Path, now: datetime) -> dict[str, bytes]:
    if now.tzinfo is None:
        raise ValueError("ACCOUNTING_TIME_INVALID")
    root = source_root.resolve(strict=True)
    result: dict[str, bytes] = {}
    total = 0
    for source in material.sources:
        relative = PurePosixPath(source.relative_path)
        url = urlsplit(source.source_url)
        if (
            relative.is_absolute()
            or any(part in ("..", ".") for part in source.relative_path.split("/"))
            or "\\" in source.relative_path
            or source.received_at > now
            or url.scheme != "https"
            or url.netloc != "www.sec.gov"
            or not url.path.startswith(f"/Archives/edgar/data/{int(material.issuer_cik)}/")
            or url.query
            or url.fragment
            or any(part == ".." for part in url.path.split("/"))
        ):
            raise ValueError("ACCOUNTING_SOURCE_INVALID")
        path = root
        for part in relative.parts:
            path /= part
            if path.is_symlink():
                raise ValueError("ACCOUNTING_SOURCE_INVALID")
        if not path.is_file() or path.stat().st_size != source.byte_length:
            raise ValueError("ACCOUNTING_SOURCE_INVALID")
        total += source.byte_length
        if total > 32 * 1024 * 1024:
            raise ValueError("ACCOUNTING_SOURCE_INVALID")
        raw = path.read_bytes()
        if len(raw) != source.byte_length or sha256(raw).hexdigest() != source.sha256:
            raise ValueError("ACCOUNTING_SOURCE_INVALID")
        if source.sha256 in result:
            raise ValueError("ACCOUNTING_SOURCE_DUPLICATE")
        result[source.sha256] = raw
    return result


def build_material(data: object, *, source_root: Path, now: datetime) -> ReviewMaterial:
    material = ReviewMaterial.model_validate(data)
    sources = read_sources(material, source_root, now)
    if (
        len({f.fact_id for f in material.facts}) != len(material.facts)
        or len({a.evidence_ref_id for a in material.amendments}) != len(material.amendments)
        or any(f.original_source_sha256 not in sources for f in material.facts)
        or any(a.source_sha256 not in sources for a in material.amendments)
    ):
        raise ValueError("ACCOUNTING_SCOPE_INVALID")
    return material


def verify_xbrl_contexts(material: ReviewMaterial, *, source_root: Path, now: datetime) -> int:
    """Independently reparse exact inline nodes; this is not accounting clearance."""
    sources = read_sources(material, source_root, now)
    namespace = "{http://www.xbrl.org/2003/instance}"
    count = 0
    for fact in material.facts:
        raw = sources[fact.original_source_sha256]
        serialized = fact.binding.get("xbrl_nodes_json")
        if serialized is None:
            continue
        try:
            if b"<!ENTITY" in raw.upper():
                raise ValueError("ACCOUNTING_CONTEXT_INVALID")
            tree = ET.fromstring(raw)
            nodes = json.loads(serialized)
            if not isinstance(nodes, list) or not 1 <= len(nodes) <= 8:
                raise ValueError("ACCOUNTING_CONTEXT_INVALID")
            seen: set[str] = set()
            for proof in nodes:
                attrs = proof["attributes"]
                node_id = attrs["id"]
                matches = [n for n in tree.iter() if n.get("id") == node_id]
                if len(matches) != 1 or node_id in seen:
                    raise ValueError("ACCOUNTING_CONTEXT_INVALID")
                seen.add(node_id)
                node = matches[0]
                contexts = [
                    n
                    for n in tree.iter(namespace + "context")
                    if n.get("id") == node.get("contextRef")
                ]
                units = [
                    n for n in tree.iter(namespace + "unit") if n.get("id") == node.get("unitRef")
                ]
                if len(contexts) != 1 or len(units) != 1:
                    raise ValueError("ACCOUNTING_CONTEXT_INVALID")
                context, unit = contexts[0], units[0]
                context_hash = sha256(ET.tostring(context, encoding="unicode").encode()).hexdigest()
                value = Decimal("".join(node.itertext()).replace(",", ""))
                value *= Decimal(10) ** int(node.get("scale", "0"))
                if node.get("sign") == "-":
                    value = -value
                if (
                    not node.tag.endswith("}nonFraction")
                    or node.get("name")
                    != f"{fact.binding['raw_taxonomy']}:{fact.binding['raw_tag']}"
                    or node.get("contextRef") != attrs["contextRef"]
                    or node.get("unitRef") != attrs["unitRef"]
                    or context_hash != fact.context_sha256
                    or context_hash != proof["context_xml_sha256"]
                    or context.findtext(f"{namespace}entity/{namespace}identifier")
                    != material.issuer_cik
                    or context.findtext(f"{namespace}period/{namespace}startDate")
                    != fact.binding["period_start"]
                    or context.findtext(f"{namespace}period/{namespace}endDate")
                    != fact.binding["period_end"]
                    or any(
                        n.tag.endswith(("}explicitMember", "}typedMember")) for n in context.iter()
                    )
                    or fact.binding["unit"] != "USD/shares"
                    or unit.findtext(
                        f"{namespace}divide/{namespace}unitNumerator/{namespace}measure"
                    )
                    != "iso4217:USD"
                    or unit.findtext(
                        f"{namespace}divide/{namespace}unitDenominator/{namespace}measure"
                    )
                    != "xbrli:shares"
                    or not value.is_finite()
                    or value != Decimal(str(fact.binding["value"]))
                ):
                    raise ValueError("ACCOUNTING_CONTEXT_INVALID")
            count += 1
        except (KeyError, TypeError, ValueError, InvalidOperation, ET.ParseError) as exc:
            raise ValueError("ACCOUNTING_CONTEXT_INVALID") from exc
    return count


def validate_submission(
    data: object, *, package: ReviewMaterial, source_root: Path, now: datetime
) -> ReviewSubmission:
    package = build_material(package.model_dump(mode="json"), source_root=source_root, now=now)
    submission = ReviewSubmission.model_validate(data)
    expected = {(f.fact_id, a.evidence_ref_id) for f in package.facts for a in package.amendments}
    actual = {(f.fact_id, f.amendment_evidence_ref_id) for f in submission.findings}
    if (
        submission.material_sha256 != material_digest(package)
        or submission.reviewed_at > now
        or submission.reviewed_at < max(s.received_at for s in package.sources)
        or actual != expected
        or len(actual) != len(submission.findings)
    ):
        raise ValueError("ACCOUNTING_SUBMISSION_SCOPE_INVALID")
    sources = read_sources(package, source_root, now)
    for finding in submission.findings:
        for citation in finding.citations:
            raw = sources.get(citation.source_sha256)
            if raw is None or citation.quote.encode() not in raw:
                raise ValueError("ACCOUNTING_CITATION_INVALID")
    return submission
