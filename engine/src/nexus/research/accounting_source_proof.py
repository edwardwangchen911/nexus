"""Verify historical archive identity independently of the reviewer's conclusions."""

import re
import xml.etree.ElementTree as ET
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from urllib.parse import urlsplit

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.persistence.models.data import DataArtifact
from nexus.persistence.models.research import FinancialFactRecord
from nexus.research.accounting_review import ReviewMaterial, read_sources
from nexus.research.sec_amendments import SecAmendmentDisclosure
from nexus.research.sec_disclosures import read_disclosures
from nexus.research.sec_exhibit_storage import configured_exhibit_store


def verify_amendment_original(
    raw: bytes,
    *,
    accession: str,
    issuer_cik: str,
    primary_url: str,
    primary_sha256: str,
    primary_raw: bytes | None = None,
) -> None:
    if sha256(raw).hexdigest() == primary_sha256:
        return
    header = raw.split(b"</SEC-HEADER>", 1)[0]
    if not re.search(
        rb"ACCESSION NUMBER:\s*" + accession.encode() + rb"\s", header
    ) or not re.search(rb"CENTRAL INDEX KEY:\s*" + issuer_cik.encode() + rb"\s", header):
        raise ValueError("ACCOUNTING_AMENDMENT_ORIGINAL_INVALID")
    filename = urlsplit(primary_url).path.rsplit("/", 1)[-1].encode()
    documents = re.findall(rb"<DOCUMENT>(.*?)</DOCUMENT>", raw, re.S)
    matches = []
    for document in documents:
        if re.search(rb"<FILENAME>\s*" + re.escape(filename) + rb"\s", document):
            if not re.search(rb"<TYPE>8-K/A\s", document):
                raise ValueError("ACCOUNTING_AMENDMENT_ORIGINAL_INVALID")
            texts = re.findall(rb"<TEXT>(.*?)</TEXT>", document, re.S)
            if len(texts) != 1:
                raise ValueError("ACCOUNTING_AMENDMENT_ORIGINAL_INVALID")
            matches.append(texts[0])
    if len(matches) != 1:
        raise ValueError("ACCOUNTING_AMENDMENT_ORIGINAL_INVALID")
    content = matches[0].strip(b"\r\n")
    wrapper = re.fullmatch(rb"<XBRL>\s*([\s\S]*?)\s*</XBRL>", content)
    if wrapper is not None:
        content = wrapper[1].strip(b"\r\n")
    if primary_sha256 in {sha256(value).hexdigest() for value in (matches[0], content)}:
        return
    if primary_raw is None or sha256(primary_raw).hexdigest() != primary_sha256:
        raise ValueError("ACCOUNTING_AMENDMENT_ORIGINAL_INVALID")
    # SEC's archived HTTP representation may append one empty delivery script.
    # Compare every other byte, including filing text, XBRL and all attributes.
    # No inline code, arbitrary tags, general whitespace or XML content is removed.
    normalized, count = re.subn(
        rb'<script type="text/javascript" +src="/[A-Za-z0-9/_-]+"></script>(?=</body>)',
        b"",
        primary_raw.strip(b"\r\n"),
    )
    if count != 1 or normalized != content:
        raise ValueError("ACCOUNTING_AMENDMENT_ORIGINAL_INVALID")


async def verify_adoption_sources(
    session: AsyncSession, material: ReviewMaterial, *, source_root: Path, now: datetime
) -> None:
    raw = read_sources(material, source_root, now)
    originals = {source.sha256: source for source in material.sources}
    ids = tuple(item.evidence_ref_id for item in material.amendments)
    disclosures = await read_disclosures(
        session, instrument_id=material.instrument_id, evidence_ids=ids, as_of=now
    )
    if set(disclosures) != set(ids):
        raise ValueError("ACCOUNTING_AMENDMENT_ARCHIVE_INVALID")
    for amendment in material.amendments:
        document = disclosures[amendment.evidence_ref_id]
        if (
            not isinstance(document, SecAmendmentDisclosure)
            or document.issuer_cik != material.issuer_cik
            or document.accession_number != amendment.accession_number
        ):
            raise ValueError("ACCOUNTING_AMENDMENT_ORIGINAL_INVALID")
        source = originals[amendment.source_sha256]
        prefix = (
            f"/Archives/edgar/data/{int(material.issuer_cik)}/"
            f"{amendment.accession_number.replace('-', '')}/"
        )
        if not urlsplit(source.source_url).path.startswith(prefix):
            raise ValueError("ACCOUNTING_AMENDMENT_ORIGINAL_INVALID")
        artifact = await session.scalar(
            select(DataArtifact).where(DataArtifact.sha256 == document.raw_content_hash)
        )
        if artifact is None:
            raise ValueError("ACCOUNTING_AMENDMENT_ARCHIVE_INVALID")
        primary_raw = await configured_exhibit_store(session).read_raw(
            f"raw/sec/amendments/{document.raw_content_hash}.html", uri=artifact.uri
        )
        verify_amendment_original(
            raw[amendment.source_sha256],
            accession=document.accession_number,
            issuer_cik=document.issuer_cik,
            primary_url=document.source_url,
            primary_sha256=document.raw_content_hash,
            primary_raw=primary_raw,
        )
    for scope in material.facts:
        fact = await session.get(FinancialFactRecord, scope.fact_id)
        if fact is None:
            raise ValueError("ACCOUNTING_FACT_ORIGINAL_INVALID")
        source = originals[scope.original_source_sha256]
        prefix = (
            f"/Archives/edgar/data/{int(material.issuer_cik)}/"
            f"{fact.accession_number.replace('-', '')}/"
        )
        tree = ET.fromstring(raw[scope.original_source_sha256])
        values = {
            name: {
                "".join(node.itertext()).strip() for node in tree.iter() if node.get("name") == name
            }
            for name in ("dei:EntityCentralIndexKey", "dei:DocumentType")
        }
        if (
            not urlsplit(source.source_url).path.startswith(prefix)
            or values["dei:EntityCentralIndexKey"] != {material.issuer_cik}
            or values["dei:DocumentType"] != {fact.form}
        ):
            raise ValueError("ACCOUNTING_FACT_ORIGINAL_INVALID")
