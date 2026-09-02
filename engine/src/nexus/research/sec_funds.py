"""Bind one explicitly selected SEC 497K to its exact fund series and share class."""

import hashlib
import json
import re
from datetime import UTC, date, datetime
from html.parser import HTMLParser
from typing import Protocol
from uuid import UUID

from nexus.providers.sec import SecCompanyFactsClient
from nexus.research.sec_disclosures import SecDisclosure, SecFundIdentity, disclosure_excerpt


class PublicSecReader(Protocol):
    async def get_bytes(
        self,
        path_segments: tuple[str, ...],
        query: tuple[tuple[str, str], ...],
        headers: tuple[tuple[str, str], ...],
    ) -> bytes: ...


def resolve_fund(mapping: object, symbol: str) -> tuple[str, str, str]:
    if not isinstance(mapping, dict) or mapping.get("fields") != [
        "cik",
        "seriesId",
        "classId",
        "symbol",
    ]:
        raise ValueError("SEC_LOCAL_FUND_MAPPING_INVALID")
    data = mapping.get("data")
    if not isinstance(data, list):
        raise ValueError("SEC_LOCAL_FUND_MAPPING_INVALID")
    rows = [row for row in data if isinstance(row, list) and len(row) == 4 and row[3] == symbol]
    if len(rows) != 1:
        raise ValueError("SEC_LOCAL_FUND_MAPPING_UNAVAILABLE")
    cik, series, share_class, _ = rows[0]
    if (
        type(cik) is not int
        or not 0 < cik < 10**10
        or not isinstance(series, str)
        or not re.fullmatch(r"S[0-9]{9}", series)
        or not isinstance(share_class, str)
        or not re.fullmatch(r"C[0-9]{9}", share_class)
    ):
        raise ValueError("SEC_LOCAL_FUND_MAPPING_INVALID")
    return str(cik).zfill(10), series, share_class


class _IndexLinks(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: set[str] = set()

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        href = dict(attrs).get("href")
        if tag == "a" and href:
            self.links.add(href)


async def fetch_fund_disclosure(
    *,
    instrument_id: UUID,
    symbol: str,
    identity: tuple[str, str, str],
    accession: str,
    client: SecCompanyFactsClient,
    directory: PublicSecReader,
    contact: str,
) -> tuple[SecDisclosure, tuple[tuple[str, bytes], ...]]:
    """After directory resolution: exactly submissions, filing index and primary document."""
    cik, series, share_class = identity
    current = await client.fetch_submissions(cik)
    payload = json.loads(current.raw_payload)
    if str(payload.get("cik")).zfill(10) != cik:
        raise ValueError("SEC_LOCAL_ISSUER_MISMATCH")
    recent = payload["filings"]["recent"]
    fields = ("accessionNumber", "acceptanceDateTime", "filingDate", "form", "primaryDocument")
    arrays = []
    for field in fields:
        array = recent.get(field)
        if not isinstance(array, list):
            raise ValueError("SEC_LOCAL_SUBMISSIONS_INVALID")
        arrays.append(array)
    if len({len(array) for array in arrays}) != 1:
        raise ValueError("SEC_LOCAL_SUBMISSIONS_INVALID")
    rows = [row for row in zip(*arrays, strict=True) if row[0] == accession]
    if len(rows) != 1 or rows[0][3] != "497K" or not all(isinstance(v, str) for v in rows[0]):
        raise ValueError("SEC_LOCAL_FUND_FILING_UNAVAILABLE")
    _, accepted, filed, _, filename = rows[0]
    accepted_at = datetime.fromisoformat(accepted.replace("Z", "+00:00"))
    filed_at = datetime.combine(date.fromisoformat(filed), datetime.min.time(), tzinfo=UTC)
    if accepted_at.tzinfo is None or max(accepted_at, filed_at) > current.received_at:
        raise ValueError("SEC_LOCAL_ACCEPTANCE_TIME_INVALID")
    if not re.fullmatch(r"[0-9]{10}-[0-9]{2}-[0-9]{6}", accession) or not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9._-]*\.html?", filename
    ):
        raise ValueError("SEC_LOCAL_DISCLOSURE_PATH_INVALID")
    root = ("Archives", "edgar", "data", str(int(cik)), accession.replace("-", ""))
    headers = (("User-Agent", f"NEXUS {contact}"), ("Accept-Encoding", "gzip, deflate"))
    index = await directory.get_bytes((*root, f"{accession}-index.html"), (), headers)
    text, truncated = disclosure_excerpt(index)
    links = _IndexLinks()
    links.feed(index.decode("utf-8-sig"))
    path = "/" + "/".join((*root, filename))
    if (
        truncated
        or set(re.findall(r"\bS[0-9]{9}\b", text)) != {series}
        or set(re.findall(r"\bC[0-9]{9}\b", text)) != {share_class}
        or not re.search(rf"\b{re.escape(symbol)}\b", text)
        or not {path, "https://www.sec.gov" + path}.intersection(links.links)
    ):
        raise ValueError("SEC_LOCAL_FUND_FILING_MISMATCH")
    raw = await directory.get_bytes((*root, filename), (), headers)
    excerpt, truncated = disclosure_excerpt(raw)
    if not re.search(rf"\b{re.escape(symbol)}\b", excerpt):
        raise ValueError("SEC_LOCAL_FUND_DOCUMENT_MISMATCH")
    document = SecDisclosure(
        instrument_id=instrument_id,
        form="497K",
        accession_number=accession,
        filed_at=filed_at,
        accepted_at=accepted_at,
        raw_content_hash=hashlib.sha256(raw).hexdigest(),
        source_url="https://www.sec.gov" + path,
        excerpt=excerpt,
        truncated=truncated,
        fund_identity=SecFundIdentity(
            cik=cik,
            series_id=series,
            class_id=share_class,
            symbol=symbol,
            filing_index_hash=hashlib.sha256(index).hexdigest(),
        ),
    )
    return document, (
        ("submissions", current.raw_payload),
        ("filing-index", index),
        ("disclosure", raw),
    )
