"""One archived 8-K and its explicitly linked 99.1 release, without accounting inference."""

import json
import re
from collections.abc import Sequence
from datetime import date, datetime
from hashlib import sha256
from typing import Any, Literal
from urllib.parse import urljoin, urlsplit
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    model_serializer,
)

from nexus.research.sec_disclosures import SecDisclosure, _VisibleText, disclosure_excerpt
from nexus.research.sec_exhibit_quarter_metrics import (
    ExhibitQuarterMetrics,
    parse_exhibit_quarter_metrics,
)

SCHEMA_VERSION: Literal["sec_8k_exhibit_v2"] = "sec_8k_exhibit_v2"
MAX_EXHIBIT_TEXT = 32000
MAX_RAW_BYTES = 8 * 1024 * 1024


class OriginalReceipt(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    source_url: str = Field(max_length=512)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    byte_length: int = Field(strict=True, ge=1, le=MAX_RAW_BYTES)
    received_at: AwareDatetime
    parent_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


class ExhibitText(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    number: Literal["99.1"] = "99.1"
    source_url: str
    parent_href: str
    raw_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    received_at: AwareDatetime
    excerpt: str = Field(min_length=20, max_length=MAX_EXHIBIT_TEXT)
    truncated: Literal[False] = False
    accounting_status: Literal["unaudited"] = "unaudited"
    filing_status: Literal["furnished_not_filed"] = "furnished_not_filed"


class SecExhibitDisclosure(SecDisclosure):
    schema_version: Literal["sec_8k_exhibit_v2"] = SCHEMA_VERSION
    form: Literal["8-K"] = "8-K"
    identity_snapshot_id: UUID
    originals: tuple[OriginalReceipt, OriginalReceipt, OriginalReceipt]
    receipt_hashes: tuple[str, str, str]
    exhibit: ExhibitText
    quarter_metrics: ExhibitQuarterMetrics | None = None

    @model_serializer(mode="wrap")
    def serialize_disclosure(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        result = handler(self)
        if self.quarter_metrics is None:
            result.pop("quarter_metrics", None)
        return result

    @property
    def available_at(self) -> datetime:
        return max(item.received_at for item in self.originals)


def _object(raw: bytes) -> dict[str, Any]:
    def unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("SEC_EXHIBIT_DUPLICATE_JSON_KEY")
            result[key] = value
        return result

    value = json.loads(raw, object_pairs_hook=unique)
    if not isinstance(value, dict):
        raise ValueError("SEC_EXHIBIT_ORIGINAL_INVALID")
    return value


class _ExhibitLinks(_VisibleText):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[tuple[str, str]] = []
        self.anchor: tuple[str, list[str]] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        super().handle_starttag(tag, attrs)
        if tag == "a" and not self.hidden:
            if self.anchor is not None or len([k for k, _ in attrs if k == "href"]) > 1:
                raise ValueError("SEC_EXHIBIT_LINK_INVALID")
            self.anchor = (dict(attrs).get("href") or "", [])

    def handle_data(self, data: str) -> None:
        if self.anchor is not None and not self.hidden:
            self.anchor[1].append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self.anchor is not None:
            href, parts = self.anchor
            self.links.append((href, "".join(parts).strip()))
            self.anchor = None
        super().handle_endtag(tag)


def _exhibit_link(parent: bytes, parent_url: str) -> tuple[str, str]:
    parser = _ExhibitLinks()
    parser.feed(parent.decode("utf-8-sig"))
    parser.close()
    matches = [href for href, label in parser.links if label == "99.1"]
    if len(matches) != 1:
        raise ValueError("SEC_EXHIBIT_REFERENCE_NOT_UNIQUE")
    href = matches[0]
    if any(token in href for token in ("%", "\\", "..", "#", "?")):
        raise ValueError("SEC_EXHIBIT_PATH_INVALID")
    url = urljoin(parent_url, href)
    target, parent_path = urlsplit(url), urlsplit(parent_url)
    if (
        target.scheme != "https"
        or target.netloc != "www.sec.gov"
        or target.path.rsplit("/", 1)[0] != parent_path.path.rsplit("/", 1)[0]
        or target.path == parent_path.path
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*\.html?", target.path.rsplit("/", 1)[1])
    ):
        raise ValueError("SEC_EXHIBIT_PATH_INVALID")
    return href, url


def build_exhibit_disclosure(
    *,
    instrument_id: UUID,
    identity_snapshot_id: UUID,
    issuer_cik: str,
    archives: Sequence[tuple[bytes, bytes]],
    now: datetime,
    include_quarter_metrics: bool = False,
) -> SecExhibitDisclosure:
    """Validate all three originals before returning a persistable, self-contained contract."""
    if len(archives) != 3 or now.tzinfo is None or not re.fullmatch(r"[0-9]{10}", issuer_cik):
        raise ValueError("SEC_EXHIBIT_IDENTITY_INVALID")
    receipts = []
    for raw, receipt_raw in archives:
        if len(raw) > MAX_RAW_BYTES or len(receipt_raw) > 65536:
            raise ValueError("SEC_EXHIBIT_ORIGINAL_TOO_LARGE")
        receipt = OriginalReceipt.model_validate(_object(receipt_raw))
        if (
            len(raw) != receipt.byte_length
            or sha256(raw).hexdigest() != receipt.sha256
            or receipt.received_at > now
        ):
            raise ValueError("SEC_EXHIBIT_ORIGINAL_MISMATCH")
        receipts.append(receipt)
    metadata, parent, exhibit = (item[0] for item in archives)
    if (
        receipts[0].parent_sha256 is not None
        or receipts[1].parent_sha256 is not None
        or receipts[2].parent_sha256 not in {None, receipts[1].sha256}
    ):
        raise ValueError("SEC_EXHIBIT_PARENT_HASH_INVALID")
    submissions = _object(metadata)
    if (
        isinstance(submissions.get("cik"), bool)
        or str(submissions.get("cik")).zfill(10) != issuer_cik
        or receipts[0].source_url != f"https://data.sec.gov/submissions/CIK{issuer_cik}.json"
    ):
        raise ValueError("SEC_EXHIBIT_IDENTITY_INVALID")
    filings = submissions.get("filings")
    recent = filings.get("recent") if isinstance(filings, dict) else None
    fields = ("accessionNumber", "form", "acceptanceDateTime", "filingDate", "primaryDocument")
    if (
        not isinstance(recent, dict)
        or any(not isinstance(recent.get(key), list) for key in fields)
        or len({len(recent[key]) for key in fields}) != 1
    ):
        raise ValueError("SEC_EXHIBIT_METADATA_INVALID")
    rows = list(zip(*(recent[key] for key in fields), strict=True))
    matched = []
    amendments = []
    for accession, form, accepted, filed, filename in rows:
        if form not in {"8-K", "8-K/A"}:
            continue
        if not all(isinstance(value, str) for value in (accession, accepted, filed, filename)):
            raise ValueError("SEC_EXHIBIT_METADATA_INVALID")
        if not re.fullmatch(r"[0-9]{10}-[0-9]{2}-[0-9]{6}", accession):
            raise ValueError("SEC_EXHIBIT_METADATA_INVALID")
        accepted_at = datetime.fromisoformat(accepted.replace("Z", "+00:00"))
        if accepted_at.tzinfo is None or accepted_at > receipts[0].received_at:
            raise ValueError("SEC_EXHIBIT_METADATA_INVALID")
        if form == "8-K/A":
            amendments.append(accepted_at)
        source_url = (
            f"https://www.sec.gov/Archives/edgar/data/{int(issuer_cik)}/"
            f"{accession.replace('-', '')}/{filename}"
        )
        if source_url == receipts[1].source_url and form == "8-K":
            matched.append((accession, accepted_at, filed))
    if len(matched) != 1:
        raise ValueError("SEC_EXHIBIT_PARENT_NOT_UNIQUE")
    accession, accepted_at, filed = matched[0]
    if date.fromisoformat(filed) > accepted_at.date():
        raise ValueError("SEC_EXHIBIT_METADATA_INVALID")
    if amendments and max(amendments) >= accepted_at:
        raise ValueError("SEC_EXHIBIT_AMENDMENT_UNSUPPORTED")
    if accepted_at > min(item.received_at for item in receipts):
        raise ValueError("SEC_EXHIBIT_RECEIPT_INVALID")
    href, url = _exhibit_link(parent, receipts[1].source_url)
    if url != receipts[2].source_url:
        raise ValueError("SEC_EXHIBIT_ORIGINAL_MISMATCH")
    parent_excerpt, parent_truncated = disclosure_excerpt(parent)
    parser = _VisibleText()
    parser.feed(exhibit.decode("utf-8-sig"))
    parser.close()
    content = "\n".join(
        line for part in "".join(parser.parts).splitlines() if (line := " ".join(part.split()))
    )
    if len(content) > MAX_EXHIBIT_TEXT:
        raise ValueError("SEC_EXHIBIT_TEXT_TOO_LONG")
    if (
        "unaudited" not in content.lower()
        or "furnished" not in parent_excerpt.lower()
        or not re.search(r"not (?:be treated as )?filed", parent_excerpt, re.I)
    ):
        raise ValueError("SEC_EXHIBIT_SEMANTICS_UNSUPPORTED")
    return SecExhibitDisclosure(
        quarter_metrics=(
            parse_exhibit_quarter_metrics(archives[2][0]) if include_quarter_metrics else None
        ),
        instrument_id=instrument_id,
        identity_snapshot_id=identity_snapshot_id,
        issuer_cik=issuer_cik,
        accession_number=accession,
        source_url=receipts[1].source_url,
        filed_at=datetime.fromisoformat(filed + "T00:00:00+00:00"),
        accepted_at=accepted_at,
        raw_content_hash=receipts[1].sha256,
        excerpt=parent_excerpt,
        truncated=parent_truncated,
        originals=(receipts[0], receipts[1], receipts[2]),
        receipt_hashes=(
            sha256(archives[0][1]).hexdigest(),
            sha256(archives[1][1]).hexdigest(),
            sha256(archives[2][1]).hexdigest(),
        ),
        exhibit=ExhibitText(
            source_url=url,
            parent_href=href,
            raw_content_hash=receipts[2].sha256,
            received_at=receipts[2].received_at,
            excerpt=content,
        ),
    )
