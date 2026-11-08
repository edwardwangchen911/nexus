"""Exact SEC filing inventory and declared text originals for future mixed research.

This does not infer amendment relationships, replace accounting facts, or admit a job.
"""

import re
from datetime import datetime
from hashlib import sha256
from html.parser import HTMLParser
from typing import Literal
from urllib.parse import urljoin

from pydantic import BaseModel, ConfigDict, Field

from nexus.domain.time import require_utc
from nexus.research.sec_disclosure_index import SecIndexEntry
from nexus.research.sec_disclosures import _VisibleText
from nexus.research.sec_index_storage import _receipt

MAX_DOCUMENTS = 17
MAX_ARCHIVE_BYTES = 8 * 1024 * 1024


def require_html_entry(entry: SecIndexEntry) -> None:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*\.html?", entry.primary_document):
        raise ValueError("SEC_MONITOR_DOCUMENT_FORMAT_UNSUPPORTED")


class FilingDocument(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    filename: str
    document_type: str
    source_url: str
    raw_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    text: str = Field(min_length=20, max_length=32000)
    truncated: Literal[False] = False


class _Inventory(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.depth = 0
        self.tables = 0
        self.rows: list[list[tuple[str, tuple[str, ...], tuple[str, ...]]]] = []
        self.row: list[tuple[str, tuple[str, ...], tuple[str, ...]]] | None = None
        self.parts: list[str] | None = None
        self.links: list[str] = []
        self.labels: list[str] = []
        self.anchor_parts: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "table":
            if self.depth:
                raise ValueError("SEC_MONITOR_INVENTORY_INVALID")
            if dict(attrs).get("summary") == "Document Format Files":
                self.tables += 1
                self.depth = 1
        elif self.depth and tag == "tr":
            if self.row is not None:
                raise ValueError("SEC_MONITOR_INVENTORY_INVALID")
            self.row = []
        elif self.depth and tag == "td":
            if self.row is None or self.parts is not None:
                raise ValueError("SEC_MONITOR_INVENTORY_INVALID")
            self.parts, self.links, self.labels = [], [], []
        elif self.parts is not None and tag == "a":
            if (
                self.anchor_parts is not None
                or len([key for key, _ in attrs if key == "href"]) != 1
            ):
                raise ValueError("SEC_MONITOR_INVENTORY_INVALID")
            self.links.append(dict(attrs).get("href") or "")
            self.anchor_parts = []

    def handle_data(self, data: str) -> None:
        if self.parts is not None:
            self.parts.append(data)
        if self.anchor_parts is not None:
            self.anchor_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self.anchor_parts is not None:
            self.labels.append(" ".join("".join(self.anchor_parts).split()))
            self.anchor_parts = None
        elif self.depth and tag == "td":
            if self.row is None or self.parts is None or self.anchor_parts is not None:
                raise ValueError("SEC_MONITOR_INVENTORY_INVALID")
            self.row.append(
                (" ".join("".join(self.parts).split()), tuple(self.links), tuple(self.labels))
            )
            self.parts = None
        elif self.depth and tag == "tr":
            if self.row:
                self.rows.append(self.row)
            self.row = None
        elif self.depth and tag == "table":
            if self.row is not None or self.parts is not None:
                raise ValueError("SEC_MONITOR_INVENTORY_INVALID")
            self.depth = 0


def document_inventory(
    raw: bytes, *, entry: SecIndexEntry, issuer_cik: str
) -> tuple[tuple[str, str, int], ...]:
    require_html_entry(entry)
    if not re.fullmatch(r"[0-9]{10}", issuer_cik) or not int(issuer_cik):
        raise ValueError("SEC_MONITOR_IDENTITY_INVALID")
    root = filing_root(issuer_cik, entry.accession_number)
    if not 0 < len(raw) <= MAX_ARCHIVE_BYTES:
        raise ValueError("SEC_MONITOR_INVENTORY_INVALID")
    parser = _Inventory()
    try:
        parser.feed(raw.decode("utf-8-sig"))
        parser.close()
    except UnicodeError:
        raise ValueError("SEC_MONITOR_INVENTORY_INVALID") from None
    if (
        parser.tables != 1
        or parser.depth
        or parser.parts is not None
        or not 1 <= len(parser.rows) <= MAX_DOCUMENTS + 1
    ):
        raise ValueError("SEC_MONITOR_INVENTORY_INVALID")
    result, sequences = [], []
    for position, row in enumerate(parser.rows):
        if len(row) != 5 or not row[4][0].isdigit() or len(row[2][1]) != 1 or len(row[2][2]) != 1:
            raise ValueError("SEC_MONITOR_INVENTORY_INVALID")
        filename, links, kind = row[2][2][0], row[2][1], row[3][0]
        if not row[0][0]:
            # SEC's final aggregate row is not a separately sequenced exhibit.
            # Keep it in the immutable original inventory; reject every other blank row.
            summary = entry.accession_number + ".txt"
            if (
                position != len(parser.rows) - 1
                or row[1][0] != "Complete submission text file"
                or kind
                or filename != summary
                or row[2][0] != summary
                or any(row[i][1] for i in (0, 1, 3, 4))
                or any(token in links[0] for token in ("%", "\\", "..", "#", "?"))
                or urljoin(root, links[0]) != root + summary
                or int(row[4][0]) <= 0
            ):
                raise ValueError("SEC_MONITOR_INVENTORY_INVALID")
            continue
        if not row[0][0].isdigit():
            raise ValueError("SEC_MONITOR_INVENTORY_INVALID")
        inline = links[0].startswith("/ix?doc=")
        if row[2][0] != filename and not (inline and row[2][0] == filename + " iXBRL"):
            raise ValueError("SEC_MONITOR_INVENTORY_INVALID")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*\.(?:html?|txt)", filename):
            raise ValueError("SEC_MONITOR_ATTACHMENT_UNSUPPORTED")
        href = links[0]
        if href.startswith("/ix?doc="):
            href = href.removeprefix("/ix?doc=")
        if (
            any(token in href for token in ("%", "\\", "..", "#", "?"))
            or urljoin(root, href) != root + filename
        ):
            raise ValueError("SEC_MONITOR_INVENTORY_IDENTITY_INVALID")
        if (filename == entry.primary_document and kind != entry.form) or (
            filename != entry.primary_document and not re.fullmatch(r"EX-[0-9]+(?:\.[0-9]+)?", kind)
        ):
            raise ValueError("SEC_MONITOR_INVENTORY_IDENTITY_INVALID")
        sequences.append(int(row[0][0]))
        result.append((filename, kind, int(row[4][0])))
    if (
        not 1 <= len(result) <= MAX_DOCUMENTS
        or sequences != sorted(set(sequences))
        or len({row[0] for row in result}) != len(result)
        or sum(row[0] == entry.primary_document for row in result) != 1
    ):
        raise ValueError("SEC_MONITOR_INVENTORY_INVALID")
    return tuple(result)


def filing_root(issuer_cik: str, accession: str) -> str:
    return (
        f"https://www.sec.gov/Archives/edgar/data/{int(issuer_cik)}/{accession.replace('-', '')}/"
    )


def verify_filing_documents(
    *,
    entry: SecIndexEntry,
    issuer_cik: str,
    inventory: tuple[bytes, bytes],
    documents: dict[str, tuple[bytes, bytes]],
    now: datetime,
) -> tuple[FilingDocument, ...]:
    require_html_entry(entry)
    now = require_utc(now)
    try:
        root = filing_root(issuer_cik, entry.accession_number)
        inventory_received = _receipt(
            *inventory, source_url=root + entry.accession_number + "-index.html", now=now
        )
        if inventory_received < max(entry.accepted_at, entry.filed_at):
            raise ValueError("SEC_MONITOR_DOCUMENT_TIME_INVALID")
        rows = document_inventory(inventory[0], entry=entry, issuer_cik=issuer_cik)
        if (
            set(documents) != {row[0] for row in rows}
            or max(entry.accepted_at, entry.filed_at) > now
        ):
            raise ValueError("SEC_MONITOR_DOCUMENT_SET_INVALID")
        result = []
        for filename, kind, size in rows:
            raw, receipt = documents[filename]
            if not 0 < size == len(raw) <= MAX_ARCHIVE_BYTES:
                raise ValueError("SEC_MONITOR_DOCUMENT_SIZE_INVALID")
            received = _receipt(raw, receipt, source_url=root + filename, now=now)
            if received < max(entry.accepted_at, entry.filed_at):
                raise ValueError("SEC_MONITOR_DOCUMENT_TIME_INVALID")
            parser = _VisibleText()
            parser.feed(raw.decode("utf-8-sig"))
            parser.close()
            text = " ".join(" ".join(parser.parts).split())
            if not 20 <= len(text) <= 32000:
                raise ValueError("SEC_MONITOR_CONTENT_CAPACITY")
            result.append(
                FilingDocument(
                    filename=filename,
                    document_type=kind,
                    source_url=root + filename,
                    raw_hash=sha256(raw).hexdigest(),
                    text=text,
                )
            )
        return tuple(result)
    except (KeyError, UnicodeError, TypeError, ValueError) as error:
        if str(error).startswith("SEC_MONITOR_"):
            raise
        raise ValueError("SEC_MONITOR_ORIGINAL_INVALID") from None
