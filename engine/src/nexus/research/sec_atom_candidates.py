"""Official company-feed hints, never accepted-at or negative coverage authority."""

import re
from datetime import date, datetime
from typing import Literal, cast
from urllib.parse import urlsplit
from xml.etree import ElementTree as ET

from pydantic import AwareDatetime, BaseModel, ConfigDict

from nexus.domain.time import require_utc

NS = "{http://www.w3.org/2005/Atom}"
MAX_BYTES = 1024 * 1024


class AtomEntry(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    accession_number: str
    form: Literal["8-K", "8-K/A"]
    filing_date: date
    source_updated_at: AwareDatetime
    filing_index_url: str


class AtomObservation(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    issuer_cik: str
    received_at: AwareDatetime
    source_updated_at: AwareDatetime
    has_next_page: bool
    entries: tuple[AtomEntry, ...]
    negative_conclusion_authorized: Literal[False] = False


def source_url(cik: str) -> str:
    if not re.fullmatch(r"[0-9]{10}", cik) or int(cik) == 0:
        raise ValueError("SEC_ATOM_SOURCE_INVALID")
    return (
        "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK="
        + cik
        + "&type=8-K&owner=exclude&count=100&output=atom"
    )


def parse_atom(raw: bytes, *, issuer_cik: str, received_at: datetime) -> AtomObservation:
    invalid = "SEC_ATOM_SOURCE_INVALID"
    source_url(issuer_cik)
    received_at = require_utc(received_at)
    if not 0 < len(raw) <= MAX_BYTES:
        raise ValueError(invalid)
    try:
        xml = raw.decode("utf-8")
        if "\x00" in xml or "<!DOCTYPE" in xml.upper() or "<!ENTITY" in xml.upper():
            raise ValueError(invalid)
        root = ET.fromstring(xml)
        if root.tag != NS + "feed" or len(root.findall(NS + "company-info")) != 1:
            raise ValueError(invalid)
        company = root.find(NS + "company-info")
        assert company is not None
        if len(company.findall(NS + "cik")) != 1 or company.findtext(NS + "cik") != issuer_cik:
            raise ValueError(invalid)

        def one(node: ET.Element, name: str) -> str:
            values = node.findall(NS + name)
            if len(values) != 1 or not values[0].text:
                raise ValueError(invalid)
            return values[0].text

        def updated(node: ET.Element) -> datetime:
            value = require_utc(datetime.fromisoformat(one(node, "updated").replace("Z", "+00:00")))
            if value > received_at:
                raise ValueError(invalid)
            return value

        feed_updated = updated(root)
        rows = root.findall(NS + "entry")
        if len(rows) > 100:
            raise ValueError(invalid)
        entries = []
        seen = set()
        for row in rows:
            content = row.find(NS + "content")
            if content is None or len(row.findall(NS + "content")) != 1:
                raise ValueError(invalid)
            form = one(content, "filing-type")
            if form not in ("8-K", "8-K/A"):
                continue
            accession = one(content, "accession-number")
            if not re.fullmatch(r"[0-9]{10}-[0-9]{2}-[0-9]{6}", accession) or accession in seen:
                raise ValueError(invalid)
            seen.add(accession)
            href = one(content, "filing-href")
            url = urlsplit(href)
            base = (
                f"/Archives/edgar/data/{int(issuer_cik)}/"
                f"{accession.replace('-', '')}/{accession}-index"
            )
            if (
                url.scheme != "https"
                or url.netloc != "www.sec.gov"
                or url.path not in (base + ".htm", base + ".html")
                or url.query
                or url.fragment
            ):
                raise ValueError(invalid)
            entries.append(
                AtomEntry(
                    accession_number=accession,
                    form=cast(Literal["8-K", "8-K/A"], form),
                    filing_date=date.fromisoformat(one(content, "filing-date")),
                    source_updated_at=updated(row),
                    filing_index_url=href,
                )
            )
        return AtomObservation(
            issuer_cik=issuer_cik,
            received_at=received_at,
            source_updated_at=feed_updated,
            has_next_page=any(x.get("rel") == "next" for x in root.findall(NS + "link")),
            entries=tuple(entries),
        )
    except (ET.ParseError, ValueError, TypeError, OverflowError):
        raise ValueError(invalid) from None
