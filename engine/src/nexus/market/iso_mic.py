"""Exact ISO BZX equities segment, parent and publication applicability from originals."""

import csv
import io
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Literal

from nexus.domain.time import require_utc
from nexus.market.cboe_listing import _one, _visible

_FIELDS = (
    "MIC",
    "OPERATING MIC",
    "OPRT/SGMT",
    "MARKET NAME-INSTITUTION DESCRIPTION",
    "LEGAL ENTITY NAME",
    "LEI",
    "MARKET CATEGORY CODE",
    "ACRONYM",
    "ISO COUNTRY CODE (ISO 3166)",
    "CITY",
    "WEBSITE",
    "STATUS",
    "CREATION DATE",
    "LAST UPDATE DATE",
    "LAST VALIDATION DATE",
    "EXPIRY DATE",
    "COMMENTS",
)
_HEADERS = [
    "Excel format",
    "PDF Format",
    "Comma Separated Values",
    "Extended Markup Language and Schema",
    "Annexes",
    "Publication date",
    "Modification implementation date",
    "Next publication date",
]


@dataclass(frozen=True)
class BzxMicObservation:
    mic: str
    operating_mic: str
    market_name: str
    publication_on: date
    implementation_on: date
    next_publication_on: date
    segment_updated_on: date
    parent_updated_on: date
    segment_row: tuple[str, ...]
    parent_row: tuple[str, ...]
    applicability_basis: Literal[
        "published_effective_dataset", "unchanged_from_previous_effective_dataset"
    ]


def parse_bzx_mic(
    *,
    publication_html: bytes,
    mic_csv: bytes,
    now: datetime,
    previous: tuple[bytes, bytes] | None = None,
) -> BzxMicObservation:
    try:
        cutoff = require_utc(now).date()
        if any(not 0 < len(raw) <= 8 * 1024 * 1024 for raw in (publication_html, mic_csv)):
            raise ValueError
        tables: list[str] = re.findall(
            r"<table(?:\s[^>]*)?>(.*?)</table>", publication_html.decode("utf-8-sig"), re.S | re.I
        )
        tables = [
            t for t in tables if 'href="/sites/default/files/ISO10383_MIC/ISO10383_MIC.csv"' in t
        ]
        if len(tables) != 1:
            raise ValueError
        headers = [
            _visible(v)
            for v in re.findall(
                r"<th(?:\s[^>]*)?>(.*?)</th>", _one(tables[0], "thead"), re.S | re.I
            )
        ]
        cells = [
            _visible(v)
            for v in re.findall(
                r"<td(?:\s[^>]*)?>(.*?)</td>", _one(_one(tables[0], "tbody"), "tr"), re.S | re.I
            )
        ]
        if headers != _HEADERS or len(cells) != 8:
            raise ValueError
        publication, implementation, next_publication = (
            datetime.strptime(value, "%d %B %Y").date() for value in cells[-3:]
        )
        if not publication <= cutoff < next_publication or implementation < publication:
            raise ValueError
        reader = csv.reader(io.StringIO(mic_csv.decode("utf-8-sig")), strict=True)
        if tuple(next(reader)) != _FIELDS:
            raise ValueError
        rows: dict[str, dict[str, str]] = {}
        for values in reader:
            if (
                len(values) != len(_FIELDS)
                or values[0] in rows
                or not re.fullmatch(r"[A-Z0-9]{4}", values[0])
            ):
                raise ValueError
            rows[values[0]] = dict(zip(_FIELDS, values, strict=True))
        segment, parent = rows["BATS"], rows["XCBO"]
        if (
            segment["OPERATING MIC"] != "XCBO"
            or segment["OPRT/SGMT"] != "SGMT"
            or segment["MARKET NAME-INSTITUTION DESCRIPTION"] != "CBOE BZX U.S. EQUITIES EXCHANGE"
            or parent["OPERATING MIC"] != "XCBO"
            or parent["OPRT/SGMT"] != "OPRT"
        ):
            raise ValueError
        updated: list[date] = []
        for row in (segment, parent):
            created = datetime.strptime(row["CREATION DATE"], "%Y%m%d").date()
            changed = datetime.strptime(row["LAST UPDATE DATE"], "%Y%m%d").date()
            if (
                row["STATUS"] != "ACTIVE"
                or row["ISO COUNTRY CODE (ISO 3166)"] != "US"
                or row["EXPIRY DATE"]
                or not created <= changed <= publication
            ):
                raise ValueError
            if (
                row["LAST VALIDATION DATE"]
                and datetime.strptime(row["LAST VALIDATION DATE"], "%Y%m%d").date() > cutoff
            ):
                raise ValueError
            updated.append(changed)
        segment_row = tuple(segment[field] for field in _FIELDS)
        parent_row = tuple(parent[field] for field in _FIELDS)
        basis: Literal[
            "published_effective_dataset", "unchanged_from_previous_effective_dataset"
        ] = "published_effective_dataset"
        if cutoff < implementation:
            if previous is None or len(previous) != 2:
                raise ValueError
            prior = parse_bzx_mic(
                publication_html=previous[0],
                mic_csv=previous[1],
                now=datetime.combine(publication, datetime.min.time(), tzinfo=UTC)
                - timedelta(microseconds=1),
            )
            if (
                prior.next_publication_on != publication
                or prior.segment_row != segment_row
                or prior.parent_row != parent_row
            ):
                raise ValueError
            basis = "unchanged_from_previous_effective_dataset"
        elif previous is not None:
            raise ValueError
        return BzxMicObservation(
            "BATS",
            "XCBO",
            segment["MARKET NAME-INSTITUTION DESCRIPTION"],
            publication,
            implementation,
            next_publication,
            updated[0],
            updated[1],
            segment_row,
            parent_row,
            basis,
        )
    except (ValueError, TypeError, UnicodeError, csv.Error, StopIteration, KeyError):
        raise ValueError("MIC_BZX_INVALID") from None
