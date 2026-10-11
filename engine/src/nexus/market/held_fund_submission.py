"""One exact fund class bound to the sole 497K in a complete SEC submission.

This structural result is not a current identity authority: listing applicability,
MIC originals, directory conflicts, broker exposure and storage bindings remain
separate mandatory checks before a held-only proof may be registered.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from hashlib import sha256
from zoneinfo import ZoneInfo

from nexus.domain.time import require_utc
from nexus.research.sec_disclosures import _VisibleText

_URL = re.compile(
    r"https://www\.sec\.gov/Archives/edgar/data/([1-9][0-9]{0,9})/"
    r"([0-9]{18})/([0-9]{10}-[0-9]{2}-[0-9]{6})\.txt"
)
_MAX_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True)
class SingleClassSubmission:
    cik: str
    series_id: str
    class_id: str
    class_name: str
    symbol: str
    accession: str
    filed_on: date
    component_filename: str
    component_raw: bytes = field(repr=False)
    component_sha256: str
    listing_name: str = "Cboe BZX Exchange, Inc."
    binding_basis: str = "single_class_submission_and_unique_497k_cover"
    header_ticker_status: str = "absent"


def _value(text: str, label: str, *, tag: bool = False) -> str:
    prefix = f"<{label}>" if tag else label + ":"
    values: list[str] = re.findall(r"^\s*" + re.escape(prefix) + r"([^\r\n]*)", text, re.M)
    if len(values) != 1 or not values[0].strip():
        raise ValueError
    return values[0].strip()


def _scope(text: str, name: str) -> str:
    # Count exact containers before taking their body, rejecting nested scopes.
    opening, closing = f"<{name}>", f"</{name}>"
    if text.count(opening) != 1 or text.count(closing) != 1:
        raise ValueError
    start, end = text.index(opening) + len(opening), text.index(closing)
    if start > end:
        raise ValueError
    return text[start:end]


def _name(value: str) -> str:
    return " ".join(value.split())


def parse_single_class_submission(
    *, raw: bytes, source_url: str, symbol: str, now: datetime
) -> SingleClassSubmission:
    """Fail closed on ambiguous containers, components, names or cover tickers."""
    try:
        cutoff = require_utc(now)
        url = _URL.fullmatch(source_url)
        if (
            not url
            or url[2] != url[3].replace("-", "")
            or not 0 < len(raw) <= _MAX_BYTES
            or not re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,15}", symbol)
        ):
            raise ValueError
        text = raw.decode("utf-8-sig")
        document = _scope(text, "SEC-DOCUMENT")
        if text[: text.index("<SEC-DOCUMENT>")].strip() or text.split("</SEC-DOCUMENT>")[1].strip():
            raise ValueError
        header = _scope(document, "SEC-HEADER")
        for label in (
            "OWNER-CIK",
            "SERIES-ID",
            "SERIES-NAME",
            "CLASS-CONTRACT-ID",
            "CLASS-CONTRACT-NAME",
        ):
            if header.count(f"<{label}>") != 1:
                raise ValueError
        if _value(header, "ACCESSION NUMBER") != url[3]:
            raise ValueError
        if _value(header, "CONFORMED SUBMISSION TYPE") != "497K":
            raise ValueError
        if len(re.findall(r"^\s*FILER:\s*$", header, re.M)) != 1:
            raise ValueError
        filer = _value(header, "CENTRAL INDEX KEY")
        if not re.fullmatch(r"[0-9]{10}", filer) or int(filer) != int(url[1]):
            raise ValueError
        filed = datetime.strptime(_value(header, "FILED AS OF DATE"), "%Y%m%d").date()
        accepted = _value(header, "ACCEPTANCE-DATETIME", tag=True)
        if not re.fullmatch(r"[0-9]{14}", accepted):
            raise ValueError
        accepted_at = datetime.strptime(accepted, "%Y%m%d%H%M%S").replace(
            tzinfo=ZoneInfo("America/New_York")
        )
        if accepted_at > cutoff or filed > accepted_at.date():
            raise ValueError
        if (
            filed > cutoff.date()
            or datetime.strptime(accepted[:8], "%Y%m%d").date() > cutoff.date()
        ):
            raise ValueError
        series_container = _scope(
            _scope(header, "SERIES-AND-CLASSES-CONTRACTS-DATA"),
            "EXISTING-SERIES-AND-CLASSES-CONTRACTS",
        )
        series_scope = _scope(series_container, "SERIES")
        class_scope = _scope(series_scope, "CLASS-CONTRACT")
        if _value(series_scope, "OWNER-CIK", tag=True) != filer:
            raise ValueError
        series = _value(series_scope, "SERIES-ID", tag=True)
        share_class = _value(class_scope, "CLASS-CONTRACT-ID", tag=True)
        if not re.fullmatch(r"S[0-9]{9}", series) or not re.fullmatch(r"C[0-9]{9}", share_class):
            raise ValueError
        class_name = _name(_value(class_scope, "CLASS-CONTRACT-NAME", tag=True))
        if class_name != _name(_value(series_scope, "SERIES-NAME", tag=True)):
            raise ValueError
        # This basis only describes a missing header ticker, never a conflict.
        if "<CLASS-CONTRACT-TICKER-SYMBOL>" in header:
            raise ValueError
        remainder = document.split("</SEC-HEADER>", 1)[1]
        components = re.findall(r"<DOCUMENT>(.*?)</DOCUMENT>", remainder, re.S)
        if (
            len(components) != int(_value(header, "PUBLIC DOCUMENT COUNT"))
            or remainder.count("<DOCUMENT>") != len(components)
            or remainder.count("</DOCUMENT>") != len(components)
            or re.sub(r"<DOCUMENT>.*?</DOCUMENT>", "", remainder, flags=re.S).strip()
        ):
            raise ValueError
        formal: tuple[str, bytes] | None = None
        sequences: set[int] = set()
        filenames: set[str] = set()
        for component in components:
            kind = _value(component, "TYPE", tag=True)
            sequence = int(_value(component, "SEQUENCE", tag=True))
            filename = _value(component, "FILENAME", tag=True)
            if (
                sequence < 1
                or sequence in sequences
                or filename in filenames
                or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", filename)
            ):
                raise ValueError
            sequences.add(sequence)
            filenames.add(filename)
            body = _scope(component, "TEXT")
            if kind == "497K":
                if formal is not None or sequence != 1 or not filename.endswith((".htm", ".html")):
                    raise ValueError
                formal = filename, body.encode("utf-8")
            elif kind != "GRAPHIC":
                raise ValueError
        if formal is None:
            raise ValueError
        html = formal[1].decode("utf-8")
        if (
            len(re.findall(r"<html(?:\s[^>]*)?>", html, re.I)) != 1
            or len(re.findall(r"</html\s*>", html, re.I)) != 1
        ):
            raise ValueError
        parser = _VisibleText()
        parser.feed(html)
        parser.close()
        visible = _name("".join(parser.parts))
        cover = visible.split("Summary Prospectus", 1)
        if len(cover) != 2 or len(cover[0]) > 1500:
            raise ValueError
        pattern = r"Trading Symbol:\s*([A-Z][A-Z0-9.\-]{0,15})(?=\s|$)"
        tickers = re.findall(pattern, cover[0])
        if tickers != [symbol]:
            raise ValueError
        name_prefix = re.split(r"Trading Symbol:", cover[0])[0].strip()
        if name_prefix != class_name:
            raise ValueError
        suffix = re.split(pattern, cover[0], maxsplit=1)[-1].strip()
        if suffix != "Listed on Cboe BZX Exchange, Inc.":
            raise ValueError
        return SingleClassSubmission(
            cik=filer,
            series_id=series,
            class_id=share_class,
            class_name=class_name,
            symbol=symbol,
            accession=url[3],
            filed_on=filed,
            component_filename=formal[0],
            component_raw=formal[1],
            component_sha256=sha256(formal[1]).hexdigest(),
        )
    except (ValueError, TypeError, IndexError, UnicodeError, OverflowError):
        raise ValueError("HELD_FUND_SUBMISSION_INVALID") from None
