"""Cross-check a dated official listed directory against its CSV and product page.

This observation is not a MIC, fund-class or broker-exposure identity proof.
"""

import csv
import io
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

from nexus.domain.time import require_utc
from nexus.research.sec_disclosures import _VisibleText

_SYMBOL = re.compile(r"[A-Z][A-Z0-9.\-]{0,15}")
_CSV_COLUMNS = [
    "Name",
    "Volume",
    "Ask Size",
    "Ask Price",
    "Bid Size",
    "Bid Price",
    "Last Price",
    "Shares Matched",
    "Shares Routed",
]
_HTML_COLUMNS = [
    "Symbol",
    "Volume",
    "Matched",
    "Routed",
    "Bid Size",
    "Bid Price",
    "Ask Size",
    "Ask Price",
    "Last Price",
]


@dataclass(frozen=True)
class CboeListedObservation:
    symbol: str
    directory_as_of: datetime
    product_name: str
    listed_on: date


def _visible(fragment: str) -> str:
    parser = _VisibleText()
    parser.feed(fragment)
    parser.close()
    return " ".join("".join(parser.parts).split())


def _one(fragment: str, tag: str) -> str:
    matches: list[str] = re.findall(
        r"<" + tag + r"(?:\s[^>]*)?>(.*?)</" + tag + r"\s*>", fragment, re.S | re.I
    )
    if len(matches) != 1:
        raise ValueError
    return matches[0]


def parse_cboe_listing(
    *, listed_html: bytes, listed_csv: bytes, product_html: bytes, symbol: str, now: datetime
) -> CboeListedObservation:
    """Require whole-directory agreement and an observation no older than 24 hours."""
    try:
        cutoff = require_utc(now)
        if not _SYMBOL.fullmatch(symbol) or any(
            not 0 < len(raw) <= 8 * 1024 * 1024 for raw in (listed_html, listed_csv, product_html)
        ):
            raise ValueError
        html = listed_html.decode("utf-8-sig")
        if _visible(_one(html, "h1")) != "Cboe Listed Symbols":
            raise ValueError
        stamps: list[tuple[str, str]] = re.findall(
            r"The following symbols are listed on Cboe\. This list was last updated as of\s+"
            r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (EDT|EST)\.",
            _visible(html),
        )
        if len(stamps) != 1:
            raise ValueError
        stamp, zone = stamps[0]
        local = datetime.strptime(stamp, "%Y-%m-%d %H:%M:%S")
        new_york = ZoneInfo("America/New_York")
        # The explicit abbreviation selects the fold; a UTC roundtrip rejects
        # spring-forward wall times that never occurred.
        instants = {
            candidate.astimezone(UTC)
            for fold in (0, 1)
            if (candidate := local.replace(tzinfo=new_york, fold=fold)).strftime("%Z") == zone
            and candidate.astimezone(UTC).astimezone(new_york).replace(tzinfo=None) == local
        }
        if len(instants) != 1:
            raise ValueError
        as_of = instants.pop()
        if not timedelta(0) <= cutoff - as_of <= timedelta(hours=24):
            raise ValueError
        table = _one(html, "table")
        headers = [
            _visible(cell)
            for cell in re.findall(r"<th(?:\s[^>]*)?>(.*?)</th>", _one(table, "thead"), re.S | re.I)
        ]
        if headers != _HTML_COLUMNS:
            raise ValueError
        body = _one(table, "tbody")
        rows: list[str] = re.findall(r"<tr(?:\s[^>]*)?>(.*?)</tr>", body, re.S | re.I)
        if len(rows) != len(re.findall(r"<tr(?:\s[^>]*)?>", body, re.I)):
            raise ValueError
        names: list[str] = []
        for row in rows:
            cells: list[str] = re.findall(r"<td(?:\s[^>]*)?>(.*?)</td>", row, re.S | re.I)
            if len(cells) != 9:
                raise ValueError
            name = _visible(cells[0])
            if not _SYMBOL.fullmatch(name) or name in names:
                raise ValueError
            links = re.findall(r'<a\s[^>]*href="([^"]+)"[^>]*>', cells[0], re.I)
            if links != [f"/us/equities/listings/listed_products/symbols/{name}/"]:
                raise ValueError
            names.append(name)
        reader = csv.reader(io.StringIO(listed_csv.decode("utf-8-sig")), strict=True)
        if next(reader) != _CSV_COLUMNS:
            raise ValueError
        csv_names: list[str] = []
        for values in reader:
            if len(values) != 9 or not _SYMBOL.fullmatch(values[0]) or values[0] in csv_names:
                raise ValueError
            csv_names.append(values[0])
        if set(csv_names) != set(names) or symbol not in names:
            raise ValueError
        main = _one(product_html.decode("utf-8-sig"), "main")
        # The official product title has class=h2; the separate Book Viewer also has h1.
        product_titles: list[str] = re.findall(
            r'<h1\s[^>]*class="h2"[^>]*>(.*?)</h1\s*>', main, re.S | re.I
        )
        if len(product_titles) != 1:
            raise ValueError
        name = _visible(product_titles[0])
        if not name or _visible(_one(main, "h2")) != f"Cboe: {symbol}":
            raise ValueError
        listed: list[str] = re.findall(r"\bListed ([A-Z][a-z]+ \d{1,2}, \d{4})\b", _visible(main))
        if len(listed) != 1:
            raise ValueError
        listed_on = datetime.strptime(listed[0], "%B %d, %Y").date()
        if listed_on > as_of.date():
            raise ValueError
        return CboeListedObservation(
            symbol, require_utc(as_of.astimezone(cutoff.tzinfo)), name, listed_on
        )
    except (ValueError, TypeError, UnicodeError, csv.Error, StopIteration):
        raise ValueError("CBOE_LISTING_INVALID") from None
