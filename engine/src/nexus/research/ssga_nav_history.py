"""Archived issuer NAV observations, without market-price or total-return authority."""

import hashlib
import io
import json
import re
from datetime import date as Date
from datetime import datetime
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
from typing import Literal
from urllib.parse import urlsplit
from uuid import UUID
from xml.etree import ElementTree as ET
from zipfile import BadZipFile, ZipFile

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from nexus.domain.time import require_utc

SOURCE = "ssga_nav_history"
SCHEMA = "issuer_nav_history_v1"
_NS = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
_HEADERS = ("Date", "NAV", "Shares Outstanding", "Total Net Assets")
_NUMBER = re.compile(r"[+-]?[0-9]+(?:\.[0-9]+)?(?:[Ee][+-]?[0-9]+)?")
_DATE = re.compile(r"[0-9]{2}-[A-Za-z]{3}-[0-9]{4}")
_DATE_LIKE = re.compile(
    r"^(?:[0-9]{1,4}[-/.\s][A-Za-z0-9]{1,9}[-/.\s][0-9]{1,4}"
    r"|[A-Za-z]{3,9}\s+[0-9]{1,2},?\s+[0-9]{4})(?:\s|$)"
)


class NavObservation(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    date: Date
    nav: Decimal = Field(gt=0, allow_inf_nan=False)
    shares_outstanding: Decimal | None = Field(default=None, ge=0, allow_inf_nan=False)
    total_net_assets: Decimal | None = Field(default=None, ge=0, allow_inf_nan=False)

    @field_validator("nav", "shares_outstanding", "total_net_assets", mode="before")
    @classmethod
    def _exact_decimal(cls, value: object) -> object:
        if isinstance(value, float | bool):
            raise ValueError("ISSUER_NAV_NUMBER_INVALID")
        return value


class IssuerNavHistory(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    instrument_id: UUID
    symbol: str = Field(pattern=r"^[A-Z][A-Z0-9.-]{0,14}$")
    isin: str = Field(pattern=r"^US[A-Z0-9]{9}[0-9]$")
    observations: tuple[NavObservation, ...] = Field(min_length=1, max_length=25000)
    currency: Literal["USD"] = "USD"
    currency_basis: Literal["issuer_page_base_currency"] = "issuer_page_base_currency"
    nav_unit: Literal["currency_per_share"] = "currency_per_share"
    html_url: str
    workbook_url: str
    html_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    workbook_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    html_byte_length: int = Field(gt=0, le=4 * 1024 * 1024)
    workbook_byte_length: int = Field(gt=0, le=8 * 1024 * 1024)
    html_received_at: AwareDatetime
    workbook_received_at: AwareDatetime
    available_at: AwareDatetime
    source_published_at: None = None
    valid_until: None = None
    revision_method: None = None
    split_adjustment: None = None
    reinvestment_method: None = None
    historical_backtest_eligible: Literal[False] = False
    market_price_eligible: Literal[False] = False
    total_return_eligible: Literal[False] = False

    @model_validator(mode="after")
    def _validate(self) -> "IssuerNavHistory":
        for value in (self.html_received_at, self.workbook_received_at, self.available_at):
            require_utc(value)
        if self.available_at != max(self.html_received_at, self.workbook_received_at):
            raise ValueError("ISSUER_NAV_RECEIPT_MISMATCH")
        days = tuple(item.date for item in self.observations)
        if len(set(days)) != len(days) or any(
            day > self.workbook_received_at.date() for day in days
        ):
            raise ValueError("ISSUER_NAV_DATE_INVALID")
        for source_url, prefix in (
            (self.html_url, "/us/en/individual/etfs/"),
            (self.workbook_url, "/library-content/products/fund-data/etfs/us/"),
        ):
            url = urlsplit(source_url)
            if (
                url.scheme != "https"
                or url.netloc != "www.ssga.com"
                or not url.path.startswith(prefix)
                or url.query
                or url.fragment
                or ".." in url.path
                or "%" in url.path
            ):
                raise ValueError("ISSUER_NAV_SOURCE_INVALID")
        return self

    def canonical_bytes(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()


class _Page(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.inputs: dict[str, str] = {}
        self.stack: list[tuple[str, int, str, list[str]]] = []
        self.counter = 0
        self.labels: dict[int, str] = {}
        self.currencies: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = dict(attrs)
        key = values.get("id")
        if tag == "input" and key in {"fund-info-ticker", "fund-quick-info"}:
            assert key is not None
            if key in self.inputs:
                raise ValueError("ISSUER_NAV_IDENTITY_INVALID")
            self.inputs[key] = values.get("value") or ""
        if tag not in {
            "area",
            "base",
            "br",
            "col",
            "embed",
            "hr",
            "img",
            "input",
            "link",
            "meta",
            "param",
            "source",
            "track",
            "wbr",
        }:
            self.counter += 1
            classes = (values.get("class") or "").split()
            kind = "label" if "label" in classes else "data" if "data" in classes else ""
            self.stack.append((tag, self.counter, kind, []))

    def handle_data(self, data: str) -> None:
        for _, _, kind, parts in self.stack:
            if kind:
                parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        for index in range(len(self.stack) - 1, -1, -1):
            if self.stack[index][0] == tag:
                _, _, kind, parts = self.stack[index]
                parent = self.stack[index - 1][1] if index else 0
                value = " ".join("".join(parts).split())
                if kind == "label":
                    self.labels[parent] = value
                elif kind == "data" and self.labels.get(parent) == "Base Currency":
                    self.currencies.append(value)
                del self.stack[index:]
                return


def _rows(raw: bytes) -> dict[int, dict[str, str]]:
    with ZipFile(io.BytesIO(raw)) as archive:
        entries = archive.infolist()
        if (
            len(entries) > 100
            or len({x.filename for x in entries}) != len(entries)
            or sum(x.file_size for x in entries) > 32 * 1024 * 1024
        ):
            raise ValueError("ISSUER_NAV_ARCHIVE_TOO_LARGE")

        def xml(path: str) -> ET.Element:
            content = archive.read(path).decode("utf-8-sig")
            if "\x00" in content or "<!DOCTYPE" in content.upper() or "<!ENTITY" in content.upper():
                raise ValueError("ISSUER_NAV_XML_INVALID")
            return ET.fromstring(content)

        sheets = xml("xl/workbook.xml").findall("s:sheets/s:sheet", _NS)
        if (
            len(sheets) != 1
            or sheets[0].get("name") != "navhist"
            or sheets[0].get("sheetId") != "1"
        ):
            raise ValueError("ISSUER_NAV_LAYOUT_INVALID")
        strings = (
            ["".join(x.itertext()) for x in xml("xl/sharedStrings.xml").findall("s:si", _NS)]
            if "xl/sharedStrings.xml" in archive.namelist()
            else []
        )
        sheet = xml("xl/worksheets/sheet1.xml")
    result: dict[int, dict[str, str]] = {}
    for row in sheet.findall("s:sheetData/s:row", _NS):
        number = int(row.attrib["r"])
        if not 1 <= number <= 30000 or number in result:
            raise ValueError("ISSUER_NAV_LAYOUT_INVALID")
        cells: dict[str, str] = {}
        for cell in row.findall("s:c", _NS):
            address = re.fullmatch(r"([A-Z]{1,3})([0-9]+)", cell.get("r", ""))
            if (
                address is None
                or int(address[2]) != number
                or address[1] in cells
                or cell.find("s:f", _NS) is not None
            ):
                raise ValueError("ISSUER_NAV_LAYOUT_INVALID")
            value = cell.findtext("s:v", default="", namespaces=_NS)
            kind = cell.get("t")
            if kind == "s":
                index = int(value)
                if not 0 <= index < len(strings):
                    raise ValueError("ISSUER_NAV_STRING_INVALID")
                value = strings[index]
            elif kind == "inlineStr":
                inline = cell.find("s:is", _NS)
                if inline is None:
                    raise ValueError("ISSUER_NAV_LAYOUT_INVALID")
                value = "".join(inline.itertext())
            elif kind not in {None, "n"}:
                raise ValueError("ISSUER_NAV_CELL_INVALID")
            cells[address[1]] = value.strip()
        result[number] = cells
    return result


def _observations(rows: dict[int, dict[str, str]], symbol: str) -> tuple[NavObservation, ...]:
    if (
        rows[2].get("A") != "Ticker Symbol:"
        or rows[2].get("B") != symbol
        or tuple(rows[4].get(chr(65 + i), "") for i in range(4)) != _HEADERS
    ):
        raise ValueError("ISSUER_NAV_IDENTITY_OR_HEADER_INVALID")
    result = []
    last_data = 4
    footer = False
    for number, cells in sorted(rows.items()):
        if number <= 4 or not any(cells.values()):
            continue
        nonempty = {key: value for key, value in cells.items() if value}
        text = cells.get("A", "")
        if _DATE.fullmatch(text):
            if footer or number != last_data + 1 or any(k not in "ABCD" for k in nonempty):
                raise ValueError("ISSUER_NAV_LAYOUT_INVALID")
            values = []
            for key in "BCD":
                value = cells.get(key, "")
                if key in "CD" and value == "-":
                    value = ""
                if value and not _NUMBER.fullmatch(value):
                    raise ValueError("ISSUER_NAV_NUMBER_INVALID")
                values.append(Decimal(value) if value else None)
            if values[0] is None:
                raise ValueError("ISSUER_NAV_NUMBER_INVALID")
            result.append(
                NavObservation(
                    date=datetime.strptime(text, "%d-%b-%Y").date(),
                    nav=values[0],
                    shares_outstanding=values[1],
                    total_net_assets=values[2],
                )
            )
            last_data = number
        elif (
            result
            and number > last_data + 1
            and text
            and not _DATE_LIKE.match(text)
            and set(nonempty).issubset({"A", "F"})
            and not any(_NUMBER.fullmatch(v) for v in nonempty.values())
        ):
            # Publisher footer follows a blank separation; never ignore a damaged data row.
            footer = True
        else:
            raise ValueError("ISSUER_NAV_LAYOUT_INVALID")
    return tuple(result)


def parse_nav_history(
    *,
    html: bytes,
    workbook: bytes,
    instrument_id: UUID,
    symbol: str,
    html_url: str,
    workbook_url: str,
    html_received_at: datetime,
    workbook_received_at: datetime,
) -> IssuerNavHistory:
    if not html or len(html) > 4 * 1024 * 1024 or not workbook or len(workbook) > 8 * 1024 * 1024:
        raise ValueError("ISSUER_NAV_ARCHIVE_SIZE_INVALID")
    try:
        page = _Page()
        page.feed(html.decode("utf-8"))
        info = json.loads(page.inputs["fund-quick-info"])["attrs"]
        if (
            page.inputs["fund-info-ticker"] != symbol
            or info["fund-ticker"]["value"] != symbol
            or not page.currencies
            or set(page.currencies) != {"USD"}
        ):
            raise ValueError("ISSUER_NAV_IDENTITY_OR_CURRENCY_INVALID")
        return IssuerNavHistory(
            instrument_id=instrument_id,
            symbol=symbol,
            isin=info["isin"]["value"],
            observations=_observations(_rows(workbook), symbol),
            html_url=html_url,
            workbook_url=workbook_url,
            html_content_hash=hashlib.sha256(html).hexdigest(),
            workbook_content_hash=hashlib.sha256(workbook).hexdigest(),
            html_byte_length=len(html),
            workbook_byte_length=len(workbook),
            html_received_at=html_received_at,
            workbook_received_at=workbook_received_at,
            available_at=max(html_received_at, workbook_received_at),
        )
    except (
        ValueError,
        TypeError,
        KeyError,
        IndexError,
        InvalidOperation,
        BadZipFile,
        ET.ParseError,
    ):
        raise ValueError("ISSUER_NAV_SOURCE_INVALID") from None
