"""Current issuer observations, without industry-risk or historical-price authority."""

import hashlib
import io
import json
import re
from datetime import date, datetime
from decimal import Decimal
from html.parser import HTMLParser
from typing import Literal
from urllib.parse import urlsplit
from uuid import UUID
from xml.etree import ElementTree as ET
from zipfile import BadZipFile, ZipFile

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError, model_validator
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.time import require_utc
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.research import ResearchEvidenceRef

SOURCE = "ssga_fund_observation"
SCHEMA = "issuer_fund_observation_v1"
_NS = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
_HEADERS = (
    "Name",
    "Ticker",
    "Identifier",
    "SEDOL",
    "Weight",
    "Sector",
    "Shares Held",
    "Local Currency",
)


class IndustryObservation(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    label: str = Field(min_length=1)
    weight_percent: Decimal = Field(allow_inf_nan=False)


class HoldingObservation(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    name: str = Field(min_length=1)
    ticker: str | None
    identifier: str | None
    sedol: str | None
    weight_percent: Decimal = Field(allow_inf_nan=False)
    classification: str | None
    shares_held_text: str | None
    local_currency: str | None


class IssuerFundObservation(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    instrument_id: UUID
    symbol: str = Field(pattern=r"^[A-Z][A-Z0-9.-]{0,14}$")
    isin: str = Field(pattern=r"^US[A-Z0-9]{9}[0-9]$")
    as_of: date
    title: Literal["Fund Industry Allocation"] = "Fund Industry Allocation"
    classification_level: Literal["industry"] = "industry"
    weight_unit: Literal["percent"] = "percent"
    industries: tuple[IndustryObservation, ...] = Field(min_length=1)
    holdings: tuple[HoldingObservation, ...] = Field(min_length=1)
    html_url: str
    workbook_url: str
    html_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    workbook_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    html_byte_length: int = Field(gt=0)
    workbook_byte_length: int = Field(gt=0)
    html_received_at: AwareDatetime
    workbook_received_at: AwareDatetime
    available_at: AwareDatetime
    source_published_at: None = None
    valid_until: None = None
    denominator: None = None
    residual_classification: None = None
    historical_backtest_eligible: Literal[False] = False
    sector_risk_eligible: Literal[False] = False

    @model_validator(mode="after")
    def _validate(self) -> "IssuerFundObservation":
        for value in (self.html_received_at, self.workbook_received_at, self.available_at):
            require_utc(value)
        if self.available_at != max(self.html_received_at, self.workbook_received_at):
            raise ValueError("ISSUER_OBSERVATION_RECEIPT_MISMATCH")
        if self.as_of > min(self.html_received_at, self.workbook_received_at).date():
            raise ValueError("ISSUER_OBSERVATION_DATE_INVALID")
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
                raise ValueError("ISSUER_OBSERVATION_SOURCE_INVALID")
        if len({row.label for row in self.industries}) != len(self.industries):
            raise ValueError("ISSUER_OBSERVATION_DUPLICATE_INDUSTRY")
        return self

    def canonical_bytes(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()


class _Page(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.inputs: dict[str, str] = {}
        self.heading = ""
        self.in_heading = False
        self.allocation_heading = ""

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "h3":
            self.heading = ""
            self.in_heading = True
        values = dict(attrs)
        key = values.get("id")
        if tag == "input" and key in {
            "fund-info-ticker",
            "fund-quick-info",
            "fund-sector-breakdown",
        }:
            assert key is not None
            if key in self.inputs:
                raise ValueError("ISSUER_OBSERVATION_DUPLICATE_INPUT")
            self.inputs[key] = values.get("value") or ""
            if key == "fund-sector-breakdown":
                self.allocation_heading = " ".join(self.heading.split())

    def handle_data(self, data: str) -> None:
        if self.in_heading:
            self.heading += data

    def handle_endtag(self, tag: str) -> None:
        if tag == "h3":
            self.in_heading = False


def _rows(raw: bytes) -> dict[int, dict[str, str]]:
    if len(raw) > 8 * 1024 * 1024:
        raise ValueError("ISSUER_ARCHIVE_TOO_LARGE")
    try:
        with ZipFile(io.BytesIO(raw)) as archive:
            entries = archive.infolist()
            if (
                len(entries) > 100
                or len({x.filename for x in entries}) != len(entries)
                or sum(x.file_size for x in entries) > 32 * 1024 * 1024
            ):
                raise ValueError("ISSUER_ARCHIVE_TOO_LARGE")
            sheets = ET.fromstring(archive.read("xl/workbook.xml")).findall("s:sheets/s:sheet", _NS)
            if (
                len(sheets) != 1
                or sheets[0].get("name") != "holdings"
                or sheets[0].get("sheetId") != "1"
            ):
                raise ValueError("ISSUER_HOLDINGS_LAYOUT_CHANGED")
            strings = []
            if "xl/sharedStrings.xml" in archive.namelist():
                strings = [
                    "".join(x.itertext())
                    for x in ET.fromstring(archive.read("xl/sharedStrings.xml")).findall(
                        "s:si", _NS
                    )
                ]
            sheet = ET.fromstring(archive.read("xl/worksheets/sheet1.xml"))
    except (BadZipFile, KeyError, ET.ParseError) as error:
        raise ValueError("ISSUER_ARCHIVE_INVALID") from error
    result: dict[int, dict[str, str]] = {}
    for row in sheet.findall("s:sheetData/s:row", _NS):
        number = int(row.attrib["r"])
        if number in result:
            raise ValueError("ISSUER_HOLDINGS_LAYOUT_CHANGED")
        cells = {}
        for cell in row.findall("s:c", _NS):
            address = re.fullmatch(r"([A-Z]+)([0-9]+)", cell.get("r", ""))
            if (
                address is None
                or int(address[2]) != number
                or address[1] in cells
                or cell.find("s:f", _NS) is not None
            ):
                raise ValueError("ISSUER_HOLDINGS_LAYOUT_CHANGED")
            value = cell.findtext("s:v", default="", namespaces=_NS)
            if cell.get("t") == "s":
                index = int(value)
                if not 0 <= index < len(strings):
                    raise ValueError("ISSUER_SHARED_STRING_INVALID")
                value = strings[index]
            elif cell.get("t") == "inlineStr":
                inline = cell.find("s:is", _NS)
                if inline is None:
                    raise ValueError("ISSUER_HOLDINGS_LAYOUT_CHANGED")
                value = "".join(inline.itertext())
            cells[address[1]] = value.strip()
        result[number] = cells
    for merged in sheet.findall("s:mergeCells/s:mergeCell", _NS):
        match = re.fullmatch(r"A([0-9]+):I([0-9]+)", merged.get("ref", ""))
        if match and int(match[1]) > 5:
            footer = result.get(int(match[1]), {})
            if footer.get("A") and not any(value for key, value in footer.items() if key != "A"):
                footer["__merged_footer__"] = "true"
    return result


def parse_fund_observation(
    *,
    html: bytes,
    workbook: bytes,
    instrument_id: UUID,
    symbol: str,
    html_url: str,
    workbook_url: str,
    html_received_at: datetime,
    workbook_received_at: datetime,
) -> IssuerFundObservation:
    if len(html) > 4 * 1024 * 1024:
        raise ValueError("ISSUER_IDENTITY_TOO_LARGE")
    page = _Page()
    page.feed(html.decode("utf-8"))
    try:
        info = json.loads(page.inputs["fund-quick-info"])["attrs"]
        allocation = json.loads(page.inputs["fund-sector-breakdown"], parse_float=Decimal)
        title = allocation["label"]
        date_text = allocation["asOfDateSimple"]
        if (
            title != "Fund Industry Allocation"
            or page.allocation_heading != f"{title} as of {date_text}"
            or page.inputs["fund-info-ticker"] != symbol
            or info["fund-ticker"]["value"] != symbol
        ):
            raise ValueError("ISSUER_OBSERVATION_IDENTITY_OR_TITLE_MISMATCH")
        day = datetime.strptime(date_text, "%b %d %Y").date()
        rows = _rows(workbook)
        if (
            rows[2].get("A") != "Ticker Symbol:"
            or rows[2].get("B") != symbol
            or rows[3].get("A") != "Holdings:"
            or datetime.strptime(rows[3]["B"], "As of %d-%b-%Y").date() != day
            or tuple(rows[5].get(chr(65 + i), "") for i in range(8)) != _HEADERS
        ):
            raise ValueError("ISSUER_OBSERVATION_HOLDINGS_MISMATCH")
        for row in allocation["attrArray"]:
            weight = row["weight"]
            shown = weight["value"]
            if (
                not isinstance(shown, str)
                or not shown.endswith("%")
                or Decimal(shown[:-1]) != Decimal(weight["originalValue"])
            ):
                raise ValueError("ISSUER_OBSERVATION_PERCENT_MISMATCH")
        footer_started = False
        for number, row in sorted(rows.items()):
            if number <= 5 or not any(row.values()):
                continue
            if row.get("__merged_footer__") == "true":
                footer_started = True
                continue
            if footer_started or not row.get("E"):
                raise ValueError("ISSUER_OBSERVATION_WEIGHT_MISSING")
        industries = tuple(
            IndustryObservation(
                label=row["name"]["value"], weight_percent=Decimal(row["weight"]["originalValue"])
            )
            for row in allocation["attrArray"]
        )
        holdings = tuple(
            HoldingObservation(
                name=row["A"],
                ticker=row.get("B") or None,
                identifier=row.get("C") or None,
                sedol=row.get("D") or None,
                weight_percent=Decimal(row["E"]),
                classification=row.get("F") or None,
                shares_held_text=row.get("G") or None,
                local_currency=row.get("H") or None,
            )
            for number, row in sorted(rows.items())
            if number > 5 and row.get("E")
        )
        return IssuerFundObservation(
            instrument_id=instrument_id,
            symbol=symbol,
            isin=info["isin"]["value"],
            as_of=day,
            industries=industries,
            holdings=holdings,
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
    except (KeyError, TypeError, ArithmeticError) as error:
        raise ValueError("ISSUER_OBSERVATION_INVALID") from error


async def persist_fund_observation(
    session: AsyncSession, document: IssuerFundObservation, *, location: str
) -> ResearchEvidenceRef:
    content = document.canonical_bytes()
    digest = hashlib.sha256(content).hexdigest()
    object_id = f"fund-observation:{document.instrument_id}:{document.as_of}"
    values = dict(
        source=SOURCE,
        provider_object_id=object_id,
        revision=digest,
        available_at=document.available_at,
    )
    await session.execute(
        insert(ResearchEvidenceRef)
        .values(**values, location=location, content_hash=digest, byte_length=len(content))
        .on_conflict_do_nothing(index_elements=["source", "provider_object_id", "revision"])
    )
    evidence = await session.scalar(
        select(ResearchEvidenceRef).where(
            ResearchEvidenceRef.source == SOURCE,
            ResearchEvidenceRef.provider_object_id == object_id,
            ResearchEvidenceRef.revision == digest,
        )
    )
    if evidence is None or (
        evidence.content_hash,
        evidence.byte_length,
        evidence.available_at,
        evidence.location,
    ) != (digest, len(content), document.available_at, location):
        raise ValueError("ISSUER_OBSERVATION_EVIDENCE_CONFLICT")
    await session.execute(
        insert(DataSnapshot)
        .values(
            **values,
            observed_at=document.available_at,
            received_at=document.available_at,
            schema_version=SCHEMA,
            payload=document.model_dump(mode="json"),
        )
        .on_conflict_do_nothing(index_elements=["source", "provider_object_id", "revision"])
    )
    return evidence


async def read_fund_observations(
    session: AsyncSession,
    *,
    instrument_id: UUID,
    as_of: datetime,
    evidence_ids: tuple[UUID, ...] | None = None,
) -> dict[UUID, IssuerFundObservation]:
    require_utc(as_of)
    query = (
        select(DataSnapshot, ResearchEvidenceRef)
        .join(
            ResearchEvidenceRef,
            (ResearchEvidenceRef.source == DataSnapshot.source)
            & (ResearchEvidenceRef.provider_object_id == DataSnapshot.provider_object_id)
            & (ResearchEvidenceRef.revision == DataSnapshot.revision),
        )
        .where(
            DataSnapshot.source == SOURCE,
            DataSnapshot.schema_version == SCHEMA,
            DataSnapshot.payload["instrument_id"].astext == str(instrument_id),
            DataSnapshot.received_at <= as_of,
            DataSnapshot.available_at <= as_of,
            DataSnapshot.created_at <= as_of,
            ResearchEvidenceRef.available_at <= as_of,
            ResearchEvidenceRef.created_at <= as_of,
        )
        .order_by(DataSnapshot.available_at.desc(), DataSnapshot.id)
        .limit(1)
    )
    if evidence_ids is not None:
        query = query.where(ResearchEvidenceRef.id.in_(evidence_ids))
    result = {}
    with session.no_autoflush:
        for snapshot, evidence in await session.execute(query):
            try:
                document = IssuerFundObservation.model_validate(snapshot.payload)
            except ValidationError:
                continue  # Invalid optional issuer data must not hide valid frozen SEC facts.
            content = document.canonical_bytes()
            if (
                hashlib.sha256(content).hexdigest()
                == snapshot.revision
                == evidence.revision
                == evidence.content_hash
                and len(content) == evidence.byte_length
                and document.available_at
                == snapshot.received_at
                == snapshot.available_at
                == evidence.available_at
            ):
                result[evidence.id] = document
    return result
