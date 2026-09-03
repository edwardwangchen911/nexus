"""Issuer cash-distribution observations for current research, never historical PIT authority."""

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

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.time import require_utc
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.research import ResearchEvidenceRef

SOURCE = "ssga_distribution"
SCHEMA = "issuer_distribution_v1"
SOURCE_URL = "https://www.ssga.com/library-content/products/fund-data/etfs/us/spdr-etf-historical-distributions.xlsx"
_NS = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
_HEADERS = (
    "FUND NAME",
    "TICKER",
    "CUSIP",
    "EX-DATE",
    "RECORD DATE",
    "PAYABLE DATE",
    "DIVIDEND ($)",
    "SHORT TERM CAPITAL GAIN ($)",
    "LONG TERM CAPITAL GAIN ($)",
    "FREQUENCY",
)


class IssuerDistribution(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    instrument_id: UUID
    symbol: str = Field(pattern=r"^[A-Z][A-Z0-9.-]{0,14}$")
    cusip: str = Field(pattern=r"^[A-Z0-9]{9}$")
    ex_date: date
    record_date: date
    payable_date: date
    dividend_per_share: Decimal = Field(gt=0, allow_inf_nan=False)
    short_term_capital_gain: Decimal | None = Field(default=None, ge=0, allow_inf_nan=False)
    long_term_capital_gain: Decimal | None = Field(default=None, ge=0, allow_inf_nan=False)
    currency: Literal["USD"] = "USD"
    amount_basis: Literal["per_share"] = "per_share"
    observation_kind: Literal["issuer_historical_distribution"] = "issuer_historical_distribution"
    historical_backtest_eligible: Literal[False] = False
    source_reported_available_at: None = None
    source_url: str = SOURCE_URL
    identity_url: str
    raw_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    identity_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    raw_byte_length: int = Field(gt=0)
    identity_byte_length: int = Field(gt=0)
    available_at: AwareDatetime

    @model_validator(mode="after")
    def validate_identity_and_dates(self) -> "IssuerDistribution":
        require_utc(self.available_at)
        url = urlsplit(self.identity_url)
        if self.source_url != SOURCE_URL:
            raise ValueError("ISSUER_DISTRIBUTION_SOURCE_INVALID")
        if (
            url.scheme != "https"
            or url.netloc != "www.ssga.com"
            or not url.path.startswith("/us/en/individual/etfs/")
            or url.query
            or url.fragment
            or ".." in url.path
            or not url.path.endswith("-" + self.symbol.lower())
        ):
            raise ValueError("ISSUER_IDENTITY_URL_INVALID")
        if not self.ex_date <= self.record_date <= self.payable_date <= self.available_at.date():
            raise ValueError("ISSUER_DISTRIBUTION_DATES_INVALID")
        return self

    def canonical_bytes(self) -> bytes:
        return json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()


class _Inputs(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.values: dict[str, str] = {}

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = dict(attrs)
        key = values.get("id")
        if tag == "input" and key in {"fund-info-ticker", "fund-quick-info"}:
            assert key is not None
            if key in self.values:
                raise ValueError("ISSUER_IDENTITY_DUPLICATE")
            self.values[key] = values.get("value") or ""


def _workbook_rows(raw: bytes) -> list[dict[str, str]]:
    if len(raw) > 8 * 1024 * 1024:
        raise ValueError("ISSUER_ARCHIVE_TOO_LARGE")
    try:
        with ZipFile(io.BytesIO(raw)) as archive:
            if (
                len(archive.infolist()) > 100
                or sum(i.file_size for i in archive.infolist()) > 32 * 1024 * 1024
            ):
                raise ValueError("ISSUER_ARCHIVE_TOO_LARGE")
            workbook = ET.fromstring(archive.read("xl/workbook.xml"))
            sheets = workbook.findall("s:sheets/s:sheet", _NS)
            if (
                len(sheets) != 1
                or sheets[0].get("name") != "dividend"
                or sheets[0].get("sheetId") != "1"
            ):
                raise ValueError("ISSUER_WORKBOOK_LAYOUT_CHANGED")
            strings = [
                "".join(si.itertext())
                for si in ET.fromstring(archive.read("xl/sharedStrings.xml")).findall("s:si", _NS)
            ]
            sheet = ET.fromstring(archive.read("xl/worksheets/sheet1.xml"))
    except (BadZipFile, KeyError, ET.ParseError) as error:
        raise ValueError("ISSUER_ARCHIVE_INVALID") from error
    rows = []
    for row in sheet.findall("s:sheetData/s:row", _NS):
        cells = {}
        for cell in row.findall("s:c", _NS):
            address = re.fullmatch(r"([A-Z]+)[0-9]+", cell.get("r", ""))
            if address is None or cell.find("s:f", _NS) is not None:
                raise ValueError("ISSUER_WORKBOOK_LAYOUT_CHANGED")
            value = cell.findtext("s:v", default="", namespaces=_NS)
            if cell.get("t") == "s":
                try:
                    value = strings[int(value)]
                except (IndexError, ValueError) as error:
                    raise ValueError("ISSUER_SHARED_STRING_INVALID") from error
            elif cell.get("t") == "inlineStr":
                value = "".join(cell.findtext("s:is/s:t", default="", namespaces=_NS))
            cells[address[1]] = value.strip()
        rows.append(cells)
    if not rows or tuple(rows[0].get(chr(65 + i), "") for i in range(10)) != _HEADERS:
        raise ValueError("ISSUER_WORKBOOK_COLUMNS_CHANGED")
    return rows[1:]


def parse_distribution(
    *,
    raw: bytes,
    identity_html: bytes,
    instrument_id: UUID,
    symbol: str,
    identity_url: str,
    received_at: datetime,
) -> IssuerDistribution:
    require_utc(received_at)
    if len(identity_html) > 4 * 1024 * 1024:
        raise ValueError("ISSUER_IDENTITY_TOO_LARGE")
    inputs = _Inputs()
    inputs.feed(identity_html.decode("utf-8"))
    try:
        attributes = json.loads(inputs.values["fund-quick-info"])["attrs"]
        isin = attributes["isin"]["value"]
        if (
            inputs.values["fund-info-ticker"] != symbol
            or attributes["fund-ticker"]["value"] != symbol
        ):
            raise ValueError("ISSUER_TICKER_MISMATCH")
        if not isinstance(isin, str) or not re.fullmatch(r"US[A-Z0-9]{9}[0-9]", isin):
            raise ValueError("ISSUER_US_SHARE_IDENTITY_REQUIRED")
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        raise ValueError("ISSUER_IDENTITY_INVALID") from error
    candidates = []
    for row in _workbook_rows(raw):
        if row.get("B") != symbol:
            continue
        if row.get("C") != isin[2:11]:
            raise ValueError("ISSUER_SHARE_CLASS_MISMATCH")
        parsed_dates = [datetime.strptime(row[k], "%m/%d/%Y").date() for k in "DEF"]
        if parsed_dates[-1] > received_at.date():
            continue  # This entry imports completed-date historical observations only.
        if not all(row.get(k) for k in "DEFG"):
            raise ValueError("ISSUER_DISTRIBUTION_INCOMPLETE")
        candidates.append((parsed_dates, row))
    if not candidates:
        raise ValueError("ISSUER_DISTRIBUTION_UNAVAILABLE")
    candidates.sort(key=lambda item: item[0][0], reverse=True)
    dates, latest = candidates[0]
    if sum(item[0][0] == dates[0] for item in candidates) != 1:
        raise ValueError("ISSUER_DISTRIBUTION_DUPLICATE")
    return IssuerDistribution(
        instrument_id=instrument_id,
        symbol=symbol,
        cusip=isin[2:11],
        ex_date=dates[0],
        record_date=dates[1],
        payable_date=dates[2],
        dividend_per_share=Decimal(latest["G"]),
        short_term_capital_gain=Decimal(latest["H"]) if latest.get("H") else None,
        long_term_capital_gain=Decimal(latest["I"]) if latest.get("I") else None,
        identity_url=identity_url,
        raw_content_hash=hashlib.sha256(raw).hexdigest(),
        identity_content_hash=hashlib.sha256(identity_html).hexdigest(),
        raw_byte_length=len(raw),
        identity_byte_length=len(identity_html),
        available_at=received_at,
    )


async def persist_distribution(
    session: AsyncSession, document: IssuerDistribution, *, location: str
) -> ResearchEvidenceRef:
    content = document.canonical_bytes()
    digest = hashlib.sha256(content).hexdigest()
    object_id = f"distribution:{document.cusip}:{document.ex_date.isoformat()}"
    await session.execute(
        insert(ResearchEvidenceRef)
        .values(
            source=SOURCE,
            provider_object_id=object_id,
            revision=digest,
            available_at=document.available_at,
            location=location,
            content_hash=digest,
            byte_length=len(content),
        )
        .on_conflict_do_nothing(index_elements=["source", "provider_object_id", "revision"])
    )
    evidence = await session.scalar(
        select(ResearchEvidenceRef).where(
            ResearchEvidenceRef.source == SOURCE,
            ResearchEvidenceRef.provider_object_id == object_id,
            ResearchEvidenceRef.revision == digest,
        )
    )
    assert evidence is not None
    if (evidence.content_hash, evidence.byte_length, evidence.available_at) != (
        digest,
        len(content),
        document.available_at,
    ):
        raise ValueError("ISSUER_EVIDENCE_CONFLICT")
    await session.execute(
        insert(DataSnapshot)
        .values(
            source=SOURCE,
            provider_object_id=object_id,
            revision=digest,
            observed_at=document.available_at,
            available_at=document.available_at,
            received_at=document.available_at,
            schema_version=SCHEMA,
            payload=document.model_dump(mode="json"),
        )
        .on_conflict_do_nothing(index_elements=["source", "provider_object_id", "revision"])
    )
    return evidence


async def read_distributions(
    session: AsyncSession,
    *,
    instrument_id: UUID,
    as_of: datetime,
    evidence_ids: tuple[UUID, ...] | None = None,
) -> dict[UUID, IssuerDistribution]:
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
    for snapshot, evidence in await session.execute(query):
        document = IssuerDistribution.model_validate(snapshot.payload)
        digest = hashlib.sha256(document.canonical_bytes()).hexdigest()
        if (
            digest == snapshot.revision == evidence.revision == evidence.content_hash
            and document.instrument_id == instrument_id
            and document.available_at
            == snapshot.available_at
            == snapshot.received_at
            == evidence.available_at
            and len(document.canonical_bytes()) == evidence.byte_length
        ):
            result[evidence.id] = document
    return result
