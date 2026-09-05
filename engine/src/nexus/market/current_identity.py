"""One current common-stock identity from a fixed definition and two archived sources."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict

from nexus.backtest.universe import UniverseDefinitionMember, load_us_liquid_v1_definition
from nexus.domain.time import require_utc
from nexus.providers.fmp_profile import _invalid_constant, _unique, parse_profile
from nexus.research.fmp_profile import RECEIPT_SCHEMA
from nexus.research.fmp_profile_archive import _read, load_profile_archive

SOURCE = "current_instrument_identity"
SCHEMA = "current_instrument_identity_v1"
MAX_SEC_BYTES = 8 * 1024 * 1024
SEC_URL = re.compile(
    r"https://www\.sec\.gov/Archives/edgar/data/([1-9][0-9]{0,9})/"
    r"([0-9]{18})/([A-Za-z0-9][A-Za-z0-9._-]*\.html?)"
)
_EXCHANGES = {
    "NYSE": "XNYS",
    "New York Stock Exchange": "XNYS",
    "NASDAQ": "XNAS",
    "Nasdaq": "XNAS",
    "The Nasdaq Stock Market LLC": "XNAS",
    # Nasdaq Listing Center FAQ 1081: Global Select is a tier of Nasdaq Stock Market LLC.
    # https://listingcenter.nasdaq.com/Material_Search.aspx?cid=38&mcd=LQ&sub_cid=67
    "The NASDAQ Global Select Market": "XNAS",
    "Nasdaq Global Select Market": "XNAS",
    "The Nasdaq Global Select Market": "XNAS",
    "NYSE American": "XASE",
    "AMEX": "XASE",
}
_FACTS = {
    "dei:Security12bTitle",
    "dei:TradingSymbol",
    "dei:SecurityExchangeName",
    "dei:EntityCentralIndexKey",
}
_VOID = {
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
}


def invalid() -> ValueError:
    return ValueError("CURRENT_IDENTITY_INVALID")


def member_for(symbol: str) -> UniverseDefinitionMember:
    for member in load_us_liquid_v1_definition(date(2026, 1, 1)).members:
        if member.symbol == symbol and member.asset_type == "common_stock":
            return member
    raise ValueError("CURRENT_IDENTITY_SCOPE_INVALID")


def cik(value: object) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9]{1,10}", value) is None or int(value) == 0:
        raise invalid()
    return value.zfill(10)


class CurrentCommonStockIdentity(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    symbol: str
    cik: str
    currency: str
    exchange: str
    fmp_exchange: str
    isin: str | None
    cusip: str | None
    sec_title: str
    sec_exchange: str
    context_id: str
    facts: tuple[dict[str, Any], ...]
    context: dict[str, Any]


class _Cover(HTMLParser):
    """Only the demonstrated inline-XBRL registration row; never resolve remote resources."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.stack: list[tuple[str, dict[str, str | None], int | None]] = []
        self.ids: set[str] = set()
        self.facts: list[dict[str, Any]] = []
        self.active: dict[str, Any] | None = None
        self.contexts: dict[str, dict[str, Any]] = {}
        self.context: dict[str, Any] | None = None
        self.field: dict[str, Any] | None = None
        self.rows = 0
        self.namespaces: dict[str, str | None] = {}
        self.root_seen = False
        # Parser-only proof: do not change the serialized context of old identity archives.
        self.plain_contexts: set[str] = set()
        self._context_nodes: list[tuple[str, str, dict[str, str | None]]] = []
        self._context_extra_text = False

    def handle_decl(self, decl: str) -> None:
        if decl.lower() != "doctype html":
            raise invalid()

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)
        if tag not in _VOID:
            self.handle_endtag(tag)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        a = dict(attrs)
        if not self.stack and (tag != "html" or self.root_seen):
            raise invalid()
        if tag == "html":
            self.root_seen = True
        if len(a) != len(attrs):
            raise invalid()
        for key, value in a.items():
            if key.startswith("xmlns:"):
                if tag == "html" and not self.stack:
                    self.namespaces[key] = value
                elif key not in self.namespaces or self.namespaces[key] != value:
                    raise invalid()
        identifier = a.get("id")
        if identifier:
            if identifier in self.ids:
                raise invalid()
            self.ids.add(identifier)
        row = self.stack[-1][2] if self.stack else None
        if tag == "tr":
            self.rows += 1
            row = self.rows
        if tag not in _VOID:
            self.stack.append((tag, a, row))
        if tag == "xbrli:context":
            if self.context is not None or not identifier:
                raise invalid()
            self.context = {"id": identifier, "fields": []}
            self._context_nodes = []
            self._context_extra_text = False
        if self.context is not None:
            parent = "" if tag == "xbrli:context" else self.stack[-2][0]
            self._context_nodes.append((tag, parent, a))
        if self.context is not None and tag in {
            "xbrli:identifier",
            "xbrli:startdate",
            "xbrli:enddate",
            "xbrli:instant",
            "xbrldi:explicitmember",
        }:
            if self.field is not None:
                raise invalid()
            if tag == "xbrli:identifier" and (
                len(self.stack) < 2 or self.stack[-2][0] != "xbrli:entity"
            ):
                raise invalid()
            self.field = {"tag": tag, "attrs": a, "parts": []}
        if tag.startswith("ix:") and self.active is not None:
            raise invalid()
        if tag == "ix:nonnumeric" and a.get("name") in _FACTS:
            if self.active is not None or not identifier or not a.get("contextref"):
                raise invalid()
            if a.get("continuedat") or a.get("xsi:nil") or a.get("escape"):
                raise invalid()
            if a["name"] != "dei:EntityCentralIndexKey" and not any(
                node[0] == "table" for node in self.stack
            ):
                raise invalid()
            if a["name"] != "dei:EntityCentralIndexKey" and any(
                node[0] == "ix:hidden"
                or re.search(r"display\s*:\s*none", node[1].get("style") or "", re.I)
                for node in self.stack
            ):
                raise invalid()
            self.active = dict(
                name=a["name"],
                id=identifier,
                context=a["contextref"],
                row=row,
                line=self.getpos()[0],
                column=self.getpos()[1],
                parts=[],
            )

    def handle_data(self, data: str) -> None:
        if self.context is not None and self.field is None and data.strip():
            self._context_extra_text = True
        if self.active is not None:
            self.active["parts"].append(data)
        if self.field is not None:
            self.field["parts"].append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in _VOID:
            return
        if not self.stack or self.stack[-1][0] != tag:
            raise invalid()
        if tag == "ix:nonnumeric" and self.active is not None:
            self.active["value"] = " ".join("".join(self.active.pop("parts")).split())
            self.facts.append(self.active)
            self.active = None
        if self.field is not None and self.field["tag"] == tag:
            self.field["value"] = "".join(self.field.pop("parts")).strip()
            assert self.context is not None
            self.context["fields"].append(self.field)
            self.field = None
        if tag == "xbrli:context":
            if self.context is None:
                raise invalid()
            if not self._context_extra_text and self._context_nodes == [
                ("xbrli:context", "", {"id": self.context["id"]}),
                ("xbrli:entity", "xbrli:context", {}),
                ("xbrli:identifier", "xbrli:entity", {"scheme": "http://www.sec.gov/CIK"}),
                ("xbrli:period", "xbrli:context", {}),
                ("xbrli:startdate", "xbrli:period", {}),
                ("xbrli:enddate", "xbrli:period", {}),
            ]:
                self.plain_contexts.add(self.context["id"])
            self.contexts[self.context["id"]] = self.context
            self.context = None
        self.stack.pop()


def _context(context: dict[str, Any], expected_cik: str) -> dict[str, str]:
    fields = context["fields"]
    ids = [field for field in fields if field["tag"] == "xbrli:identifier"]
    if (
        len(ids) != 1
        or ids[0]["attrs"].get("scheme") != "http://www.sec.gov/CIK"
        or cik(ids[0]["value"]) != expected_cik
    ):
        raise invalid()
    periods = [field for field in fields if field["tag"] in {"xbrli:startdate", "xbrli:enddate"}]
    if len(periods) != 2 or {f["tag"] for f in periods} != {"xbrli:startdate", "xbrli:enddate"}:
        raise invalid()
    dates = {field["tag"]: date.fromisoformat(field["value"]) for field in periods}
    if dates["xbrli:startdate"] > dates["xbrli:enddate"]:
        raise invalid()
    members = [field for field in fields if field["tag"] == "xbrldi:explicitmember"]
    dimensions = {field["attrs"].get("dimension"): field["value"] for field in members}
    if len(dimensions) != len(members):
        raise invalid()
    return dimensions


def parse_current_common_stock(
    *, symbol: str, fmp_raw: bytes, sec_raw: bytes
) -> CurrentCommonStockIdentity:
    try:
        member_for(symbol)
        row = parse_profile(fmp_raw, symbol)
        identifier = cik(row.get("cik"))
        if row.get("currency") != "USD" or any(
            type(row.get(k)) is not bool or row[k] for k in ("isEtf", "isFund", "isAdr")
        ):
            raise invalid()
        exchange = _EXCHANGES.get(row.get("exchange", ""))
        if exchange not in {"XNYS", "XNAS"} or not 0 < len(sec_raw) <= MAX_SEC_BYTES:
            raise invalid()
        parser = _Cover()
        parser.feed(sec_raw.decode("utf-8-sig"))
        parser.close()
        if parser.stack or parser.active or parser.context:
            raise invalid()
        fixed_ns = {
            "xmlns:ix": "http://www.xbrl.org/2013/inlineXBRL",
            "xmlns:xbrli": "http://www.xbrl.org/2003/instance",
            "xmlns:xbrldi": "http://xbrl.org/2006/xbrldi",
        }
        if any(parser.namespaces.get(k) != v for k, v in fixed_ns.items()) or any(
            re.fullmatch(pattern, parser.namespaces.get(key) or "") is None
            for key, pattern in {
                "xmlns:dei": r"http://xbrl\.sec\.gov/dei/[0-9]{4}",
                "xmlns:us-gaap": r"http://fasb\.org/us-gaap/[0-9]{4}",
            }.items()
        ):
            raise invalid()
        exchange_namespace = parser.namespaces.get("xmlns:exch")
        if (
            "xmlns:exch" in parser.namespaces
            and re.fullmatch(r"http://xbrl\.sec\.gov/exch/[0-9]{4}", exchange_namespace or "")
            is None
        ):
            raise invalid()
        symbols = [
            f for f in parser.facts if f["name"] == "dei:TradingSymbol" and f["value"] == symbol
        ]
        ciks = [f for f in parser.facts if f["name"] == "dei:EntityCentralIndexKey"]
        if len(symbols) != 1 or len(ciks) != 1 or cik(ciks[0]["value"]) != identifier:
            raise invalid()
        stock = symbols[0]
        record = [f for f in parser.facts if f["row"] == stock["row"]]
        if (
            stock["row"] is None
            or len(record) != 3
            or {f["name"] for f in record} != _FACTS - {"dei:EntityCentralIndexKey"}
            or {f["context"] for f in record} != {stock["context"]}
        ):
            raise invalid()
        context = parser.contexts[stock["context"]]
        dimensions = _context(context, identifier)
        _context(parser.contexts[ciks[0]["context"]], identifier)
        class_dimension = {"us-gaap:StatementClassOfStockAxis": "us-gaap:CommonStockMember"}
        exchange_dimension = {"dei:EntityListingsExchangeAxis": f"exch:{exchange}"}
        plain_stock = not dimensions and stock["context"] in parser.plain_contexts
        if not plain_stock and dimensions not in (
            class_dimension,
            class_dimension | exchange_dimension,
        ):
            raise invalid()
        if "dei:EntityListingsExchangeAxis" in dimensions and not exchange_namespace:
            raise invalid()
        values = {f["name"]: f["value"] for f in record}
        title = values["dei:Security12bTitle"]
        if (
            not re.match(r"^(?:Common Stock|Common Shares|Ordinary Shares)\b", title, re.I)
            or re.search(
                r"preferred|depositary|warrant|\bunits?\b|\bnotes?\b|\bbonds?\b|\bfund\b",
                title,
                re.I,
            )
            or _EXCHANGES.get(values["dei:SecurityExchangeName"]) != exchange
        ):
            raise invalid()
        return CurrentCommonStockIdentity(
            symbol=symbol,
            cik=identifier,
            currency="USD",
            exchange=exchange,
            fmp_exchange=row["exchange"],
            isin=row.get("isin"),
            cusip=row.get("cusip"),
            sec_title=title,
            sec_exchange=values["dei:SecurityExchangeName"],
            context_id=stock["context"],
            facts=tuple([*record, ciks[0]]),
            context=context,
        )
    except (ValueError, TypeError, KeyError, RecursionError, UnicodeError) as error:
        raise invalid() from error


@dataclass(frozen=True)
class IdentityInputs:
    identity: CurrentCommonStockIdentity
    contents: tuple[bytes, bytes, bytes, bytes]
    received: tuple[datetime, datetime]
    sec_url: str

    @property
    def revision(self) -> str:
        return sha256(
            (
                self.identity.symbol
                + ":us_liquid_v1:"
                + ":".join(sha256(b).hexdigest() for b in self.contents)
            ).encode()
        ).hexdigest()


def validate_identity_originals(
    *, symbol: str, contents: tuple[bytes, bytes, bytes, bytes], now: datetime
) -> IdentityInputs:
    try:
        fmp, fm_receipt, sec, sec_receipt = contents
        identity = parse_current_common_stock(symbol=symbol, fmp_raw=fmp, sec_raw=sec)
        receipts = [
            json.loads(
                b,
                object_pairs_hook=_unique,
                parse_constant=_invalid_constant,
                parse_float=_invalid_constant,
            )
            for b in (fm_receipt, sec_receipt)
        ]
        f, s = receipts
        received = tuple(require_utc(datetime.fromisoformat(r["received_at"])) for r in receipts)
        if any(t > require_utc(now) for t in received):
            raise invalid()
        expected_fmp = dict(
            schema=RECEIPT_SCHEMA,
            source="fmp",
            origin="https://financialmodelingprep.com",
            path="/stable/profile",
            symbol=symbol,
            raw_sha256=sha256(fmp).hexdigest(),
            raw_byte_length=len(fmp),
            received_at=f["received_at"],
        )
        match = SEC_URL.fullmatch(s["source_url"])
        if (
            f != expected_fmp
            or type(f["raw_byte_length"]) is not int
            or not match
            or cik(match[1]) != identity.cik
        ):
            raise invalid()
        if (
            s
            != dict(
                source_url=s["source_url"],
                received_at=s["received_at"],
                sha256=sha256(sec).hexdigest(),
                byte_length=len(sec),
                http_status=200,
            )
            or type(s["byte_length"]) is not int
            or type(s["http_status"]) is not int
        ):
            raise invalid()
        return IdentityInputs(identity, contents, (received[0], received[1]), s["source_url"])
    except (ValueError, TypeError, KeyError, RecursionError) as error:
        raise ValueError("CURRENT_IDENTITY_ARCHIVE_INVALID") from error


def load_identity_archives(
    *, symbol: str, fmp_archive: Path, sec_archive: Path, now: datetime | None = None
) -> IdentityInputs:
    try:
        cutoff = require_utc(now or datetime.now(UTC))
        fmp = load_profile_archive(
            fmp_archive, instrument_id=member_for(symbol).instrument_id, symbol=symbol, now=cutoff
        )
        fd = os.open(sec_archive, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            if os.fstat(fd).st_mode & 0o077:
                raise invalid()
            receipt = _read(fd, "receipt.json", 8192)
            doc = json.loads(receipt, object_pairs_hook=_unique)
            match = SEC_URL.fullmatch(doc["source_url"])
            if not match or set(os.listdir(fd)) != {"receipt.json", match[3]}:
                raise invalid()
            raw = _read(fd, match[3], MAX_SEC_BYTES)
        finally:
            os.close(fd)
        return validate_identity_originals(
            symbol=symbol,
            contents=(fmp.originals[0].content, fmp.originals[1].content, raw, receipt),
            now=cutoff,
        )
    except (OSError, ValueError, TypeError, KeyError, RecursionError) as error:
        raise ValueError("CURRENT_IDENTITY_ARCHIVE_INVALID") from error
