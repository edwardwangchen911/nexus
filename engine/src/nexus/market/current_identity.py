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
    "NASDAQ Global Select Market": "XNAS",
    "Nasdaq Stock Market LLC": "XNAS",
    "NYSE American": "XASE",
    "AMEX": "XASE",
}
_FACTS = {
    "dei:Security12bTitle",
    "dei:TradingSymbol",
    "dei:SecurityExchangeName",
    "dei:EntityCentralIndexKey",
}
_REGISTRANT_FACTS = {"dei:EntityRegistrantName", "dei:EntityTaxIdentificationNumber"}
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

    def __init__(
        self, *, registrants: bool = False, dual_listing: bool = False, hidden_listing: bool = False
    ) -> None:
        super().__init__(convert_charrefs=True)
        self.fact_names = _FACTS | _REGISTRANT_FACTS if registrants else _FACTS
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
        self.class_contexts: set[str] = set()
        self.legal_contexts: set[str] = set()
        self._context_nodes: list[tuple[str, str, dict[str, str | None]]] = []
        self._context_extra_text = False
        self.dual_listing = dual_listing
        self.parents: list[dict[str, Any]] = []
        self.fact_scope: dict[str, dict[str, Any]] = {}
        self.scope_exceptions: set[str] = set()
        self.exchange_contexts: set[str] = set()
        self.hidden_listing = hidden_listing
        self.display_nodes: list[dict[str, Any]] = []
        self.display_active: list[dict[str, Any]] = []
        self.unsupported_scope = False

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
        if self.hidden_listing:
            if self.active is not None and not _directly_visible([(tag, a, row)]):
                self.fact_scope[self.active["id"]]["hidden_descendant"] = True
            for node in self.display_active:
                node["children"].append((tag, a))
            if any(k in a for k in ("target", "tupleref", "order", "xml:base")):
                self.unsupported_scope = True
            if "-sec-ix-hidden" in (a.get("style") or "").lower():
                self.display_active.append(
                    dict(
                        tag=tag,
                        attrs=a,
                        row=row,
                        depth=len(self.stack),
                        ancestors=list(self.stack),
                        parts=[],
                        children=[],
                    )
                )
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
            if (
                not self.dual_listing
                or tag != "ix:nonnumeric"
                or self.parents
                or a.get("name") != self.active["name"]
                or a.get("name") not in {"dei:Security12bTitle", "dei:TradingSymbol"}
            ):
                raise invalid()
            self.parents.append(self.active)
            self.active = None
        if tag == "ix:nonnumeric" and a.get("name") in self.fact_names:
            if self.active is not None or not identifier or not a.get("contextref"):
                raise invalid()
            if a.get("continuedat") or a.get("xsi:nil") or a.get("escape"):
                raise invalid()
            security = a["name"] in _FACTS - {"dei:EntityCentralIndexKey"}
            visible = not any(
                node[0] == "ix:hidden"
                or re.search(r"display\s*:\s*none", node[1].get("style") or "", re.I)
                for node in self.stack
            )
            in_table = any(node[0] == "table" for node in self.stack)
            if security and (not in_table or not visible):
                if not self.hidden_listing and (
                    not self.dual_listing or a["name"] != "dei:SecurityExchangeName"
                ):
                    raise invalid()
                self.scope_exceptions.add(identifier)
            self.fact_scope[identifier] = dict(
                visible=visible,
                parent_fact_id=self.parents[-1]["id"] if self.parents else None,
                ix_ancestors={t for t, _, _ in self.stack if t.startswith("ix:")},
                attrs=a,
                ancestors=list(self.stack),
                hidden_descendant=False,
            )
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
        for node in self.display_active:
            node["parts"].append(data)
        if self.context is not None and self.field is None and data.strip():
            self._context_extra_text = True
        if self.active is not None:
            self.active["parts"].append(data)
        for parent in self.parents:
            parent["parts"].append(data)
        if self.field is not None:
            self.field["parts"].append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in _VOID:
            return
        if not self.stack or self.stack[-1][0] != tag:
            raise invalid()
        if self.display_active and self.display_active[-1]["depth"] == len(self.stack):
            node = self.display_active.pop()
            node["value"] = " ".join("".join(node.pop("parts")).split())
            self.display_nodes.append(node)
        if tag == "ix:nonnumeric" and self.active is not None:
            self.active["value"] = " ".join("".join(self.active.pop("parts")).split())
            self.facts.append(self.active)
            self.active = self.parents.pop() if self.parents else None
        if self.field is not None and self.field["tag"] == tag:
            self.field["value"] = "".join(self.field.pop("parts")).strip()
            assert self.context is not None
            self.context["fields"].append(self.field)
            self.field = None
        if tag == "xbrli:context":
            if self.context is None:
                raise invalid()
            nodes = self._context_nodes
            if (
                not self._context_extra_text
                and len(nodes) == 9
                and nodes[:4]
                == [
                    ("xbrli:context", "", {"id": self.context["id"]}),
                    ("xbrli:entity", "xbrli:context", {}),
                    ("xbrli:identifier", "xbrli:entity", {"scheme": "http://www.sec.gov/CIK"}),
                    ("xbrli:segment", "xbrli:entity", {}),
                ]
                and all(n[0:2] == ("xbrldi:explicitmember", "xbrli:segment") for n in nodes[4:6])
                and {tuple(n[2].items()) for n in nodes[4:6]}
                == {
                    (("dimension", "us-gaap:StatementClassOfStockAxis"),),
                    (("dimension", "dei:EntityListingsExchangeAxis"),),
                }
                and nodes[6:]
                == [
                    ("xbrli:period", "xbrli:context", {}),
                    ("xbrli:startdate", "xbrli:period", {}),
                    ("xbrli:enddate", "xbrli:period", {}),
                ]
            ):
                self.exchange_contexts.add(self.context["id"])
            if not self._context_extra_text and self._context_nodes == [
                ("xbrli:context", "", {"id": self.context["id"]}),
                ("xbrli:entity", "xbrli:context", {}),
                ("xbrli:identifier", "xbrli:entity", {"scheme": "http://www.sec.gov/CIK"}),
                ("xbrli:period", "xbrli:context", {}),
                ("xbrli:startdate", "xbrli:period", {}),
                ("xbrli:enddate", "xbrli:period", {}),
            ]:
                self.plain_contexts.add(self.context["id"])
            if not self._context_extra_text and self._context_nodes == [
                ("xbrli:context", "", {"id": self.context["id"]}),
                ("xbrli:entity", "xbrli:context", {}),
                ("xbrli:identifier", "xbrli:entity", {"scheme": "http://www.sec.gov/CIK"}),
                ("xbrli:segment", "xbrli:entity", {}),
                (
                    "xbrldi:explicitmember",
                    "xbrli:segment",
                    {"dimension": "us-gaap:StatementClassOfStockAxis"},
                ),
                ("xbrli:period", "xbrli:context", {}),
                ("xbrli:startdate", "xbrli:period", {}),
                ("xbrli:enddate", "xbrli:period", {}),
            ]:
                self.class_contexts.add(self.context["id"])
            if not self._context_extra_text and self._context_nodes == [
                ("xbrli:context", "", {"id": self.context["id"]}),
                ("xbrli:entity", "xbrli:context", {}),
                ("xbrli:identifier", "xbrli:entity", {"scheme": "http://www.sec.gov/CIK"}),
                ("xbrli:segment", "xbrli:entity", {}),
                ("xbrldi:explicitmember", "xbrli:segment", {"dimension": "dei:LegalEntityAxis"}),
                ("xbrli:period", "xbrli:context", {}),
                ("xbrli:startdate", "xbrli:period", {}),
                ("xbrli:enddate", "xbrli:period", {}),
            ]:
                self.legal_contexts.add(self.context["id"])
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


def _period(context: dict[str, Any]) -> dict[str, str]:
    return {
        f["tag"]: f["value"]
        for f in context["fields"]
        if f["tag"] in {"xbrli:startdate", "xbrli:enddate"}
    }


def _issuer_scope(
    parser: _Cover,
    ciks: list[dict[str, Any]],
    stock_context: dict[str, Any],
    identifier: str,
    raw: bytes,
) -> None:
    if len(ciks) == 1:
        if cik(ciks[0]["value"]) != identifier:
            raise invalid()
        _context(parser.contexts[ciks[0]["context"]], identifier)
        return
    # SEC EDGAR XBRL Guide 6.2.7: a shared identifier plus explicit LegalEntityAxis
    # distinguishes co-registrants; the default registrant omits that dimension.
    # https://www.sec.gov/files/edgar/filer-information/specifications/xbrl-guide-2025-07-10.pdf
    values = [cik(f["value"]) for f in ciks]
    targets = [f for f in ciks if cik(f["value"]) == identifier]
    if (
        len(targets) != 1
        or len(set(values)) != len(values)
        or len({f["context"] for f in ciks}) != len(ciks)
        or targets[0]["context"] not in parser.plain_contexts
        or stock_context["id"] not in parser.plain_contexts | parser.class_contexts
    ):
        raise invalid()
    # Extra facts are parsed only on this new branch, preserving single-CIK behavior.
    registrants = _Cover(registrants=True)
    registrants.feed(raw.decode("utf-8-sig"))
    registrants.close()
    members: set[tuple[str, str]] = set()
    for fact in ciks:
        context = parser.contexts[fact["context"]]
        dimensions = _context(context, identifier)
        if _period(context) != _period(stock_context):
            raise invalid()
        if fact is not targets[0]:
            if context["id"] not in parser.legal_contexts or set(dimensions) != {
                "dei:LegalEntityAxis"
            }:
                raise invalid()
            member = re.fullmatch(
                r"([A-Za-z_][A-Za-z0-9_.-]*):([A-Za-z_][A-Za-z0-9_.-]*Member)",
                dimensions["dei:LegalEntityAxis"],
            )
            if not member or member[1] in {
                "dei",
                "us-gaap",
                "srt",
                "ix",
                "xbrli",
                "xbrldi",
                "exch",
            }:
                raise invalid()
            namespace = parser.namespaces.get("xmlns:" + member[1]) or ""
            if re.fullmatch(r"https?://[^\s<>]+", namespace) is None or re.match(
                r"https?://(?:www\.)?(?:fasb\.org|xbrl\.sec\.gov|xbrl\.org)(?:/|$)", namespace
            ):
                raise invalid()
            expanded = (namespace, member[2])
            if expanded in members:
                raise invalid()
            members.add(expanded)
        elif dimensions:
            raise invalid()
        for name in _REGISTRANT_FACTS:
            matches = [
                f for f in registrants.facts if f["name"] == name and f["context"] == context["id"]
            ]
            if len(matches) != 1 or not matches[0]["value"]:
                raise invalid()


def _dual_listing_record(
    parser: _Cover, symbols: list[dict[str, Any]], identifier: str, exchange: str
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """One demonstrated NYSE/Chicago dual tag, not a general Inline XBRL processor.

    Inline XBRL 1.1 Part 1 section 11 permits nested nonNumeric facts. SEC XBRL
    Guide 3.2.4.2-3 requires exchange-axis scope and same-context title/exchange.
    """
    if len(symbols) != 2 or exchange != "XNYS":
        raise invalid()
    rows = {f["row"] for f in symbols}
    contexts = {f["context"] for f in symbols}
    if None in rows or len(rows) != 1 or len(contexts) != 2:
        raise invalid()
    by_exchange = {}
    for context_id in contexts:
        if context_id not in parser.exchange_contexts:
            raise invalid()
        context = parser.contexts[context_id]
        dimensions = _context(context, identifier)
        if dimensions.get("us-gaap:StatementClassOfStockAxis") != "us-gaap:CommonStockMember":
            raise invalid()
        mic = dimensions.get("dei:EntityListingsExchangeAxis")
        if mic not in {"exch:XNYS", "exch:XCHI"} or mic in by_exchange:
            raise invalid()
        by_exchange[mic] = context_id
    chosen, other = by_exchange["exch:XNYS"], by_exchange["exch:XCHI"]
    if _period(parser.contexts[chosen]) != _period(parser.contexts[other]):
        raise invalid()
    row = [f for f in parser.facts if f["row"] in rows]
    if len(row) != 5 or any(not parser.fact_scope[f["id"]]["visible"] for f in row):
        raise invalid()
    pairs = []
    for name in ("dei:Security12bTitle", "dei:TradingSymbol"):
        pair = [f for f in row if f["name"] == name]
        if len(pair) != 2 or {f["context"] for f in pair} != contexts:
            raise invalid()
        if len({f["value"] for f in pair}) != 1:
            raise invalid()
        edges = [(f["id"], parser.fact_scope[f["id"]]["parent_fact_id"]) for f in pair]
        if not any(parent == peer["id"] for _, parent in edges for peer in pair):
            raise invalid()
        pairs.extend(pair)
    nested_ids = {key for key, value in parser.fact_scope.items() if value["parent_fact_id"]}
    if nested_ids != {f["id"] for f in pairs if parser.fact_scope[f["id"]]["parent_fact_id"]}:
        raise invalid()
    record = [f for f in row if f["context"] == chosen]
    if len(record) != 3 or {f["name"] for f in record} != _FACTS - {"dei:EntityCentralIndexKey"}:
        raise invalid()
    auxiliary = [
        f for f in parser.facts if f["context"] == other and f["name"] == "dei:SecurityExchangeName"
    ]
    if len(auxiliary) != 1:
        raise invalid()
    aux = auxiliary[0]
    if aux["value"] != "CHX" or parser.fact_scope[aux["id"]]["visible"] or aux["row"] is not None:
        raise invalid()
    if parser.scope_exceptions != {aux["id"]}:
        raise invalid()
    relevant = [*row, aux]
    # No hidden/second-row fact may silently become another fact for either context.
    if {
        f["id"]
        for f in parser.facts
        if f["context"] in contexts and f["name"] != "dei:EntityCentralIndexKey"
    } != {f["id"] for f in relevant}:
        raise invalid()
    evidence = []
    for fact in relevant:
        scope = parser.fact_scope[fact["id"]]
        if scope["ix_ancestors"] - {"ix:nonnumeric", "ix:header", "ix:hidden"}:
            raise invalid()
        if any(k in scope["attrs"] for k in ("target", "tupleref", "order", "xml:base")):
            raise invalid()
        if "format" in scope["attrs"] and not (
            fact["name"] == "dei:SecurityExchangeName"
            and fact["context"] == chosen
            and scope["attrs"]["format"] == "ixt-sec:exchnameen"
            and parser.namespaces.get("xmlns:ixt-sec")
            == "http://www.sec.gov/inlineXBRL/transformation/2015-08-31"
            and _EXCHANGES.get(fact["value"]) == exchange
        ):
            raise invalid()
        evidence.append(
            {
                **fact,
                "listing_context": parser.contexts[fact["context"]],
                "parent_fact_id": scope["parent_fact_id"],
                "visible": scope["visible"],
                "format": scope["attrs"].get("format"),
                "selected_listing": fact["context"] == chosen,
            }
        )
    return next(f for f in record if f["name"] == "dei:TradingSymbol"), record, evidence


def _separate_hidden_listing(
    parser: _Cover, symbols: list[dict[str, Any]], identifier: str, exchange: str
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """One visible common-stock row plus one CSS-linked, non-target preferred row.

    The hidden ticker and its displayed label are separate evidence, not aliases.
    This exception grants no identity to the ancillary security.
    """
    if len(symbols) != 1 or len(parser.facts) != 7 or parser.unsupported_scope:
        raise invalid()
    stock = symbols[0]
    record = [f for f in parser.facts if f["row"] == stock["row"]]
    ciks = [f for f in parser.facts if f["name"] == "dei:EntityCentralIndexKey"]
    hidden = [f for f in parser.facts if f["id"] in parser.scope_exceptions]
    if len(ciks) != 1 or len(hidden) != 1:
        raise invalid()
    auxiliary = hidden[0]
    if (
        auxiliary["name"] != "dei:TradingSymbol"
        or auxiliary["value"] == stock["value"]
        or not auxiliary["value"]
        or auxiliary["row"] is not None
        or auxiliary["context"] == stock["context"]
        or stock["row"] is None
        or len(record) != 3
    ):
        raise invalid()
    for fact in parser.facts:
        scope = parser.fact_scope[fact["id"]]
        ancestors = scope["ancestors"]
        allowed_ix = {"ix:nonnumeric"}
        if fact is auxiliary or fact in ciks:
            allowed_ix |= {"ix:header", "ix:hidden"}
        if (
            scope["ix_ancestors"] - allowed_ix
            or scope["parent_fact_id"]
            or scope["hidden_descendant"]
        ):
            raise invalid()
        attrs = scope["attrs"]
        if set(attrs) - {"id", "name", "contextref", "format"}:
            raise invalid()
        if "format" in attrs and not (
            fact["name"] == "dei:SecurityExchangeName"
            and attrs["format"] == "ixt-sec:exchnameen"
            and parser.namespaces.get("xmlns:ixt-sec")
            == "http://www.sec.gov/inlineXBRL/transformation/2015-08-31"
            and _EXCHANGES.get(fact["value"]) == exchange
        ):
            raise invalid()
        if fact is auxiliary:
            if "ix:hidden" not in scope["ix_ancestors"]:
                raise invalid()
        elif fact not in ciks and (
            not _directly_visible(ancestors) or sum(t == "tr" for t, _, _ in ancestors) != 1
        ):
            raise invalid()
    stock_context = parser.contexts[stock["context"]]
    other_context = parser.contexts[auxiliary["context"]]
    for context in (stock_context, other_context):
        if context["id"] not in parser.class_contexts:
            raise invalid()
    if _context(stock_context, identifier) != {
        "us-gaap:StatementClassOfStockAxis": "us-gaap:CommonStockMember"
    }:
        raise invalid()
    dims = _context(other_context, identifier)
    member = re.fullmatch(
        r"([A-Za-z_][A-Za-z0-9_.-]*):([A-Za-z_][A-Za-z0-9_.-]*Member)",
        dims.get("us-gaap:StatementClassOfStockAxis", ""),
    )
    if member is None or member[1] in {"dei", "us-gaap", "srt", "ix", "xbrli", "xbrldi", "exch"}:
        raise invalid()
    namespace = parser.namespaces.get("xmlns:" + member[1]) or ""
    if re.fullmatch(r"https?://[^\s<>]+", namespace) is None or re.match(
        r"https?://(?:www\.)?(?:fasb\.org|xbrl\.sec\.gov|xbrl\.org)(?:/|$)", namespace
    ):
        raise invalid()
    if any(
        _period(context) != _period(stock_context)
        for context in (other_context, parser.contexts[ciks[0]["context"]])
    ):
        raise invalid()
    other = [f for f in parser.facts if f["context"] == auxiliary["context"] and f is not auxiliary]
    if (
        len(other) != 2
        or {f["name"] for f in other} != {"dei:Security12bTitle", "dei:SecurityExchangeName"}
        or len({f["row"] for f in other}) != 1
        or other[0]["row"] in {None, stock["row"]}
    ):
        raise invalid()
    values = {f["name"]: f["value"] for f in other}
    if (
        not re.search(r"\bdepositary\b", values["dei:Security12bTitle"], re.I)
        or not re.search(r"\bpreferred\b", values["dei:Security12bTitle"], re.I)
        or _EXCHANGES.get(values["dei:SecurityExchangeName"]) != exchange
    ):
        raise invalid()
    links = []
    for node in parser.display_nodes:
        refs = re.findall(r"(?:^|;)\s*-sec-ix-hidden\s*:\s*([^;]+)", node["attrs"]["style"], re.I)
        if any(ref.strip() in {f["id"] for f in parser.facts} for ref in refs):
            if len(refs) != 1 or refs[0].strip() != auxiliary["id"]:
                raise invalid()
            links.append(node)
    if len(links) != 1:
        raise invalid()
    link = links[0]
    if (
        link["tag"] != "span"
        or set(link["attrs"]) != {"style"}
        or not link["value"]
        or link["row"] != other[0]["row"]
        or not _plain_display_children(link["children"])
        or not _directly_visible(link["ancestors"])
        or any(t.startswith("ix:") for t, _, _ in link["ancestors"])
        or sum(t == "tr" for t, _, _ in link["ancestors"]) != 1
    ):
        raise invalid()
    evidence = []
    for fact in (*record, auxiliary, *other):
        scope = parser.fact_scope[fact["id"]]
        proof = dict(
            fact,
            listing_context=parser.contexts[fact["context"]],
            selected_listing=fact in record,
            visible=scope["visible"],
            parent_fact_id=None,
            format=scope["attrs"].get("format"),
        )
        if fact is auxiliary:
            proof["display_reference"] = dict(
                fact_id=fact["id"],
                row=link["row"],
                value=link["value"],
                matches_fact_value=link["value"] == fact["value"],
            )
        evidence.append(proof)
    return stock, record, evidence


def _directly_visible(ancestors: list[tuple[str, dict[str, str | None], int | None]]) -> bool:
    return not any(
        t == "ix:hidden"
        or "hidden" in a
        or re.search(
            r"(?:display\s*:\s*none|visibility\s*:\s*(?:hidden|collapse))",
            a.get("style") or "",
            re.I,
        )
        for t, a, _ in ancestors
    )


def _plain_display_children(children: list[tuple[str, dict[str, str | None]]]) -> bool:
    # A single demonstrated typography span; never inherit its parent's fact ID.
    if not children:
        return True
    if len(children) != 1:
        return False
    tag, attrs = children[0]
    if tag != "span" or set(attrs) != {"style"}:
        return False
    declarations = [part.strip() for part in (attrs["style"] or "").split(";") if part.strip()]
    return len(declarations) == 4 and set(declarations) == {
        "color:#000000",
        "white-space:pre-wrap",
        "font-kerning:none",
        "min-width:fit-content",
    }


def parse_current_common_stock(
    *,
    symbol: str,
    fmp_raw: bytes,
    sec_raw: bytes,
    _member_definition: dict[str, Any] | None = None,
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
        parser = _Cover(dual_listing=True, hidden_listing=True)
        parser.feed(sec_raw.decode("utf-8-sig"))
        parser.close()
        if parser.stack or parser.active or parser.parents or parser.context:
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
        if not ciks:
            raise invalid()
        expanded = None
        if parser.scope_exceptions or any(s["parent_fact_id"] for s in parser.fact_scope.values()):
            if len(ciks) != 1:
                raise invalid()
            resolver = (
                _separate_hidden_listing
                if any(
                    f["name"] == "dei:TradingSymbol" and f["id"] in parser.scope_exceptions
                    for f in parser.facts
                )
                else _dual_listing_record
            )
            stock, record, expanded = resolver(parser, symbols, identifier, exchange)
            if _period(parser.contexts[ciks[0]["context"]]) != _period(
                parser.contexts[stock["context"]]
            ):
                raise invalid()
        else:
            if len(symbols) != 1:
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
        _issuer_scope(parser, ciks, context, identifier, sec_raw)
        class_dimension = {"us-gaap:StatementClassOfStockAxis": "us-gaap:CommonStockMember"}
        exchange_dimension = {"dei:EntityListingsExchangeAxis": f"exch:{exchange}"}
        plain_stock = not dimensions and stock["context"] in parser.plain_contexts
        # FASB Subsequent Events guide, Fig. 1.2: CommonClassAMember is Common Class A.
        # https://xbrl.fasb.org/impdocs/SE2_TIG/subsequentevents_2.pdf
        class_a = (
            dimensions == {"us-gaap:StatementClassOfStockAxis": "us-gaap:CommonClassAMember"}
            and stock["context"] in parser.class_contexts
        )
        custom = (
            _member_definition is not None
            and stock["context"] in parser.class_contexts
            and dimensions == {"us-gaap:StatementClassOfStockAxis": _member_definition["qname"]}
            and parser.namespaces.get("xmlns:" + _member_definition["qname"].split(":")[0])
            == _member_definition["namespace"]
            and expanded is None
        )
        if (
            not plain_stock
            and not class_a
            and not custom
            and dimensions
            not in (
                class_dimension,
                class_dimension | exchange_dimension,
            )
        ):
            raise invalid()
        if "dei:EntityListingsExchangeAxis" in dimensions and not exchange_namespace:
            raise invalid()
        values = {f["name"]: f["value"] for f in record}
        title = values["dei:Security12bTitle"]
        title_pattern = (
            r"^Class A Common Stock\b"
            if class_a
            else r"^(?:Common Stock|Common Shares|Ordinary Shares)\b"
        )
        # Only the fully validated dual-listing CommonStockMember scope can support
        # this neutral legal title; capital stock alone does not prove common equity.
        capital = (
            re.fullmatch(r"Capital Stock, par value \$([0-9]*\.[0-9]+) per share", title, re.I)
            if expanded is not None and len(symbols) == 2
            else None
        )
        capital_common = capital is not None and any(c in "123456789" for c in capital[1])
        if (
            (not re.match(title_pattern, title, re.I) and not capital_common)
            or (expanded is not None and re.search(r"\bclass\b", title, re.I))
            or (class_a and re.search(r"\bclass\b", title[len("Class A Common Stock") :], re.I))
            or re.search(
                r"preferred|depositary|warrant|\bunits?\b|\bnotes?\b|\bbonds?\b|\bfund\b",
                title,
                re.I,
            )
            or _EXCHANGES.get(values["dei:SecurityExchangeName"]) != exchange
        ):
            raise invalid()
        if _member_definition is not None:
            if (
                not custom
                or title != _member_definition["title"]
                or re.search(r"\bclass\b", title, re.I)
            ):
                raise invalid()
            record = [
                dict(f, member_definition=_member_definition) if f["id"] == stock["id"] else f
                for f in record
            ]
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
            facts=tuple([*(expanded if expanded is not None else record), *ciks]),
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
    *,
    symbol: str,
    contents: tuple[bytes, bytes, bytes, bytes],
    now: datetime,
    _member_definition: dict[str, Any] | None = None,
) -> IdentityInputs:
    try:
        fmp, fm_receipt, sec, sec_receipt = contents
        identity = parse_current_common_stock(
            symbol=symbol, fmp_raw=fmp, sec_raw=sec, _member_definition=_member_definition
        )
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
    cutoff = require_utc(now or datetime.now(UTC))
    return validate_identity_originals(
        symbol=symbol,
        contents=read_identity_archive_contents(
            symbol=symbol, fmp_archive=fmp_archive, sec_archive=sec_archive, now=cutoff
        ),
        now=cutoff,
    )


def read_identity_archive_contents(
    *, symbol: str, fmp_archive: Path, sec_archive: Path, now: datetime
) -> tuple[bytes, bytes, bytes, bytes]:
    try:
        cutoff = require_utc(now)
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
        return fmp.originals[0].content, fmp.originals[1].content, raw, receipt
    except (OSError, ValueError, TypeError, KeyError, RecursionError) as error:
        raise ValueError("CURRENT_IDENTITY_ARCHIVE_INVALID") from error
