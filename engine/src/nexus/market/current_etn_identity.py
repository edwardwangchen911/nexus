"""Current-held exchange-traded note identity from one bound SEC 424B2."""

import json
import re
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from html.parser import HTMLParser
from typing import Any, Literal, cast
from urllib.parse import urljoin

from pydantic import BaseModel, ConfigDict

from nexus.domain.time import require_utc
from nexus.market.current_etf_identity import require_outside_fixed_etf_definition
from nexus.market.current_identity import MAX_SEC_BYTES, cik
from nexus.providers.fmp_profile import _invalid_constant, _unique, parse_profile
from nexus.research.sec_disclosures import _VisibleText

SOURCE = "current_held_etn_identity"
SCHEMA = "current_held_etn_identity_v2"
_EXCHANGES = {
    "Nasdaq": ("XNAS", "The Nasdaq Stock Market LLC"),
    "NYSE": ("XNYS", "New York Stock Exchange"),
    "NYSE American": ("XASE", "NYSE American"),
    "NYSE Arca": ("ARCX", "NYSE Arca"),
}
_ARCHIVE = re.compile(
    r"https://www\.sec\.gov/Archives/edgar/data/([1-9][0-9]{0,9})/"
    r"([0-9]{18})/([A-Za-z0-9][A-Za-z0-9._-]*\.html?)"
)


class CurrentHeldEtnIdentity(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    symbol: str
    cik: str
    issuer_name: str
    cusip: str
    isin: str
    fmp_exchange: str
    fmp_is_etf: Literal[True] = True
    fmp_is_fund: Literal[False] = False
    provider_classification_basis: Literal["provider_etf_label_overridden_by_sec_debt_contract"] = (
        "provider_etf_label_overridden_by_sec_debt_contract"
    )
    cover_binding_sha256: str
    asset_type: Literal["exchange_traded_note"] = "exchange_traded_note"
    risk_bucket: Literal["EXCHANGE_TRADED_NOTE"] = "EXCHANGE_TRADED_NOTE"
    currency: Literal["USD"] = "USD"
    exchange: Literal["XNAS", "XNYS", "XASE", "ARCX"]
    sec_exchange: str
    form: Literal["424B2"] = "424B2"
    accession: str
    debt_basis: Literal["bound_424b2_senior_unsecured_debt_security"] = (
        "bound_424b2_senior_unsecured_debt_security"
    )
    listing_basis: Literal["bound_424b2_ticker_exchange_and_maturity"] = (
        "bound_424b2_ticker_exchange_and_maturity"
    )
    scope: Literal["fosun_current_holding"] = "fosun_current_holding"
    universe_published: Literal[False] = False
    new_position_eligible: Literal[False] = False
    historical_backtest_eligible: Literal[False] = False
    execution_locked: Literal[True] = True


@dataclass(frozen=True)
class CurrentHeldEtnInputs:
    identity: CurrentHeldEtnIdentity
    contents: tuple[bytes, ...]
    sec_url: str
    received: tuple[datetime, ...]

    @property
    def revision(self) -> str:
        return sha256(
            (
                SCHEMA
                + self.identity.symbol
                + ":".join(sha256(raw).hexdigest() for raw in self.contents)
            ).encode()
        ).hexdigest()


class _Links(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "a" and (href := dict(attrs).get("href")):
            self.links.append(href)


class _Rows(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.rows: list[tuple[str, ...]] = []
        self.row: list[str] | None = None
        self.cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "tr":
            if self.row is not None:
                raise ValueError
            self.row = []
        elif tag in {"td", "th"} and self.row is not None:
            if self.cell is not None:
                raise ValueError
            self.cell = []

    def handle_data(self, data: str) -> None:
        if self.cell is not None:
            self.cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th"} and self.cell is not None and self.row is not None:
            self.row.append(" ".join("".join(self.cell).split()))
            self.cell = None
        elif tag == "tr" and self.row is not None:
            if self.cell is not None:
                raise ValueError
            self.rows.append(tuple(self.row))
            self.row = None


def _json(raw: bytes) -> Any:
    return json.loads(raw, object_pairs_hook=_unique, parse_constant=_invalid_constant)


def _visible(raw: bytes) -> str:
    parser = _VisibleText()
    parser.feed(raw.decode("utf-8-sig"))
    return " ".join(" ".join(parser.parts).replace("\xa0", " ").split())


def validate_current_held_etn_originals(
    *, symbol: str, contents: tuple[bytes, ...], now: datetime
) -> CurrentHeldEtnInputs:
    """Bind one held note to its issuer, exact SEC filing, debt form, and listing."""

    try:
        require_outside_fixed_etf_definition(symbol)
        cutoff = require_utc(now)
        if len(contents) != 6 or any(not raw or len(raw) > MAX_SEC_BYTES for raw in contents):
            raise ValueError
        fmp_raw, fmp_receipt_raw, submissions_raw, index_raw, document_raw, receipts_raw = contents
        fmp = parse_profile(fmp_raw, symbol)
        fmp_receipt = _json(fmp_receipt_raw)
        if fmp_receipt != dict(
            schema="fmp_profile_receipt_v1",
            source="fmp",
            origin="https://financialmodelingprep.com",
            path="/stable/profile",
            symbol=symbol,
            raw_sha256=sha256(fmp_raw).hexdigest(),
            raw_byte_length=len(fmp_raw),
            received_at=fmp_receipt["received_at"],
        ):
            raise ValueError
        fmp_received = require_utc(datetime.fromisoformat(fmp_receipt["received_at"]))
        cusip_value, isin_value, fmp_exchange = (
            fmp.get("cusip"),
            fmp.get("isin"),
            fmp.get("exchange"),
        )
        if (
            fmp_received > cutoff
            or fmp.get("currency") != "USD"
            or fmp.get("isEtf") is not True
            or fmp.get("isFund") is not False
            or not isinstance(cusip_value, str)
            or re.fullmatch(r"[A-Z0-9]{9}", cusip_value) is None
            or not isinstance(isin_value, str)
            or re.fullmatch(r"[A-Z]{2}[A-Z0-9]{10}", isin_value) is None
            or not isinstance(fmp_exchange, str)
        ):
            raise ValueError
        receipts = _json(receipts_raw)
        if not isinstance(receipts, list) or len(receipts) != 3:
            raise ValueError
        received: list[datetime] = []
        for receipt, raw in zip(receipts, contents[2:5], strict=True):
            if receipt != dict(
                source_url=receipt["source_url"],
                received_at=receipt["received_at"],
                sha256=sha256(raw).hexdigest(),
                byte_length=len(raw),
                http_status=200,
            ):
                raise ValueError
            stamp = require_utc(datetime.fromisoformat(receipt["received_at"]))
            if stamp > cutoff:
                raise ValueError
            received.append(stamp)
        submission_url = re.fullmatch(
            r"https://data\.sec\.gov/submissions/CIK([0-9]{10})\.json",
            receipts[0]["source_url"],
        )
        if submission_url is None:
            raise ValueError
        issuer_cik = cik(submission_url[1])
        submissions = _json(submissions_raw)
        issuer_name = submissions.get("name")
        if cik(str(submissions.get("cik"))) != issuer_cik or not isinstance(issuer_name, str):
            raise ValueError
        document_match = _ARCHIVE.fullmatch(receipts[2]["source_url"])
        if not document_match or cik(document_match[1]) != issuer_cik:
            raise ValueError
        compact, primary = document_match[2], document_match[3]
        accession = compact[:10] + "-" + compact[10:12] + "-" + compact[12:]
        base = receipts[2]["source_url"].rsplit("/", 1)[0] + "/"
        if receipts[1]["source_url"] != base + accession + "-index.html":
            raise ValueError
        recent = submissions["filings"]["recent"]
        selected = [
            index for index, value in enumerate(recent["accessionNumber"]) if value == accession
        ]
        if len(selected) != 1:
            raise ValueError
        selected_index = selected[0]
        accepted = require_utc(datetime.fromisoformat(recent["acceptanceDateTime"][selected_index]))
        if (
            recent["form"][selected_index] != "424B2"
            or recent["primaryDocument"][selected_index] != primary
            or accepted > min(received[1:])
        ):
            raise ValueError
        links = _Links()
        links.feed(index_raw.decode("utf-8-sig"))
        index_text = _visible(index_raw)
        if (
            sum(urljoin(base, link) == receipts[2]["source_url"] for link in links.links) != 1
            or len(re.findall(r"SEC Accession No\.\s*" + re.escape(accession), index_text)) != 1
            or not re.search(r"\bCIK\s*:?\s*" + issuer_cik + r"\b", index_text)
            or len(re.findall(r"\bForm\s+424B2\b", index_text)) != 1
        ):
            raise ValueError
        text = _visible(document_raw)
        issuer_tokens = re.findall(r"[A-Za-z0-9]+", issuer_name)
        minimum_width = 2 if len(issuer_tokens) == 2 else 3
        if len(issuer_tokens) < minimum_width:
            raise ValueError
        issuer_core = r"\W+".join(
            re.escape(token) for token in issuer_tokens[:minimum_width]
        )
        issuer_positions = [
            match.start() for match in re.finditer(issuer_core, text, re.IGNORECASE)
        ]
        cik_positions = [
            match.start()
            for match in re.finditer(r"\b0*" + str(int(issuer_cik)) + r"\b", text)
        ]
        rows = _Rows()
        rows.feed(document_raw.decode("utf-8-sig"))
        ticker_rows: list[int] = []
        for row_number, row in enumerate(rows.rows[1:], start=1):
            headers = rows.rows[row_number - 1]
            if len(headers) != len(row):
                continue
            for header, value in zip(headers, row, strict=True):
                if (
                    re.fullmatch(r"Ticker(?:\s+Trading)?\s+Symbol", header, re.IGNORECASE)
                    and value == symbol
                    and sum(cell == symbol for cell in row) == 1
                    and any(
                        re.fullmatch(
                            r"CUSIP(?:\s+(?:Number|No\.?))?",
                            candidate_header,
                            re.IGNORECASE,
                        )
                        and candidate_value == cusip_value
                        for candidate_header, candidate_value in zip(headers, row, strict=True)
                    )
                ):
                    ticker_rows.append(row_number)
        if len(ticker_rows) != 1:
            raise ValueError
        cover_rows = " ".join(
            " ".join(row) for row in rows.rows[max(0, ticker_rows[0] - 25) : ticker_rows[0] + 1]
        )
        cover_titles = list(
            re.finditer(r"(?:Exchange.Traded Notes?|ETNs?)", cover_rows, re.IGNORECASE)
        )
        if (
            len(cover_titles) != 1
            or not re.search(r"\bdue\b|\bmaturity\b", cover_rows, re.IGNORECASE)
            or not re.search(issuer_core, cover_rows, re.IGNORECASE)
            or not re.search(r"\b0*" + str(int(issuer_cik)) + r"\b", cover_rows)
        ):
            raise ValueError
        segments = re.split(r"(?<=[.!?])\s+|\s{2,}", text)
        cover_windows: list[str] = []
        for segment_number, segment in enumerate(segments):
            if len(re.findall(r"\b" + re.escape(symbol) + r"\b", segment)) != 1:
                continue
            window = " ".join(
                segments[max(0, segment_number - 6) : segment_number + 7]
            )
            exchanges = [
                (exchange, legal_exchange)
                for _name, (exchange, legal_exchange) in _EXCHANGES.items()
                if re.search(re.escape(legal_exchange), window, re.IGNORECASE)
            ]
            if len(exchanges) == 1 and re.search(issuer_core, window, re.IGNORECASE):
                cover_windows.append(exchanges[0][0])
        senior_segments = [
            segment
            for segment in segments
            if re.search(r"\bsenior unsecured\b", segment, re.IGNORECASE)
            and re.search(r"\bnotes?\b|\bETNs?\b", segment, re.IGNORECASE)
            and re.search(issuer_core, segment, re.IGNORECASE)
        ]
        debt_segments = [
            segment
            for segment in segments
            if re.search(r"\bdebt securities\b", segment, re.IGNORECASE)
            and re.search(r"\bnotes?\b|\bETNs?\b", segment, re.IGNORECASE)
        ]
        if (
            len(cover_windows) != 1
            or len(senior_segments) != 1
            or not debt_segments
            or not issuer_positions
            or min(
                (
                    abs(issuer - identifier)
                    for issuer in issuer_positions
                    for identifier in cik_positions
                ),
                default=9999,
            )
            > 200
            or re.search(r"\bsubject to completion\b", text[:5000], re.IGNORECASE)
        ):
            raise ValueError
        exchange = cover_windows[0]
        exchange = cast(Literal["XNAS", "XNYS", "XASE", "ARCX"], exchange)
        sec_exchange = next(name for name, value in _EXCHANGES.items() if value[0] == exchange)
        identity = CurrentHeldEtnIdentity(
            symbol=symbol,
            cik=issuer_cik,
            issuer_name=issuer_name,
            cusip=cusip_value,
            isin=isin_value,
            fmp_exchange=fmp_exchange,
            cover_binding_sha256=sha256(
                " ".join(cover_rows.casefold().split()).encode()
            ).hexdigest(),
            exchange=exchange,
            sec_exchange=sec_exchange,
            accession=accession,
        )
        return CurrentHeldEtnInputs(
            identity=identity,
            contents=contents,
            sec_url=receipts[2]["source_url"],
            received=(fmp_received, fmp_received, *received, max(received)),
        )
    except (ValueError, TypeError, KeyError, IndexError, UnicodeError) as error:
        raise ValueError("CURRENT_ETN_IDENTITY_INVALID") from error
