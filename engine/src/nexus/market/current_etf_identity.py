"""Current equity ETF proof from a bound 497K; no historical or other fund inference."""

import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from hashlib import sha256
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from nexus.backtest.universe import UniverseDefinitionMember, load_us_liquid_v1_definition
from nexus.domain.time import require_utc
from nexus.market.current_identity import MAX_SEC_BYTES, cik
from nexus.providers.fmp_profile import _invalid_constant, _unique, parse_profile
from nexus.providers.sec import SecCompanyFactsClient
from nexus.providers.sec_http import SecHttpTransport
from nexus.research.sec_disclosures import _VisibleText
from nexus.research.sec_funds import fetch_fund_disclosure, resolve_fund

SOURCE = "current_etf_identity"
SCHEMA = "current_etf_identity_v1"


def member_for(symbol: str) -> UniverseDefinitionMember:
    for member in load_us_liquid_v1_definition(date(2026, 1, 1)).members:
        if member.symbol == symbol and (
            member.asset_type == "equity_etf"
            or (
                symbol == "TLT"
                and member.asset_type == "non_equity_etf"
                and member.risk_bucket == "US_TREASURY"
            )
            or (
                symbol == "GLD"
                and member.asset_type == "non_equity_etf"
                and member.risk_bucket == "GOLD"
            )
        ):
            return member
    raise ValueError("CURRENT_ETF_IDENTITY_INVALID")


def require_outside_fixed_etf_definition(symbol: str) -> None:
    """Fail closed unless the fixed definition loads and the symbol is absent from it."""

    definition = load_us_liquid_v1_definition(date(2026, 1, 1))
    if symbol != symbol.strip().upper() or not symbol or any(
        member.symbol == symbol for member in definition.members
    ):
        raise ValueError("CURRENT_ETF_IDENTITY_INVALID")


class CurrentEtfIdentity(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    symbol: str
    cik: str
    series_id: str
    class_id: str
    legal_name: str
    asset_type: Literal["equity_etf"] = "equity_etf"
    currency: Literal["USD"] = "USD"
    exchange: Literal["ARCX", "XNAS"] = "ARCX"
    fmp_exchange: str
    listing_basis: Literal["bound_sec_497k_cover"] = "bound_sec_497k_cover"
    fmp_exchange_precision: Literal["provider_label_only"] = "provider_label_only"
    objective: str
    legal_type_statement: str
    mapping_stored_at: datetime
    mapping_original_received_at: None = None


class CurrentTreasuryEtfIdentity(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    symbol: str
    cik: str
    series_id: str
    class_id: str
    legal_name: str
    asset_type: Literal["non_equity_etf"] = "non_equity_etf"
    currency: Literal["USD"] = "USD"
    exchange: Literal["XNAS"] = "XNAS"
    fmp_exchange: str
    listing_basis: Literal["bound_sec_497k_cover"] = "bound_sec_497k_cover"
    fmp_exchange_precision: Literal["provider_label_only"] = "provider_label_only"
    objective: str
    legal_type_statement: str
    mapping_stored_at: datetime
    mapping_original_received_at: None = None

    underlying_asset_class: Literal["us_treasury_bonds"] = "us_treasury_bonds"


class CurrentEtfShareClassIdentity(CurrentEtfIdentity):
    """SEC legal share-class name stays authoritative; conflicting provider label stays visible."""

    fmp_name: str
    fmp_name_matches_sec: bool


class CurrentNasdaqEtfIdentity(CurrentEtfShareClassIdentity):
    exchange: Literal["XNAS"] = "XNAS"
    filing_index_ticker: str | None
    filing_index_class_name: str
    reclassification_date: date
    ticker_binding_basis: Literal["current_directory_and_bound_497k_cover"] = (
        "current_directory_and_bound_497k_cover"
    )


class CurrentHeldNasdaqEtfIdentity(CurrentEtfIdentity):
    exchange: Literal["XNAS"] = "XNAS"
    filing_index_class_name: str
    fmp_cik: str | None
    fmp_cik_matches_sec: bool | None
    strategy_statement: str
    equity_basis: Literal["bound_497k_equity_exposure_and_strategy"] = (
        "bound_497k_equity_exposure_and_strategy"
    )
    ticker_binding_basis: Literal["current_directory_and_bound_497k_index"] = (
        "current_directory_and_bound_497k_index"
    )


class CurrentGoldTrustIdentity(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    symbol: str
    cik: str
    legal_name: str
    shares_name: str
    fmp_name: str
    fmp_name_matches_sec: bool
    asset_type: Literal["non_equity_etf"] = "non_equity_etf"
    currency: Literal["USD"] = "USD"
    exchange: Literal["ARCX"] = "ARCX"
    legal_structure: Literal["new_york_investment_trust"] = "new_york_investment_trust"
    underlying_asset_class: Literal["gold_bullion"] = "gold_bullion"
    listing_basis: Literal["bound_sec_10k_registration"] = "bound_sec_10k_registration"
    fmp_exchange: str
    sec_exchange: str
    fmp_exchange_precision: Literal["provider_label_only"] = "provider_label_only"
    objective: str
    legal_type_statement: str
    share_statement: str
    registration_facts: tuple[dict[str, Any], ...]
    registration_context: dict[str, Any]


class CurrentUitIdentity(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)
    symbol: str
    cik: str
    legal_name: str
    former_name: str
    fmp_name: str
    fmp_name_matches_sec: bool
    asset_type: Literal["equity_etf"] = "equity_etf"
    currency: Literal["USD"] = "USD"
    exchange: Literal["ARCX"] = "ARCX"
    legal_structure: Literal["unit_investment_trust"] = "unit_investment_trust"
    listing_basis: Literal["bound_sec_497_cover"] = "bound_sec_497_cover"
    fmp_exchange: str
    sec_exchange: str
    fmp_exchange_precision: Literal["provider_label_only"] = "provider_label_only"
    objective: str
    legal_type_statement: str


class _IndexRows(_VisibleText):
    def __init__(self) -> None:
        super().__init__()
        self.rows: list[tuple[str, ...]] = []
        self.cells: list[list[str]] | None = None
        self.in_cell = False
        self.scope_depth: int | None = None
        self.scope_seen = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        # SEC headers omit </tr>; a new row closes only a completed non-target row.
        if tag == "tr" and self.scope_depth is not None and self.cells is not None:
            first = " ".join("".join(self.cells[0]).split()) if self.cells else ""
            if self.in_cell or first.startswith(("Series ", "Class/Contract ")):
                raise ValueError
            self.handle_endtag("tr")
        super().handle_starttag(tag, attrs)
        if tag == "div" and dict(attrs).get("id") == "seriesDiv":
            if self.scope_seen or self.hidden:
                raise ValueError
            self.scope_depth = len(self.stack)
            self.scope_seen = True
        if self.scope_depth is None:
            return
        if tag == "tr" and not self.hidden:
            if self.cells is not None:
                raise ValueError
            self.cells = []
        if tag in {"td", "th"} and self.cells is not None:
            if self.in_cell:
                raise ValueError
            self.cells.append([])
            self.in_cell = True

    def handle_data(self, data: str) -> None:
        if self.in_cell and self.cells is not None and not self.hidden:
            self.cells[-1].append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "div" and self.scope_depth == len(self.stack):
            if self.cells is not None or self.in_cell:
                raise ValueError
            self.scope_depth = None
        if tag in {"td", "th"}:
            self.in_cell = False
        if tag == "tr" and self.cells is not None:
            if self.in_cell:
                raise ValueError
            self.rows.append(tuple(" ".join("".join(cell).split()) for cell in self.cells))
            self.cells = None
        super().handle_endtag(tag)


def _bound_nasdaq_class(
    *, index: bytes, symbol: str, series: str, share_class: str
) -> tuple[str, str | None]:
    parser = _IndexRows()
    parser.feed(index.decode("utf-8-sig"))
    if not parser.scope_seen or parser.scope_depth is not None:
        raise ValueError
    series_rows = [r for r in parser.rows if r and r[0].startswith("Series ")]
    class_rows = [r for r in parser.rows if r and r[0].startswith("Class/Contract ")]
    if (
        len(series_rows) != 1
        or len(class_rows) != 1
        or len(series_rows[0]) != 3
        or len(class_rows[0]) != 4
    ):
        raise ValueError
    sr, cr = series_rows[0], class_rows[0]
    if (
        sr[0] != "Series " + series
        or cr[0] != "Class/Contract " + share_class
        or sr[2] != cr[2]
        or cr[3] not in {"", symbol}
    ):
        raise ValueError
    return cr[2], cr[3] or None


def _nasdaq_identity(
    *,
    text: str,
    index: bytes,
    submissions: bytes,
    symbol: str,
    identifier: str,
    series: str,
    share_class: str,
    fmp_name: str,
    fmp_exchange: str,
    stored_at: datetime,
    now: datetime,
) -> CurrentNasdaqEtfIdentity:
    class_name, index_ticker = _bound_nasdaq_class(
        index=index, symbol=symbol, series=series, share_class=share_class
    )
    for marker in (
        "Investment Objective",
        "Fund Fees and Expenses",
        "Principal Investment Strategies",
        "Principal Risks of Investing in the Fund",
        "Purchase and Sale of Shares",
        "Tax Information",
    ):
        if text.count(marker) != 1:
            raise ValueError
    cover = re.search(
        rf"Summary Prospectus ([A-Z][a-z]+ [0-9]{{1,2}}, [0-9]{{4}}) {symbol} "
        r"(.+?) The Nasdaq Stock Market LLC Before you invest",
        text[:1400],
    )
    if not cover:
        raise ValueError
    rendered_name = cover[2]
    name = re.sub(r"\s+SM\s*(?=,)", "", rendered_name)
    issuer_name = _json(submissions).get("name")
    if (
        not isinstance(issuer_name, str)
        or issuer_name.casefold() != name.casefold()
        or name.removeprefix("Invesco ") != class_name
        or datetime.strptime(cover[1], "%B %d, %Y").date() > now.date()
    ):
        raise ValueError
    objective = re.search(r"Investment Objective (.+?) Fund Fees and Expenses", text)
    strategy = re.search(
        r"Principal Investment Strategies (.+?) Principal Risks of Investing in the Fund", text
    )
    purchase = re.search(r"Purchase and Sale of Shares (.+?) Tax Information", text)
    if (
        not objective
        or not strategy
        or not purchase
        or re.fullmatch(
            "The "
            + re.escape(rendered_name)
            + r' \(the (?:“Fund” or “Trust”|"Fund" or "Trust"|Fund or Trust)\) '
            r"seeks to track the investment results, before fees and expenses, of the .+? "
            r'\(the (?:“Underlying Index”|"Underlying Index"|Underlying Index)\)\.',
            objective[1],
        )
        is None
        or not strategy[1].startswith(
            "The Fund will invest substantially all of its assets in the securities "
            "that comprise the Underlying Index."
        )
        or "common stocks and tracking stocks as well as American Depositary Receipts"
        not in strategy[1]
        or not purchase[1].startswith("The Fund issues and redeems Shares at NAV only with APs")
        or (
            "Individual Shares may only be bought and sold in the secondary market "
            "(i.e., on a national securities exchange) through a broker or dealer "
            "at a market price."
        )
        not in purchase[1]
    ):
        raise ValueError
    legal = re.findall(
        r"Effective after market close on ([A-Z][a-z]+ [0-9]{1,2}, [0-9]{4}), "
        r"the Fund was reclassified as an open-end management investment company",
        text,
    )
    if len(legal) != 1:
        raise ValueError
    changed = datetime.strptime(legal[0], "%B %d, %Y").date()
    if (
        changed >= now.date()
        or changed > datetime.strptime(cover[1], "%B %d, %Y").date()
        or "Prior to the Reclassification, the Fund operated as a unit investment trust" not in text
    ):
        raise ValueError
    return CurrentNasdaqEtfIdentity(
        symbol=symbol,
        cik=identifier,
        series_id=series,
        class_id=share_class,
        legal_name=name,
        fmp_name=fmp_name,
        fmp_name_matches_sec=fmp_name == name,
        fmp_exchange=fmp_exchange,
        objective=objective[1] + " Principal Investment Strategies " + strategy[1],
        legal_type_statement="Effective after market close on "
        + legal[0]
        + ", the Fund was reclassified as an open-end management investment company. "
        + purchase[1],
        mapping_stored_at=stored_at,
        filing_index_ticker=index_ticker,
        filing_index_class_name=class_name,
        reclassification_date=changed,
    )


def _current_held_nasdaq_identity(
    *,
    text: str,
    index: bytes,
    symbol: str,
    identifier: str,
    series: str,
    share_class: str,
    fmp_name: str,
    fmp_exchange: str,
    fmp_cik: str | None,
    stored_at: datetime,
    now: datetime,
) -> CurrentHeldNasdaqEtfIdentity:
    class_name, index_ticker = _bound_nasdaq_class(
        index=index, symbol=symbol, series=series, share_class=share_class
    )
    cover = text[:1600]
    dates = set(
        re.findall(
            r"(?:January|February|March|April|May|June|July|August|September|October|"
            r"November|December) [0-9]{1,2}, [0-9]{4}",
            cover,
        )
    )
    objective = re.search(r"Investment Objective (.+?) Fund Fees and Expenses", text)
    strategy = re.search(
        r"Principal Investment Strategy (.+?) Principal Investment Risks", text
    )
    purchase = re.search(r"Purchase and Sale of Fund Shares (.+?) Tax Information", text)
    if (
        index_ticker != symbol
        or class_name.casefold() != fmp_name.casefold()
        or cover.casefold().count(fmp_name.casefold()) != 1
        or len(re.findall(rf"(?<![A-Z0-9]){re.escape(symbol)}(?![A-Z0-9])", cover)) != 1
        or cover.count("The Nasdaq Stock Market LLC") != 1
        or len(dates) != 1
        or datetime.strptime(next(iter(dates)), "%B %d, %Y").date() > now.date()
        or text.count("Investment Objective") != 1
        or text.count("Fund Fees and Expenses") != 1
        or text.count("Principal Investment Strategy") != 1
        or text.count("Principal Investment Risks") != 1
        or text.count("Purchase and Sale of Fund Shares") != 1
        or text.count("Tax Information") != 1
        or not objective
        or not strategy
        or not purchase
    ):
        raise ValueError
    strategy_text = strategy[1].casefold()
    purchase_text = purchase[1]
    if (
        "swap agreement" not in strategy_text
        or "underlying security" not in strategy_text
        or "securities" not in strategy_text
        or (
            "Publicly issued equity securities, including common stocks, are subject to market "
            "risks that may cause their prices to fluctuate over time. Fluctuations in the value "
            "of equity securities in which the Fund invests, and/or has exposure to"
        )
        not in text
        or "Unlike most ETFs, the Fund currently intends to effect creations and redemptions"
        not in text
        or "secondary market" not in purchase_text
        or "broker-dealer" not in purchase_text
        or "Authorized Participants" not in purchase_text
    ):
        raise ValueError
    return CurrentHeldNasdaqEtfIdentity(
        symbol=symbol,
        cik=identifier,
        series_id=series,
        class_id=share_class,
        legal_name=class_name,
        fmp_exchange=fmp_exchange,
        fmp_cik=fmp_cik,
        fmp_cik_matches_sec=None if fmp_cik is None else fmp_cik == identifier,
        objective=objective[1],
        legal_type_statement=purchase[1],
        mapping_stored_at=stored_at,
        filing_index_class_name=class_name,
        strategy_statement=strategy[1],
    )


def _treasury_identity(
    *,
    text: str,
    index: bytes,
    symbol: str,
    identifier: str,
    series: str,
    share_class: str,
    fmp_name: str,
    fmp_exchange: str,
    stored_at: datetime,
    now: datetime,
) -> CurrentTreasuryEtfIdentity:
    parser = _IndexRows()
    parser.feed(index.decode("utf-8-sig"))
    series_rows = [r for r in parser.rows if r and r[0].startswith("Series ")]
    class_rows = [r for r in parser.rows if r and r[0].startswith("Class/Contract ")]
    if (
        not parser.scope_seen
        or parser.scope_depth is not None
        or len(series_rows) != 1
        or len(class_rows) != 1
    ):
        raise ValueError
    sr, cr = series_rows[0], class_rows[0]
    if (
        len(sr) != 3
        or len(cr) != 4
        or sr[0] != "Series " + series
        or cr[0] != "Class/Contract " + share_class
        or sr[2] != cr[2]
        or cr[3] != symbol
        or sr[2] != fmp_name
        or fmp_exchange not in {"NASDAQ", "XNAS"}
    ):
        raise ValueError
    name = sr[2]
    cover = re.search(
        r"FORM 497K (.+?) TLT \| Nasdaq Summary Prospectus — "
        r"([A-Za-z]+ [0-9]+, [0-9]{4}) Before you invest",
        text[:1000],
    )
    if (
        symbol != "TLT"
        or not cover
        or " ".join(cover[1].replace("®", "").split()).casefold() != name.casefold()
        or datetime.strptime(cover[2], "%B %d, %Y").date() > now.date()
        or text.count("FORM 497K") != 1
    ):
        raise ValueError
    for marker in (
        "Investment Objective",
        "Fees and Expenses",
        "Principal Investment Strategies",
        "Summary of Principal Risks",
        "Purchase and Sale of Fund Shares",
        "Tax Information",
    ):
        if text.count(marker) != 1:
            raise ValueError
    objective = re.search(r"Investment Objective (.+?) Fees and Expenses", text)
    strategy = re.search(r"Principal Investment Strategies (.+?) Summary of Principal Risks", text)
    legal = re.search(r"Purchase and Sale of Fund Shares (.+?) Tax Information", text)
    if (
        not objective
        or not strategy
        or not legal
        or re.fullmatch(
            re.escape(f"The {name} ")
            + r"\(the “\s*Fund\s*”\s*\) seeks to track the investment results "
            r"of an index composed of U\.S\. Treasury bonds with remaining maturities "
            r"greater than twenty years\.",
            objective[1],
        )
        is None
        or not re.match(
            r"The Fund seeks to track the investment results of the ICE(?: ?®)? "
            r"U\.S\. Treasury 20\+ Year Bond Index \(the “\s*Underlying Index\s*”\s*\), "
            r"which measures the performance of public obligations of the U\.S\. "
            r"Treasury that have a remaining maturity greater than twenty years\.",
            strategy[1],
        )
        or "The Fund will invest at least 80% of its assets in the component securities of the "
        "Underlying Index, and the Fund will invest at least 90% of its assets in U.S. Treasury "
        "securities that BFA believes will help the Fund track the Underlying Index."
        not in strategy[1]
        or not legal[1].startswith(
            "The Fund is an exchange-traded fund (commonly referred to as an"
        )
        or "Individual shares of the Fund may only be bought and sold in the secondary market "
        "through a broker-dealer."
        not in legal[1]
    ):
        raise ValueError
    return CurrentTreasuryEtfIdentity(
        symbol=symbol,
        cik=identifier,
        series_id=series,
        class_id=share_class,
        legal_name=name,
        fmp_exchange=fmp_exchange,
        objective=objective[1] + " Principal Investment Strategies " + strategy[1],
        legal_type_statement=legal[1],
        mapping_stored_at=stored_at,
    )


def _share_class_identity(
    *,
    text: str,
    index: bytes,
    symbol: str,
    identifier: str,
    series: str,
    share_class: str,
    fmp_name: str,
    fmp_exchange: str,
    stored_at: datetime,
) -> CurrentEtfShareClassIdentity:
    parser = _VisibleText()
    parser.feed(index.decode("utf-8-sig"))
    index_text = " ".join(" ".join(parser.parts).split())
    match = re.search(
        rf"Series {series} (.+?) Class/Contract {share_class} ETF Shares (\S+)", index_text
    )
    if not match or not 1 < len(match[1]) < 200 or match[2] != symbol:
        raise ValueError
    fund_name = match[1]
    legal_name = fund_name + " ETF Shares"
    statement = (
        "Exchange-traded fund shares that are not individually redeemable "
        "and are listed on NYSE Arca"
    )
    if any(
        text.count(marker) != 1
        for marker in (
            statement,
            "Investment Objective",
            "Fees and Expenses",
            "Principal Investment Strategies",
            "Principal Risks",
        )
    ):
        raise ValueError
    cover = re.search(
        rf"Summary Prospectus (.+?) {statement} {re.escape(legal_name)} "
        rf"\({re.escape(symbol)}\)",
        text[:1200],
    )
    objective = re.search(r"Investment Objective (.+?) Fees and Expenses", text)
    strategy = re.search(r"Principal Investment Strategies (.+?) Principal Risks", text)
    if (
        not cover
        or not objective
        or not strategy
        or re.fullmatch(
            re.escape(cover[1] + ", an exchange-traded share class of " + fund_name)
            + r' \(the (?:“Fund”|"Fund"|Fund)\), seeks to track the performance of a '
            r"benchmark index that measures the investment return of the overall stock market\.",
            objective[1],
        )
        is None
        or not strategy[1].startswith("The Fund ")
        or "investable U.S. stock market" not in strategy[1]
        or "Fund invests at least 80% of its net assets" not in strategy[1]
        or "in the stocks that make up the Target Index" not in strategy[1]
    ):
        raise ValueError
    return CurrentEtfShareClassIdentity(
        symbol=symbol,
        cik=identifier,
        series_id=series,
        class_id=share_class,
        legal_name=legal_name,
        fmp_name=fmp_name,
        fmp_name_matches_sec=fmp_name in {legal_name, cover[1]},
        fmp_exchange=fmp_exchange,
        objective=objective[1] + " Principal Investment Strategies " + strategy[1],
        legal_type_statement=statement,
        mapping_stored_at=stored_at,
    )


@dataclass(frozen=True)
class EtfInputs:
    identity: (
        CurrentEtfIdentity
        | CurrentUitIdentity
        | CurrentTreasuryEtfIdentity
        | CurrentGoldTrustIdentity
    )
    contents: tuple[bytes, ...]
    sec_url: str
    received: tuple[datetime | None, ...]

    @property
    def revision(self) -> str:
        return sha256(
            (
                SCHEMA
                + self.identity.symbol
                + ":".join(sha256(raw).hexdigest() for raw in self.contents)
            ).encode()
        ).hexdigest()


def _json(raw: bytes) -> Any:
    return json.loads(raw, object_pairs_hook=_unique, parse_constant=_invalid_constant)


class _Replay:
    """Only already archived paths; cannot issue HTTP or fall back to a network source."""

    def __init__(self, values: dict[tuple[str, ...], bytes]):
        self.values = values
        self.used: set[tuple[str, ...]] = set()

    async def get_bytes(
        self,
        path_segments: tuple[str, ...],
        query: tuple[tuple[str, str], ...],
        headers: tuple[tuple[str, str], ...],
    ) -> bytes:
        if query or path_segments not in self.values or path_segments in self.used:
            raise ValueError
        self.used.add(path_segments)
        return self.values[path_segments]


async def _validate_etf_originals(
    *,
    symbol: str,
    contents: tuple[bytes, ...],
    now: datetime,
    allow_treasury: bool = False,
    current_holding_only: bool = False,
) -> EtfInputs:
    try:
        if current_holding_only:
            require_outside_fixed_etf_definition(symbol)
            asset_type = "equity_etf"
            if allow_treasury:
                raise ValueError
        else:
            asset_type = member_for(symbol).asset_type
            if allow_treasury != (asset_type == "non_equity_etf"):
                raise ValueError
        cutoff = require_utc(now)
        if len(contents) != 8 or any(not b or len(b) > MAX_SEC_BYTES for b in contents):
            raise ValueError
        fmp, fm_receipt, mapping, submissions, index, raw, receipts_raw, mapping_receipt = contents
        row = parse_profile(fmp, symbol)
        identifier, series, share_class = resolve_fund(_json(mapping), symbol)
        fmp_cik = None if row.get("cik") is None else cik(row.get("cik"))
        if (
            (not current_holding_only and fmp_cik != identifier)
            or row.get("currency") != "USD"
            or row.get("isEtf") is not True
            or row.get("exchange")
            not in {"AMEX", "ARCA", "NYSE Arca", "NYSEARCA", "NASDAQ", "XNAS"}
        ):
            raise ValueError
        f = _json(fm_receipt)
        if (
            f
            != dict(
                schema="fmp_profile_receipt_v1",
                source="fmp",
                origin="https://financialmodelingprep.com",
                path="/stable/profile",
                symbol=symbol,
                raw_sha256=sha256(fmp).hexdigest(),
                raw_byte_length=len(fmp),
                received_at=f["received_at"],
            )
            or type(f["raw_byte_length"]) is not int
            or require_utc(datetime.fromisoformat(f["received_at"])) > cutoff
        ):
            raise ValueError
        binding = _json(mapping_receipt)
        stored_at = require_utc(datetime.fromisoformat(binding["stored_at"]))
        if (
            binding["sha256"] != sha256(mapping).hexdigest()
            or binding["original_received_at"] is not None
            or stored_at > cutoff
        ):
            raise ValueError
        receipts = _json(receipts_raw)
        if not isinstance(receipts, list) or len(receipts) != 3:
            raise ValueError
        paths = []
        for number, (receipt, content) in enumerate(
            zip(receipts, (submissions, index, raw), strict=True)
        ):
            origin = "https://data.sec.gov/" if number == 0 else "https://www.sec.gov/"
            if (
                not receipt["url"].startswith(origin)
                or receipt
                != dict(
                    url=receipt["url"],
                    status=200,
                    received_at=receipt["received_at"],
                    sha256=sha256(content).hexdigest(),
                    file=f"response-{number + 1}.raw",
                )
                or type(receipt["status"]) is not int
                or require_utc(datetime.fromisoformat(receipt["received_at"])) > cutoff
            ):
                raise ValueError
            paths.append(tuple(receipt["url"][len(origin) :].split("/")))
        accession_match = re.fullmatch(r"([0-9]{10}-[0-9]{2}-[0-9]{6})-index.html", paths[1][-1])
        if not accession_match or paths[0] != ("submissions", f"CIK{identifier}.json"):
            raise ValueError
        replay = _Replay(dict(zip(paths, (submissions, index, raw), strict=True)))
        disclosure, _ = await fetch_fund_disclosure(
            instrument_id=UUID(int=0),
            symbol=symbol,
            identity=(identifier, series, share_class),
            accession=accession_match[1],
            client=SecCompanyFactsClient(
                SecHttpTransport(replay),
                contact="offline@example.invalid",
                max_attempts=1,
                now=lambda: datetime.fromisoformat(receipts[0]["received_at"]),
            ),
            directory=replay,
            contact="offline@example.invalid",
        )
        if disclosure.source_url != receipts[2]["url"] or len(replay.used) != 3:
            raise ValueError
        visible = _VisibleText()
        visible.feed(raw.decode("utf-8-sig"))
        text = " ".join(" ".join(visible.parts).split())
        name = row.get("companyName")
        if not isinstance(name, str) or not name.strip():
            raise ValueError
        name = " ".join(name.split())
        identity: (
            CurrentEtfIdentity
            | CurrentUitIdentity
            | CurrentTreasuryEtfIdentity
            | CurrentGoldTrustIdentity
        )
        if asset_type == "non_equity_etf":
            identity = _treasury_identity(
                text=text,
                index=index,
                symbol=symbol,
                identifier=identifier,
                series=series,
                share_class=share_class,
                fmp_name=name,
                fmp_exchange=row["exchange"],
                stored_at=stored_at,
                now=cutoff,
            )
        elif row["exchange"] in {"NASDAQ", "XNAS"}:
            if current_holding_only:
                identity = _current_held_nasdaq_identity(
                    text=text,
                    index=index,
                    symbol=symbol,
                    identifier=identifier,
                    series=series,
                    share_class=share_class,
                    fmp_name=name,
                    fmp_exchange=row["exchange"],
                    fmp_cik=fmp_cik,
                    stored_at=stored_at,
                    now=cutoff,
                )
            else:
                identity = _nasdaq_identity(
                    text=text,
                    index=index,
                    submissions=submissions,
                    symbol=symbol,
                    identifier=identifier,
                    series=series,
                    share_class=share_class,
                    fmp_name=name,
                    fmp_exchange=row["exchange"],
                    stored_at=stored_at,
                    now=cutoff,
                )
        else:
            cover = re.search(
                rf"FORM 497K {re.escape(name)} {re.escape(symbol)} "
                r"\| NYSE Arca Summary Prospectus\b",
                text[:1000],
            )
            objective = re.search(r"Investment Objective (.+?) Fees and Expenses", text)
            legal = re.search(
                r"Purchase and Sale of Fund Shares (The Fund is an exchange-traded fund\b[^.]*\.)",
                text,
            )
            if (
                not cover
                or not objective
                or not legal
                or name not in objective[1]
                or not re.search(r"index composed of [^.]*U\.S\. equities\.", objective[1])
                or len(re.findall(r"\bFORM 497K\b", text)) != 1
            ):
                identity = _share_class_identity(
                    text=text,
                    index=index,
                    symbol=symbol,
                    identifier=identifier,
                    series=series,
                    share_class=share_class,
                    fmp_name=name,
                    fmp_exchange=row["exchange"],
                    stored_at=stored_at,
                )
            else:
                identity = CurrentEtfIdentity(
                    symbol=symbol,
                    cik=identifier,
                    series_id=series,
                    class_id=share_class,
                    legal_name=name,
                    fmp_exchange=row["exchange"],
                    objective=objective[1],
                    legal_type_statement=legal[1],
                    mapping_stored_at=stored_at,
                )
        fmp_received = require_utc(datetime.fromisoformat(f["received_at"]))
        sec_received = tuple(
            require_utc(datetime.fromisoformat(r["received_at"])) for r in receipts
        )
        if tuple(sorted(sec_received)) != sec_received:
            raise ValueError
        return EtfInputs(
            identity,
            contents,
            disclosure.source_url,
            (fmp_received, fmp_received, None, *sec_received, sec_received[-1], None),
        )
    except (ValueError, TypeError, KeyError, IndexError, UnicodeError, RecursionError) as error:
        raise ValueError("CURRENT_ETF_IDENTITY_INVALID") from error


async def validate_etf_originals(
    *, symbol: str, contents: tuple[bytes, ...], now: datetime, allow_treasury: bool = False
) -> EtfInputs:
    return await _validate_etf_originals(
        symbol=symbol,
        contents=contents,
        now=now,
        allow_treasury=allow_treasury,
    )


async def validate_current_held_etf_originals(
    *, symbol: str, contents: tuple[bytes, ...], now: datetime
) -> EtfInputs:
    """Validate a current holding without admitting it to the fixed research universe."""

    return await _validate_etf_originals(
        symbol=symbol,
        contents=contents,
        now=now,
        current_holding_only=True,
    )
