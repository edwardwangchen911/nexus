"""Current SEC directory conflicts and narrowly supported 2X long ETF source classification.

No broker exposure, canonical identity registration, account reconciliation or
research/trading authorization is supplied by this offline source validation.
"""

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from hashlib import sha256
from typing import Any, Literal

from nexus.domain.time import require_utc
from nexus.market.cboe_listing import _visible
from nexus.market.cboe_source_chain import CboeFundSourceChain, _json, validate_cboe_source_chain


@dataclass(frozen=True)
class CboeFundIdentitySources:
    source_chain: CboeFundSourceChain
    company_directory_status: Literal["absent"]
    fund_directory_status: Literal["absent", "exact_match"]
    directory_contents: tuple[bytes, ...] = field(repr=False)
    directory_received: tuple[datetime, ...]
    underlying_symbol: str
    objective: str = field(repr=False)
    strategy: str = field(repr=False)
    asset_type: Literal["equity_etf"] = "equity_etf"
    strategy_kind: Literal["daily_2x_long_single_stock"] = "daily_2x_long_single_stock"
    leverage_factor: Literal[2] = 2
    identity_registration_ready: Literal[False] = False
    new_position_eligible: Literal[False] = False
    execution_locked: Literal[True] = True

    @property
    def revision(self) -> str:
        return sha256(
            (
                "cboe_fund_sources_v1"
                + self.source_chain.revision
                + ":".join(sha256(raw).hexdigest() for raw in self.directory_contents)
            ).encode()
        ).hexdigest()


def _directory(
    raw: bytes, receipt_raw: bytes, *, fields: list[str], filename: str, cutoff: datetime
) -> tuple[list[list[Any]], datetime]:
    doc = _json(raw)
    if (
        not isinstance(doc, dict)
        or set(doc) != {"fields", "data"}
        or doc["fields"] != fields
        or not isinstance(doc["data"], list)
        or not doc["data"]
    ):
        raise ValueError
    rows: list[list[Any]] = doc["data"]
    for row in rows:
        if (
            not isinstance(row, list)
            or len(row) != 4
            or type(row[0]) is not int
            or not 0 < row[0] < 10**10
            or not isinstance(row[2], str)
            or not isinstance(row[3], str | None)
        ):
            raise ValueError
        if filename == "company_tickers_mf.json" and (
            not isinstance(row[1], str)
            or not re.fullmatch(r"S[0-9]{9}", row[1])
            or not re.fullmatch(r"C[0-9]{9}", row[2])
            or not isinstance(row[3], str)
        ):
            raise ValueError
    receipt = _json(receipt_raw)
    if not isinstance(receipt, dict):
        raise ValueError
    received = require_utc(datetime.fromisoformat(receipt["received_at"]))
    expected = {
        "source_url": "https://www.sec.gov/files/" + filename,
        "http_status": 200,
        "received_at": receipt["received_at"],
        "sha256": sha256(raw).hexdigest(),
        "byte_length": len(raw),
    }
    # Preserve the already collected company directory's exact original receipt schema.
    if filename == "company_tickers_exchange.json" and "schema" in receipt:
        expected["schema"] = "sec_identity_source_receipt_v1"
    if (
        receipt != expected
        or type(receipt["byte_length"]) is not int
        or type(receipt["http_status"]) is not int
        or not timedelta(0) <= cutoff - received <= timedelta(hours=24)
    ):
        raise ValueError
    return rows, received


def _section(paragraphs: list[str], start: str, end: str, maximum: int) -> str:
    headings = [
        index for index, paragraph in enumerate(paragraphs) if re.fullmatch(start, paragraph, re.I)
    ]
    boundaries = [
        index for index, paragraph in enumerate(paragraphs) if re.fullmatch(end, paragraph, re.I)
    ]
    if len(headings) != 1 or len(boundaries) != 1 or boundaries[0] <= headings[0]:
        raise ValueError
    result = " ".join(paragraphs[headings[0] + 1 : boundaries[0]]).strip()
    if not 20 <= len(result) <= maximum:
        raise ValueError
    return result


def validate_cboe_fund_sources(
    *,
    symbol: str,
    contents: tuple[bytes, ...],
    directory_contents: tuple[bytes, ...],
    now: datetime,
) -> CboeFundIdentitySources:
    try:
        cutoff = require_utc(now)
        chain = validate_cboe_source_chain(symbol=symbol, contents=contents, now=cutoff)
        if len(directory_contents) != 4 or any(
            not 0 < len(raw) <= (8192 if index % 2 else 8 * 1024 * 1024)
            for index, raw in enumerate(directory_contents)
        ):
            raise ValueError
        companies, company_received = _directory(
            directory_contents[0],
            directory_contents[1],
            fields=["cik", "name", "ticker", "exchange"],
            filename="company_tickers_exchange.json",
            cutoff=cutoff,
        )
        funds, fund_received = _directory(
            directory_contents[2],
            directory_contents[3],
            fields=["cik", "seriesId", "classId", "symbol"],
            filename="company_tickers_mf.json",
            cutoff=cutoff,
        )
        if any(row[2] == symbol for row in companies):
            raise ValueError
        matches = [row for row in funds if row[3] == symbol]
        class_matches = [row for row in funds if row[2] == chain.submission.class_id]
        expected_row = [
            int(chain.submission.cik),
            chain.submission.series_id,
            chain.submission.class_id,
            symbol,
        ]
        if matches != class_matches or (matches and matches != [expected_row]):
            raise ValueError
        fund_status: Literal["absent", "exact_match"] = "exact_match" if matches else "absent"
        if (
            chain.listing.product_name.casefold() != chain.submission.class_name.casefold()
            or not re.search(r"\b2X Long\b", chain.submission.class_name)
        ):
            raise ValueError
        html = chain.submission.component_raw.decode("utf-8-sig")
        paragraph_raw: list[str] = re.findall(r"<p(?:\s[^>]*)?>(.*?)</p\s*>", html, re.S | re.I)
        if len(paragraph_raw) != len(re.findall(r"<p(?:\s[^>]*)?>", html, re.I)):
            raise ValueError
        paragraphs = [_visible(paragraph) for paragraph in paragraph_raw]
        objective = _section(
            paragraphs,
            r"Investment Objective\b",
            r"(?:Fund Fees and Expenses|Fees and Expenses(?: of the Fund)?)\b",
            3000,
        )
        strategy = _section(
            paragraphs,
            r"Principal Investment Strateg(?:y|ies)\b",
            r"Principal(?: Investment)? Risks\b",
            10000,
        )
        # This branch accepts the supported positive target and its daily-period
        # caveat, not arbitrary prose containing otherwise positive keywords.
        underlying = re.fullmatch(
            r"The Fund seeks daily investment results, before fees and expenses, of "
            r"two times \(200%\) the daily percentage change in the share price of "
            r".+?\((?:Nasdaq|NASDAQ|NYSE): ([A-Z][A-Z0-9.\-]{0,15})\)"
            r"(?: \(the [“\"]Underlying Security[”\"]\))?\."
            r"(?: The Fund does not seek(?: to achieve)? its stated investment objective "
            r"(?:over|for) (?:a )?(?:period(?: of time)?|time period) "
            r"(?:different|longer|greater|other) than (?:a|one|a single) (?:trading )?day\.)?",
            objective,
        )
        ticker_pattern = r"\b(?:Nasdaq|NASDAQ|NYSE): ([A-Z][A-Z0-9.\-]{0,15})\b"
        objective_symbols = re.findall(ticker_pattern, objective)
        strategy_symbols = re.findall(ticker_pattern, strategy)
        multipliers = re.findall(
            r"\b(one|two|three|four|five|[0-9]+(?:\.[0-9]+)?) times \(([+-]?[0-9]+)%\)",
            strategy,
            re.I,
        )
        if (
            underlying is None
            or underlying[1] == symbol
            or objective_symbols != [underlying[1]]
            or any(stock != underlying[1] for stock in strategy_symbols)
            or not re.search(r"exchange[ -]traded fund", strategy, re.I)
            or not re.search(
                r"\b(?:seeks|attempts) to achieve two times \(200%\) "
                r"the daily percentage change in the share price of the Underlying Security\b",
                strategy,
                re.I,
            )
            or any(
                (factor.casefold(), percentage) != ("two", "200")
                for factor, percentage in multipliers
            )
            or re.search(
                r"\b(?:does not|never|no longer)\s+(?:seek|attempt)s?\s+to achieve\b"
                r"|\b(?:negative|minus|inverse)\b.{0,60}"
                r"\b(?:times|daily percentage change|daily investment results|exposure|returns)\b"
                r"|\b(?:monthly|weekly|quarterly|annual|yearly)\s+"
                r"(?:investment )?(?:results|returns|percentage change)\b",
                strategy,
                re.I,
            )
            or "swap agreements" not in strategy.casefold()
            or "listed options contracts" not in strategy.casefold()
            or re.search(
                r"\b(?:bitcoin|ether|bullion|commodit(?:y|ies)|futures)\b",
                objective + " " + strategy,
                re.I,
            )
        ):
            raise ValueError
        return CboeFundIdentitySources(
            chain,
            "absent",
            fund_status,
            directory_contents,
            (company_received, company_received, fund_received, fund_received),
            underlying[1],
            objective,
            strategy,
        )
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
        raise ValueError("CBOE_FUND_SOURCES_INVALID") from None
