"""Narrow current stock UIT proof from registered ticker and bound full 497 originals."""

import re
from datetime import datetime
from hashlib import sha256
from html.parser import HTMLParser
from urllib.parse import urljoin

from nexus.domain.time import require_utc
from nexus.market.current_etf_identity import (
    CurrentUitIdentity,
    EtfInputs,
    _json,
    member_for,
)
from nexus.market.current_identity import MAX_SEC_BYTES, SEC_URL, cik
from nexus.providers.fmp_profile import parse_profile
from nexus.research.sec_disclosures import _VisibleText


class _Links(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "a" and (href := dict(attrs).get("href")):
            self.links.append(href)


def _visible(raw: bytes) -> str:
    parser = _VisibleText()
    parser.feed(raw.decode("utf-8-sig"))
    return " ".join(" ".join(parser.parts).replace("®", "").split())


def validate_uit_originals(*, symbol: str, contents: tuple[bytes, ...], now: datetime) -> EtfInputs:
    try:
        # Only independently bound stock-index UIT contracts; no inference for other funds.
        if symbol not in {"SPY", "DIA"}:
            raise ValueError
        member_for(symbol)
        cutoff = require_utc(now)
        if len(contents) != 8 or any(not raw or len(raw) > MAX_SEC_BYTES for raw in contents):
            raise ValueError
        fmp, fm_receipt, submissions, sr, index, ir, document, dr = contents
        row = parse_profile(fmp, symbol)
        identifier = cik(row.get("cik"))
        if (
            row.get("currency") != "USD"
            or row.get("isEtf") is not True
            or row.get("exchange") not in {"AMEX", "NYSE", "ARCA", "NYSE Arca"}
        ):
            raise ValueError
        f = _json(fm_receipt)
        if f != dict(
            schema="fmp_profile_receipt_v1",
            source="fmp",
            origin="https://financialmodelingprep.com",
            path="/stable/profile",
            symbol=symbol,
            raw_sha256=sha256(fmp).hexdigest(),
            raw_byte_length=len(fmp),
            received_at=f["received_at"],
        ):
            raise ValueError
        times = [require_utc(datetime.fromisoformat(f["received_at"]))] * 2
        receipts = [_json(r) for r in (sr, ir, dr)]
        for raw, receipt in zip((submissions, index, document), receipts, strict=True):
            if receipt != dict(
                source_url=receipt["source_url"],
                received_at=receipt["received_at"],
                sha256=sha256(raw).hexdigest(),
                byte_length=len(raw),
                http_status=200,
            ):
                raise ValueError
            times.extend([require_utc(datetime.fromisoformat(receipt["received_at"]))] * 2)
        if any(stamp > cutoff for stamp in times):
            raise ValueError
        if receipts[0]["source_url"] != f"https://data.sec.gov/submissions/CIK{identifier}.json":
            raise ValueError
        match = SEC_URL.fullmatch(receipts[2]["source_url"])
        if not match or cik(match[1]) != identifier:
            raise ValueError
        compact, primary = match[2], match[3]
        accession = compact[:10] + "-" + compact[10:12] + "-" + compact[12:]
        base = receipts[2]["source_url"].rsplit("/", 1)[0] + "/"
        if receipts[1]["source_url"] != base + accession + "-index.html":
            raise ValueError
        sub = _json(submissions)
        if (
            not isinstance(sub.get("name"), str)
            or cik(sub["cik"]) != identifier
            or sub["tickers"] != [symbol]
            or sub["exchanges"] != ["NYSE"]
        ):
            raise ValueError
        recent = sub["filings"]["recent"]
        selected = [i for i, value in enumerate(recent["accessionNumber"]) if value == accession]
        if len(selected) != 1:
            raise ValueError
        i = selected[0]
        accepted = require_utc(datetime.fromisoformat(recent["acceptanceDateTime"][i]))
        if (
            recent["form"][i] != "497"
            or recent["primaryDocument"][i] != primary
            or accepted > min(times[2:])
        ):
            raise ValueError
        links = _Links()
        links.feed(index.decode("utf-8-sig"))
        if sum(urljoin(base, link) == receipts[2]["source_url"] for link in links.links) != 1:
            raise ValueError
        index_text = _visible(index)
        if (
            len(re.findall(r"SEC Accession No\. " + re.escape(accession), index_text)) != 1
            or not re.search(r"\bCIK\s*:?\s*" + identifier + r"\b", index_text)
            or not re.search(r"\bForm 497\b", index_text)
        ):
            raise ValueError
        text = _visible(document)
        cover = re.search(
            r"Filed Pursuant to Rule 497\(b\) Registration File No\. [0-9-]+ "
            rf"(.+?) \(“{symbol}” or the “Trust”\) \(formerly, “(.+?)”\) "
            r"\(A Unit Investment Trust\) Principal U\.S\. Listing Exchange for (.+?): "
            rf"NYSE Arca, Inc\. under the symbol “{symbol}” Prospectus Dated "
            r"([A-Za-z]+ [0-9]+, [0-9]{4})",
            text[:1400],
        )
        former = cover[2] if cover else ""
        if symbol == "DIA":
            former = re.sub(r"(?<=INDUSTRIAL AVERAGE) SM(?= ETF TRUST)", "", former, flags=re.I)
        if (
            not cover
            or cover[1] != cover[3]
            or former.casefold() != sub["name"].casefold()
            or text.count("Filed Pursuant to Rule 497(b)") != 1
            or datetime.strptime(cover[4], "%B %d, %Y").date() > accepted.date()
        ):
            raise ValueError
        index_objective = (
            "S&P 500 Index (the “Index”)"
            if symbol == "SPY"
            else "Dow Jones Industrial Average (the “DJIA” or “Index”)"
        )
        strategy_index = "Index" if symbol == "SPY" else "DJIA"
        objective = (
            "SUMMARY Investment Objective The Trust seeks to provide investment results that, "
            "before expenses, correspond generally to the price and yield performance of the "
            f"{index_objective}. Fees and Expenses of the Trust"
        )
        stocks = (
            "The Trust seeks to achieve its investment objective by holding a portfolio of the "
            f"common stocks that are included in the {strategy_index} (the “Portfolio”), "
            "with the weight of "
            "each stock in the Portfolio substantially corresponding to the weight of such stock "
            f"in the {strategy_index}."
        )
        if (
            text.count(objective) != 1
            or text.count("The Trust’s Investments and Portfolio Turnover " + stocks) != 1
        ):
            raise ValueError
        identity = CurrentUitIdentity(
            symbol=symbol,
            cik=identifier,
            legal_name=cover[1],
            former_name=former,
            fmp_name=row["companyName"],
            fmp_name_matches_sec=row["companyName"] == cover[1],
            fmp_exchange=row["exchange"],
            sec_exchange=sub["exchanges"][0],
            objective=objective + " " + stocks,
            legal_type_statement=cover[0],
        )
        return EtfInputs(identity, contents, receipts[2]["source_url"], tuple(times))
    except (ValueError, TypeError, KeyError, IndexError, UnicodeError) as error:
        raise ValueError("CURRENT_ETF_IDENTITY_INVALID") from error
