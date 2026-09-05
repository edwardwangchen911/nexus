"""Current physical gold trust evidence; no common-stock or registered-fund inference."""

import re
from datetime import date, datetime
from hashlib import sha256
from urllib.parse import urljoin

from nexus.domain.time import require_utc
from nexus.market.current_etf_identity import CurrentGoldTrustIdentity, EtfInputs, _json, member_for
from nexus.market.current_identity import MAX_SEC_BYTES, SEC_URL, _Cover, cik
from nexus.market.current_uit_identity import _Links, _visible
from nexus.providers.fmp_profile import parse_profile


def validate_gold_originals(
    *, symbol: str, contents: tuple[bytes, ...], now: datetime
) -> EtfInputs:
    try:
        # Bind the physical gold trust itself, without inferring registered-fund status.
        if symbol != "GLD":
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
            recent["form"][i] != "10-K"
            or recent["primaryDocument"][i] != primary
            or accepted > min(times[2:])
        ):
            raise ValueError
        links = _Links()
        links.feed(index.decode("utf-8-sig"))
        # SEC wraps inline-XBRL primary documents in a same-origin viewer URL.
        primary_links = [
            urljoin("https://www.sec.gov", link[len("/ix?doc=") :])
            if link.startswith("/ix?doc=/Archives/edgar/data/")
            else urljoin(base, link)
            for link in links.links
        ]
        if primary_links.count(receipts[2]["source_url"]) != 1:
            raise ValueError
        index_text = _visible(index)
        if (
            len(re.findall(r"SEC Accession No\. " + re.escape(accession), index_text)) != 1
            or not re.search(r"\bCIK\s*:?\s*" + identifier + r"\b", index_text)
            or not re.search(r"\bForm 10-K\b", index_text)
        ):
            raise ValueError
        parser = _Cover(dual_listing=True)
        parser.feed(document.decode("utf-8-sig"))
        parser.close()
        if (
            parser.stack
            or parser.active
            or parser.parents
            or parser.context
            or parser.scope_exceptions
            or len(parser.facts) != 4
        ):
            raise ValueError
        if (
            parser.namespaces.get("xmlns:ix") != "http://www.xbrl.org/2013/inlineXBRL"
            or parser.namespaces.get("xmlns:xbrli") != "http://www.xbrl.org/2003/instance"
            or not re.fullmatch(
                r"http://xbrl\.sec\.gov/dei/[0-9]{4}", parser.namespaces.get("xmlns:dei") or ""
            )
        ):
            raise ValueError
        for fact in parser.facts:
            scope = parser.fact_scope[fact["id"]]
            if scope["parent_fact_id"] or scope["ix_ancestors"] - {
                "ix:nonnumeric",
                "ix:header",
                "ix:hidden",
            }:
                raise ValueError
            if any(key in scope["attrs"] for key in ("target", "tupleref", "order", "xml:base")):
                raise ValueError
            if "format" in scope["attrs"] and not (
                fact["name"] == "dei:SecurityExchangeName"
                and fact["value"] == "NYSE Arca, Inc."
                and scope["attrs"]["format"] == "ixt-sec:exchnameen"
                and parser.namespaces.get("xmlns:ixt-sec")
                == "http://www.sec.gov/inlineXBRL/transformation/2015-08-31"
            ):
                raise ValueError
        facts = {fact["name"]: fact for fact in parser.facts}
        if set(facts) != {
            "dei:EntityCentralIndexKey",
            "dei:TradingSymbol",
            "dei:Security12bTitle",
            "dei:SecurityExchangeName",
        }:
            raise ValueError
        issuer = facts["dei:EntityCentralIndexKey"]
        listing = [fact for fact in parser.facts if fact is not issuer]
        if (
            cik(issuer["value"]) != identifier
            or len({f["context"] for f in parser.facts}) != 1
            or len({f["row"] for f in listing}) != 1
            or listing[0]["row"] is None
            or issuer["context"] not in parser.plain_contexts
            or any(
                not parser.fact_scope[f["id"]]["visible"]
                or parser.fact_scope[f["id"]]["parent_fact_id"]
                for f in listing
            )
        ):
            raise ValueError
        context = parser.contexts[issuer["context"]]
        fields = context["fields"]
        if (
            cik(fields[0]["value"]) != identifier
            or not date.fromisoformat(fields[1]["value"])
            <= date.fromisoformat(fields[2]["value"])
            <= accepted.date()
        ):
            raise ValueError
        shares = facts["dei:Security12bTitle"]["value"].replace("®", "")
        shares = " ".join(shares.split())
        if (
            facts["dei:TradingSymbol"]["value"].removesuffix("®") != symbol
            or facts["dei:SecurityExchangeName"]["value"] != "NYSE Arca, Inc."
        ):
            raise ValueError
        text = _visible(document)
        business = list(
            re.finditer(
                r"Item 1\. Business ("
                + re.escape(sub["name"])
                + r") \(the “Trust”\) is an investment trust, formed on "
                r"([A-Za-z]+ [0-9]+, [0-9]{4}) under New York law pursuant to a trust indenture "
                r"\(the “Trust Indenture”\)\. (.+?) Item 1A\. Risk Factors",
                text,
                re.I,
            )
        )
        if len(business) != 1 or text.count("FORM 10-K") != 1:
            raise ValueError
        business_match = business[0]
        if datetime.strptime(business_match[2], "%B %d, %Y").date() > accepted.date():
            raise ValueError
        region = business_match[3]
        holding = (
            "The Trust holds gold and from time to time issues "
            + shares
            + (
                " (the “Shares”) in Baskets in exchange for deposits of gold and distributes gold "
                "in connection with redemptions of Baskets."
            )
        )
        objective = (
            "The investment objective of the Trust is for the Shares to reflect the "
            "performance of the price of gold bullion, less the Trust’s expenses."
        )
        share_statement = (
            "The Shares represent units of fractional undivided beneficial interest "
            "in and ownership of the Trust."
        )
        legal = (
            "The Trust is not registered as an investment company under the Investment "
            "Company Act of 1940 and is not required to register under such act."
        )
        if any(
            region.count(statement) != 1
            for statement in (holding, objective, share_statement, legal)
        ):
            raise ValueError
        identity = CurrentGoldTrustIdentity(
            symbol=symbol,
            cik=identifier,
            legal_name=business_match[1],
            shares_name=shares,
            fmp_name=row["companyName"],
            fmp_name_matches_sec=row["companyName"] == business_match[1],
            fmp_exchange=row["exchange"],
            sec_exchange=sub["exchanges"][0],
            objective=objective + " " + holding,
            legal_type_statement=business_match[0].split(holding, 1)[0] + legal,
            share_statement=share_statement,
            registration_facts=tuple(parser.facts),
            registration_context=context,
        )
        return EtfInputs(identity, contents, receipts[2]["source_url"], tuple(times))
    except (ValueError, TypeError, KeyError, IndexError, UnicodeError) as error:
        raise ValueError("CURRENT_ETF_IDENTITY_INVALID") from error
