"""Handwritten synthetic fund archives; no real captures."""

import json
from hashlib import sha256


def originals(
    *,
    document=None,
    exchange="AMEX",
    is_etf=True,
    index_class="C000000002",
    symbol="IWM",
    company_name="Synthetic Equity Fund",
    index_name="",
):
    stamp = "2026-01-03T00:00:00+00:00"
    accession = "0000123456-26-000001"
    base = "https://www.sec.gov/Archives/edgar/data/123456/000012345626000001/"
    fmp = json.dumps(
        [
            dict(
                symbol=symbol,
                companyName=company_name,
                cik="0000123456",
                currency="USD",
                exchange=exchange,
                isEtf=is_etf,
            )
        ]
    ).encode()
    receipt = json.dumps(
        dict(
            schema="fmp_profile_receipt_v1",
            source="fmp",
            origin="https://financialmodelingprep.com",
            path="/stable/profile",
            symbol=symbol,
            raw_sha256=sha256(fmp).hexdigest(),
            raw_byte_length=len(fmp),
            received_at=stamp,
        )
    ).encode()
    mapping = json.dumps(
        dict(
            fields=["cik", "seriesId", "classId", "symbol"],
            data=[[123456, "S000000001", "C000000002", symbol]],
        )
    ).encode()
    submissions = json.dumps(
        dict(
            cik=123456,
            filings=dict(
                recent=dict(
                    accessionNumber=[accession],
                    acceptanceDateTime=["2026-01-02T00:00:00Z"],
                    filingDate=["2026-01-02"],
                    form=["497K"],
                    primaryDocument=["sample.htm"],
                )
            ),
        )
    ).encode()
    index = (
        f"<html><body>Series S000000001 {index_name} Class/Contract {index_class} "
        f"ETF Shares {symbol} "
        f'<a href="{base}sample.htm">497K</a></body></html>'
    ).encode()
    document = (
        document
        or b"""<html><body>FORM 497K Synthetic Equity Fund IWM | NYSE Arca
Summary Prospectus Investment Objective The Synthetic Equity Fund seeks to track
an index composed of small-capitalization U.S. equities. Fees and Expenses
Purchase and Sale of Fund Shares The Fund is an exchange-traded fund.
Tax Information</body></html>"""
    )
    receipts = json.dumps(
        [
            dict(
                url=url,
                status=200,
                received_at=stamp,
                sha256=sha256(raw).hexdigest(),
                file=f"response-{i + 1}.raw",
            )
            for i, (url, raw) in enumerate(
                zip(
                    [
                        "https://data.sec.gov/submissions/CIK0000123456.json",
                        base + accession + "-index.html",
                        base + "sample.htm",
                    ],
                    [submissions, index, document],
                    strict=True,
                )
            )
        ]
    ).encode()
    mapping_receipt = json.dumps(
        dict(sha256=sha256(mapping).hexdigest(), stored_at=stamp, original_received_at=None)
    ).encode()
    return (fmp, receipt, mapping, submissions, index, document, receipts, mapping_receipt)


def archives(root, **changes):
    values = originals(**changes)
    fm, sec = root / "fmp", root / "sec"
    fm.mkdir(mode=0o700, parents=True)
    sec.mkdir(mode=0o700)
    mapping = sec / "mapping.raw"
    names = (
        fm / "profile.json",
        fm / "receipt.json",
        mapping,
        sec / "submissions.raw",
        sec / "filing-index.raw",
        sec / "disclosure.raw",
        sec / "receipts.json",
        sec / "mapping-binding.json",
    )
    for path, content in zip(names, values, strict=True):
        path.write_bytes(content)
        path.chmod(0o600)
    return fm, sec, mapping


def share_class_document():
    return b"""<html><body>497K 1 example.htm Summary Prospectus Synthetic Total Stock ETF
Exchange-traded fund shares that are not individually redeemable and are listed on NYSE Arca
Synthetic Total Stock Index Fund ETF Shares (VTI)
Investment Objective Synthetic Total Stock ETF, an exchange-traded share class of
Synthetic Total Stock Index Fund (the Fund), seeks to track the performance of a
benchmark index that measures the investment return of the overall stock market.
Fees and Expenses Costs. Principal Investment Strategies
The Fund tracks an index which represents 100% of the investable U.S. stock market.
Under normal circumstances, the Fund invests at least 80% of its net assets,
plus the amount of any borrowings for investment purposes,
in the stocks that make up the Target Index.
Principal Risks The investment may lose value.</body></html>"""
