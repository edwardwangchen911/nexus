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
    index_symbol=None,
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
        f"ETF Shares {index_symbol or symbol} "
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


def archives(root, *, original_values=None, **changes):
    values = original_values or originals(**changes)
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


def nasdaq_originals(*, document=None, index_ticker="", class_name="QQQ Synthetic Trust, Series 1"):
    name = "Invesco QQQ Synthetic Trust, Series 1"
    document = (
        document
        or b"""<html><body>497K 1 sample.htm 497K
Summary Prospectus January 3, 2026 QQQ Invesco QQQ Synthetic Trust SM , Series 1
The Nasdaq Stock Market LLC Before you invest, read the prospectus.
Investment Objective The Invesco QQQ Synthetic Trust SM , Series 1 (the Fund or Trust)
seeks to track the investment results, before fees and expenses, of the Synthetic Index
(the Underlying Index).
Fund Fees and Expenses Expenses apply.
Principal Investment Strategies The Fund will invest substantially all of its assets
in the securities that comprise the Underlying Index.
Eligible securities are common stocks and tracking stocks as well as American Depositary Receipts.
Principal Risks of Investing in the Fund Risks apply.
Performance Effective after market close on December 19, 2025, the Fund was reclassified
as an open-end management investment company (the Reclassification).
Prior to the Reclassification, the Fund operated as a unit investment trust (UIT).
Management of the Fund Management information.
Purchase and Sale of Shares The Fund issues and redeems Shares at NAV only with APs
and only in large blocks. Individual Shares may only be bought and sold in the secondary
market (i.e., on a national securities exchange) through a broker or dealer at a market price.
Tax Information Taxes apply.</body></html>"""
    )
    values = list(originals(symbol="QQQ", exchange="NASDAQ", company_name=name, document=document))
    submissions = json.loads(values[3])
    submissions["name"] = name.upper()
    values[3] = json.dumps(submissions).encode()
    values[4] = f"""<html><body><div id="seriesDiv"><table>
<tr><td>Series S000000001</td><td></td><td>{class_name}</td></tr>
<tr><td>Class/Contract C000000002</td><td></td><td>{class_name}</td><td>{index_ticker}</td></tr>
</table></div><a href="https://www.sec.gov/Archives/edgar/data/123456/000012345626000001/sample.htm">497K</a>
</body></html>""".encode()
    receipts = json.loads(values[6])
    for receipt, raw in zip(receipts, values[3:6], strict=True):
        receipt["sha256"] = sha256(raw).hexdigest()
    values[6] = json.dumps(receipts).encode()
    return tuple(values)


def treasury_originals(change=None):
    name = "Synthetic Treasury Fund"
    document = """<html><body>FORM 497K Synthetic Treasury Fund TLT | Nasdaq
Summary Prospectus — January 2, 2026 Before you invest.
Investment Objective The Synthetic Treasury Fund (the “Fund”) seeks to track the investment results
of an index composed of U.S. Treasury bonds with remaining maturities greater than twenty years.
Fees and Expenses Details.
Principal Investment Strategies The Fund seeks to track the investment results of the ICE
U.S. Treasury 20+ Year Bond Index (the “Underlying Index”), which measures the performance
of public obligations of the U.S. Treasury that have a remaining maturity greater than twenty years.
The Fund will invest at least 80% of its assets in the component securities of the Underlying Index,
and the Fund will invest at least 90% of its assets in U.S. Treasury securities that BFA believes
will help the Fund track the Underlying Index.
Summary of Principal Risks Details.
Purchase and Sale of Fund Shares The Fund is an exchange-traded fund
(commonly referred to as an ETF).
Individual shares of the Fund may only be bought and sold in the secondary market
through a broker-dealer.
Tax Information Details.</body></html>""".encode()
    values = list(originals(symbol="TLT", exchange="NASDAQ", company_name=name, document=document))
    values[4] = f"""<html><body><div id="seriesDiv"><table>
<tr><td>Series S000000001</td><td></td><td>{name}</td></tr>
<tr><td>Class/Contract C000000002</td><td></td><td>{name}</td><td>TLT</td></tr>
</table></div><a href="https://www.sec.gov/Archives/edgar/data/123456/000012345626000001/sample.htm">497K</a></body></html>""".encode()
    if change == "alias_spacing":
        values[5] = (
            values[5]
            .replace("“Fund”)".encode(), "“ Fund ” )".encode())
            .replace("“Underlying Index”)".encode(), "“ Underlying Index ” )".encode())
        )
    if change == "class":
        values[4] = values[4].replace(b"C000000002", b"C000000099")
    if change == "ticker":
        values[4] = values[4].replace(b">TLT<", b">TLT.A<")
    if change == "stock":
        values[5] = values[5].replace(b"Treasury bonds", b"common stocks")
    if change == "corporate":
        values[5] = values[5].replace(b"U.S. Treasury that", b"corporate issuers that")
    if change == "threshold":
        values[5] = values[5].replace(b"90%", b"60%")
    receipts = json.loads(values[6])
    for receipt, raw in zip(receipts, values[3:6], strict=True):
        receipt["sha256"] = sha256(raw).hexdigest()
    values[6] = json.dumps(receipts).encode()
    return tuple(values)
