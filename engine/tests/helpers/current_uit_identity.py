"""Handwritten UIT source fixture, never real account evidence."""

import json
from hashlib import sha256


def originals(change=None):
    stamp = "2026-02-02T00:00:00+00:00"
    base = "https://www.sec.gov/Archives/edgar/data/123456/000012345626000001/"
    fmp = json.dumps(
        [
            dict(
                symbol="SPY",
                companyName="Synthetic Provider ETF",
                cik="123456",
                currency="USD",
                exchange="AMEX",
                isEtf=True,
            )
        ]
    ).encode()
    f = dict(
        schema="fmp_profile_receipt_v1",
        source="fmp",
        origin="https://financialmodelingprep.com",
        path="/stable/profile",
        symbol="SPY",
        raw_sha256=sha256(fmp).hexdigest(),
        raw_byte_length=len(fmp),
        received_at=stamp,
    )
    sub = dict(
        cik="123456",
        name="SYNTHETIC OLD TRUST",
        tickers=["SPY"],
        exchanges=["NYSE"],
        filings=dict(
            recent=dict(
                accessionNumber=["0000123456-26-000001"],
                acceptanceDateTime=["2026-01-26T12:00:00Z"],
                filingDate=["2026-01-26"],
                form=["497"],
                primaryDocument=["sample.htm"],
            )
        ),
    )
    index = (
        "<html><body>Form 497 SEC Accession No. 0000123456-26-000001 CIK 0000123456 "
        f'<a href="{base}sample.htm">sample.htm</a></body></html>'
    )
    body = """<html><body>Filed Pursuant to Rule 497(b) Registration File No. 033-12345
Synthetic New Trust (“SPY” or the “Trust”) (formerly, “Synthetic Old Trust”)
(A Unit Investment Trust) Principal U.S. Listing Exchange for Synthetic New Trust:
NYSE Arca, Inc. under the symbol “SPY” Prospectus Dated January 26, 2026
SUMMARY Investment Objective The Trust seeks to provide investment results that, before expenses,
correspond generally to the price and yield performance of the S&amp;P 500 Index (the “Index”).
Fees and Expenses of the Trust Details.
The Trust’s Investments and Portfolio Turnover The Trust seeks to achieve its investment objective
by holding a portfolio of the common stocks that are included in the Index (the “Portfolio”),
with the weight of each stock in the Portfolio substantially corresponding
to the weight of such stock
in the Index.
</body></html>"""
    if change == "ticker":
        sub["tickers"] = ["SPY.A"]
    if change == "cik":
        sub["cik"] = "654321"
    if change == "form":
        sub["filings"]["recent"]["form"] = ["497K"]
    if change == "name":
        sub["name"] = "OTHER TRUST"
    if change == "listing":
        body = body.replace("NYSE Arca", "NASDAQ")
    if change == "bond":
        body = body.replace("common stocks", "bonds")
    if change == "future":
        body = body.replace("January 26, 2026", "January 26, 2099")
    if change == "duplicate":
        body += body
    if change == "wrong_link":
        index = index.replace("sample.htm", "other.htm")
    raws = [json.dumps(sub).encode(), index.encode(), body.encode()]
    urls = [
        "https://data.sec.gov/submissions/CIK0000123456.json",
        base + "0000123456-26-000001-index.html",
        base + "sample.htm",
    ]
    values = [fmp, json.dumps(f).encode()]
    for raw, url in zip(raws, urls, strict=True):
        values.extend(
            [
                raw,
                json.dumps(
                    dict(
                        source_url=url,
                        received_at=stamp,
                        sha256=sha256(raw).hexdigest(),
                        byte_length=len(raw),
                        http_status=200,
                    )
                ).encode(),
            ]
        )
    return tuple(values)


def dia_originals():
    values = list(originals())
    values = [raw.replace(b"SPY", b"DIA") for raw in values]
    values[2] = values[2].replace(
        b"SYNTHETIC OLD TRUST", b"SYNTHETIC DOW JONES INDUSTRIAL AVERAGE ETF TRUST"
    )
    values[6] = values[6].replace(
        b"Synthetic Old Trust", b"Synthetic Dow Jones Industrial Average SM ETF TRUST"
    )
    values[6] = values[6].replace(
        "S&amp;P 500 Index (the “Index”)".encode(),
        "Dow Jones Industrial Average (the “DJIA” or “Index”)".encode(),
    )
    values[6] = (
        values[6]
        .replace(b"included in the Index", b"included in the DJIA")
        .replace(b"in the Index.", b"in the DJIA.")
    )
    for index in (0, 2, 4, 6):
        receipt = json.loads(values[index + 1])
        receipt["raw_sha256" if index == 0 else "sha256"] = sha256(values[index]).hexdigest()
        receipt["raw_byte_length" if index == 0 else "byte_length"] = len(values[index])
        values[index + 1] = json.dumps(receipt).encode()
    return tuple(values)
