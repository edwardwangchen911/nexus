"""Handwritten synthetic fixtures; no captured source or account data."""

import json
from datetime import timedelta
from hashlib import sha256

from helpers.cboe_source_chain import originals
from helpers.held_fund_submission import BODY, submission
from helpers.iso_mic import NOW

OBJECTIVE = (
    "The Fund seeks daily investment results, before fees and expenses, of two times (200%) "
    "the daily percentage change in the share price of Example Issuer (Nasdaq: BASE)."
)

STRATEGY = (
    "The Fund is an actively managed exchange traded fund that attempts to achieve two times "
    "(200%) the daily percentage change in the share price of the Underlying Security by "
    "employing derivatives, namely swap agreements and/or listed options contracts."
)


def source_chain_contents(objective=OBJECTIVE, strategy=STRATEGY):
    values = list(originals())
    body = BODY.replace(
        b"<p>Investment Objective</p><p>Exposure to shares of BASE.</p>",
        (
            "<p>Investment Objective</p><p>" + objective + "</p><p>Fees and Expenses</p>"
            "<p>Principal Investment Strategies</p><p>" + strategy + "</p>"
            "<p>Principal Investment Risks</p><p>Leverage risk.</p>"
        ).encode(),
    )
    values[2] = submission(body=body).replace(
        b"Synthetic Daily 2X ETF", b"Synthetic Daily 2X Long ETF"
    )
    values[12] = values[12].replace(b"Synthetic Daily 2x ETF", b"Synthetic Daily 2x Long ETF")
    for index in (1, 6):
        receipt = json.loads(values[index * 2 + 1])
        receipt["sha256"] = sha256(values[index * 2]).hexdigest()
        receipt["byte_length"] = len(values[index * 2])
        values[index * 2 + 1] = json.dumps(receipt).encode()
    return tuple(values)


def directories(company=None, fund=None):
    rows = (
        {
            "fields": ["cik", "name", "ticker", "exchange"],
            "data": company
            if company is not None
            else [[987654, "Other Issuer", "OTHER", "Nasdaq"]],
        },
        {
            "fields": ["cik", "seriesId", "classId", "symbol"],
            "data": fund if fund is not None else [[987654, "S000987654", "C000987654", "OTHER"]],
        },
    )
    result = []
    for doc, filename in zip(
        rows, ("company_tickers_exchange.json", "company_tickers_mf.json"), strict=True
    ):
        raw = json.dumps(doc).encode()
        receipt = {
            "source_url": "https://www.sec.gov/files/" + filename,
            "received_at": (NOW - timedelta(minutes=2)).isoformat(),
            "sha256": sha256(raw).hexdigest(),
            "byte_length": len(raw),
            "http_status": 200,
        }
        result.extend((raw, json.dumps(receipt).encode()))
    return tuple(result)
