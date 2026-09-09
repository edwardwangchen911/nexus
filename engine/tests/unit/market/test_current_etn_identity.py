"""Synthetic current-held ETN originals; never real account or issuer data."""

import json
from datetime import UTC, datetime
from hashlib import sha256

import pytest


def _originals(*, symbol: str = "SYNZ", exchange: str = "Nasdaq") -> tuple[bytes, ...]:
    submissions = json.dumps(
        {
            "cik": "0000123456",
            "name": "Synthetic Bank",
            "tickers": ["BANK"],
            "exchanges": [exchange],
            "filings": {
                "recent": {
                    "accessionNumber": ["0000123456-26-000001"],
                    "acceptanceDateTime": ["2026-01-02T12:00:00.000Z"],
                    "form": ["424B2"],
                    "primaryDocument": ["synthetic.htm"],
                }
            },
        }
    ).encode()
    index = b"""<html><body>SEC Accession No. 0000123456-26-000001
CIK: 0000123456 Form 424B2
<a href="synthetic.htm">synthetic.htm</a></body></html>"""
    document = f"""<html><body>Filed Pursuant to Rule 424(b)(2)
Synthetic Bank CIK: 0000123456 Synthetic Market Exposure Exchange Traded Notes due January 2, 2036
Ticker Symbol: {symbol} Listing Exchange: The Nasdaq Stock Market LLC
The notes are senior unsecured debt securities of Synthetic Bank.
The notes are not shares of an exchange-traded fund and do not represent equity ownership.
</body></html>""".encode()
    urls = (
        "https://data.sec.gov/submissions/CIK0000123456.json",
        "https://www.sec.gov/Archives/edgar/data/123456/000012345626000001/"
        "0000123456-26-000001-index.html",
        "https://www.sec.gov/Archives/edgar/data/123456/000012345626000001/synthetic.htm",
    )
    raws = (submissions, index, document)
    receipts = json.dumps(
        [
            {
                "source_url": url,
                "received_at": "2026-01-02T12:01:00+00:00",
                "sha256": sha256(raw).hexdigest(),
                "byte_length": len(raw),
                "http_status": 200,
            }
            for url, raw in zip(urls, raws, strict=True)
        ]
    ).encode()
    return (*raws, receipts)


def test_current_held_etn_accepts_only_explicit_sec_debt_and_listing_identity():
    from nexus.market.current_etn_identity import validate_current_held_etn_originals

    result = validate_current_held_etn_originals(
        symbol="SYNZ", contents=_originals(), now=datetime(2026, 1, 3, tzinfo=UTC)
    )

    assert result.identity.asset_type == "exchange_traded_note"
    assert result.identity.risk_bucket == "EXCHANGE_TRADED_NOTE"
    assert result.identity.exchange == "XNAS"
    assert result.identity.symbol == "SYNZ"


@pytest.mark.parametrize(
    "change",
    ["fixed_member", "wrong_exchange", "not_debt", "wrong_symbol", "wrong_form"],
)
def test_current_held_etn_rejects_unscoped_or_ambiguous_identity(change, monkeypatch):
    from nexus.market import current_etn_identity as identity

    values = list(_originals())
    if change == "fixed_member":
        monkeypatch.setattr(
            identity,
            "require_outside_fixed_etf_definition",
            lambda _symbol: (_ for _ in ()).throw(ValueError("definition member")),
        )
    elif change == "wrong_exchange":
        values[2] = values[2].replace(
            b"The Nasdaq Stock Market LLC", b"The Nasdaq Stock Market LLC and NYSE Arca"
        )
    elif change == "not_debt":
        values[2] = values[2].replace(
            b"senior unsecured debt securities", b"interests in an investment fund"
        )
    elif change == "wrong_symbol":
        values[2] = values[2].replace(b"SYNZ", b"OTHER")
    else:
        submissions = json.loads(values[0])
        submissions["filings"]["recent"]["form"] = ["10-K"]
        values[0] = json.dumps(submissions).encode()
    receipts = json.loads(values[3])
    for receipt, raw in zip(receipts, values[:3], strict=True):
        receipt["sha256"] = sha256(raw).hexdigest()
        receipt["byte_length"] = len(raw)
    values[3] = json.dumps(receipts).encode()

    with pytest.raises(ValueError, match="CURRENT_ETN_IDENTITY_INVALID"):
        identity.validate_current_held_etn_originals(
            symbol="SYNZ", contents=tuple(values), now=datetime(2026, 1, 3, tzinfo=UTC)
        )
