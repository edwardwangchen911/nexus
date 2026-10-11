"""Handwritten synthetic fixtures; no captured source or account data."""

import json
from datetime import timedelta
from hashlib import sha256

from helpers.cboe_listing import CSV as LIST_CSV
from helpers.cboe_listing import HTML as LIST_HTML
from helpers.cboe_listing import PRODUCT
from helpers.held_fund_submission import URL, submission
from helpers.iso_mic import CSV as MIC_CSV
from helpers.iso_mic import NOW, PUBLICATION


def originals():
    received = (NOW - timedelta(minutes=1)).isoformat()
    profile = json.dumps(
        [
            {
                "symbol": "TEST",
                "currency": "USD",
                "isEtf": True,
                "exchange": "CBOE",
                "companyName": "Synthetic Daily 2X ETF",
                "cik": None,
            }
        ]
    ).encode()
    profile_receipt = json.dumps(
        {
            "schema": "fmp_profile_receipt_v1",
            "source": "fmp",
            "origin": "https://financialmodelingprep.com",
            "path": "/stable/profile",
            "symbol": "TEST",
            "raw_sha256": sha256(profile).hexdigest(),
            "raw_byte_length": len(profile),
            "received_at": received,
        }
    ).encode()
    result = [profile, profile_receipt]
    for raw, url in (
        (submission(), URL),
        (PUBLICATION, "https://www.iso20022.org/market-identifier-codes"),
        (MIC_CSV, "https://www.iso20022.org/sites/default/files/ISO10383_MIC/ISO10383_MIC.csv"),
        (LIST_HTML, "https://www.cboe.com/us/equities/market_statistics/listed_symbols/"),
        (LIST_CSV, "https://www.cboe.com/us/equities/market_statistics/listed_symbols/csv"),
        (PRODUCT, "https://www.cboe.com/us/equities/listings/listed_products/symbols/TEST/"),
    ):
        result.extend(
            (
                raw,
                json.dumps(
                    {
                        "source_url": url,
                        "received_at": received,
                        "sha256": sha256(raw).hexdigest(),
                        "byte_length": len(raw),
                        "http_status": 200,
                    }
                ).encode(),
            )
        )
    return tuple(result)
