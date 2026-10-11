"""Bind exact archived provider, SEC, MIC and Cboe sources without identity registration.

Current directory conflict checks, SEC strategy classification and fresh broker
exposure remain mandatory before any held-only identity can be registered.
"""

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from hashlib import sha256
from typing import Any, Literal

from nexus.domain.time import require_utc
from nexus.market.cboe_listing import CboeListedObservation, parse_cboe_listing
from nexus.market.current_identity import cik
from nexus.market.held_fund_submission import (
    _URL,
    SingleClassSubmission,
    parse_single_class_submission,
)
from nexus.market.iso_mic import BzxMicObservation, parse_bzx_mic
from nexus.providers.fmp_profile import _invalid_constant, _unique, parse_profile

SCHEMA = "cboe_fund_source_chain_v1"


@dataclass(frozen=True)
class CboeFundSourceChain:
    submission: SingleClassSubmission
    mic: BzxMicObservation
    listing: CboeListedObservation
    contents: tuple[bytes, ...] = field(repr=False)
    received: tuple[datetime, ...]
    fmp_name: str | None
    fmp_cik: str | None
    identity_registration_ready: Literal[False] = False
    execution_locked: Literal[True] = True

    @property
    def revision(self) -> str:
        return sha256(
            (
                SCHEMA
                + self.submission.symbol
                + ":".join(sha256(raw).hexdigest() for raw in self.contents)
            ).encode()
        ).hexdigest()


def _json(raw: bytes) -> Any:
    return json.loads(
        raw,
        object_pairs_hook=_unique,
        parse_float=_invalid_constant,
        parse_constant=_invalid_constant,
    )


def _receipt(raw: bytes, content: bytes, source_url: str, cutoff: datetime) -> datetime:
    doc = _json(raw)
    if not isinstance(doc, dict):
        raise ValueError
    received = require_utc(datetime.fromisoformat(doc["received_at"]))
    if (
        doc
        != {
            "source_url": source_url,
            "received_at": doc["received_at"],
            "sha256": sha256(content).hexdigest(),
            "byte_length": len(content),
            "http_status": 200,
        }
        or type(doc["byte_length"]) is not int
        or type(doc["http_status"]) is not int
        or received > cutoff
    ):
        raise ValueError
    return received


def validate_cboe_source_chain(
    *, symbol: str, contents: tuple[bytes, ...], now: datetime
) -> CboeFundSourceChain:
    try:
        cutoff = require_utc(now)
        if len(contents) not in {14, 18} or any(
            not 0 < len(raw) <= (8192 if index % 2 else 8 * 1024 * 1024)
            for index, raw in enumerate(contents)
        ):
            raise ValueError
        profile = parse_profile(contents[0], symbol)
        if (
            profile.get("currency") != "USD"
            or profile.get("isEtf") is not True
            or profile.get("exchange") != "CBOE"
            or profile.get("isAdr") is True
        ):
            raise ValueError
        doc = _json(contents[1])
        if not isinstance(doc, dict):
            raise ValueError
        profile_received = require_utc(datetime.fromisoformat(doc["received_at"]))
        if (
            doc
            != {
                "schema": "fmp_profile_receipt_v1",
                "source": "fmp",
                "origin": "https://financialmodelingprep.com",
                "path": "/stable/profile",
                "symbol": symbol,
                "raw_sha256": sha256(contents[0]).hexdigest(),
                "raw_byte_length": len(contents[0]),
                "received_at": doc["received_at"],
            }
            or type(doc["raw_byte_length"]) is not int
        ):
            raise ValueError
        sec_receipt = _json(contents[3])
        sec_url = sec_receipt["source_url"]
        if not isinstance(sec_url, str) or not _URL.fullmatch(sec_url):
            raise ValueError
        received = [profile_received]
        sources = (
            sec_url,
            "https://www.iso20022.org/market-identifier-codes",
            "https://www.iso20022.org/sites/default/files/ISO10383_MIC/ISO10383_MIC.csv",
            "https://www.cboe.com/us/equities/market_statistics/listed_symbols/",
            "https://www.cboe.com/us/equities/market_statistics/listed_symbols/csv",
            f"https://www.cboe.com/us/equities/listings/listed_products/symbols/{symbol}/",
        )
        for index, source in enumerate(sources, start=1):
            received.append(_receipt(contents[index * 2 + 1], contents[index * 2], source, cutoff))
        if any(
            not timedelta(0) <= cutoff - timestamp <= timedelta(hours=24)
            for index, timestamp in enumerate(received)
            if index != 1
        ):
            raise ValueError
        if any(
            max(group) - min(group) > timedelta(minutes=2)
            for group in (received[2:4], received[4:7])
        ):
            raise ValueError
        submission = parse_single_class_submission(
            raw=contents[2], source_url=sec_url, symbol=symbol, now=received[1]
        )
        mic = parse_bzx_mic(
            publication_html=contents[4],
            mic_csv=contents[6],
            now=cutoff,
            previous=(contents[14], contents[16]) if len(contents) == 18 else None,
        )
        if any(timestamp.date() < mic.publication_on for timestamp in received[2:4]):
            raise ValueError
        if len(contents) == 18:
            previous_received = [
                _receipt(contents[15], contents[14], sources[1], cutoff),
                _receipt(contents[17], contents[16], sources[2], cutoff),
            ]
            if any(
                timestamp.date() >= mic.publication_on for timestamp in previous_received
            ) or max(previous_received) - min(previous_received) > timedelta(minutes=2):
                raise ValueError
            # Prior data is historical corroboration, never relabelled current authority.
            parse_bzx_mic(
                publication_html=contents[14], mic_csv=contents[16], now=min(previous_received)
            )
            received.extend(previous_received)
        listing = parse_cboe_listing(
            listed_html=contents[8],
            listed_csv=contents[10],
            product_html=contents[12],
            symbol=symbol,
            now=cutoff,
        )
        if listing.directory_as_of > min(received[4:7]):
            raise ValueError
        provider_cik = None if profile.get("cik") is None else cik(profile["cik"])
        if provider_cik is not None and provider_cik != submission.cik:
            raise ValueError
        return CboeFundSourceChain(
            submission,
            mic,
            listing,
            contents,
            tuple(timestamp for timestamp in received for _ in range(2)),
            profile.get("companyName"),
            provider_cik,
        )
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError):
        raise ValueError("CBOE_SOURCE_CHAIN_INVALID") from None
