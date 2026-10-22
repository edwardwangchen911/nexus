"""Read receipt-bound SEC identity sources without collecting or registering authority."""

import json
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256

from nexus.domain.time import require_utc
from nexus.providers.fmp_profile import validate_symbol
from nexus.research.identity_sources import (
    directory_binding,
    parse_identity_source,
    require_submissions_binding,
)

POLICY_ID = "research_observation_identity_v1"
POLICY = {
    "id": POLICY_ID,
    "receipt_window": "received_and_registered_before_research_cutoff",
    "remote_latest_status": "unverified",
    "purpose": "research_historical_observation_only",
    "fallback": "none",
}
POLICY_HASH = sha256(json.dumps(POLICY, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
RAW_SCHEMA = "sec_research_identity_source_v1"
RECEIPT_SCHEMA = "sec_identity_source_receipt_v1"


@dataclass(frozen=True)
class IdentitySourceOriginal:
    content: bytes
    schema_version: str
    received_at: datetime


@dataclass(frozen=True)
class ValidatedIdentitySources:
    symbol: str
    cik: str
    exchange: str
    available_at: datetime
    originals: tuple[IdentitySourceOriginal, ...]
    source_effective_at: None = None
    remote_latest_status: str = "unverified"
    policy_id: str = POLICY_ID
    policy_hash: str = POLICY_HASH


def validate_observation_source_originals(
    *,
    contents: tuple[bytes, bytes, bytes, bytes],
    symbol: str,
    cik: str,
    exchange: str,
    as_of: datetime,
) -> ValidatedIdentitySources:
    try:
        validate_symbol(symbol)
        cutoff = require_utc(as_of)
        if len(cik) != 10 or not cik.isascii() or not cik.isdigit():
            raise ValueError
        directory, directory_receipt, submissions, submissions_receipt = contents
        urls = (
            "https://www.sec.gov/files/company_tickers_exchange.json",
            f"https://data.sec.gov/submissions/CIK{cik}.json",
        )
        originals: list[IdentitySourceOriginal] = []
        for raw, receipt_raw, url in (
            (directory, directory_receipt, urls[0]),
            (submissions, submissions_receipt, urls[1]),
        ):
            if not 0 < len(raw) <= 8 * 1024 * 1024 or not 0 < len(receipt_raw) <= 8192:
                raise ValueError
            receipt = parse_identity_source(receipt_raw)
            received = require_utc(datetime.fromisoformat(receipt["received_at"]))
            if (
                received > cutoff
                or type(receipt["http_status"]) is not int
                or type(receipt["byte_length"]) is not int
                or receipt
                != {
                    "schema": RECEIPT_SCHEMA,
                    "source_url": url,
                    "http_status": 200,
                    "received_at": receipt["received_at"],
                    "sha256": sha256(raw).hexdigest(),
                    "byte_length": len(raw),
                }
            ):
                raise ValueError
            originals.extend(
                (
                    IdentitySourceOriginal(raw, RAW_SCHEMA, received),
                    IdentitySourceOriginal(receipt_raw, RECEIPT_SCHEMA, received),
                )
            )
        if directory_binding(parse_identity_source(directory), symbol) != (cik, exchange):
            raise ValueError
        require_submissions_binding(parse_identity_source(submissions), symbol, cik, exchange)
        return ValidatedIdentitySources(
            symbol,
            cik,
            exchange,
            max(item.received_at for item in originals),
            tuple(originals),
        )
    except (ValueError, TypeError, KeyError, RecursionError, AttributeError) as error:
        raise ValueError("RESEARCH_OBSERVATION_SOURCE_INVALID") from error
