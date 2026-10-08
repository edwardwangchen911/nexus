"""Read receipt-bound SEC identity sources without collecting or registering authority."""

import json
import os
import stat
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from zoneinfo import ZoneInfo

from nexus.domain.time import require_utc
from nexus.providers.fmp_profile import validate_symbol
from nexus.research.fmp_profile_archive import _read
from nexus.research.identity_sources import (
    directory_binding,
    parse_identity_source,
    require_submissions_binding,
)

POLICY_ID = "research_current_identity_session_v1"
POLICY = {
    "id": POLICY_ID,
    "timezone": "America/New_York",
    "receipt_window": "decision_natural_day_start_through_decision_time",
    "purpose": "research_issuer_security_binding_only",
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
    decision_session: date
    available_at: datetime
    originals: tuple[IdentitySourceOriginal, ...]
    policy_id: str = POLICY_ID
    policy_hash: str = POLICY_HASH


def validate_identity_source_originals(
    *,
    contents: tuple[bytes, bytes, bytes, bytes],
    symbol: str,
    cik: str,
    exchange: str,
    decision_time: datetime,
    decision_session: date | None = None,
) -> ValidatedIdentitySources:
    try:
        validate_symbol(symbol)
        cutoff = require_utc(decision_time)
        session_day = cutoff.astimezone(ZoneInfo("America/New_York")).date()
        if (
            cutoff > datetime.now(UTC)
            or decision_session not in (None, session_day)
            or len(cik) != 10
            or not cik.isascii()
            or not cik.isdigit()
        ):
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
                or received.astimezone(ZoneInfo("America/New_York")).date() != session_day
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
            session_day,
            max(item.received_at for item in originals),
            tuple(originals),
        )
    except (ValueError, TypeError, KeyError, RecursionError, AttributeError) as error:
        raise ValueError("RESEARCH_IDENTITY_SOURCE_INVALID") from error


def load_identity_source_archive(
    directory: Path,
    *,
    symbol: str,
    cik: str,
    exchange: str,
    decision_time: datetime,
    decision_session: date | None = None,
) -> ValidatedIdentitySources:
    try:
        validate_symbol(symbol)
        fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            info = os.fstat(fd)
            if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
                raise ValueError
            names = set(os.listdir(fd))
            symbols = {
                name.removesuffix("-submissions.json")
                for name in names
                if name.endswith("-submissions.json")
            }
            if not 1 <= len(symbols) <= 5 or symbol not in symbols:
                raise ValueError
            for item in symbols:
                validate_symbol(item)
            expected = {"directory.json", "directory-receipt.json", "result.json"}
            for item in symbols:
                expected.update({f"{item}-submissions.json", f"{item}-submissions-receipt.json"})
            if names != expected:
                raise ValueError
            result = parse_identity_source(_read(fd, "result.json", 8192))
            if (
                type(result.get("requests")) is not int
                or result
                != {
                    "status": "archived",
                    "requests": 1 + len(symbols),
                    "authority_registered": False,
                    "execution_locked": True,
                }
                or result["authority_registered"] is not False
                or result["execution_locked"] is not True
            ):
                raise ValueError
            contents = (
                _read(fd, "directory.json", 8 * 1024 * 1024),
                _read(fd, "directory-receipt.json", 8192),
                _read(fd, f"{symbol}-submissions.json", 8 * 1024 * 1024),
                _read(fd, f"{symbol}-submissions-receipt.json", 8192),
            )
        finally:
            os.close(fd)
        return validate_identity_source_originals(
            contents=contents,
            symbol=symbol,
            cik=cik,
            exchange=exchange,
            decision_time=decision_time,
            decision_session=decision_session,
        )
    except (OSError, ValueError, TypeError, KeyError, RecursionError) as error:
        raise ValueError("RESEARCH_IDENTITY_ARCHIVE_INVALID") from error
