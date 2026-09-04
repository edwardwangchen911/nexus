"""Validate the two fixed private originals; no source discovery or network access."""

import json
import os
import stat
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from uuid import UUID

from nexus.domain.time import require_utc
from nexus.providers.fmp_profile import (
    MAX_RAW_BYTES,
    ORIGIN,
    PATH,
    _invalid_constant,
    _unique,
    parse_profile,
)
from nexus.research.fmp_profile import RAW_SCHEMA, RECEIPT_SCHEMA, FmpCompanyProfile


@dataclass(frozen=True)
class ProfileOriginal:
    content: bytes
    schema_version: str
    received_at: datetime


@dataclass(frozen=True)
class ValidatedProfileArchive:
    document: FmpCompanyProfile
    originals: tuple[ProfileOriginal, ...]


def _read(directory_fd: int, name: str, limit: int) -> bytes:
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd)
    with os.fdopen(fd, "rb") as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or not 0 < info.st_size <= limit:
            raise ValueError
        content = handle.read(limit + 1)
        if len(content) != info.st_size:
            raise ValueError
        return content


def load_profile_archive(
    directory: Path, *, instrument_id: UUID, symbol: str, now: datetime | None = None
) -> ValidatedProfileArchive:
    try:
        cutoff = require_utc(now or datetime.now(UTC))
        fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            if os.fstat(fd).st_mode & 0o077 or set(os.listdir(fd)) != {
                "profile.json",
                "receipt.json",
            }:
                raise ValueError
            raw = _read(fd, "profile.json", MAX_RAW_BYTES)
            receipt_raw = _read(fd, "receipt.json", 8192)
        finally:
            os.close(fd)
        receipt = json.loads(
            receipt_raw,
            object_pairs_hook=_unique,
            parse_float=_invalid_constant,
            parse_constant=_invalid_constant,
        )
        if not isinstance(receipt, dict) or set(receipt) != {
            "schema",
            "source",
            "origin",
            "path",
            "symbol",
            "raw_sha256",
            "raw_byte_length",
            "received_at",
        }:
            raise ValueError
        received = require_utc(datetime.fromisoformat(receipt["received_at"]))
        if (
            received > cutoff
            or receipt
            != {
                "schema": RECEIPT_SCHEMA,
                "source": "fmp",
                "origin": ORIGIN,
                "path": PATH,
                "symbol": symbol,
                "raw_sha256": sha256(raw).hexdigest(),
                "raw_byte_length": len(raw),
                "received_at": receipt["received_at"],
            }
            or type(receipt["raw_byte_length"]) is not int
        ):
            raise ValueError
        row = parse_profile(raw, symbol)
        document = FmpCompanyProfile(
            instrument_id=instrument_id,
            symbol=symbol,
            company_name=row.get("companyName"),
            description=row.get("description"),
            cik=row.get("cik"),
            isin=row.get("isin"),
            cusip=row.get("cusip"),
            industry=row.get("industry"),
            sector=row.get("sector"),
            source_url=f"{ORIGIN}{PATH}?symbol={symbol}",
            received_at=received,
            available_at=received,
            raw_content_hash=sha256(raw).hexdigest(),
            raw_byte_length=len(raw),
            receipt_content_hash=sha256(receipt_raw).hexdigest(),
            receipt_byte_length=len(receipt_raw),
        )
        return ValidatedProfileArchive(
            document,
            (
                ProfileOriginal(raw, RAW_SCHEMA, received),
                ProfileOriginal(receipt_raw, RECEIPT_SCHEMA, received),
            ),
        )
    except (OSError, ValueError, TypeError, KeyError, RecursionError) as error:
        raise ValueError("FMP_PROFILE_ARCHIVE_INVALID") from error
