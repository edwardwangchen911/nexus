"""One private FMP estimates observation; never historical or execution authority."""

import argparse
import asyncio
import json
import os
import stat
from datetime import UTC, date, datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from typing import Any

import httpx
from pydantic import SecretStr

from nexus.domain.time import require_utc
from nexus.ops.fosun_local import load_local_environment
from nexus.ops.local_recovery import write_private
from nexus.ops.research_budget_diagnostic import _private_directory
from nexus.providers.fmp_profile import MAX_RAW_BYTES, _invalid_constant, _unique, validate_symbol
from nexus.providers.http import (
    AllowedOrigin,
    FixedOriginHttpClient,
    HttpPolicyError,
    HttpRequestPolicy,
)

PATH = "/stable/analyst-estimates"
CODES = {
    "UNKNOWN",
    "FORECAST_SECRET_REFLECTION",
    "FORECAST_PAYLOAD_INVALID",
    "FORECAST_CURRENT_OBSERVED",
    "FORECAST_HTTP_REJECTED",
}


def parse_estimates(raw: bytes, symbol: str) -> list[dict[str, str]]:
    """Period-end is a forecast target, never a publication/vintage timestamp."""
    try:
        validate_symbol(symbol)
        if not 0 < len(raw) <= MAX_RAW_BYTES:
            raise ValueError
        rows = json.loads(
            raw, object_pairs_hook=_unique, parse_float=Decimal, parse_constant=_invalid_constant
        )
        if not isinstance(rows, list) or not 1 <= len(rows) <= 10:
            raise ValueError
        result = []
        periods = set()
        for row in rows:
            if not isinstance(row, dict) or row.get("symbol") != symbol:
                raise ValueError
            period = row["date"]
            if date.fromisoformat(period).isoformat() != period or period in periods:
                raise ValueError
            periods.add(period)
            item = dict(symbol=symbol, period_end=period)
            for field in ("epsAvg", "revenueAvg"):
                value = row[field]
                if isinstance(value, bool) or not isinstance(value, int | Decimal):
                    raise ValueError
                item[field] = str(value)
            result.append(item)
        return result
    except (KeyError, ValueError, TypeError, OverflowError, RecursionError):
        raise ValueError("FORECAST_PAYLOAD_INVALID") from None


async def collect(
    directory: Path, *, symbol: str, key: SecretStr, client: httpx.AsyncClient | None = None
) -> dict[str, Any]:
    validate_symbol(symbol)
    _private_directory(directory.parent)
    secret = key.get_secret_value()
    if not secret or not secret.isascii() or any(not 33 <= ord(c) <= 126 for c in secret):
        raise ValueError("FORECAST_CREDENTIAL_MISSING")
    directory.mkdir(mode=0o700)  # Persistent claim; never reuse even after an unknown outcome.
    receipt: dict[str, Any] = dict(
        version=1,
        symbol=symbol,
        path=PATH,
        period="annual",
        started_at=datetime.now(UTC).isoformat(),
        access_status="UNVERIFIED",
        code="UNKNOWN",
        http_status=None,
        received_at=None,
        raw_sha256=None,
        raw_byte_length=None,
        execution_permitted=False,
    )
    owned = client is None
    transport = client or httpx.AsyncClient(follow_redirects=False, timeout=30)
    stage = "request"
    try:
        fixed = FixedOriginHttpClient(
            provider_name="fmp_estimates",
            origin=AllowedOrigin(scheme="https", host="financialmodelingprep.com"),
            policy=HttpRequestPolicy(max_attempts=1, max_response_bytes=MAX_RAW_BYTES),
            client=transport,
        )
        raw = await fixed.get_bytes(
            ("stable", "analyst-estimates"),
            (("symbol", symbol), ("period", "annual"), ("page", "0"), ("limit", "10")),
            (("apikey", secret),),
        )
        receipt["received_at"] = datetime.now(UTC).isoformat()
        stage = "archive"
        if secret.encode() in raw:
            receipt["code"] = "FORECAST_SECRET_REFLECTION"
            return receipt
        write_private(directory / "raw.json", raw)
        receipt.update(raw_sha256=sha256(raw).hexdigest(), raw_byte_length=len(raw))
        stage = "parse"
        parse_estimates(raw, symbol)
        receipt.update(access_status="AVAILABLE", code="FORECAST_CURRENT_OBSERVED")
    except HttpPolicyError as error:
        receipt.update(code="FORECAST_HTTP_REJECTED", http_status=error.http_status)
    except ValueError as error:
        receipt["code"] = (
            "FORECAST_PAYLOAD_INVALID" if error.args == ("FORECAST_PAYLOAD_INVALID",) else "UNKNOWN"
        )
    finally:
        receipt["stage"] = stage
        receipt["finished_at"] = datetime.now(UTC).isoformat()
        write_private(directory / "receipt.json", json.dumps(receipt, sort_keys=True).encode())
        if owned:
            await transport.aclose()
    return receipt


def _read(path: Path, limit: int) -> bytes:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as handle:
        info = os.fstat(handle.fileno())
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_mode & 0o077
            or not 0 < info.st_size <= limit
        ):
            raise ValueError
        raw = handle.read(limit + 1)
        if len(raw) != info.st_size:
            raise ValueError
        return raw


def read_archive(directory: Path, *, as_of: datetime) -> dict[str, Any]:
    try:
        cutoff = require_utc(as_of)
        _private_directory(directory.parent)
        _private_directory(directory)
        receipt = json.loads(_read(directory / "receipt.json", 8192), object_pairs_hook=_unique)
        if (
            set(receipt)
            != {
                "version",
                "symbol",
                "path",
                "period",
                "started_at",
                "finished_at",
                "access_status",
                "code",
                "http_status",
                "received_at",
                "raw_sha256",
                "raw_byte_length",
                "execution_permitted",
                "stage",
            }
            or type(receipt["version"]) is not int
            or receipt["version"] != 1
            or receipt["path"] != PATH
            or receipt["period"] != "annual"
            or receipt["execution_permitted"] is not False
            or receipt["code"] not in CODES
            or receipt["stage"] not in {"request", "archive", "parse"}
            or receipt["access_status"] not in {"AVAILABLE", "UNVERIFIED"}
            or (receipt["access_status"] == "AVAILABLE")
            != (receipt["code"] == "FORECAST_CURRENT_OBSERVED")
            or receipt["http_status"] is not None
            and (
                type(receipt["http_status"]) is not int or not 100 <= receipt["http_status"] <= 599
            )
        ):
            raise ValueError
        validate_symbol(receipt["symbol"])
        started = require_utc(datetime.fromisoformat(receipt["started_at"]))
        finished = require_utc(datetime.fromisoformat(receipt["finished_at"]))
        if started > finished or finished > datetime.now(UTC):
            raise ValueError
        if receipt["received_at"] is not None:
            received = require_utc(datetime.fromisoformat(receipt["received_at"]))
            if not started <= received <= finished:
                raise ValueError
        expected_files = {"receipt.json"}
        if receipt["raw_sha256"] is not None:
            expected_files.add("raw.json")
            raw = _read(directory / "raw.json", MAX_RAW_BYTES)
            if (
                sha256(raw).hexdigest() != receipt["raw_sha256"]
                or len(raw) != receipt["raw_byte_length"]
                or type(receipt["raw_byte_length"]) is not int
            ):
                raise ValueError
        if set(os.listdir(directory)) != expected_files:
            raise ValueError
        if receipt["access_status"] == "AVAILABLE":
            parse_estimates(raw, receipt["symbol"])
            received = require_utc(datetime.fromisoformat(receipt["received_at"]))
            if not started <= received <= finished:
                raise ValueError
            historical = (
                "FORECAST_RECEIVED_AFTER_CUTOFF"
                if received > cutoff
                else "FORECAST_VINTAGE_UNVERIFIED"
            )
        else:
            historical = "FORECAST_ACCESS_UNVERIFIED"
        # Only allowlisted summaries leave the private archive.
        return dict(
            access_status=receipt["access_status"],
            code=receipt["code"],
            stage=receipt["stage"],
            http_status=receipt["http_status"],
            received_at=receipt["received_at"],
            historical_binding_status=historical,
            execution_permitted=False,
        )
    except (OSError, ValueError, KeyError, TypeError, UnboundLocalError):
        raise ValueError("FORECAST_ARCHIVE_INVALID") from None


def current_context(directory: Path, *, as_of: datetime) -> dict[str, Any]:
    """New diagnostic input only: cannot enter old frozen inputs or trading authority."""
    summary = read_archive(directory, as_of=as_of)
    if summary["access_status"] != "AVAILABLE":
        raise ValueError("FORECAST_ACCESS_UNVERIFIED")
    if summary["historical_binding_status"] == "FORECAST_RECEIVED_AFTER_CUTOFF":
        raise ValueError("FORECAST_RECEIVED_AFTER_CUTOFF")
    receipt = json.loads(_read(directory / "receipt.json", 8192))
    raw = _read(directory / "raw.json", MAX_RAW_BYTES)
    if sha256(raw).hexdigest() != receipt["raw_sha256"]:
        raise ValueError("FORECAST_ARCHIVE_INVALID")
    return dict(
        schema="forecast_diagnostic_context_v1",
        status="partial",
        **summary,
        as_of=as_of.isoformat(),
        raw_sha256=sha256(raw).hexdigest(),
        items=parse_estimates(raw, receipt["symbol"]),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    collect_parser = sub.add_parser("probe")
    collect_parser.add_argument("--repo-root", type=Path, required=True)
    collect_parser.add_argument("--symbol", required=True)
    read_parser = sub.add_parser("read")
    read_parser.add_argument("--directory", type=Path, required=True)
    for command in (collect_parser, read_parser):
        command.add_argument("--as-of", type=datetime.fromisoformat, required=True)
    args = parser.parse_args()
    try:
        require_utc(args.as_of)
        if args.action == "probe":
            root = args.repo_root.resolve()
            _private_directory(root / ".secrets")
            parent = root / ".secrets/nexus-forecast-probe"
            if not parent.exists():
                parent.mkdir(mode=0o700)
            _private_directory(parent)
            validate_symbol(args.symbol)
            directory = parent / f"{datetime.now(UTC):%Y%m%d}-{args.symbol}-v1"
            environment = load_local_environment(root, os.environ)
            asyncio.run(
                collect(
                    directory, symbol=args.symbol, key=SecretStr(environment.get("FMP_API_KEY", ""))
                )
            )
        else:
            directory = args.directory.absolute()
        result = read_archive(directory, as_of=args.as_of)
    except FileExistsError:
        result = dict(code="FORECAST_ALREADY_ATTEMPTED", access_status="UNVERIFIED")
    except Exception:
        result = dict(code="FORECAST_PROBE_BLOCKED", access_status="UNVERIFIED")
    print(json.dumps(result))
    return 0 if result["access_status"] == "AVAILABLE" else 1


if __name__ == "__main__":
    raise SystemExit(main())
