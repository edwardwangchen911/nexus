"""Collect one delayed, bounded news page to a new private archive."""

import argparse
import asyncio
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import NoReturn

import httpx
from pydantic import SecretStr

from nexus.ops.fosun_local import load_local_environment
from nexus.ops.local_recovery import write_private
from nexus.providers.http import (
    AllowedOrigin,
    FixedOriginHttpClient,
    HttpPolicyError,
    HttpRequestPolicy,
)
from nexus.providers.news import NewsError, aware, parse_news, validate_symbol


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise NewsError("NEWS_REQUEST_INVALID")


def news_window(
    *, now: datetime, start: datetime | None = None, end: datetime | None = None,
) -> tuple[datetime, datetime]:
    aware(now)
    latest = now - timedelta(minutes=15)
    start = aware(start) if start is not None else now - timedelta(days=7)
    end = aware(end) if end is not None else latest
    if not start < end <= latest or end - start > timedelta(days=7):
        raise NewsError("NEWS_WINDOW_INVALID")
    return start.astimezone(UTC), end.astimezone(UTC)


async def collect_news(
    *, symbol: str, output_dir: Path, key_id: SecretStr, secret: SecretStr,
    start: datetime | None = None, end: datetime | None = None,
    client: httpx.AsyncClient | None = None,
) -> dict[str, object]:
    validate_symbol(symbol)
    start, end = news_window(now=datetime.now(UTC), start=start, end=end)
    credentials = (key_id.get_secret_value(), secret.get_secret_value())
    if any(not value or not value.isascii() or any(not 33 <= ord(c) <= 126 for c in value)
           for value in credentials):
        raise NewsError("NEWS_CREDENTIALS_REQUIRED")
    try:
        output_dir.mkdir(mode=0o700)
    except OSError as error:
        raise NewsError("NEWS_NEW_OUTPUT_REQUIRED") from error
    query = (
        ("symbols", symbol), ("start", start.isoformat()), ("end", end.isoformat()),
        ("limit", "50"), ("sort", "desc"), ("include_content", "false"),
        ("exclude_contentless", "false"),
    )
    owned = client is None
    transport = client or httpx.AsyncClient(follow_redirects=False, timeout=30)
    try:
        fixed = FixedOriginHttpClient(
            provider_name="alpaca",
            origin=AllowedOrigin(scheme="https", host="data.alpaca.markets"),
            policy=HttpRequestPolicy(max_attempts=1, max_response_bytes=2 * 1024 * 1024),
            client=transport,
        )
        raw = await fixed.get_bytes(
            ("v1beta1", "news"), query,
            (("APCA-API-KEY-ID", credentials[0]), ("APCA-API-SECRET-KEY", credentials[1])),
        )
    except HttpPolicyError as error:
        allowed = {"STATUS_REJECTED", "REDIRECT_REJECTED", "RETRY_BUDGET_EXHAUSTED",
                   "DEADLINE_EXCEEDED", "RESPONSE_TOO_LARGE"}
        code = str(error) if str(error) in allowed else "FAILED"
        raise NewsError(f"NEWS_HTTP_{code}") from None
    except Exception:
        raise NewsError("NEWS_HTTP_FAILED") from None
    finally:
        if owned:
            await transport.aclose()
    received_at = datetime.now(UTC)
    # Check both raw and JSON-escaped secret representations before either file exists.
    if any(value.encode() in raw or json.dumps(value)[1:-1].encode() in raw
           for value in credentials):
        raise NewsError("NEWS_SECRET_REFLECTION")
    try:
        decoded = json.dumps(json.loads(raw), ensure_ascii=False)
    except (ValueError, TypeError):
        raise NewsError("NEWS_RESPONSE_INVALID") from None
    if any(value in decoded for value in credentials):
        raise NewsError("NEWS_SECRET_REFLECTION")
    try:
        page = parse_news(raw, symbol=symbol, received_at=received_at)
        if any(not start <= item.updated_at <= end for item in page.articles):
            raise NewsError("NEWS_WINDOW_RESPONSE_INVALID")
    except NewsError as error:
        rejected = {
            "schema": "nexus_news_rejected_receipt_v1", "code": str(error),
            "received_at": received_at.isoformat(), "raw_sha256": sha256(raw).hexdigest(),
            "raw_byte_length": len(raw),
        }
        try:
            write_private(output_dir / "rejected-raw.json", raw)
            write_private(output_dir / "rejected-receipt.json",
                          json.dumps(rejected, sort_keys=True).encode())
        except OSError:
            raise NewsError("NEWS_ARCHIVE_FAILED") from None
        raise
    receipt = {
        "schema": "nexus_news_receipt_v1", "source": "alpaca", "symbol": symbol,
        "start": start.isoformat(), "end": end.isoformat(),
        "received_at": received_at.isoformat(), "raw_sha256": sha256(raw).hexdigest(),
        "raw_byte_length": len(raw), "has_more": page.has_more,
    }
    try:
        write_private(output_dir / "raw.json", raw)
        write_private(output_dir / "receipt.json", json.dumps(receipt, sort_keys=True).encode())
    except OSError as error:
        raise NewsError("NEWS_ARCHIVE_FAILED") from error
    return {"status": "collected", "article_count": len(page.articles),
            "has_more": page.has_more, "realtime": False}


def main() -> int:
    parser = _Parser(description=__doc__)
    parser.add_argument("collect", choices=("collect",))
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--start", type=datetime.fromisoformat)
    parser.add_argument("--end", type=datetime.fromisoformat)
    try:
        args = parser.parse_args()
        environment = load_local_environment(args.repo_root, os.environ)
        result = asyncio.run(collect_news(
            symbol=args.symbol, output_dir=args.output_dir, start=args.start, end=args.end,
            key_id=SecretStr(environment.get("ALPACA_API_KEY_ID", "")),
            secret=SecretStr(environment.get("ALPACA_API_SECRET_KEY", "")),
        ))
    except NewsError as error:
        result = {"status": "failed", "code": str(error)}
    except (Exception, KeyboardInterrupt):
        result = {"status": "failed", "code": "NEWS_REQUEST_FAILED"}
    success = result["status"] == "collected"
    print(json.dumps(result), file=sys.stdout if success else sys.stderr)
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
