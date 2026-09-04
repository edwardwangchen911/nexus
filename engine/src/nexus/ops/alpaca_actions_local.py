"""Explicit bounded Alpaca originals collection; no database or ledger effects."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from time import monotonic
from typing import NoReturn

import httpx
from pydantic import SecretStr

from nexus.ops.fosun_local import load_local_environment
from nexus.ops.local_recovery import write_private
from nexus.providers.corporate_actions.alpaca_http import (
    AlpacaCorporateActionsTransport,
    CorporateActionsRequest,
    parse_corporate_actions_page,
)
from nexus.providers.http import AllowedOrigin, FixedOriginHttpClient, HttpRequestPolicy

_COLLECTION_SECONDS = 120.0
_REQUEST_SPACING_SECONDS = 0.35
_SOURCE = "alpaca"
_ORIGIN = "https://data.alpaca.markets"
_PATH = "/v1/corporate-actions"


class CollectionError(ValueError):
    """Only fixed, value-free codes leave the command boundary."""


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise CollectionError("ALPACA_ACTIONS_REQUEST_INVALID")


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _secret(value: str | None) -> SecretStr:
    if not value or not value.isascii() or any(not 33 <= ord(char) <= 126 for char in value):
        raise CollectionError("ALPACA_ACTIONS_CREDENTIALS_REQUIRED")
    return SecretStr(value)


async def collect_actions(
    request: CorporateActionsRequest,
    *,
    key_id: SecretStr,
    secret: SecretStr,
    output_dir: Path,
    max_pages: int,
) -> dict[str, object]:
    """Archive bytes before parsing; exhaustion says nothing about source completeness.

    The caller has already created a new private directory. Each original and
    final manifest is exclusive-create; a failed invocation must never be replayed
    into the same directory. No provider fields are synthesized or normalized.
    """
    deadline = monotonic() + _COLLECTION_SECONDS
    pages: list[dict[str, object]] = []
    accepted_pages = 0
    exhausted = False
    error_code: str | None = None
    phase_code = "ALPACA_ACTIONS_HTTP_FAILED"
    cursor: str | None = None
    seen: set[str] = set()

    def check_deadline() -> None:
        if monotonic() >= deadline:
            raise TimeoutError

    try:
        async with asyncio.timeout(_COLLECTION_SECONDS):
            async with httpx.AsyncClient(follow_redirects=False, timeout=30) as client:
                fixed = FixedOriginHttpClient(
                    provider_name="alpaca",
                    origin=AllowedOrigin(scheme="https", host="data.alpaca.markets"),
                    policy=HttpRequestPolicy(max_attempts=1, max_response_bytes=8 * 1024 * 1024),
                    client=client,
                )
                transport = AlpacaCorporateActionsTransport(fixed, key_id=key_id, secret=secret)
                for index in range(1, max_pages + 1):
                    phase_code = "ALPACA_ACTIONS_HTTP_FAILED"
                    if index > 1:
                        await asyncio.sleep(_REQUEST_SPACING_SECONDS)
                    check_deadline()
                    raw = await transport.fetch_raw(request, cursor)
                    received_at = datetime.now(UTC).isoformat()
                    phase_code = "ALPACA_ACTIONS_ARCHIVE_FAILED"
                    raw_name = f"page-{index:03}.json"
                    receipt_name = f"page-{index:03}.receipt.json"
                    # Preserve even malformed JSON. Receipt time is observation time,
                    # never a historical corporate-action availability assertion.
                    write_private(output_dir / raw_name, raw)
                    receipt = {
                        "schema": "nexus_alpaca_actions_receipt_v1",
                        "source": _SOURCE,
                        "origin": _ORIGIN,
                        "path": _PATH,
                        "query": request.query(cursor),
                        "raw_file": raw_name,
                        "sha256": sha256(raw).hexdigest(),
                        "byte_length": len(raw),
                        "received_at": received_at,
                    }
                    write_private(output_dir / receipt_name, _json_bytes(receipt))
                    pages.append({"raw_file": raw_name, "receipt_file": receipt_name})
                    check_deadline()
                    phase_code = "ALPACA_ACTIONS_PARSE_FAILED"
                    parsed = parse_corporate_actions_page(raw)
                    check_deadline()
                    accepted_pages += 1
                    if parsed.next_cursor is None:
                        exhausted = True
                        break
                    if parsed.next_cursor in seen:
                        raise CollectionError("ALPACA_ACTIONS_CURSOR_CYCLE")
                    seen.add(parsed.next_cursor)
                    cursor = parsed.next_cursor
                if not exhausted:
                    raise CollectionError("ALPACA_ACTIONS_PAGE_LIMIT")
                check_deadline()
    except TimeoutError:
        error_code = "ALPACA_ACTIONS_DEADLINE_EXCEEDED"
    except CollectionError as error:
        error_code = str(error)
    except Exception:
        error_code = phase_code

    if error_code is not None:
        exhausted = False
    status = "collected" if exhausted else ("partial" if accepted_pages else "failed")
    summary: dict[str, object] = {
        "status": status,
        "pagination_exhausted": exhausted,
        "source_completeness": "unknown",
        "execution_locked": True,
        "historical_backtest_eligible": False,
    }
    manifest = {
        "schema": "nexus_alpaca_actions_collection_v1",
        "source": _SOURCE,
        "origin": _ORIGIN,
        "path": _PATH,
        "query": request.query(),
        "pages": pages,
        "error_code": error_code,
        **summary,
    }
    # Persist the bounded attempt even when timeout/parse/HTTP failed. There is
    # deliberately no retry and no transaction that deletes received originals.
    write_private(output_dir / "manifest.json", _json_bytes(manifest))
    if error_code is not None:
        summary["code"] = error_code
    return summary


def main() -> None:
    parser = _Parser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--max-pages", type=int, default=10)
    try:
        args = parser.parse_args()
        request = CorporateActionsRequest(args.symbol, args.start, args.end)
        if not 1 <= args.max_pages <= 100:
            raise CollectionError("ALPACA_ACTIONS_REQUEST_INVALID")
        environment = load_local_environment(args.repo_root, os.environ)
        key_id = _secret(environment.get("ALPACA_API_KEY_ID"))
        secret = _secret(environment.get("ALPACA_API_SECRET_KEY"))
        try:
            args.output_dir.mkdir(mode=0o700)
        except OSError as error:
            raise CollectionError("ALPACA_ACTIONS_NEW_OUTPUT_REQUIRED") from error
        result = asyncio.run(
            collect_actions(
                request,
                key_id=key_id,
                secret=secret,
                output_dir=args.output_dir,
                max_pages=args.max_pages,
            )
        )
    except KeyboardInterrupt:
        # Cancellation may leave originals without a final manifest. Keep them;
        # never replay this attempt or claim exhaustion after interruption.
        result = {"status": "failed", "code": "ALPACA_ACTIONS_INTERRUPTED"}
    except CollectionError as error:
        result = {"status": "failed", "code": str(error)}
    except Exception:
        result = {"status": "failed", "code": "ALPACA_ACTIONS_REQUEST_FAILED"}
    success = result["status"] == "collected"
    print(json.dumps(result), file=sys.stdout if success else sys.stderr)
    if not success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
