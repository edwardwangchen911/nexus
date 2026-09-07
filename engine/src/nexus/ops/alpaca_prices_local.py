"""Collect bounded current-research price originals; no business writes or authority."""

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
from uuid import UUID

import httpx
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from nexus.market.universe import stable_instrument_id
from nexus.ops.fosun_local import load_local_environment
from nexus.ops.local_recovery import write_private
from nexus.persistence.models.market import Instrument
from nexus.providers.base import BarsRequest
from nexus.providers.http import (
    AllowedOrigin,
    FixedOriginHttpClient,
    HttpPolicyError,
    HttpRequestPolicy,
)
from nexus.providers.market.alpaca import MarketProviderPayloadError
from nexus.providers.market.alpaca_http import (
    AlpacaHttpTransport,
    SqlInstrumentSymbolResolver,
    _bars_query,
)
from nexus.research.alpaca_prices_archive import (
    MAX_PAGE_BYTES,
    MAX_PAGES,
    ORIGIN,
    UNKNOWN_QUALIFICATION,
    _verify_pending_collection,
    collection_window,
    json_bytes,
    price_page_cursor,
)

_COLLECTION_SECONDS = 120.0
_REQUEST_SPACING_SECONDS = 0.35


class CollectionError(ValueError):
    """Only fixed value-free errors leave this local entrypoint."""


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise CollectionError("ALPACA_PRICES_REQUEST_INVALID")


class _FrozenSymbol:
    def __init__(self, request: BarsRequest, symbol: str, identity_asof: date) -> None:
        self.request, self.symbol, self.identity_asof = request, symbol, identity_asof

    async def resolve(self, instrument_id: UUID, as_of: date) -> str:
        if instrument_id != self.request.instrument_id or as_of != self.identity_asof:
            raise CollectionError("ALPACA_PRICES_IDENTITY_INVALID")
        return self.symbol


class _ArchivingBytes:
    """Archive only complete successful HTTP bytes, before the transport parses them."""

    def __init__(
        self, client: FixedOriginHttpClient, output: Path, common: dict[str, object]
    ) -> None:
        self.client, self.output, self.common = client, output, common
        self.pages: list[dict[str, str]] = []
        self.phase = "ALPACA_PRICES_HTTP_FAILED"

    async def get_bytes(
        self,
        path_segments: tuple[str, ...],
        query: tuple[tuple[str, str], ...],
        headers: tuple[tuple[str, str], ...],
    ) -> bytes:
        self.phase = "ALPACA_PRICES_HTTP_FAILED"
        raw = await self.client.get_bytes(path_segments, query, headers)
        received_at = datetime.now(UTC).isoformat()
        self.phase = "ALPACA_PRICES_ARCHIVE_FAILED"
        number = len(self.pages) + 1
        raw_name, receipt_name = f"page-{number:03}.json", f"page-{number:03}.receipt.json"
        write_private(self.output / raw_name, raw)
        receipt = json_bytes(
            {
                **self.common,
                "schema": "nexus_alpaca_prices_receipt_v1",
                "query": query,
                "raw_file": raw_name,
                "sha256": sha256(raw).hexdigest(),
                "byte_length": len(raw),
                "received_at": received_at,
            }
        )
        write_private(self.output / receipt_name, receipt)
        self.pages.append(
            {
                "raw_file": raw_name,
                "receipt_file": receipt_name,
                "raw_sha256": sha256(raw).hexdigest(),
                "receipt_sha256": sha256(receipt).hexdigest(),
            }
        )
        self.phase = "ALPACA_PRICES_PARSE_FAILED"
        return raw


async def _resolve_identity(database_url: str, request: BarsRequest, identity_asof: date) -> str:
    database = make_url(database_url)
    if (
        database.drivername != "postgresql+asyncpg"
        or database.host not in {"localhost", "127.0.0.1", "::1"}
        or database.query
    ):
        raise CollectionError("ALPACA_PRICES_LOCAL_DATABASE_REQUIRED")
    engine = create_async_engine(database_url, connect_args={"timeout": 10})
    try:
        async with asyncio.timeout(15):
            async with AsyncSession(engine) as session, session.begin():
                await session.execute(text("SET TRANSACTION READ ONLY"))
                try:
                    return await SqlInstrumentSymbolResolver(session).resolve(
                        request.instrument_id, identity_asof
                    )
                except MarketProviderPayloadError as error:
                    if (
                        str(error) != "INSTRUMENT_SYMBOL_AUTHORITY_INVALID"
                        or request.instrument_id != stable_instrument_id("SPY")
                    ):
                        raise CollectionError("ALPACA_PRICES_IDENTITY_INVALID") from None
                    master = await session.get(Instrument, request.instrument_id)
                    if (
                        master is None
                        or master.exchange != "ARCX"
                        or master.currency != "USD"
                    ):
                        raise CollectionError("ALPACA_PRICES_IDENTITY_INVALID") from None
                    return "SPY"
    finally:
        await engine.dispose()


async def collect_prices(
    request: BarsRequest,
    *,
    symbol: str,
    calendar: dict[str, str],
    resolved_at: datetime,
    key_id: SecretStr,
    secret: SecretStr,
    output_dir: Path,
    max_pages: int,
    identity_asof: date | None = None,
) -> dict[str, object]:
    started_at = datetime.now(UTC)
    identity_date = identity_asof or request.end.date()
    if identity_date > started_at.date():
        raise CollectionError("ALPACA_PRICES_IDENTITY_DATE_INVALID")
    deadline = monotonic() + _COLLECTION_SECONDS
    common: dict[str, object] = {
        "source": "alpaca",
        "origin": ORIGIN,
        "path": f"/v2/stocks/{symbol}/bars",
        "instrument_id": str(request.instrument_id),
        "resolved_symbol": symbol,
        "identity_asof": identity_date.isoformat(),
        "identity_resolved_at": resolved_at.isoformat(),
        "calendar": calendar,
    }
    archive: _ArchivingBytes | None = None
    accepted_pages = 0
    exhausted = False
    error_code: str | None = None
    try:
        async with asyncio.timeout(_COLLECTION_SECONDS):
            async with httpx.AsyncClient(follow_redirects=False, timeout=30) as client:
                fixed = FixedOriginHttpClient(
                    provider_name="alpaca",
                    origin=AllowedOrigin(scheme="https", host="data.alpaca.markets"),
                    policy=HttpRequestPolicy(max_attempts=1, max_response_bytes=MAX_PAGE_BYTES),
                    client=client,
                )
                archive = _ArchivingBytes(fixed, output_dir, common)
                transport = AlpacaHttpTransport(
                    archive,
                    key_id=key_id,
                    secret=secret,
                    feed="sip",
                    symbol_resolver=_FrozenSymbol(request, symbol, identity_date),
                    identity_asof=identity_date,
                )
                cursor: str | None = None
                seen: set[str] = set()
                for index in range(max_pages):
                    if index:
                        await asyncio.sleep(_REQUEST_SPACING_SECONDS)
                    response = await transport.fetch_bars(request, cursor)
                    cursor = price_page_cursor(response.payload)
                    # asyncio cannot preempt synchronous archive/JSON work.
                    if monotonic() >= deadline:
                        raise TimeoutError
                    accepted_pages += 1
                    if cursor is None:
                        exhausted = True
                        break
                    if cursor in seen:
                        raise CollectionError("ALPACA_PRICES_CURSOR_CYCLE")
                    seen.add(cursor)
                if not exhausted:
                    raise CollectionError("ALPACA_PRICES_PAGE_LIMIT")
                if monotonic() >= deadline:
                    raise TimeoutError
    except TimeoutError:
        error_code = "ALPACA_PRICES_DEADLINE_EXCEEDED"
    except CollectionError as error:
        error_code = str(error)
    except HttpPolicyError as error:
        safe_code = str(error)
        if safe_code not in {
            "REDIRECT_REJECTED",
            "RESPONSE_TOO_LARGE",
            "RETRY_BUDGET_EXHAUSTED",
            "STATUS_REJECTED",
        }:
            safe_code = "HTTP_FAILED"
        error_code = f"ALPACA_PRICES_{safe_code}"
    except Exception:
        error_code = "ALPACA_PRICES_HTTP_FAILED" if archive is None else archive.phase
    exhausted = exhausted and error_code is None
    summary: dict[str, object] = {
        "status": "collected" if exhausted else ("partial" if accepted_pages else "failed"),
        "pagination_exhausted": exhausted,
        **UNKNOWN_QUALIFICATION,
    }
    manifest = {
        **common,
        **summary,
        "schema": "nexus_alpaca_prices_collection_v1",
        "query": _bars_query(request, None, "sip", identity_date),
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "pages": [] if archive is None else archive.pages,
        "error_code": error_code,
    }
    if exhausted:
        pending = output_dir / "manifest.pending.json"
        published = output_dir / "manifest.json"
        write_private(pending, json_bytes(manifest))
        if monotonic() >= deadline:
            raise CollectionError("ALPACA_PRICES_DEADLINE_EXCEEDED")
        _verify_pending_collection(output_dir, instrument_id=request.instrument_id)
        if monotonic() >= deadline:
            raise CollectionError("ALPACA_PRICES_DEADLINE_EXCEEDED")
        # Exclusive publication of already-fsynced, verified bytes. On a late
        # publish retain the pending original, but revoke this attempt's link.
        os.link(pending, published)
        if monotonic() >= deadline:
            published.unlink()
            raise CollectionError("ALPACA_PRICES_DEADLINE_EXCEEDED")
        pending.unlink()
        if monotonic() >= deadline:
            published.rename(pending)
            raise CollectionError("ALPACA_PRICES_DEADLINE_EXCEEDED")
        summary["readback_verified"] = True
    else:
        write_private(output_dir / "manifest.json", json_bytes(manifest))
        summary["code"] = error_code
    return summary


def _secret(value: str | None) -> SecretStr:
    if not value or not value.isascii() or any(not 33 <= ord(c) <= 126 for c in value):
        raise CollectionError("ALPACA_PRICES_CREDENTIALS_REQUIRED")
    return SecretStr(value)


def main() -> int:
    parser = _Parser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--instrument-id", type=UUID, required=True)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--adjustment", choices=("raw", "split", "all"), required=True)
    parser.add_argument("--identity-asof", type=date.fromisoformat)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--max-pages", type=int, default=10)
    try:
        args = parser.parse_args()
        if not 1 <= args.max_pages <= MAX_PAGES:
            raise CollectionError("ALPACA_PRICES_REQUEST_INVALID")
        request, calendar = collection_window(
            args.instrument_id, args.start, args.end, args.adjustment, now=datetime.now(UTC)
        )
        identity_date = args.identity_asof or request.end.date()
        if identity_date > datetime.now(UTC).date():
            raise CollectionError("ALPACA_PRICES_IDENTITY_DATE_INVALID")
        environment = load_local_environment(args.repo_root, os.environ)
        key_id, secret = (
            _secret(environment.get("ALPACA_API_KEY_ID")),
            _secret(environment.get("ALPACA_API_SECRET_KEY")),
        )
        symbol = asyncio.run(
            _resolve_identity(environment.get("DATABASE_URL", ""), request, identity_date)
        )
        resolved_at = datetime.now(UTC)
        try:
            args.output_dir.mkdir(mode=0o700)
        except OSError as error:
            raise CollectionError("ALPACA_PRICES_NEW_OUTPUT_REQUIRED") from error
        result = asyncio.run(
            collect_prices(
                request,
                symbol=symbol,
                calendar=calendar,
                resolved_at=resolved_at,
                key_id=key_id,
                secret=secret,
                output_dir=args.output_dir,
                max_pages=args.max_pages,
                identity_asof=identity_date,
            )
        )
    except KeyboardInterrupt:
        result = {"status": "failed", "code": "ALPACA_PRICES_INTERRUPTED"}
    except CollectionError as error:
        result = {"status": "failed", "code": str(error)}
    except Exception:
        result = {"status": "failed", "code": "ALPACA_PRICES_REQUEST_FAILED"}
    success = result["status"] == "collected"
    print(json.dumps(result), file=sys.stdout if success else sys.stderr)
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
