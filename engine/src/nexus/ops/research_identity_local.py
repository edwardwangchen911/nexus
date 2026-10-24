"""Archive current SEC identity sources only; no database or research authority writes."""

import argparse
import asyncio
import json
import os
import stat
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, NoReturn

import httpx

from nexus.ops.fosun_local import load_local_environment
from nexus.ops.local_recovery import write_private
from nexus.providers.fmp_profile import validate_symbol
from nexus.providers.http import (
    AllowedOrigin,
    FixedOriginHttpClient,
    HttpPolicyError,
    HttpRequestPolicy,
)
from nexus.research.identity_sources import (
    directory_binding as _binding,
)
from nexus.research.identity_sources import (
    parse_identity_source as _document,
)
from nexus.research.identity_sources import (
    require_submissions_binding as _require_submissions,
)


class _Exact200Client(FixedOriginHttpClient):
    async def _read_bounded(self, response: httpx.Response) -> bytes:
        # SEC's complete-original receipt must not relabel partial/empty 2xx as 200.
        if response.status_code != 200:
            raise HttpPolicyError("STATUS_REJECTED", http_status=response.status_code)
        return await super()._read_bounded(response)


async def collect_identity_sources(
    *,
    symbols: tuple[str, ...],
    contact: str,
    output_dir: Path,
    client: httpx.AsyncClient | None = None,
) -> dict[str, object]:
    if not 1 <= len(symbols) <= 5 or len(set(symbols)) != len(symbols):
        raise ValueError("IDENTITY_COLLECTION_REQUEST_INVALID")
    for symbol in symbols:
        validate_symbol(symbol)
    if (
        not contact
        or contact != contact.strip()
        or "@" not in contact
        or len(contact.encode()) > 254
        or any(ord(c) < 32 for c in contact)
    ):
        raise ValueError("IDENTITY_COLLECTION_REQUEST_INVALID")
    parent = output_dir.parent
    if (
        parent.is_symlink()
        or parent.stat().st_uid != os.getuid()
        or stat.S_IMODE(parent.stat().st_mode) != 0o700
        or output_dir.exists()
        or output_dir.is_symlink()
    ):
        raise ValueError("IDENTITY_COLLECTION_DIRECTORY_INVALID")
    output_dir.mkdir(mode=0o700)
    owned = client is None
    transport = client or httpx.AsyncClient(follow_redirects=False, timeout=30)
    requests = 0
    stage = "directory"

    async def get(host: str, parts: tuple[str, ...], name: str) -> dict[str, Any]:
        nonlocal requests
        requests += 1
        fixed = _Exact200Client(
            provider_name="sec",
            origin=AllowedOrigin("https", host),
            policy=HttpRequestPolicy(max_attempts=1),
            client=transport,
        )
        raw = await fixed.get_bytes(parts, (), (("User-Agent", f"NEXUS {contact}"),))
        received = datetime.now(UTC)
        if contact.encode() in raw:
            raise ValueError("IDENTITY_COLLECTION_CONTACT_REFLECTION")
        write_private(output_dir / f"{name}.json", raw)
        receipt = {
            "schema": "sec_identity_source_receipt_v1",
            "source_url": "https://" + host + "/" + "/".join(parts),
            "http_status": 200,
            "received_at": received.isoformat(),
            "sha256": sha256(raw).hexdigest(),
            "byte_length": len(raw),
        }
        write_private(
            output_dir / f"{name}-receipt.json", json.dumps(receipt, sort_keys=True).encode()
        )
        return _document(raw)

    try:
        async with asyncio.timeout(60):
            directory = await get(
                "www.sec.gov", ("files", "company_tickers_exchange.json"), "directory"
            )
            bindings = {symbol: _binding(directory, symbol) for symbol in symbols}
            for symbol, (cik, exchange) in bindings.items():
                stage = "submissions"
                document = await get(
                    "data.sec.gov", ("submissions", f"CIK{cik}.json"), f"{symbol}-submissions"
                )
                _require_submissions(document, symbol, cik, exchange)
        result: dict[str, object] = {
            "status": "archived",
            "requests": requests,
            "authority_registered": False,
            "execution_locked": True,
        }
        write_private(output_dir / "result.json", json.dumps(result).encode())
        return result
    except Exception:
        result = {
            "status": "unavailable",
            "code": "IDENTITY_COLLECTION_FAILED",
            "stage": stage,
            "requests": requests,
            "authority_registered": False,
            "execution_locked": True,
        }
        write_private(output_dir / "result.json", json.dumps(result).encode())
        raise ValueError("IDENTITY_COLLECTION_FAILED") from None
    finally:
        if owned:
            await transport.aclose()


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise ValueError("IDENTITY_COLLECTION_REQUEST_INVALID")


def main() -> None:
    parser = _Parser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--symbol", action="append", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    try:
        args = parser.parse_args()
        environment = load_local_environment(args.repo_root, os.environ)
        result = asyncio.run(
            collect_identity_sources(
                symbols=tuple(args.symbol),
                contact=environment.get("SEC_CONTACT", ""),
                output_dir=args.output_dir,
            )
        )
        print(json.dumps(result))
    except Exception:
        print(json.dumps({"status": "unavailable", "code": "IDENTITY_COLLECTION_FAILED"}))
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
