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
from nexus.providers.http import AllowedOrigin, FixedOriginHttpClient, HttpRequestPolicy

_EXCHANGES = {"Nasdaq": "XNAS", "NYSE": "XNYS", "NYSE American": "XASE"}


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("IDENTITY_COLLECTION_JSON_INVALID")
        result[key] = value
    return result


def _constant(value: str) -> NoReturn:
    raise ValueError("IDENTITY_COLLECTION_JSON_INVALID")


def _document(raw: bytes) -> dict[str, Any]:
    value = json.loads(raw, object_pairs_hook=_unique, parse_constant=_constant)
    if not isinstance(value, dict):
        raise ValueError("IDENTITY_COLLECTION_JSON_INVALID")
    return value


def _binding(directory: dict[str, Any], symbol: str) -> tuple[str, str]:
    if directory.get("fields") != ["cik", "name", "ticker", "exchange"]:
        raise ValueError("IDENTITY_COLLECTION_MAPPING_INVALID")
    rows = directory.get("data")
    if not isinstance(rows, list) or any(not isinstance(r, list) or len(r) != 4 for r in rows):
        raise ValueError("IDENTITY_COLLECTION_MAPPING_INVALID")
    matches = [r for r in rows if r[2] == symbol]
    if len(matches) != 1:
        raise ValueError("IDENTITY_COLLECTION_MAPPING_INVALID")
    cik, _, _, exchange = matches[0]
    if type(cik) is not int or not 0 < cik < 10**10 or exchange not in _EXCHANGES:
        raise ValueError("IDENTITY_COLLECTION_MAPPING_INVALID")
    return str(cik).zfill(10), _EXCHANGES[exchange]


def _require_submissions(document: dict[str, Any], symbol: str, cik: str, exchange: str) -> None:
    issuer = document.get("cik")
    if type(issuer) not in (str, int) or not str(issuer).isascii() or not str(issuer).isdigit():
        raise ValueError("IDENTITY_COLLECTION_ISSUER_INVALID")
    tickers, exchanges = document.get("tickers"), document.get("exchanges")
    if (
        str(issuer).zfill(10) != cik
        or not isinstance(tickers, list)
        or not isinstance(exchanges, list)
        or len(tickers) != len(exchanges)
        or not all(isinstance(v, str) for v in tickers + exchanges)
    ):
        raise ValueError("IDENTITY_COLLECTION_ISSUER_INVALID")
    matches = [
        (t, _EXCHANGES.get(e)) for t, e in zip(tickers, exchanges, strict=True) if t == symbol
    ]
    if matches != [(symbol, exchange)]:
        raise ValueError("IDENTITY_COLLECTION_ISSUER_INVALID")


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
        fixed = FixedOriginHttpClient(
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
