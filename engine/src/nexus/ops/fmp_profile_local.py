"""Explicit FMP company-background collection and local archive import."""

import argparse
import asyncio
import json
import os
import sys
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import NoReturn
from urllib.parse import urlsplit
from uuid import UUID

import httpx
from pydantic import SecretStr
from sqlalchemy.engine import make_url

from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops.fosun_local import load_local_environment
from nexus.ops.local_recovery import write_private
from nexus.persistence.session import session_factory
from nexus.providers.fmp_profile import ORIGIN, PATH, fetch_profile, parse_profile, validate_symbol
from nexus.research.fmp_profile import RECEIPT_SCHEMA, SCHEMA
from nexus.research.fmp_profile_archive import load_profile_archive
from nexus.research.fmp_profile_storage import (
    persist_company_profile,
    read_company_profiles,
    require_profile_identity,
    save_profile_artifact,
)
from nexus.settings import EngineSettings


async def collect_profile(
    *, symbol: str, key: SecretStr, output_dir: Path, client: httpx.AsyncClient | None = None
) -> dict[str, object]:
    validate_symbol(symbol)
    output_dir.mkdir(mode=0o700)
    owned_client = client is None
    transport = client or httpx.AsyncClient(follow_redirects=False, timeout=30)
    try:
        async with asyncio.timeout(45):
            raw = await fetch_profile(transport, symbol=symbol, key=key)
            # Never archive an accidental reflected request credential.
            if key.get_secret_value().encode() in raw:
                raise ValueError("FMP_PROFILE_SECRET_REFLECTION")
            received = datetime.now(UTC)
            write_private(output_dir / "profile.json", raw)
            receipt = {
                "schema": RECEIPT_SCHEMA,
                "source": "fmp",
                "origin": ORIGIN,
                "path": PATH,
                "symbol": symbol,
                "raw_sha256": sha256(raw).hexdigest(),
                "raw_byte_length": len(raw),
                "received_at": received.isoformat(),
            }
            write_private(
                output_dir / "receipt.json",
                json.dumps(
                    receipt,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode(),
            )
            parse_profile(raw, symbol)
    finally:
        if owned_client:
            await transport.aclose()
    return {"status": "collected", "execution_locked": True, "model_called": False}


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise ValueError("FMP_PROFILE_REQUEST_INVALID")


async def import_profile(
    settings: EngineSettings,
    *,
    instrument_id: UUID,
    symbol: str,
    collection_dir: Path,
    store: S3ProviderEvidenceStore | None = None,
) -> dict[str, object]:
    """Persist background only; objects may remain if a subsequent DB transaction fails."""
    endpoint = urlsplit(settings.object_store_endpoint or "")
    database = make_url(settings.database_url) if settings.database_url else None
    local = {"localhost", "127.0.0.1", "::1"}
    if (
        database is None
        or database.host not in local
        or "host" in database.query
        or endpoint.scheme not in {"http", "https"}
        or endpoint.hostname not in local
        or endpoint.username
        or endpoint.password
        or endpoint.path
        or endpoint.query
        or endpoint.fragment
    ):
        raise ValueError("FMP_PROFILE_LOCAL_SERVICES_REQUIRED")
    archive = load_profile_archive(collection_dir, instrument_id=instrument_id, symbol=symbol)
    document = archive.document
    factory = session_factory(settings)
    async with factory.begin() as session:
        await require_profile_identity(session, document)
        object_store = store or S3ProviderEvidenceStore(settings, disable_environment_proxies=True)
        for original in archive.originals:
            await save_profile_artifact(
                session,
                object_store,
                content=original.content,
                schema=original.schema_version,
                received_at=original.received_at,
            )
        location = await save_profile_artifact(
            session,
            object_store,
            content=document.canonical_bytes(),
            schema=SCHEMA,
            received_at=document.available_at,
        )
        evidence = await persist_company_profile(session, document, location=location)
        documents = await read_company_profiles(
            session,
            instrument_id=instrument_id,
            as_of=datetime.now(UTC),
            evidence_ids=(evidence.id,),
        )
        if documents.get(evidence.id) != document:
            raise ValueError("FMP_PROFILE_READBACK_FAILED")
        return {
            "status": "stored_context",
            "evidence_id": str(evidence.id),
            "content_hash": evidence.content_hash,
            "available_at": document.available_at.isoformat(),
            "research_created": False,
            "execution_locked": True,
            "model_called": False,
        }


def main() -> None:
    parser = _Parser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    commands = parser.add_subparsers(dest="action", required=True)
    collect = commands.add_parser("collect")
    collect.add_argument("--symbol", required=True)
    collect.add_argument("--output-dir", type=Path, required=True)
    importer = commands.add_parser("import")
    importer.add_argument("--symbol", required=True)
    importer.add_argument("--instrument-id", type=UUID, required=True)
    importer.add_argument("--collection-dir", type=Path, required=True)
    importer.add_argument("--output", type=Path, required=True)
    try:
        args = parser.parse_args()
        validate_symbol(args.symbol)
        environment = load_local_environment(args.repo_root, os.environ)
        if args.action == "collect":
            key = SecretStr(environment.get("FMP_API_KEY", ""))
            result = asyncio.run(
                collect_profile(symbol=args.symbol, key=key, output_dir=args.output_dir)
            )
        else:
            details = asyncio.run(
                import_profile(
                    EngineSettings.model_validate(environment),
                    instrument_id=args.instrument_id,
                    symbol=args.symbol,
                    collection_dir=args.collection_dir,
                )
            )
            write_private(args.output, json.dumps(details, sort_keys=True).encode())
            result = {"status": "stored_context", "model_called": False, "research_created": False}
    except KeyboardInterrupt:
        result = {"status": "failed", "code": "FMP_PROFILE_INTERRUPTED"}
    except Exception:
        result = {"status": "failed", "code": "FMP_PROFILE_FAILED"}
    success = result["status"] != "failed"
    print(json.dumps(result), file=sys.stdout if success else sys.stderr)
    if not success:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
