"""Explicit SEC collection, offline current identity import, and exact local readback."""

import argparse
import asyncio
import json
import os
import re
from dataclasses import asdict
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from typing import NoReturn
from urllib.parse import urlsplit
from uuid import UUID

import httpx
from sqlalchemy import text
from sqlalchemy.engine import make_url

from nexus.backtest.future_identity import prepare_future_identity_bindings
from nexus.market.current_identity import SEC_URL
from nexus.market.current_identity_storage import (
    CurrentIdentityImportResult,
    IdentityStore,
    import_current_common_stock,
    read_current_identity,
)
from nexus.market.current_listing_revision import import_listing_revision, read_listing_revision
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops.fosun_local import load_local_environment
from nexus.ops.local_recovery import write_private
from nexus.ops.sec_local import _BoundedBytes, _Budget
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings


async def collect_sec(
    *, source_url: str, contact: str, output_dir: Path, client: httpx.AsyncClient | None = None
) -> None:
    match = SEC_URL.fullmatch(source_url)
    if (
        not match
        or not contact
        or contact != contact.strip()
        or "@" not in contact
        or len(contact.encode()) > 254
        or any(ord(c) < 32 for c in contact)
    ):
        raise ValueError("CURRENT_IDENTITY_SEC_REQUEST_INVALID")
    output_dir.mkdir(mode=0o700)
    owned = client is None
    transport = client or httpx.AsyncClient(follow_redirects=False, timeout=30)
    statuses: list[int] = []

    async def record_status(response: httpx.Response) -> None:
        statuses.append(response.status_code)

    transport.event_hooks["response"].append(record_status)
    try:
        raw = await _BoundedBytes(transport, "www.sec.gov", _Budget(1)).get_bytes(
            tuple(urlsplit(source_url).path.lstrip("/").split("/")),
            (),
            (("User-Agent", f"NEXUS {contact}"), ("Accept-Encoding", "gzip, deflate")),
        )
        received = datetime.now(UTC)
        if statuses != [200]:
            raise ValueError("CURRENT_IDENTITY_SEC_STATUS_INVALID")
        if contact.encode() in raw:
            raise ValueError("CURRENT_IDENTITY_CONTACT_REFLECTION")
        write_private(output_dir / match[3], raw)
        receipt = dict(
            source_url=source_url,
            received_at=received.isoformat(),
            sha256=sha256(raw).hexdigest(),
            byte_length=len(raw),
            http_status=200,
        )
        write_private(output_dir / "receipt.json", json.dumps(receipt, sort_keys=True).encode())
    finally:
        transport.event_hooks["response"].remove(record_status)
        if owned:
            await transport.aclose()


def _local(settings: EngineSettings) -> None:
    database = make_url(settings.database_url) if settings.database_url else None
    endpoint = urlsplit(settings.object_store_endpoint or "")
    if (
        database is None
        or database.host not in {"localhost", "127.0.0.1", "::1"}
        or "host" in database.query
        or endpoint.scheme not in {"http", "https"}
        or endpoint.hostname not in {"localhost", "127.0.0.1", "::1"}
        or endpoint.username
        or endpoint.password
        or endpoint.path
        or endpoint.query
        or endpoint.fragment
    ):
        raise ValueError("CURRENT_IDENTITY_LOCAL_SERVICES_REQUIRED")


async def import_identity(
    settings: EngineSettings,
    *,
    symbol: str,
    fmp_archive: Path,
    sec_archive: Path,
    store: IdentityStore | None = None,
) -> CurrentIdentityImportResult:
    _local(settings)
    storage = store or S3ProviderEvidenceStore(settings, disable_environment_proxies=True)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            result = await import_current_common_stock(
                session, storage, symbol=symbol, fmp_archive=fmp_archive, sec_archive=sec_archive
            )
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await read_current_identity(session, storage, snapshot_id=result.snapshot_id)
        return result
    finally:
        await factory.kw["bind"].dispose()


async def read_identity(settings: EngineSettings, *, snapshot_id: UUID) -> None:
    _local(settings)
    storage = S3ProviderEvidenceStore(settings, disable_environment_proxies=True)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await read_current_identity(session, storage, snapshot_id=snapshot_id)
    finally:
        await factory.kw["bind"].dispose()


async def prepare_universe(
    settings: EngineSettings,
    *,
    effective_session: date,
    store: IdentityStore | None = None,
    use_listing_revisions: bool = False,
) -> dict[str, object]:
    _local(settings)
    storage = store or S3ProviderEvidenceStore(settings, disable_environment_proxies=True)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            return await prepare_future_identity_bindings(
                session,
                storage,
                effective_session=effective_session,
                use_listing_revisions=use_listing_revisions,
            )
    finally:
        await factory.kw["bind"].dispose()


async def revise_listing(
    settings: EngineSettings,
    *,
    symbol: str,
    fmp_archive: Path,
    sec_archive: Path,
    store: IdentityStore | None = None,
) -> CurrentIdentityImportResult:
    _local(settings)
    storage = store or S3ProviderEvidenceStore(settings, disable_environment_proxies=True)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            result = await import_listing_revision(
                session, storage, symbol=symbol, fmp_archive=fmp_archive, sec_archive=sec_archive
            )
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await read_listing_revision(session, storage, snapshot_id=result.snapshot_id)
        return result
    finally:
        await factory.kw["bind"].dispose()


async def read_listing(settings: EngineSettings, *, snapshot_id: UUID) -> None:
    _local(settings)
    storage = S3ProviderEvidenceStore(settings, disable_environment_proxies=True)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await read_listing_revision(session, storage, snapshot_id=snapshot_id)
    finally:
        await factory.kw["bind"].dispose()


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise ValueError("CURRENT_IDENTITY_REQUEST_INVALID")


def main() -> None:
    parser = _Parser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    commands = parser.add_subparsers(dest="action", required=True)
    collect = commands.add_parser("collect-sec")
    collect.add_argument("--source-url", required=True)
    collect.add_argument("--output-dir", type=Path, required=True)
    importer = commands.add_parser("import")
    importer.add_argument("--symbol", required=True)
    importer.add_argument("--fmp-archive", type=Path, required=True)
    importer.add_argument("--sec-archive", type=Path, required=True)
    importer.add_argument("--output", type=Path, required=True)
    revision = commands.add_parser("revise-listing")
    revision.add_argument("--symbol", required=True)
    revision.add_argument("--fmp-archive", required=True, type=Path)
    revision.add_argument("--sec-archive", required=True, type=Path)
    revision.add_argument("--output", required=True, type=Path)
    revision_reader = commands.add_parser("read-listing-revision")
    revision_reader.add_argument("--snapshot-id", required=True, type=UUID)
    prepare = commands.add_parser("prepare-universe")
    prepare.add_argument("--use-listing-revisions", action="store_true")
    prepare.add_argument("--effective-session", required=True, type=date.fromisoformat)
    prepare.add_argument("--output", required=True, type=Path)
    reader = commands.add_parser("read")
    reader.add_argument("--snapshot-id", type=UUID, required=True)
    try:
        args = parser.parse_args()
        environment = load_local_environment(args.repo_root, os.environ)
        if args.action == "collect-sec":
            asyncio.run(
                collect_sec(
                    source_url=args.source_url,
                    contact=environment.get("SEC_CONTACT", ""),
                    output_dir=args.output_dir,
                )
            )
            status = "collected"
        else:
            settings = EngineSettings.model_validate(environment)
            if args.action == "prepare-universe":
                if args.output.exists() or args.output.is_symlink():
                    raise ValueError("CURRENT_IDENTITY_NEW_OUTPUT_REQUIRED")
                prepared = asyncio.run(
                    prepare_universe(
                        settings,
                        effective_session=args.effective_session,
                        use_listing_revisions=args.use_listing_revisions,
                    )
                )
                write_private(args.output, json.dumps(prepared, sort_keys=True).encode())
                status = str(prepared["status"])
            elif args.action in {"import", "revise-listing"}:
                if args.output.exists() or args.output.is_symlink():
                    raise ValueError("CURRENT_IDENTITY_NEW_OUTPUT_REQUIRED")
                result = asyncio.run(
                    (revise_listing if args.action == "revise-listing" else import_identity)(
                        settings,
                        symbol=args.symbol,
                        fmp_archive=args.fmp_archive,
                        sec_archive=args.sec_archive,
                    )
                )
                write_private(
                    args.output, json.dumps(asdict(result), default=str, sort_keys=True).encode()
                )
                status = result.status
            else:
                asyncio.run(
                    (read_listing if args.action == "read-listing-revision" else read_identity)(
                        settings, snapshot_id=args.snapshot_id
                    )
                )
                status = "verified"
        print(
            json.dumps(
                dict(
                    status=status,
                    universe_published=False,
                    research_created=False,
                    execution_locked=True,
                )
            )
        )
    except Exception as error:
        code = (
            str(error)
            if isinstance(error, ValueError)
            and re.fullmatch(
                r"(?:CURRENT_IDENTITY|CURRENT_LISTING|BACKTEST_UNIVERSE)_[A-Z_]+", str(error)
            )
            else "CURRENT_IDENTITY_FAILED"
        )
        print(json.dumps(dict(status="failed", code=code)))
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
