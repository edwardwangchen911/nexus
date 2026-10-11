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
    import_current_held_common_stock,
    read_current_held_common_stock,
    read_current_identity,
)
from nexus.market.current_listing_revision import import_listing_revision, read_listing_revision
from nexus.market.custom_identity import archive_member_taxonomy
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops.cboe_listing_local import collect_listing
from nexus.ops.fosun_local import load_local_environment
from nexus.ops.local_recovery import write_private
from nexus.ops.sec_local import _BoundedBytes, _Budget
from nexus.persistence.session import session_factory
from nexus.providers.broker.fosun_contracts import AccountItem
from nexus.providers.broker.gateway_client import FosunGatewayClient
from nexus.providers.broker.settings import BrokerWorkerSettings
from nexus.providers.http import HttpPolicyError
from nexus.settings import EngineSettings

SEC_SUBMISSION_URL = re.compile(
    r"https://www\.sec\.gov/Archives/edgar/data/([1-9][0-9]{0,9})/"
    r"([0-9]{18})/([0-9]{10}-[0-9]{2}-[0-9]{6}\.txt)"
)
SEC_DIRECTORY_FILES = {
    "company": "company_tickers_exchange.json",
    "fund": "company_tickers_mf.json",
}


async def collect_sec(
    *,
    source_url: str,
    contact: str,
    output_dir: Path,
    client: httpx.AsyncClient | None = None,
    submission: bool = False,
    directory_kind: str | None = None,
) -> None:
    if directory_kind is None:
        match = (SEC_SUBMISSION_URL if submission else SEC_URL).fullmatch(source_url)
        filename = match[3] if match else None
        valid_source = bool(match) and (
            not submission or (match is not None and match[3][:-4].replace("-", "") == match[2])
        )
    else:
        filename = SEC_DIRECTORY_FILES.get(directory_kind)
        valid_source = (
            not submission
            and filename is not None
            and source_url == "https://www.sec.gov/files/" + filename
        )
    if (
        not valid_source
        or filename is None
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
    content_lengths: list[int | None] = []
    stage = "request"
    bytes_read_boundary: int | None = None

    async def record_status(response: httpx.Response) -> None:
        nonlocal stage
        stage = "response_read"
        statuses.append(response.status_code)
        value = response.headers.get("content-length")
        content_lengths.append(
            int(value) if value is not None and value.isascii() and value.isdecimal() else None
        )

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
        bytes_read_boundary = len(raw)
        stage = "archive_write"
        write_private(output_dir / filename, raw)
        receipt = dict(
            source_url=source_url,
            received_at=received.isoformat(),
            sha256=sha256(raw).hexdigest(),
            byte_length=len(raw),
            http_status=200,
        )
        write_private(output_dir / "receipt.json", json.dumps(receipt, sort_keys=True).encode())
    except Exception as error:
        codes = {
            "STATUS_REJECTED": "CURRENT_IDENTITY_SEC_STATUS_REJECTED",
            "REDIRECT_REJECTED": "CURRENT_IDENTITY_SEC_REDIRECT_REJECTED",
            "RESPONSE_TOO_LARGE": "CURRENT_IDENTITY_SEC_RESPONSE_TOO_LARGE",
            "DEADLINE_EXCEEDED": "CURRENT_IDENTITY_SEC_DEADLINE_EXCEEDED",
            "RETRY_BUDGET_EXHAUSTED": "CURRENT_IDENTITY_SEC_RETRY_BUDGET_EXHAUSTED",
        }
        code = (
            codes.get(str(error), "CURRENT_IDENTITY_FAILED")
            if isinstance(error, HttpPolicyError)
            else "CURRENT_IDENTITY_FAILED"
        )
        policy_content_length = getattr(error, "content_length", None)
        policy_bytes_read = getattr(error, "bytes_read_boundary", None)
        failure = {
            "stage": stage,
            "code": code,
            "http_status": statuses[-1] if statuses else None,
            "content_length": (
                policy_content_length
                if type(policy_content_length) is int
                else content_lengths[-1]
                if content_lengths
                else None
            ),
            "bytes_read_boundary": (
                policy_bytes_read if type(policy_bytes_read) is int else bytes_read_boundary
            ),
        }
        write_private(output_dir / "failure.json", json.dumps(failure, sort_keys=True).encode())
        if isinstance(error, HttpPolicyError):
            raise ValueError(code) from None
        raise
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
    taxonomy_archive: Path | None = None,
    store: IdentityStore | None = None,
) -> CurrentIdentityImportResult:
    _local(settings)
    storage = store or S3ProviderEvidenceStore(settings, disable_environment_proxies=True)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            result = await import_current_common_stock(
                session,
                storage,
                symbol=symbol,
                fmp_archive=fmp_archive,
                sec_archive=sec_archive,
                taxonomy_archive=taxonomy_archive,
            )
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await read_current_identity(session, storage, snapshot_id=result.snapshot_id)
        return result
    finally:
        await factory.kw["bind"].dispose()


async def import_held_identity(
    settings: EngineSettings,
    *,
    symbol: str,
    fmp_archive: Path,
    sec_archive: Path,
    store: IdentityStore | None = None,
    broker_settings: BrokerWorkerSettings | None = None,
    broker: FosunGatewayClient | None = None,
) -> CurrentIdentityImportResult:
    """Persist one source-proven current holding without widening research scope."""

    _local(settings)
    storage = store or S3ProviderEvidenceStore(settings, disable_environment_proxies=True)
    if broker is None and broker_settings is None:
        raise ValueError("CURRENT_HELD_COMMON_STOCK_GATEWAY_REQUIRED")
    if broker is None:
        assert broker_settings is not None
        gateway = FosunGatewayClient(broker_settings)
    else:
        gateway = broker
    owns_gateway = broker is None
    try:
        accounts_page = await gateway.read_accounts()
        accounts = tuple(AccountItem.model_validate(item) for item in accounts_page.items)
        if len(accounts) != 1 or accounts_page.quality != "PASS" or accounts_page.has_more:
            raise ValueError("CURRENT_HELD_COMMON_STOCK_HOLDING_INVALID")
        holdings_page = await gateway.read_holdings(
            accounts[0].external_account_id, cursor=None, limit=100
        )
    finally:
        if owns_gateway:
            await gateway.aclose()
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            result = await import_current_held_common_stock(
                session,
                storage,
                symbol=symbol,
                fmp_archive=fmp_archive,
                sec_archive=sec_archive,
                accounts_page=accounts_page,
                holdings_page=holdings_page,
            )
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await read_current_held_common_stock(session, storage, snapshot_id=result.snapshot_id)
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


async def read_held_identity(settings: EngineSettings, *, snapshot_id: UUID) -> None:
    _local(settings)
    storage = S3ProviderEvidenceStore(settings, disable_environment_proxies=True)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await read_current_held_common_stock(session, storage, snapshot_id=snapshot_id)
    finally:
        await factory.kw["bind"].dispose()


async def prepare_universe(
    settings: EngineSettings,
    *,
    effective_session: date,
    store: IdentityStore | None = None,
    use_listing_revisions: bool = False,
    use_etf_identities: bool = False,
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
                use_etf_identities=use_etf_identities,
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
    submission = commands.add_parser("collect-submission")
    submission.add_argument("--source-url", required=True)
    submission.add_argument("--output-dir", type=Path, required=True)
    directory = commands.add_parser("collect-sec-directory")
    directory.add_argument("--kind", choices=("company", "fund"), required=True)
    directory.add_argument("--output-dir", type=Path, required=True)
    listing = commands.add_parser("collect-cboe-listing")
    listing.add_argument("--symbol", required=True)
    listing.add_argument("--output-dir", type=Path, required=True)
    taxonomy = commands.add_parser("archive-taxonomy")
    taxonomy.add_argument("--symbol", required=True)
    taxonomy.add_argument("--fmp-archive", type=Path, required=True)
    taxonomy.add_argument("--sec-archive", type=Path, required=True)
    taxonomy.add_argument("--output-dir", type=Path, required=True)
    for name in ("schema", "definition", "label"):
        taxonomy.add_argument("--" + name, type=Path, required=True)
        taxonomy.add_argument("--" + name + "-receipt", type=Path, required=True)
    for action in ("import", "import-held"):
        importer = commands.add_parser(action)
        importer.add_argument("--symbol", required=True)
        importer.add_argument("--fmp-archive", type=Path, required=True)
        importer.add_argument("--sec-archive", type=Path, required=True)
        importer.add_argument("--output", type=Path, required=True)
        if action == "import":
            importer.add_argument("--taxonomy-archive", type=Path)
    revision = commands.add_parser("revise-listing")
    revision.add_argument("--symbol", required=True)
    revision.add_argument("--fmp-archive", required=True, type=Path)
    revision.add_argument("--sec-archive", required=True, type=Path)
    revision.add_argument("--output", required=True, type=Path)
    revision_reader = commands.add_parser("read-listing-revision")
    revision_reader.add_argument("--snapshot-id", required=True, type=UUID)
    prepare = commands.add_parser("prepare-universe")
    prepare.add_argument("--use-listing-revisions", action="store_true")
    prepare.add_argument("--use-etf-identities", action="store_true")
    prepare.add_argument("--effective-session", required=True, type=date.fromisoformat)
    prepare.add_argument("--output", required=True, type=Path)
    reader = commands.add_parser("read")
    reader.add_argument("--snapshot-id", type=UUID, required=True)
    held_reader = commands.add_parser("read-held")
    held_reader.add_argument("--snapshot-id", type=UUID, required=True)
    try:
        args = parser.parse_args()
        environment = load_local_environment(args.repo_root, os.environ)
        if args.action == "archive-taxonomy":
            archive_member_taxonomy(
                symbol=args.symbol,
                fmp_archive=args.fmp_archive,
                sec_archive=args.sec_archive,
                output_dir=args.output_dir,
                originals=tuple(
                    getattr(args, n + s)
                    for n in ("schema", "definition", "label")
                    for s in ("", "_receipt")
                ),
            )
            status = "archived"
        elif args.action == "collect-sec-directory":
            asyncio.run(
                collect_sec(
                    source_url="https://www.sec.gov/files/" + SEC_DIRECTORY_FILES[args.kind],
                    contact=environment.get("SEC_CONTACT", ""),
                    output_dir=args.output_dir,
                    directory_kind=args.kind,
                )
            )
            status = "collected"
        elif args.action == "collect-cboe-listing":
            asyncio.run(collect_listing(symbol=args.symbol, output_dir=args.output_dir))
            status = "collected"
        elif args.action in {"collect-sec", "collect-submission"}:
            asyncio.run(
                collect_sec(
                    source_url=args.source_url,
                    contact=environment.get("SEC_CONTACT", ""),
                    output_dir=args.output_dir,
                    submission=args.action == "collect-submission",
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
                        use_etf_identities=args.use_etf_identities,
                    )
                )
                write_private(args.output, json.dumps(prepared, sort_keys=True).encode())
                status = str(prepared["status"])
            elif args.action in {"import", "import-held", "revise-listing"}:
                if args.output.exists() or args.output.is_symlink():
                    raise ValueError("CURRENT_IDENTITY_NEW_OUTPUT_REQUIRED")
                if args.action == "revise-listing":
                    result = asyncio.run(
                        revise_listing(
                            settings,
                            symbol=args.symbol,
                            fmp_archive=args.fmp_archive,
                            sec_archive=args.sec_archive,
                        )
                    )
                elif args.action == "import-held":
                    result = asyncio.run(
                        import_held_identity(
                            settings,
                            symbol=args.symbol,
                            fmp_archive=args.fmp_archive,
                            sec_archive=args.sec_archive,
                            broker_settings=BrokerWorkerSettings.model_validate(environment),
                        )
                    )
                else:
                    result = asyncio.run(
                        import_identity(
                            settings,
                            symbol=args.symbol,
                            fmp_archive=args.fmp_archive,
                            sec_archive=args.sec_archive,
                            taxonomy_archive=args.taxonomy_archive,
                        )
                    )
                write_private(
                    args.output, json.dumps(asdict(result), default=str, sort_keys=True).encode()
                )
                status = result.status
            elif args.action == "read-listing-revision":
                asyncio.run(read_listing(settings, snapshot_id=args.snapshot_id))
                status = "verified"
            elif args.action == "read":
                asyncio.run(read_identity(settings, snapshot_id=args.snapshot_id))
                status = "verified"
            else:
                asyncio.run(read_held_identity(settings, snapshot_id=args.snapshot_id))
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
                r"(?:CURRENT_IDENTITY|CURRENT_LISTING|BACKTEST_UNIVERSE|CBOE_LISTING)_[A-Z_]+",
                str(error),
            )
            else "CURRENT_IDENTITY_FAILED"
        )
        print(json.dumps(dict(status="failed", code=code)))
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
