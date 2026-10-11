"""Offline import and strict readback of a first current equity ETF identity."""

import asyncio
import json
import os
import re
from dataclasses import asdict
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import BinaryIO, Protocol, cast
from uuid import UUID

import httpx
from sqlalchemy import text

from nexus.market.current_etf_identity import (
    MAX_SEC_BYTES,
    member_for,
    require_outside_fixed_etf_definition,
)
from nexus.market.current_etf_identity_storage import (
    _private_files,
    import_current_etf,
    import_current_held_etf,
    read_current_etf,
    read_current_held_etf,
)
from nexus.market.current_identity_storage import CurrentIdentityImportResult, IdentityStore
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops.fosun_local import load_local_environment
from nexus.ops.instrument_identity_local import _local, _Parser
from nexus.ops.local_recovery import write_private
from nexus.ops.sec_local import _BoundedBytes, _Budget
from nexus.persistence.session import session_factory
from nexus.providers.broker.fosun_contracts import AccountItem
from nexus.providers.broker.gateway_client import FosunGatewayClient
from nexus.providers.broker.settings import BrokerWorkerSettings
from nexus.providers.sec import SecCompanyFactsClient
from nexus.providers.sec_http import SecHttpTransport
from nexus.research.sec_funds import fetch_fund_disclosure, resolve_fund
from nexus.settings import EngineSettings


class _VersionListing(Protocol):
    def list_object_versions(self, **kwargs: object) -> dict[str, object]: ...


async def verify_mapping_origin(
    storage: S3ProviderEvidenceStore, *, mapping_file: Path, sec_archive: Path
) -> None:
    """Verify the retained directory's exact stored version and real object timestamp."""
    raw = _private_files(mapping_file.parent, (mapping_file.name,))[0]
    binding = json.loads(_private_files(sec_archive, ("mapping-binding.json",))[0])
    digest = sha256(raw).hexdigest()
    key = binding["key"]
    if (
        not re.fullmatch(
            re.escape(storage._prefix)
            + r"/raw/sec/[0-9]{4}-[0-9]{2}-[0-9]{2}/ticker-directory/"
            + digest
            + r"\.json",
            key,
        )
        or binding["sha256"] != digest
        or binding["original_received_at"] is not None
    ):
        raise ValueError("CURRENT_ETF_MAPPING_ORIGIN_INVALID")

    def fetch() -> tuple[bytes, datetime, str]:
        response = storage._client.get_object(
            Bucket=storage._bucket, Key=key, VersionId=binding["version_id"]
        )
        body = cast(BinaryIO, response["Body"])
        try:
            content = body.read(MAX_SEC_BYTES + 1)
        finally:
            body.close()
        version = response["VersionId"]
        # GetObject's HTTP-date drops subsecond precision; version listing preserves it.
        listing = cast(_VersionListing, storage._client).list_object_versions(
            Bucket=storage._bucket, Prefix=key, MaxKeys=1000
        )
        versions = listing.get("Versions", [])
        if listing.get("IsTruncated") or not isinstance(versions, list):
            raise ValueError("CURRENT_ETF_MAPPING_ORIGIN_INVALID")
        matches = [
            v
            for v in versions
            if isinstance(v, dict) and v.get("Key") == key and v.get("VersionId") == version
        ]
        if len(matches) != 1:
            raise ValueError("CURRENT_ETF_MAPPING_ORIGIN_INVALID")
        stored = matches[0]["LastModified"]
        if not isinstance(stored, datetime) or not isinstance(version, str):
            raise ValueError("CURRENT_ETF_MAPPING_ORIGIN_INVALID")
        return content, stored, version

    content, stored_at, version = await asyncio.to_thread(fetch)
    if (
        content != raw
        or version != binding["version_id"]
        or stored_at != datetime.fromisoformat(binding["stored_at"])
    ):
        raise ValueError("CURRENT_ETF_MAPPING_ORIGIN_INVALID")


async def collect_sec(
    settings: EngineSettings,
    *,
    symbol: str,
    accession: str,
    contact: str,
    mapping_file: Path,
    mapping_archive: Path,
    output_dir: Path,
    current_holding_only: bool = False,
) -> None:
    _local(settings)
    if current_holding_only:
        require_outside_fixed_etf_definition(symbol)
    else:
        member_for(symbol)
    storage = S3ProviderEvidenceStore(settings, disable_environment_proxies=True)
    await verify_mapping_origin(storage, mapping_file=mapping_file, sec_archive=mapping_archive)
    mapping = _private_files(mapping_file.parent, (mapping_file.name,))[0]
    identity = resolve_fund(json.loads(mapping), symbol)
    binding = _private_files(mapping_archive, ("mapping-binding.json",))[0]
    output_dir.mkdir(mode=0o700, exist_ok=False)
    write_private(output_dir / "mapping-binding.json", binding)
    receipts: list[dict[str, object]] = []

    async def response_hook(response: httpx.Response) -> None:
        raw = await response.aread()
        if len(raw) > MAX_SEC_BYTES:
            raise ValueError("CURRENT_ETF_RESPONSE_TOO_LARGE")
        filename = f"response-{len(receipts) + 1}.raw"
        write_private(output_dir / filename, raw)
        receipts.append(
            dict(
                url=str(response.url),
                status=response.status_code,
                received_at=datetime.now(UTC).isoformat(),
                sha256=sha256(raw).hexdigest(),
                file=filename,
            )
        )
        # Each response has its own receipt; failures keep collected originals, never retry.
        write_private(
            output_dir / f"receipt-{len(receipts)}.json", json.dumps(receipts[-1]).encode()
        )
        if response.status_code != 200:
            raise ValueError("CURRENT_ETF_HTTP_FAILURE")

    budget = _Budget(3)
    async with httpx.AsyncClient(
        follow_redirects=False, timeout=30, event_hooks={"response": [response_hook]}
    ) as client:
        async with asyncio.timeout(100):
            _, originals = await fetch_fund_disclosure(
                instrument_id=UUID(int=0),
                symbol=symbol,
                identity=identity,
                accession=accession,
                client=SecCompanyFactsClient(
                    SecHttpTransport(_BoundedBytes(client, "data.sec.gov", budget)),
                    contact=contact,
                    max_attempts=1,
                ),
                directory=_BoundedBytes(client, "www.sec.gov", budget),
                contact=contact,
            )
            for name, raw in originals:
                write_private(output_dir / (name + ".raw"), raw)
            write_private(output_dir / "receipts.json", json.dumps(receipts).encode())


async def import_identity(
    settings: EngineSettings,
    *,
    symbol: str,
    fmp_archive: Path,
    sec_archive: Path,
    mapping_file: Path | None,
    store: IdentityStore | None = None,
    allow_treasury: bool = False,
    allow_gold: bool = False,
) -> CurrentIdentityImportResult:
    _local(settings)
    storage = store or S3ProviderEvidenceStore(settings, disable_environment_proxies=True)
    if store is None and mapping_file is not None:
        assert isinstance(storage, S3ProviderEvidenceStore)
        await verify_mapping_origin(storage, mapping_file=mapping_file, sec_archive=sec_archive)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            result = await import_current_etf(
                session,
                storage,
                symbol=symbol,
                fmp_archive=fmp_archive,
                sec_archive=sec_archive,
                mapping_file=mapping_file,
                allow_treasury=allow_treasury,
                allow_gold=allow_gold,
            )
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await read_current_etf(session, storage, snapshot_id=result.snapshot_id)
        return result
    finally:
        await factory.kw["bind"].dispose()


async def import_held_identity(
    settings: EngineSettings,
    broker_settings: BrokerWorkerSettings,
    *,
    symbol: str,
    fmp_archive: Path,
    sec_archive: Path,
    mapping_file: Path,
    store: IdentityStore | None = None,
    broker: FosunGatewayClient | None = None,
) -> CurrentIdentityImportResult:
    """Read one complete holdings page, then persist only the requested held ETF identity."""

    _local(settings)
    storage = store or S3ProviderEvidenceStore(settings, disable_environment_proxies=True)
    if store is None:
        assert isinstance(storage, S3ProviderEvidenceStore)
        await verify_mapping_origin(storage, mapping_file=mapping_file, sec_archive=sec_archive)
    gateway = broker or FosunGatewayClient(broker_settings)
    owns_gateway = broker is None
    try:
        accounts_page = await gateway.read_accounts()
        try:
            accounts = tuple(AccountItem.model_validate(item) for item in accounts_page.items)
            if len(accounts) != 1 or accounts_page.quality != "PASS" or accounts_page.has_more:
                raise ValueError
        except (ValueError, TypeError, KeyError):
            raise ValueError("CURRENT_HELD_ETF_HOLDING_INVALID") from None
        holdings_page = await gateway.read_holdings(
            accounts[0].external_account_id,
            cursor=None,
            limit=100,
        )
    finally:
        if owns_gateway:
            await gateway.aclose()

    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            result = await import_current_held_etf(
                session,
                storage,
                symbol=symbol,
                fmp_archive=fmp_archive,
                sec_archive=sec_archive,
                mapping_file=mapping_file,
                accounts_page=accounts_page,
                holdings_page=holdings_page,
            )
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await read_current_held_etf(session, storage, snapshot_id=result.snapshot_id)
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
            await read_current_etf(session, storage, snapshot_id=snapshot_id)
    finally:
        await factory.kw["bind"].dispose()


async def read_held_identity(settings: EngineSettings, *, snapshot_id: UUID) -> None:
    _local(settings)
    storage = S3ProviderEvidenceStore(settings, disable_environment_proxies=True)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await read_current_held_etf(session, storage, snapshot_id=snapshot_id)
    finally:
        await factory.kw["bind"].dispose()


def main() -> None:
    parser = _Parser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    commands = parser.add_subparsers(dest="action", required=True)
    collector = commands.add_parser("collect-sec")
    for name in ("mapping-file", "mapping-archive", "output-dir"):
        collector.add_argument("--" + name, type=Path, required=True)
    collector.add_argument("--symbol", required=True)
    collector.add_argument("--accession", required=True)
    held_collector = commands.add_parser("collect-held-sec")
    for name in ("mapping-file", "mapping-archive", "output-dir"):
        held_collector.add_argument("--" + name, type=Path, required=True)
    held_collector.add_argument("--symbol", required=True)
    held_collector.add_argument("--accession", required=True)
    importer = commands.add_parser("import")
    for name in ("fmp-archive", "sec-archive", "mapping-file", "output"):
        importer.add_argument("--" + name, type=Path, required=True)
    importer.add_argument("--symbol", required=True)
    held_importer = commands.add_parser("import-held")
    for name in ("fmp-archive", "sec-archive", "mapping-file", "output"):
        held_importer.add_argument("--" + name, type=Path, required=True)
    held_importer.add_argument("--symbol", required=True)
    cboe_importer = commands.add_parser("import-held-cboe")
    for name in (
        "fmp-archive",
        "submission-file",
        "mic-archive",
        "listing-archive",
        "company-directory-file",
        "company-receipt-file",
        "fund-directory-file",
        "fund-receipt-file",
        "output",
    ):
        cboe_importer.add_argument("--" + name, type=Path, required=True)
    cboe_importer.add_argument("--previous-mic-archive", type=Path)
    cboe_importer.add_argument("--symbol", required=True)
    uit = commands.add_parser("import-uit")
    for name in ("fmp-archive", "sec-archive", "output"):
        uit.add_argument("--" + name, type=Path, required=True)
    uit.add_argument("--symbol", required=True)
    uit.set_defaults(mapping_file=None)
    treasury = commands.add_parser("import-treasury")
    for name in ("fmp-archive", "sec-archive", "mapping-file", "output"):
        treasury.add_argument("--" + name, type=Path, required=True)
    treasury.add_argument("--symbol", required=True)
    gold = commands.add_parser("import-gold")
    for name in ("fmp-archive", "sec-archive", "output"):
        gold.add_argument("--" + name, type=Path, required=True)
    gold.add_argument("--symbol", required=True)
    gold.set_defaults(mapping_file=None)
    reader = commands.add_parser("read")
    reader.add_argument("--snapshot-id", type=UUID, required=True)
    held_reader = commands.add_parser("read-held")
    held_reader.add_argument("--snapshot-id", type=UUID, required=True)
    cboe_reader = commands.add_parser("read-held-cboe")
    cboe_reader.add_argument("--snapshot-id", type=UUID, required=True)
    try:
        args = parser.parse_args()
        environment = load_local_environment(args.repo_root, os.environ)
        settings = EngineSettings.model_validate(environment)
        if args.action in {"collect-sec", "collect-held-sec"}:
            asyncio.run(
                collect_sec(
                    settings,
                    symbol=args.symbol,
                    accession=args.accession,
                    contact=environment.get("SEC_CONTACT", ""),
                    mapping_file=args.mapping_file,
                    mapping_archive=args.mapping_archive,
                    output_dir=args.output_dir,
                    current_holding_only=args.action == "collect-held-sec",
                )
            )
            status: str = "collected"
        elif args.action in {
            "import",
            "import-held",
            "import-uit",
            "import-treasury",
            "import-gold",
            "import-held-cboe",
        }:
            if args.output.exists() or args.output.is_symlink():
                raise ValueError("CURRENT_ETF_NEW_OUTPUT_REQUIRED")
            if args.action == "import-held-cboe":
                from nexus.ops.cboe_identity_local import import_held_identity as import_cboe

                result = asyncio.run(
                    import_cboe(
                        settings,
                        BrokerWorkerSettings.model_validate(environment),
                        symbol=args.symbol,
                        fmp_archive=args.fmp_archive,
                        submission_file=args.submission_file,
                        mic_archive=args.mic_archive,
                        listing_archive=args.listing_archive,
                        company_directory_file=args.company_directory_file,
                        company_receipt_file=args.company_receipt_file,
                        fund_directory_file=args.fund_directory_file,
                        fund_receipt_file=args.fund_receipt_file,
                        previous_mic_archive=args.previous_mic_archive,
                    )
                )
            elif args.action == "import-held":
                result = asyncio.run(
                    import_held_identity(
                        settings,
                        BrokerWorkerSettings.model_validate(environment),
                        symbol=args.symbol,
                        fmp_archive=args.fmp_archive,
                        sec_archive=args.sec_archive,
                        mapping_file=args.mapping_file,
                    )
                )
            else:
                result = asyncio.run(
                    import_identity(
                        settings,
                        symbol=args.symbol,
                        fmp_archive=args.fmp_archive,
                        sec_archive=args.sec_archive,
                        mapping_file=args.mapping_file,
                        allow_treasury=args.action == "import-treasury",
                        allow_gold=args.action == "import-gold",
                    )
                )
            write_private(
                args.output, json.dumps(asdict(result), default=str, sort_keys=True).encode()
            )
            status = result.status
        elif args.action == "read-held-cboe":
            from nexus.ops.cboe_identity_local import read_identity as read_cboe

            asyncio.run(read_cboe(settings, snapshot_id=args.snapshot_id))
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
                    historical_backtest_eligible=False,
                    universe_published=False,
                    execution_locked=True,
                )
            )
        )
    except Exception as error:
        code = (
            str(error)
            if isinstance(error, ValueError)
            and re.fullmatch(r"(?:CURRENT_ETF|CBOE)_[A-Z_]+", str(error))
            else "CURRENT_ETF_FAILED"
        )
        print(json.dumps(dict(status="failed", code=code)))
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
