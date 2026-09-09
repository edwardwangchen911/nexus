"""Collect, import, and verify one current-held SEC-proven ETN identity."""

import asyncio
import json
import os
import re
from dataclasses import asdict
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import cast
from uuid import UUID

import httpx
from sqlalchemy import text

from nexus.market.current_etn_identity import validate_current_held_etn_originals
from nexus.market.current_etn_identity_storage import (
    import_current_held_etn,
    read_current_held_etn,
)
from nexus.market.current_identity_storage import CurrentIdentityImportResult
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops.fosun_local import load_local_environment
from nexus.ops.instrument_identity_local import _local, _Parser
from nexus.ops.local_recovery import write_private
from nexus.ops.sec_local import _BoundedBytes, _Budget
from nexus.persistence.session import session_factory
from nexus.providers.broker.fosun_contracts import AccountItem
from nexus.providers.broker.gateway_client import FosunGatewayClient
from nexus.providers.broker.settings import BrokerWorkerSettings
from nexus.settings import EngineSettings


def _locator(path: Path) -> dict[str, object]:
    if path.is_symlink() or path.stat().st_mode & 0o077:
        raise ValueError("CURRENT_ETN_LOCATOR_INVALID")
    value = json.loads(path.read_bytes())
    if set(value) != {
        "cik",
        "accession",
        "primary_document",
        "filing_date",
        "document_url",
        "index_url",
        "directory_match",
    }:
        raise ValueError("CURRENT_ETN_LOCATOR_INVALID")
    cik = value["cik"]
    accession = value["accession"]
    primary = value["primary_document"]
    if (
        not isinstance(cik, str)
        or re.fullmatch(r"[0-9]{10}", cik) is None
        or not isinstance(accession, str)
        or re.fullmatch(r"[0-9]{10}-[0-9]{2}-[0-9]{6}", accession) is None
        or not isinstance(primary, str)
        or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*\.html?", primary) is None
        or value["directory_match"] is not False
    ):
        raise ValueError("CURRENT_ETN_LOCATOR_INVALID")
    compact = accession.replace("-", "")
    base = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{compact}/"
    if (
        value["document_url"] != base + primary
        or value["index_url"] != base + accession + "-index.html"
    ):
        raise ValueError("CURRENT_ETN_LOCATOR_INVALID")
    return cast(dict[str, object], value)


async def collect_sec(
    settings: EngineSettings,
    *,
    symbol: str,
    locator_file: Path,
    output_dir: Path,
    contact: str,
) -> None:
    _local(settings)
    locator = _locator(locator_file)
    if not contact or "@" not in contact or contact != contact.strip():
        raise ValueError("CURRENT_ETN_CONTACT_REQUIRED")
    output_dir.mkdir(mode=0o700, exist_ok=False)
    headers = (("User-Agent", f"NEXUS {contact}"), ("Accept-Encoding", "gzip, deflate"))
    cik = str(locator["cik"])
    accession = str(locator["accession"])
    compact = accession.replace("-", "")
    primary = str(locator["primary_document"])
    requests = (
        (
            "submissions.raw",
            "https://data.sec.gov/submissions/CIK" + cik + ".json",
            "data.sec.gov",
            ("submissions", "CIK" + cik + ".json"),
        ),
        (
            "filing-index.raw",
            str(locator["index_url"]),
            "www.sec.gov",
            ("Archives", "edgar", "data", str(int(cik)), compact, accession + "-index.html"),
        ),
        (
            "disclosure.raw",
            str(locator["document_url"]),
            "www.sec.gov",
            ("Archives", "edgar", "data", str(int(cik)), compact, primary),
        ),
    )
    receipts: list[dict[str, object]] = []
    budget = _Budget(3)
    async with httpx.AsyncClient(follow_redirects=False, timeout=30) as client:
        async with asyncio.timeout(100):
            for filename, url, host, segments in requests:
                raw = await _BoundedBytes(client, host, budget).get_bytes(segments, (), headers)
                received = datetime.now(UTC).isoformat()
                write_private(output_dir / filename, raw)
                receipts.append(
                    dict(
                        source_url=url,
                        received_at=received,
                        sha256=sha256(raw).hexdigest(),
                        byte_length=len(raw),
                        http_status=200,
                    )
                )
                write_private(
                    output_dir / f"receipt-{len(receipts)}.json",
                    json.dumps(receipts[-1]).encode(),
                )
    write_private(output_dir / "receipts.json", json.dumps(receipts).encode())
    validate_current_held_etn_originals(
        symbol=symbol,
        contents=(
            (output_dir / "submissions.raw").read_bytes(),
            (output_dir / "filing-index.raw").read_bytes(),
            (output_dir / "disclosure.raw").read_bytes(),
            (output_dir / "receipts.json").read_bytes(),
        ),
        now=datetime.now(UTC),
    )


async def import_identity(
    settings: EngineSettings,
    broker_settings: BrokerWorkerSettings,
    *,
    symbol: str,
    sec_archive: Path,
) -> CurrentIdentityImportResult:
    _local(settings)
    storage = S3ProviderEvidenceStore(settings, disable_environment_proxies=True)
    gateway = FosunGatewayClient(broker_settings)
    try:
        accounts_page = await gateway.read_accounts()
        accounts = tuple(AccountItem.model_validate(item) for item in accounts_page.items)
        if len(accounts) != 1 or accounts_page.quality != "PASS" or accounts_page.has_more:
            raise ValueError("CURRENT_HELD_ETN_HOLDING_INVALID")
        holdings_page = await gateway.read_holdings(
            accounts[0].external_account_id, cursor=None, limit=100
        )
    finally:
        await gateway.aclose()
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            result = await import_current_held_etn(
                session,
                storage,
                symbol=symbol,
                sec_archive=sec_archive,
                accounts_page=accounts_page,
                holdings_page=holdings_page,
            )
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await read_current_held_etn(session, storage, snapshot_id=result.snapshot_id)
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
            await read_current_held_etn(session, storage, snapshot_id=snapshot_id)
    finally:
        await factory.kw["bind"].dispose()


def main() -> None:
    parser = _Parser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    commands = parser.add_subparsers(dest="action", required=True)
    collector = commands.add_parser("collect-sec")
    collector.add_argument("--symbol", required=True)
    collector.add_argument("--locator-file", type=Path, required=True)
    collector.add_argument("--output-dir", type=Path, required=True)
    importer = commands.add_parser("import-held")
    importer.add_argument("--symbol", required=True)
    importer.add_argument("--sec-archive", type=Path, required=True)
    importer.add_argument("--output", type=Path, required=True)
    reader = commands.add_parser("read-held")
    reader.add_argument("--snapshot-id", type=UUID, required=True)
    try:
        args = parser.parse_args()
        environment = load_local_environment(args.repo_root, os.environ)
        settings = EngineSettings.model_validate(environment)
        if args.action == "collect-sec":
            asyncio.run(
                collect_sec(
                    settings,
                    symbol=args.symbol,
                    locator_file=args.locator_file,
                    output_dir=args.output_dir,
                    contact=environment.get("SEC_CONTACT", ""),
                )
            )
            status = "collected"
        elif args.action == "import-held":
            if args.output.exists() or args.output.is_symlink():
                raise ValueError("CURRENT_ETN_NEW_OUTPUT_REQUIRED")
            result = asyncio.run(
                import_identity(
                    settings,
                    BrokerWorkerSettings.model_validate(environment),
                    symbol=args.symbol,
                    sec_archive=args.sec_archive,
                )
            )
            write_private(
                args.output, json.dumps(asdict(result), default=str, sort_keys=True).encode()
            )
            status = result.status
        else:
            asyncio.run(read_identity(settings, snapshot_id=args.snapshot_id))
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
            and re.fullmatch(r"CURRENT_(?:HELD_)?ETN_[A-Z_]+", str(error))
            else "CURRENT_ETN_FAILED"
        )
        print(json.dumps(dict(status="failed", code=code)))
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
