"""Normal local ingress for source-bound, current-held Cboe fund identity."""

import os
import stat
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from sqlalchemy import text

from nexus.market.cboe_fund_sources import validate_cboe_fund_sources
from nexus.market.cboe_held_identity_storage import (
    import_cboe_held_identity,
    read_cboe_held_identity,
)
from nexus.market.current_identity_storage import CurrentIdentityImportResult, IdentityStore
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops.instrument_identity_local import _local
from nexus.persistence.session import session_factory
from nexus.providers.broker.fosun_contracts import AccountItem
from nexus.providers.broker.gateway_client import FosunGatewayClient
from nexus.providers.broker.settings import BrokerWorkerSettings
from nexus.settings import EngineSettings


def _file(path: Path, limit: int) -> bytes:
    directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        info = os.fstat(directory_fd)
        if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
            raise ValueError
        fd = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd)
        with os.fdopen(fd, "rb") as handle:
            info = os.fstat(handle.fileno())
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600
                or not 0 < info.st_size <= limit
            ):
                raise ValueError
            content = handle.read(limit + 1)
            if len(content) != info.st_size:
                raise ValueError
            return content
    finally:
        os.close(directory_fd)


def load_archives(
    *,
    fmp_archive: Path,
    submission_file: Path,
    mic_archive: Path,
    listing_archive: Path,
    company_directory_file: Path,
    company_receipt_file: Path,
    fund_directory_file: Path,
    fund_receipt_file: Path,
    previous_mic_archive: Path | None = None,
) -> tuple[tuple[bytes, ...], tuple[bytes, ...]]:
    try:
        files = [
            fmp_archive / "profile.json",
            fmp_archive / "receipt.json",
            submission_file,
            submission_file.parent / "receipt.json",
        ]
        mic_names = (
            "publication.html",
            "publication.html.receipt.json",
            "ISO10383_MIC.csv",
            "ISO10383_MIC.csv.receipt.json",
        )
        files.extend(mic_archive / name for name in mic_names)
        files.extend(
            listing_archive / name
            for name in (
                "listed.html",
                "listed.html.receipt.json",
                "listed.csv",
                "listed.csv.receipt.json",
                "product.html",
                "product.html.receipt.json",
            )
        )
        if previous_mic_archive is not None:
            files.extend(previous_mic_archive / name for name in mic_names)
        contents = tuple(_file(p, 8192 if n % 2 else 8 * 1024 * 1024) for n, p in enumerate(files))
        directories = tuple(
            _file(p, 8192 if n % 2 else 8 * 1024 * 1024)
            for n, p in enumerate(
                (
                    company_directory_file,
                    company_receipt_file,
                    fund_directory_file,
                    fund_receipt_file,
                )
            )
        )
        return contents, directories
    except (OSError, ValueError, TypeError):
        raise ValueError("CBOE_HELD_ARCHIVE_INVALID") from None


async def import_held_identity(
    settings: EngineSettings,
    broker_settings: BrokerWorkerSettings,
    *,
    symbol: str,
    fmp_archive: Path,
    submission_file: Path,
    mic_archive: Path,
    listing_archive: Path,
    company_directory_file: Path,
    company_receipt_file: Path,
    fund_directory_file: Path,
    fund_receipt_file: Path,
    previous_mic_archive: Path | None = None,
    store: IdentityStore | None = None,
    broker: FosunGatewayClient | None = None,
) -> CurrentIdentityImportResult:
    _local(settings)
    contents, directories = load_archives(
        fmp_archive=fmp_archive,
        submission_file=submission_file,
        mic_archive=mic_archive,
        listing_archive=listing_archive,
        company_directory_file=company_directory_file,
        company_receipt_file=company_receipt_file,
        fund_directory_file=fund_directory_file,
        fund_receipt_file=fund_receipt_file,
        previous_mic_archive=previous_mic_archive,
    )
    # Refuse invalid or stale public sources before any broker call or DB write.
    validate_cboe_fund_sources(
        symbol=symbol, contents=contents, directory_contents=directories, now=datetime.now(UTC)
    )
    gateway = broker or FosunGatewayClient(broker_settings)
    try:
        accounts_page = await gateway.read_accounts()
        try:
            accounts = tuple(AccountItem.model_validate(item) for item in accounts_page.items)
            if len(accounts) != 1 or accounts_page.quality != "PASS" or accounts_page.has_more:
                raise ValueError
        except (ValueError, TypeError, KeyError):
            raise ValueError("CBOE_HELD_BROKER_SCOPE_INVALID") from None
        holdings_page = await gateway.read_holdings(
            accounts[0].external_account_id, cursor=None, limit=100
        )
    finally:
        if broker is None:
            await gateway.aclose()
    storage = store or S3ProviderEvidenceStore(settings, disable_environment_proxies=True)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            result = await import_cboe_held_identity(
                session,
                storage,
                symbol=symbol,
                contents=contents,
                directory_contents=directories,
                accounts_page=accounts_page,
                holdings_page=holdings_page,
            )
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await read_cboe_held_identity(session, storage, snapshot_id=result.snapshot_id)
        return result
    finally:
        await factory.kw["bind"].dispose()


async def read_identity(settings: EngineSettings, *, snapshot_id: "UUID") -> None:
    _local(settings)
    storage = S3ProviderEvidenceStore(settings, disable_environment_proxies=True)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await read_cboe_held_identity(session, storage, snapshot_id=snapshot_id)
    finally:
        await factory.kw["bind"].dispose()
