"""Resolve persisted pending instrument discoveries through bounded public evidence."""

import argparse
import asyncio
import json
import os
import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, NoReturn, Protocol, cast
from uuid import UUID

import httpx
from pydantic import SecretStr
from sqlalchemy import text

from nexus.domain.time import require_utc
from nexus.market.current_etf_identity_storage import (
    import_current_held_etf,
    read_current_held_etf,
)
from nexus.market.current_identity import _EXCHANGES, cik
from nexus.market.current_identity_storage import (
    IdentityStore,
    import_current_held_common_stock,
    read_current_held_common_stock,
)
from nexus.market.fosun_instrument_discovery_storage import (
    read_pending_instrument_discoveries,
)
from nexus.market.object_store import _version_id_from_frozen_uri
from nexus.ops.fmp_profile_local import collect_profile
from nexus.ops.instrument_identity_local import collect_sec
from nexus.ops.local_recovery import write_private
from nexus.ops.sec_local import _BoundedBytes, _Budget
from nexus.persistence.session import session_factory
from nexus.providers.broker.fosun_contracts import AccountItem
from nexus.providers.broker.gateway_client import FosunGatewayClient
from nexus.providers.broker.settings import BrokerWorkerSettings
from nexus.providers.fmp_profile import parse_profile, validate_symbol
from nexus.research.sec_disclosures import disclosure_excerpt
from nexus.research.sec_funds import resolve_fund
from nexus.settings import EngineSettings

_ACCESSION = re.compile(r"[0-9]{10}-[0-9]{2}-[0-9]{6}")
_PRIMARY_DOCUMENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\.html?")


class _IndexLinks(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: set[str] = set()

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        href = dict(attrs).get("href")
        if tag == "a" and href:
            self.links.add(href)


@dataclass(frozen=True)
class CommonStockFilingSelection:
    cik: str
    exchange: str
    accession: str
    primary_document: str
    source_url: str


@dataclass(frozen=True)
class FundFilingSelection:
    cik: str
    series_id: str
    class_id: str
    accession: str
    primary_document: str


@dataclass(frozen=True)
class CollectedIdentityEvidence:
    fmp_archive: Path
    sec_archive: Path
    request_count: int
    mapping_file: Path | None = None


def _load_existing_evidence(item_dir: Path, *, symbol: str, kind: str) -> CollectedIdentityEvidence:
    """Reuse only a complete protected archive from an interrupted resolver run."""

    directories = (item_dir, item_dir / "fmp", item_dir / "sec", item_dir / "selectors")
    if any(
        path.is_symlink() or not path.is_dir() or path.stat().st_mode & 0o077
        for path in directories
    ):
        raise ValueError("PENDING_IDENTITY_RESUME_INVALID")
    selection_path = item_dir / "selectors/selection.json"
    if selection_path.is_symlink() or not selection_path.is_file():
        raise ValueError("PENDING_IDENTITY_RESUME_INVALID")
    selection = _json_object(selection_path.read_bytes(), "PENDING_IDENTITY_RESUME_INVALID")
    expected_schema = (
        "pending_common_stock_selection_v1"
        if kind == "common_stock"
        else "pending_fund_selection_v1"
    )
    if selection.get("schema") != expected_schema or selection.get("symbol") != symbol:
        raise ValueError("PENDING_IDENTITY_RESUME_INVALID")
    common_files = (
        item_dir / "fmp/profile.json",
        item_dir / "fmp/receipt.json",
        selection_path,
        item_dir / "selectors/submissions.json",
    )
    required: tuple[Path, ...]
    if kind == "common_stock":
        primary_document = selection.get("primary_document")
        if (
            not isinstance(primary_document, str)
            or _PRIMARY_DOCUMENT.fullmatch(primary_document) is None
        ):
            raise ValueError("PENDING_IDENTITY_RESUME_INVALID")
        required = common_files + (
            item_dir / "selectors/company_tickers_exchange.json",
            item_dir / "sec/receipt.json",
            item_dir / "sec" / primary_document,
        )
        mapping_file = None
    elif kind == "equity_etf":
        required = common_files + (
            item_dir / "selectors/company_tickers_mf.json",
            item_dir / "selectors/class-search.html",
            item_dir / "sec/submissions.raw",
            item_dir / "sec/filing-index.raw",
            item_dir / "sec/disclosure.raw",
            item_dir / "sec/receipts.json",
        )
        # Collection can finish before a later batch item fails. The resolver
        # binds these originals before import; that binding is not a collection
        # prerequisite. An existing binding must still pass the private-file guard.
        binding_file = item_dir / "sec/mapping-binding.json"
        if binding_file.exists() or binding_file.is_symlink():
            required += (binding_file,)
        mapping_file = item_dir / "selectors/company_tickers_mf.json"
    else:
        raise ValueError("PENDING_IDENTITY_RESUME_INVALID")
    if any(
        path.is_symlink()
        or not path.is_file()
        or path.stat().st_mode & 0o077
        or path.stat().st_size == 0
        for path in required
    ):
        raise ValueError("PENDING_IDENTITY_RESUME_INVALID")
    return CollectedIdentityEvidence(
        item_dir / "fmp",
        item_dir / "sec",
        0,
        mapping_file,
    )


class _MappingVersionClient(Protocol):
    def list_object_versions(self, **kwargs: object) -> Mapping[str, object]: ...


class _FundMappingStore(Protocol):
    _bucket: str
    _prefix: str
    _client: _MappingVersionClient

    async def put_if_absent(self, *, key: str, content: bytes) -> str: ...


class _ResolverStore(IdentityStore, _FundMappingStore, Protocol):
    pass


async def collect_common_stock_evidence(
    *,
    symbol: str,
    fmp_key: SecretStr,
    sec_contact: str,
    output_dir: Path,
    client: httpx.AsyncClient,
) -> CollectedIdentityEvidence:
    validate_symbol(symbol)
    if (
        not sec_contact
        or sec_contact != sec_contact.strip()
        or "@" not in sec_contact
        or any(ord(character) < 32 for character in sec_contact)
    ):
        raise ValueError("PENDING_IDENTITY_SEC_CONTACT_REQUIRED")
    output_dir.mkdir(mode=0o700, exist_ok=False)
    fmp_archive = output_dir / "fmp"
    sec_archive = output_dir / "sec"
    selectors = output_dir / "selectors"
    selectors.mkdir(mode=0o700)
    await collect_profile(symbol=symbol, key=fmp_key, output_dir=fmp_archive, client=client)

    budget = _Budget(2)
    headers = (("User-Agent", f"NEXUS {sec_contact}"), ("Accept-Encoding", "gzip, deflate"))
    mapping_raw = await _BoundedBytes(client, "www.sec.gov", budget).get_bytes(
        ("files", "company_tickers_exchange.json"), (), headers
    )
    mapping = _json_object(mapping_raw, "PENDING_IDENTITY_COMMON_MAPPING_UNAVAILABLE")
    try:
        rows = mapping.get("data")
        if not isinstance(rows, list):
            raise ValueError
        matches = [
            row for row in rows if isinstance(row, list) and len(row) == 4 and row[2] == symbol
        ]
        if len(matches) != 1 or type(matches[0][0]) is not int:
            raise ValueError
        selected_cik = str(matches[0][0]).zfill(10)
    except (TypeError, ValueError):
        raise ValueError("PENDING_IDENTITY_COMMON_MAPPING_UNAVAILABLE") from None
    submissions_raw = await _BoundedBytes(client, "data.sec.gov", budget).get_bytes(
        ("submissions", f"CIK{selected_cik}.json"), (), headers
    )
    selected = select_common_stock_filing(
        mapping_raw=mapping_raw,
        submissions_raw=submissions_raw,
        symbol=symbol,
        received_at=datetime.now(UTC),
    )
    row = parse_profile((fmp_archive / "profile.json").read_bytes(), symbol)
    if (
        cik(row.get("cik")) != selected.cik
        or row.get("currency") != "USD"
        or row.get("isEtf") is not False
        or _EXCHANGES.get(row.get("exchange", "")) != selected.exchange
    ):
        raise ValueError("PENDING_IDENTITY_COMMON_SOURCE_CONFLICT")
    write_private(selectors / "company_tickers_exchange.json", mapping_raw)
    write_private(selectors / "submissions.json", submissions_raw)
    write_private(
        selectors / "selection.json",
        json.dumps(
            {
                "schema": "pending_common_stock_selection_v1",
                "symbol": symbol,
                "cik": selected.cik,
                "exchange": selected.exchange,
                "accession": selected.accession,
                "primary_document": selected.primary_document,
                "source_url": selected.source_url,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode(),
    )
    await collect_sec(
        source_url=selected.source_url,
        contact=sec_contact,
        output_dir=sec_archive,
        client=client,
    )
    return CollectedIdentityEvidence(fmp_archive, sec_archive, 4)


async def collect_fund_evidence(
    *,
    symbol: str,
    fmp_key: SecretStr,
    sec_contact: str,
    output_dir: Path,
    client: httpx.AsyncClient,
) -> CollectedIdentityEvidence:
    validate_symbol(symbol)
    if (
        not sec_contact
        or sec_contact != sec_contact.strip()
        or "@" not in sec_contact
        or any(ord(character) < 32 for character in sec_contact)
    ):
        raise ValueError("PENDING_IDENTITY_SEC_CONTACT_REQUIRED")
    output_dir.mkdir(mode=0o700, exist_ok=False)
    fmp_archive = output_dir / "fmp"
    sec_archive = output_dir / "sec"
    selectors = output_dir / "selectors"
    sec_archive.mkdir(mode=0o700)
    selectors.mkdir(mode=0o700)
    await collect_profile(symbol=symbol, key=fmp_key, output_dir=fmp_archive, client=client)

    budget = _Budget(5)
    headers = (("User-Agent", f"NEXUS {sec_contact}"), ("Accept-Encoding", "gzip, deflate"))
    directory = _BoundedBytes(client, "www.sec.gov", budget)
    mapping_raw = await directory.get_bytes(("files", "company_tickers_mf.json"), (), headers)
    try:
        selected_cik, series_id, class_id = resolve_fund(
            _json_object(mapping_raw, "PENDING_IDENTITY_FUND_MAPPING_UNAVAILABLE"), symbol
        )
    except ValueError:
        raise ValueError("PENDING_IDENTITY_FUND_MAPPING_UNAVAILABLE") from None
    mapping_file = selectors / "company_tickers_mf.json"
    write_private(mapping_file, mapping_raw)
    class_search_raw = await directory.get_bytes(
        ("cgi-bin", "browse-edgar"),
        (
            ("CIK", class_id),
            ("action", "getcompany"),
            ("type", "497K"),
            ("owner", "exclude"),
            ("count", "10"),
        ),
        headers,
    )
    write_private(selectors / "class-search.html", class_search_raw)
    submissions_raw = await _BoundedBytes(client, "data.sec.gov", budget).get_bytes(
        ("submissions", f"CIK{selected_cik}.json"), (), headers
    )
    write_private(selectors / "submissions.json", submissions_raw)
    received_at = datetime.now(UTC)
    selected = select_fund_class_filing(
        mapping_raw=mapping_raw,
        class_search_raw=class_search_raw,
        submissions_raw=submissions_raw,
        symbol=symbol,
        received_at=received_at,
    )
    row = parse_profile((fmp_archive / "profile.json").read_bytes(), symbol)
    if (
        row.get("currency") != "USD"
        or row.get("isEtf") is not True
        or row.get("exchange") not in {"AMEX", "ARCA", "NYSE Arca", "NYSEARCA", "NASDAQ", "XNAS"}
    ):
        raise ValueError("PENDING_IDENTITY_FUND_SOURCE_CONFLICT")

    root = (
        "Archives",
        "edgar",
        "data",
        str(int(selected.cik)),
        selected.accession.replace("-", ""),
    )
    index_name = f"{selected.accession}-index.html"
    index_raw = await directory.get_bytes((*root, index_name), (), headers)
    if not _fund_index_matches(
        index_raw,
        root=root,
        primary_document=selected.primary_document,
        symbol=symbol,
        series_id=selected.series_id,
        class_id=selected.class_id,
    ):
        raise ValueError("PENDING_IDENTITY_FUND_FILING_UNAVAILABLE")
    disclosure_raw = await directory.get_bytes((*root, selected.primary_document), (), headers)
    excerpt, _ = disclosure_excerpt(disclosure_raw)
    if not re.search(rf"\b{re.escape(symbol)}\b", excerpt):
        raise ValueError("PENDING_IDENTITY_FUND_DOCUMENT_MISMATCH")
    originals = (
        (
            "submissions.raw",
            submissions_raw,
            f"https://data.sec.gov/submissions/CIK{selected.cik}.json",
        ),
        (
            "filing-index.raw",
            index_raw,
            "https://www.sec.gov/" + "/".join((*root, index_name)),
        ),
        (
            "disclosure.raw",
            disclosure_raw,
            "https://www.sec.gov/" + "/".join((*root, selected.primary_document)),
        ),
    )
    receipts: list[dict[str, object]] = []
    for number, (name, content, url) in enumerate(originals, start=1):
        receipt = {
            "url": url,
            "status": 200,
            "received_at": datetime.now(UTC).isoformat(),
            "sha256": __import__("hashlib").sha256(content).hexdigest(),
            "file": f"response-{number}.raw",
        }
        write_private(sec_archive / name, content)
        write_private(sec_archive / f"response-{number}.raw", content)
        write_private(sec_archive / f"receipt-{number}.json", json.dumps(receipt).encode())
        receipts.append(receipt)
    write_private(sec_archive / "receipts.json", json.dumps(receipts).encode())
    write_private(
        selectors / "selection.json",
        json.dumps(
            {
                "schema": "pending_fund_selection_v1",
                "symbol": symbol,
                "cik": selected.cik,
                "series_id": selected.series_id,
                "class_id": selected.class_id,
                "accession": selected.accession,
                "primary_document": selected.primary_document,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode(),
    )
    return CollectedIdentityEvidence(fmp_archive, sec_archive, 6, mapping_file)


async def bind_fund_mapping(
    store: _FundMappingStore,
    *,
    mapping_file: Path,
    sec_archive: Path,
    session_date: date,
) -> None:
    if (
        mapping_file.is_symlink()
        or not mapping_file.is_file()
        or mapping_file.stat().st_mode & 0o077
        or sec_archive.is_symlink()
        or not sec_archive.is_dir()
        or sec_archive.stat().st_mode & 0o077
    ):
        raise ValueError("PENDING_IDENTITY_FUND_MAPPING_ARCHIVE_INVALID")
    content = mapping_file.read_bytes()
    if not content or len(content) > 8 * 1024 * 1024:
        raise ValueError("PENDING_IDENTITY_FUND_MAPPING_ARCHIVE_INVALID")
    digest = sha256(content).hexdigest()
    relative_key = f"raw/sec/{session_date.isoformat()}/ticker-directory/{digest}.json"
    uri = await store.put_if_absent(key=relative_key, content=content)
    storage_key = f"{store._prefix}/{relative_key}"
    version_id = _version_id_from_frozen_uri(
        uri,
        bucket=store._bucket,
        storage_key=storage_key,
    )

    listing = await asyncio.to_thread(
        store._client.list_object_versions,
        Bucket=store._bucket,
        Prefix=storage_key,
        MaxKeys=1000,
    )
    versions = listing.get("Versions")
    if listing.get("IsTruncated") or not isinstance(versions, list):
        raise ValueError("PENDING_IDENTITY_FUND_MAPPING_VERSION_INVALID")
    matches = [
        item
        for item in versions
        if isinstance(item, Mapping)
        and item.get("Key") == storage_key
        and item.get("VersionId") == version_id
    ]
    if len(matches) != 1 or not isinstance(matches[0].get("LastModified"), datetime):
        raise ValueError("PENDING_IDENTITY_FUND_MAPPING_VERSION_INVALID")
    stored_at = require_utc(matches[0]["LastModified"])
    write_private(
        sec_archive / "mapping-binding.json",
        json.dumps(
            {
                "key": storage_key,
                "version_id": version_id,
                "sha256": digest,
                "stored_at": stored_at.isoformat(),
                "original_received_at": None,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode(),
    )


MAX_PENDING_PER_RUN = 3


async def list_pending_identities(settings: EngineSettings) -> dict[str, object]:
    """List validated persisted record IDs without exposing holding codes or calling providers."""
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            pending = await read_pending_instrument_discoveries(session, as_of=datetime.now(UTC))
        return {
            "status": "pending_list",
            "pending_count": len(pending),
            "items": [
                {
                    "discovery_id": str(snapshot_id),
                    "identity_route": document.recommended_identity_route,
                    "available_at": document.available_at.isoformat(),
                }
                for snapshot_id, document in sorted(pending.items(), key=lambda item: str(item[0]))
            ],
            "public_requests": 0,
            "broker_requests": 0,
            "execution_locked": True,
        }
    finally:
        await factory.kw["bind"].dispose()


def _pending_identity_item_output(output_dir: Path, provider_code: str) -> tuple[Path, bool]:
    """Keep resumable evidence bound to one opaque provider identity."""
    digest = sha256(provider_code.encode("ascii")).hexdigest()[:16]
    item_dir = output_dir / f"item-{digest}"
    return item_dir, item_dir.exists()


async def resolve_pending_identities(
    settings: EngineSettings,
    broker_settings: BrokerWorkerSettings,
    *,
    fmp_key: SecretStr,
    sec_contact: str,
    output_dir: Path,
    client: httpx.AsyncClient | None = None,
    store: _ResolverStore | None = None,
    broker: FosunGatewayClient | None = None,
    discovery_ids: tuple[UUID, ...] | None = None,
) -> dict[str, object]:
    """Resolve a bounded persisted batch; never accepts a symbol from the caller."""

    from nexus.market.object_store import S3ProviderEvidenceStore

    factory = session_factory(settings)
    owned_client = client is None
    http_client = client or httpx.AsyncClient(follow_redirects=False, timeout=30)
    evidence_store = store or cast(
        _ResolverStore,
        S3ProviderEvidenceStore(settings, disable_environment_proxies=True),
    )
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            pending = await read_pending_instrument_discoveries(session, as_of=datetime.now(UTC))
        pending_count = len(pending)
        if discovery_ids is not None:
            if not discovery_ids or len(set(discovery_ids)) != len(discovery_ids):
                raise ValueError("PENDING_IDENTITY_SELECTION_INVALID")
            if len(discovery_ids) > MAX_PENDING_PER_RUN:
                raise ValueError("PENDING_IDENTITY_BATCH_LIMIT")
            if any(discovery_id not in pending for discovery_id in discovery_ids):
                raise ValueError("PENDING_IDENTITY_SELECTION_UNAVAILABLE")
            documents = sorted(
                (pending[discovery_id] for discovery_id in discovery_ids),
                key=lambda item: item.provider_code,
            )
        else:
            documents = sorted(pending.values(), key=lambda item: item.provider_code)
        if len(documents) > MAX_PENDING_PER_RUN:
            raise ValueError("PENDING_IDENTITY_BATCH_LIMIT")
        if not documents:
            return {
                "status": "nothing_pending",
                "pending_before": 0,
                "resolved": 0,
                "remaining": 0,
                "resolution_counts": {"common_stock": 0, "equity_etf": 0},
                "portfolio_refresh_required": False,
                "public_requests": 0,
                "broker_requests": 0,
                "execution_locked": True,
            }
        if output_dir.exists():
            if (
                output_dir.is_symlink()
                or not output_dir.is_dir()
                or output_dir.stat().st_mode & 0o077
            ):
                raise ValueError("PENDING_IDENTITY_RESUME_INVALID")
        else:
            output_dir.mkdir(mode=0o700, exist_ok=False)
        collected: list[tuple[str, str, CollectedIdentityEvidence]] = []
        public_requests = 0
        manual_required = 0
        for document in documents:
            symbol = document.provider_code.removeprefix("us")
            item_dir, resume_item = _pending_identity_item_output(
                output_dir, document.provider_code
            )
            route = document.recommended_identity_route
            if route == "current_held_common_stock":
                kind = "common_stock"
                evidence = (
                    _load_existing_evidence(item_dir, symbol=symbol, kind=kind)
                    if resume_item
                    else await collect_common_stock_evidence(
                        symbol=symbol,
                        fmp_key=fmp_key,
                        sec_contact=sec_contact,
                        output_dir=item_dir,
                        client=http_client,
                    )
                )
            elif route == "current_held_fund_or_structured":
                kind = "equity_etf"
                try:
                    evidence = (
                        _load_existing_evidence(item_dir, symbol=symbol, kind=kind)
                        if resume_item
                        else await collect_fund_evidence(
                            symbol=symbol,
                            fmp_key=fmp_key,
                            sec_contact=sec_contact,
                            output_dir=item_dir,
                            client=http_client,
                        )
                    )
                except ValueError as error:
                    if str(error) != "PENDING_IDENTITY_FUND_MAPPING_UNAVAILABLE":
                        raise
                    manual_required += 1
                    continue
            else:
                manual_required += 1
                continue
            public_requests += evidence.request_count
            collected.append((kind, symbol, evidence))

        if not collected:
            return {
                "status": "manual_review_required",
                "pending_before": pending_count,
                "resolved": 0,
                "remaining": pending_count,
                "manual_review_required": manual_required,
                "resolution_counts": {"common_stock": 0, "equity_etf": 0},
                "portfolio_refresh_required": False,
                "public_requests": public_requests,
                "broker_requests": 0,
                "execution_locked": True,
            }

        gateway = broker or FosunGatewayClient(broker_settings)
        owns_gateway = broker is None
        try:
            accounts_page = await gateway.read_accounts()
            accounts = tuple(AccountItem.model_validate(item) for item in accounts_page.items)
            if len(accounts) != 1 or accounts_page.quality != "PASS" or accounts_page.has_more:
                raise ValueError("PENDING_IDENTITY_ACCOUNT_INVALID")
            holdings_page = await gateway.read_holdings(
                accounts[0].external_account_id, cursor=None, limit=100
            )
        finally:
            if owns_gateway:
                await gateway.aclose()

        resolved = 0
        resolution_counts = {"common_stock": 0, "equity_etf": 0}
        for kind, symbol, evidence in collected:
            if kind == "equity_etf":
                assert evidence.mapping_file is not None
                if not (evidence.sec_archive / "mapping-binding.json").exists():
                    await bind_fund_mapping(
                        evidence_store,
                        mapping_file=evidence.mapping_file,
                        sec_archive=evidence.sec_archive,
                        session_date=datetime.now(UTC).date(),
                    )
            async with factory() as session, session.begin():
                if kind == "common_stock":
                    result = await import_current_held_common_stock(
                        session,
                        evidence_store,
                        symbol=symbol,
                        fmp_archive=evidence.fmp_archive,
                        sec_archive=evidence.sec_archive,
                        accounts_page=accounts_page,
                        holdings_page=holdings_page,
                    )
                else:
                    assert evidence.mapping_file is not None
                    result = await import_current_held_etf(
                        session,
                        evidence_store,
                        symbol=symbol,
                        fmp_archive=evidence.fmp_archive,
                        sec_archive=evidence.sec_archive,
                        mapping_file=evidence.mapping_file,
                        accounts_page=accounts_page,
                        holdings_page=holdings_page,
                    )
            async with factory() as session, session.begin():
                await session.execute(text("SET TRANSACTION READ ONLY"))
                if kind == "common_stock":
                    await read_current_held_common_stock(
                        session, evidence_store, snapshot_id=result.snapshot_id
                    )
                else:
                    await read_current_held_etf(
                        session, evidence_store, snapshot_id=result.snapshot_id
                    )
            resolved += 1
            resolution_counts[kind] += 1

        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            remaining = len(
                await read_pending_instrument_discoveries(session, as_of=datetime.now(UTC))
            )
        return {
            "status": "resolved" if remaining == 0 else "partial",
            "pending_before": pending_count,
            "resolved": resolved,
            "remaining": remaining,
            "manual_review_required": manual_required,
            "resolution_counts": resolution_counts,
            "portfolio_refresh_required": resolved > 0,
            "public_requests": public_requests,
            "broker_requests": 2,
            "execution_locked": True,
        }
    finally:
        if owned_client:
            await http_client.aclose()
        await factory.kw["bind"].dispose()


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        del message
        raise ValueError("PENDING_IDENTITY_REQUEST_INVALID")


def main() -> None:
    from nexus.ops.fosun_local import load_local_environment

    parser = _Parser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--list-pending", action="store_true")
    parser.add_argument(
        "--discovery-id",
        type=UUID,
        action="append",
        help="Select an existing pending discovery ID; repeat up to three times.",
    )
    try:
        arguments = parser.parse_args()
        if arguments.list_pending:
            if arguments.discovery_id or arguments.output_dir is not None:
                raise ValueError("PENDING_IDENTITY_REQUEST_INVALID")
        elif arguments.output_dir is None:
            raise ValueError("PENDING_IDENTITY_REQUEST_INVALID")
        environment = load_local_environment(arguments.repo_root, os.environ)
        settings = EngineSettings.model_validate(environment)
        if arguments.list_pending:
            result = asyncio.run(list_pending_identities(settings))
        else:
            assert arguments.output_dir is not None
            result = asyncio.run(
                resolve_pending_identities(
                    settings,
                    BrokerWorkerSettings.model_validate(environment),
                    fmp_key=SecretStr(environment.get("FMP_API_KEY", "")),
                    sec_contact=environment.get("SEC_CONTACT", ""),
                    output_dir=arguments.output_dir,
                    discovery_ids=(
                        tuple(arguments.discovery_id) if arguments.discovery_id else None
                    ),
                )
            )
    except Exception as error:
        code = (
            str(error)
            if isinstance(error, ValueError)
            and re.fullmatch(r"PENDING_IDENTITY_[A-Z_]+", str(error))
            else "PENDING_IDENTITY_FAILED"
        )
        print(json.dumps({"status": "failed", "code": code}), file=sys.stderr)
        raise SystemExit(1) from None
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()


def _json_object(raw: bytes, code: str) -> dict[str, Any]:
    def unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(code)
            result[key] = value
        return result

    try:
        value = json.loads(raw, object_pairs_hook=unique)
    except (UnicodeError, json.JSONDecodeError, ValueError):
        raise ValueError(code) from None
    if not isinstance(value, dict):
        raise ValueError(code)
    return value


def _latest_filing(
    submissions: dict[str, Any],
    *,
    cik: str,
    forms: frozenset[str],
    received_at: datetime,
    unavailable_code: str,
) -> tuple[str, str]:
    return _filing_candidates(
        submissions,
        cik=cik,
        forms=forms,
        received_at=received_at,
        unavailable_code=unavailable_code,
    )[0]


def _filing_candidates(
    submissions: dict[str, Any],
    *,
    cik: str,
    forms: frozenset[str],
    received_at: datetime,
    unavailable_code: str,
) -> list[tuple[str, str]]:
    try:
        if str(submissions["cik"]).zfill(10) != cik:
            raise ValueError
        recent = submissions["filings"]["recent"]
        fields = (
            "accessionNumber",
            "acceptanceDateTime",
            "filingDate",
            "form",
            "primaryDocument",
        )
        arrays = [recent[field] for field in fields]
        if any(not isinstance(array, list) for array in arrays):
            raise ValueError
        if len({len(array) for array in arrays}) != 1:
            raise ValueError
        cutoff = require_utc(received_at)
        candidates: list[tuple[datetime, str, str]] = []
        for accession, accepted, filed, form, primary in zip(*arrays, strict=True):
            if form not in forms:
                continue
            if not all(isinstance(value, str) for value in (accession, accepted, filed, primary)):
                raise ValueError
            accepted_at = datetime.fromisoformat(accepted.replace("Z", "+00:00"))
            filed_at = date.fromisoformat(filed)
            if (
                accepted_at.tzinfo is None
                or accepted_at.astimezone(UTC) > cutoff
                or filed_at > cutoff.date()
                or _ACCESSION.fullmatch(accession) is None
                or _PRIMARY_DOCUMENT.fullmatch(primary) is None
            ):
                continue
            candidates.append((accepted_at.astimezone(UTC), accession, primary))
        if not candidates:
            raise ValueError
        return [(accession, primary) for _, accession, primary in sorted(candidates, reverse=True)]
    except (KeyError, TypeError, ValueError):
        raise ValueError(unavailable_code) from None


def _fund_index_matches(
    raw: bytes,
    *,
    root: tuple[str, ...],
    primary_document: str,
    symbol: str,
    series_id: str,
    class_id: str,
) -> bool:
    try:
        text_value, truncated = disclosure_excerpt(raw)
        links = _IndexLinks()
        links.feed(raw.decode("utf-8-sig"))
    except (UnicodeError, ValueError):
        return False
    path = "/" + "/".join((*root, primary_document))
    return bool(
        not truncated
        and set(re.findall(r"\bS[0-9]{9}\b", text_value)) == {series_id}
        and set(re.findall(r"\bC[0-9]{9}\b", text_value)) == {class_id}
        and re.search(rf"\b{re.escape(symbol)}\b", text_value)
        and {path, "https://www.sec.gov" + path}.intersection(links.links)
    )


def select_common_stock_filing(
    *, mapping_raw: bytes, submissions_raw: bytes, symbol: str, received_at: datetime
) -> CommonStockFilingSelection:
    unavailable = "PENDING_IDENTITY_COMMON_MAPPING_UNAVAILABLE"
    mapping = _json_object(mapping_raw, unavailable)
    try:
        if mapping.get("fields") != ["cik", "name", "ticker", "exchange"]:
            raise ValueError
        rows = mapping.get("data")
        if not isinstance(rows, list):
            raise ValueError
        matches = [
            row for row in rows if isinstance(row, list) and len(row) == 4 and row[2] == symbol
        ]
        if len(matches) != 1:
            raise ValueError
        raw_cik, _, _, raw_exchange = matches[0]
        if type(raw_cik) is not int or not 0 < raw_cik < 10**10:
            raise ValueError
        exchange = _EXCHANGES.get(raw_exchange)
        if exchange is None:
            raise ValueError
        cik = str(raw_cik).zfill(10)
    except (TypeError, ValueError):
        raise ValueError(unavailable) from None
    accession, primary = _latest_filing(
        _json_object(submissions_raw, "PENDING_IDENTITY_COMMON_FILING_UNAVAILABLE"),
        cik=cik,
        forms=frozenset({"10-K", "10-Q"}),
        received_at=received_at,
        unavailable_code="PENDING_IDENTITY_COMMON_FILING_UNAVAILABLE",
    )
    compact = accession.replace("-", "")
    source_url = f"https://www.sec.gov/Archives/edgar/data/{raw_cik}/{compact}/{primary}"
    return CommonStockFilingSelection(cik, exchange, accession, primary, source_url)


def select_fund_filing(
    *, mapping_raw: bytes, submissions_raw: bytes, symbol: str, received_at: datetime
) -> FundFilingSelection:
    try:
        cik, series_id, class_id = resolve_fund(
            _json_object(mapping_raw, "PENDING_IDENTITY_FUND_MAPPING_UNAVAILABLE"), symbol
        )
    except ValueError:
        raise ValueError("PENDING_IDENTITY_FUND_MAPPING_UNAVAILABLE") from None
    accession, primary = _latest_filing(
        _json_object(submissions_raw, "PENDING_IDENTITY_FUND_FILING_UNAVAILABLE"),
        cik=cik,
        forms=frozenset({"497K"}),
        received_at=received_at,
        unavailable_code="PENDING_IDENTITY_FUND_FILING_UNAVAILABLE",
    )
    return FundFilingSelection(cik, series_id, class_id, accession, primary)


def select_fund_class_filing(
    *,
    mapping_raw: bytes,
    class_search_raw: bytes,
    submissions_raw: bytes,
    symbol: str,
    received_at: datetime,
) -> FundFilingSelection:
    """Select the latest 497K returned by SEC's exact class-level search."""

    try:
        cik_value, series_id, class_id = resolve_fund(
            _json_object(mapping_raw, "PENDING_IDENTITY_FUND_MAPPING_UNAVAILABLE"), symbol
        )
        search_text, truncated = disclosure_excerpt(class_search_raw)
        if (
            truncated
            or len(re.findall(rf"\bClass/Contract:\s*{class_id}\b", search_text)) != 1
            or len(re.findall(rf"\bTicker Symbol:\s*{re.escape(symbol)}\b", search_text)) != 1
        ):
            raise ValueError
        links = _IndexLinks()
        links.feed(class_search_raw.decode("utf-8-sig"))
        browse_accessions: set[str] = set()
        for link in links.links:
            match = re.fullmatch(
                r"(?:https://www\.sec\.gov)?/Archives/edgar/data/[0-9]+/([0-9]{18})/"
                r"([0-9]{10}-[0-9]{2}-[0-9]{6})-index\.html?",
                link,
            )
            if match and match[1] == match[2].replace("-", ""):
                browse_accessions.add(match[2])
        candidates = _filing_candidates(
            _json_object(submissions_raw, "PENDING_IDENTITY_FUND_FILING_UNAVAILABLE"),
            cik=cik_value,
            forms=frozenset({"497K"}),
            received_at=received_at,
            unavailable_code="PENDING_IDENTITY_FUND_FILING_UNAVAILABLE",
        )
        selected = [candidate for candidate in candidates if candidate[0] in browse_accessions]
        if not selected:
            raise ValueError
        accession, primary_document = selected[0]
        return FundFilingSelection(
            cik_value,
            series_id,
            class_id,
            accession,
            primary_document,
        )
    except (UnicodeError, ValueError):
        raise ValueError("PENDING_IDENTITY_FUND_FILING_UNAVAILABLE") from None
