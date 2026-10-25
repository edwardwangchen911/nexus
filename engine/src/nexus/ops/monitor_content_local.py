"""Collect one exact archived SEC entry, preserving originals without research dispatch."""

import asyncio
import json
import os
import re
import stat
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from uuid import UUID

import httpx
from sqlalchemy import func, select

from nexus.monitoring.filing_archive import import_filing_content
from nexus.monitoring.filing_content import MAX_DOCUMENTS, document_inventory, filing_root
from nexus.ops.local_recovery import write_private
from nexus.ops.sec_index_local import _store
from nexus.persistence.session import session_factory
from nexus.providers.http import (
    AllowedOrigin,
    FixedOriginHttpClient,
    HttpPolicyError,
    HttpRequestPolicy,
)
from nexus.research.sec_disclosure_index import SecIndexEntry
from nexus.research.sec_exhibit_storage import ExhibitStore
from nexus.research.sec_index_storage import read_index_capture
from nexus.settings import EngineSettings

_SAFE_CODES = frozenset(
    {
        "SEC_MONITOR_INVENTORY_INVALID",
        "SEC_MONITOR_ATTACHMENT_UNSUPPORTED",
        "SEC_MONITOR_INVENTORY_IDENTITY_INVALID",
        "SEC_MONITOR_DOCUMENT_SIZE_INVALID",
        "SEC_MONITOR_DOCUMENT_TIME_INVALID",
        "SEC_MONITOR_DOCUMENT_SET_INVALID",
        "SEC_MONITOR_CONTENT_CAPACITY",
        "SEC_MONITOR_ARCHIVE_INVALID",
        "SEC_MONITOR_ARCHIVE_UNAVAILABLE",
        "SEC_MONITOR_ORIGINAL_INVALID",
        "SEC_MONITOR_CLOCK_UNAVAILABLE",
    }
)


class _Exact200Client(FixedOriginHttpClient):
    async def _read_bounded(self, response: httpx.Response) -> bytes:
        # The common transport allows all 2xx. A receipt for a complete original
        # must never relabel a partial 206 (or an empty 204) as HTTP 200.
        if response.status_code != 200:
            raise HttpPolicyError("STATUS_REJECTED", http_status=response.status_code)
        return await super()._read_bounded(response)


def _contact(contact: str) -> None:
    if (
        not contact
        or contact != contact.strip()
        or "@" not in contact
        or len(contact.encode()) > 254
        or any(ord(char) < 32 for char in contact)
    ):
        raise ValueError("SEC_MONITOR_CONTACT_REQUIRED")


def _private_directory(path: Path) -> None:
    parent = path.parent
    if (
        parent.is_symlink()
        or not parent.is_dir()
        or parent.stat().st_uid != os.getuid()
        or stat.S_IMODE(parent.stat().st_mode) != 0o700
        or path.exists()
        or path.is_symlink()
    ):
        raise ValueError("SEC_MONITOR_COLLECTION_DIRECTORY_INVALID")
    path.mkdir(mode=0o700)


def _failure(path: Path, *, error: BaseException, requests: int, stage: str) -> str:
    code = (
        str(error)
        if isinstance(error, ValueError) and str(error) in _SAFE_CODES
        else ("SEC_MONITOR_COLLECTION_FAILED")
    )
    write_private(
        path / "result.json",
        json.dumps(
            {
                "status": "unavailable",
                "error_code": code,
                "stage": stage,
                "provider_requests": requests,
                "model_requests": 0,
                "job_created": False,
                "execution_locked": True,
            },
            sort_keys=True,
        ).encode(),
    )
    return code


async def _collect_documents(
    *,
    entry: SecIndexEntry,
    issuer_cik: str,
    archive_dir: Path,
    contact: str,
    http_client: httpx.AsyncClient | None = None,
) -> int:
    _contact(contact)
    root = filing_root(issuer_cik, entry.accession_number)
    # Reuse the fixed SEC origin/path and receipt bounds, never an href as a URL.
    parts = tuple(root.removeprefix("https://www.sec.gov/").strip("/").split("/"))
    owned = http_client is None
    client = http_client or httpx.AsyncClient(follow_redirects=False, timeout=30)
    requests = 0
    stage = "inventory"
    try:
        fixed = _Exact200Client(
            provider_name="sec",
            origin=AllowedOrigin("https", "www.sec.gov"),
            policy=HttpRequestPolicy(max_attempts=1),
            client=client,
        )
        _private_directory(archive_dir)

        async def get(name: str) -> bytes:
            nonlocal requests
            if requests >= MAX_DOCUMENTS + 1:
                raise ValueError("SEC_MONITOR_CONTENT_CAPACITY")
            requests += 1
            raw = await fixed.get_bytes(parts + (name,), (), (("User-Agent", f"NEXUS {contact}"),))
            received = datetime.now(UTC)
            if contact.encode() in raw:
                raise ValueError("SEC_MONITOR_COLLECTION_CONTACT_REFLECTION")
            write_private(archive_dir / name, raw)
            write_private(
                archive_dir / (name + ".receipt.json"),
                json.dumps(
                    {
                        "source_url": root + name,
                        "http_status": 200,
                        "received_at": received.isoformat(),
                        "sha256": sha256(raw).hexdigest(),
                        "byte_length": len(raw),
                    },
                    sort_keys=True,
                ).encode(),
            )
            return raw

        try:
            async with asyncio.timeout(60):
                inventory = await get(entry.accession_number + "-index.html")
                rows = document_inventory(inventory, entry=entry, issuer_cik=issuer_cik)
                stage = "documents"
                for name, _, size in rows:
                    raw = await get(name)
                    if len(raw) != size:
                        raise ValueError("SEC_MONITOR_DOCUMENT_SIZE_INVALID")
            return requests
        except asyncio.CancelledError as error:
            _failure(archive_dir, error=error, requests=requests, stage=stage)
            raise
        except Exception as error:
            code = _failure(archive_dir, error=error, requests=requests, stage=stage)
            raise ValueError(code) from None
    finally:
        if owned:
            await client.aclose()


async def collect_filing_content(
    settings: EngineSettings,
    *,
    index_snapshot_id: UUID,
    accession: str,
    archive_dir: Path,
    contact: str,
    http_client: httpx.AsyncClient | None = None,
    store: ExhibitStore | None = None,
) -> dict[str, object]:
    """Collect only the specified immutable index entry, then use the normal importer."""
    _contact(contact)
    if re.fullmatch(r"[0-9]{10}-[0-9]{2}-[0-9]{6}", accession) is None:
        raise ValueError("SEC_MONITOR_ENTRY_INVALID")
    archive_store = _store(settings, store)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            now = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(now, datetime):
                raise ValueError("SEC_MONITOR_CLOCK_UNAVAILABLE")
            _, index = await read_index_capture(
                session, archive_store, snapshot_id=index_snapshot_id, as_of=now
            )
            matches = [entry for entry in index.entries if entry.accession_number == accession]
            if len(matches) != 1:
                raise ValueError("SEC_MONITOR_ENTRY_INVALID")
        requests = await _collect_documents(
            entry=matches[0],
            issuer_cik=index.issuer_cik,
            archive_dir=archive_dir,
            contact=contact,
            http_client=http_client,
        )
        try:
            async with factory() as session, session.begin():
                session.info["sec_exhibit_store"] = archive_store
                now = await session.scalar(select(func.clock_timestamp()))
                if not isinstance(now, datetime):
                    raise ValueError("SEC_MONITOR_CLOCK_UNAVAILABLE")
                verified, created = await import_filing_content(
                    session,
                    index_snapshot_id=index_snapshot_id,
                    accession=accession,
                    archive_dir=archive_dir,
                    now=now,
                )
            result: dict[str, object] = {
                "status": "created" if created else "existing",
                "snapshot_id": str(verified.snapshot_id),
                "evidence_ref_id": str(verified.evidence_ref_id),
                "canonical_hash": verified.canonical_hash,
                "provider_requests": requests,
                "model_requests": 0,
                "job_created": False,
                "execution_locked": True,
                "impact_review": "not_performed",
            }
        except asyncio.CancelledError as error:
            _failure(archive_dir, error=error, requests=requests, stage="import")
            raise
        except Exception as error:
            code = _failure(archive_dir, error=error, requests=requests, stage="import")
            raise ValueError(code) from None
        write_private(archive_dir / "result.json", json.dumps(result, sort_keys=True).encode())
        return result
    finally:
        await factory.kw["bind"].dispose()
