"""Bounded listed-source archival used only by the normal explicit index entry."""

import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

import httpx

from nexus.ops.local_recovery import write_private
from nexus.ops.monitor_content_local import _contact
from nexus.ops.research_identity_local import _Exact200Client
from nexus.providers.http import AllowedOrigin, HttpRequestPolicy
from nexus.research.sec_disclosure_index import MAX_RAW_BYTES, _object
from nexus.research.sec_listed_history import listed_files


async def collect_listed_sources(
    *, archive_dir: Path, symbol: str, contact: str, client: httpx.AsyncClient | None = None
) -> int:
    """Archive one declared shard and closing parent; caller owns the shared HTTP deadline."""
    _contact(contact)
    parent = (archive_dir / f"{symbol}-submissions.json").read_bytes()
    doc = _object(parent)
    cik = str(doc["cik"]).zfill(10)
    files = listed_files(parent, issuer_cik=cik)
    # Missing declarations are not completed by a guessed empty list.
    if files is None:
        raise ValueError("SEC_INDEX_HISTORY_DECLARATION_MISSING")
    used = sum(
        (archive_dir / name).stat().st_size
        for name in (
            "directory.json",
            "directory-receipt.json",
            f"{symbol}-submissions.json",
            f"{symbol}-submissions-receipt.json",
        )
    )
    owned = client is None
    transport = client or httpx.AsyncClient(follow_redirects=False, timeout=30)
    requests = 0
    try:

        async def get(name: str, output: str) -> None:
            nonlocal used, requests
            remaining = MAX_RAW_BYTES - used
            if remaining <= 0:
                raise ValueError("SEC_INDEX_HISTORY_LIMIT")
            fixed = _Exact200Client(
                provider_name="sec",
                origin=AllowedOrigin("https", "data.sec.gov"),
                policy=HttpRequestPolicy(max_attempts=1, max_response_bytes=remaining),
                client=transport,
            )
            requests += 1
            raw = await fixed.get_bytes(
                ("submissions", name), (), (("User-Agent", f"NEXUS {contact}"),)
            )
            if contact.encode() in raw:
                raise ValueError("IDENTITY_COLLECTION_CONTACT_REFLECTION")
            receipt = json.dumps(
                dict(
                    schema="sec_identity_source_receipt_v1",
                    source_url="https://data.sec.gov/submissions/" + name,
                    received_at=datetime.now(UTC).isoformat(),
                    sha256=sha256(raw).hexdigest(),
                    byte_length=len(raw),
                    http_status=200,
                )
            ).encode()
            write_private(archive_dir / f"{output}.json", raw)
            write_private(archive_dir / f"{output}-receipt.json", receipt)
            used += len(raw) + len(receipt)
            if used > MAX_RAW_BYTES:
                raise ValueError("SEC_INDEX_HISTORY_LIMIT")

        for i, item in enumerate(files):
            await get(item.name, f"history-{i}")
        await get(f"CIK{cik}.json", f"{symbol}-submissions-close")
        if (archive_dir / f"{symbol}-submissions-close.json").read_bytes() != parent:
            raise ValueError("SEC_INDEX_HISTORY_PARENT_CHANGED")
        write_private(
            archive_dir / "history-manifest.json",
            json.dumps(
                dict(schema="sec_listed_history_archive_v2", files=[x.name for x in files])
            ).encode(),
        )
        return requests
    except BaseException:
        write_private(
            archive_dir / "history-failure.json",
            json.dumps(dict(requests=requests, authority_registered=False, retried=False)).encode(),
        )
        raise
    finally:
        if owned:
            await transport.aclose()


def read_listed_sources(
    archive_dir: Path, symbol: str
) -> tuple[tuple[bytes, bytes], tuple[tuple[str, bytes, bytes], ...]]:
    from nexus.ops.sec_index_local import _read

    manifest = _object(_read(archive_dir / "history-manifest.json", limit=8192))
    names = manifest.get("files")
    if (
        set(manifest) != {"schema", "files"}
        or manifest["schema"] != "sec_listed_history_archive_v2"
        or not isinstance(names, list)
        or len(names) > 1
        or any(not isinstance(n, str) for n in names)
    ):
        raise ValueError("SEC_INDEX_HISTORY_MANIFEST_INVALID")
    closing = (
        _read(archive_dir / f"{symbol}-submissions-close.json", limit=MAX_RAW_BYTES),
        _read(archive_dir / f"{symbol}-submissions-close-receipt.json", limit=65536),
    )
    history = tuple(
        (
            name,
            _read(archive_dir / f"history-{i}.json", limit=MAX_RAW_BYTES),
            _read(archive_dir / f"history-{i}-receipt.json", limit=65536),
        )
        for i, name in enumerate(names)
    )
    return closing, history
