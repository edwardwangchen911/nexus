"""Bounded reuse of exact versioned bytes during one workbench read only.

This cache stores no validation result. Every caller still validates its own
hash, receipt, time, database binding and policy against the frozen bytes.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Literal
from urllib.parse import parse_qs, urlsplit

from sqlalchemy.ext.asyncio import AsyncSession

from nexus.research.sec_exhibit_storage import ExhibitStore, configured_exhibit_store
from nexus.settings import EngineSettings


class _ReadStore:
    def __init__(self, source: ExhibitStore, max_bytes: int, max_entries: int) -> None:
        self.source = source
        self.max_bytes = max_bytes
        self.max_entries = max_entries
        self.bytes = 0
        self.active = True
        self.cache: dict[tuple[str, str, str], bytes] = {}

    async def put_if_absent(self, *, key: str, content: bytes) -> str:
        raise ValueError("WORKBENCH_READ_ONLY")

    async def read_raw(self, key: str, *, uri: str) -> bytes:
        return await self._read("raw", key, uri)

    async def read_evidence(self, key: str, *, uri: str) -> bytes:
        return await self._read("evidence", key, uri)

    async def _read(self, kind: Literal["raw", "evidence"], key: str, uri: str) -> bytes:
        versions = parse_qs(urlsplit(uri).query).get("versionId", [])
        reusable = self.active and len(versions) == 1 and versions[0] not in {"", "null"}
        identity = (kind, key, uri)
        if reusable and identity in self.cache:
            return self.cache[identity]
        method = self.source.read_raw if kind == "raw" else self.source.read_evidence
        # The underlying store verifies namespace, frozen version and size.
        # Exceptions and unversioned reads are never retained.
        content = await method(key, uri=uri)
        if (
            reusable
            and self.active
            and len(self.cache) < self.max_entries
            and self.bytes + len(content) <= self.max_bytes
        ):
            self.cache[identity] = content
            self.bytes += len(content)
        return content


@contextmanager
def evidence_read_scope(
    session: AsyncSession, *, max_bytes: int = 32 * 1024 * 1024, max_entries: int = 512
) -> Iterator[None]:
    """Restore the original store on every exit; never persist bytes across reads."""
    if session.info.get("sec_exhibit_store") is None and not isinstance(
        session.info.get("engine_settings"), EngineSettings
    ):
        yield
        return
    present = "sec_exhibit_store" in session.info
    previous = session.info.get("sec_exhibit_store")
    try:
        source = configured_exhibit_store(session)
    except ValueError:
        # Optional byte reuse must not make empty projections require storage.
        # Actual source consumers retain their normal missing-store rejection.
        yield
        return
    reader = _ReadStore(source, max_bytes, max_entries)
    session.info["sec_exhibit_store"] = reader
    try:
        yield
    finally:
        reader.active = False
        reader.cache.clear()
        reader.bytes = 0
        if present:
            session.info["sec_exhibit_store"] = previous
        else:
            session.info.pop("sec_exhibit_store", None)
