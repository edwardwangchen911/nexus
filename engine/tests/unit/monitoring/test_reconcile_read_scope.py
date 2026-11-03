"""A scan reuses only versioned originals within its own source-reading scope."""

from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from nexus.monitoring import reconciliation
from nexus.research import sec_exhibit_storage
from nexus.settings import EngineSettings
from nexus.workbench.evidence_read_scope import evidence_read_scope


@pytest.mark.asyncio
async def test_empty_scan_never_constructs_an_unused_archive_client(monkeypatch):
    settings = EngineSettings.for_test().model_copy(
        update={"object_store_endpoint": "http://127.0.0.1:9000"}
    )
    session = SimpleNamespace(info={"engine_settings": settings})

    def unavailable(*args, **kwargs):
        raise RuntimeError("archive unavailable")

    async def empty(*args, **kwargs):
        return {"recorded": 0}

    monkeypatch.setattr(sec_exhibit_storage, "S3ProviderEvidenceStore", unavailable)
    monkeypatch.setattr(reconciliation, "_reconcile_candidates", empty)
    assert await reconciliation.reconcile_account_results(
        session, account_id=uuid4(), instrument_ids=(), now=datetime.now(UTC)
    ) == {"recorded": 0}
    assert "sec_exhibit_store" not in session.info

    # A consumer that actually needs the unavailable archive still fails closed.
    with pytest.raises(RuntimeError, match="archive unavailable"), evidence_read_scope(session):
        await session.info["sec_exhibit_store"].read_raw("raw/a", uri="s3://test/raw/a?versionId=1")
    assert "sec_exhibit_store" not in session.info


@pytest.mark.asyncio
async def test_scan_reuses_exact_bytes_but_next_scan_reads_again(monkeypatch):
    reads = []

    class Source:
        async def read_raw(self, key, *, uri):
            reads.append((key, uri))
            return b"original"

    source = Source()
    session = SimpleNamespace(info={"sec_exhibit_store": source})

    async def consume(session, **kwargs):
        reader = sec_exhibit_storage.configured_exhibit_store(session)
        for version in ("one", "one", "two"):
            assert (
                await reader.read_raw("raw/a", uri=f"s3://test/raw/a?versionId={version}")
                == b"original"
            )
        return {"recorded": 0}

    monkeypatch.setattr(reconciliation, "_reconcile_candidates", consume)
    for count in (2, 4):
        await reconciliation.reconcile_account_results(
            session, account_id=uuid4(), instrument_ids=(), now=datetime.now(UTC)
        )
        assert len(reads) == count
        assert session.info["sec_exhibit_store"] is source
