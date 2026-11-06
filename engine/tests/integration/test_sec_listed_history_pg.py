"""Explicit normal v2 archive routing preserves v1 authority and old events."""

from datetime import UTC, datetime, timedelta

import httpx
import pytest
from helpers.current_identity import CIK
from integration.test_sec_index_pg import seed, submissions
from nexus.ops.sec_index_local import collect_index, import_index, inspect_index
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.runtime import Job, OutboxEvent
from nexus.persistence.session import session_factory
from nexus.providers.http import HttpPolicyError
from nexus.research.sec_listed_storage import SOURCE, ListedCapture, read_listed_capture
from sqlalchemy import func, select


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", [None, "http", "timeout", "parent_changed", "count", "unsafe_path"]
)
async def test_normal_listed_history_capture_and_readback_never_dispatches(
    migrated_database_url, tmp_path, failure, monkeypatch
):
    settings, store, identity, parent = await seed(migrated_database_url, tmp_path)
    metadata = submissions()
    name = f"CIK{CIK}-submissions-001.json"
    at = datetime.now(UTC) - timedelta(days=365)
    old = dict(
        accessionNumber=[f"{CIK}-25-000001"],
        form=["8-K/A"],
        acceptanceDateTime=[at.isoformat()],
        filingDate=[at.date().isoformat()],
        primaryDocument=["old.txt"],
    )
    metadata["filings"]["files"] = [
        dict(
            name=name,
            filingCount=1,
            filingFrom=at.date().isoformat(),
            filingTo=at.date().isoformat(),
        )
    ]
    requests = []

    def handle(request):
        requests.append(str(request.url))
        if request.url.host == "www.sec.gov":
            return httpx.Response(
                200,
                json=dict(
                    fields=["cik", "name", "ticker", "exchange"],
                    data=[[int(CIK), "Synthetic", "AAPL", "NYSE"]],
                ),
            )
        if request.url.path.endswith(name):
            if failure == "http":
                return httpx.Response(503)
            if failure == "timeout":
                raise httpx.ReadTimeout("synthetic timeout")
            return httpx.Response(200, json=old)
        assert request.url.path == f"/submissions/CIK{CIK}.json"
        if failure == "parent_changed" and len(requests) == 4:
            return httpx.Response(200, json={**metadata, "changed": True})
        return httpx.Response(200, json=metadata)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        if failure == "count":
            metadata["filings"]["files"][0]["filingCount"] = 2
        if failure == "unsafe_path":
            metadata["filings"]["files"][0]["name"] = "../" + name
        if failure is not None:
            with pytest.raises((ValueError, HttpPolicyError)):
                await collect_index(
                    settings,
                    identity_snapshot_id=identity.snapshot_id,
                    archive_dir=parent / "v2",
                    contact="test@example.invalid",
                    store=store,
                    client=client,
                    listed_history=True,
                )
            assert len(requests) == ({"unsafe_path": 2, "http": 3, "timeout": 3}.get(failure, 4))
            factory = session_factory(settings)
            try:
                async with factory() as db:
                    assert (
                        await db.scalar(
                            select(func.count())
                            .select_from(DataSnapshot)
                            .where(DataSnapshot.source == SOURCE)
                        )
                        == 0
                    )
                    assert await db.scalar(select(func.count()).select_from(Job)) == 0
                    assert await db.scalar(select(func.count()).select_from(OutboxEvent)) == 0
            finally:
                await factory.kw["bind"].dispose()
            return
        legacy = await collect_index(
            settings,
            identity_snapshot_id=identity.snapshot_id,
            archive_dir=parent / "v1",
            contact="test@example.invalid",
            store=store,
            client=client,
        )
        requests.clear()
        captured = await collect_index(
            settings,
            identity_snapshot_id=identity.snapshot_id,
            archive_dir=parent / "v2",
            contact="test@example.invalid",
            store=store,
            client=client,
            listed_history=True,
        )
    assert len(requests) == 4 and captured["provider_requests"] == 4
    from uuid import UUID

    snapshot = UUID(captured["snapshot_id"])
    view = await inspect_index(settings, snapshot_id=snapshot, store=store, listed_history=True)
    assert view["history_complete"] is True and view["entries"] == 2
    assert view["schema_version"] == "sec_disclosure_index_v2"
    # All originals, including directory and receipts, share the v2 capture cap.
    from nexus.research import sec_listed_storage

    archive = parent / "v2"
    total = sum(
        (archive / name).stat().st_size
        for name in (
            "directory.json",
            "directory-receipt.json",
            "AAPL-submissions.json",
            "AAPL-submissions-receipt.json",
            "AAPL-submissions-close.json",
            "AAPL-submissions-close-receipt.json",
            "history-0.json",
            "history-0-receipt.json",
        )
    )
    objects_before = dict(store.objects)
    with monkeypatch.context() as patch:
        patch.setattr(sec_listed_storage, "MAX_RAW_BYTES", total - 1)
        with pytest.raises(ValueError, match="SEC_INDEX_HISTORY_LIMIT"):
            await import_index(
                settings,
                identity_snapshot_id=identity.snapshot_id,
                archive_dir=archive,
                store=store,
                listed_history=True,
            )
        assert store.objects == objects_before
        patch.setattr(sec_listed_storage, "MAX_RAW_BYTES", total)
        exact = await import_index(
            settings,
            identity_snapshot_id=identity.snapshot_id,
            archive_dir=archive,
            store=store,
            listed_history=True,
        )
        assert exact["snapshot_id"] == captured["snapshot_id"]
    repeated = await import_index(
        settings,
        identity_snapshot_id=identity.snapshot_id,
        archive_dir=parent / "v2",
        store=store,
        listed_history=True,
    )
    assert repeated["snapshot_id"] == captured["snapshot_id"] and repeated["status"] == "existing"
    oldview = await inspect_index(settings, snapshot_id=UUID(legacy["snapshot_id"]), store=store)
    assert oldview["entries"] == 1 and not oldview["history_complete"]
    # A v1 consumer cannot silently turn this new source into old-plan authority.
    with pytest.raises(ValueError, match="SEC_INDEX_CAPTURE_UNAVAILABLE"):
        await inspect_index(settings, snapshot_id=snapshot, store=store)
    factory = session_factory(settings)
    try:
        async with factory() as db:
            assert await db.scalar(select(func.count()).select_from(Job)) == 0
            assert await db.scalar(select(func.count()).select_from(OutboxEvent)) == 0
            row = await db.get(DataSnapshot, snapshot)
            with pytest.raises(ValueError, match="SEC_INDEX_CAPTURE_UNAVAILABLE"):
                await read_listed_capture(
                    db,
                    store,
                    snapshot_id=snapshot,
                    as_of=row.available_at - timedelta(microseconds=1),
                )
            binding = ListedCapture.model_validate(row.payload).originals[-2]
            key = binding.uri.removeprefix("s3://synthetic/")
            original = store.objects[key]
            store.objects[key] = b"corrupted original"
            try:
                with pytest.raises(ValueError, match="SEC_INDEX_ARTIFACT_INVALID"):
                    await inspect_index(
                        settings, snapshot_id=snapshot, store=store, listed_history=True
                    )
            finally:
                store.objects[key] = original
    finally:
        await factory.kw["bind"].dispose()
