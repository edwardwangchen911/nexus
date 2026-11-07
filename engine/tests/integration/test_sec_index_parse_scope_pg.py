"""Fixed JSON mapping reuse never reuses capture/time/original qualification."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import httpx
import pytest
from integration.test_sec_index_pg import response, seed, submissions
from nexus.ops.sec_index_local import collect_index
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.session import session_factory
from nexus.research import identity_sources, sec_index_storage, sec_listed_storage


@pytest.mark.asyncio
@pytest.mark.parametrize("listed", [False, True])
async def test_reader_reuses_only_pure_mapping_inside_existing_scope(
    migrated_database_url, tmp_path, monkeypatch, listed
):
    settings, store, identity, parent = await seed(migrated_database_url, tmp_path)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(response(submissions(), []))
    ) as client:
        result = await collect_index(
            settings,
            identity_snapshot_id=identity.snapshot_id,
            archive_dir=parent / "index",
            contact="test@example.invalid",
            client=client,
            store=store,
            listed_history=listed,
        )
    module = sec_listed_storage if listed else sec_index_storage
    reader = module.read_listed_capture if listed else module.read_index_capture
    parser = identity_sources.parse_identity_source
    directories = []

    def counted(raw):
        value = parser(raw)
        if "fields" in value:
            directories.append(raw)
        return value

    monkeypatch.setattr(identity_sources, "parse_identity_source", counted)
    monkeypatch.setattr(module, "parse_identity_source", counted)
    factory = session_factory(settings)
    try:
        async with factory() as db:
            snapshot_id = UUID(result["snapshot_id"])
            snapshot = await db.get(DataSnapshot, snapshot_id)
            args = dict(snapshot_id=snapshot_id, as_of=datetime.now(UTC))
            with identity_sources.identity_source_parse_scope():
                first = await reader(db, store, **args)
                assert await reader(db, store, **args) == first
                assert len(directories) == 1
                with pytest.raises(ValueError, match="SEC_INDEX_CAPTURE_UNAVAILABLE"):
                    await reader(
                        db,
                        store,
                        snapshot_id=snapshot_id,
                        as_of=snapshot.created_at - timedelta(microseconds=1),
                    )
                capture = (module.ListedCapture if listed else module.IndexCapture).model_validate(
                    snapshot.payload
                )
                key = capture.originals[0].uri.split("s3://synthetic/", 1)[1]
                original = store.objects[key]
                store.objects[key] = b"changed fixed original"
                with pytest.raises(ValueError):
                    await reader(db, store, **args)
                store.objects[key] = original
            with identity_sources.identity_source_parse_scope():
                assert await reader(db, store, **args) == first
            assert len(directories) == 2
    finally:
        await factory.kw["bind"].dispose()
