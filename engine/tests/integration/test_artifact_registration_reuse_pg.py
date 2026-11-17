"""Clean mapped immutable registrations reuse reads without masking ORM mutations."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from nexus.persistence.models.data import DataArtifact
from nexus.persistence.session import session_factory
from nexus.research.artifact_read_scope import artifact_read_scope, read_artifact
from nexus.settings import EngineSettings
from sqlalchemy import event
from sqlalchemy.exc import DBAPIError


@pytest.mark.asyncio
async def test_mapped_registration_reuse_expiry_and_immutable_update(migrated_database_url):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    now = datetime.now(UTC)
    digest = uuid4().hex * 2
    selects = []

    def queried(conn, cursor, statement, parameters, context, many):
        if statement.startswith("SELECT") and "data_artifacts" in statement:
            selects.append(statement)

    event.listen(factory.kw["bind"].sync_engine, "before_cursor_execute", queried)
    try:
        async with factory.begin() as db:
            db.add(
                DataArtifact(
                    id=uuid4(),
                    sha256=digest,
                    uri="s3://test/raw/a?versionId=one",
                    byte_size=8,
                    mime_type="application/json",
                    schema_version="test_v1",
                    source="sec",
                    observed_at=now,
                    available_at=now,
                    received_at=now,
                )
            )
        async with factory() as db:
            with artifact_read_scope(db):
                first = await read_artifact(db, digest)
                assert isinstance(first, DataArtifact)
                assert await read_artifact(db, digest) is first
                assert len(selects) == 1
                db.expire(first, ["uri"])
                assert await read_artifact(db, digest) is first
                assert first.uri == "s3://test/raw/a?versionId=one"
                assert len(selects) == 2
                first.uri = "s3://test/raw/changed?versionId=two"
                # Reuse must not swallow the normal autoflush and immutable SQL guard.
                with pytest.raises(DBAPIError, match="immutable"):
                    await read_artifact(db, digest)
                await db.rollback()
                restored = await read_artifact(db, digest)
                assert restored.uri == "s3://test/raw/a?versionId=one"
                assert len(selects) == 3
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_listing_reader_reuses_rows_but_rechecks_originals_and_cutoff(
    migrated_database_url, tmp_path
):
    from helpers.current_identity import MemoryStore, archives
    from integration.test_current_listing_revision import config, seed
    from nexus.market.current_listing_revision import read_listing_revision
    from nexus.ops.instrument_identity_local import revise_listing

    class ObservedStore(MemoryStore):
        reads = 0

        async def read_raw(self, key, *, uri):
            self.reads += 1
            return await super().read_raw(key, uri=uri)

    settings = config(migrated_database_url)
    factory = session_factory(settings)
    store = ObservedStore()
    selects = []

    def queried(conn, cursor, statement, parameters, context, many):
        if statement.startswith("SELECT data_artifacts."):
            selects.append(statement)

    event.listen(factory.kw["bind"].sync_engine, "before_cursor_execute", queried)
    try:
        await seed(factory)
        fm, sc = archives(tmp_path / "listing")
        created = await revise_listing(
            settings, symbol="AAPL", fmp_archive=fm, sec_archive=sc, store=store
        )
        selects.clear()
        before_reads = store.reads
        async with factory() as db:
            with artifact_read_scope(db):
                first = await read_listing_revision(db, store, snapshot_id=created.snapshot_id)
                again = await read_listing_revision(db, store, snapshot_id=created.snapshot_id)
                assert first == again
                unique = len({binding.sha256 for binding in first.artifacts})
                assert len(selects) == unique
                assert store.reads - before_reads == len(first.artifacts) * 2
                with pytest.raises(ValueError, match="CURRENT_LISTING_READBACK_INVALID"):
                    await read_listing_revision(
                        db,
                        store,
                        snapshot_id=created.snapshot_id,
                        as_of=first.checked_at - timedelta(microseconds=1),
                    )
    finally:
        await factory.kw["bind"].dispose()
