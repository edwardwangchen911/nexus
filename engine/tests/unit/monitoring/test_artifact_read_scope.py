"""Immutable registrations may be reused; mutable session semantics may not."""

from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from nexus.persistence.models.data import DataArtifact
from nexus.research.artifact_read_scope import artifact_read_scope, read_artifact
from sqlalchemy import inspect
from sqlalchemy.orm import Session as OrmSession
from sqlalchemy.orm import make_transient_to_detached


class Session:
    def __init__(self):
        self.info = {}
        self.sync_session = self
        self.is_active = True
        self.transaction = SimpleNamespace(is_active=True)
        self.nested = None
        self.new, self.dirty, self.deleted = set(), set(), set()
        self.identity_map = {}
        self.calls = 0
        self.row = DataArtifact(
            id=uuid4(),
            sha256="a" * 64,
            uri="s3://bucket/raw/a?versionId=one",
            byte_size=8,
            mime_type="application/json",
            schema_version="test_v1",
            source="sec",
            observed_at=datetime.now(UTC),
            available_at=datetime.now(UTC),
            received_at=datetime.now(UTC),
            created_at=datetime.now(UTC),
        )

    def get_transaction(self):
        return self.transaction

    def get_nested_transaction(self):
        return self.nested

    def identity_key(self, model, identity):
        return model, identity

    async def scalar(self, query):
        self.calls += 1
        return self.row


@pytest.mark.asyncio
async def test_registration_is_bounded_frozen_and_not_reused_across_scopes():
    session = Session()
    with artifact_read_scope(session, max_entries=1):
        first = await read_artifact(session, "a" * 64)
        reused = await read_artifact(session, "a" * 64)
        assert reused.uri == first.uri and session.calls == 1
        with pytest.raises(FrozenInstanceError):
            reused.uri = "changed"
        await read_artifact(session, "b" * 64)
        await read_artifact(session, "b" * 64)
        assert session.calls == 3
    assert session.info == {}
    with artifact_read_scope(session):
        await read_artifact(session, "a" * 64)
    assert session.calls == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["new", "dirty", "deleted", "identity_map"])
async def test_session_state_always_uses_original_query_and_autoflush(state):
    session = Session()
    with artifact_read_scope(session):
        await read_artifact(session, "a" * 64)
        if state == "identity_map":
            session.identity_map[session.identity_key(DataArtifact, session.row.id)] = session.row
        else:
            getattr(session, state).add(session.row)
        assert await read_artifact(session, "a" * 64) is session.row
        assert session.calls == 2


@pytest.mark.asyncio
async def test_clean_persistent_identity_reuses_same_original_model():
    session = Session()
    orm = OrmSession()
    make_transient_to_detached(session.row)
    orm.add(session.row)
    session.sync_session = orm
    session.identity_map = orm.identity_map
    session.identity_key = lambda model, identity: orm.identity_key(model, identity)
    try:
        assert inspect(session.row).persistent and not inspect(session.row).modified
        with artifact_read_scope(session):
            first = await read_artifact(session, "a" * 64)
            assert await read_artifact(session, "a" * 64) is first
            assert session.calls == 1
        with artifact_read_scope(session):
            assert await read_artifact(session, "a" * 64) is first
            assert session.calls == 2
    finally:
        orm.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["modified", "committed_difference"])
async def test_changed_persistent_model_retains_original_query(change):
    from sqlalchemy.orm.attributes import set_committed_value

    session = Session()
    orm = OrmSession()
    make_transient_to_detached(session.row)
    orm.add(session.row)
    session.sync_session = orm
    session.identity_map = orm.identity_map
    session.identity_key = lambda model, identity: orm.identity_key(model, identity)
    try:
        with artifact_read_scope(session):
            await read_artifact(session, "a" * 64)
            if change == "modified":
                session.row.uri = "s3://bucket/raw/changed?versionId=two"
            else:
                set_committed_value(session.row, "uri", "s3://bucket/raw/changed?versionId=two")
            assert await read_artifact(session, "a" * 64) is session.row
            assert session.calls == 2
    finally:
        orm.close()


@pytest.mark.asyncio
async def test_missing_results_and_exceptions_do_not_survive_scopes():
    session = Session()
    row, session.row = session.row, None
    with pytest.raises(RuntimeError), artifact_read_scope(session):
        assert await read_artifact(session, "a" * 64) is None
        session.row = row
        assert await read_artifact(session, "a" * 64) is row
        raise RuntimeError("stop")
    assert session.info == {} and session.calls == 2
    await read_artifact(session, "a" * 64)
    assert session.calls == 3


@pytest.mark.asyncio
async def test_other_session_cannot_consume_scope_even_if_info_is_shared():
    session, other = Session(), Session()
    with artifact_read_scope(session):
        await read_artifact(session, "a" * 64)
        other.info = session.info
        assert await read_artifact(other, "a" * 64) is other.row
        assert other.calls == 1


@pytest.mark.asyncio
async def test_transaction_and_savepoint_changes_discard_registrations():
    session = Session()
    with artifact_read_scope(session):
        await read_artifact(session, "a" * 64)
        await read_artifact(session, "a" * 64)
        assert session.calls == 1
        session.nested = SimpleNamespace(is_active=True)
        await read_artifact(session, "a" * 64)
        assert session.calls == 2
        session.nested = None
        session.row = None
        assert await read_artifact(session, "a" * 64) is None
        assert session.calls == 3
        session.transaction = SimpleNamespace(is_active=True)
        assert await read_artifact(session, "a" * 64) is None
        assert session.calls == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("inactive", ["session", "outer", "nested"])
async def test_inactive_state_discards_cached_rows_and_never_fills(inactive):
    session = Session()
    session.nested = SimpleNamespace(is_active=True)
    with artifact_read_scope(session):
        await read_artifact(session, "a" * 64)
        await read_artifact(session, "a" * 64)
        assert session.calls == 1
        target = {"session": session, "outer": session.transaction, "nested": session.nested}[
            inactive
        ]
        target.is_active = False
        assert await read_artifact(session, "a" * 64) is session.row
        scope = session.info["nexus_immutable_artifact_read_scope"]
        assert not scope.rows and session.calls == 2
        target.is_active = True
        await read_artifact(session, "a" * 64)
        assert session.calls == 3
