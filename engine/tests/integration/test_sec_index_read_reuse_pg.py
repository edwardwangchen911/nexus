"""SEC metadata batching preserves caller state and original evidence validation."""

import json
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
import pytest
from integration.test_sec_index_pg import response, seed, submissions
from nexus.ops.sec_index_local import collect_index
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.session import session_factory
from nexus.research import sec_index_storage as module
from sqlalchemy import event, select


async def module_collect(settings, identity, parent, client, store):
    return await collect_index(
        settings,
        identity_snapshot_id=identity.snapshot_id,
        archive_dir=parent / "first",
        contact="test@example.invalid",
        client=client,
        store=store,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scenario",
    [
        "clean",
        "source",
        "uri",
        "sha256",
        "byte_size",
        "created_at",
        "available_at",
        "received_at",
        "observed_at",
        "missing",
        "original_bytes",
        "late_metadata",
        "late_original",
        "dirty_cached",
        "expired_cached",
        "pending_new",
        "deleted_cached",
        "between_calls",
        "late_pending_valid",
        "late_pending_invalid",
    ],
)
async def test_metadata_read_semantics(migrated_database_url, tmp_path, scenario):
    settings, store, identity, parent = await seed(migrated_database_url, tmp_path)
    metadata, requests = submissions(), []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(response(metadata, requests))
    ) as client:
        receipt = await module_collect(settings, identity, parent, client, store)
    assert len(requests) == 2
    factory = session_factory(settings)
    reader = module.read_index_capture
    outcome = None
    queries = []
    mutations = 0
    try:
        async with factory() as session:
            # Keep the snapshot strong just as existing callers may do; do not
            # prime the artifact identity map except for explicit ORM cases.
            snapshot = await session.get(DataSnapshot, UUID(receipt["snapshot_id"]))
            capture = module.IndexCapture.model_validate(snapshot.payload)
            target = capture.originals[3]
            assert len({b.artifact_id for b in capture.originals}) == 4
            as_of = datetime.now(UTC)
            key = target.uri.split("s3://synthetic/", 1)[1]
            retained = []
            local_case = scenario in {
                "source",
                "uri",
                "sha256",
                "byte_size",
                "created_at",
                "available_at",
                "received_at",
                "observed_at",
                "missing",
                "late_metadata",
                "between_calls",
            }
            if local_case:
                # A caller-owned unflushed identity-map change is a real input to
                # AsyncSession.get. Keep autoflush disabled only for these caller
                # modes; never disable database immutability triggers.
                session.autoflush = False

            async def mutate_metadata():
                nonlocal mutations
                artifact = await session.get(DataArtifact, target.artifact_id)
                retained.append(artifact)
                artifact.source = "tampered"
                mutations += 1

            if scenario in {
                "source",
                "uri",
                "sha256",
                "byte_size",
                "created_at",
                "available_at",
                "received_at",
                "observed_at",
                "missing",
            }:
                if scenario == "missing":
                    payload = json.loads(json.dumps(snapshot.payload))
                    payload["originals"][3]["artifact_id"] = str(uuid4())
                    snapshot.payload = payload
                    snapshot.revision = module.sha256(
                        module.IndexCapture.model_validate(payload).canonical()
                    ).hexdigest()
                else:
                    artifact = await session.get(DataArtifact, target.artifact_id)
                    retained.append(artifact)
                    value = {
                        "source": "tampered",
                        "uri": "s3://synthetic/raw/wrong",
                        "sha256": "f" * 64,
                        "byte_size": target.byte_size + 1,
                    }.get(scenario)
                    if scenario.endswith("_at"):
                        value = as_of + timedelta(days=1)
                    setattr(artifact, scenario, value)
            if scenario == "original_bytes":
                store.objects[key] = b"tampered"
            if scenario in {"dirty_cached", "expired_cached", "pending_new", "deleted_cached"}:
                retained = list(
                    await session.scalars(
                        select(DataArtifact).where(
                            DataArtifact.id.in_([b.artifact_id for b in capture.originals])
                        )
                    )
                )
                first = next(a for a in retained if a.id == capture.originals[0].artifact_id)
                if scenario == "dirty_cached":
                    first.source = "tampered"
                elif scenario == "expired_cached":
                    session.expire(first)
                elif scenario == "deleted_cached":
                    await session.delete(first)
                else:
                    session.add(
                        DataArtifact(
                            uri="s3://synthetic/raw/pending",
                            sha256="e" * 64,
                            byte_size=1,
                            mime_type="text/plain",
                            schema_version="synthetic",
                            source="sec",
                            observed_at=as_of,
                            available_at=as_of,
                            received_at=as_of,
                        )
                    )
            native_read = store.read_raw
            read_calls = 0

            async def read_raw(k, *, uri):
                nonlocal read_calls
                raw = await native_read(k, uri=uri)
                read_calls += 1
                if read_calls == 1 and scenario == "late_metadata":
                    await mutate_metadata()
                elif read_calls == 1 and scenario in {"late_pending_valid", "late_pending_invalid"}:
                    session.add(
                        DataArtifact(
                            uri="s3://synthetic/raw/late",
                            sha256="d" * 64,
                            byte_size=1,
                            mime_type="text/plain",
                            schema_version="synthetic",
                            source="sec" if scenario == "late_pending_valid" else None,
                            observed_at=as_of,
                            available_at=as_of,
                            received_at=as_of,
                        )
                    )
                elif read_calls == 1 and scenario == "late_original":
                    store.objects[key] = b"tampered"
                return raw

            store.read_raw = read_raw

            def sql(conn, cursor, statement, parameters, context, executemany):
                queries.append(" ".join(statement.split()))

            event.listen(factory.kw["bind"].sync_engine, "before_cursor_execute", sql)
            try:
                if scenario == "between_calls":
                    await reader(session, store, snapshot_id=snapshot.id, as_of=as_of)
                    await mutate_metadata()
                try:
                    result = await reader(session, store, snapshot_id=snapshot.id, as_of=as_of)
                    outcome = {
                        "status": "accepted",
                        "index_hash": result[1].model_dump(mode="json"),
                    }
                except Exception as error:
                    outcome = {
                        "status": "rejected",
                        "error": type(error).__name__,
                        "code": str(error).splitlines()[0][:180],
                    }
            finally:
                event.remove(factory.kw["bind"].sync_engine, "before_cursor_execute", sql)
            writes = [s for s in queries if s.upper().startswith(("INSERT ", "UPDATE ", "DELETE "))]
            operations = [statement.split()[0] for statement in writes]
            expected_writes = {
                "deleted_cached": ["DELETE"],
                "pending_new": ["INSERT"],
                "late_pending_valid": ["INSERT"],
                "late_pending_invalid": ["INSERT"],
            }
            assert operations == expected_writes.get(scenario, [])
            if scenario in {
                "clean",
                "observed_at",
                "expired_cached",
                "pending_new",
                "late_pending_valid",
            }:
                assert outcome["status"] == "accepted"
            elif scenario in {"deleted_cached", "late_pending_invalid"}:
                assert outcome["status"] == "rejected" and outcome["error"] == "IntegrityError"
            else:
                assert outcome["status"] == "rejected"
                assert outcome["error"] == "ValueError"
                assert outcome["code"] == "SEC_INDEX_ARTIFACT_INVALID"
            if scenario == "late_metadata":
                assert mutations == 1
            if scenario == "clean":
                assert sum("data_artifacts.id IN (" in statement for statement in queries) == 1
            if scenario == "dirty_cached":
                assert not queries

    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scenario",
    [
        "clean",
        "source",
        "uri",
        "sha256",
        "byte_size",
        "created_at",
        "available_at",
        "received_at",
        "observed_at",
        "duplicate_same",
        "duplicate_changed",
        "baseline_observed",
        "observation_observed",
    ],
)
async def test_fast_bindings(migrated_database_url, tmp_path, scenario):
    settings, store, identity, parent = await seed(migrated_database_url, tmp_path)
    requests = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(response(submissions(), requests))
    ) as client:
        original = await module_collect(settings, identity, parent, client, store)
    factory = session_factory(settings)
    try:
        # Construct a new synthetic row rather than changing a frozen one. A
        # new receipt with harmless JSON whitespace supplies a unique hash.
        async with factory() as s, s.begin():
            old = await s.get(DataSnapshot, UUID(original["snapshot_id"]))
            payload = json.loads(json.dumps(old.payload))
            old_binding = module.IndexArchiveBinding.model_validate(payload["originals"][3])
            old_artifact = await s.get(DataArtifact, old_binding.artifact_id)
            key = "raw/sec/probe-new-receipt.json"
            raw = store.objects[old_binding.uri.split("s3://synthetic/", 1)[1]] + b" \n   "
            uri = await store.put_if_absent(key=key, content=raw)
            digest = module.sha256(raw).hexdigest()
            artifact_id = uuid4()
            fields = dict(
                uri=uri,
                sha256=digest,
                byte_size=len(raw),
                source="sec",
                mime_type="application/json",
                schema_version=old_artifact.schema_version,
                observed_at=old_artifact.observed_at,
                available_at=old_artifact.available_at,
                received_at=old_artifact.received_at,
                created_at=old_artifact.created_at,
            )
            if scenario in {"source", "uri", "sha256", "byte_size"}:
                fields[scenario] = {
                    "source": "tampered",
                    "uri": "s3://synthetic/raw/not-this",
                    "sha256": "a" * 64,
                    "byte_size": len(raw) + 1,
                }[scenario]
            elif scenario in {
                "created_at",
                "available_at",
                "received_at",
                "observed_at",
                "baseline_observed",
                "observation_observed",
            }:
                fields["observed_at" if scenario.endswith("observed") else scenario] = datetime.now(
                    UTC
                ) + timedelta(days=1)
            s.add(DataArtifact(id=artifact_id, **fields))
            payload["originals"][3] = {
                "artifact_id": str(artifact_id),
                "uri": uri,
                "sha256": digest,
                "byte_size": len(raw),
            }
            if scenario.startswith("duplicate_"):
                payload["originals"][3] = dict(payload["originals"][1])
                if scenario == "duplicate_changed":
                    payload["originals"][3]["byte_size"] += 1
            capture = module.IndexCapture.model_validate(payload)
            new_id = uuid4()
            snap_fields = {
                c.name: getattr(old, c.name)
                for c in DataSnapshot.__table__.columns
                if c.name not in {"id", "provider_object_id", "revision", "payload"}
            }
            s.add(
                DataSnapshot(
                    id=new_id,
                    provider_object_id=f"{identity.instrument_id}:{digest}",
                    revision=module.sha256(capture.canonical()).hexdigest(),
                    payload=payload,
                    **snap_fields,
                )
            )
        queries = []
        async with factory() as s:
            s.info["sec_exhibit_store"] = store
            assert not s.new and not s.dirty and not s.deleted and not s.identity_map
            native = module.read_index_capture
            reader = native
            from nexus.monitoring import disclosure_context, disclosure_observations

            def sql(conn, cursor, statement, parameters, context, executemany):
                queries.append(" ".join(statement.split()))

            event.listen(factory.kw["bind"].sync_engine, "before_cursor_execute", sql)
            try:
                try:
                    if scenario == "baseline_observed":
                        await disclosure_context._exact_baseline(
                            s,
                            snapshot_id=new_id,
                            instrument_id=identity.instrument_id,
                            as_of=datetime.now(UTC),
                        )
                    elif scenario == "observation_observed":
                        await disclosure_observations._capture(
                            s, store, snapshot_id=new_id, as_of=datetime.now(UTC)
                        )
                    else:
                        await reader(s, store, snapshot_id=new_id, as_of=datetime.now(UTC))
                    result = {"status": "accepted"}
                except Exception as error:
                    result = {
                        "status": "rejected",
                        "error": type(error).__name__,
                        "code": str(error).splitlines()[0][:180],
                    }
            finally:
                event.remove(factory.kw["bind"].sync_engine, "before_cursor_execute", sql)
            writes = [q for q in queries if q.startswith(("UPDATE ", "INSERT ", "DELETE "))]
            selected_batch = any("data_artifacts.id IN (" in q for q in queries)
            assert not writes
            assert selected_batch
            if scenario in {"clean", "observed_at"}:
                assert result["status"] == "accepted"
            else:
                assert result["status"] == "rejected" and result["error"] == "ValueError"
                if scenario == "baseline_observed":
                    assert result["code"] == "SEC_BASELINE_INVALID"
                elif scenario in {"duplicate_same"}:
                    assert result["code"] == "SEC_INDEX_RECEIPT_INVALID"
                else:
                    assert result["code"] == "SEC_INDEX_ARTIFACT_INVALID"
    finally:
        await factory.kw["bind"].dispose()
