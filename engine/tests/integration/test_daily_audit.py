"""Actual manual jobs and audit chain: bounded observation, never business replay."""

import asyncio
import gzip
import importlib
import json
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from nexus.audit.service import append_audit_event
from nexus.jobs.daily_manual import enqueue_manual_daily_root, manual_daily_job_ids
from nexus.jobs.models import QueueName
from nexus.jobs.repository import claim_next_job
from nexus.persistence.models.runtime import AuditChainManifest, AuditEvent, Job
from sqlalchemy import func, select, text

from engine.tests.integration.test_daily_manual import _request, _seed, _tick


class Store:
    def __init__(self):
        self.objects = {}
        self.fail = False

    async def put_if_absent(self, *, key, content):
        if self.fail:
            raise RuntimeError("private-storage-canary")
        assert key not in self.objects or self.objects[key] == content
        self.objects[key] = content
        return f"s3://synthetic-audit/{key}"


def _service():
    assert importlib.util.find_spec("nexus.jobs.daily_audit") is not None, "audit service missing"
    return importlib.import_module("nexus.jobs.daily_audit").audit_manual_daily


async def _root(url):
    factory = await _seed(url)
    async with factory() as session, session.begin():
        root = await enqueue_manual_daily_root(
            session,
            request=_request(datetime.now(UTC)),
            now=datetime.now(UTC),
        )
        return factory, root.id


@pytest.mark.asyncio
async def test_observation_is_private_scoped_replayed_and_manifest_covered(migrated_database_url):
    audit = _service()
    factory, root = await _root(migrated_database_url)
    store = Store()
    first = await audit(factory, root_id=root, object_store=store)
    again = await audit(factory, root_id=root, object_store=store)
    assert first["status"] == "recorded"
    assert first["manifest_status"] == again["manifest_status"] == "sealed"
    assert first["event_id"] == again["event_id"]
    assert first["facts_hash"] == again["facts_hash"]
    assert first["scope"] == {
        "root_and_diagnostics": True,
        "research": "not_selected",
        "excluded": ["proposals", "paper", "performance"],
    }
    assert first["execution_locked"] is True
    async with factory() as session:
        events = list(await session.scalars(select(AuditEvent)))
        assert len(events) == 1
        assert events[0].payload["facts"]["jobs"][0]["status"] == "queued"
        assert events[0].payload["facts"]["root_id"] == str(root)
        assert await session.scalar(select(func.count()).select_from(Job)) == 1
        manifest = await session.scalar(select(AuditChainManifest))
        assert manifest.first_event_seq <= events[0].event_seq <= manifest.last_event_seq
    document = gzip.decompress(next(iter(store.objects.values()))).decode()
    assert str(root) not in document
    assert "snapshot_id" not in document and "facts" not in document
    assert json.loads(document)["outcome"] == {"status": "recorded"}


@pytest.mark.asyncio
async def test_changed_stages_append_without_relabeling_existing_daily_manifest(
    migrated_database_url,
):
    audit = _service()
    factory, root = await _root(migrated_database_url)
    store = Store()
    before = await audit(factory, root_id=root, object_store=store)
    for _ in range(3):
        assert await _tick(factory, root)
    after = await audit(factory, root_id=root, object_store=store)
    assert after["facts_hash"] != before["facts_hash"]
    assert after["manifest_status"] == "pending"
    assert len(store.objects) == 1
    async with factory() as session:
        event = await session.get(AuditEvent, UUID(after["event_id"]))
        assert [job["status"] for job in event.payload["facts"]["jobs"]] == ["succeeded"] * 3
        assert len(await manual_daily_job_ids(session, root_id=root)) == 3
    tomorrow = datetime.now(UTC) + timedelta(days=1)
    sealed = await audit(factory, root_id=root, object_store=store, clock=lambda: tomorrow)
    assert sealed["event_id"] == after["event_id"]
    assert sealed["manifest_status"] == "sealed"
    assert len(store.objects) == 2


@pytest.mark.asyncio
async def test_storage_failure_keeps_committed_observation_and_safe_unknown(migrated_database_url):
    audit = _service()
    factory, root = await _root(migrated_database_url)
    store = Store()
    store.fail = True
    result = await audit(factory, root_id=root, object_store=store)
    assert result["status"] == "recorded" and result["manifest_status"] == "unknown"
    assert "private-storage" not in json.dumps(result)
    async with factory() as session:
        assert await session.get(AuditEvent, UUID(result["event_id"])) is not None
        assert await session.scalar(select(func.count()).select_from(AuditChainManifest)) == 0
    store.fail = False
    replay = await audit(factory, root_id=root, object_store=store)
    assert replay["event_id"] == result["event_id"] and replay["manifest_status"] == "sealed"


@pytest.mark.asyncio
async def test_concurrent_observers_share_one_fact_event(migrated_database_url):
    audit = _service()
    factory, root = await _root(migrated_database_url)
    store = Store()
    results = await asyncio.gather(
        *(audit(factory, root_id=root, object_store=store) for _ in range(4))
    )
    assert len({item["event_id"] for item in results}) == 1
    assert {item["manifest_status"] for item in results} == {"sealed"}
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(AuditEvent)) == 1
    assert len(store.objects) == 1


async def _wait_blocked(factory):
    async with asyncio.timeout(5):
        while True:
            async with factory() as session:
                if await session.scalar(
                    text(
                        "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() "
                        "AND wait_event_type='Lock' AND pid<>pg_backend_pid()"
                    )
                ):
                    return
            await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_observer_does_not_wait_on_running_worker_business_locks(migrated_database_url):
    audit = _service()
    factory, root = await _root(migrated_database_url)
    async with factory() as writer:
        async with writer.begin():
            claim = await claim_next_job(
                writer,
                queue=QueueName.GENERAL,
                worker_id="audit-concurrent-fixture",
                now=datetime.now(UTC),
                job_ids=(root,),
            )
            assert claim is not None
            result = await asyncio.wait_for(audit(factory, root_id=root, object_store=Store()), 1)
    async with factory() as session:
        event = await session.get(AuditEvent, UUID(result["event_id"]))
        assert event.payload["facts"]["jobs"][0]["status"] == "queued"
        assert event.payload["facts"]["jobs"][0]["attempt_count"] == 0
    fresh = await audit(factory, root_id=root, object_store=Store())
    async with factory() as session:
        event = await session.get(AuditEvent, UUID(fresh["event_id"]))
        assert event.payload["facts"]["jobs"][0]["status"] == "running"


@pytest.mark.asyncio
async def test_waiting_for_global_chain_lock_does_not_keep_stale_head(migrated_database_url):
    audit = _service()
    factory, root = await _root(migrated_database_url)
    async with factory() as writer:
        async with writer.begin():
            earlier = await append_audit_event(
                writer,
                actor_id="system:test",
                action="audit.synthetic",
                schema_version="test_v1",
                payload={"status": "recorded"},
                occurred_at=datetime.now(UTC),
            )
            task = asyncio.create_task(audit(factory, root_id=root, object_store=Store()))
            await _wait_blocked(factory)
        result = await asyncio.wait_for(task, 5)
    async with factory() as session:
        event = await session.get(AuditEvent, UUID(result["event_id"]))
        assert event.event_seq == earlier.event_seq + 1
        assert event.previous_hash == earlier.event_hash


@pytest.mark.asyncio
async def test_root_materialization_during_observation_cannot_mix_snapshots(
    migrated_database_url,
    monkeypatch,
):
    audit = _service()
    module = importlib.import_module("nexus.jobs.daily_audit")
    factory, root = await _root(migrated_database_url)
    read_ids = module.manual_daily_job_ids

    async def materialize_between_reads(session, *, root_id):
        assert await _tick(factory, root_id)
        return await read_ids(session, root_id=root_id)

    monkeypatch.setattr(module, "manual_daily_job_ids", materialize_between_reads)
    result = await audit(factory, root_id=root, object_store=Store())
    async with factory() as session:
        event = await session.get(AuditEvent, UUID(result["event_id"]))
        assert len(event.payload["facts"]["jobs"]) == 1
        assert event.payload["facts"]["jobs"][0]["status"] == "queued"
        assert len(await manual_daily_job_ids(session, root_id=root)) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("uncertain_transaction", [2, 3])
async def test_commit_acknowledgement_failure_never_claims_rollback(
    migrated_database_url,
    uncertain_transaction,
):
    from contextlib import asynccontextmanager

    audit = _service()
    factory, root = await _root(migrated_database_url)
    calls = 0

    @asynccontextmanager
    async def uncertain_factory():
        nonlocal calls
        calls += 1
        async with factory() as session:
            yield session
        if calls == uncertain_transaction:
            raise RuntimeError("private-commit-ack-canary")

    store = Store()
    result = await audit(uncertain_factory, root_id=root, object_store=store)
    assert result["manifest_status"] == "unknown"
    assert result["status"] == ("unknown" if uncertain_transaction == 2 else "recorded")
    assert "private-commit" not in json.dumps(result)
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(AuditEvent)) == 1
    replay = await audit(factory, root_id=root, object_store=store)
    assert replay["status"] == "recorded" and replay["manifest_status"] == "sealed"
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(AuditEvent)) == 1


@pytest.mark.asyncio
async def test_wrong_root_or_selected_run_never_writes_audit(migrated_database_url):
    audit = _service()
    factory, root = await _root(migrated_database_url)
    for selected_root, run in [(UUID(int=9), None), (root, UUID(int=10))]:
        with pytest.raises(ValueError, match="DAILY_AUDIT_BINDING_INVALID"):
            await audit(factory, root_id=selected_root, run_id=run, object_store=Store())
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(AuditEvent)) == 0


@pytest.mark.asyncio
async def test_selected_unknown_research_is_bound_and_audited_through_normal_cli_service(
    migrated_database_url,
    monkeypatch,
):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from helpers.diagnostic_artifacts import FixtureArtifactReader
    from helpers.research_snapshots import INSTRUMENT, NOW
    from nexus.agents import codex_staged
    from nexus.ops import daily_once
    from nexus.ops.openai_local import prepare_database
    from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
    from nexus.persistence.models.backtest import (
        ExchangeCalendarAuthorityRecord,
        ExchangeCalendarSessionRecord,
    )
    from nexus.persistence.session import session_factory

    from engine.tests.integration.test_daily_research_recovery import _settings
    from engine.tests.integration.test_research_explicit_portfolio import _seed as research_seed

    config = _settings(migrated_database_url)
    await prepare_database(config, structured_synthesis=True)
    factory = session_factory(config)
    close = NOW - timedelta(hours=8)
    async with factory() as session, session.begin():
        _, snapshot = await research_seed(session)
        calendar = ExchangeCalendarAuthorityRecord(
            exchange="XNYS",
            version="audit-test",
            source="fixture",
            range_start=close.date(),
            range_end=close.date(),
            query_parameters={},
            completeness_status="complete",
            content_hash="f" * 64,
            published_at=close,
        )
        session.add(calendar)
        await session.flush()
        session.add(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=calendar.id,
                session=close.date(),
                market_open_at=close - timedelta(hours=6, minutes=30),
                market_close_at=close,
                decision_at=close + timedelta(hours=1),
            )
        )
        model_id = await session.scalar(select(AgentModelConfigRecord.id))
    monkeypatch.setattr(
        daily_once,
        "S3ProviderEvidenceStore",
        lambda _: SimpleNamespace(
            read_normalized=FixtureArtifactReader({}),
        ),
    )
    admitted = await daily_once.run_once(
        config,
        daily_once.DailyAdmitRequest(
            action="admit",
            snapshot_id=snapshot.id,
            calendar_authority_id=calendar.id,
            session=close.date(),
            cutoff_at=datetime.now(UTC),
        ),
    )
    root = UUID(admitted["root_id"])
    await daily_once.run_once(
        config, daily_once.DailyAdvanceRequest(action="advance", root_id=root)
    )
    research = await daily_once.run_once(
        config,
        daily_once.DailyResearchAdmitRequest(
            action="research-admit",
            root_id=root,
            instrument_id=INSTRUMENT,
            research_as_of=datetime.now(UTC),
            model_config_id=model_id,
        ),
    )
    run_id = UUID(research["run_id"])
    transport = AsyncMock(side_effect=RuntimeError("private-transport-canary"))
    monkeypatch.setattr(codex_staged, "invoke_codex", transport)
    await daily_once.run_once(
        config,
        daily_once.DailyResearchRunRequest(
            action="research-run",
            root_id=root,
            run_id=run_id,
        ),
    )
    assert transport.await_count == 1

    def forbidden(*args, **kwargs):
        pytest.fail("audit must never invoke a worker or a provider reader")

    for name in ("run_job_worker_once", "run_agent_worker_once", "S3ProviderEvidenceStore"):
        monkeypatch.setattr(daily_once, name, forbidden)
    store = Store()
    monkeypatch.setattr(daily_once, "S3AuditManifestObjectStore", lambda _: store)
    request = daily_once.DailyAuditRequest(action="audit", root_id=root, run_id=run_id)
    result = await daily_once.run_once(config, request)
    assert result["status"] == "recorded" and result["scope"]["research"] == "selected"
    replay = await daily_once.run_once(config, request)
    assert result["event_id"] == replay["event_id"]
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, run_id)
        event = await session.get(AuditEvent, UUID(result["event_id"]))
        facts = event.payload["facts"]["research"]
        assert facts["status"] == run.status == "failed"
        assert facts["latest_output"]["reason"] == "model_usage_uncertain"
        assert facts["reported_input_tokens"] == run.input_tokens
        assert facts["reported_output_tokens"] == run.output_tokens
        assert len(await manual_daily_job_ids(session, root_id=root)) == 3
    assert transport.await_count == 1
    assert "private-transport-canary" not in json.dumps(result)
    assert str(root) not in gzip.decompress(next(iter(store.objects.values()))).decode()
