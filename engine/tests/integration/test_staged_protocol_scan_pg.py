"""Native monitor admission/price reads/scan around a disposable stage boundary.

Transport waits are controlled Events. This does NOT replace the original
Worker+scan RED test or prove the future SDK/tool-session adapter is complete.
"""

import asyncio
from datetime import timedelta
from uuid import UUID

import pytest
from helpers.paper_batch import CALENDAR_ID
from helpers.staged_protocol import StagedProtocol
from integration import test_monitor_mixed_increment_pg as mixed
from integration.test_staged_protocol_pg import controlled_observe
from nexus.agents.persistence import load_frozen_evidence, revalidate_frozen_price_authority
from nexus.jobs import worker as job_worker
from nexus.ops.research_monitor import scan_configured_once
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.monitoring import MonitorCheck
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import Job, OutboxEvent
from nexus.worker import run_agent_worker_once as native_worker
from sqlalchemy import select, text


class NativeClaimCaptured(BaseException):
    pass


@pytest.mark.asyncio
async def test_normal_scan_commits_while_isolated_model_and_cleanup_wait_without_db_transaction(
    migrated_database_url, monkeypatch, tmp_path
):
    observed = {}
    prepare = mixed.prepared_materials

    async def capture_materials(*args, **kwargs):
        result = await prepare(*args, **kwargs)
        observed["fixture"] = result
        return result

    async def capture_claim(session, **kwargs):
        observed["claim"] = kwargs["claim"]
        raise NativeClaimCaptured

    async def stop_native_after_claim(settings, **kwargs):
        observed["settings"] = settings
        with monkeypatch.context() as patch:
            patch.setattr(job_worker, "execute_and_finalize_job", capture_claim)
            await native_worker(settings, **kwargs)

    monkeypatch.setattr(mixed, "prepared_materials", capture_materials)
    monkeypatch.setattr("nexus.worker.run_agent_worker_once", stop_native_after_claim)
    with pytest.raises(NativeClaimCaptured):
        await mixed.test_sealed_price_and_two_filings_admit_one_single_stock_job_and_three_calls(
            migrated_database_url, monkeypatch, tmp_path, unknown="unresolved"
        )
    factory, _, _, captured, _, _, _ = observed["fixture"]
    claim = observed["claim"]
    settings = observed["settings"]
    now = captured["clock"][0]
    protocol = StagedProtocol(factory)
    model_entered, model_release = asyncio.Event(), asyncio.Event()
    cleanup_entered, cleanup_release = asyncio.Event(), asyncio.Event()
    task = None
    try:
        async with factory() as session:
            scope = await session.scalar(select(AgentPaperCycleRecord.id))
            job = await session.get(Job, claim.job_id)
            run_id = UUID(job.payload["run_id"])
            checks_before = set(await session.scalars(select(MonitorCheck.id)))
            notifications = set(await session.scalars(select(OutboxEvent.id)))
        await protocol.install(
            scope_id=scope,
            claims=(claim,),
            now=now,
            work_deadline=now + timedelta(seconds=100),
            cleanup_deadline=now + timedelta(seconds=110),
        )
        # Full native frozen-price validator still takes its original locks.
        # Finish this read before entering any external wait.
        async with factory.begin() as session:
            run = await session.get(AgentResearchRunRecord, run_id)
            evidence = await load_frozen_evidence(session, run, allow_fixture_evidence=False)
            assert evidence
            status = await revalidate_frozen_price_authority(
                session, run=run, evidence_records=evidence, allow_fixture=False
            )
            assert status.value != "invalid"
        ticket = await protocol.authorize(claim, stage=1, now=now, wire=b"isolated-stage-envelope")

        async def transport():
            model_entered.set()
            await model_release.wait()
            await controlled_observe(
                protocol, ticket, now=now, payload=b"known-response", tokens=(100, 100)
            )
            cleanup_entered.set()
            await cleanup_release.wait()
            await protocol.cleanup(ticket, now=now, confirmed=True)

        task = asyncio.create_task(transport())
        for phase in ("model", "cleanup"):
            await asyncio.wait_for(
                (model_entered if phase == "model" else cleanup_entered).wait(), 10
            )
            async with factory() as observer:
                assert (
                    await observer.scalar(text("SELECT count(*) FROM stage_proto_authorization"))
                    == 1
                )
                # No transaction is carried by the waiting external operation.
                assert (
                    await observer.scalar(
                        text(
                            "SELECT count(*) FROM pg_stat_activity "
                            "WHERE datname=current_database() "
                            "AND pid<>pg_backend_pid() AND state='idle in transaction'"
                        )
                    )
                    == 0
                )
            scanned = await scan_configured_once(settings, calendar_authority_id=CALENDAR_ID)
            assert scanned["new_triggers"] == scanned["notifications"] == 0
            assert not task.done()
            async with factory() as observer:
                checks = set(await observer.scalars(select(MonitorCheck.id)))
                assert checks - checks_before
                assert set(await observer.scalars(select(OutboxEvent.id))) == notifications
                assert (await observer.get(Job, claim.job_id)).status == "running"
                if phase == "cleanup":
                    assert (
                        await observer.scalar(
                            text("SELECT input_tokens FROM stage_proto_observation")
                        )
                        == 100
                    )
            if phase == "model":
                model_release.set()
        cleanup_release.set()
        await task
    finally:
        model_release.set()
        cleanup_release.set()
        if task is not None and not task.done():
            await task
        await factory.kw["bind"].dispose()
