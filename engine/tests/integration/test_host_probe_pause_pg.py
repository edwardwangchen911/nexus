import asyncio
from datetime import timedelta
from uuid import uuid4

import pytest
from nexus.ops.agent_paper_loop import read_agent_paper_service_heartbeat
from nexus.ops.manual_paper_host import claim_configured_manual_request
from nexus.ops.pending_probe import ReadProbeDeadline
from nexus.paper.host_probe_state import latest_probe_state, publish_probe_state
from nexus.paper.manual_entry import (
    MANUAL_HOST_PROTOCOL,
    manual_host_scope,
    require_configured_manual_host,
)
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.runtime import WorkerHeartbeat
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from nexus.workbench.read_service import WorkbenchReadService
from sqlalchemy import func, select, text


@pytest.mark.asyncio
async def test_existing_reader_rechecks_pause_at_claim_and_new_instance_cannot_erase_it(
    migrated_database_url,
):
    account = uuid4()
    settings = EngineSettings(
        database_url=migrated_database_url,
        release_revision="a" * 40,
        agent_paper_account_id=account,
    )
    factory = session_factory(settings)
    ident = f"agent-paper-loop-{uuid4()}:manual:{manual_host_scope(settings)}"
    version = MANUAL_HOST_PROTOCOL + "+" + "a" * 40
    try:
        async with factory.begin() as s:
            now = await s.scalar(select(func.clock_timestamp()))
            started = now - timedelta(seconds=5)
            s.add(
                WorkerHeartbeat(
                    queue="agent_paper",
                    worker_id=ident,
                    process_version=version,
                    migration_head=CURRENT_MIGRATION_HEAD,
                    started_at=started,
                    seen_at=now,
                )
            )
        binding = dict(account_id=account, worker=ident, started=started, version=version)
        deadline = ReadProbeDeadline("independent_proof", 14500, 15000)
        async with factory.begin() as consumer:
            assert await latest_probe_state(consumer, account) is None
            async with factory.begin() as writer:
                await publish_probe_state(writer, **binding, deadline=deadline)
                await writer.flush()
                consumer_pid = await consumer.scalar(text("select pg_backend_pid()"))
                claim = asyncio.create_task(
                    claim_configured_manual_request(
                        consumer, settings, request_id=uuid4(), calendar_authority_id=None
                    )
                )
                async with asyncio.timeout(5):
                    while not await writer.scalar(
                        text("select pg_backend_pid()=any(pg_blocking_pids(:pid))"),
                        {"pid": consumer_pid},
                    ):
                        await asyncio.sleep(0.01)
            with pytest.raises(ValueError, match="MANUAL_HOST_NOT_READY"):
                await claim
        async with factory.begin() as writer:
            await publish_probe_state(writer, **binding, deadline=deadline)
        async with factory.begin() as consumer:
            from nexus.monitoring.admission import require_monitor_account_available

            await require_monitor_account_available(consumer, account_id=account, now=now)
            with pytest.raises(ValueError, match="MANUAL_HOST_NOT_READY"):
                await claim_configured_manual_request(
                    consumer, settings, request_id=uuid4(), calendar_authority_id=None
                )
            with pytest.raises(ValueError, match="MANUAL_HOST_NOT_READY"):
                await require_configured_manual_host(consumer, settings, now=now)
            state = await latest_probe_state(consumer, account)
            assert state.consecutive == state.total == 2
            loop, _ = await WorkbenchReadService(
                consumer, now, expected_release_revision="a" * 40
            )._loop_status(account)
            assert (loop.status, loop.reason_code) == ("degraded", "READ_PROBE_DEADLINE")
        heartbeat = await read_agent_paper_service_heartbeat(
            migrated_database_url, account_id=account
        )
        assert heartbeat["status"] == "fresh" and heartbeat["dispatch_ready"] is False
        fresh_ident = f"agent-paper-loop-{uuid4()}:manual:{manual_host_scope(settings)}"
        async with factory.begin() as writer:
            from nexus.paper.manual_host_exit import record_host_exit

            await record_host_exit(
                writer, worker_id=ident, started_at=started, process_version=version
            )
            writer.add(
                WorkerHeartbeat(
                    queue="agent_paper",
                    worker_id=fresh_ident,
                    process_version=version,
                    migration_head=CURRENT_MIGRATION_HEAD,
                    started_at=now,
                    seen_at=now,
                )
            )
        async with factory.begin() as writer:
            with pytest.raises(ValueError, match="HOST_READ_PROBE_STATE_INVALID"):
                await publish_probe_state(writer, **binding, deadline=None)
            assert (await latest_probe_state(writer, account)).consecutive == 2
            await publish_probe_state(
                writer,
                account_id=account,
                worker=fresh_ident,
                started=now,
                version=version,
                deadline=None,
            )
        async with factory.begin() as reader:
            state = await latest_probe_state(reader, account)
            assert state.worker == fresh_ident and state.consecutive == state.total == 0
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["price", "market", "material", "disclosure"])
@pytest.mark.parametrize("terminal", ["known", "unknown"])
async def test_monitor_claim_without_known_closure_blocks_pause(
    migrated_database_url, monkeypatch, tmp_path, kind, terminal
):
    from integration.test_monitor_batch_prepare_pg import setup
    from nexus.audit.service import append_audit_event
    from nexus.ops import monitor_batch_prepare as material
    from nexus.ops import monitor_disclosure_refresh as disclosure
    from nexus.ops import monitor_market_prepare as market
    from nexus.ops import monitor_price_refresh as price
    from nexus.ops.monitor_probe_idle import require_monitor_probe_idle
    from nexus.persistence.models.monitoring import MonitorObservationBatch

    factory, _, _, _, batch_id, _ = await setup(migrated_database_url, monkeypatch, tmp_path)
    try:
        async with factory.begin() as s:
            batch = await s.get(MonitorObservationBatch, batch_id)
            account = batch.account_id
            now = await s.scalar(select(func.clock_timestamp()))
            action, result_action, schema = {
                "price": (price._ACTION, price._ACTION, price._SCHEMA),
                "market": (market._ACTION, market._ACTION, market._SCHEMA),
                "material": (material.CLAIM, material.RESULT, material.SCHEMA),
                "disclosure": (disclosure.CLAIM, disclosure.RESULT, disclosure.SCHEMA),
            }[kind]
            payload = dict(
                account_id=str(account),
                batch_id=str(batch_id),
                instrument_id=str(batch.instrument_id),
                operation_id=str(uuid4()),
                scheduled_at=now.isoformat(),
            )
            if kind in {"price", "market"}:
                payload["phase"] = "claimed"
            claim = await append_audit_event(
                s,
                actor_id="research-monitor",
                action=action,
                schema_version=schema,
                payload=payload,
                occurred_at=now,
            )
        async with factory.begin() as s:
            with pytest.raises(ValueError, match="HOST_READ_PROBE_STATE_INVALID"):
                await require_monitor_probe_idle(s, account)
            status = (
                ("refreshed" if kind == "disclosure" else "prepared")
                if terminal == "known"
                else "unknown"
            )
            if kind in {"price", "market"}:
                settled = {
                    **payload,
                    "phase": "result",
                    "claim_id": str(claim.id),
                    "claim_hash": claim.event_hash,
                    "result": {"status": status, "provider_requests": 1},
                }
            else:
                settled = {**payload, "status": status, "provider_requests": 1}
            await append_audit_event(
                s,
                actor_id="research-monitor",
                action=result_action,
                schema_version=schema,
                payload=settled,
                occurred_at=now,
            )
        async with factory.begin() as s:
            if terminal == "known":
                await require_monitor_probe_idle(s, account)
            else:
                with pytest.raises(ValueError, match="HOST_READ_PROBE_STATE_INVALID"):
                    await require_monitor_probe_idle(s, account)
    finally:
        await factory.kw["bind"].dispose()
