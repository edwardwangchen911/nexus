"""Checks against a normal held, unknown native increment in disposable PostgreSQL."""

from datetime import timedelta
from uuid import uuid4

import httpx
import pytest
from nexus.agents.research_stages.proof import read_terminal_proof
from nexus.agents.research_stages.protocol import ProtocolRejected
from nexus.audit.service import append_audit_event
from nexus.monitoring import original_occupancy as occupancy
from nexus.monitoring.admission import require_monitor_account_available
from nexus.monitoring.durable_sec_permission import SecRequestFence
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession


async def assert_disposition(factory, monkeypatch, account_id, run_id, job_id, failure, now):
    params = dict(run=run_id, job=job_id, account=account_id, now=now, failure=failure)
    async with factory.begin() as db:
        facts = (await db.execute(occupancy._FACTS, params)).mappings().one()
    digest = facts["digest"]
    monkeypatch.setattr(occupancy, "RUN_ID", run_id)
    monkeypatch.setattr(occupancy, "JOB_ID", job_id)
    monkeypatch.setattr(occupancy, "_FAILURE_HASH", failure)
    monkeypatch.setattr(occupancy, "_PROOF_HASH", digest)
    normal_scalar = AsyncSession.scalar

    async def clock(db, statement, *args, **kwargs):
        if str(statement) == "SELECT clock_timestamp()":
            return now
        return await normal_scalar(db, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "scalar", clock)

    async def decide(db, action, *, subject=run_id, proof=digest, at=now):
        return await occupancy.dispose(
            db,
            action=action,
            run_id=subject,
            account_id=account_id,
            now=at,
            expected_hash=proof,
        )

    async def account(db, *, at=now):
        await require_monitor_account_available(db, account_id=account_id, now=at)

    async with factory.begin() as db:
        with pytest.raises(ValueError, match="MONITOR_ACCOUNT_BUSY_OR_UNKNOWN"):
            await account(db)
        assert await occupancy.active_monitor_runs(db, account_id=account_id, now=now) == (run_id,)
        assert (await decide(db, "check"))["status"] == "eligible_at_check"
        for extra in (
            {"subject": uuid4()},
            {"proof": "f" * 64},
            {"at": now - timedelta(minutes=4)},
        ):
            with pytest.raises(ValueError, match=occupancy.BLOCKED):
                await decide(db, "record", **extra)

    # A write followed by any precommit failure is fully rolled back.
    with pytest.raises(RuntimeError, match="precommit failure"):
        async with factory.begin() as db:
            assert (await decide(db, "record"))["status"] == "recorded"
            async with factory.begin() as concurrent:
                with pytest.raises(ValueError, match=occupancy.BLOCKED):
                    await decide(concurrent, "record")
            raise RuntimeError("precommit failure")
    async with factory.begin() as db:
        assert (await decide(db, "check"))["status"] == "eligible_at_check"
        first = await decide(db, "record")
    async with factory.begin() as db:
        assert await decide(db, "check") == first
        assert await decide(db, "record", at=now + timedelta(seconds=1)) == first
        from nexus.agents.research_stages import public

        with monkeypatch.context() as scoped:

            async def should_not_read_terminal(*args, **kwargs):
                pytest.fail("Exact historical occupancy must not decode generic terminal proof")

            scoped.setattr(public, "read_terminal_proof", should_not_read_terminal)
            await account(db, at=now + timedelta(days=1))
        assert not await occupancy.active_monitor_runs(db, account_id=account_id, now=now)
        with pytest.raises(ProtocolRejected, match="STAGED_TERMINAL_PROOF_INVALID"):
            await read_terminal_proof(db, run_id=run_id, now=now)
    async with factory.begin() as db:
        with pytest.raises(ValueError, match=occupancy.BLOCKED):
            await occupancy.require_for_send(db, run_id, fenced_transport=True)
        with pytest.raises(ValueError, match=occupancy.BLOCKED):
            await occupancy.require_for_account_send(db, account_id, fenced_transport=False)
        assert not await occupancy.require_for_account_send(db, uuid4(), fenced_transport=False)

    # A late additional failure fact blocks immediately even when the frozen
    # terminal, old lease, once, and budget have not changed.
    async with factory.begin() as db:
        savepoint = await db.begin_nested()
        await append_audit_event(
            db,
            actor_id="test",
            action="research.stage_failure",
            schema_version="research_stage_failure_v1",
            payload={"run_id": str(run_id)},
            occurred_at=now,
        )
        result = await decide(db, "check")
        assert result == {**first, "status": "inapplicable"}
        with pytest.raises(ValueError, match=occupancy.BLOCKED):
            await occupancy.require_for_account_send(db, account_id, fenced_transport=True)
        await savepoint.rollback()

    # Exercise the actual HTTP header callbacks without sending a request.
    request = httpx.Request("POST", "https://api.openai.com/v1/responses")
    fence = SecRequestFence(factory, None, account_id=account_id)
    await fence.acquire(request)
    parallel = SecRequestFence(factory, None, account_id=account_id)
    second_request = httpx.Request("POST", "https://api.openai.com/v1/responses")
    try:
        await parallel.acquire(second_request)
        await second_request.extensions["trace"]("http2.send_request_headers.started", {})
        async with factory.begin() as db:
            with pytest.raises(ValueError, match=occupancy.BLOCKED):
                await decide(db, "revoke")
            with pytest.raises(ValueError, match=occupancy.BLOCKED):
                await append_audit_event(
                    db,
                    actor_id="test",
                    action="research.stage_failure",
                    schema_version="research_stage_failure_v1",
                    payload={"run_id": str(run_id)},
                    occurred_at=now,
                )
        await request.extensions["trace"]("http11.send_request_headers.started", {})
        await request.extensions["trace"]("http11.send_request_headers.complete", {})
        await second_request.extensions["trace"]("http2.send_request_headers.complete", {})
    finally:
        await fence.release()
        await parallel.release()
    async with factory.begin() as db:
        revoked = await decide(db, "revoke")
        assert revoked["status"] == "revoked"
    with pytest.raises(ValueError, match=occupancy.BLOCKED):
        await fence.acquire(httpx.Request("POST", "https://api.openai.com/v1/responses"))
    async with factory.begin() as db:
        assert await decide(db, "check") == revoked
        assert await occupancy.active_monitor_runs(db, account_id=account_id, now=now) == (run_id,)
        with pytest.raises(ValueError, match="MONITOR_ACCOUNT_BUSY_OR_UNKNOWN"):
            await account(db)
        assert (await db.execute(occupancy._FACTS, params)).mappings().one()["digest"] == digest
        assert (
            await db.scalar(
                select(text("count(*)"))
                .select_from(text("audit_events"))
                .where(text("action LIKE 'research.monitor.original_occupancy_%'"))
            )
            == 2
        )
