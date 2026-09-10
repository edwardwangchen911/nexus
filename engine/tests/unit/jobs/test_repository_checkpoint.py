"""Unit seams for fenced Job checkpoints."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.jobs.repository import ClaimedJob, LeaseLostError, checkpoint_job

NOW = datetime(2026, 9, 10, 9, tzinfo=UTC)


@pytest.mark.asyncio
async def test_checkpoint_applies_then_extends_the_locked_claim(monkeypatch) -> None:
    claim = _claim()
    job = SimpleNamespace(status="running")
    lease = SimpleNamespace(
        lease_token=claim.lease_token,
        fence_token=claim.fence_token,
        worker_id=claim.worker_id,
        heartbeat_at=NOW,
        expires_at=NOW + timedelta(seconds=30),
    )
    attempt = SimpleNamespace(outcome="running")
    lock = AsyncMock(return_value=(job, lease, attempt))
    monkeypatch.setattr("nexus.jobs.repository._lock_active_claim", lock)
    session = SimpleNamespace(flush=AsyncMock())
    applied = False

    async def apply(_session) -> None:
        nonlocal applied
        applied = True

    expires_at = await checkpoint_job(
        session,
        claim=claim,
        now=NOW + timedelta(seconds=10),
        apply=apply,
        extend_seconds=60,
    )

    assert applied is True
    assert lease.heartbeat_at == NOW + timedelta(seconds=10)
    assert expires_at == NOW + timedelta(seconds=70)
    session.flush.assert_awaited_once()


@pytest.mark.asyncio
async def test_checkpoint_rejects_claim_changed_by_apply(monkeypatch) -> None:
    claim = _claim()
    job = SimpleNamespace(status="running")
    lease = SimpleNamespace(
        lease_token=claim.lease_token,
        fence_token=claim.fence_token,
        worker_id=claim.worker_id,
        heartbeat_at=NOW,
        expires_at=NOW + timedelta(seconds=30),
    )
    attempt = SimpleNamespace(outcome="running")
    monkeypatch.setattr(
        "nexus.jobs.repository._lock_active_claim",
        AsyncMock(return_value=(job, lease, attempt)),
    )
    session = SimpleNamespace(flush=AsyncMock())

    async def apply(_session) -> None:
        lease.fence_token += 1

    with pytest.raises(LeaseLostError, match="changed during checkpoint"):
        await checkpoint_job(session, claim=claim, now=NOW, apply=apply)

    session.flush.assert_not_awaited()


@pytest.mark.asyncio
async def test_checkpoint_rejects_backward_time_before_apply(monkeypatch) -> None:
    claim = _claim()
    job = SimpleNamespace(status="running")
    lease = SimpleNamespace(
        lease_token=claim.lease_token,
        fence_token=claim.fence_token,
        worker_id=claim.worker_id,
        heartbeat_at=NOW + timedelta(seconds=1),
        expires_at=NOW + timedelta(seconds=30),
    )
    attempt = SimpleNamespace(outcome="running")
    monkeypatch.setattr(
        "nexus.jobs.repository._lock_active_claim",
        AsyncMock(return_value=(job, lease, attempt)),
    )
    session = SimpleNamespace(flush=AsyncMock())
    applied = False

    async def apply(_session) -> None:
        nonlocal applied
        applied = True

    with pytest.raises(LeaseLostError, match="move backwards"):
        await checkpoint_job(session, claim=claim, now=NOW, apply=apply)

    assert applied is False
    session.flush.assert_not_awaited()


def _claim() -> ClaimedJob:
    return ClaimedJob(
        job_id=UUID("01990000-0000-7000-8000-000000000001"),
        job_type="agent.portfolio_decision",
        schema_version="portfolio_decision_job_v1",
        payload={},
        queue="general",
        priority=100,
        retry_policy="never",
        max_attempts=1,
        attempt_no=1,
        effective_at=NOW,
        worker_id="worker-checkpoint",
        lease_token=UUID("01990000-0000-7000-8000-000000000002"),
        fence_token=1,
        expires_at=NOW + timedelta(seconds=30),
        version=2,
        dependencies_satisfied=True,
        dependency_result_code=None,
    )
