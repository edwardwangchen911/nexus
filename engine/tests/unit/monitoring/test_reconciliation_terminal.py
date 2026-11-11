"""Monitoring consumes the public staged closeout proof without relaxing native identity."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from nexus.monitoring import admission, reconciliation


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("finished", "consumed", "staged"),
    [(True, False, True), (False, False, True), (True, True, False), (False, True, True)],
)
async def test_reconciliation_preserves_legacy_fast_path_and_staged_cleanup(
    monkeypatch: pytest.MonkeyPatch, finished: bool, consumed: bool, staged: bool
) -> None:
    now = datetime(2026, 11, 11, tzinfo=UTC)
    run = SimpleNamespace(id=uuid4(), instrument_id=uuid4(), decision_time=now, status="failed")
    job = SimpleNamespace(
        status="failed" if consumed else "dead_letter",
        job_type="agent.paper.research",
        schema_version="agent_research_job_v1",
        payload={"run_id": str(run.id)},
        max_attempts=1,
        manual_retry_count=0,
        attempt_count=1,
        scope_kind="instrument",
        scope_id=str(run.instrument_id),
        effective_at=now,
        input_hash="original-frozen-hash",
        id=uuid4(),
    )
    lease = SimpleNamespace(job_id=job.id)
    session = SimpleNamespace(
        scalars=AsyncMock(side_effect=[(job,), (lease,)]), scalar=AsyncMock(return_value=staged)
    )
    monkeypatch.setattr(
        "nexus.agents.codex_formal_stage._formal_stage_job_hash", lambda _: "original-frozen-hash"
    )
    monkeypatch.setattr(reconciliation, "job_lease_consumed", AsyncMock(return_value=consumed))
    public = AsyncMock(return_value=finished)
    monkeypatch.setattr(admission, "_monitor_job_finished", public)
    assert await reconciliation._native_finished(session, run, now) is finished
    if consumed and not staged:
        public.assert_not_awaited()
    else:
        public.assert_awaited_once_with(
            session, run=run, job=job, leases=(lease,), now=now, rejected_before_claim=False
        )


@pytest.mark.asyncio
async def test_terminal_proof_never_overrides_wrong_original_job_hash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 11, 11, tzinfo=UTC)
    run = SimpleNamespace(id=uuid4(), instrument_id=uuid4(), decision_time=now)
    job = SimpleNamespace(
        status="dead_letter",
        job_type="agent.paper.research",
        schema_version="agent_research_job_v1",
        payload={"run_id": str(run.id)},
        max_attempts=1,
        manual_retry_count=0,
        attempt_count=1,
        scope_kind="instrument",
        scope_id=str(run.instrument_id),
        effective_at=now,
        input_hash="different-frozen-hash",
        id=uuid4(),
    )
    session = SimpleNamespace(
        scalars=AsyncMock(side_effect=[(job,), (SimpleNamespace(job_id=job.id),)])
    )
    monkeypatch.setattr(
        "nexus.agents.codex_formal_stage._formal_stage_job_hash", lambda _: "original-frozen-hash"
    )
    public = AsyncMock(return_value=True)
    monkeypatch.setattr(admission, "_monitor_job_finished", public)
    assert not await reconciliation._native_finished(session, run, now)
    public.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["MONITOR_RUN_FAILED", "MONITOR_UNKNOWN_USAGE", None])
async def test_disclosure_check_reason_requires_a_receipt_for_exact_latest_run(
    monkeypatch: pytest.MonkeyPatch, reason: str | None
) -> None:
    from nexus.monitoring import batches

    now = datetime(2026, 11, 11, tzinfo=UTC)
    latest = uuid4()
    plan = SimpleNamespace(account_id=uuid4(), source_run_id=uuid4())
    monkeypatch.setattr(batches, "_latest_disclosure_source_id", AsyncMock(return_value=latest))
    session = SimpleNamespace(
        scalar=AsyncMock(return_value=None if reason is None else SimpleNamespace(reason=reason))
    )
    assert await batches.disclosure_inactive_reason(session, plan, now) == (
        reason or "MONITOR_AWAITING_RECONCILIATION"
    )
    query = session.scalar.call_args.args[0]
    params = query.compile().params
    assert latest in params.values() and plan.account_id in params.values()
    assert "disclosure" in params.values() and now in params.values()
