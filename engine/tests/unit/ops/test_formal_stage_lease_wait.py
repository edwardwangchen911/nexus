from dataclasses import replace
from datetime import timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
from nexus.ops import agent_paper_loop as loop
from unit.ops.test_agent_paper_loop import snapshot


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [False, True])
async def test_formal_defer_waits_for_original_claim_and_continues_heartbeats(drift):
    from nexus.agents.research_stages.formal_defer import FormalStageGrantBusy

    job, scope, run = uuid4(), uuid4(), uuid4()
    state = snapshot(
        cycle_id=scope,
        cycle_status="researching",
        research_job_id=job,
        research_job_status="queued",
    )
    notice = FormalStageGrantBusy(
        scope_id=scope, run_id=run, stage=2, claim=SimpleNamespace(job_id=job)
    )
    actions, reads, beats = [], [], []

    async def inspect():
        if len(actions) == 2:
            return replace(state, enabled=False)
        return replace(
            state,
            research_job_status="running" if actions else "queued",
            research_job_id=uuid4() if actions and drift else job,
        )

    async def execute(action, snapshot):
        actions.append(action)
        if len(actions) == 1:
            raise notice
        assert action == loop.AgentPaperLoopAction.RESUME
        assert snapshot.research_job_id == job and snapshot.research_job_status == "running_expired"

    async def sleep(seconds):
        pass

    async def heartbeat(at):
        beats.append(at)

    async def remaining(selected):
        assert selected is notice
        reads.append(1)
        return 0.0 if len(reads) == 3 else 15.0

    stopped = await loop.run_foreground_loop(
        inspect=inspect,
        execute=execute,
        sleep=sleep,
        heartbeat=heartbeat,
        deferred_lease_remaining=remaining,
        heartbeat_interval=timedelta(seconds=15),
    )
    if drift:
        assert stopped == loop.AgentPaperLoopAction.STOP_RESULT_UNKNOWN and reads == []
        assert actions == [loop.AgentPaperLoopAction.RUN_RESEARCH_JOB]
    else:
        assert stopped == loop.AgentPaperLoopAction.STOP_DISABLED
        assert len(beats) >= 3 and len(reads) == 3
        assert actions == [
            loop.AgentPaperLoopAction.RUN_RESEARCH_JOB,
            loop.AgentPaperLoopAction.RESUME,
        ]
