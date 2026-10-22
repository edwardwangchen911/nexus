"""A stop during SDK setup is proven uncalled, unlike a lost in-flight response."""

import json

import pytest
from nexus.agents import codex_sdk, codex_staged
from nexus.agents.committee import ROLE_ORDER
from nexus.agents.responses_budget import UsageSnapshot
from unit.agents.test_codex_sdk import _authority, _sdk, _staged_batch, _tools


@pytest.mark.asyncio
@pytest.mark.parametrize("completed_research", [False, True])
async def test_peer_stop_during_sdk_setup_retains_known_usage_without_new_turn(
    monkeypatch, completed_research
):
    request, evidence, prompts = _authority()
    client, thread, response, _ = _sdk(monkeypatch)
    response.final_response = json.dumps(_staged_batch("research", ROLE_ORDER[:5]))
    committee = codex_staged.StagedCodexCommittee(prompts=prompts, tools=_tools())
    if completed_research:
        for role in ROLE_ORDER[:5]:
            await committee.analyze(role, request, evidence, prompts[role])
    prior = committee.usage
    prior_calls = thread.run.await_count
    stopped = False
    account = client.account.return_value

    async def account_with_peer_failure(**_):
        nonlocal stopped
        stopped = True
        return account

    def guard():
        if stopped:
            raise codex_sdk.CodexInvocationError(
                "CODEX_RESEARCH_STOPPED",
                provider_call_started=False,
                usage=UsageSnapshot(),
            )

    client.account.side_effect = account_with_peer_failure
    committee.dispatch_guard = guard
    role = "bear" if completed_research else "orchestrator"
    with pytest.raises(codex_sdk.CodexInvocationError) as caught:
        await committee.analyze(role, request, evidence, prompts[role])
    assert thread.run.await_count == prior_calls
    assert caught.value.provider_call_started is False
    assert caught.value.reason == "CODEX_RESEARCH_STOPPED"
    assert committee.failure_reason == "model_research_stopped"
    assert committee.usage.input_tokens == prior.input_tokens
    assert committee.usage.output_tokens == prior.output_tokens
    assert committee.usage.provider_calls == prior.provider_calls
    assert tuple(committee._outputs) == (ROLE_ORDER[:5] if completed_research else ())


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["incomplete", "usage_missing"])
async def test_response_failure_stops_peers_before_blocked_cleanup(monkeypatch, invalid):
    import asyncio
    from datetime import UTC, datetime
    from uuid import uuid4

    from nexus.agents.research_dispatch import ResearchDispatch, dispatch_member
    from nexus.paper.manual_request import ResearchManifestView, ResearchMemberView

    run_id = uuid4()
    state = ResearchDispatch(
        uuid4(),
        ResearchManifestView(
            manifest_id=uuid4(),
            manifest_hash="a" * 64,
            research_as_of=datetime.now(UTC),
            members=[
                ResearchMemberView(
                    instrument_id=uuid4(),
                    run_id=run_id,
                    job_id=uuid4(),
                    status="prepared",
                    quality=None,
                    usage_known=False,
                )
            ],
        ),
    )
    state.start(run_id)
    client, _, response, _ = _sdk(monkeypatch)
    if invalid == "incomplete":
        response.final_response = ""
    else:
        response.usage = None
    closing = asyncio.Event()
    release = asyncio.Event()

    async def close():
        closing.set()
        await release.wait()

    client.close.side_effect = close
    with dispatch_member(state, run_id):
        task = asyncio.create_task(codex_sdk.invoke_codex({}))
        try:
            await asyncio.wait_for(closing.wait(), 30)
            assert state.stopped, (
                "invalid response was known before cleanup but peers stayed enabled"
            )
        finally:
            release.set()
            with pytest.raises(codex_sdk.CodexInvocationError):
                await task


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_at", ["body", "commit"])
async def test_worker_failure_stops_peers_before_session_cleanup(monkeypatch, failure_at):
    import asyncio
    from datetime import UTC, datetime
    from unittest.mock import AsyncMock
    from uuid import uuid4

    from nexus.agents.research_dispatch import ResearchDispatch, dispatch_member
    from nexus.jobs import worker
    from nexus.jobs.models import QueueName
    from nexus.jobs.registry import JobRegistry
    from nexus.paper.manual_request import ResearchManifestView, ResearchMemberView

    run_id = uuid4()
    state = ResearchDispatch(
        uuid4(),
        ResearchManifestView(
            manifest_id=uuid4(),
            manifest_hash="a" * 64,
            research_as_of=datetime.now(UTC),
            members=[
                ResearchMemberView(
                    instrument_id=uuid4(),
                    run_id=run_id,
                    job_id=uuid4(),
                    status="prepared",
                    quality=None,
                    usage_known=False,
                )
            ],
        ),
    )
    state.start(run_id)
    cleaning = asyncio.Event()
    release = asyncio.Event()

    class Transaction:
        async def __aenter__(self):
            return self

        async def __aexit__(self, kind, error, traceback):
            if kind is None and failure_at == "commit":
                raise RuntimeError("synthetic_commit_failure")

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            cleaning.set()
            await release.wait()

        def begin(self):
            return Transaction()

    for name in ("upsert_worker_heartbeat", "reclaim_expired_jobs", "claim_next_job"):
        monkeypatch.setattr(worker, name, AsyncMock(return_value=None))

    async def preclaim(_):
        if failure_at == "body":
            raise RuntimeError("synthetic_body_failure")

    now = datetime.now(UTC)
    with dispatch_member(state, run_id):
        task = asyncio.create_task(
            worker.run_job_worker_once(
                Session,
                registry=JobRegistry(),
                queue=QueueName.AGENT_PAPER,
                worker_id="synthetic",
                worker_started_at=now,
                process_version="test",
                migration_head="test",
                clock=lambda: now,
                preclaim_guard=preclaim,
            )
        )
        try:
            await asyncio.wait_for(cleaning.wait(), 30)
            assert state.stopped
            with pytest.raises(codex_sdk.CodexInvocationError, match="CODEX_RESEARCH_STOPPED"):
                state.guard()
        finally:
            release.set()
            with pytest.raises(RuntimeError, match=f"synthetic_{failure_at}_failure"):
                await task
