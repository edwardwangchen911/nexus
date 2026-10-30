"""HTTP fact adapter against native transactions; this does not enable a runner route."""

import asyncio
import json
from datetime import timedelta

import httpx
import pytest
from nexus.agents.chatgpt_grant import Grant
from nexus.agents.comparison_claim import claim_comparison_position
from nexus.agents.research_stages.http_lifecycle import (
    HttpStageFailure,
    http_stage_wire,
    invoke_http_stage,
)
from nexus.agents.research_stages.proof import read_terminal_proof
from nexus.agents.research_stages.protocol import StagedProtocol
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text
from test_comparison_routes_pg import prepared_position
from unit.agents.test_http_stage_lifecycle import Answer, Body, terminal


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,has_usage,disconnect",
    [
        ("completed", True, False),
        ("failed", True, False),
        ("incomplete", True, False),
        ("completed", True, True),
        ("failed", False, False),
    ],
)
async def test_http_native_usage_cleanup_common_terminal_and_old_reservation(
    migrated_database_url, status, has_usage, disconnect
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    result, manifest, old = await prepared_position(factory)
    # Transport fact adapter is independent of runner selection: the registered
    # position remains SDK. No test rewrites its model/transport/manifest binding.
    claim = await claim_comparison_position(
        factory, experiment_id=manifest.experiment_id, ordinal=1, worker_id="http-facts-fixture"
    )
    anchor = claim.expires_at - timedelta(seconds=330)
    mono = asyncio.get_running_loop().time()

    def clock():
        return anchor + timedelta(seconds=asyncio.get_running_loop().time() - mono)

    protocol = StagedProtocol(factory)
    await protocol.install(
        scope_id=manifest.experiment_id,
        claims=(claim,),
        now=clock(),
        work_deadline=anchor + timedelta(seconds=300),
        cleanup_deadline=anchor + timedelta(seconds=307),
        comparison=True,
    )
    wire = http_stage_wire({"stage": "research"}, output_model=Answer, forbidden=())
    ticket = await protocol.authorize(claim, stage=1, now=clock(), wire=wire)
    original = httpx.ReadError("fixture disconnect") if disconnect else None
    body = Body(("data: " + json.dumps(terminal(status, has_usage)) + "\n\n").encode(), original)
    sends = []

    def handle(request):
        sends.append(request)
        assert request.content == wire
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=body)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handle))
    grant = Grant({"access_token": "fixture-token"}, anchor.timestamp() + 3600, 0)
    try:
        raw, usage = await invoke_http_stage(
            protocol=protocol,
            scope=manifest.experiment_id,
            ticket=ticket,
            wire=wire,
            output_model=Answer,
            grant=grant,
            clock=clock,
            wall_anchor=anchor,
            monotonic_anchor=mono,
            work_deadline=mono + 30,
            cleanup_deadline=mono + 37,
            client=client,
        )
        assert status == "completed" and not disconnect
        assert json.loads(raw) == {"answer": "own result"} and usage.input_tokens == 20
    except HttpStageFailure as error:
        assert status != "completed" or disconnect
        assert (error.usage is not None) == has_usage
        if disconnect:
            assert error.primary is original
    assert len(sends) == 1 and client.is_closed and body.closed == 1
    # End this fact-only fixture through the actual public failed terminal path;
    # a completed HTTP stage is not a completed three-stage business research.
    assert await protocol.close(claim, now=clock()) is has_usage
    async with factory() as session:
        proof = await read_terminal_proof(session, run_id=result.run_id, now=clock())
        assert proof.known is has_usage and proof.cleanup_confirmed and not proof.orphan
        assert (
            await session.scalar(
                text("SELECT state FROM agent_daily_budget_reservations WHERE id=:id"), {"id": old}
            )
            == "reserved"
        )
        assert await session.scalar(
            text("SELECT state FROM agent_daily_budget_reservations WHERE run_id=:run"),
            {"run": result.run_id},
        ) == ("settled" if has_usage else "reserved")
        assert (
            await session.scalar(
                text("SELECT count(*) FROM research_stage_authorization WHERE run_id=:run"),
                {"run": result.run_id},
            )
            == 1
        )
    if not has_usage:
        await protocol.observe(ticket, now=clock(), payload=b"late-diagnostic", tokens=(20, 9))
        async with factory() as session:
            late = await read_terminal_proof(session, run_id=result.run_id, now=clock())
            assert not late.known and late.evidence_hash == proof.evidence_hash
    with pytest.raises(ValueError, match="COMPARISON_(STOPPED|POSITION_CONSUMED)"):
        await claim_comparison_position(
            factory, experiment_id=manifest.experiment_id, ordinal=1, worker_id="cannot-replay"
        )
