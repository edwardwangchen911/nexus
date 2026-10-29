"""Staged interrupted root through existing explicit CLI seal and fresh admission."""

from datetime import timedelta
from uuid import UUID

import pytest
from helpers.staged_protocol import StagedProtocol
from helpers.staged_public import install_public_readers
from helpers.staged_routes import ensure_routes, register_prepared_runs
from integration import test_formal_research_orphan_pg as orphan_case
from integration.test_research_orphan_seal_pg import (
    test_orphan_normal_seal_closes_manual_and_admits_fresh_request as seal_case,
)
from nexus.jobs import worker
from nexus.ops import agent_paper_once
from nexus.persistence.models.agents import AgentResearchRunRecord
from sqlalchemy import text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case", ["same_day", "no_ack", "process_live", "late_fact", "close_rollback"]
)
async def test_staged_root_uses_normal_recovery_seal_audit_and_new_admission(
    migrated_database_url,
    monkeypatch,
    tmp_path,
    case,
    rollback_recovery=False,
    cleanup_confirmed=True,
    controls=None,
):
    install_public_readers(monkeypatch)
    seed = orphan_case.seed_cycle
    start = agent_paper_once.start_schwab_agent_paper_cycle
    controls = controls or {}
    state = {}
    if rollback_recovery:
        from nexus.paper import research_orphan

        close = research_orphan._close_research_orphan

        async def fail_after_close(session, **binding):
            await close(session, **binding)
            raise ValueError("controlled staged recovery rollback")

        monkeypatch.setattr(research_orphan, "_close_research_orphan", fail_after_close)

    async def prepare(session, **kwargs):
        await ensure_routes(session)
        result = await start(session, **kwargs)
        if not result.replayed:
            # Preserve first known and third unused historical siblings unchanged.
            state["target"] = result.research_run_ids[1]
            await register_prepared_runs(session, (state["target"],))
        return result

    async def selected_claim(session, **kwargs):
        await session.execute(text("SET LOCAL nexus.stage_protocol='staged-experiment-v1'"))
        return await native_claim(session, **kwargs)

    native_claim = worker.claim_next_job
    monkeypatch.setattr(worker, "claim_next_job", selected_claim)
    monkeypatch.setattr(agent_paper_once, "start_schwab_agent_paper_cycle", prepare)

    async def root(*args, **kwargs):
        async def capture(settings, factory, authority, manifest, clock):
            state.update(factory=factory, clock=clock)

        kwargs["before_research"] = capture
        interrupted = worker.execute_and_finalize_job

        async def interrupted_with_stage(session, **options):
            try:
                return await interrupted(session, **options)
            except orphan_case.VanishedWorker:
                claim = options["claim"]
                assert UUID(claim.payload["run_id"]) == state["target"]
                async with state["factory"]() as reader:
                    run = await reader.get(AgentResearchRunRecord, state["target"])
                    from helpers.staged_routes import owner
                    from nexus.persistence.models.runtime import Job

                    scope, _ = await owner(reader, run, await reader.get(Job, claim.job_id))
                protocol = StagedProtocol(state["factory"])
                now = state["clock"][0]
                await protocol.install(
                    scope_id=scope,
                    claims=(claim,),
                    now=now,
                    work_deadline=now + timedelta(seconds=100),
                    cleanup_deadline=now + timedelta(seconds=110),
                )
                ticket = await protocol.authorize(
                    claim, stage=1, now=now, wire=b"controlled-unknown-stage"
                )
                if not controls.get("late_cleanup"):
                    await protocol.cleanup(ticket, now=now, confirmed=cleanup_confirmed)
                state.update(ticket=ticket, claim=claim, scope=scope)
                from integration.test_research_orphan_seal_pg import original_facts

                async with state["factory"]() as reader:
                    state["before_recovery"] = await original_facts(reader)
                state["protocol"] = protocol
                raise

        monkeypatch.setattr(worker, "execute_and_finalize_job", interrupted_with_stage)
        return await seed(*args, **kwargs)

    monkeypatch.setattr(orphan_case, "seed_cycle", root)
    if controls.get("configure"):
        await controls["configure"](state, monkeypatch)
    if rollback_recovery:
        from integration.test_research_orphan_seal_pg import original_facts

        with pytest.raises(ValueError, match="controlled staged recovery rollback"):
            await seal_case(migrated_database_url, monkeypatch, tmp_path, case)
        async with state["factory"]() as session:
            assert await original_facts(session) == state["before_recovery"]
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_terminal")) == 0
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_authorization")) == 1
        return
    if not cleanup_confirmed:
        with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
            await seal_case(migrated_database_url, monkeypatch, tmp_path, case)
        async with state["factory"]() as session:
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM audit_events "
                        "WHERE action='paper.legacy_research.sealed'"
                    )
                )
                == 0
            )
            assert (
                await session.scalar(
                    text("SELECT state FROM agent_daily_budget_reservations WHERE run_id=:run"),
                    {"run": state["target"]},
                )
                == "reserved"
            )
        return
    await seal_case(migrated_database_url, monkeypatch, tmp_path, case)
    if controls.get("after_seal"):
        await controls["after_seal"](state)
    async with state["factory"]() as session:
        # Native CLI/API/audit/unknown preservation/new identity assertions above
        # remain intact. Additionally require the same staged recovery proof.
        assert await session.scalar(text("SELECT count(*) FROM stage_proto_terminal")) == 1
        assert await session.scalar(text("SELECT known FROM stage_proto_terminal")) is False
        assert await session.scalar(text("SELECT count(*) FROM stage_proto_authorization")) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["recovery_rollback", "cleanup_unknown"])
async def test_public_recovery_is_atomic_and_cleanup_is_not_an_acceptance(
    migrated_database_url, monkeypatch, tmp_path, boundary
):
    await test_staged_root_uses_normal_recovery_seal_audit_and_new_admission(
        migrated_database_url,
        monkeypatch,
        tmp_path,
        "same_day",
        rollback_recovery=boundary == "recovery_rollback",
        cleanup_confirmed=boundary != "cleanup_unknown",
    )
