"""Unknown SDK usage must close without waiting for a consumed lease to expire."""

from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from helpers import disclosure_observations
from helpers.formal_sdk import controlled_sdk
from integration import test_candidate_multistock_path as candidates
from integration.test_research_stages_runtime_pg import advancing_formal_clock
from nexus.agents import codex_sdk
from nexus.paper.manual_request import read_manual_request, require_manual_account_available
from nexus.paper.research_request import finish_research_request, require_research_jobs_settled
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.models.runtime import Job, JobLease
from openai_codex.errors import JsonRpcError
from sqlalchemy import select, text


@pytest.mark.asyncio
@pytest.mark.parametrize("expired", [False, True])
@pytest.mark.parametrize("cleanup_confirmed", [True, False])
@pytest.mark.parametrize("rpc_failure", [False, True])
async def test_unknown_root_closes_only_with_consumed_stage_proof(
    migrated_database_url, monkeypatch, tmp_path, expired, cleanup_confirmed, rpc_failure
):
    controlled_sdk(monkeypatch)
    captured = advancing_formal_clock(monkeypatch)
    create, run = codex_sdk.AsyncCodex, candidates.run_agent_worker_once
    clients = []

    def sdk(config):
        client = create(config)
        client.thread_start.return_value.run.side_effect = (
            JsonRpcError(
                -32602, "private-diagnostic-canary", {"secret": "private-diagnostic-canary"}
            )
            if rpc_failure
            else RuntimeError("controlled terminal failure")
        )
        if not cleanup_confirmed:
            client.close.side_effect = RuntimeError("controlled cleanup failure")
        clients.append((client, client.close))
        return client

    monkeypatch.setattr(codex_sdk, "AsyncCodex", sdk)

    class Observed(BaseException):
        pass

    async def checked(settings, **kwargs):
        assert await run(settings, **kwargs)
        factory = captured["factory"]
        async with factory() as session:
            row = (await session.scalars(select(ManualPaperRequest))).one()
            job = await session.get(Job, kwargs["job_ids"][0])
            target = UUID(job.payload["run_id"])
            lease = await session.scalar(select(JobLease).where(JobLease.job_id == job.id))
            assert captured["clock"][0] < lease.expires_at
            if expired:
                captured["clock"][0] = lease.expires_at + timedelta(seconds=1)
            failed = await session.get(AgentResearchRunRecord, target)
            assert job.status == failed.status == "failed"
            assert failed.input_tokens is failed.output_tokens is failed.settled_cost_usd is None
            output = await session.scalar(
                select(AgentResearchOutputRecord).where(AgentResearchOutputRecord.run_id == target)
            )
            assert output.payload["reason"] == "model_usage_uncertain"
            assert output.payload["diagnostic"]["invocation_phase"] == "turn_run"
            assert output.payload["diagnostic"]["failure_facts"].get("rpc_error_code") == (
                -32602 if rpc_failure else None
            )
            assert "private-diagnostic-canary" not in str(output.payload)
            assert (
                await session.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 1
            )
            assert (
                await session.scalar(text("SELECT count(*) FROM research_stage_observation")) == 0
            )
            assert await session.scalar(text("SELECT count(*) FROM research_stage_cleanup")) == int(
                cleanup_confirmed
            )
            assert await session.scalar(text("SELECT known FROM research_stage_terminal")) is False
        if cleanup_confirmed:
            async with factory() as session:
                current = await session.get(ManualPaperRequest, row.id)
                view = await read_manual_request(session, current)
                one = view.research.model_copy(
                    update={"members": [m for m in view.research.members if m.run_id == target]}
                )
                await require_research_jobs_settled(
                    session,
                    row=current,
                    view=one,
                    now=captured["clock"][0],
                    allow_unknown_usage=True,
                )
                with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                    await require_research_jobs_settled(
                        session, row=current, view=one, now=captured["clock"][0]
                    )
                # Persist a different, unconsumed lease in this rollback-only
                # test transaction. The old terminal cannot consume its new fence.
                changed = await session.scalar(select(JobLease).where(JobLease.job_id == job.id))
                changed.fence_token += 1
                changed.lease_token = uuid4()
                changed.acquired_at = changed.heartbeat_at = captured["clock"][0]
                changed.expires_at = captured["clock"][0] + timedelta(seconds=60)
                await session.flush()
                with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                    await require_research_jobs_settled(
                        session,
                        row=current,
                        view=one,
                        now=captured["clock"][0],
                        allow_unknown_usage=True,
                    )
            async with factory.begin() as session:
                result = await finish_research_request(
                    session, row=await session.get(ManualPaperRequest, row.id), cancel_unused=True
                )
                assert result.status == "unknown" and result.code == "MANUAL_RESULT_UNKNOWN"
                assert sorted(m.status for m in result.research.members) == [
                    "abstained",
                    "abstained",
                    "failed",
                ]
            async with factory.begin() as session:
                # Idempotent close does not accept unknown impact or release its budget.
                repeated = await finish_research_request(
                    session, row=await session.get(ManualPaperRequest, row.id), cancel_unused=True
                )
                assert repeated == result
        else:
            with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                async with factory.begin() as session:
                    await finish_research_request(
                        session,
                        row=await session.get(ManualPaperRequest, row.id),
                        cancel_unused=True,
                    )
        async with factory() as session:
            budget = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == target
                )
            )
            assert budget.state == "reserved" and budget.settled_usd is None
            assert await session.scalar(
                text("SELECT count(*) FROM agent_daily_budget_reservations WHERE state='released'")
            ) == (2 if cleanup_confirmed else 0)
            with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                await require_manual_account_available(
                    session, account_id=row.account_id, now=captured["clock"][0], read_only=True
                )
            for table in ("research_monitor_plans", "outbox_events", "paper_orders"):
                assert await session.scalar(text(f"SELECT count(*) FROM {table}")) == 0
        assert len(clients) == 1
        client, original_close = clients[0]
        client.thread_start.return_value.run.assert_awaited_once()
        original_close.assert_awaited_once()
        raise Observed

    monkeypatch.setattr(candidates, "run_agent_worker_once", checked)
    with pytest.raises(Observed):
        await disclosure_observations.reader_source(
            migrated_database_url, monkeypatch, tmp_path, independent=True
        )


@pytest.mark.asyncio
async def test_two_flight_unknowns_close_before_lease_expiry_without_third_call(
    migrated_database_url, monkeypatch, tmp_path
):
    import asyncio

    from helpers import staged_routes
    from integration.test_staged_roots_pg import (
        test_normal_three_stock_roots_use_one_staged_protocol as roots,
    )
    from nexus.agents.research_stages.proof import read_terminal_proof
    from nexus.ops.manual_paper_host import _dispatch_two_flight_members
    from nexus.paper import manual_request

    controlled_sdk(monkeypatch)

    async def noop(*args, **kwargs):
        pass

    monkeypatch.setattr(staged_routes, "ensure_routes", noop)
    monkeypatch.setattr(staged_routes, "register_prepared_runs", noop)
    admit = manual_request.admit_manual_request

    async def two_flight(session, **kwargs):
        return await admit(session, **{**kwargs, "two_flight": True})

    monkeypatch.setattr(manual_request, "admit_manual_request", two_flight)

    class Observed(BaseException):
        pass

    async def before_worker(shared, settings, options):
        factory = shared["fixture"][0]
        async with factory() as session:
            row = (await session.scalars(select(ManualPaperRequest))).one()
            view = await read_manual_request(session, row)
        # Root preparation already gives Worker and database reads the same
        # advancing clock. A second anchor can put public reads behind terminal.
        create, clients, calls, both = codex_sdk.AsyncCodex, [], [], asyncio.Event()

        def sdk(config):
            client = create(config)
            thread = client.thread_start.return_value

            async def failure(*args, **kwargs):
                calls.append(thread)
                if len(calls) == 2:
                    both.set()
                await both.wait()
                raise RuntimeError("controlled terminal failure without usage")

            thread.run.side_effect = failure
            clients.append((thread, client.close))
            return client

        monkeypatch.setattr(codex_sdk, "AsyncCodex", sdk)
        async with asyncio.timeout(90):
            await _dispatch_two_flight_members(settings, view=view)
        async with factory.begin() as session:
            leases = tuple(await session.scalars(select(JobLease)))
            assert len(leases) == 2
            assert all(lease.expires_at > shared["clock"][0] for lease in leases)
            result = await finish_research_request(
                session, row=await session.get(ManualPaperRequest, row.id), cancel_unused=True
            )
            assert result.status == "unknown"
            for member in view.research.members[:2]:
                proof = await read_terminal_proof(
                    session, run_id=member.run_id, now=shared["clock"][0]
                )
                assert not proof.known and proof.cleanup_confirmed
            assert sorted(m.status for m in result.research.members) == [
                "abstained",
                "failed",
                "failed",
            ]
            assert (
                await session.scalar(text("SELECT count(*) FROM research_stage_observation")) == 0
            )
            assert await session.scalar(text("SELECT count(*) FROM research_stage_cleanup")) == 2
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM agent_daily_budget_reservations "
                        "WHERE state='reserved'"
                    )
                )
                == 2
            )
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM agent_daily_budget_reservations "
                        "WHERE state='released'"
                    )
                )
                == 1
            )
            with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                await require_manual_account_available(
                    session, account_id=row.account_id, now=shared["clock"][0], read_only=True
                )
        assert len(clients) == len(calls) == 2
        for thread, original_close in clients:
            thread.run.assert_awaited_once()
            original_close.assert_awaited_once()
        raise Observed

    with pytest.raises(Observed):
        await roots(
            migrated_database_url, monkeypatch, tmp_path, True, False, before_worker=before_worker
        )
