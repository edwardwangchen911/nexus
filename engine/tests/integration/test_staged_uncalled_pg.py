"""Native claimed pure-preparation stop must close atomically and admit normally."""

from datetime import timedelta
from uuid import UUID

import pytest
from helpers.staged_public import install_public_readers
from helpers.staged_worker import run_staged_worker
from integration.test_staged_roots_pg import (
    test_normal_three_stock_roots_use_one_staged_protocol as roots,
)
from integration.test_staged_worker_pg import ObservedCloseout
from nexus.agents.research_dispatch import ResearchDispatch, current_dispatch
from nexus.paper import manual_request, research_request
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.models.runtime import Job, JobAttempt, JobLease
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["normal", "commit", "rollback", "sql_error"])
async def test_claimed_uncalled_stop_closes_with_native_job_and_public_admission(
    migrated_database_url, monkeypatch, tmp_path, outcome, proof_mutation=None
):
    import nexus.worker as worker_module
    from nexus.ops.agent_paper_recovery import inspect_recovery_readiness
    from nexus.ops.manual_paper_host import _dispatch_two_flight_members

    install_public_readers(monkeypatch)
    native_admit = manual_request.admit_manual_request

    async def admit(session, **kwargs):
        return await native_admit(session, **{**kwargs, "two_flight": True})

    monkeypatch.setattr(manual_request, "admit_manual_request", admit)
    native_committee = worker_module._committee
    captured = {}
    native_bind = ResearchDispatch.bind_claim

    def stop_after_real_claim(state, run_id, claim):
        native_bind(state, run_id, claim)
        captured.update(run_id=run_id, claim=claim)
        # Controlled stop at the real claim-publication boundary, before a peer
        # is admitted. The native handler must perform every terminal write.
        state.stop(run_id)

    monkeypatch.setattr(ResearchDispatch, "bind_claim", stop_after_real_claim)

    async def stop_at_preparation(session, settings, run, **kwargs):
        committee = await native_committee(session, settings, run, **kwargs)
        state = current_dispatch(run.id)
        if state is None or run.id not in state.claims:
            return committee
        captured["run_id"] = run.id
        captured["claim"] = state.claims[run.id]
        state.stop(run.id)
        state.guard()

    async def before_worker(shared, settings, _options):
        factory = shared["fixture"][0]
        captured["clock"] = shared["clock"]
        async with factory() as session:
            row = (await session.scalars(select(ManualPaperRequest))).one()
            view = await manual_request.read_manual_request(session, row)

        observer_engine = create_async_engine(migrated_database_url)
        observer_factory = async_sessionmaker(observer_engine, expire_on_commit=False)

        async def observe_uncommitted(preparation, claim):
            async with observer_factory() as observer:
                old_job = await observer.get(Job, claim.job_id)
                old_run = await observer.get(AgentResearchRunRecord, UUID(claim.payload["run_id"]))
                old_budget = await observer.scalar(
                    select(AgentDailyBudgetReservationRecord).where(
                        AgentDailyBudgetReservationRecord.run_id == old_run.id
                    )
                )
                assert old_job.status == "running" and old_run.status == "prepared"
                assert old_budget.state == "reserved" and old_budget.settled_usd is None
                for table in ("agent_research_events", "agent_research_outputs"):
                    assert (
                        await observer.scalar(
                            text(f"SELECT count(*) FROM {table} WHERE run_id=:run"),
                            {"run": old_run.id},
                        )
                        == 0
                    )
            captured["uncommitted_verified"] = True

        captured["uncalled_before_commit_hook"] = observe_uncommitted
        if outcome in {"commit", "rollback"}:
            captured["uncalled_commit_fault"] = outcome == "commit"
        if outcome == "sql_error":
            async with factory.begin() as session:
                await session.execute(
                    text(
                        "CREATE FUNCTION reject_uncalled_terminal() RETURNS trigger "
                        "LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION "
                        "'controlled terminal rejection'; END $$"
                    )
                )
                await session.execute(
                    text(
                        "CREATE TRIGGER reject_uncalled_terminal BEFORE UPDATE ON jobs "
                        "FOR EACH ROW WHEN (OLD.status='running' AND NEW.status='skipped') "
                        "EXECUTE FUNCTION reject_uncalled_terminal()"
                    )
                )

        async def staged(config, **options):
            return await run_staged_worker(
                config,
                monkeypatch=monkeypatch,
                fixture=(factory, None, None, captured, None, None, None),
                **options,
            )

        monkeypatch.setattr(worker_module, "_committee", stop_at_preparation)
        monkeypatch.setattr("nexus.ops.agent_paper_once.run_agent_worker_once", staged)
        if outcome == "normal":
            await _dispatch_two_flight_members(settings, view=view)
        else:
            from helpers.staged_worker import StagedCommitUnconfirmed, StagedFinalizationFailed

            expected = (
                StagedFinalizationFailed if outcome == "sql_error" else StagedCommitUnconfirmed
            )
            with pytest.raises(expected):
                await _dispatch_two_flight_members(settings, view=view)
        assert captured.get("sdk_run_calls", 0) == 0
        if outcome != "sql_error":
            assert captured["uncommitted_verified"]
        if outcome in {"rollback", "sql_error"}:
            async with observer_factory() as session:
                job = await session.get(Job, captured["claim"].job_id)
                run = await session.get(AgentResearchRunRecord, captured["run_id"])
                budget = await session.scalar(
                    select(AgentDailyBudgetReservationRecord).where(
                        AgentDailyBudgetReservationRecord.run_id == run.id
                    )
                )
                lease = (
                    await session.scalars(select(JobLease).where(JobLease.job_id == job.id))
                ).one()
                assert job.status == "running" and run.status == "prepared"
                assert budget.state == "reserved" and budget.settled_usd is None
                assert lease.expires_at == captured["claim"].expires_at
                assert await session.scalar(text("SELECT count(*) FROM agent_research_events")) == 0
                assert (
                    await session.scalar(text("SELECT count(*) FROM agent_research_outputs")) == 0
                )
            await observer_engine.dispose()
            if outcome == "sql_error":
                async with factory.begin() as session:
                    await session.execute(text("DROP TRIGGER reject_uncalled_terminal ON jobs"))
            with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                async with factory.begin() as session:
                    await research_request.finish_research_request(
                        session,
                        row=await session.get(ManualPaperRequest, row.id),
                        cancel_unused=True,
                    )
            blocked = await inspect_recovery_readiness(
                settings.model_copy(
                    update={"agent_paper_cycle_enabled": True, "release_revision": "a" * 40}
                )
            )
            assert blocked["status"] != "ready" and blocked["execution_locked"]
            assert blocked["external_requests"] == 0
            raise ObservedCloseout
        async with observer_factory() as session:
            job = await session.get(Job, captured["claim"].job_id)
            run = await session.get(AgentResearchRunRecord, captured["run_id"])
            budget = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == run.id
                )
            )
            attempt = (
                await session.scalars(select(JobAttempt).where(JobAttempt.job_id == job.id))
            ).one()
            lease = (await session.scalars(select(JobLease).where(JobLease.job_id == job.id))).one()
            assert job.status == "skipped" and job.result_code == "AGENT_ABSTAINED"
            assert attempt.outcome == "skipped" and attempt.finished_at == job.finished_at
            assert lease.expires_at == captured["claim"].expires_at
            assert run.status == "abstained"
            assert (run.input_tokens, run.output_tokens, run.settled_cost_usd) == (0, 0, 0)
            assert budget.state == "released" and budget.settled_usd is None
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_authorization")) == 0
        await observer_engine.dispose()
        if proof_mutation is not None:
            await proof_mutation(factory, row.id, captured)
        from helpers.staged_protocol import StagedProtocol
        from nexus.jobs.repository import LeaseLostError

        with pytest.raises(LeaseLostError):
            await StagedProtocol(factory).install(
                scope_id=row.id,
                claims=(captured["claim"],),
                now=shared["clock"][0],
                work_deadline=shared["clock"][0] + timedelta(seconds=1),
                cleanup_deadline=shared["clock"][0] + timedelta(seconds=2),
                independent=True,
            )
        async with factory.begin() as session:
            closed = await research_request.finish_research_request(
                session, row=await session.get(ManualPaperRequest, row.id), cancel_unused=True
            )
            assert closed.status == "unavailable"
        async with factory.begin() as session:
            repeated = await research_request.finish_research_request(
                session, row=await session.get(ManualPaperRequest, row.id), cancel_unused=True
            )
            assert repeated == closed
        runtime = settings.model_copy(
            update={"agent_paper_cycle_enabled": True, "release_revision": "a" * 40}
        )
        ready = await inspect_recovery_readiness(runtime)
        assert ready["status"] == "ready" and ready["execution_locked"]
        assert ready["external_requests"] == 0
        from integration.test_staged_independent_admission_pg import verify_activation_boundary

        await verify_activation_boundary(runtime, monkeypatch, tmp_path / "activation", ready=True)
        async with factory.begin() as session:
            next_request = await manual_request.admit_manual_request(
                session,
                account_id=row.account_id,
                batch_id=row.candidate_batch_id,
                model_config_id=row.model_config_id,
                mode=row.mode,
                contract_version=row.contract_version,
                idempotency_key="after-uncalled-close",
            )
            assert next_request.status == "queued" and next_request.request_id != row.id
            assert await manual_request.read_manual_request(session, row) == closed
        raise ObservedCloseout

    with pytest.raises(ObservedCloseout):
        await roots(
            migrated_database_url, monkeypatch, tmp_path, True, False, before_worker=before_worker
        )


@pytest.mark.asyncio
async def test_uncalled_proof_rejects_damaged_facts_and_late_writes(
    migrated_database_url, monkeypatch, tmp_path
):
    from helpers.staged_public import _selected_member_settled
    from nexus.paper.research_failure import require_claimed_uncalled_research_closed
    from nexus.persistence.models.agents import AgentResearchEventRecord, AgentResearchOutputRecord
    from sqlalchemy.exc import DBAPIError

    async def reject_damage(factory, request_id, captured):
        # Reader corruption probes remain unflushed. Durable immutability is
        # verified independently below; no database guard is disabled.
        for field in ("event_hash", "output_hash", "budget", "tokens", "lease"):
            async with factory() as session:
                run = await session.get(AgentResearchRunRecord, captured["run_id"])
                event = await session.scalar(
                    select(AgentResearchEventRecord).where(
                        AgentResearchEventRecord.run_id == run.id,
                        AgentResearchEventRecord.sequence == 2,
                    )
                )
                output = await session.scalar(
                    select(AgentResearchOutputRecord).where(
                        AgentResearchOutputRecord.run_id == run.id,
                    )
                )
                budget = await session.scalar(
                    select(AgentDailyBudgetReservationRecord).where(
                        AgentDailyBudgetReservationRecord.run_id == run.id,
                    )
                )
                lease = await session.scalar(
                    select(JobLease).where(
                        JobLease.job_id == captured["claim"].job_id,
                    )
                )
                view = await manual_request.read_manual_request(
                    session, await session.get(ManualPaperRequest, request_id)
                )
                member = next(m for m in view.research.members if m.run_id == run.id)
                now = await research_request.database_now(session)
                if field == "event_hash":
                    event.event_hash = "0" * 64
                elif field == "output_hash":
                    output.content_hash = "0" * 64
                elif field == "budget":
                    budget.state = "reserved"
                elif field == "tokens":
                    run.input_tokens = None
                else:
                    lease.worker_id = "wrong-worker"
                with session.no_autoflush:
                    with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                        await require_claimed_uncalled_research_closed(
                            session, run_id=run.id, job_id=member.job_id, now=now
                        )
                    with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                        await _selected_member_settled(
                            session,
                            owner_id=request_id,
                            kind="independent",
                            member=member,
                            now=now,
                            allow_unknown_usage=True,
                        )
        async with factory.begin() as session:
            with pytest.raises(DBAPIError):
                async with session.begin_nested():
                    await session.execute(
                        text(
                            "UPDATE agent_research_outputs SET content_hash=:bad WHERE run_id=:run"
                        ),
                        {"bad": "0" * 64, "run": captured["run_id"]},
                    )
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_authorization")) == 0

    await test_claimed_uncalled_stop_closes_with_native_job_and_public_admission(
        migrated_database_url, monkeypatch, tmp_path, "normal", proof_mutation=reject_damage
    )


@pytest.mark.asyncio
async def test_native_nonstop_abstention_keeps_original_expired_lease_policy(
    migrated_database_url, monkeypatch, tmp_path
):
    from integration.test_research_request_pg import prepare_scope
    from nexus.agents import execution
    from nexus.jobs.models import QueueName
    from nexus.jobs.repository import claim_next_job, finalize_job

    factory, settings, store, _ = await prepare_scope(migrated_database_url, tmp_path)

    async def no_visible_evidence(*args, **kwargs):
        return ()

    monkeypatch.setattr(execution, "load_frozen_evidence", no_visible_evidence)
    try:
        async with factory.begin() as session:
            admitted = await manual_request.admit_manual_request(
                session,
                account_id=settings.agent_paper_account_id,
                batch_id=settings.agent_paper_candidate_batch_id,
                model_config_id=settings.agent_paper_formal_model_config_id,
                mode="research_only",
                idempotency_key="native-abstention",
                contract_version=2,
            )
            await manual_request.claim_manual_request(session, request_id=admitted.request_id)
        async with factory.begin() as session:
            session.info["sec_exhibit_store"] = store
            active = await research_request.prepare_research_request(
                session, settings, request_id=admitted.request_id
            )
        for member in active.research.members:
            async with factory.begin() as session:
                now = await research_request.database_now(session)
                run = await session.get(AgentResearchRunRecord, member.run_id)

                async def exact_queued(_session, selected):
                    return selected.id == member.job_id and selected.attempt_count == 0

                claim = await claim_next_job(
                    session,
                    queue=QueueName.GENERAL,
                    worker_id="native-abstention",
                    now=now,
                    lease_seconds=210,
                    job_ids=(member.job_id,),
                    job_types=("agent.paper.research",),
                    research_budget_profile=run.pricing_version,
                    locked_job_guard=exact_queued,
                )
                assert claim is not None
                result = await execution.execute_research_run(
                    session,
                    run_id=run.id,
                    committee=None,
                    allow_fixture_evidence=False,
                )
                assert result.status == "abstained"
                await finalize_job(
                    session, claim=claim, now=now, outcome="skipped", result_code="AGENT_ABSTAINED"
                )

        async def after_expiry(_session):
            return claim.expires_at + timedelta(seconds=1)

        monkeypatch.setattr(research_request, "database_now", after_expiry)
        async with factory.begin() as session:
            closed = await research_request.finish_research_request(
                session, row=await session.get(ManualPaperRequest, admitted.request_id)
            )
            assert closed.status == "unavailable"
            assert all(m.usage_known and m.status == "abstained" for m in closed.research.members)
    finally:
        await factory.kw["bind"].dispose()
