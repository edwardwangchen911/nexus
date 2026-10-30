"""Normal v3 maintenance accepts occupancy, never unknown usage or old execution."""

import asyncio
from contextlib import nullcontext
from datetime import timedelta
from types import SimpleNamespace

import pytest
from helpers import disclosure_observations
from helpers.formal_sdk import controlled_sdk
from integration import test_candidate_multistock_path as candidates
from integration.test_research_stages_runtime_pg import advancing_formal_clock
from nexus.agents import codex_sdk
from nexus.ops import manual_paper
from nexus.paper import manual_preparation_close
from nexus.paper.independent_unknown_acceptance import require_independent_execution_open
from nexus.paper.manual_request import (
    require_manual_account_available,
    require_no_running_manual_request,
)
from nexus.paper.research_request import finish_research_request
from nexus.persistence.models.manual_paper import ManualPaperRequest
from sqlalchemy import UUID as UUIDType
from sqlalchemy import select, text


def test_independent_acceptance_cli_requires_explicit_proof_and_consent():
    parser = manual_paper.parser()
    base = ["--repo-root", "/tmp", "--env-file", "infra/.env.local"]
    request = "00000000-0000-0000-0000-000000000001"
    check = parser.parse_args([*base, "check-independent-acceptance", "--request-id", request])
    assert check.action == "check-independent-acceptance"
    with pytest.raises(SystemExit):
        parser.parse_args([*base, "accept-independent-unknown", "--request-id", request])


@pytest.mark.parametrize(
    "case",
    [
        "normal",
        "late_before",
        "late_after",
        "stale_execution_read",
        "next_day",
        "wrong_hash",
        "wrong_scope",
        "wrong_account",
        "legacy_request",
        "unconsumed_lease",
        "concurrent",
        "rollback",
        "live",
        "unrelated_audit",
        "commit_ack_lost",
        "rollback_ack_lost",
        "two_unknown",
        "mixed_completed",
        "mixed_plan",
        "mixed_increment_unknown",
        "stale_history_read",
        "concurrent_history_writer",
        "settled_consumer",
        "readonly_recovery",
        "new_request",
        "budget_preserved",
    ],
)
@pytest.mark.asyncio
async def test_independent_acceptance_preserves_native_facts_and_history_gates(
    migrated_database_url, monkeypatch, tmp_path, case
):
    controlled_sdk(monkeypatch)
    captured = advancing_formal_clock(monkeypatch)
    create, run = codex_sdk.AsyncCodex, candidates.run_agent_worker_once
    import_identity = disclosure_observations.import_baseline_identity

    async def capture_identity(*args, **kwargs):
        receipt = await import_identity(*args, **kwargs)
        captured.setdefault("identity", receipt)
        return receipt

    monkeypatch.setattr(disclosure_observations, "import_baseline_identity", capture_identity)
    calls = []
    both, failures = asyncio.Event(), []
    if case == "two_unknown":
        from nexus.paper import manual_request

        admit = manual_request.admit_manual_request

        async def two_flight(session, **kwargs):
            return await admit(session, **{**kwargs, "two_flight": True})

        monkeypatch.setattr(manual_request, "admit_manual_request", two_flight)

    def sdk(config):
        client = create(config)
        if (
            case not in {"mixed_completed", "mixed_plan", "mixed_increment_unknown"}
            or len(calls) >= 3
        ):
            client.thread_start.return_value.run.side_effect = RuntimeError("controlled failure")
        if case == "two_unknown":

            async def fail_together(*_args, **_kwargs):
                failures.append(client)
                if len(failures) == 2:
                    both.set()
                await both.wait()
                raise RuntimeError("controlled two-flight failure")

            client.thread_start.return_value.run.side_effect = fail_together
        calls.append(client)
        return client

    monkeypatch.setattr(codex_sdk, "AsyncCodex", sdk)
    monkeypatch.setattr(manual_paper, "_require_maintenance_code", lambda *_: None)
    monkeypatch.setattr(manual_paper, "preparation_service_control_lock", lambda *_: nullcontext())
    monkeypatch.setattr(
        manual_preparation_close, "require_preparation_producers_stopped", lambda *_: None
    )

    class Observed(BaseException):
        pass

    async def facts(factory):
        async with factory() as session:
            return {
                table: tuple(
                    await session.scalars(
                        text(f"SELECT to_jsonb(t)::text FROM {table} t ORDER BY to_jsonb(t)::text")
                    )
                )
                for table in (
                    "manual_paper_requests",
                    "manual_paper_request_events",
                    "agent_research_runs",
                    "agent_research_outputs",
                    "agent_daily_budget_reservations",
                    "jobs",
                    "job_attempts",
                    "job_leases",
                    "research_stage_authorization",
                    "research_stage_cleanup",
                    "research_stage_terminal",
                    "outbox_events",
                )
            }

    async def checked(settings, **kwargs):
        factory = captured["factory"]
        if case == "two_unknown":
            from nexus.ops.manual_paper_host import _dispatch_two_flight_members
            from nexus.paper.manual_request import read_manual_request

            async with factory() as session:
                root = (await session.scalars(select(ManualPaperRequest))).one()
                active = await read_manual_request(session, root)
            async with asyncio.timeout(90):
                await _dispatch_two_flight_members(settings, view=active)
        else:
            if case == "mixed_increment_unknown":
                from nexus.persistence.models.agents import AgentResearchRunRecord
                from nexus.persistence.models.market import InstrumentAlias
                from nexus.persistence.models.runtime import Job

                async with factory() as session:
                    first_job = await session.scalar(
                        select(Job.id)
                        .join(
                            AgentResearchRunRecord,
                            AgentResearchRunRecord.id
                            == Job.payload["run_id"].astext.cast(UUIDType),
                        )
                        .join(
                            InstrumentAlias,
                            InstrumentAlias.instrument_id == AgentResearchRunRecord.instrument_id,
                        )
                        .where(InstrumentAlias.symbol == "AAPL")
                    )
                kwargs = {**kwargs, "job_ids": (first_job,)}
            assert await run(settings, **kwargs)
        if case in {"mixed_completed", "mixed_plan", "mixed_increment_unknown"}:
            from nexus.persistence.models.runtime import Job

            async with factory() as session:
                next_job = await session.scalar(
                    select(Job.id)
                    .where(Job.status == "queued", Job.job_type == "agent.paper.research")
                    .order_by(Job.id)
                )
            assert next_job is not None
            assert await run(settings, **{**kwargs, "job_ids": (next_job,)})
        async with factory.begin() as session:
            row = (await session.scalars(select(ManualPaperRequest))).one()
            try:
                result = await finish_research_request(session, row=row, cancel_unused=True)
            except ValueError as error:
                # Synthetic-only failure facts keep a failing native proof diagnosable.
                import traceback

                if error.__context__ is not None:
                    error.add_note(repr(traceback.extract_tb(error.__context__.__traceback__)))
                diagnostics = (
                    (
                        await session.execute(
                            text("""
                    SELECT clock_timestamp() AS database_time,
                      t.closed_at, t.known, t.evidence,
                      j.status, j.started_at, j.finished_at, j.result_code,
                      l.acquired_at, l.heartbeat_at, l.expires_at,
                      a.started_at AS attempt_start, a.finished_at AS attempt_end,
                      a.outcome, m.work_deadline
                    FROM research_stage_member m
                    LEFT JOIN research_stage_terminal t USING(run_id)
                    JOIN jobs j ON j.id=m.job_id
                    JOIN job_leases l ON l.job_id=j.id
                    JOIN job_attempts a ON a.job_id=j.id
                """)
                        )
                    )
                    .mappings()
                    .all()
                )
                error.add_note(repr(diagnostics))
                error.add_note("fixture_clock=" + captured["clock"][0].isoformat())
                raise
            assert row.contract_version == 3 and result.status == "unknown"
            assert sum(not m.usage_known for m in result.research.members) == (
                2 if case == "two_unknown" else 1
            )
            if case in {"mixed_completed", "mixed_plan", "mixed_increment_unknown"}:
                assert sorted(m.status for m in result.research.members) == [
                    "abstained",
                    "completed",
                    "failed",
                ]
        if case == "stale_execution_read":
            async with factory.begin() as session:
                await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
                with pytest.raises(ValueError, match="INDEPENDENT_UNKNOWN_ACCEPTANCE_BLOCKED"):
                    await require_independent_execution_open(session, request_id=row.id)
            raise Observed
        before = await facts(factory)

        async def late_observation():
            async with factory.begin() as session:
                await session.execute(
                    text("""INSERT INTO research_stage_observation
                    (run_id,stage,response_hash,input_tokens,output_tokens,valid,observed_at,eligible)
                    SELECT a.run_id,a.stage,repeat('a',64),NULL,NULL,false,t.closed_at,false
                    FROM research_stage_authorization a
                    JOIN research_stage_terminal t ON t.run_id=a.run_id""")
                )

        args = SimpleNamespace(
            action="check-independent-acceptance",
            request_id=row.id,
            repo_root=tmp_path,
            env_file=tmp_path / "infra/.env.local",
            accept_unknown_effects=False,
            expected_proof_hash=None,
        )
        async with factory() as session:
            with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                await require_manual_account_available(
                    session, account_id=row.account_id, now=captured["clock"][0]
                )
        check = await manual_paper.run(settings, args)
        assert check.status == "eligible_at_check" and len(check.proof_hash) == 64
        assert await facts(factory) == before
        if case == "unconsumed_lease":
            from nexus.jobs.models import JobKeyV1, JobType, QueueName, RetryPolicy
            from nexus.jobs.repository import claim_next_job, dead_letter_job, enqueue_job

            async with factory.begin() as session:
                at = await session.scalar(text("SELECT clock_timestamp()"))
                job = await enqueue_job(
                    session,
                    key=JobKeyV1(
                        job_type=JobType.AGENT_RESEARCH,
                        scope_kind="portfolio",
                        scope_id=str(row.account_id),
                        effective_at=at,
                        input_hash="c" * 64,
                    ),
                    schema_version="isolated_v1",
                    queue=QueueName.GENERAL,
                    payload={},
                    retry_policy=RetryPolicy.NEVER,
                    max_attempts=1,
                )
                # A native dead-letter receipt is terminal, but it does not prove lease consumption.
                claim = await claim_next_job(
                    session,
                    queue=QueueName.GENERAL,
                    worker_id="isolated-owner",
                    now=at,
                    lease_seconds=60,
                    job_ids=(job.id,),
                )
                assert claim is not None
                await dead_letter_job(session, claim=claim, now=at, error_code="ISOLATED_UNSAFE")
        args.action = "accept-independent-unknown"
        args.expected_proof_hash = check.proof_hash
        with pytest.raises(ValueError):
            await manual_paper.run(settings, args)
        args.accept_unknown_effects = True
        if case == "live":

            def live(*_):
                raise ValueError("INDEPENDENT_UNKNOWN_ACCEPTANCE_BLOCKED")

            monkeypatch.setattr(
                manual_preparation_close, "require_preparation_producers_stopped", live
            )
        if case in {
            "late_before",
            "wrong_hash",
            "wrong_scope",
            "wrong_account",
            "unconsumed_lease",
            "live",
        }:
            if case == "late_before":
                await late_observation()
            elif case == "wrong_hash":
                args.expected_proof_hash = "0" * 64
            elif case == "wrong_scope":
                from uuid import uuid4

                from nexus.paper import independent_unknown_proof

                async def other_route(*_args):
                    return {"kind": "independent", "scope_id": uuid4()}

                monkeypatch.setattr(independent_unknown_proof, "selected_route", other_route)
            elif case == "wrong_account":
                from uuid import uuid4

                settings = settings.model_copy(update={"agent_paper_account_id": uuid4()})
            with pytest.raises(ValueError):
                await manual_paper.run(settings, args)
            async with factory() as session:
                assert (
                    await session.scalar(
                        text(
                            "SELECT count(*) FROM audit_events "
                            "WHERE action='paper.independent_unknown.accepted'"
                        )
                    )
                    == 0
                )
            raise Observed
        if case in {"concurrent", "rollback"}:
            from nexus.paper.independent_unknown_acceptance import (
                inspect_or_accept_independent_unknown,
            )

            async with factory() as owner:
                await owner.begin()
                first = await inspect_or_accept_independent_unknown(
                    owner,
                    settings=settings,
                    request_id=row.id,
                    repo_root=tmp_path,
                    env_file=args.env_file,
                    accept=True,
                    accept_unknown_effects=True,
                    expected_proof_hash=check.proof_hash,
                )
                assert not first.replayed
                with pytest.raises(ValueError, match="INDEPENDENT_UNKNOWN_ACCEPTANCE_BLOCKED"):
                    await manual_paper.run(settings, args)
                if case == "concurrent":
                    await owner.commit()
                else:
                    await owner.rollback()
            if case == "rollback":
                async with factory() as session:
                    assert (
                        await session.scalar(
                            text(
                                "SELECT count(*) FROM audit_events "
                                "WHERE action='paper.independent_unknown.accepted'"
                            )
                        )
                        == 0
                    )
                assert await facts(factory) == before
                raise Observed
        if case in {"commit_ack_lost", "rollback_ack_lost"}:
            from sqlalchemy.ext.asyncio import AsyncSession

            commit, rollback = AsyncSession.commit, AsyncSession.rollback

            async def lost_ack(session):
                if case == "commit_ack_lost":
                    await commit(session)
                else:
                    await rollback(session)
                raise ConnectionError("controlled acceptance acknowledgement loss")

            with monkeypatch.context() as boundary:
                boundary.setattr(AsyncSession, "commit", lost_ack)
                with pytest.raises(ConnectionError):
                    await manual_paper.run(settings, args)
            # Inspect only; never treat an absent or lost receipt as permission to resubmit.
            args.action = "check-independent-acceptance"
            inspected = await manual_paper.run(settings, args)
            assert inspected.status == (
                "already_acknowledged" if case == "commit_ack_lost" else "eligible_at_check"
            )
            async with factory() as session:
                assert await session.scalar(
                    text(
                        "SELECT count(*) FROM audit_events "
                        "WHERE action='paper.independent_unknown.accepted'"
                    )
                ) == int(case == "commit_ack_lost")
            assert await facts(factory) == before
            raise Observed
        receipt = await manual_paper.run(settings, args)
        assert receipt.status == "independent_unknown_acknowledged" and receipt.replayed == (
            case == "concurrent"
        )
        assert receipt.budget_preserved and receipt.execution_locked
        if case == "normal":
            from nexus.paper.manual_entry import read_configured_manual_request

            async with factory.begin() as session:
                await session.execute(
                    text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                )
                presented = await read_configured_manual_request(
                    session, settings, request_id=row.id
                )
                assert presented.independent_disposition == "unknown_acknowledged"
                assert presented.status == result.status == "unknown"
                assert presented.research == result.research
            assert await facts(factory) == before
            from nexus.paper.research_request import prepare_research_request

            async with factory.begin() as session:
                current = await session.get(ManualPaperRequest, row.id)
                assert (
                    await finish_research_request(session, row=current, cancel_unused=True)
                    == result
                )
                with pytest.raises(ValueError, match="INDEPENDENT_UNKNOWN_ACCEPTANCE_BLOCKED"):
                    await prepare_research_request(session, settings, request_id=row.id)
        if case == "mixed_increment_unknown":
            from integration import test_monitor_batches_pg
            from integration.test_monitor_mixed_increment_pg import (
                test_sealed_price_and_two_filings_admit_one_single_stock_job_and_three_calls,
            )
            from nexus.monitoring.disclosure_context import configured_exhibit_store
            from nexus.paper.independent_unknown_acceptance import independent_unknown_accepted
            from nexus.persistence.models.agents import (
                AgentResearchOutputRecord,
                AgentResearchRunRecord,
            )

            completed = next(m for m in result.research.members if m.status == "completed")
            async with factory() as session:
                completed_run = await session.get(AgentResearchRunRecord, completed.run_id)
                output = await session.scalar(
                    select(AgentResearchOutputRecord).where(
                        AgentResearchOutputRecord.run_id == completed.run_id,
                        AgentResearchOutputRecord.role.is_(None),
                        AgentResearchOutputRecord.status == "completed",
                    )
                )
            pointer = SimpleNamespace(
                account_id=row.account_id,
                instrument_id=completed.instrument_id,
                source_run_id=completed.run_id,
                source_output_id=output.id,
                data_snapshot_id=completed_run.data_snapshot_id,
            )
            captured["account_id"] = row.account_id

            async def existing_source(*_args, **_kwargs):
                return factory, settings, configured_exhibit_store(settings), captured, pointer

            monkeypatch.setattr(test_monitor_batches_pg, "reader_source", existing_source)
            monkeypatch.setattr(codex_sdk, "AsyncCodex", create)
            await test_sealed_price_and_two_filings_admit_one_single_stock_job_and_three_calls(
                migrated_database_url,
                monkeypatch,
                tmp_path,
                True,
                independent=True,
                accepted_unknown_root=next(
                    m.run_id for m in result.research.members if not m.usage_known
                ),
            )
            async with factory.begin() as session:
                assert await independent_unknown_accepted(
                    session, row=row, now=captured["clock"][0]
                )
                with pytest.raises(ValueError, match="MONITOR_ACCOUNT_BUSY_OR_UNKNOWN"):
                    await require_manual_account_available(
                        session,
                        account_id=row.account_id,
                        now=captured["clock"][0],
                        allow_scheduled_completion=True,
                    )
            raise Observed
        if case == "mixed_plan":
            from nexus.monitoring.reconciliation import reconcile_account_results
            from nexus.persistence.models.monitoring import MonitorPlan

            completed = next(m for m in result.research.members if m.status == "completed")
            async with factory.begin() as session:
                reconciled = await reconcile_account_results(
                    session,
                    account_id=row.account_id,
                    instrument_ids=(completed.instrument_id,),
                    now=captured["clock"][0],
                )
                plans = tuple(
                    await session.scalars(
                        select(MonitorPlan).where(MonitorPlan.source_run_id == completed.run_id)
                    )
                )
                assert plans, reconciled
            # New qualified plans do not rewrite or invalidate accepted root facts.
            repeated = await manual_paper.run(settings, args)
            assert repeated.replayed
        if case == "readonly_recovery":
            async with factory.begin() as session:
                await session.execute(text("SET TRANSACTION READ ONLY"))
                await require_manual_account_available(
                    session, account_id=row.account_id, now=captured["clock"][0], read_only=True
                )
            from nexus.ops import agent_paper_recovery
            from nexus.settings import AgentRuntimeMode

            ready = await agent_paper_recovery.inspect_recovery_readiness(
                settings.model_copy(
                    update={
                        "agent_paper_cycle_enabled": True,
                        "agent_paper_account_id": row.account_id,
                        "agent_paper_candidate_batch_id": row.candidate_batch_id,
                        "agent_paper_formal_model_config_id": row.model_config_id,
                        "agent_runtime_mode": AgentRuntimeMode.CODEX_SDK,
                        "release_revision": "a" * 40,
                    }
                )
            )
            assert ready["status"] == "ready", ready
            assert ready["external_requests"] == 0
            assert await facts(factory) == before
            await late_observation()
            # Read-only readiness is not a cached permit for later writable admission.
            async with factory.begin() as session:
                with pytest.raises(ValueError, match="INDEPENDENT_UNKNOWN_ACCEPTANCE_BLOCKED"):
                    await require_manual_account_available(
                        session, account_id=row.account_id, now=captured["clock"][0]
                    )
            raise Observed
        if case in {"new_request", "legacy_request", "budget_preserved"}:
            from decimal import Decimal

            from nexus.paper.manual_request import admit_manual_request

            async with factory() as session:
                if case == "budget_preserved":
                    from nexus.agents.budget import BudgetExhausted
                    from nexus.agents.persistence import (
                        _shared_daily_budget_used,
                        reserve_daily_budget,
                    )
                    from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord

                    unknown = next(m for m in result.research.members if not m.usage_known)
                    reservation = await session.scalar(
                        select(AgentDailyBudgetReservationRecord).where(
                            AgentDailyBudgetReservationRecord.run_id == unknown.run_id
                        )
                    )
                    at = await session.scalar(text("SELECT clock_timestamp()"))
                    used = await _shared_daily_budget_used(session, business_date=at.date())
                    assert reservation.state == "reserved" and reservation.settled_usd is None
                    # The approved subscription profile has exact zero dollar pricing.
                    # Unknown remains reserved even at zero; acceptance cannot
                    # bypass the shared money gate for a new positive quote.
                    assert reservation.reserved_usd == 0
                    cap = used + Decimal("0.005")
                    with pytest.raises(BudgetExhausted, match="daily research budget exhausted"):
                        await reserve_daily_budget(
                            session,
                            run_id=unknown.run_id,
                            now=at,
                            daily_limit_usd=cap,
                            worst_case_usd=Decimal("0.01"),
                            pricing_version=reservation.pricing_version,
                            pricing_fresh=True,
                        )
                    await session.rollback()
                    assert await facts(factory) == before
                else:
                    fresh = await admit_manual_request(
                        session,
                        account_id=row.account_id,
                        batch_id=row.candidate_batch_id,
                        model_config_id=settings.agent_paper_formal_model_config_id,
                        mode="research_only"
                        if case == "legacy_request"
                        else "research_and_monitor",
                        idempotency_key="new-after-acceptance",
                        contract_version=2 if case == "legacy_request" else 3,
                    )
                    assert fresh.request_id != row.id and fresh.status == "queued"
                    await session.commit()
            if case == "legacy_request":
                legacy_args = SimpleNamespace(**vars(args))
                legacy_args.action = "check-independent-acceptance"
                legacy_args.request_id = fresh.request_id
                with pytest.raises(ValueError, match="INDEPENDENT_UNKNOWN_ACCEPTANCE_BLOCKED"):
                    await manual_paper.run(settings, legacy_args)
            if case in {"new_request", "legacy_request"}:
                repeated = await manual_paper.run(settings, args)
                assert repeated.replayed and repeated.accepted_at == receipt.accepted_at
            raise Observed
        if case == "settled_consumer":
            from nexus.paper.research_request import require_research_jobs_settled

            async with factory() as session:
                current = await session.get(ManualPaperRequest, row.id)
                await require_research_jobs_settled(
                    session, row=current, view=result.research, now=captured["clock"][0]
                )
                subset = result.research.model_copy(
                    update={
                        "members": [
                            next(
                                member
                                for member in result.research.members
                                if not member.usage_known
                            )
                        ]
                    }
                )
                with pytest.raises(ValueError, match="INDEPENDENT_UNKNOWN_ACCEPTANCE_BLOCKED"):
                    await require_research_jobs_settled(
                        session, row=current, view=subset, now=captured["clock"][0]
                    )
            assert await facts(factory) == before
            raise Observed
        if case == "stale_history_read":
            async with factory.begin() as session:
                await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
                with pytest.raises(ValueError, match="INDEPENDENT_UNKNOWN_ACCEPTANCE_BLOCKED"):
                    await require_manual_account_available(
                        session, account_id=row.account_id, now=captured["clock"][0], read_only=True
                    )
            raise Observed
        if case == "concurrent_history_writer":
            async with factory() as owner:
                await owner.execute(
                    text("SELECT id FROM research_stage_scope WHERE id=:id FOR UPDATE"),
                    {"id": row.id},
                )
                async with factory() as session:
                    with pytest.raises(ValueError, match="INDEPENDENT_UNKNOWN_ACCEPTANCE_BLOCKED"):
                        await require_manual_account_available(
                            session,
                            account_id=row.account_id,
                            now=captured["clock"][0],
                            read_only=True,
                        )
                await owner.rollback()
            raise Observed
        if case == "late_after":
            await late_observation()
            from nexus.paper.manual_entry import read_configured_manual_request

            async with factory.begin() as session:
                await session.execute(
                    text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                )
                presented = await read_configured_manual_request(
                    session, settings, request_id=row.id
                )
                assert presented.independent_disposition == "recheck_required"
                assert presented.status == "unknown"
            async with factory() as session:
                with pytest.raises(ValueError, match="INDEPENDENT_UNKNOWN_ACCEPTANCE_BLOCKED"):
                    await require_manual_account_available(
                        session, account_id=row.account_id, now=captured["clock"][0], read_only=True
                    )
                with pytest.raises(ValueError, match="INDEPENDENT_UNKNOWN_ACCEPTANCE_BLOCKED"):
                    await require_independent_execution_open(session, request_id=row.id)
            with pytest.raises(ValueError):
                await manual_paper.run(settings, args)
            async with factory() as session:
                assert (
                    await session.scalar(
                        text(
                            "SELECT count(*) FROM audit_events "
                            "WHERE action='paper.independent_unknown.accepted'"
                        )
                    )
                    == 1
                )
            raise Observed
        if case == "unrelated_audit":
            from nexus.audit.service import append_audit_event

            async with factory.begin() as session:
                await append_audit_event(
                    session,
                    actor_id="isolated-user",
                    action="isolated.later",
                    schema_version="isolated_v1",
                    payload={"unrelated": True},
                    occurred_at=await session.scalar(text("SELECT clock_timestamp()")),
                )
        if case == "next_day":
            captured["clock"][0] += timedelta(days=1)
        repeated = await manual_paper.run(settings, args)
        assert repeated.replayed and repeated.accepted_at == receipt.accepted_at
        assert await facts(factory) == before
        async with factory() as session:
            await require_manual_account_available(
                session, account_id=row.account_id, now=captured["clock"][0], read_only=True
            )
            await require_no_running_manual_request(session, account_id=row.account_id)
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM audit_events "
                        "WHERE action='paper.independent_unknown.accepted'"
                    )
                )
                == 1
            )
        assert len(calls) == (
            4
            if case in {"mixed_completed", "mixed_plan", "mixed_increment_unknown"}
            else 2
            if case == "two_unknown"
            else 1
        )
        for client in calls:
            client.thread_start.return_value.run.assert_awaited_once()
        raise Observed

    monkeypatch.setattr(candidates, "run_agent_worker_once", checked)
    with pytest.raises(Observed):
        await disclosure_observations.reader_source(
            migrated_database_url, monkeypatch, tmp_path, independent=True
        )
