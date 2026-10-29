"""Offline design experiments, not a staged executor or a production enablement.

These characterize why moving begin() is not an equivalent research protocol.
All claims and constraints are native; the existing fixture replaces external I/O.
"""

from datetime import timedelta
from uuid import UUID

import pytest
from integration.test_candidate_multistock_path import (
    test_fresh_multistock_candidate_cycle_freezes_native_jobs as seed_cycle,
)
from integration.test_job_checkpoint import NOW, _claim
from nexus.agents.execution import _advance, _append_event
from nexus.jobs import worker
from nexus.jobs.models import QueueName
from nexus.jobs.registry import JobHandlerContext
from nexus.jobs.repository import (
    LeaseLostError,
    checkpoint_job,
    fail_job,
    reclaim_expired_jobs,
)
from nexus.paper.agent_cycle import resume_schwab_agent_paper_cycle
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchEventRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import Job, JobAttempt, JobLease
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from pydantic import BaseModel
from sqlalchemy import func, select


class EmptyPayload(BaseModel):
    pass


class LostOwner(BaseException):
    """Stop this isolated fixture after the native claim has committed."""


@pytest.mark.asyncio
async def test_committed_price_freeze_survives_without_continuous_reader_lock(
    migrated_database_url,
):
    from integration.test_fosun_market_authority_migration import (
        _append_fosun_calendar_session,
        _insert_agent_price_binding,
        _insert_fosun_bar_authority,
        _mutate_bound_manifest,
        _mutate_capability,
    )
    from nexus.integrations.service import IntegrationStatusService
    from nexus.market.price_bar_authority import prepare_price_bar_set
    from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord
    from nexus.persistence.models.data import PriceSeriesManifest
    from nexus.quality.source_policy import load_fosun_market_admission
    from sqlalchemy.exc import DBAPIError

    authority = await _insert_fosun_bar_authority(
        migrated_database_url, interval="1Day", delivery_mode="real_time"
    )
    snapshot_id = await _insert_agent_price_binding(migrated_database_url, authority)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async with factory.begin() as session:
            link = await session.scalar(
                select(AgentDataSnapshotEvidenceRecord).where(
                    AgentDataSnapshotEvidenceRecord.data_snapshot_id == snapshot_id
                )
            )
            manifest = await session.get(PriceSeriesManifest, authority["manifest_id"])
            original = await prepare_price_bar_set(session, manifest.id, as_of=link.selected_at)
            assert original is not None and original.content_hash == link.content_hash
            selected_at, capability_id = link.selected_at, manifest.provider_capability_id
            assert (
                await load_fosun_market_admission(
                    session,
                    decision_time=selected_at,
                    operation="fosun_get_bars",
                    market_scope="US",
                )
                is not None
            )
        # Every attempted writer uses its own connection after the reader has
        # committed. 23514 is a persistent guard, not a hidden lock timeout.
        for change, identity in (
            (_append_fosun_calendar_session, authority),
            (_mutate_bound_manifest, manifest.id),
            (_mutate_capability, capability_id),
        ):
            with pytest.raises(DBAPIError) as rejected:
                await change(migrated_database_url, identity)
            assert rejected.value.orig.sqlstate == "23514"
        async with factory.begin() as session:
            service = IntegrationStatusService(session, selected_at + timedelta(seconds=1))
            status = await service.find_status("fosun")
            await service.write_status(
                "fosun",
                mode="disabled",
                expected_version=status.version,
                last_success_at=status.last_success_at,
                admitted_contract_hash=None,
            )
        async with factory.begin() as session:
            assert (
                await load_fosun_market_admission(
                    session,
                    decision_time=selected_at + timedelta(seconds=2),
                    operation="fosun_get_bars",
                    market_scope="US",
                )
                is None
            )
            # Current collection permission is not historical price validity.
            assert await prepare_price_bar_set(session, manifest.id, as_of=selected_at) == original
            unchanged = await session.scalar(
                select(AgentDataSnapshotEvidenceRecord).where(
                    AgentDataSnapshotEvidenceRecord.data_snapshot_id == snapshot_id
                )
            )
            assert unchanged.content_hash == original.content_hash
            assert unchanged.selected_at == selected_at
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_settling_partial_usage_cannot_be_a_reusable_stage_checkpoint(migrated_database_url):
    from decimal import Decimal

    from integration.test_portfolio_synthesis_persistence_pg import seed_synthesis
    from nexus.agents.budget import BudgetExhausted
    from nexus.agents.persistence import require_reserved_daily_budget, settle_daily_budget
    from sqlalchemy.exc import DBAPIError

    engine, factory, _ = await seed_synthesis(migrated_database_url, complete_research=False)
    try:
        async with factory.begin() as session:
            hold = await session.scalar(
                select(AgentDailyBudgetReservationRecord).order_by(
                    AgentDailyBudgetReservationRecord.id
                )
            )
            run_id, hold_id = hold.run_id, hold.id
            assert hold.state == "reserved" and hold.reserved_usd == Decimal("0.0002")
            await settle_daily_budget(
                session,
                run_id=run_id,
                actual_usd=Decimal("0.0001"),
                input_tokens=1,
                output_tokens=1,
            )
        async with factory.begin() as session:
            with pytest.raises(BudgetExhausted, match="no open budget reservation"):
                await require_reserved_daily_budget(session, run_id=run_id)
        with pytest.raises(DBAPIError) as rejected:
            async with factory.begin() as session:
                hold = await session.get(AgentDailyBudgetReservationRecord, hold_id)
                hold.state = "reserved"
                hold.settled_usd = None
                await session.flush()
        assert rejected.value.orig.sqlstate == "23514"
        async with factory() as session:
            hold = await session.get(AgentDailyBudgetReservationRecord, hold_id)
            assert hold.state == "settled" and hold.settled_usd == Decimal("0.0001")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("commit_case", ["rollback", "confirmed", "disconnect_after_commit"])
async def test_committed_research_stage_facts_need_a_distinct_orphan_contract(
    migrated_database_url, monkeypatch, tmp_path, commit_case
):
    claims = []

    async def stop_before_handler(session, **kwargs):
        claims.append(kwargs["claim"])
        raise LostOwner

    monkeypatch.setattr(worker, "execute_and_finalize_job", stop_before_handler)
    with pytest.raises(LostOwner):
        await seed_cycle(
            migrated_database_url,
            monkeypatch,
            count=3,
            trade=False,
            profile="formal_v11",
            tmp_path=tmp_path,
        )
    assert len(claims) == 1
    claim = claims[0]
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async with factory() as session:
            cycle = await session.scalar(select(AgentPaperCycleRecord))
            job = await session.get(Job, claim.job_id)
            run_id = UUID(job.payload["run_id"])
            lease = await session.scalar(select(JobLease).where(JobLease.job_id == job.id))
            phase_at = lease.acquired_at + timedelta(seconds=1)
            expired_at = lease.expires_at + timedelta(seconds=1)
            original_budget = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == run_id
                )
            )
            budget_before = tuple(
                getattr(original_budget, c.name) for c in original_budget.__table__.columns
            )

        async def publish_intermediate(session):
            run = await session.get(AgentResearchRunRecord, run_id, with_for_update=True)
            await _append_event(session, run, 1, "prepared", {})
            await _advance(session, run, "running")
            await _append_event(session, run, 2, "running", {})
            if commit_case == "rollback":
                raise LostOwner

        try:
            async with factory.begin() as session:
                deadline = await checkpoint_job(
                    session,
                    claim=claim,
                    now=phase_at,
                    apply=publish_intermediate,
                    extend_seconds=1,
                )
                assert deadline == claim.expires_at
            if commit_case == "disconnect_after_commit":
                # Deliberately after commit has returned. This proves durable
                # readback after process loss, NOT a lost COMMIT acknowledgement.
                raise LostOwner
        except LostOwner:
            assert commit_case in {"rollback", "disconnect_after_commit"}

        async with factory() as observer:
            assert await observer.scalar(
                select(func.count())
                .select_from(AgentResearchEventRecord)
                .where(AgentResearchEventRecord.run_id == run_id)
            ) == (0 if commit_case == "rollback" else 2)
            assert (
                await observer.scalar(
                    select(func.count())
                    .select_from(AgentResearchOutputRecord)
                    .where(AgentResearchOutputRecord.run_id == run_id)
                )
                == 0
            )
            assert (await observer.get(AgentResearchRunRecord, run_id)).status == (
                "prepared" if commit_case == "rollback" else "running"
            )

        async with factory.begin() as session:
            # Formal research is excluded from generic reclaim, even now that
            # no long transaction holds its Job lock.
            assert (
                await reclaim_expired_jobs(
                    session, queue=QueueName.GENERAL, now=expired_at, job_ids=(claim.job_id,)
                )
                == 0
            )

        async def resume():
            async with factory.begin() as session:
                return await resume_schwab_agent_paper_cycle(
                    session,
                    cycle_id=cycle.id,
                    idempotency_key="stage-experiment-same-owner",
                    now=expired_at,
                    formal_stage_capacity=True,
                )

        if commit_case == "rollback":
            assert (await resume()).status == "unavailable"
        else:
            # This is a design counterexample, not desired T behavior. Removing
            # this guard alone would destroy the existing unknown-only proof.
            with pytest.raises(ValueError, match="AGENT_PAPER_RESEARCH_ORPHAN_BINDING_INVALID"):
                await resume()

        async with factory() as observer:
            budget = await observer.get(AgentDailyBudgetReservationRecord, original_budget.id)
            assert tuple(getattr(budget, c.name) for c in budget.__table__.columns) == budget_before
            assert budget.state == "reserved" and budget.settled_usd is None
            job = await observer.get(Job, claim.job_id)
            attempt = await observer.scalar(select(JobAttempt).where(JobAttempt.job_id == job.id))
            assert job.attempt_count == 1 and job.manual_retry_count == 0
            assert job.status == ("dead_letter" if commit_case == "rollback" else "running")
            assert attempt.outcome == ("lost" if commit_case == "rollback" else "running")
            run = await observer.get(AgentResearchRunRecord, run_id)
            assert run.input_tokens is run.output_tokens is run.settled_cost_usd is None
        assert len(claims) == 1
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_captured_lease_guard_cannot_replace_current_database_owner(migrated_database_url):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    claim = await _claim(factory, lease_seconds=30)
    try:
        async with factory() as session:
            context = JobHandlerContext(
                session=session,
                claim=claim,
                payload=EmptyPayload(),
                _clock=lambda: NOW + timedelta(seconds=2),
            )
            async with factory.begin() as controller:
                await fail_job(
                    controller,
                    claim=claim,
                    now=NOW + timedelta(seconds=1),
                    error_code="CONTROLLED_OWNER_CLOSED",
                )
            # The old synchronous guard still passes before its captured expiry.
            context.require_dispatch_lease()
        applied = False

        async def next_stage(session):
            nonlocal applied
            applied = True

        with pytest.raises(LeaseLostError):
            async with factory.begin() as session:
                await checkpoint_job(
                    session,
                    claim=claim,
                    now=NOW + timedelta(seconds=2),
                    apply=next_stage,
                    extend_seconds=1,
                )
        assert not applied
        async with factory() as session:
            assert (await session.get(Job, claim.job_id)).status == "dead_letter"
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(JobAttempt)
                    .where(JobAttempt.job_id == claim.job_id)
                )
                == 1
            )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "commit_stage", [False, True], ids=["response-rollback", "response-commit"]
)
async def test_native_stage_response_commit_is_not_yet_recoverable_protocol(
    migrated_database_url, monkeypatch, tmp_path, commit_stage
):
    from nexus.agents import codex_staged, execution

    analyze = codex_staged.StagedCodexCommittee.analyze
    append = execution._append_output
    observed = {}

    async def observe_usage(self, role, *args, **kwargs):
        result = await analyze(self, role, *args, **kwargs)
        if role == "bull":
            observed["usage"] = self.usage
        return result

    async def interrupt_after_stage(session, run, **kwargs):
        result = await append(session, run, **kwargs)
        if kwargs["role"] == "bull":
            observed["run_id"] = run.id
            if commit_stage:
                # A TEST-ONLY boundary experiment: retain the real Worker,
                # model-stage construction/validation and all SQL guards.
                # Do not install this commit into a production handler.
                await session.commit()
            raise LostOwner
        return result

    monkeypatch.setattr(codex_staged.StagedCodexCommittee, "analyze", observe_usage)
    monkeypatch.setattr(execution, "_append_output", interrupt_after_stage)
    with pytest.raises(LostOwner):
        await seed_cycle(
            migrated_database_url,
            monkeypatch,
            count=3,
            trade=False,
            profile="formal_v11",
            tmp_path=tmp_path,
        )
    assert (observed["usage"].input_tokens, observed["usage"].output_tokens) == (100, 100)
    assert observed["usage"].provider_calls == 1
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async with factory() as session:
            run = await session.get(AgentResearchRunRecord, observed["run_id"])
            cycle = await session.scalar(select(AgentPaperCycleRecord))
            job = await session.scalar(
                select(Job).where(Job.payload["run_id"].as_string() == str(run.id))
            )
            lease = await session.scalar(select(JobLease).where(JobLease.job_id == job.id))
            outputs = tuple(
                await session.scalars(
                    select(AgentResearchOutputRecord)
                    .where(AgentResearchOutputRecord.run_id == run.id)
                    .order_by(AgentResearchOutputRecord.sequence)
                )
            )
            assert [o.role for o in outputs] == (
                ["orchestrator", "fundamental", "market_state", "macro_sector", "bull"]
                if commit_stage
                else []
            )
            assert all(o.status == "running" and o.intent is None for o in outputs)
            old_output_hashes = {o.id: o.content_hash for o in outputs}
            # A real known stage response has no separately committed usage
            # record in the current protocol, even if its role rows commit.
            assert run.input_tokens is run.output_tokens is run.settled_cost_usd is None
            expired_at = lease.expires_at + timedelta(seconds=1)

        async def close_original():
            async with factory.begin() as session:
                return await resume_schwab_agent_paper_cycle(
                    session,
                    cycle_id=cycle.id,
                    idempotency_key="same-observed-stage",
                    now=expired_at,
                    formal_stage_capacity=True,
                )

        if commit_stage:
            with pytest.raises(ValueError, match="AGENT_PAPER_RESEARCH_ORPHAN_BINDING_INVALID"):
                await close_original()
        else:
            assert (await close_original()).status == "unavailable"
            from sqlalchemy.exc import DBAPIError

            # A late known observation cannot rewrite an already closed Run.
            # T needs a separate diagnostic observation identity, not this update.
            with pytest.raises(DBAPIError) as rejected:
                async with factory.begin() as session:
                    terminal = await session.get(AgentResearchRunRecord, run.id)
                    terminal.input_tokens = 100
                    terminal.output_tokens = 100
                    await session.flush()
            assert rejected.value.orig.sqlstate == "23514"
        async with factory() as session:
            hold = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == run.id
                )
            )
            assert hold.state == "reserved" and hold.settled_usd is None
            for output_id, digest in old_output_hashes.items():
                assert (
                    await session.get(AgentResearchOutputRecord, output_id)
                ).content_hash == digest
            assert (await session.get(Job, job.id)).attempt_count == 1
            terminal = await session.get(AgentResearchRunRecord, run.id)
            assert (
                terminal.input_tokens is terminal.output_tokens is terminal.settled_cost_usd is None
            )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_known_first_stage_validation_failure_settles_without_later_dispatch(
    migrated_database_url, monkeypatch, tmp_path
):
    from integration import test_candidate_multistock_path as fixture
    from nexus.agents import codex_staged

    native_worker = fixture.run_agent_worker_once
    observed = {}
    calls = []

    async def one_worker(settings, **kwargs):
        fixture_invoke = codex_staged.invoke_codex

        async def invalid_response(payload, **options):
            _, usage = await fixture_invoke(payload, **options)
            calls.append(payload["stage"])
            return '{"roles": []}', usage

        monkeypatch.setattr(codex_staged, "invoke_codex", invalid_response)
        assert await native_worker(settings, **kwargs)
        observed["job_id"] = kwargs["job_ids"][0]
        # Stop the outer scenario after the real Worker has committed the failure.
        raise LostOwner

    monkeypatch.setattr(fixture, "run_agent_worker_once", one_worker)
    with pytest.raises(LostOwner):
        await seed_cycle(
            migrated_database_url,
            monkeypatch,
            count=3,
            trade=False,
            profile="formal_v11",
            tmp_path=tmp_path,
        )
    assert calls == ["research"]
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async with factory() as session:
            job = await session.get(Job, observed["job_id"])
            run = await session.get(AgentResearchRunRecord, UUID(job.payload["run_id"]))
            hold = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == run.id
                )
            )
            output = await session.scalar(
                select(AgentResearchOutputRecord).where(AgentResearchOutputRecord.run_id == run.id)
            )
            assert job.status == run.status == "failed"
            assert job.attempt_count == 1 and run.retry_count == 0
            assert (run.input_tokens, run.output_tokens) == (100, 100)
            assert run.settled_cost_usd is not None
            assert hold.state == "settled" and hold.settled_usd == run.settled_cost_usd
            assert output.intent is None and output.status == "failed"
            assert output.payload["reason"] == "model_stage_roles_invalid"
    finally:
        await factory.kw["bind"].dispose()
