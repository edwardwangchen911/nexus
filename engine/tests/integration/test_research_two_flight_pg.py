"""Normal research entry must overlap bounded stocks without losing accounting."""

import asyncio
import json

import pytest
from integration.test_research_request_pg import prepare_scope
from nexus.agents.persistence import try_lock_budget_day
from nexus.agents.responses_budget import UsageSnapshot
from nexus.ops.manual_paper_host import run_pure_research_once
from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord
from sqlalchemy import select
from unit.agents.test_codex_sdk import _assessment_payload, _staged_batch


@pytest.mark.asyncio
async def test_two_flight_rejects_legacy_model_before_creating_request(
    migrated_database_url, tmp_path
):
    from nexus.paper.manual_request import admit_manual_request
    from nexus.persistence.models.manual_paper import ManualPaperRequest
    from sqlalchemy import func

    factory, settings, _, _ = await prepare_scope(migrated_database_url, tmp_path)
    try:
        async with factory.begin() as session:
            with pytest.raises(ValueError, match="^MANUAL_CONFIGURATION_UNAVAILABLE$"):
                await admit_manual_request(
                    session,
                    account_id=settings.agent_paper_account_id,
                    batch_id=settings.agent_paper_candidate_batch_id,
                    model_config_id=settings.agent_paper_formal_model_config_id,
                    mode="research_only",
                    idempotency_key="unsupported-two-flight",
                    contract_version=2,
                    two_flight=True,
                )
            assert await session.scalar(select(func.count()).select_from(ManualPaperRequest)) == 0
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_two_flight_direct_worker_without_host_controller_never_calls_sdk(
    migrated_database_url, tmp_path, monkeypatch
):
    from nexus.ops.agent_paper_once import run_cycle_job_once
    from nexus.paper.manual_request import admit_manual_request, claim_manual_request
    from nexus.paper.research_request import prepare_research_request
    from nexus.persistence.models.runtime import Job

    factory, settings, store, _ = await prepare_scope(migrated_database_url, tmp_path, bounded=True)
    calls = []

    async def forbidden(*_, **__):
        calls.append(True)
        raise AssertionError("direct worker bypassed the two-flight controller")

    monkeypatch.setattr("nexus.agents.codex_staged.invoke_codex", forbidden)
    monkeypatch.setattr(
        "nexus.research.sec_exhibit_storage.S3ProviderEvidenceStore", lambda *_: store
    )
    try:
        async with factory.begin() as session:
            request = await admit_manual_request(
                session,
                account_id=settings.agent_paper_account_id,
                batch_id=settings.agent_paper_candidate_batch_id,
                model_config_id=settings.agent_paper_formal_model_config_id,
                mode="research_only",
                idempotency_key="missing-controller",
                contract_version=2,
                two_flight=True,
            )
            await claim_manual_request(session, request_id=request.request_id)
        async with factory.begin() as session:
            session.info["sec_exhibit_store"] = store
            active = await prepare_research_request(
                session, settings, request_id=request.request_id
            )
        selected = active.research.members[0]
        with pytest.raises(ValueError, match="RESEARCH_ONLY_POOL_BINDING_INVALID"):
            await run_cycle_job_once(settings, job_id=selected.job_id, formal_stage_capacity=True)
        assert calls == []
        async with factory.begin() as session:
            job = await session.get(Job, selected.job_id)
            assert job.status == "queued" and job.attempt_count == 0
            reservations = list(await session.scalars(select(AgentDailyBudgetReservationRecord)))
            assert len(reservations) == 3 and all(row.state == "reserved" for row in reservations)
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_normal_entry_overlaps_two_stocks_and_settles_all_three(
    migrated_database_url, tmp_path, monkeypatch
):
    factory, settings, store, _ = await prepare_scope(migrated_database_url, tmp_path, bounded=True)
    settings = settings.model_copy(update={"release_revision": "a" * 40})
    first_started = asyncio.Event()
    second_started = asyncio.Event()
    release = asyncio.Event()
    in_flight = 0
    peak = 0
    stages = []
    day_unlocked = False

    async def invoke(payload, **options):
        nonlocal in_flight, peak
        options["dispatch_guard"]()
        in_flight += 1
        peak = max(peak, in_flight)
        stages.append(payload["stage"])
        try:
            if payload["stage"] == "research":
                if not first_started.is_set():
                    first_started.set()
                else:
                    second_started.set()
                await release.wait()
            body = (
                _assessment_payload(payload["previous_claims"])
                if payload["stage"] == "synthesis"
                else _staged_batch(payload["stage"], payload["requested_roles"])
            )
            return json.dumps(body), UsageSnapshot(
                input_tokens=300, output_tokens=80, provider_calls=1
            )
        finally:
            in_flight -= 1

    monkeypatch.setattr("nexus.agents.codex_staged.invoke_codex", invoke)
    monkeypatch.setattr(
        "nexus.research.sec_exhibit_storage.S3ProviderEvidenceStore", lambda *_: store
    )
    task = asyncio.create_task(run_pure_research_once(settings, idempotency_key="two-flight"))
    overlapped = False
    try:
        await asyncio.wait_for(first_started.wait(), 60)
        async with factory.begin() as session:
            reservations = list(await session.scalars(select(AgentDailyBudgetReservationRecord)))
            assert len(reservations) == 3
            assert all(row.state == "reserved" for row in reservations)
            assert len({row.business_date for row in reservations}) == 1
            day_unlocked = await try_lock_budget_day(
                session, reservations[0].business_date.isoformat()
            )
        # Watchdog only: the behavior is event overlap, never a speed threshold.
        try:
            await asyncio.wait_for(second_started.wait(), 30)
            overlapped = True
        except TimeoutError:
            pass
        finally:
            release.set()
        view = await asyncio.wait_for(task, 90)
        assert overlapped, "second stock never reached SDK while first was in flight"
        assert peak == 2
        assert day_unlocked, "read-only reservation check held the budget day during SDK work"
        assert view.status == "completed"
        assert len(view.research.members) == 3
        assert all(m.status == "completed" and m.usage_known for m in view.research.members)
        assert {stage: stages.count(stage) for stage in set(stages)} == {
            "research": 3,
            "critique": 3,
            "synthesis": 3,
        }
        async with factory.begin() as session:
            reservations = list(await session.scalars(select(AgentDailyBudgetReservationRecord)))
            assert len(reservations) == 3
            assert all(row.state == "settled" for row in reservations)
            assert all(row.settled_usd <= row.reserved_usd for row in reservations)

        async def stop_before_next_model(*_, **__):
            # Exercise the next normal admission and full preparation, then use
            # native unused-work closeout without consuming another model turn.
            return False

        monkeypatch.setattr("nexus.ops.agent_paper_once.run_cycle_job_once", stop_before_next_model)
        next_view = await run_pure_research_once(settings, idempotency_key="next-preparation-only")
        assert next_view.request_id != view.request_id
        assert next_view.status == "unavailable"
        assert len(next_view.research.members) == 3
        assert all(m.status == "abstained" and m.usage_known for m in next_view.research.members)
        assert not {m.run_id for m in view.research.members} & {
            m.run_id for m in next_view.research.members
        }
        assert len(stages) == 9
        async with factory.begin() as session:
            reservations = list(await session.scalars(select(AgentDailyBudgetReservationRecord)))
            assert len(reservations) == 6
            assert sorted(row.state for row in reservations) == ["released"] * 3 + ["settled"] * 3
    finally:
        release.set()
        if not task.done():
            await asyncio.wait_for(task, 90)
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("unknown", [False, True])
async def test_first_failure_drains_peer_stage_and_never_starts_third(
    migrated_database_url, tmp_path, monkeypatch, unknown
):
    from uuid import UUID

    from nexus.agents.codex_sdk import CodexInvocationError
    from nexus.paper.manual_request import read_manual_request, require_manual_account_available
    from nexus.paper.research_request import finish_research_request
    from nexus.persistence.models.agents import AgentResearchOutputRecord, AgentResearchRunRecord
    from nexus.persistence.models.manual_paper import ManualPaperRequest
    from nexus.persistence.models.runtime import Job, JobLease
    from sqlalchemy import func

    factory, settings, store, _ = await prepare_scope(migrated_database_url, tmp_path, bounded=True)
    settings = settings.model_copy(update={"release_revision": "a" * 40})
    second_started = asyncio.Event()
    failure_seen = asyncio.Event()
    calls = []
    instruments = []

    async def invoke(payload, **options):
        options["dispatch_guard"]()
        calls.append(payload["stage"])
        instruments.append(UUID(payload["request"]["instrument_id"]))
        assert payload["stage"] == "research", "peer started another stage after first failure"
        ordinal = len(calls)
        assert ordinal <= 2, "third stock was dispatched after first failure"
        if ordinal == 1:
            await asyncio.wait_for(second_started.wait(), 60)
            failure_seen.set()
            raise CodexInvocationError(
                "CODEX_TIMEOUT",
                provider_call_started=True,
                usage=None if unknown else UsageSnapshot(input_tokens=300, output_tokens=80),
            )
        second_started.set()
        await failure_seen.wait()
        return (
            json.dumps(_staged_batch("research", payload["requested_roles"])),
            UsageSnapshot(input_tokens=300, output_tokens=80, provider_calls=1),
        )

    monkeypatch.setattr("nexus.agents.codex_staged.invoke_codex", invoke)
    monkeypatch.setattr(
        "nexus.research.sec_exhibit_storage.S3ProviderEvidenceStore", lambda *_: store
    )
    try:
        if unknown:
            # Existing unknown-usage protection requires lease expiry before closeout.
            # Retain that refusal, then test the same closeout at its legal clock.
            with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                await run_pure_research_once(settings, idempotency_key="two-flight-failure")
            async with factory.begin() as session:
                row = await session.scalar(select(ManualPaperRequest))
                assert (await read_manual_request(session, row)).status == "active"
                expiry = await session.scalar(select(func.max(JobLease.expires_at)))

            async def after_expiry(session):
                from datetime import timedelta

                return expiry + timedelta(seconds=1)

            monkeypatch.setattr("nexus.paper.research_request.database_now", after_expiry)
            async with factory.begin() as session:
                row = await session.scalar(select(ManualPaperRequest))
                view = await finish_research_request(session, row=row, cancel_unused=True)
        else:
            view = await run_pure_research_once(settings, idempotency_key="two-flight-failure")
        assert calls == ["research", "research"]
        assert view.status == ("unknown" if unknown else "unavailable")
        async with factory.begin() as session:
            runs = [
                await session.get(AgentResearchRunRecord, m.run_id) for m in view.research.members
            ]
            jobs = [await session.get(Job, m.job_id) for m in view.research.members]
            by_instrument = {run.instrument_id: run for run in runs}
            failed, peer = (by_instrument[key] for key in instruments)
            unused = next(run for run in runs if run.instrument_id not in instruments)
            assert [job.attempt_count for job in jobs] == [1, 1, 1]
            assert [job.status for job in jobs] == ["failed", "failed", "skipped"]
            assert peer.input_tokens == 300 and peer.output_tokens == 80
            assert peer.settled_cost_usd is not None
            assert unused.input_tokens == unused.output_tokens == 0
            peer_roles = list(
                await session.scalars(
                    select(AgentResearchOutputRecord.role)
                    .where(
                        AgentResearchOutputRecord.run_id == peer.id,
                        AgentResearchOutputRecord.role.is_not(None),
                    )
                    .order_by(AgentResearchOutputRecord.sequence)
                )
            )
            assert peer_roles == [
                "orchestrator",
                "fundamental",
                "market_state",
                "macro_sector",
                "bull",
            ]
            budgets = {
                row.run_id: row
                for row in await session.scalars(select(AgentDailyBudgetReservationRecord))
            }
            assert budgets[failed.id].state == ("reserved" if unknown else "settled")
            assert budgets[peer.id].state == "settled"
            assert budgets[unused.id].state == "released"
            now = await session.scalar(select(func.clock_timestamp()))
            if unknown:
                with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                    await require_manual_account_available(
                        session, account_id=settings.agent_paper_account_id, now=now
                    )
            else:
                await require_manual_account_available(
                    session, account_id=settings.agent_paper_account_id, now=now
                )
    finally:
        await factory.kw["bind"].dispose()
