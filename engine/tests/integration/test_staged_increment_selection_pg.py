"""Normal increment preparation selects a protocol before any Worker can claim it."""

import pytest
from helpers.staged_routes import selected_route
from integration import test_staged_worker_pg as worker_cases
from nexus.persistence.models.monitoring import MonitorIncrement
from sqlalchemy import select, text


class PreparedAborted(BaseException):
    pass


@pytest.mark.asyncio
@pytest.mark.parametrize("unknown", [False, True])
async def test_price_only_normal_entry_selects_same_worker_with_original_usage_guards(
    migrated_database_url, monkeypatch, unknown
):
    from helpers.staged_routes import install_increment_preparation
    from helpers.staged_worker import run_staged_worker
    from integration import test_monitor_increment_pg as price_cases

    original = price_cases.ready_trigger
    captured = {}

    async def prepare(*args, **kwargs):
        result = await original(*args, **kwargs)
        factory, _, _, _, now = result
        captured["fixture"] = (factory, None, None, {"clock": [now]}, None, None, None)
        install_increment_preparation(monkeypatch)
        return result

    async def worker(settings, **kwargs):
        factory = captured["fixture"][0]
        async with factory() as session:
            member = (await session.scalars(select(MonitorIncrement))).one()
            route = await selected_route(session, member.job_id)
            assert route is not None and route["kind"] == "increment"
            assert route["scope_id"] == member.id
        return await run_staged_worker(
            settings, monkeypatch=monkeypatch, fixture=captured["fixture"], **kwargs
        )

    monkeypatch.setattr(price_cases, "ready_trigger", prepare)
    monkeypatch.setattr("nexus.worker.run_agent_worker_once", worker)
    # Original normal CLI, native once-only claim, financial, usage-unknown,
    # parent binding and formal-consumption rejection assertions remain intact.
    original_case = getattr(
        price_cases,
        "test_normal_trigger_admission_is_single_stock_idempotent_and_blocks_competing_entry",
    )
    await original_case(migrated_database_url, monkeypatch, unknown)


@pytest.mark.asyncio
@pytest.mark.parametrize("independent", [False, True])
async def test_normal_increment_is_selected_before_claim_and_keeps_parent_protocol(
    migrated_database_url, monkeypatch, tmp_path, independent
):
    native = worker_cases.run_staged_worker
    inspected = []

    async def selected(settings, **kwargs):
        from nexus.persistence.models.runtime import Job
        from sqlalchemy.exc import DBAPIError

        factory = kwargs["fixture"][0]
        first = not inspected
        async with factory() as session:
            member = (await session.scalars(select(MonitorIncrement))).one()
            route = await selected_route(session, member.job_id)
            assert route is not None, "normal preparation did not select the increment protocol"
            assert (route["kind"], route["scope_id"], route["run_id"]) == (
                "increment",
                member.id,
                member.run_id,
            )
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_route")) == 1
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_authorization")) == (
                0 if first else 3
            )
            inspected.append(member.run_id)
        options = {
            key: value for key, value in kwargs.items() if key not in {"monkeypatch", "fixture"}
        }
        # A legacy Worker cannot claim a newly selected identity.
        if first:
            with pytest.raises(DBAPIError, match="staged worker required"):
                await worker_cases.run_agent_worker_once(settings, **options)
            async with factory() as session:
                job = await session.get(Job, member.job_id)
                assert job.status == "queued" and job.attempt_count == 0
        result = await native(settings, **kwargs)
        assert result is first  # Original fixture verifies one run, then an empty poll.
        async with factory() as session:
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_authorization")) == 3
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_terminal")) == 1
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_route")) == 1
        return result

    monkeypatch.setattr(worker_cases, "run_staged_worker", selected)
    await worker_cases.exercise(
        migrated_database_url, monkeypatch, tmp_path, "unresolved", independent=independent
    )
    assert len(inspected) == 2 and inspected[0] == inspected[1]


@pytest.mark.asyncio
async def test_unselected_increment_keeps_legacy_execution_without_stage_authorization(
    migrated_database_url, monkeypatch, tmp_path
):
    native = worker_cases.run_staged_worker
    checked = []
    from helpers.staged_routes import install_increment_preparation
    from nexus.monitoring import mixed_admission

    original_admit = mixed_admission.admit_batch
    original_arguments = {}

    async def remember(session, **kwargs):
        original_arguments.update(kwargs)
        return await original_admit(session, **kwargs)

    monkeypatch.setattr(mixed_admission, "admit_batch", remember)

    async def legacy(settings, **kwargs):
        factory = kwargs["fixture"][0]
        install_increment_preparation(monkeypatch)
        async with factory.begin() as session:
            old = (await session.scalars(select(MonitorIncrement))).one()
            repeated = await mixed_admission.admit_batch(session, **original_arguments)
            assert repeated.id == old.id and repeated.run_id == old.run_id
            assert await selected_route(session, old.job_id) is None
        result = await native(settings, **kwargs)
        async with factory() as session:
            table = await session.scalar(text("SELECT to_regclass('stage_proto_authorization')"))
            assert (
                table is None
                or await session.scalar(text("SELECT count(*) FROM stage_proto_authorization")) == 0
            ), "an unselected old increment was implicitly upgraded"
        checked.append(True)
        return result

    monkeypatch.setattr(worker_cases, "run_staged_worker", legacy)
    await worker_cases.exercise(
        migrated_database_url,
        monkeypatch,
        tmp_path,
        "unresolved",
        controls={"select_increment_protocol": False},
    )
    assert checked == [True, True]  # Normal work followed by the original empty poll.


@pytest.mark.asyncio
@pytest.mark.parametrize("public_catch", [False, True])
async def test_increment_selection_and_budget_rollback_together_before_public_visibility(
    migrated_database_url, monkeypatch, tmp_path, public_catch
):
    from helpers import staged_routes
    from helpers.staged_protocol import ProtocolRejected
    from nexus.persistence.models.agents import (
        AgentDailyBudgetReservationRecord,
        AgentResearchRunRecord,
    )
    from nexus.persistence.models.runtime import Job

    native = staged_routes.register_prepared_runs
    captured = {}
    prepare = worker_cases.mixed.prepared_materials
    original_entry = worker_cases.mixed.run

    async def entry(settings, args):
        receipt = await original_entry(settings, args)
        if public_catch and args.action == "admit-batch":
            assert receipt["status"] == "blocked"
            assert receipt["error_code"] == "MONITOR_CONTEXT_INVALID"
            assert receipt["execution_locked"]
            assert receipt["model_requests"] == receipt["provider_requests"] == 0
            captured["receipt"] = receipt
            # The normal entry has returned after committing its real diagnostic.
            raise PreparedAborted
        return receipt

    async def remember(*args, **kwargs):
        fixture = await prepare(*args, **kwargs)
        captured["factory"] = fixture[0]
        captured["runtime"] = fixture[3]
        async with fixture[0].begin() as session:
            if public_catch:
                await staged_routes.ensure_routes(session)
            captured["old_budgets"] = await worker_cases.mixed.budget_rows(session)
            captured["old_attempts"] = await session.scalar(
                text("SELECT count(*) FROM job_attempts")
            )
        return fixture

    async def abort(session, run_ids):
        await native(session, run_ids)
        run_id = tuple(run_ids)[0]
        member = (await session.scalars(select(MonitorIncrement))).one()
        assert member.run_id == run_id
        assert await selected_route(session, member.job_id) is not None
        async with captured["factory"]() as observer:
            assert await observer.get(AgentResearchRunRecord, run_id) is None
            assert await observer.get(Job, member.job_id) is None
            assert (
                await observer.scalar(
                    select(AgentDailyBudgetReservationRecord).where(
                        AgentDailyBudgetReservationRecord.run_id == run_id
                    )
                )
                is None
            )
            assert await observer.get(MonitorIncrement, member.id) is None
        captured["run_id"] = run_id
        captured["job_id"] = member.job_id
        captured["scope_id"] = member.id
        if public_catch:
            raise ProtocolRejected("MONITOR_CONTEXT_INVALID")
        raise PreparedAborted

    monkeypatch.setattr(worker_cases.mixed, "run", entry)
    monkeypatch.setattr(worker_cases.mixed, "prepared_materials", remember)
    monkeypatch.setattr(staged_routes, "register_prepared_runs", abort)
    with pytest.raises(PreparedAborted):
        await worker_cases.exercise(migrated_database_url, monkeypatch, tmp_path, "unresolved")
    async with captured["factory"]() as session:
        assert await session.get(AgentResearchRunRecord, captured["run_id"]) is None
        assert await session.get(Job, captured["job_id"]) is None
        assert (await session.scalars(select(MonitorIncrement))).all() == []
        assert await session.scalar(text("SELECT count(*) FROM agent_research_runs")) == 3
        assert (
            await session.scalar(text("SELECT count(*) FROM agent_daily_budget_reservations")) == 3
        )
        assert await worker_cases.mixed.budget_rows(session) == captured["old_budgets"]
        assert (
            await session.scalar(text("SELECT count(*) FROM job_attempts"))
            == captured["old_attempts"]
        )
        assert captured["runtime"].get("sdk_run_calls", 0) == 0
        table = await session.scalar(text("SELECT to_regclass('stage_proto_route')"))
        assert (table is not None) is public_catch
        if public_catch:
            from uuid import UUID

            from nexus.persistence.models.monitoring import MonitorTrigger

            batch_id = UUID(captured["receipt"]["batch_id"])
            for table in ("scope", "route", "prepared_job", "authorization"):
                assert await session.scalar(text(f"SELECT count(*) FROM stage_proto_{table}")) == 0
            triggers = (
                await session.scalars(
                    select(MonitorTrigger).where(MonitorTrigger.batch_id == batch_id)
                )
            ).all()
            assert triggers and all(
                t.status == "blocked" and t.research_run_id is None for t in triggers
            )
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM audit_events WHERE "
                        "action='research.monitor.batch_admission_blocked' "
                        "AND payload->>'batch_id'=:batch"
                    ),
                    {"batch": str(batch_id)},
                )
                == 1
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("preexisting_member", [False, True])
async def test_unselected_increment_cannot_acquire_stage_send_rights_after_normal_claim(
    migrated_database_url, monkeypatch, tmp_path, preexisting_member
):
    from datetime import timedelta

    from helpers.staged_protocol import StagedProtocol
    from helpers.staged_routes import ensure_routes
    from nexus.jobs import worker as jobs_worker
    from nexus.worker import run_agent_worker_once
    from sqlalchemy.exc import DBAPIError

    class ClaimCaptured(BaseException):
        def __init__(self, claim):
            self.claim = claim

    async def check(settings, **kwargs):
        fixture = kwargs.pop("fixture")
        kwargs.pop("monkeypatch")
        factory, captured = fixture[0], fixture[3]
        now = captured["clock"][0]

        async def capture(session, *, claim, **options):
            raise ClaimCaptured(claim)

        with monkeypatch.context() as patch:
            patch.setattr(jobs_worker, "execute_and_finalize_job", capture)
            with pytest.raises(ClaimCaptured) as found:
                await run_agent_worker_once(settings, **kwargs)
        claim = found.value.claim
        async with factory() as session:
            member = (await session.scalars(select(MonitorIncrement))).one()
        protocol = StagedProtocol(factory)
        install = dict(
            scope_id=member.id,
            claims=(claim,),
            now=now,
            work_deadline=now + timedelta(seconds=100),
            cleanup_deadline=now + timedelta(seconds=110),
            increment=True,
        )
        if preexisting_member:
            await protocol.install(**install)
        async with factory.begin() as session:
            await ensure_routes(session)
        with pytest.raises(DBAPIError, match="staged increment route required"):
            if preexisting_member:
                await protocol.authorize(claim, stage=1, now=now, wire=b"increment-wire")
            else:
                await protocol.install(**install)
        async with factory() as session:
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_authorization")) == 0
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_route")) == 0
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM agent_daily_budget_reservations "
                        "WHERE state='reserved'"
                    )
                )
                == 1
            )
        assert captured.get("sdk_run_calls", 0) == 0
        raise PreparedAborted

    monkeypatch.setattr(worker_cases, "run_staged_worker", check)
    with pytest.raises(PreparedAborted):
        await worker_cases.exercise(
            migrated_database_url,
            monkeypatch,
            tmp_path,
            "unresolved",
            controls={"select_increment_protocol": False},
        )
