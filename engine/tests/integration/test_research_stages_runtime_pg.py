"""Formal entries use production stage execution; only SDK/provider edges are controlled."""

import pytest
from helpers.formal_sdk import controlled_sdk
from sqlalchemy import text


class BeforeWorker(BaseException):
    pass


@pytest.mark.asyncio
@pytest.mark.parametrize("independent", [False, True])
async def test_normal_root_selects_formal_protocol_without_test_registration(
    migrated_database_url, monkeypatch, tmp_path, independent
):
    from helpers import staged_routes
    from integration.test_staged_roots_pg import (
        test_normal_three_stock_roots_use_one_staged_protocol as prepare,
    )

    async def noop(*args, **kwargs):
        pass

    monkeypatch.setattr(staged_routes, "ensure_routes", noop)
    monkeypatch.setattr(staged_routes, "register_prepared_runs", noop)

    async def inspect(captured, settings, options):
        factory = captured["fixture"][0]
        async with factory() as session:
            assert await session.scalar(text("SELECT count(*) FROM research_stage_route")) == 3
            assert (
                await session.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 0
            )
        raise BeforeWorker

    with pytest.raises(BeforeWorker):
        await prepare(
            migrated_database_url, monkeypatch, tmp_path, independent, False, before_worker=inspect
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("independent", [False, True])
async def test_formal_roots_real_worker_sdk_and_public_closeout(
    migrated_database_url, monkeypatch, tmp_path, independent
):
    import asyncio
    from datetime import timedelta

    from helpers import disclosure_observations
    from integration import test_candidate_multistock_path as candidates
    from nexus import worker
    from nexus.agents.research_stages.proof import read_terminal_proof
    from nexus.persistence.models.agents import AgentResearchRunRecord
    from sqlalchemy import select

    calls = controlled_sdk(monkeypatch)
    captured = {}
    native_cycle = disclosure_observations.normal_cycle
    native_worker = candidates.run_agent_worker_once

    class Checked(BaseException):
        pass

    async def verify(factory):
        async with factory() as session:
            assert await session.scalar(text("SELECT count(*) FROM research_stage_route")) == 3
            assert (
                await session.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 9
            )
            assert (
                await session.scalar(
                    text("SELECT count(*) FROM research_stage_terminal WHERE known")
                )
                == 3
            )
            runs = tuple(await session.scalars(select(AgentResearchRunRecord)))
            for run in runs:
                proof = await read_terminal_proof(session, run_id=run.id, now=captured["clock"][0])
                assert proof.known and proof.cleanup_confirmed and not proof.orphan
        assert len(calls) == 9
        for close, thread in calls:
            thread.run.assert_awaited_once()
            close.assert_awaited_once()

    async def cycle(*args, **kwargs):
        before = kwargs["before_research"]

        async def prepare(settings, factory, authority, manifest, clock):
            await before(settings, factory, authority, manifest, clock)
            captured.update(clock=clock, factory=factory)

        async def stop_before_old_downgrade(factory):
            await verify(factory)
            raise Checked

        kwargs["before_research"] = prepare
        kwargs["before_downgrade"] = stop_before_old_downgrade
        return await native_cycle(*args, **kwargs)

    async def run(settings, **kwargs):
        clock = captured["clock"]
        base = worker.datetime
        at, anchor = clock[0], asyncio.get_running_loop().time()

        class Clock(base):
            @classmethod
            def now(cls, tz=None):
                clock[0] = max(
                    clock[0], at + timedelta(seconds=asyncio.get_running_loop().time() - anchor)
                )
                return clock[0] if tz is None else clock[0].astimezone(tz)

        monkeypatch.setattr(worker, "datetime", Clock)
        try:
            return await native_worker(settings, **kwargs)
        finally:
            Clock.now()
            monkeypatch.setattr(worker, "datetime", base)

    monkeypatch.setattr(disclosure_observations, "normal_cycle", cycle)
    monkeypatch.setattr(candidates, "run_agent_worker_once", run)
    if independent:
        await disclosure_observations.reader_source(
            migrated_database_url, monkeypatch, tmp_path, independent=True
        )
        await verify(captured["factory"])
    else:
        with pytest.raises(Checked):
            await disclosure_observations.reader_source(
                migrated_database_url, monkeypatch, tmp_path
            )


def advancing_formal_clock(monkeypatch):
    import asyncio
    from datetime import timedelta

    from helpers import disclosure_observations
    from integration import test_candidate_multistock_path as candidates
    from nexus import worker

    captured = {}
    native_cycle = disclosure_observations.normal_cycle
    native_worker = worker.run_agent_worker_once

    async def cycle(*args, **kwargs):
        before = kwargs["before_research"]

        async def prepare(settings, factory, authority, manifest, clock):
            await before(settings, factory, authority, manifest, clock)
            captured.update(clock=clock, factory=factory)

        kwargs["before_research"] = prepare
        return await native_cycle(*args, **kwargs)

    async def run(settings, **kwargs):
        clock = captured["clock"]
        base = worker.datetime
        at, anchor = clock[0], asyncio.get_running_loop().time()

        class Clock(base):
            @classmethod
            def now(cls, tz=None):
                clock[0] = max(
                    clock[0], at + timedelta(seconds=asyncio.get_running_loop().time() - anchor)
                )
                return clock[0] if tz is None else clock[0].astimezone(tz)

        monkeypatch.setattr(worker, "datetime", Clock)
        try:
            return await native_worker(settings, **kwargs)
        finally:
            Clock.now()
            monkeypatch.setattr(worker, "datetime", base)

    monkeypatch.setattr(disclosure_observations, "normal_cycle", cycle)
    monkeypatch.setattr(candidates, "run_agent_worker_once", run)
    monkeypatch.setattr(worker, "run_agent_worker_once", run)
    return captured


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", [False, True, "capacity"])
async def test_formal_increment_scan_settlement_and_next_admission(
    migrated_database_url, monkeypatch, tmp_path, outcome
):
    from integration.test_monitor_mixed_increment_pg import (
        test_sealed_price_and_two_filings_admit_one_single_stock_job_and_three_calls as scenario,
    )

    controlled_sdk(monkeypatch)
    captured = advancing_formal_clock(monkeypatch)
    await scenario(migrated_database_url, monkeypatch, tmp_path, outcome, independent=True)
    async with captured["factory"]() as session:
        assert await session.scalar(text("SELECT count(*) FROM research_stage_route")) == 4
        assert await session.scalar(text("SELECT count(*) FROM research_stage_terminal")) == (
            3 if outcome == "capacity" else 4
        )


@pytest.mark.asyncio
async def test_zero_call_receipt_cannot_override_unknown_trigger(
    migrated_database_url, monkeypatch, tmp_path
):
    from integration.test_monitor_mixed_increment_pg import (
        test_sealed_price_and_two_filings_admit_one_single_stock_job_and_three_calls as scenario,
    )
    from nexus.monitoring.admission import require_monitor_account_available
    from nexus.persistence.models.monitoring import MonitorIncrement, MonitorTrigger
    from sqlalchemy import select

    controlled_sdk(monkeypatch)
    captured = advancing_formal_clock(monkeypatch)
    await scenario(migrated_database_url, monkeypatch, tmp_path, "capacity", independent=True)
    async with captured["factory"]() as session:
        member = await session.scalar(select(MonitorIncrement))
        triggers = tuple(
            await session.scalars(
                select(MonitorTrigger).where(MonitorTrigger.research_run_id == member.run_id)
            )
        )
        # A transient unknown projection must never grant admission, even beside
        # a valid zero-call receipt. Do not rewrite persisted terminal facts.
        assert len(triggers) == 2
        for trigger in triggers:
            trigger.status = "unknown"
        with session.no_autoflush:
            from nexus.agents.research_stages.public import _zero_call_increment

            assert await _zero_call_increment(
                session, run_id=member.run_id, now=captured["clock"][0]
            )
            with pytest.raises(ValueError, match="MONITOR_ACCOUNT_BUSY_OR_UNKNOWN"):
                await require_monitor_account_available(
                    session, account_id=member.account_id, now=captured["clock"][0]
                )


@pytest.mark.asyncio
async def test_formal_known_usage_cleanup_failure_keeps_admission_closed(
    migrated_database_url, monkeypatch, tmp_path
):
    from integration.test_monitor_mixed_increment_pg import (
        test_sealed_price_and_two_filings_admit_one_single_stock_job_and_three_calls as scenario,
    )
    from nexus import worker
    from nexus.agents import codex_sdk
    from nexus.monitoring.admission import finish_increment, require_monitor_account_available
    from nexus.persistence.models.agents import (
        AgentDailyBudgetReservationRecord,
        AgentResearchRunRecord,
    )
    from nexus.persistence.models.monitoring import MonitorIncrement
    from sqlalchemy import select

    calls = controlled_sdk(monkeypatch)
    captured = advancing_formal_clock(monkeypatch)
    create, run = codex_sdk.AsyncCodex, worker.run_agent_worker_once
    created = []

    def sdk(config):
        client = create(config)
        created.append(client)
        if len(created) == 10:  # Nine completed root stages, first child stage.

            async def failed_close():
                raise RuntimeError("controlled SDK cleanup failure")

            client.close.side_effect = failed_close
        return client

    monkeypatch.setattr(codex_sdk, "AsyncCodex", sdk)

    class Observed(BaseException):
        pass

    async def checked(settings, **kwargs):
        result = await run(settings, **kwargs)
        async with captured["factory"]() as session:
            member = await session.scalar(
                select(MonitorIncrement).where(MonitorIncrement.job_id == kwargs["job_ids"][0])
            )
            if member is None:
                return result
            child = await session.get(AgentResearchRunRecord, member.run_id)
            budget = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == child.id
                )
            )
            assert child.status == "failed"
            assert (child.input_tokens, child.output_tokens) == (100, 100)
            assert budget.state == "settled" and budget.settled_usd == child.settled_cost_usd
            assert (
                await session.scalar(
                    text("SELECT known FROM research_stage_terminal WHERE run_id=:run"),
                    {"run": child.id},
                )
                is True
            )
            assert (
                await session.scalar(
                    text("SELECT count(*) FROM research_stage_cleanup WHERE run_id=:run"),
                    {"run": child.id},
                )
                == 0
            )
        async with captured["factory"].begin() as session:
            trigger = await finish_increment(
                session,
                account_id=member.account_id,
                trigger_id=member.trigger_id,
                now=captured["clock"][0],
            )
            assert trigger.status == "admitted"
            with pytest.raises(ValueError, match="MONITOR_ACCOUNT_BUSY_OR_UNKNOWN"):
                await require_monitor_account_available(
                    session, account_id=member.account_id, now=captured["clock"][0]
                )
        assert len(created) == len(calls) == 10
        raise Observed

    monkeypatch.setattr(worker, "run_agent_worker_once", checked)
    with pytest.raises(Observed):
        await scenario(migrated_database_url, monkeypatch, tmp_path, "unresolved", independent=True)


@pytest.mark.asyncio
async def test_unselected_v11_fixture_retains_original_output_downgrade_fence(
    migrated_database_url, monkeypatch, tmp_path
):
    """A legacy preparation still reaches the original V11 immutable-output fence."""
    from integration.test_candidate_multistock_path import test_v11_manual_entry_to_result
    from nexus.agents.research_stages import routes

    async def legacy_preparation(session, run_ids):
        return None

    monkeypatch.setattr(routes, "register_new_prepared_runs", legacy_preparation)
    await test_v11_manual_entry_to_result(migrated_database_url, monkeypatch, tmp_path, 3, False)
