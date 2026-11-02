"""Normal independent admission, immutable native routes and shared holds; no network."""

import hashlib
import json
from datetime import timedelta
from decimal import Decimal

import pytest
from integration.test_research_request_pg import prepare_scope
from integration.test_sol_profile_pg import current_record, register
from nexus.ops.openai_local import prepare_database
from nexus.paper.manual_request import admit_manual_request, claim_manual_request
from nexus.paper.research_request import prepare_research_request
from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord
from nexus.persistence.models.runtime import Job
from nexus.settings import AgentRuntimeMode
from pydantic import SecretStr
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from unit.ops.test_formal_stage_registration import OPTIONS


async def scope(url, tmp_path):
    factory, settings, store, ids = await prepare_scope(url, tmp_path, bounded=True)
    await prepare_database(
        settings,
        **(
            OPTIONS
            | dict(
                max_wire_bytes=100479,
                full_input_capacity=True,
                formal_bounded_stage_budget=True,
                price_monitoring=True,
                disclosure_monitoring=True,
            )
        ),
    )
    model_id = await register(factory, await current_record(factory))
    settings = settings.model_copy(
        update=dict(
            agent_paper_formal_model_config_id=model_id,
            agent_runtime_mode=AgentRuntimeMode.RESPONSES,
            openai_api_key=SecretStr("sk-proj-" + "synthetic-only" * 5),
            openai_extraction_model="gpt-6.1-sol",
            openai_reasoning_model="gpt-6.1-sol",
            agent_daily_budget_usd=Decimal("10"),
        )
    )
    return factory, settings, store, ids


async def admit(factory, settings, store, *, two_flight=True, claim=True):
    async with factory.begin() as session:
        session.info["sec_exhibit_store"] = store
        receipt = await admit_manual_request(
            session,
            account_id=settings.agent_paper_account_id,
            batch_id=settings.agent_paper_candidate_batch_id,
            model_config_id=settings.agent_paper_formal_model_config_id,
            mode="research_and_monitor",
            contract_version=3,
            idempotency_key="native-sol-one",
            runtime_mode="responses",
            daily_limit_usd=settings.agent_daily_budget_usd,
            two_flight=two_flight,
        )
        if claim:
            await claim_manual_request(session, request_id=receipt.request_id)
    return receipt.request_id


@pytest.mark.asyncio
async def test_formal_manual_request_keeps_normal_owner_and_requires_matching_runtime(
    migrated_database_url, tmp_path
):
    factory, settings, store, _ = await scope(migrated_database_url, tmp_path)
    try:
        common = dict(
            account_id=settings.agent_paper_account_id,
            batch_id=settings.agent_paper_candidate_batch_id,
            model_config_id=settings.agent_paper_formal_model_config_id,
            mode="research_and_paper",
            contract_version=1,
            daily_limit_usd=settings.agent_daily_budget_usd,
        )
        with pytest.raises(ValueError, match="MANUAL_CONFIGURATION_UNAVAILABLE"):
            async with factory.begin() as session:
                session.info["sec_exhibit_store"] = store
                await admit_manual_request(
                    session, **common, idempotency_key="wrong-runtime", runtime_mode="codex_sdk"
                )
        async with factory.begin() as session:
            session.info["sec_exhibit_store"] = store
            receipt = await admit_manual_request(
                session, **common, idempotency_key="formal-native", runtime_mode="responses"
            )
            assert receipt.status == "queued" and receipt.mode == "research_and_paper"
            assert receipt.cycle_id is None
            assert await session.scalar(select(Job.id).limit(1)) is None
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_normal_sol_preparation_freezes_all_three_native_routes_and_shared_holds(
    migrated_database_url, tmp_path
):
    factory, settings, store, ids = await scope(migrated_database_url, tmp_path)
    try:
        request_id = await admit(factory, settings, store)
        async with factory.begin() as session:
            session.info["sec_exhibit_store"] = store
            result = await prepare_research_request(session, settings, request_id=request_id)
            assert result.status == "active" and result.cycle_id is None
            assert {m.instrument_id for m in result.research.members} == set(ids)
            async with factory() as observer:
                assert await observer.scalar(text("SELECT count(*) FROM research_stage_route")) == 0
                assert await observer.scalar(select(Job.id).limit(1)) is None
        async with factory() as session:
            rows = (
                (await session.execute(text("SELECT * FROM research_stage_route"))).mappings().all()
            )
            assert len(rows) == 3
            for row in rows:
                assert row["kind"] == "independent" and row["scope_id"] == request_id
                binding = row["platform_binding"]
                assert binding["transport"] == "platform_responses"
                assert binding["model_config_id"] == str(
                    settings.agent_paper_formal_model_config_id
                )
                assert binding["allocation"]["profile"]["model"] == "gpt-6.1-sol"
                assert binding["allocation"]["output_limit"] == 8192
            holds = (await session.scalars(select(AgentDailyBudgetReservationRecord))).all()
            assert len(holds) == 3 and all(h.state == "reserved" for h in holds)
            assert sum(h.reserved_usd for h in holds) <= settings.agent_daily_budget_usd
            assert await session.scalar(text("SELECT count(*) FROM research_comparison_chain")) == 0
            assert (
                await session.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 0
            )
            from nexus.agents.research_stages.routes import selected_route

            original = await selected_route(session, rows[0]["job_id"])
            assert original["platform_binding"] == rows[0]["platform_binding"]
        with pytest.raises(DBAPIError, match="immutable"):
            async with factory.begin() as session:
                await session.execute(
                    text("UPDATE research_stage_route SET platform_binding=NULL WHERE run_id=:run"),
                    {"run": rows[0]["run_id"]},
                )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_formal_root_preserves_cycle_owner_and_freezes_native_sol_routes(
    migrated_database_url, monkeypatch, after_prepared=None, monitoring=False
):
    from integration import test_candidate_multistock_path as candidates
    from nexus.agents.platform_native import credential_for_preparation
    from nexus.persistence.session import session_factory
    from nexus.settings import EngineSettings

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        now = await session.scalar(text("SELECT clock_timestamp()"))
    target = now.replace(hour=15, minute=0, second=0, microsecond=0)
    while target <= now or target.weekday() >= 5:
        target += timedelta(days=1)
    monkeypatch.setattr(candidates, "AT", target)
    native_start = candidates.start_schwab_agent_paper_cycle
    captured = {}

    class Prepared(BaseException):
        pass

    async def prepare(session, **kwargs):
        from integration.test_sol_profile_pg import SOURCE
        from nexus.agents.platform_profile import register_sol_business_profile

        record = await current_record(factory)
        record["expires_at"] = (now + timedelta(days=6)).isoformat()
        model = await register_sol_business_profile(
            session, price_record=record, source_evidence=SOURCE
        )
        settings = EngineSettings(database_url=migrated_database_url).model_copy(
            update=dict(
                agent_runtime_mode=AgentRuntimeMode.RESPONSES,
                openai_api_key=SecretStr("sk-proj-" + "synthetic-only" * 5),
            )
        )
        result = await native_start(
            session,
            **(
                kwargs
                | dict(
                    formal_model_config_id=model.id,
                    daily_limit_usd=Decimal("10"),
                    platform_credential=credential_for_preparation(settings),
                )
            ),
        )
        assert result.status == "researching"
        captured["cycle"] = result.cycle_id
        captured["settings"] = settings.model_copy(
            update={
                "openai_extraction_model": "gpt-6.1-sol",
                "openai_reasoning_model": "gpt-6.1-sol",
                "agent_daily_budget_usd": Decimal("10"),
                "agent_paper_formal_model_config_id": model.id,
            }
        )
        await session.commit()
        raise Prepared()

    monkeypatch.setattr(candidates, "start_schwab_agent_paper_cycle", prepare)
    try:
        with pytest.raises(Prepared):
            await candidates.test_fresh_multistock_candidate_cycle_freezes_native_jobs(
                migrated_database_url,
                monkeypatch,
                count=3,
                trade=False,
                profile="formal_v11",
                monitoring=monitoring,
            )
        async with factory() as session:
            rows = (
                (await session.execute(text("SELECT * FROM research_stage_route"))).mappings().all()
            )
            assert len(rows) == 3
            assert all(r["kind"] == "formal" and r["scope_id"] == captured["cycle"] for r in rows)
            assert all(r["platform_binding"]["transport"] == "platform_responses" for r in rows)
            assert (
                await session.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 0
            )
        if after_prepared is not None:
            await after_prepared(factory, captured["settings"], captured["cycle"])
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["credential", "model", "rehashed_limit", "transport"])
async def test_native_route_sql_rejects_rehashed_forgery_and_rolls_back_complete_pool(
    migrated_database_url, tmp_path, monkeypatch, fault
):
    from nexus.agents import platform_native

    original = platform_native.freeze_native_route_binding

    async def corrupt(*args, **kwargs):
        value = await original(*args, **kwargs)
        if fault == "credential":
            value["credential_binding_hash"] = "missing"
        elif fault == "model":
            value["model_hash"] = "0" * 64
        elif fault == "transport":
            value["transport"] = "codex_sdk"
        else:
            value["allocation"]["stage_input_upper_bounds"] = [272001] * 3
            value["allocation_hash"] = hashlib.sha256(
                json.dumps(
                    value["allocation"], sort_keys=True, ensure_ascii=False, separators=(",", ":")
                ).encode()
            ).hexdigest()
        return value

    monkeypatch.setattr(platform_native, "freeze_native_route_binding", corrupt)
    factory, settings, store, _ = await scope(migrated_database_url, tmp_path)
    try:
        request_id = await admit(factory, settings, store)
        async with factory.begin() as session:
            session.info["sec_exhibit_store"] = store
            with pytest.raises(DBAPIError, match="PLATFORM_NATIVE_ROUTE_INVALID"):
                await prepare_research_request(session, settings, request_id=request_id)
        async with factory() as session:
            for table in (
                "agent_research_runs",
                "jobs",
                "agent_daily_budget_reservations",
                "research_stage_route",
                "research_request_manifests",
            ):
                assert await session.scalar(text(f"SELECT count(*) FROM {table}")) == 0
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["missing_key", "shared_budget"])
async def test_native_pool_preparation_cannot_leave_partial_jobs_or_holds(
    migrated_database_url, tmp_path, fault
):
    factory, settings, store, _ = await scope(migrated_database_url, tmp_path)
    settings = settings.model_copy(
        update=(
            {"openai_api_key": None}
            if fault == "missing_key"
            else {"agent_daily_budget_usd": Decimal("2.13")}
        )
    )
    try:
        request_id = await admit(factory, settings, store)
        async with factory.begin() as session:
            session.info["sec_exhibit_store"] = store
            with pytest.raises(
                ValueError,
                match=(
                    "PLATFORM_CREDENTIAL_REQUIRED"
                    if fault == "missing_key"
                    else "daily research budget exhausted"
                ),
            ):
                await prepare_research_request(session, settings, request_id=request_id)
        async with factory() as session:
            for table in (
                "agent_research_runs",
                "jobs",
                "agent_daily_budget_reservations",
                "research_stage_route",
                "research_request_manifests",
            ):
                assert await session.scalar(text(f"SELECT count(*) FROM {table}")) == 0
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_normal_increment_selects_sol_and_retains_exact_sdk_predecessor(
    migrated_database_url, monkeypatch, after_admitted=None
):
    from helpers.paper_batch import CALENDAR_ID
    from integration import test_candidate_multistock_path as candidates
    from integration import test_monitor_increment_pg as price_cases
    from nexus.monitoring.admission import require_increment_binding
    from nexus.monitoring.context import read_monitor_context
    from nexus.ops.research_monitor import parser
    from nexus.ops.research_monitor import run as monitor_entry
    from nexus.persistence.models.agents import AgentResearchRunRecord
    from nexus.persistence.models.monitoring import MonitorIncrement
    from nexus.persistence.session import session_factory
    from nexus.settings import EngineSettings

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        now = await session.scalar(text("SELECT clock_timestamp()"))
    target = now.replace(hour=15, minute=0, second=0, microsecond=0)
    while target <= now or target.weekday() >= 5:
        target += timedelta(days=1)
    monkeypatch.setattr(candidates, "AT", target)
    monkeypatch.setattr(price_cases, "AT", target)
    await factory.kw["bind"].dispose()
    factory, settings, trigger, parent, at = await price_cases.ready_trigger(
        migrated_database_url, monkeypatch
    )
    original = (parent.model_config_id, parent.model_hash, parent.pricing_version)
    record = await current_record(factory)
    record["expires_at"] = (now + timedelta(days=6)).isoformat()
    model_id = await register(factory, record)
    settings = settings.model_copy(
        update=dict(
            agent_runtime_mode=AgentRuntimeMode.RESPONSES,
            agent_paper_formal_model_config_id=model_id,
            openai_api_key=SecretStr("sk-proj-" + "synthetic-only" * 5),
            agent_daily_budget_usd=Decimal("10"),
        )
    )
    args = parser().parse_args(
        ["admit", "--trigger-id", str(trigger.id), "--calendar-authority-id", str(CALENDAR_ID)]
    )
    try:
        receipt = await monitor_entry(settings, args)
        assert receipt["status"] == "admitted", receipt
        async with factory() as session:
            member = (await session.scalars(select(MonitorIncrement))).one()
            child = await session.get(AgentResearchRunRecord, member.run_id)
            assert (
                child.model_config_id == model_id
                and child.model_config_id != parent.model_config_id
            )
            await require_increment_binding(session, child)
            bound = await read_monitor_context(
                session,
                snapshot_id=child.data_snapshot_id,
                instrument_id=child.instrument_id,
                as_of=child.decision_time,
            )
            assert bound[0].parent_run_id == parent.id
            row = (
                (
                    await session.execute(
                        text("SELECT * FROM research_stage_route WHERE run_id=:run"),
                        {"run": child.id},
                    )
                )
                .mappings()
                .one()
            )
            assert row["kind"] == "increment" and row["scope_id"] == member.id
            assert row["platform_binding"]["transport"] == "platform_responses"
            # Exercise the exact 0113 INSERT guard predicate against the registered
            # child and its actual SDK predecessor, before any provider invocation.
            invalid_routes = await session.execute(
                text("""
                SELECT nexus_sol_native_route_valid(jsonb_populate_record(r,
                           jsonb_build_object('platform_binding', NULL))),
                       nexus_sol_native_route_valid(jsonb_populate_record(r,
                           jsonb_build_object('run_id', CAST(:parent AS uuid)))),
                       nexus_sol_native_route_valid(jsonb_populate_record(r,
                           jsonb_build_object('kind', 'comparison')))
                  FROM research_stage_route r WHERE r.run_id=:child
            """),
                {"child": child.id, "parent": parent.id},
            )
            assert invalid_routes.one() == (False, False, False)
            previous = await session.get(AgentResearchRunRecord, parent.id)
            assert (
                previous.model_config_id,
                previous.model_hash,
                previous.pricing_version,
            ) == original
            hold = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == child.id
                )
            )
            assert hold.state == "reserved" and hold.settled_usd is None
            job = await session.get(Job, member.job_id)
            assert job.attempt_count == 0 and job.max_attempts == 1
            from types import SimpleNamespace
            from uuid import uuid4

            from nexus.agents.platform_native import native_increment_model_allowed

            # Selector compatibility is not a fallback, dispatch or rollback execution proof.
            assert await native_increment_model_allowed(
                session, parent=child, model_config_id=parent.model_config_id
            )
            assert not await native_increment_model_allowed(
                session, parent=child, model_config_id=uuid4()
            )
            changed_parent = SimpleNamespace(
                model_config_id=child.model_config_id,
                model_hash="0" * 64,
                pricing_version=child.pricing_version,
            )
            assert not await native_increment_model_allowed(
                session, parent=changed_parent, model_config_id=parent.model_config_id
            )
        if after_admitted is not None:
            await after_admitted(
                factory,
                settings.model_copy(
                    update={
                        "openai_extraction_model": "gpt-6.1-sol",
                        "openai_reasoning_model": "gpt-6.1-sol",
                    }
                ),
                member,
                trigger,
                parent,
            )
    finally:
        await factory.kw["bind"].dispose()
