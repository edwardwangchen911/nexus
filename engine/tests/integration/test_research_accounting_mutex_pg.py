"""Cross-day accounting publication and actual new reservations are serialized."""

import asyncio
from datetime import timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from integration.test_agent_persistence_pg import _seed_authority, _seed_runs
from integration.test_research_occupancy_pg import observed, verify
from nexus.agents import execution
from nexus.agents.persistence import (
    BudgetExhausted,
    _shared_daily_budget_used,
    reserve_daily_budget,
)
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def blocked_or_finished(factory, task, pid):
    async with asyncio.timeout(15):
        while not task.done():
            async with factory() as probe:
                if await probe.scalar(
                    text("SELECT cardinality(pg_blocking_pids(:pid))"), {"pid": pid}
                ):
                    return True
            await asyncio.sleep(0.02)
    return False


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["verified_overrun", "known_held_zero", "settled_zero"])
@pytest.mark.parametrize("publisher_first", [False, True])
async def test_cross_day_native_publication_and_new_allocation_linearize(
    migrated_database_url, monkeypatch, kind, publisher_first
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    tokens = (20_000_000, 0) if kind == "verified_overrun" else (0, 0)
    result, manifest, old, claim, now, protocol, ticket = await observed(factory, tokens=tokens)
    fresh = uuid4()
    await _seed_authority(factory)
    await _seed_runs(factory, (fresh,))
    tomorrow = now + timedelta(days=1)
    if kind != "verified_overrun":
        assert await verify(protocol, ticket, now, tokens=tokens)
        await protocol.cleanup(ticket, now=now, confirmed=True)

        async def refusal(*args, **kwargs):
            raise BudgetExhausted("synthetic bounded settlement refusal")

        if kind == "known_held_zero":
            monkeypatch.setattr(execution, "settle_daily_budget", refusal)
    entered, release, allocator_started, publisher_started = (asyncio.Event() for _ in range(4))
    state = {}
    original = AsyncSession.execute

    async def pause_publication(session, statement, *args, **kwargs):
        sql = str(statement)
        if sql.startswith("INSERT INTO research_stage_verification") or sql.startswith(
            "INSERT INTO research_stage_terminal"
        ):
            state["publisher_pid"] = await session.scalar(text("SELECT pg_backend_pid()"))
            publisher_started.set()
            answer = await original(session, statement, *args, **kwargs)
            if publisher_first:
                entered.set()
                await release.wait()
            return answer
        return await original(session, statement, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "execute", pause_publication)

    async def publish():
        # Capture PID before the prospective budget lock; _member is a read+scope lock.
        member = protocol._member

        async def capture(session, *args):
            row = await member(session, *args)
            state["publisher_pid"] = await session.scalar(text("SELECT pg_backend_pid()"))
            publisher_started.set()
            return row

        monkeypatch.setattr(protocol, "_member", capture)
        if kind == "verified_overrun":
            return await verify(protocol, ticket, now, tokens=tokens)
        return await protocol.close(claim, now=now)

    async def allocate():
        async with factory.begin() as session:
            cached_run = await session.get(AgentResearchRunRecord, result.run_id)
            assert cached_run.status == "prepared"
            state["allocator_pid"] = await session.scalar(text("SELECT pg_backend_pid()"))
            allocator_started.set()
            await reserve_daily_budget(
                session,
                run_id=fresh,
                now=tomorrow,
                daily_limit_usd=Decimal(1000),
                worst_case_usd=Decimal(2),
                pricing_version="fixture-pricing-v1",
                pricing_fresh=True,
            )
            if not publisher_first:
                entered.set()
                await release.wait()

    first = asyncio.create_task(publish() if publisher_first else allocate())
    second = None
    try:
        await asyncio.wait_for(entered.wait(), 15)
        second = asyncio.create_task(allocate() if publisher_first else publish())
        await asyncio.wait_for(
            (allocator_started if publisher_first else publisher_started).wait(), 15
        )
        pid = state["allocator_pid" if publisher_first else "publisher_pid"]
        assert await blocked_or_finished(factory, second, pid), (
            "different dates escaped shared accounting mutex"
        )
        release.set()
        await first
        if publisher_first and kind != "settled_zero":
            with pytest.raises(BudgetExhausted, match="RESEARCH_ACCOUNTING_UNSETTLED"):
                await second
        else:
            assert await second is (None if publisher_first else True)
        async with factory() as session:
            used = await _shared_daily_budget_used(session, business_date=tomorrow.date())
            assert used == (0 if publisher_first and kind != "settled_zero" else Decimal(2))
            assert (
                await session.scalar(
                    text("SELECT state FROM agent_daily_budget_reservations WHERE id=:id"),
                    {"id": old},
                )
                == "reserved"
            )
    finally:
        release.set()
        await asyncio.gather(first, *([] if second is None else [second]), return_exceptions=True)


async def accounting_locks(session, key):
    return (
        await session.execute(
            text("""
        SELECT pid, granted FROM pg_locks WHERE locktype='advisory'
        AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
        AND classid::bigint=((hashtext(:key)::bigint>>32)&4294967295)
        AND objid::bigint=(hashtext(:key)::bigint&4294967295) AND objsubid=1
    """),
            {"key": key},
        )
    ).all()


@pytest.mark.asyncio
@pytest.mark.parametrize("runtime", ["sdk", "responses"])
@pytest.mark.parametrize("same_day", [False, True])
async def test_legacy_provider_keeps_original_day_without_global_accounting_wait(
    migrated_database_url, monkeypatch, runtime, same_day
):
    """A native external wait must not serialize other dates or deadlock its own closeout."""
    from integration import test_agent_execution_pg as case
    from nexus.agents.models import ResearchRequest
    from nexus.agents.persistence import admit_research_run
    from nexus.agents.research_dispatch import current_dispatch

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    entered, release = asyncio.Event(), asyncio.Event()
    facts = {}
    native_worker = case.run_agent_worker_once

    async def exact_original_worker(settings, **options):
        if "job" not in facts:
            async with factory() as session:
                # Before starting the original Worker only its native Job exists.
                jobs = (await session.execute(text("SELECT id FROM jobs"))).scalars().all()
                assert len(jobs) == 1
                facts["job"] = jobs[0]
        # The new contender is a real queued Job. Recheck only the original identity.
        return await native_worker(settings, job_ids=(facts["job"],), **options)

    monkeypatch.setattr(case, "run_agent_worker_once", exact_original_worker)

    async def pause():
        if entered.is_set():
            return
        assert current_dispatch() is None
        async with factory() as session:
            assert await session.scalar(text("SELECT count(*) FROM research_stage_route")) == 0
            row = (
                await session.execute(
                    text(
                        "SELECT r.id, r.model_config_id, b.business_date "
                        "FROM agent_research_runs r "
                        "JOIN agent_daily_budget_reservations b ON b.run_id=r.id"
                    )
                )
            ).one()
            facts.update(run=row.id, model=row.model_config_id, day=row.business_date)
            facts["global"] = await accounting_locks(session, "agent-budget:accounting")
            facts["day_locks"] = await accounting_locks(
                session, f"agent-budget:{row.business_date}"
            )
        entered.set()
        await release.wait()

    if runtime == "responses":
        original_client = case._DurableResponsesClient

        def client():
            result = original_client()
            native = result.responses.create

            async def create(**kwargs):
                await pause()
                return await native(**kwargs)

            result.responses.create = create
            return result

        monkeypatch.setattr(case, "_DurableResponsesClient", client)
        coroutine = (
            case.test_responses_worker_reaches_terminal_with_injected_zero_network_transport(
                migrated_database_url
            )
        )
    else:

        async def sdk_case():
            import json
            from datetime import UTC, datetime

            from nexus.agents import codex_sdk, codex_staged
            from nexus.agents.responses_budget import UsageSnapshot
            from nexus.ops.openai_local import prepare_database
            from unit.agents.test_codex_sdk import _assessment_payload, _restore_view, _staged_batch

            settings = EngineSettings(
                database_url=migrated_database_url, agent_runtime_mode="codex_sdk"
            )
            await case._seed_authority(factory, include_base_evidence=False)
            await case._seed_database_tool_authority(
                factory, link_price=False, bar_count=1, price_quality_flags=("missing",)
            )
            await prepare_database(settings, structured_synthesis=True)
            async with factory() as session, session.begin():
                model_id = await session.scalar(
                    text("SELECT id FROM agent_model_configs WHERE version=:version"),
                    {"version": codex_sdk.STRUCTURED_CONFIG_VERSION},
                )
                admission = await admit_research_run(
                    session,
                    actor_id="legacy-sdk-compatibility",
                    route="/internal/v1/research-runs",
                    idempotency_key="one-original-run",
                    request_hash="b" * 64,
                    request=ResearchRequest(
                        instrument_id=case._INSTRUMENT_ID,
                        data_snapshot_id=case._SNAPSHOT_ID,
                        decision_time=case._NOW,
                    ),
                    prompt_version_id=None,
                    model_config_id=model_id,
                    daily_limit_usd=Decimal("3"),
                    now=datetime(2026, 9, 2, tzinfo=UTC),
                )
            sent = []

            async def invoke(wire, *, output_model, **kwargs):
                await pause()
                payload = _restore_view(wire)
                sent.append(payload["stage"])
                body = (
                    _assessment_payload(payload["previous_claims"])
                    if payload["stage"] == "synthesis"
                    else _staged_batch(payload["stage"], payload["requested_roles"])
                )
                output_model.model_validate(body)
                return json.dumps(body), UsageSnapshot(input_tokens=300, output_tokens=80)

            monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
            assert await exact_original_worker(settings) is True
            assert sent == ["research", "critique", "synthesis"]
            async with factory() as session:
                row = (
                    await session.execute(
                        text(
                            "SELECT status,input_tokens,output_tokens,retry_count "
                            "FROM agent_research_runs WHERE id=:run"
                        ),
                        {"run": admission.run_id},
                    )
                ).one()
                assert tuple(row) == ("completed", 900, 240, 0)
                assert (
                    await session.scalar(
                        text(
                            "SELECT count(*) FROM agent_research_outputs "
                            "WHERE run_id=:run AND role IS NOT NULL"
                        ),
                        {"run": admission.run_id},
                    )
                    == 8
                )
            assert await exact_original_worker(settings) is False

        coroutine = sdk_case()
    task = asyncio.create_task(coroutine)
    waiting = asyncio.create_task(entered.wait())
    try:
        async with asyncio.timeout(30):
            done, _ = await asyncio.wait((task, waiting), return_when=asyncio.FIRST_COMPLETED)
            if task in done:
                await task
                pytest.fail("original native path did not reach provider")
            assert facts["day_locks"] and all(row.granted for row in facts["day_locks"])
            assert facts["global"] == []

            async def allocate():
                async with factory() as session, session.begin():
                    from datetime import UTC, datetime

                    day = facts["day"] + timedelta(days=0 if same_day else 1)
                    return await admit_research_run(
                        session,
                        actor_id="legacy-budget-contender",
                        route="/internal/v1/research-runs",
                        idempotency_key="one-new-native-admission",
                        request_hash="c" * 64,
                        request=ResearchRequest(
                            instrument_id=case._INSTRUMENT_ID,
                            data_snapshot_id=case._SNAPSHOT_ID,
                            decision_time=case._NOW,
                        ),
                        prompt_version_id=None,
                        model_config_id=facts["model"],
                        daily_limit_usd=Decimal("3"),
                        now=datetime.combine(day, case._NOW.time(), tzinfo=UTC),
                    )

            if same_day:
                from nexus.agents.budget import ResearchAccountingBusy

                with pytest.raises(ResearchAccountingBusy, match="RESEARCH_ACCOUNTING_BUSY"):
                    await allocate()
            else:
                fresh = await allocate()  # Its whole transaction commits BEFORE provider release.
                assert fresh.run_id != facts["run"]
            async with factory() as session:
                assert not await accounting_locks(session, "agent-budget:accounting")
                assert await session.scalar(text("SELECT count(*) FROM jobs")) == (
                    1 if same_day else 2
                )
            release.set()
            await (
                task
            )  # Preserve all original output, usage, and same-identity no-reclaim assertions.
            async with factory() as session:
                row = (
                    await session.execute(
                        text(
                            "SELECT state,business_date FROM agent_daily_budget_reservations "
                            "WHERE run_id=:run"
                        ),
                        {"run": facts["run"]},
                    )
                ).one()
                assert (row.state, row.business_date) == ("settled", facts["day"])
                assert not await accounting_locks(session, "agent-budget:accounting")
    finally:
        release.set()
        waiting.cancel()
        await asyncio.gather(task, waiting, return_exceptions=True)
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_native_research_api_busy_rolls_back_all_admission_rows(
    migrated_database_url, monkeypatch
):
    import json

    import httpx
    from helpers.portfolio_api import app, headers
    from integration import test_agent_execution_pg as case

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await case._seed_authority(factory, public_snapshot=True)
    monkeypatch.setattr("nexus.api.routes.research.utc_now", lambda: case._NOW)
    path = "/internal/v1/research-runs"
    body = json.dumps(
        {
            "instrument_id": str(case._INSTRUMENT_ID),
            "data_snapshot_id": str(case._SNAPSHOT_ID),
            "decision_time": "2026-08-24T12:00:00Z",
        }
    ).encode()
    async with factory() as holder, holder.begin():
        await holder.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:key))"),
            {"key": f"agent-budget:{case._NOW.date()}"},
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(
                app=app(migrated_database_url), raise_app_exceptions=False
            ),
            base_url="http://testserver",
        ) as client:
            response = await client.post(
                path,
                content=body,
                headers={**headers("POST", path, body), "Idempotency-Key": "busy-one-attempt"},
            )
        assert response.status_code == 409
        assert response.json()["error"]["code"] == "RESEARCH_ACCOUNTING_BUSY"
        async with factory() as probe:
            assert not await accounting_locks(probe, "agent-budget:accounting")
            for table in ("agent_research_runs", "agent_daily_budget_reservations", "jobs"):
                assert await probe.scalar(text(f"SELECT count(*) FROM {table}")) == 0
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["verification", "close"])
async def test_stage_accounting_busy_exits_owning_transaction_without_false_terminal(
    migrated_database_url, phase
):
    from nexus.agents.budget import ResearchAccountingBusy

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    result, manifest, old, claim, now, protocol, ticket = await observed(factory, tokens=(10, 5))
    if phase == "close":
        assert await verify(protocol, ticket, now, tokens=(10, 5))
        await protocol.cleanup(ticket, now=now, confirmed=True)
    async with factory() as holder, holder.begin():
        await holder.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:key))"),
            {"key": f"agent-budget:{now.date()}"},
        )
        with pytest.raises(ResearchAccountingBusy, match="RESEARCH_ACCOUNTING_BUSY"):
            if phase in {"verification", "verification_released"}:
                await verify(protocol, ticket, now, tokens=(10, 5))
            else:
                await protocol.close(claim, now=now)
        async with factory() as probe:
            assert not await accounting_locks(probe, "agent-budget:accounting")
            assert await probe.scalar(text("SELECT count(*) FROM research_stage_observation")) == 1
            assert (
                await probe.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 1
            )
            assert await probe.scalar(text("SELECT count(*) FROM research_stage_verification")) == (
                0 if phase in {"verification", "verification_released"} else 1
            )
            assert await probe.scalar(text("SELECT count(*) FROM research_stage_terminal")) == 0
            assert await probe.scalar(text("SELECT count(*) FROM agent_research_outputs")) == 0
            assert (
                await probe.scalar(
                    text(
                        "SELECT count(*) FROM agent_daily_budget_reservations "
                        "WHERE state<>'reserved' OR settled_usd IS NOT NULL"
                    )
                )
                == 0
            )
            assert (
                await probe.scalar(
                    text("SELECT status FROM jobs WHERE id=:job"), {"job": claim.job_id}
                )
                == "running"
            )
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_worker_accounting_busy_keeps_claim_and_never_schedules_retry(migrated_database_url):
    from integration import test_job_worker as jobs
    from nexus.agents.budget import ResearchAccountingBusy
    from nexus.agents.persistence import _lock_budget_day
    from nexus.jobs.models import JobType, QueueName
    from nexus.jobs.registry import JobRegistry
    from nexus.jobs.worker import run_job_worker_once

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    job_id = await jobs._enqueue(
        factory, job_type=JobType.QUALITY_FREEZE, payload={"value": "accounting-boundary"}
    )
    original_errors = []

    async def handler(context):
        try:
            await _lock_budget_day(context.session, jobs._NOW.date().isoformat())
        except ResearchAccountingBusy as error:
            original_errors.append(error)
            raise
        pytest.fail("a conflicting day cannot enter the handler's external work")

    async with factory() as holder, holder.begin():
        await holder.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:key))"),
            {"key": f"agent-budget:{jobs._NOW.date()}"},
        )
        with pytest.raises(ResearchAccountingBusy) as caught:
            await run_job_worker_once(
                factory,
                registry=JobRegistry(
                    (jobs._spec(handler, job_type=JobType.QUALITY_FREEZE, timeout_seconds=30),)
                ),
                queue=QueueName.GENERAL,
                worker_id="accounting-busy-worker",
                worker_started_at=jobs._NOW,
                process_version="fixture",
                migration_head="0105_comparison_preclaim",
                clock=lambda: jobs._NOW,
                job_ids=(job_id,),
            )
        assert caught.value is original_errors[0]
        async with factory() as probe:
            assert not await accounting_locks(probe, "agent-budget:accounting")
            row = (
                await probe.execute(
                    text(
                        "SELECT j.status,j.attempt_count,a.outcome,a.finished_at "
                        "FROM jobs j JOIN job_attempts a ON a.job_id=j.id WHERE j.id=:id"
                    ),
                    {"id": job_id},
                )
            ).one()
            assert tuple(row) == ("running", 1, "running", None)
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["sdk", "responses"])
@pytest.mark.parametrize(
    "phase", ["authorization", "verification", "verification_released", "finalization"]
)
async def test_native_transport_busy_cleans_once_and_escapes_full_finalization(
    migrated_database_url, monkeypatch, tmp_path, transport, phase
):
    """A real observation/verification race must not fall into legacy finalization wait."""
    from integration import test_comparison_execution_pg as case
    from nexus.agents.budget import ResearchAccountingBusy
    from nexus.agents.research_stages import executor
    from nexus.agents.research_stages.protocol import StagedProtocol
    from nexus.ops import research_comparison

    class Observed(BaseException):
        pass

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    holder = factory()
    entered = asyncio.Event()
    state = {}
    sdk_calls, bodies = [], []
    original_sdk = case.controlled_sdk
    original_body = case.Body
    original_seed = case.seed
    original_observe = StagedProtocol.observe
    original_route = executor.selected_route
    original_member = StagedProtocol._member
    original_verify = StagedProtocol.verify
    original_budget = execution.require_reserved_daily_budget
    original_install = StagedProtocol.install
    original_authorize = StagedProtocol.authorize
    native = research_comparison.native_command

    async def budget(session, **kwargs):
        if phase == "authorization" and kwargs["run_id"] == state.get("run"):
            assert executor._CAPTURE.get() and kwargs.get("accounting") is True
            state["prepared_budget"] = True
            try:
                return await original_budget(session, **kwargs)
            except ResearchAccountingBusy as error:
                state["preparation_busy"] = error
                raise
        return await original_budget(session, **kwargs)

    async def install(protocol, *args, **kwargs):
        if phase == "authorization" and entered.is_set():
            state["installed"] = True
        return await original_install(protocol, *args, **kwargs)

    async def authorize(protocol, *args, **kwargs):
        if phase == "authorization" and entered.is_set():
            state["authorized"] = True
        return await original_authorize(protocol, *args, **kwargs)

    def controlled(patch):
        result = original_sdk(patch)
        sdk_calls.append(result)
        return result

    class Body(original_body):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            bodies.append(self)

    async def seed(*args, **kwargs):
        result = await original_seed(*args, **kwargs)
        state["old_budget"] = result[-1]
        return result

    async def hold_day(run_id):
        async with factory() as probe:
            ordinal = await probe.scalar(
                text("SELECT ordinal FROM research_comparison_chain WHERE run_id=:run"),
                {"run": run_id},
            )
            day = await probe.scalar(
                text("SELECT business_date FROM agent_daily_budget_reservations WHERE run_id=:run"),
                {"run": run_id},
            )
        if not entered.is_set() and ordinal == (1 if transport == "sdk" else 2):
            await holder.begin()
            state["holder_pid"] = await holder.scalar(text("SELECT pg_backend_pid()"))
            await holder.execute(
                text("SELECT pg_advisory_xact_lock(hashtext(:key))"), {"key": f"agent-budget:{day}"}
            )
            state["run"] = run_id
            state["day"] = day
            entered.set()

    async def observe(protocol, ticket, **kwargs):
        await original_observe(protocol, ticket, **kwargs)
        if phase in {"verification", "verification_released"}:
            await hold_day(ticket.run_id)

    async def verify(protocol, *args, **kwargs):
        try:
            return await original_verify(protocol, *args, **kwargs)
        except ResearchAccountingBusy as error:
            if phase == "verification_released":
                state["busy"] = error
                await holder.rollback()
            raise

    async def route(session, job_id):
        result = await original_route(session, job_id)
        if phase == "authorization" and result is not None:
            await hold_day(result["run_id"])
        return result

    async def member(protocol, session, claim):
        result = await original_member(protocol, session, claim)
        if phase == "finalization":
            count = await session.scalar(
                text("SELECT count(*) FROM research_stage_cleanup WHERE run_id=:run AND confirmed"),
                {"run": result["run_id"]},
            )
            if count == 3:
                await hold_day(result["run_id"])
        return result

    async def inspect(args):
        if args.position != (1 if transport == "sdk" else 2):
            return await native(args)
        task = asyncio.create_task(native(args))
        waiting = asyncio.create_task(entered.wait())
        blocked = False
        try:
            async with asyncio.timeout(30):
                done, _ = await asyncio.wait((task, waiting), return_when=asyncio.FIRST_COMPLETED)
                if task in done and not entered.is_set():
                    return await task  # Original expiration/identity preflight negative cases.
                while not task.done():
                    async with factory() as probe:
                        blocked = bool(
                            await probe.scalar(
                                text(
                                    "SELECT EXISTS(SELECT 1 FROM pg_stat_activity "
                                    "WHERE :holder = ANY(pg_blocking_pids(pid)))"
                                ),
                                {"holder": state["holder_pid"]},
                            )
                        )
                    if blocked:
                        break
                    await asyncio.sleep(0.02)
                if blocked:
                    await holder.rollback()
                    await asyncio.gather(task, return_exceptions=True)
                    pytest.fail(f"short {phase} path waited for the contended day")
                with pytest.raises(ResearchAccountingBusy) as caught:
                    await task
                if phase == "authorization":
                    # The route-time day conflict now fails in native prepare,
                    # before stage installation or protocol authorization.
                    assert state["prepared_budget"] is True
                    assert caught.value is state["preparation_busy"]
                    assert "installed" not in state and "authorized" not in state
                if phase == "verification_released":
                    assert caught.value is state["busy"]
                async with factory() as probe:
                    assert not await accounting_locks(probe, "agent-budget:accounting")
                    if phase == "authorization":
                        assert holder.in_transaction()
                        assert await accounting_locks(probe, f"agent-budget:{state['day']}") == [
                            (state["holder_pid"], True)
                        ]
                        assert (
                            await probe.scalar(
                                text(
                                    "SELECT count(*) FROM research_stage_member WHERE run_id=:run"
                                ),
                                {"run": state["run"]},
                            )
                            == 0
                        )
                        claim = (
                            await probe.execute(
                                text(
                                    "SELECT j.status,j.attempt_count FROM jobs j "
                                    "JOIN research_comparison_chain c ON c.job_id=j.id "
                                    "WHERE c.run_id=:run"
                                ),
                                {"run": state["run"]},
                            )
                        ).one()
                        assert tuple(claim) == ("running", 1)
                    for table, expected in (
                        (
                            "research_stage_authorization",
                            (
                                3
                                if phase == "finalization"
                                else int(phase in {"verification", "verification_released"})
                            ),
                        ),
                        (
                            "research_stage_observation",
                            (
                                3
                                if phase == "finalization"
                                else int(phase in {"verification", "verification_released"})
                            ),
                        ),
                        ("research_stage_verification", 3 if phase == "finalization" else 0),
                        ("research_stage_terminal", 0),
                        ("agent_research_outputs", 0),
                    ):
                        assert (
                            await probe.scalar(
                                text(f"SELECT count(*) FROM {table} WHERE run_id=:run"),
                                {"run": state["run"]},
                            )
                            == expected
                        )
                    assert await probe.scalar(
                        text(
                            "SELECT bool_and(confirmed) FROM research_stage_cleanup "
                            "WHERE run_id=:run"
                        ),
                        {"run": state["run"]},
                    ) is (None if phase == "authorization" else True)
                    assert (
                        await probe.scalar(
                            text(
                                "SELECT state FROM agent_daily_budget_reservations "
                                "WHERE run_id=:run"
                            ),
                            {"run": state["run"]},
                        )
                        == "reserved"
                    )
                    assert (
                        await probe.scalar(
                            text("SELECT state FROM agent_daily_budget_reservations WHERE id=:id"),
                            {"id": state["old_budget"]},
                        )
                        == "reserved"
                    )
                assert len(sdk_calls[0]) == (
                    (
                        3
                        if phase == "finalization"
                        else int(phase in {"verification", "verification_released"})
                    )
                    if transport == "sdk"
                    else 3
                )
                assert all(close.await_count == 1 for close, _ in sdk_calls[0])
                assert len(bodies) == (
                    0
                    if transport == "sdk"
                    else (
                        3
                        if phase == "finalization"
                        else int(phase in {"verification", "verification_released"})
                    )
                )
                assert all(body.closed == 1 for body in bodies)
                raise Observed
        finally:
            waiting.cancel()
            await holder.close()
            await asyncio.gather(task, waiting, return_exceptions=True)

    monkeypatch.setattr(case, "controlled_sdk", controlled)
    monkeypatch.setattr(case, "Body", Body)
    monkeypatch.setattr(case, "seed", seed)
    monkeypatch.setattr(StagedProtocol, "observe", observe)
    monkeypatch.setattr(executor, "selected_route", route)
    monkeypatch.setattr(StagedProtocol, "_member", member)
    monkeypatch.setattr(StagedProtocol, "verify", verify)
    monkeypatch.setattr(execution, "require_reserved_daily_budget", budget)
    monkeypatch.setattr(StagedProtocol, "install", install)
    monkeypatch.setattr(StagedProtocol, "authorize", authorize)
    monkeypatch.setattr(research_comparison, "native_command", inspect)
    try:
        with pytest.raises(Observed):
            await case.test_normal_sdk_and_http_chain_use_own_predecessors_and_common_persistence(
                migrated_database_url, monkeypatch, False, tmp_path
            )
    finally:
        await holder.close()
        await factory.kw["bind"].dispose()
