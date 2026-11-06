"""The normal independent entrance must retain exact accepted historical facts."""

import pytest
from integration.test_unknown_preparation_ack_pg import (
    test_explicit_unknown_batch_is_atomic_and_does_not_replay as exercise_history,
)
from nexus.paper.manual_entry import start_configured_manual_request
from nexus.paper.manual_request import read_manual_request
from nexus.persistence.models.manual_paper import ManualPaperRequest
from sqlalchemy import text


async def independent_settings(settings, clock, monkeypatch):
    from datetime import timedelta
    from decimal import Decimal

    from nexus.ops.openai_local import prepare_database
    from nexus.paper import independent_history_scope as scope
    from nexus.settings import AgentRuntimeMode
    from unit.ops.test_formal_stage_registration import OPTIONS

    monkeypatch.setattr(scope, "POLICY_AT", clock[0] - timedelta(days=1))
    await prepare_database(
        settings.model_copy(update={"agent_runtime_mode": AgentRuntimeMode.CODEX_SDK}),
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
    return settings.model_copy(update={"agent_daily_budget_usd": Decimal("30")})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "budget",
        "window",
        "future_policy",
        "late_claim",
        "new_unknown",
        "economic_first",
        "independent_first",
        "claim_busy",
        "old_root",
        "late_after_queue",
        "reader_error",
    ],
)
async def test_independent_history_refusals_preserve_original_protection(
    migrated_database_url,
    monkeypatch,
    fault,
):
    from datetime import timedelta
    from decimal import Decimal
    from types import SimpleNamespace

    from integration.test_manual_paper_request_pg import AT
    from nexus.agents.budget import ResearchAccountingBusy
    from nexus.paper import independent_history_scope as scope
    from nexus.paper import preparation_unknown_ack as old_scope
    from nexus.paper.formal_budget_protection import apply_protection
    from nexus.paper.manual_request import (
        admit_manual_request,
        claim_manual_request,
        record_manual_request_event,
    )
    from nexus.persistence.models.runtime import IdempotencyRecord
    from sqlalchemy import select, update

    async def check(factory, settings, old, clock, original_scalar):
        settings = await independent_settings(settings, clock, monkeypatch)
        async with factory() as db:
            protected = await db.scalar(
                text(
                    "SELECT jsonb_agg(to_jsonb(b) ORDER BY id) "
                    "FROM agent_daily_budget_reservations b"
                )
            )

        async def economic(db):
            return await apply_protection(
                db,
                account_id=settings.agent_paper_account_id,
                batch_id=settings.agent_paper_candidate_batch_id,
                enabled=True,
                daily_limit_usd=Decimal("30"),
                configuration_hash="d" * 64,
                idempotency_key="economic-new",
                now=clock[0],
                formal_model_config_id=settings.agent_paper_formal_model_config_id,
            )

        expected = {
            "budget": scope.BUDGET,
            "window": scope.WINDOW,
            "future_policy": scope.SCOPE,
            "late_claim": scope.PROOF,
            "new_unknown": "MANUAL_ACCOUNT_BUSY_OR_UNKNOWN",
            "economic_first": scope.ECONOMIC,
        }.get(fault)
        if fault == "budget":
            settings = settings.model_copy(update={"agent_daily_budget_usd": Decimal("13")})
        if fault == "window":
            clock[0] = clock[0].replace(hour=13, minute=29)
        if fault == "future_policy":
            monkeypatch.setattr(scope, "POLICY_AT", clock[0] + timedelta(seconds=1))
        if fault == "late_claim":
            async with factory.begin() as db:
                audit = (await old_scope._audits(db, settings.agent_paper_account_id))[0]
                await db.execute(
                    update(IdempotencyRecord)
                    .where(IdempotencyRecord.id == audit.payload["claim_ids"][0])
                    .values(response_payload={"late": True})
                )
        if fault == "new_unknown":
            async with factory.begin() as db:
                other = await admit_manual_request(
                    db,
                    account_id=settings.agent_paper_account_id,
                    batch_id=settings.agent_paper_candidate_batch_id,
                    model_config_id=settings.agent_paper_formal_model_config_id,
                    mode="research_and_paper",
                    runtime_mode="responses",
                    idempotency_key="another-unknown",
                )
                await claim_manual_request(db, request_id=other.request_id)
                await record_manual_request_event(
                    db,
                    await db.get(ManualPaperRequest, other.request_id),
                    status="unknown",
                    code="MANUAL_RESULT_UNKNOWN",
                )
        if fault == "economic_first":
            async with factory.begin() as db:
                await economic(db)
        if expected:
            with pytest.raises(ValueError, match=f"^{expected}$"):
                async with factory.begin() as db:
                    await start_configured_manual_request(
                        db, settings, mode="research_and_monitor", idempotency_key="must-not-create"
                    )
            async with factory() as db:
                assert (
                    await db.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 0
                )
                assert (
                    await db.scalar(
                        text(
                            "SELECT jsonb_agg(to_jsonb(b) ORDER BY id) "
                            "FROM agent_daily_budget_reservations b"
                        )
                    )
                    == protected
                )
            return
        if fault == "reader_error":
            from sqlalchemy.ext.asyncio import AsyncSession

            original_execute = AsyncSession.execute
            sentinel = ValueError("synthetic_read_failure")

            async def fail_read(db, statement, *args, **kwargs):
                if getattr(statement, "_for_update_arg", None) is not None:
                    raise sentinel
                return await original_execute(db, statement, *args, **kwargs)

            monkeypatch.setattr(AsyncSession, "execute", fail_read)
            with pytest.raises(ValueError) as caught:
                async with factory.begin() as db:
                    await start_configured_manual_request(
                        db, settings, mode="research_and_monitor", idempotency_key="failed-read"
                    )
            assert caught.value is sentinel
            async with factory() as db:
                assert (
                    await db.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 0
                )
            return
        if fault == "claim_busy":
            async with factory.begin() as writer:
                audit = (await old_scope._audits(writer, settings.agent_paper_account_id))[0]
                await writer.execute(
                    select(IdempotencyRecord.id)
                    .where(IdempotencyRecord.id == audit.payload["claim_ids"][0])
                    .with_for_update()
                )
                with pytest.raises(ResearchAccountingBusy):
                    async with factory.begin() as db:
                        await start_configured_manual_request(
                            db,
                            settings,
                            mode="research_and_monitor",
                            idempotency_key="busy-no-root",
                        )
            return
        async with factory.begin() as db:
            receipt = await start_configured_manual_request(
                db, settings, mode="research_and_monitor", idempotency_key="future-root"
            )
        if fault == "late_after_queue":
            from nexus.ops.manual_paper_host import claim_configured_manual_request
            from nexus.persistence.models.runtime import AuditEvent

            async with factory.begin() as db:
                audit = (await old_scope._audits(db, settings.agent_paper_account_id))[0]
                await db.execute(
                    update(IdempotencyRecord)
                    .where(IdempotencyRecord.id == audit.payload["claim_ids"][0])
                    .values(response_payload={"late": True})
                )
            async with factory.begin() as db:
                view, claimed = await claim_configured_manual_request(
                    db, settings, request_id=receipt.request_id
                )
                assert not claimed and view.status == "unavailable"
                audits = (
                    await db.scalars(
                        select(AuditEvent).where(AuditEvent.action == "research.request_failure")
                    )
                ).all()
                assert audits[-1].payload["failure_code"] == scope.PROOF
            return
        if fault == "independent_first":
            with pytest.raises(ValueError, match="^FORMAL_BUDGET_SLOT_ALREADY_CLAIMED$"):
                async with factory.begin() as db:
                    await economic(db)
        if fault == "old_root":
            async with factory.begin() as db:
                row = await db.get(ManualPaperRequest, receipt.request_id)
                old_shape = SimpleNamespace(
                    **{column.name: getattr(row, column.name) for column in row.__table__.columns}
                )
                old_shape.research_profile = dict(row.research_profile)
                old_shape.research_profile.pop("historical_exception")
                with pytest.raises(ValueError, match=f"^{scope.SCOPE}$"):
                    await scope.recheck(db, old_shape, now=clock[0])

    await exercise_history(
        migrated_database_url,
        monkeypatch,
        "sol-successor",
        after_successor_setup=check,
        successor_at=(AT + timedelta(days=3)).replace(hour=9, minute=0),
    )


@pytest.mark.asyncio
async def test_normal_independent_queue_preserves_accepted_unknown_and_deduplicates(
    migrated_database_url, monkeypatch
):
    async def check(factory, settings, old, clock, original_scalar):
        from datetime import timedelta
        from decimal import Decimal

        from nexus.paper import independent_history_scope as scope

        settings = settings.model_copy(update={"agent_daily_budget_usd": Decimal("30")})
        monkeypatch.setattr(scope, "POLICY_AT", clock[0] - timedelta(days=1))
        from nexus.ops.openai_local import prepare_database
        from nexus.settings import AgentRuntimeMode
        from unit.ops.test_formal_stage_registration import OPTIONS

        await prepare_database(
            settings.model_copy(update={"agent_runtime_mode": AgentRuntimeMode.CODEX_SDK}),
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
        from nexus.paper.manual_entry import check_configured_independent_admission

        async with factory.begin() as db:
            await db.execute(text("SET TRANSACTION READ ONLY"))
            checked = await check_configured_independent_admission(db, settings)
            assert checked.status == "independent_request_queueable"
            assert checked.budget_reserved is False and checked.external_requests == 0
        async with factory.begin() as db:
            before = await db.scalar(
                text("SELECT jsonb_agg(to_jsonb(a) ORDER BY id) FROM audit_events a")
            )
            receipt = await start_configured_manual_request(
                db, settings, mode="research_and_monitor", idempotency_key="new-independent"
            )
            assert receipt.status == "queued" and receipt.cycle_id is None
            assert receipt.request_id != old.request_id
            same = await start_configured_manual_request(
                db, settings, mode="research_and_monitor", idempotency_key="new-independent"
            )
            assert same == receipt
            row = await db.get(ManualPaperRequest, receipt.request_id)
            assert row.research_profile["historical_exception"]["version"] == (
                "independent_accepted_history_v1"
            )
            assert (
                await read_manual_request(db, await db.get(ManualPaperRequest, old.request_id))
            ).status == "unknown"
            assert (
                await db.scalar(
                    text("SELECT jsonb_agg(to_jsonb(a) ORDER BY id) FROM audit_events a")
                )
                == before
            )

    from datetime import timedelta

    from integration.test_manual_paper_request_pg import AT

    await exercise_history(
        migrated_database_url,
        monkeypatch,
        "sol-successor",
        after_successor_setup=check,
        successor_at=(AT + timedelta(days=3)).replace(hour=9, minute=0),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("late_stage", [None, 2, 3, "cleanup"])
async def test_accepted_history_three_stock_preparation_worker_and_close(
    migrated_database_url,
    monkeypatch,
    tmp_path,
    late_stage,
):
    from datetime import timedelta
    from decimal import Decimal
    from types import SimpleNamespace

    from helpers.single_sol_clock import preopen_fixture_clock, preopen_target
    from integration import test_unknown_preparation_ack_pg as history_fixture
    from integration.test_research_request_pg import prepare_scope
    from integration.test_sol_native_stage_pg import install_sol_http
    from nexus import worker
    from nexus.ops.openai_local import prepare_database
    from nexus.paper import independent_history_scope as scope
    from nexus.paper.manual_request import admit_manual_request, claim_manual_request
    from nexus.paper.research_request import finish_research_request, prepare_research_request
    from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord
    from nexus.persistence.session import session_factory
    from nexus.settings import AgentRuntimeMode, EngineSettings
    from pydantic import SecretStr
    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import AsyncSession
    from unit.ops.test_formal_stage_registration import OPTIONS

    clock_factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with clock_factory() as db:
        target = preopen_target(
            await db.scalar(text("SELECT pg_catalog.clock_timestamp()"))
        ).replace(hour=9)
    import os

    images = bool(os.environ.get("NEXUS_TEST_HISTORY_FORMAL_IMAGE"))
    if images:
        async with clock_factory() as db:
            target = await db.scalar(text("SELECT pg_catalog.clock_timestamp()")) - timedelta(
                minutes=2
            )
    await clock_factory.kw["bind"].dispose()
    monkeypatch.setattr(
        history_fixture, "AT", (target - timedelta(days=7 if images else 3)).replace(hour=14)
    )

    async def check(factory, settings, old, clock, original_scalar):
        monkeypatch.setattr(AsyncSession, "scalar", original_scalar)
        await preopen_fixture_clock(migrated_database_url, monkeypatch, target=clock[0])
        await factory.kw["bind"].dispose()
        monkeypatch.setattr(scope, "POLICY_AT", clock[0] - timedelta(days=1))
        prepared_factory, prepared_settings, store, ids = await prepare_scope(
            migrated_database_url,
            tmp_path,
            bounded=True,
            account=SimpleNamespace(account_id=settings.agent_paper_account_id),
        )
        await prepare_database(
            prepared_settings,
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
        live = settings.model_copy(
            update={
                "agent_paper_candidate_batch_id": prepared_settings.agent_paper_candidate_batch_id,
                "agent_runtime_mode": AgentRuntimeMode.RESPONSES,
                "agent_daily_budget_usd": Decimal("30"),
                "openai_api_key": SecretStr("sk-proj-" + "synthetic-only" * 5),
            }
        )
        try:
            async with factory.begin() as db:
                db.info["sec_exhibit_store"] = store
                protected_holds = await db.scalar(
                    text(
                        "SELECT jsonb_object_agg(id,to_jsonb(b)) "
                        "FROM agent_daily_budget_reservations b"
                    )
                )
                receipt = await admit_manual_request(
                    db,
                    account_id=live.agent_paper_account_id,
                    batch_id=live.agent_paper_candidate_batch_id,
                    model_config_id=live.agent_paper_formal_model_config_id,
                    mode="research_and_monitor",
                    contract_version=3,
                    runtime_mode="responses",
                    daily_limit_usd=live.agent_daily_budget_usd,
                    idempotency_key="full-new-root",
                )
                await claim_manual_request(db, request_id=receipt.request_id)
            async with factory.begin() as db:
                db.info["sec_exhibit_store"] = store
                prepared = await prepare_research_request(db, live, request_id=receipt.request_id)
                assert len(prepared.research.members) == 3
                assert {m.instrument_id for m in prepared.research.members} == set(ids)

            async def change_history(payload):
                previous = {2: "research", 3: "critique"}.get(late_stage)
                if payload["stage"] == previous:
                    from nexus.paper import preparation_unknown_ack
                    from nexus.persistence.models.runtime import IdempotencyRecord
                    from sqlalchemy import update

                    async with factory.begin() as db:
                        audit = (
                            await preparation_unknown_ack._audits(db, live.agent_paper_account_id)
                        )[0]
                        await db.execute(
                            update(IdempotencyRecord)
                            .where(IdempotencyRecord.id == audit.payload["claim_ids"][0])
                            .values(response_payload={"late": True})
                        )

            calls, clients = install_sol_http(monkeypatch, store, on_request=change_history)
            if late_stage == "cleanup":
                from nexus.agents import platform_native

                original_client = platform_native.native_http_client

                def unconfirmed_cleanup():
                    client = original_client()
                    close = client.aclose

                    async def failed_close():
                        await close()
                        raise RuntimeError("synthetic cleanup confirmation missing")

                    client.aclose = failed_close
                    return client

                monkeypatch.setattr(platform_native, "native_http_client", unconfirmed_cleanup)
                first, second = prepared.research.members[:2]
                assert await worker.run_agent_worker_once(
                    live, job_ids=(first.job_id,), formal_stage_capacity=True
                )
                before = list(calls)
                from nexus.paper.research_failure import CapacityPreclaimRejected

                with pytest.raises(
                    CapacityPreclaimRejected, match="^PLATFORM_NATIVE_POOL_NOT_READY$"
                ):
                    await worker.run_agent_worker_once(
                        live, job_ids=(second.job_id,), formal_stage_capacity=True
                    )
                assert calls == before == ["research"]
                async with factory() as db:
                    hold = await db.scalar(
                        select(AgentDailyBudgetReservationRecord).where(
                            AgentDailyBudgetReservationRecord.run_id == first.run_id
                        )
                    )
                    assert hold.state == "settled" and hold.settled_usd > 0
                    assert (
                        await db.scalar(
                            text(
                                "SELECT count(*) FROM research_stage_cleanup "
                                "WHERE run_id=:run AND confirmed"
                            ),
                            {"run": first.run_id},
                        )
                        == 0
                    )
                    assert (
                        await db.scalar(
                            text(
                                "SELECT count(*) FROM research_stage_authorization "
                                "WHERE run_id=:run"
                            ),
                            {"run": second.run_id},
                        )
                        == 0
                    )
                return
            if late_stage is not None:
                member = prepared.research.members[0]
                assert await worker.run_agent_worker_once(
                    live, job_ids=(member.job_id,), formal_stage_capacity=True
                )
                assert calls == ["research", "critique"][: late_stage - 1]
                assert all(client.is_closed for client in clients)
                async with factory() as db:
                    from nexus.persistence.models.runtime import AuditEvent

                    failures = (
                        await db.scalars(
                            select(AuditEvent).where(
                                AuditEvent.action == "research.request_failure"
                            )
                        )
                    ).all()
                    hold = await db.scalar(
                        select(AgentDailyBudgetReservationRecord).where(
                            AgentDailyBudgetReservationRecord.run_id == member.run_id
                        )
                    )
                    assert hold.state == "settled" and hold.settled_usd > 0
                    remaining = (
                        await db.scalars(
                            select(AgentDailyBudgetReservationRecord).where(
                                AgentDailyBudgetReservationRecord.run_id.in_(
                                    [m.run_id for m in prepared.research.members[1:]]
                                )
                            )
                        )
                    ).all()
                    assert len(remaining) == 2 and all(h.state == "reserved" for h in remaining)
                    for identity, frozen in protected_holds.items():
                        assert (
                            await db.scalar(
                                text(
                                    "SELECT to_jsonb(b) FROM agent_daily_budget_reservations b "
                                    "WHERE id=CAST(:id AS uuid)"
                                ),
                                {"id": identity},
                            )
                            == frozen
                        )
                    assert any(
                        a.payload["request_id"] == str(receipt.request_id)
                        and a.payload["failure_phase"] == f"research_stage_{late_stage}"
                        and a.payload["failure_code"] == scope.PROOF
                        for a in failures
                    )
                    assert (
                        await db.scalar(text("SELECT count(*) FROM research_stage_authorization"))
                        == late_stage - 1
                    )
                    assert (
                        await read_manual_request(
                            db, await db.get(ManualPaperRequest, old.request_id)
                        )
                    ).status == "unknown"
                return
            for member in prepared.research.members:
                assert await worker.run_agent_worker_once(
                    live, job_ids=(member.job_id,), formal_stage_capacity=True
                )
            assert calls == ["research", "critique", "synthesis"] * 3
            assert all(client.is_closed for client in clients)
            async with factory.begin() as db:
                row = await db.get(ManualPaperRequest, receipt.request_id)
                closed = await finish_research_request(db, row=row)
                assert closed.status == "completed" and closed.cycle_id is None
                holds = (
                    await db.scalars(
                        select(AgentDailyBudgetReservationRecord).where(
                            AgentDailyBudgetReservationRecord.run_id.in_(
                                [m.run_id for m in prepared.research.members]
                            )
                        )
                    )
                ).all()
                assert len(holds) == 3 and all(h.state == "settled" for h in holds)
                assert await db.scalar(text("SELECT count(*) FROM single_sol_budget_credits")) == 0
                assert (
                    await read_manual_request(db, await db.get(ManualPaperRequest, old.request_id))
                ).status == "unknown"
            if images:
                from helpers.independent_history_images import verify_images

                await verify_images(
                    factory, migrated_database_url, receipt.request_id, old.request_id
                )
        finally:
            await prepared_factory.kw["bind"].dispose()

    await exercise_history(
        migrated_database_url,
        monkeypatch,
        "sol-successor",
        after_successor_setup=check,
        successor_at=target,
        history_symbols=("HST1", "HST2", "HST3"),
        include_scheduled_history=True,
    )


@pytest.mark.asyncio
async def test_after_close_full_native_obligations_allow_independent_capacity(
    migrated_database_url, monkeypatch, tmp_path
):
    from datetime import timedelta
    from decimal import Decimal

    from integration.test_single_sol_budget_pg import (
        test_install_single_protection_after_exact_natural_close as exercise_natural,
    )
    from nexus.paper import research_request
    from nexus.paper.independent_history_scope import require_capacity
    from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
    from sqlalchemy import select

    async def check(factory, host, portfolio):
        async with factory.begin() as db:
            calendars = (
                await db.scalars(
                    select(ExchangeCalendarSessionRecord).order_by(
                        ExchangeCalendarSessionRecord.session
                    )
                )
            ).all()
            current = next(c for c in calendars if c.session == portfolio.decision_time.date())
            last = calendars[-1]
            day = last.session + timedelta(days=1)
            while day.weekday() >= 5:
                day += timedelta(days=1)
            shift = day - last.session
            db.add(
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=last.calendar_authority_id,
                    session=day,
                    market_open_at=last.market_open_at + shift,
                    market_close_at=last.market_close_at + shift,
                    decision_at=last.decision_at + shift,
                    created_at=last.created_at,
                )
            )
            await db.flush()
            now = current.market_close_at + timedelta(minutes=10)

            async def observed(_session):
                return now

            with monkeypatch.context() as patch:
                patch.setattr(research_request, "database_now", observed)
                await require_capacity(
                    db,
                    account_id=host.agent_paper_account_id,
                    batch_id=portfolio.candidate_batch_id,
                    model_id=portfolio.model_config_id,
                    daily_limit=Decimal("30"),
                    now=now,
                )
            assert await db.scalar(text("SELECT count(*) FROM single_sol_budget_scopes")) == 0

    await exercise_natural(migrated_database_url, monkeypatch, tmp_path, after_natural_close=check)


@pytest.mark.asyncio
async def test_compact_independent_reads_original_legacy_natural_obligations(
    migrated_database_url, monkeypatch, tmp_path
):
    from integration.test_compact_sol_binding_pg import compact_model
    from integration.test_single_sol_budget_pg import (
        LIMIT,
    )
    from integration.test_single_sol_budget_pg import (
        test_install_single_protection_after_exact_natural_close as exercise_natural,
    )
    from nexus.paper import research_request, single_sol_obligations
    from nexus.paper.independent_history_scope import UNAVAILABLE, require_capacity

    async def check(factory, host, portfolio, now):
        selected = await compact_model(factory, host)

        async def observed(_session):
            return now

        monkeypatch.setattr(research_request, "database_now", observed)
        args = dict(
            account_id=host.agent_paper_account_id,
            batch_id=portfolio.candidate_batch_id,
            model_id=selected,
            daily_limit=LIMIT,
            now=now,
        )
        async with factory.begin() as db:
            before = await db.scalar(
                text("SELECT jsonb_agg(to_jsonb(g)) FROM single_sol_budget_scopes g")
            )
            await require_capacity(db, **args)
            assert (
                await db.scalar(
                    text("SELECT jsonb_agg(to_jsonb(g)) FROM single_sol_budget_scopes g")
                )
                == before
            )
        sentinel = ValueError("SINGLE_SOL_ORIGINAL_SETTLEMENT_UNPROVEN")

        async def missing_proof(*_args, **_kwargs):
            raise sentinel

        with monkeypatch.context() as patch:
            patch.setattr(single_sol_obligations, "funded_obligations", missing_proof)
            with pytest.raises(ValueError, match=f"^{UNAVAILABLE}$") as caught:
                async with factory.begin() as db:
                    await require_capacity(db, **args)
            assert caught.value.__cause__ is sentinel

    await exercise_natural(migrated_database_url, monkeypatch, tmp_path, after_guard_closed=check)
