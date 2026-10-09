"""Protected demand is database authority, never a caller supplied credit."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from helpers.paper_batch import INSTRUMENTS
from nexus.agents.budget import BudgetExhausted
from nexus.agents.persistence import reserve_portfolio_decision_budget
from nexus.paper.formal_budget_protection import apply_protection, protected_demand
from nexus.persistence.models.paper import AgentPaperCycleRecord
from sqlalchemy import text
from test_portfolio_synthesis_persistence_pg import seed_synthesis


def controlled_clock(session, monkeypatch, now):
    """Only DB clock is controlled; migration, locks and facts use real PostgreSQL."""
    scalar = session.scalar

    async def read(statement, *args, **kwargs):
        if getattr(statement, "is_select", False) and any(
            getattr(column, "name", None) == "clock_timestamp"
            for column in statement.selected_columns
        ):
            return now
        return await scalar(statement, *args, **kwargs)

    monkeypatch.setattr(session, "scalar", read)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "context", "synthesis", "nonterminal_usage"])
async def test_normal_three_research_and_full_synthesis_convert_their_own_f_once(
    migrated_database_url,
    monkeypatch,
    damage,
):
    import integration.test_portfolio_synthesis_persistence_pg as seed_module
    from integration.test_portfolio_synthesis_worker_pg import _seed_normal_synthesis
    from nexus.agents.portfolio_synthesis_persistence import admit_portfolio_synthesis
    from nexus.paper.agent_cycle import reserve_schwab_agent_paper_trigger
    from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord
    from sqlalchemy import select

    original_start = seed_module._start_cycle
    applied = []

    async def protected_start(session, **kwargs):
        now = kwargs["authority"].decision_time
        controlled_clock(session, monkeypatch, now.replace(hour=13, minute=0))
        # Isolated historical fixtures supply pre-slot persisted authority.
        for table in (
            "agent_paper_candidate_batches",
            "agent_model_configs",
            "agent_prompt_versions",
            "universe_snapshots",
            "universe_members",
            "instruments",
            "instrument_aliases",
            "exchange_calendar_authorities",
            "exchange_calendar_sessions",
        ):
            await session.execute(text(f"ALTER TABLE {table} DISABLE TRIGGER USER"))
            await session.execute(
                text(f"UPDATE {table} SET created_at=:at WHERE created_at>:at"),
                {"at": now.replace(hour=13, minute=0)},
            )
            await session.execute(text(f"ALTER TABLE {table} ENABLE TRIGGER USER"))
        from hashlib import sha256

        from nexus.persistence.models.agents import AgentPromptVersionRecord

        content = "Synthetic frozen portfolio synthesis prompt."
        session.add(
            AgentPromptVersionRecord(
                role="portfolio_decision",
                version="portfolio-management-synthesis-v1",
                content=content,
                content_hash=sha256(content.encode()).hexdigest(),
                created_at=now.replace(hour=13),
            )
        )
        await session.flush()
        binding = await apply_protection(
            session,
            account_id=kwargs["authority"].account_id,
            batch_id=kwargs["candidate_batch_id"],
            enabled=True,
            daily_limit_usd=Decimal("0.0008"),
            configuration_hash="a" * 64,
            idempotency_key="full-normal",
            now=now.replace(hour=13, minute=0),
        )
        applied.append(binding.id)
        await reserve_schwab_agent_paper_trigger(
            session,
            account_id=kwargs["authority"].account_id,
            candidate_batch_id=kwargs["candidate_batch_id"],
            client_idempotency_key="protected-normal",
            now=now,
        )
        with pytest.raises(ValueError, match="FORMAL_BUDGET_SLOT_ALREADY_CLAIMED"):
            await apply_protection(
                session,
                account_id=kwargs["authority"].account_id,
                batch_id=kwargs["candidate_batch_id"],
                enabled=True,
                daily_limit_usd=Decimal("0.0008"),
                configuration_hash="b" * 64,
                idempotency_key="post-trigger-change",
                now=now.replace(hour=13),
            )
        # A later publication must not silently change an already pinned slot.
        from datetime import timedelta
        from uuid import uuid4

        from nexus.agents.codex_sdk import MODEL_ID
        from nexus.agents.codex_versions import DEADLINE_BUDGET_CONFIG_VERSION
        from nexus.persistence.models.agents import AgentModelConfigRecord

        session.add(
            AgentModelConfigRecord(
                id=uuid4(),
                model_id=MODEL_ID,
                version=DEADLINE_BUDGET_CONFIG_VERSION,
                config_hash="b" * 64,
                max_input_tokens=96000,
                max_output_tokens=16384,
                max_tool_calls=0,
                input_usd_per_million=Decimal(1),
                output_usd_per_million=Decimal(1),
                pricing_available_at=now.replace(hour=13, minute=30),
                pricing_expires_at=now + timedelta(days=1),
                created_at=now.replace(hour=13, minute=30),
            )
        )
        session.add(
            AgentPromptVersionRecord(
                role="orchestrator",
                version="pending-new-orchestrator",
                content="Pending new prompt.",
                content_hash=sha256(b"Pending new prompt.").hexdigest(),
                created_at=now.replace(hour=13, minute=30),
            )
        )
        await session.flush()
        kwargs["daily_limit_usd"] = Decimal("0.0008")
        result = await original_start(session, **kwargs)
        demand = await protected_demand(session, business_date=now.date(), now=now)
        assert demand.native_usd == Decimal("0.0006")
        assert demand.future_usd == Decimal("0.0002")
        return result

    monkeypatch.setattr(seed_module, "_start_cycle", protected_start)
    # The synthesis fixture normally inserts its prompt after research; this
    # scenario has already installed it before apply. Omit only that duplicate.
    from sqlalchemy.ext.asyncio import AsyncSession

    original_add = AsyncSession.add

    def add_once(session, instance, *args, **kwargs):
        from nexus.persistence.models.agents import AgentPromptVersionRecord

        if (
            applied
            and isinstance(instance, AgentPromptVersionRecord)
            and instance.role == "portfolio_decision"
        ):
            return
        return original_add(session, instance, *args, **kwargs)

    monkeypatch.setattr(AsyncSession, "add", add_once)
    engine, sessions, args = await _seed_normal_synthesis(
        migrated_database_url, monkeypatch, candidate_instruments=INSTRUMENTS
    )
    try:
        args["daily_limit_usd"] = Decimal("0.0008")
        async with sessions.begin() as session:
            admitted = await admit_portfolio_synthesis(session, **args)
            demand = await protected_demand(
                session, business_date=args["now"].date(), now=args["now"]
            )
            assert demand.native_usd == Decimal("0.0008")
            assert demand.future_usd == 0
            assert admitted.run.cycle_id == args["cycle_id"]
            assert len(tuple(await session.scalars(select(AgentDailyBudgetReservationRecord)))) == 3
            if damage is not None:
                if damage == "synthesis":
                    table = "agent_portfolio_synthesis_runs"
                    mutation = (
                        "UPDATE agent_portfolio_synthesis_runs "
                        "SET frozen_input=frozen_input-'prompt_hash'"
                    )
                elif damage == "context":
                    table = "agent_research_runs"
                    mutation = (
                        "UPDATE agent_research_runs SET evidence_manifest=("
                        "SELECT jsonb_agg(jsonb_set(item,'{location}',"
                        "'\"fixture-damaged\"'::jsonb)) "
                        "FROM jsonb_array_elements(evidence_manifest) AS item)"
                    )
                else:
                    table, mutation = None, None
                    from nexus.persistence.models.agents import (
                        AgentPortfolioSynthesisBudgetReservationRecord,
                    )

                    reservation = await session.scalar(
                        select(AgentPortfolioSynthesisBudgetReservationRecord)
                    )
                    reservation.state, reservation.settled_usd = "released", Decimal(0)
                    await session.flush()
                if table is not None:
                    await session.execute(text(f"ALTER TABLE {table} DISABLE TRIGGER USER"))
                    await session.execute(text(mutation))
                    await session.execute(text(f"ALTER TABLE {table} ENABLE TRIGGER USER"))
                    session.expire_all()
                with pytest.raises(ValueError):
                    await reserve_portfolio_decision_budget(
                        session,
                        run_id=args["cycle_id"],
                        now=args["now"],
                        daily_limit_usd=Decimal("0.0008"),
                        worst_case_usd=Decimal("0.0002"),
                        pricing_version="test",
                        pricing_fresh=True,
                    )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("expire_at", [None, 1, 3])
async def test_applied_three_candidate_day_protects_four_obligations_without_jobs(
    migrated_database_url,
    monkeypatch,
    expire_at,
    tmp_path,
):
    from sqlalchemy import func, select

    engine, sessions, seed = await seed_synthesis(
        migrated_database_url, candidate_instruments=INSTRUMENTS, complete_research=False
    )
    now = datetime(2026, 8, 27, 13, tzinfo=UTC)
    try:
        async with sessions.begin() as session:
            controlled_clock(session, monkeypatch, now)
            # Only this migrated temporary fixture: make pre-existing authorities
            # available before the synthetic budget day, without changing payloads.
            for table in (
                "exchange_calendar_authorities",
                "exchange_calendar_sessions",
                "agent_model_configs",
                "agent_prompt_versions",
            ):
                await session.execute(text(f"ALTER TABLE {table} DISABLE TRIGGER USER"))
                await session.execute(text(f"UPDATE {table} SET created_at='2026-08-26T20:00:00Z'"))
                await session.execute(text(f"ALTER TABLE {table} ENABLE TRIGGER USER"))
            cycle = await session.get(AgentPaperCycleRecord, seed["cycle_id"])
            assert cycle is not None and cycle.candidate_batch_id is not None
            if expire_at is not None:
                scalar = session.scalar
                clock_reads = 0

                async def expiring_clock(statement, *args, **kwargs):
                    nonlocal clock_reads
                    if getattr(statement, "is_select", False) and any(
                        getattr(column, "name", None) == "clock_timestamp"
                        for column in statement.selected_columns
                    ):
                        clock_reads += 1
                        return now.replace(hour=14) if clock_reads >= expire_at else now
                    return await scalar(statement, *args, **kwargs)

                monkeypatch.setattr(session, "scalar", expiring_clock)
                with pytest.raises(ValueError, match="FORMAL_BUDGET_APPLY_WINDOW_PASSED"):
                    await apply_protection(
                        session,
                        account_id=cycle.account_id,
                        batch_id=cycle.candidate_batch_id,
                        enabled=True,
                        daily_limit_usd=Decimal("0.0008"),
                        configuration_hash="a" * 64,
                        idempotency_key="expired-test",
                        now=now,
                    )
                from nexus.persistence.models.agents import FormalBudgetBindingRecord
                from nexus.persistence.models.runtime import AuditEvent

                assert (
                    await session.scalar(
                        select(func.count()).select_from(FormalBudgetBindingRecord)
                    )
                    == 0
                )
                assert (
                    await session.scalar(
                        select(func.count())
                        .select_from(AuditEvent)
                        .where(AuditEvent.action == "formal_budget_binding_apply")
                    )
                    == 0
                )
                return
            binding = await apply_protection(
                session,
                account_id=cycle.account_id,
                batch_id=cycle.candidate_batch_id,
                enabled=True,
                daily_limit_usd=Decimal("0.0008"),
                configuration_hash="a" * 64,
                idempotency_key="apply-test",
                now=now,
            )
            demand = await protected_demand(session, business_date=now.date(), now=now)
            assert demand is not None
            assert demand.future_usd == Decimal("0.0008")
            assert len(demand.future_tasks) == 4
            assert binding.sequence == 1
            replay = await apply_protection(
                session,
                account_id=cycle.account_id,
                batch_id=cycle.candidate_batch_id,
                enabled=True,
                daily_limit_usd=Decimal("0.0008"),
                configuration_hash="a" * 64,
                idempotency_key="apply-test",
                now=now,
            )
            assert replay.id == binding.id
            account_id, batch_id = cycle.account_id, cycle.candidate_batch_id
        # Exercise the normal apply/config-read entry in this isolated fixture.
        # Only protected configuration and DB clock are supplied by test doubles.
        from pathlib import Path
        from uuid import uuid4

        from helpers.paper_api import settings
        from nexus.ops import formal_budget_protection as entry
        from sqlalchemy.ext.asyncio import AsyncSession

        test_settings = settings(migrated_database_url).model_copy(
            update={
                "formal_budget_protection_enabled": True,
                "agent_daily_budget_usd": Decimal("0.0008"),
            }
        )
        monkeypatch.setattr(
            entry,
            "configuration",
            lambda _root: (
                test_settings,
                account_id,
                batch_id,
                True,
                "a" * 64,
            ),
        )
        real_scalar = AsyncSession.scalar
        runtime_now = [now]

        async def runtime_clock(self, statement, *args, **kwargs):
            if getattr(statement, "is_select", False) and any(
                getattr(column, "name", None) == "clock_timestamp"
                for column in statement.selected_columns
            ):
                return runtime_now[0]
            return await real_scalar(self, statement, *args, **kwargs)

        monkeypatch.setattr(AsyncSession, "scalar", runtime_clock)
        receipt = await entry.apply(Path.cwd(), idempotency_key="normal-entry")
        assert receipt["status"] == "applied" and receipt["sequence"] == 2
        environment = {
            "DATABASE_URL": migrated_database_url,
            "NEXUS_FORMAL_BUDGET_PROTECTION_ENABLED": "true",
            "NEXUS_AGENT_PAPER_ACCOUNT_ID": str(uuid4()),
            "NEXUS_AGENT_PAPER_CANDIDATE_BATCH_ID": str(uuid4()),
            "NEXUS_AGENT_PAPER_CYCLE_ENABLED": "true",
            "AGENT_DAILY_BUDGET_USD": "999",
        }
        bound, calendar = await entry.bind_runtime_environment(test_settings, environment)
        assert bound["NEXUS_AGENT_PAPER_ACCOUNT_ID"] == str(account_id)
        assert bound["NEXUS_AGENT_PAPER_CANDIDATE_BATCH_ID"] == str(batch_id)
        assert Decimal(bound["AGENT_DAILY_BUDGET_USD"]) == Decimal("0.0008")
        assert calendar is not None
        from datetime import timedelta

        from nexus.ops import agent_paper_loop as loop
        from nexus.persistence.models.agents import FormalBudgetBindingRecord
        from nexus.persistence.models.runtime import AuditEvent

        env_file = tmp_path / ".env.protected-test"
        env_file.write_text("\n".join(f"{k}={v}" for k, v in environment.items()) + "\n")
        env_file.chmod(0o600)
        file_bound, file_calendar = await loop._load_bound_loop_environment(
            test_settings, tmp_path, {}, env_file=env_file
        )
        assert file_bound == bound and file_calendar == calendar
        async with sessions.begin() as session:
            before_audits = await session.scalar(select(func.count()).select_from(AuditEvent))
            before_bindings = await session.scalar(
                select(func.count()).select_from(FormalBudgetBindingRecord)
            )
        runtime_now[0] = now + timedelta(days=1)
        with pytest.raises(ValueError, match="FORMAL_BUDGET_APPLY_REQUIRED"):
            await loop._load_bound_loop_environment(test_settings, tmp_path, {}, env_file=env_file)
        # Across UTC day, stopping does not attempt apply or require the missing binding.
        env_file.write_text(
            env_file.read_text().replace(
                "NEXUS_AGENT_PAPER_CYCLE_ENABLED=true", "NEXUS_AGENT_PAPER_CYCLE_ENABLED=false"
            )
        )
        stopped_action = await loop.execute_agent_paper_loop_action(
            test_settings,
            account_id=account_id,
            repo_root=tmp_path,
            env_file=env_file,
            action=loop.AgentPaperLoopAction.TRIGGER,
            snapshot=loop.AgentPaperLoopSnapshot(now=runtime_now[0]),
        )
        assert stopped_action == loop.AgentPaperLoopAction.STOP_DISABLED
        async with sessions.begin() as session:
            assert (
                await session.scalar(select(func.count()).select_from(AuditEvent)) == before_audits
            )
            assert (
                await session.scalar(select(func.count()).select_from(FormalBudgetBindingRecord))
                == before_bindings
            )
        runtime_now[0] = now
        wrong_target = dict(environment, DATABASE_URL=migrated_database_url + "_different")
        with pytest.raises(ValueError, match="FORMAL_BUDGET_DATABASE_MISMATCH"):
            await entry.bind_runtime_environment(test_settings, wrong_target)

        environment["NEXUS_AGENT_PAPER_CYCLE_ENABLED"] = "false"
        stopped, _ = await entry.bind_runtime_environment(test_settings, environment)
        assert stopped["NEXUS_AGENT_PAPER_CYCLE_ENABLED"] == "false"

        from integration.test_agent_version_evaluation_pg import _request
        from nexus.paper.agent_version_evaluation import register_agent_version_evaluation

        request = _request(uuid4(), uuid4())
        shift = now.date() - request.train_slots[0].date()
        request = request.model_copy(
            update={
                "train_slots": tuple(slot + shift for slot in request.train_slots),
                "oos_slots": tuple(slot + shift for slot in request.oos_slots),
            }
        )
        import asyncio

        import nexus.agents.persistence as native

        reached_day_lock = asyncio.Event()
        nonformal_day_lock = asyncio.Event()
        real_lock = native._lock_budget_day

        async def observed_lock(session, business_date):
            if business_date == now.date().isoformat():
                if session.info.get("attempt") == "nonformal":
                    nonformal_day_lock.set()
                else:
                    reached_day_lock.set()
            await real_lock(session, business_date)

        async def register():
            async with sessions.begin() as session:
                session.info["attempt"] = "registration"
                with pytest.raises(ValueError, match="FORMAL_REGISTERED_EVALUATION_UNSUPPORTED"):
                    await register_agent_version_evaluation(
                        session,
                        request=request,
                        actor_id="isolated-registration",
                        idempotency_key="protected-overlap",
                        now=now,
                    )

        async def nonformal():
            async with sessions.begin() as session:
                session.info["attempt"] = "nonformal"
                with pytest.raises(BudgetExhausted, match="FORMAL_BUDGET_PROTECTED"):
                    await reserve_portfolio_decision_budget(
                        session,
                        run_id=seed["cycle_id"],
                        now=now,
                        daily_limit_usd=Decimal("0.0008"),
                        worst_case_usd=Decimal("0.0002"),
                        pricing_version="test",
                        pricing_fresh=True,
                    )

        async with sessions.begin() as session:
            await apply_protection(
                session,
                account_id=account_id,
                batch_id=batch_id,
                enabled=True,
                daily_limit_usd=Decimal("0.0008"),
                configuration_hash="a" * 64,
                idempotency_key="concurrent-apply",
                now=now,
            )
            monkeypatch.setattr(native, "_lock_budget_day", observed_lock)
            registration_task = asyncio.create_task(register())
            nonformal_task = asyncio.create_task(nonformal())
            await asyncio.wait_for(reached_day_lock.wait(), timeout=10)
            await asyncio.wait_for(nonformal_day_lock.wait(), timeout=10)
            assert not registration_task.done()
            assert not nonformal_task.done()
        await asyncio.wait_for(registration_task, timeout=10)
        await asyncio.wait_for(nonformal_task, timeout=10)
        async with sessions.begin() as session:
            demand = await protected_demand(session, business_date=now.date(), now=now)
            assert demand.future_usd == Decimal("0.0008")
            from nexus.paper.economic_budget_hold import HoldRequest, hold_pair_budget

            actual = await session.scalar(text("SELECT current_database()"))
            with pytest.raises(ValueError, match="ECONOMIC_PROTOCOL_BUSINESS_STATE_FORBIDDEN"):
                await hold_pair_budget(
                    session,
                    HoldRequest(
                        evaluation_key="protected-diagnostic",
                        protocol_hash="c" * 64,
                        slot_ordinal=1,
                        research_cost_usd=Decimal("0.0000001"),
                        synthesis_cost_usd=Decimal("0.0000001"),
                        global_limit_usd=Decimal("0.0008"),
                        uncovered_formal_usd=Decimal(0),
                    ),
                    expected_database=actual,
                )
            with pytest.raises(BudgetExhausted, match="FORMAL_BUDGET_PROTECTED"):
                await reserve_portfolio_decision_budget(
                    session,
                    run_id=seed["cycle_id"],
                    now=now,
                    daily_limit_usd=Decimal("0.0008"),
                    worst_case_usd=Decimal("0.0002"),
                    pricing_version="test",
                    pricing_fresh=True,
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("transfer", [False, True, "sdk_zero"])
async def test_empty_isolated_protected_day_hold_uses_database_f_not_caller_zero(
    migrated_database_url,
    monkeypatch,
    transfer,
):
    from hashlib import sha256

    from helpers.paper_batch import DECISION_TIME, seed_paper_batch
    from integration.test_agent_paper_cycle_pg import _seed_two_candidate_authority
    from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
    from nexus.paper.economic_budget_hold import HoldRequest, hold_pair_budget
    from nexus.persistence.models.agents import AgentPromptVersionRecord
    from nexus.strategy.forward import load_forward_strategy_decision
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    seed = await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime(2026, 8, 27, 13, tzinfo=UTC)
    try:
        async with sessions.begin() as session:
            controlled_clock(session, monkeypatch, now)
            registered = await load_forward_strategy_decision(
                session,
                account_id=seed.account_id,
                strategy_version="baseline_v1",
                decision_session=DECISION_TIME.date(),
            )
            authority = await _seed_two_candidate_authority(
                session, registered, candidate_instruments=INSTRUMENTS
            )
            batch = await create_agent_paper_candidate_batch(
                session,
                account_id=authority.account_id,
                universe_snapshot_id=authority.universe_snapshot_id,
                instrument_ids=tuple(i[0] for i in INSTRUMENTS),
                idempotency_key="pre-job-candidates",
                created_at=authority.decision_time,
            )
            session.add(
                AgentPromptVersionRecord(
                    role="portfolio_decision",
                    version="portfolio-management-synthesis-v1",
                    content="Pre-job synthetic synthesis.",
                    content_hash=sha256(b"Pre-job synthetic synthesis.").hexdigest(),
                    created_at=now,
                )
            )
            await session.flush()
            for table in (
                "exchange_calendar_authorities",
                "exchange_calendar_sessions",
                "agent_prompt_versions",
                "agent_model_configs",
            ):
                await session.execute(text(f"ALTER TABLE {table} DISABLE TRIGGER USER"))
                await session.execute(text(f"UPDATE {table} SET created_at=:at"), {"at": now})
                await session.execute(text(f"ALTER TABLE {table} ENABLE TRIGGER USER"))
            if transfer == "sdk_zero":
                from datetime import timedelta

                from nexus.agents.codex_versions import DEADLINE_BUDGET_CONFIG_VERSION
                from nexus.persistence.models.agents import AgentModelConfigRecord

                session.add(
                    AgentModelConfigRecord(
                        model_id="gpt-5.6-terra",
                        version=DEADLINE_BUDGET_CONFIG_VERSION,
                        config_hash="e" * 64,
                        max_input_tokens=96000,
                        max_output_tokens=8192,
                        max_tool_calls=0,
                        input_usd_per_million=Decimal(0),
                        output_usd_per_million=Decimal(0),
                        pricing_available_at=now - timedelta(days=1),
                        pricing_expires_at=now + timedelta(days=1),
                        created_at=now,
                    )
                )
                await session.flush()
            formal_binding = await apply_protection(
                session,
                account_id=seed.account_id,
                batch_id=batch.batch_id,
                enabled=True,
                daily_limit_usd=Decimal("0.002" if transfer else "0.0008"),
                configuration_hash="a" * 64,
                idempotency_key="pre-job-apply",
                now=now,
            )
            actual = await session.scalar(text("SELECT current_database()"))
            if transfer:
                await _verify_priced_protected_transfer(
                    session, now, actual, formal_binding, normal_binding=transfer == "sdk_zero"
                )
                return
            with pytest.raises(BudgetExhausted, match="ECONOMIC_PAIR_BUDGET_INSUFFICIENT"):
                await hold_pair_budget(
                    session,
                    HoldRequest(
                        evaluation_key="protected-pre-job",
                        protocol_hash="c" * 64,
                        slot_ordinal=1,
                        research_cost_usd=Decimal("0.0000001"),
                        synthesis_cost_usd=Decimal("0.0000001"),
                        global_limit_usd=Decimal("0.0008"),
                        uncovered_formal_usd=Decimal(0),
                    ),
                    expected_database=actual,
                )
    finally:
        await engine.dispose()


async def _verify_priced_protected_transfer(session, now, actual, formal, *, normal_binding):
    """Priced synthetic authority exercises real readers, not SDK activation.

    The current normal SDK registration only accepts zero pricing. A typed,
    canonical fixture binding is explicit here so positive dollars test actual
    H-to-native accounting without pretending it is a usable SDK publication.
    """
    from datetime import timedelta
    from uuid import UUID, uuid4

    from helpers.paper_batch import DATA_SNAPSHOT_ID
    from nexus.agents.persistence import reserve_daily_budget
    from nexus.audit.service import append_audit_event
    from nexus.paper.economic_budget_hold import (
        TRANSFER_ACTION,
        AllocationRequest,
        HoldRequest,
        hold_pair_budget,
    )
    from nexus.paper.economic_version_binding import ACTION, ACTOR, SCHEMA, VersionBinding
    from nexus.paper.portfolio_economic_preflight import EconomicPlanRequest, _hash
    from nexus.paper.portfolio_economic_protocol import decode_protocol, register_protocol
    from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
    from nexus.persistence.models.market import InstrumentAlias
    from nexus.persistence.models.runtime import AuditEvent
    from sqlalchemy import func, select

    model = await session.get(AgentModelConfigRecord, UUID(formal.payload["model_id"]))
    if normal_binding:
        from nexus.agents.codex_versions import DEADLINE_BUDGET_CONFIG_VERSION

        model = await session.scalar(
            select(AgentModelConfigRecord).where(
                AgentModelConfigRecord.version == DEADLINE_BUDGET_CONFIG_VERSION
            )
        )
    cost = model.worst_case_cost_usd
    formal_cost = Decimal("0.0002")
    assert cost == (Decimal(0) if normal_binding else Decimal("0.0002"))
    candidate_ids = tuple(UUID(i) for i in formal.payload["instrument_ids"][:2])
    symbols = [
        await session.scalar(
            select(InstrumentAlias.symbol).where(InstrumentAlias.instrument_id == i)
        )
        for i in candidate_ids
    ]
    protocol_event = await register_protocol(
        session,
        EconomicPlanRequest(
            evaluation_key="protected-priced-case",
            instruments=tuple(
                {"instrument_id": i, "symbol": symbol}
                for i, symbol in zip(candidate_ids, symbols, strict=True)
            ),
            start_session=now.date(),
            research_cost_usd=cost,
            synthesis_cost_usd=cost,
            global_daily_limit_usd=Decimal("0.002"),
        ),
        expected_database=actual,
    )
    protocol = decode_protocol(protocol_event)
    # The diagnostic fixture uses existing actual FK/model facts; it does not
    # run a model, publish a challenger, create a cycle, or weaken any guard.
    prompt = formal.payload["synthesis_prompt"]
    binding = VersionBinding(
        protocol_hash=protocol.content_hash,
        proposal_hash="b" * 64,
        publication_event_hash="c" * 64,
        shared_model_id=model.id,
        shared_model_version=model.version,
        shared_model_hash=model.config_hash,
        incumbent_prompt_id=UUID(prompt["id"]),
        incumbent_prompt_hash=prompt["content_hash"],
        challenger_prompt_id=uuid4(),
        challenger_prompt_hash="d" * 64,
        pricing_available_at=model.pricing_available_at,
        pricing_expires_at=model.pricing_expires_at,
        configured_cost_usd=cost,
        content_hash="0" * 64,
    )
    binding = binding.model_copy(
        update={"content_hash": _hash(binding.model_dump(mode="json", exclude={"content_hash"}))}
    )

    async def fixture_observed():
        await session.execute(text("ALTER TABLE audit_events DISABLE TRIGGER USER"))
        await session.execute(text("UPDATE audit_events SET created_at=:at"), {"at": now})
        await session.execute(text("ALTER TABLE audit_events ENABLE TRIGGER USER"))

    if normal_binding:
        from hashlib import sha256

        from nexus.paper.agent_challenger import (
            PORTFOLIO_CHALLENGER_PUBLICATION_ACTION,
            PortfolioChallengerPublicationV1,
        )
        from nexus.paper.economic_version_binding import bind_versions, decode_binding
        from nexus.persistence.models.agents import AgentPromptVersionRecord

        challenger = AgentPromptVersionRecord(
            role="portfolio_decision",
            version="test-challenger",
            content="Synthetic challenger.",
            content_hash=sha256(b"Synthetic challenger.").hexdigest(),
            release_channel="challenger",
            created_at=now,
        )
        session.add(challenger)
        await session.flush()
        publication = PortfolioChallengerPublicationV1(
            schema_version="portfolio_challenger_pub_v1",
            proposal_hash="b" * 64,
            incumbent_prompt_version_id=UUID(prompt["id"]),
            challenger_prompt_version_id=challenger.id,
            challenger_prompt_hash=challenger.content_hash,
            challenger_model_config_id=model.id,
            replayed=False,
        )
        await append_audit_event(
            session,
            actor_id="isolated-fixture",
            action=PORTFOLIO_CHALLENGER_PUBLICATION_ACTION,
            schema_version=publication.schema_version,
            payload=publication.model_dump(mode="json"),
            occurred_at=now,
        )
        await fixture_observed()
        event = await bind_versions(
            session,
            protocol_hash=protocol.content_hash,
            proposal_hash="b" * 64,
            expected_database=actual,
        )
        assert decode_binding(event).shared_model_id == model.id
    else:
        await append_audit_event(
            session,
            actor_id=ACTOR,
            action=ACTION,
            schema_version=SCHEMA,
            payload=binding.model_dump(mode="json"),
            occurred_at=now,
        )
    await fixture_observed()
    held = await hold_pair_budget(
        session,
        HoldRequest(
            evaluation_key=protocol.request.evaluation_key,
            protocol_hash=protocol.content_hash,
            slot_ordinal=1,
            research_cost_usd=cost,
            synthesis_cost_usd=cost,
            global_limit_usd=Decimal("0.002"),
            uncovered_formal_usd=Decimal(0),
        ),
        expected_database=actual,
    )
    before = await protected_demand(session, business_date=now.date(), now=now)
    assert (before.native_usd, before.hold_usd, before.future_usd) == (
        Decimal(0),
        6 * cost,
        4 * formal_cost,
    )
    run = AgentResearchRunRecord(
        instrument_id=candidate_ids[0],
        data_snapshot_id=DATA_SNAPSHOT_ID,
        prompt_version_id=UUID(formal.payload["research_prompts"]["orchestrator"]["id"]),
        model_config_id=model.id,
        decision_time=now + timedelta(hours=1),
        actor_id="isolated-economic-receiver",
        idempotency_key="priced-native-one",
        status="prepared",
        schema_hash="a" * 64,
        model_hash=model.config_hash,
        prompt_hash="a" * 64,
        prompt_manifest={},
        evidence_hash="a" * 64,
        evidence_manifest=[{"synthetic": True}],
        tool_hash="a" * 64,
        input_hash="a" * 64,
        reserved_cost_usd=cost,
        pricing_version=model.version,
    )
    session.add(run)
    await session.flush()
    allocation = AllocationRequest(hold_event_id=held.id, lane="incumbent", action="research_1")
    child = await reserve_daily_budget(
        session,
        run_id=run.id,
        now=now,
        daily_limit_usd=Decimal("0.002"),
        worst_case_usd=cost,
        pricing_version=model.version,
        pricing_fresh=True,
        economic_allocation=allocation,
    )
    assert child.reserved_usd == cost
    after = await protected_demand(session, business_date=now.date(), now=now)
    assert (after.native_usd, after.hold_usd, after.future_usd) == (cost, 5 * cost, 4 * formal_cost)
    assert after.native_usd + after.hold_usd + after.future_usd == 6 * cost + 4 * formal_cost
    repeat = await reserve_daily_budget(
        session,
        run_id=run.id,
        now=now,
        daily_limit_usd=Decimal("0.002"),
        worst_case_usd=cost,
        pricing_version=model.version,
        pricing_fresh=True,
        economic_allocation=allocation,
    )
    assert repeat.id == child.id
    assert (
        await session.scalar(
            select(func.count()).select_from(AuditEvent).where(AuditEvent.action == TRANSFER_ACTION)
        )
        == 1
    )
    # A new unallocated request cannot spend F or double-count the consumed H.
    with pytest.raises(BudgetExhausted, match="FORMAL_BUDGET_PROTECTED"):
        from nexus.paper.formal_budget_protection import require_protected_admission

        await require_protected_admission(
            session,
            kind="portfolio",
            run_id=uuid4(),
            now=now,
            daily_limit_usd=Decimal("0.002"),
            amount=Decimal("0.0020001") if normal_binding else cost,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "frozen_input"])
async def test_protected_synthesis_transfers_hold_with_normal_frozen_input(
    migrated_database_url,
    monkeypatch,
    damage,
):
    """Test-only allocation wiring; no economic activation or provider call."""
    from uuid import UUID

    import integration.test_portfolio_synthesis_persistence_pg as seed_module
    from nexus.agents import portfolio_synthesis_persistence as admission_module
    from nexus.audit.service import append_audit_event
    from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
    from nexus.paper.economic_budget_hold import AllocationRequest, HoldRequest, hold_pair_budget
    from nexus.paper.economic_version_binding import ACTION, ACTOR, SCHEMA, VersionBinding
    from nexus.paper.portfolio_economic_preflight import EconomicPlanRequest, _hash
    from nexus.paper.portfolio_economic_protocol import decode_protocol, register_protocol
    from nexus.persistence.models.market import InstrumentAlias
    from nexus.persistence.models.paper import PaperAccountRecord
    from nexus.persistence.models.portfolio import Account, Portfolio
    from sqlalchemy import func, select

    original_start = seed_module._start_cycle
    held_events = []
    protocols = []

    async def hold_before_cycle(session, **kwargs):
        authority = kwargs["authority"]
        now = authority.decision_time.replace(hour=13, minute=0)
        controlled_clock(session, monkeypatch, now)
        symbols = [
            await session.scalar(
                select(InstrumentAlias.symbol).where(InstrumentAlias.instrument_id == i)
            )
            for i in kwargs["requested_instrument_ids"]
        ]
        actual = await session.scalar(text("SELECT current_database()"))
        event = await register_protocol(
            session,
            EconomicPlanRequest(
                evaluation_key="protected-synthesis-case",
                instruments=tuple(
                    {"instrument_id": i, "symbol": symbol}
                    for i, symbol in zip(kwargs["requested_instrument_ids"], symbols, strict=True)
                ),
                start_session=now.date(),
                research_cost_usd=Decimal("0.0002"),
                synthesis_cost_usd=Decimal("0.0002"),
                global_daily_limit_usd=Decimal("0.0022"),
            ),
            expected_database=actual,
        )
        protocol = decode_protocol(event)
        protocols.append(protocol)
        held_events.append(
            await hold_pair_budget(
                session,
                HoldRequest(
                    evaluation_key=protocol.request.evaluation_key,
                    protocol_hash=protocol.content_hash,
                    slot_ordinal=1,
                    research_cost_usd=Decimal("0.0002"),
                    synthesis_cost_usd=Decimal("0.0002"),
                    global_limit_usd=Decimal("0.0022"),
                    uncovered_formal_usd=Decimal("0.0006"),
                ),
                expected_database=actual,
            )
        )
        return await original_start(session, **kwargs)

    monkeypatch.setattr(seed_module, "_start_cycle", hold_before_cycle)
    engine, sessions, kwargs = await seed_module.seed_synthesis(migrated_database_url)
    kwargs = dict(kwargs, daily_limit_usd=Decimal("0.0022"))
    try:
        async with sessions.begin() as session:
            now = kwargs["now"].replace(hour=13, minute=0)
            controlled_clock(session, monkeypatch, now)
            # Historical synthetic fixture availability, not a runtime backfill.
            for table in (
                "agent_model_configs",
                "agent_prompt_versions",
                "instruments",
                "instrument_aliases",
                "universe_snapshots",
                "universe_members",
                "exchange_calendar_authorities",
                "exchange_calendar_sessions",
            ):
                await session.execute(text(f"ALTER TABLE {table} DISABLE TRIGGER USER"))
                await session.execute(text(f"UPDATE {table} SET created_at=:at"), {"at": now})
                if table in {"universe_snapshots", "exchange_calendar_authorities"}:
                    await session.execute(text(f"UPDATE {table} SET published_at=:at"), {"at": now})
                await session.execute(text(f"ALTER TABLE {table} ENABLE TRIGGER USER"))
            source_cycle = await session.get(AgentPaperCycleRecord, kwargs["cycle_id"])
            from nexus.persistence.models.paper import AgentPaperCandidateBatchRecord

            source_batch = await session.get(
                AgentPaperCandidateBatchRecord, source_cycle.candidate_batch_id
            )
            # Separate formal account prevents borrowing its exact task credit.
            underlying = Account(provider="internal", status="ACTIVE", created_at=now)
            session.add(underlying)
            await session.flush()
            portfolio = Portfolio(account_id=underlying.id, name="Synthetic formal", created_at=now)
            session.add(portfolio)
            await session.flush()
            account = PaperAccountRecord(
                account_id=underlying.id,
                portfolio_id=portfolio.id,
                code="synthetic-protected-formal",
                currency="USD",
                account_model="cash",
                position_model="long_only",
                initial_capital=Decimal(10000),
                capability="simulation_write_internal",
                created_at=now,
            )
            session.add(account)
            await session.flush()
            batch = await create_agent_paper_candidate_batch(
                session,
                account_id=account.id,
                universe_snapshot_id=source_batch.universe_snapshot_id,
                instrument_ids=tuple(i[0] for i in INSTRUMENTS[:2]),
                idempotency_key="separate-formal",
                created_at=now,
            )
            formal = await apply_protection(
                session,
                account_id=account.id,
                batch_id=batch.batch_id,
                enabled=True,
                daily_limit_usd=Decimal("0.0022"),
                configuration_hash="a" * 64,
                idempotency_key="separate-formal-apply",
                now=now,
            )
            p = formal.payload
            model_spec = p["model_spec"]
            binding = VersionBinding(
                protocol_hash=protocols[0].content_hash,
                proposal_hash="b" * 64,
                publication_event_hash="c" * 64,
                shared_model_id=UUID(p["model_id"]),
                shared_model_version=p["pricing_version"],
                shared_model_hash=p["model_hash"],
                incumbent_prompt_id=UUID(p["synthesis_prompt"]["id"]),
                incumbent_prompt_hash=p["synthesis_prompt"]["content_hash"],
                challenger_prompt_id=UUID(int=9999),
                challenger_prompt_hash="d" * 64,
                pricing_available_at=datetime.fromisoformat(model_spec["pricing_available_at"]),
                pricing_expires_at=datetime.fromisoformat(model_spec["pricing_expires_at"]),
                configured_cost_usd=Decimal("0.0002"),
                content_hash="0" * 64,
            )
            binding = binding.model_copy(
                update={
                    "content_hash": _hash(binding.model_dump(mode="json", exclude={"content_hash"}))
                }
            )
            await append_audit_event(
                session,
                actor_id=ACTOR,
                action=ACTION,
                schema_version=SCHEMA,
                payload=binding.model_dump(mode="json"),
                occurred_at=now,
            )
            await session.execute(text("ALTER TABLE audit_events DISABLE TRIGGER USER"))
            await session.execute(text("UPDATE audit_events SET created_at=:at"), {"at": now})
            await session.execute(text("ALTER TABLE audit_events ENABLE TRIGGER USER"))
            before = await protected_demand(session, business_date=now.date(), now=now)
            assert (before.native_usd, before.hold_usd, before.future_usd) == (
                Decimal("0.0004"),
                Decimal("0.0012"),
                Decimal("0.0006"),
            )
            original_reserve = admission_module.reserve_portfolio_synthesis_budget
            allocation = AllocationRequest(
                hold_event_id=held_events[0].id, lane="incumbent", action="synthesis"
            )

            async def funded_reserve(session, **reserve_kwargs):
                if damage:
                    run = await session.get(
                        seed_module.AgentPortfolioSynthesisRunRecord, reserve_kwargs["run_id"]
                    )
                    run.input_hash = "0" * 64
                with session.no_autoflush:
                    return await original_reserve(
                        session, **reserve_kwargs, economic_allocation=allocation
                    )

            monkeypatch.setattr(
                admission_module, "reserve_portfolio_synthesis_budget", funded_reserve
            )
            controlled_clock(session, monkeypatch, kwargs["now"])
            if damage:
                with pytest.raises(ValueError, match="ECONOMIC_TRANSFER_RECEIVER_INVALID"):
                    await admission_module.admit_portfolio_synthesis(session, **kwargs)
                assert await protected_demand(session, business_date=now.date(), now=now) == before
                assert (
                    await session.scalar(
                        select(func.count()).select_from(
                            seed_module.AgentPortfolioSynthesisRunRecord
                        )
                    )
                    == 0
                )
                assert (
                    await session.scalar(
                        select(func.count()).select_from(
                            seed_module.AgentPortfolioSynthesisBudgetReservationRecord
                        )
                    )
                    == 0
                )
                return
            admitted = await admission_module.admit_portfolio_synthesis(session, **kwargs)
            after = await protected_demand(session, business_date=now.date(), now=now)
            assert (after.native_usd, after.hold_usd, after.future_usd) == (
                Decimal("0.0006"),
                Decimal("0.001"),
                Decimal("0.0006"),
            )
            replay = await original_reserve(
                session,
                run_id=admitted.run.id,
                now=kwargs["now"],
                daily_limit_usd=Decimal("0.0022"),
                worst_case_usd=Decimal("0.0002"),
                pricing_version=admitted.run.pricing_version,
                pricing_fresh=True,
                economic_allocation=allocation,
            )
            assert replay.run_id == admitted.run.id
            from nexus.paper.economic_budget_hold import TRANSFER_ACTION
            from nexus.persistence.models.runtime import AuditEvent

            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(AuditEvent)
                    .where(AuditEvent.action == TRANSFER_ACTION)
                )
                == 1
            )
            assert (
                await session.scalar(
                    select(func.count()).select_from(
                        seed_module.AgentPortfolioSynthesisBudgetReservationRecord
                    )
                )
                == 1
            )
            assert await protected_demand(session, business_date=now.date(), now=now) == after
    finally:
        await engine.dispose()


@pytest.mark.parametrize("apply_hour", [0, 4, 13])
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ambient",
    [None, "DATABASE_URL", "AGENT_DAILY_BUDGET_USD", "NEXUS_FORMAL_BUDGET_PROTECTION_ENABLED"],
)
async def test_normal_protected_file_apply_pins_runtime_without_config_mock(
    migrated_database_url, monkeypatch, tmp_path, ambient, apply_hour
):
    """Use the actual ignored config parser; only the DB clock is synthetic."""
    import subprocess
    from datetime import timedelta

    from helpers.paper_batch import CALENDAR_ID
    from nexus.ops import agent_paper_loop as loop
    from nexus.ops import formal_budget_protection as entry
    from nexus.ops.formal_budget_inventory import configuration
    from nexus.persistence.models.agents import AgentModelConfigRecord, FormalBudgetBindingRecord
    from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
    from nexus.persistence.models.runtime import AuditEvent
    from sqlalchemy import func, select
    from sqlalchemy.ext.asyncio import AsyncSession

    engine, sessions, seed = await seed_synthesis(
        migrated_database_url, candidate_instruments=INSTRUMENTS, complete_research=False
    )
    now = datetime(2026, 8, 27, 13, tzinfo=UTC)
    try:
        async with sessions.begin() as session:
            assert await session.scalar(text("SELECT current_database()")) != "postgres"
            for table in (
                "exchange_calendar_authorities",
                "exchange_calendar_sessions",
                "agent_model_configs",
                "agent_prompt_versions",
            ):
                await session.execute(text(f"ALTER TABLE {table} DISABLE TRIGGER USER"))
                await session.execute(text(f"UPDATE {table} SET created_at='2026-08-26T20:00:00Z'"))
                await session.execute(text(f"ALTER TABLE {table} ENABLE TRIGGER USER"))
            cycle = await session.get(AgentPaperCycleRecord, seed["cycle_id"])
            assert cycle is not None and cycle.candidate_batch_id is not None
            account, batch = cycle.account_id, cycle.candidate_batch_id
            next_day = now + timedelta(days=1)
            session.add(
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=CALENDAR_ID,
                    session=next_day.date(),
                    market_open_at=next_day.replace(hour=13, minute=30),
                    market_close_at=next_day.replace(hour=20),
                    decision_at=next_day.replace(hour=20),
                    created_at=now - timedelta(days=1),
                )
            )
            before_cycles = await session.scalar(
                select(func.count()).select_from(AgentPaperCycleRecord)
            )
        root = tmp_path / "protected-config"
        (root / "infra").mkdir(parents=True)
        subprocess.run(["git", "init", "--quiet", str(root)], check=True, capture_output=True)
        (root / ".gitignore").write_text("infra/.env.local\n")
        values = {
            "NEXUS_ENV": "test",
            "DATABASE_URL": migrated_database_url,
            "NEXUS_FORMAL_BUDGET_PROTECTION_ENABLED": "true",
            "NEXUS_AGENT_PAPER_CYCLE_ENABLED": "true",
            "NEXUS_AGENT_PAPER_ACCOUNT_ID": str(account),
            "NEXUS_AGENT_PAPER_CANDIDATE_BATCH_ID": str(batch),
            "AGENT_DAILY_BUDGET_USD": "0.0008",
        }
        config_file = root / "infra/.env.local"
        config_file.write_text("\n".join(f"{key}={value}" for key, value in values.items()) + "\n")
        config_file.chmod(0o600)
        # Explicitly isolate this test from the runner's administrative DB URL.
        for key in values:
            monkeypatch.delenv(key, raising=False)
        if ambient is not None:
            monkeypatch.setenv(
                ambient,
                {
                    "DATABASE_URL": "postgresql+asyncpg://fixture:fixture@127.0.0.1:1/wrong_target",
                    "AGENT_DAILY_BUDGET_USD": "999",
                    "NEXUS_FORMAL_BUDGET_PROTECTION_ENABLED": "false",
                }[ambient],
            )
        original_scalar = AsyncSession.scalar

        async def clock(self, statement, *args, **kwargs):
            if getattr(statement, "is_select", False) and any(
                getattr(column, "name", None) == "clock_timestamp"
                for column in statement.selected_columns
            ):
                return now
            return await original_scalar(self, statement, *args, **kwargs)

        monkeypatch.setattr(AsyncSession, "scalar", clock)
        runtime_settings, parsed_account, parsed_batch, enabled, fingerprint = configuration(root)
        assert runtime_settings.database_url == migrated_database_url
        assert runtime_settings.agent_daily_budget_usd == Decimal("0.0008")
        assert runtime_settings.formal_budget_protection_enabled is True
        assert (parsed_account, parsed_batch, enabled) == (account, batch, True)
        before_status = await entry.inspect_protection(root)
        assert before_status["status"] == "apply_required"
        assert before_status["budget_binding_ready"] is False
        if ambient is None and apply_hour == 13:
            import asyncio

            import integration.test_agent_version_evaluation_pg as legacy
            import nexus.agents.persistence as native
            from nexus.paper.agent_version_evaluation import invalidate_agent_version_evaluation
            from nexus.persistence.models.agent_versions import AgentVersionEvaluationDecisionRecord
            from nexus.persistence.models.market import Instrument, InstrumentAlias

            # An intentionally invalid legacy fixture overlaps the formal UTC day.
            # Both contracts use the same actual AAPL Instrument/alias, not a duplicate.
            seed_authority = legacy._seed_authority

            async def legacy_authority(session, **kwargs):
                add_all, add = session.add_all, session.add

                def existing_instrument(instances):
                    add_all([item for item in instances if not isinstance(item, Instrument)])

                def existing_alias(item):
                    if not isinstance(item, InstrumentAlias):
                        add(item)

                with monkeypatch.context() as seed_fixture:
                    seed_fixture.setattr(session, "add_all", existing_instrument)
                    seed_fixture.setattr(session, "add", existing_alias)
                    return await seed_authority(
                        session,
                        pricing_expires_at=now.replace(hour=14) + timedelta(days=1),
                        **kwargs,
                    )

            with monkeypatch.context() as fixture:
                fixture.setattr(legacy, "_seed_authority", legacy_authority)
                fixture.setattr(legacy, "NOW", now - timedelta(days=1))
                fixture.setattr(legacy, "TRAIN_DATES", (now.date(), next_day.date()))
                async with sessions.begin() as session:
                    aapl = await session.scalar(
                        select(InstrumentAlias.instrument_id).where(
                            InstrumentAlias.symbol == "AAPL"
                        )
                    )
                    assert aapl is not None
                    fixture.setattr(legacy, "INSTRUMENT_ID", aapl)
                    evaluation_id = await legacy._seed_legacy_invalid_evaluation(session)
            with pytest.raises(ValueError, match="FORMAL_REGISTERED_EVALUATION_UNSUPPORTED"):
                await entry.apply(root, idempotency_key="protected-file-apply")

            reached = asyncio.Event()
            real_lock = native._lock_budget_day

            async def observed_lock(session, business_date):
                reached.set()
                await real_lock(session, business_date)

            task = None
            with monkeypatch.context() as observation:
                try:
                    async with sessions.begin() as terminal:
                        result = await invalidate_agent_version_evaluation(
                            terminal, evaluation_id=evaluation_id, now=now
                        )
                        assert result.status == "UNKNOWN" and result.execution_locked
                        observation.setattr(native, "_lock_budget_day", observed_lock)
                        task = asyncio.create_task(
                            entry.apply(root, idempotency_key="protected-file-apply")
                        )
                        await asyncio.wait_for(reached.wait(), timeout=10)
                        assert not task.done()
                        async with sessions.begin() as reader:
                            assert (
                                await reader.scalar(
                                    select(func.count()).select_from(FormalBudgetBindingRecord)
                                )
                                == 0
                            )
                            assert (
                                await reader.scalar(
                                    select(func.count()).select_from(
                                        AgentVersionEvaluationDecisionRecord
                                    )
                                )
                                == 0
                            )
                    first = await asyncio.wait_for(task, timeout=10)
                finally:
                    if task is not None and not task.done():
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
        else:
            first = await entry.apply(root, idempotency_key="protected-file-apply")
        replay = await entry.apply(root, idempotency_key="protected-file-apply")
        assert first == replay
        assert first["status"] == "applied" and first["sequence"] == 1
        assert first["execution_locked"] is True
        assert first["economic_activation_permitted"] is False
        bound, calendar = await entry.bind_runtime_environment(runtime_settings, values)
        assert bound["NEXUS_AGENT_PAPER_ACCOUNT_ID"] == str(account)
        assert bound["NEXUS_AGENT_PAPER_CANDIDATE_BATCH_ID"] == str(batch)
        assert Decimal(bound["AGENT_DAILY_BUDGET_USD"]) == Decimal("0.0008")
        assert calendar is not None
        async with sessions.begin() as session:
            records = (await session.scalars(select(FormalBudgetBindingRecord))).all()
            assert len(records) == 1 and records[0].payload["configuration_hash"] == fingerprint
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(AuditEvent)
                    .where(AuditEvent.action == "formal_budget_binding_apply")
                )
                == 1
            )
            assert (
                await session.scalar(select(func.count()).select_from(AgentPaperCycleRecord))
                == before_cycles
            )
        observed_status = await entry.inspect_protection(root)
        assert observed_status["status"] == "applied"
        assert observed_status["budget_binding_ready"] is True
        assert observed_status["binding_hash"] == first["binding_hash"]
        if ambient is None and apply_hour == 13:
            validate_binding = entry._validate_binding
            original_config = config_file.read_text()

            async def disable_during_read(session, binding):
                await validate_binding(session, binding)
                config_file.write_text(
                    original_config.replace(
                        "NEXUS_FORMAL_BUDGET_PROTECTION_ENABLED=true",
                        "NEXUS_FORMAL_BUDGET_PROTECTION_ENABLED=false",
                    )
                )

            monkeypatch.setattr(entry, "_validate_binding", disable_during_read)
            try:
                with pytest.raises(ValueError, match="FORMAL_CONFIGURATION_CHANGED_DURING_READ"):
                    await entry.inspect_protection(root)
            finally:
                monkeypatch.setattr(entry, "_validate_binding", validate_binding)
                config_file.write_text(original_config)
        config_file.write_text(
            config_file.read_text().replace(
                "AGENT_DAILY_BUDGET_USD=0.0008", "AGENT_DAILY_BUDGET_USD=0.0009"
            )
        )
        pending = await entry.inspect_protection(root)
        assert pending["status"] == "configuration_pending_apply"
        assert pending["budget_binding_ready"] is True
        config_file.write_text(
            config_file.read_text().replace(
                "AGENT_DAILY_BUDGET_USD=0.0009", "AGENT_DAILY_BUDGET_USD=0.0008"
            )
        )
        old_hash = first["binding_hash"]
        now = (now + timedelta(days=1)).replace(hour=apply_hour)
        assert (await entry.inspect_protection(root))["status"] == "apply_required"
        with pytest.raises(ValueError, match="FORMAL_BUDGET_APPLY_REQUIRED"):
            await entry.bind_runtime_environment(runtime_settings, values)
        with pytest.raises(ValueError, match="FORMAL_BUDGET_APPLY_CONFLICT"):
            await entry.apply(root, idempotency_key="protected-file-apply")
        # The old fixture pricing ends exactly at the next formal slot.
        # Preserve it and supply a distinct new-day synthetic authority.
        with pytest.raises(ValueError, match="FORMAL_BUDGET_SOURCE_UNAVAILABLE"):
            await entry.apply(root, idempotency_key="protected-file-next-day")
        async with sessions.begin() as session:
            old_model = await session.scalar(select(AgentModelConfigRecord))
            assert old_model is not None
            old_expiry = old_model.pricing_expires_at
            assert old_expiry <= now.replace(hour=14)
            assert (
                await session.scalar(select(func.count()).select_from(FormalBudgetBindingRecord))
                == 1
            )
            session.add(
                AgentModelConfigRecord(
                    model_id="fixture-next-day-formal-budget",
                    version=old_model.version,
                    config_hash="7" * 64,
                    max_input_tokens=old_model.max_input_tokens,
                    max_tool_calls=old_model.max_tool_calls,
                    max_output_tokens=old_model.max_output_tokens,
                    input_usd_per_million=old_model.input_usd_per_million,
                    output_usd_per_million=old_model.output_usd_per_million,
                    pricing_available_at=now - timedelta(hours=1),
                    pricing_expires_at=now + timedelta(days=1),
                    created_at=now,
                )
            )
        next_receipt = await entry.apply(root, idempotency_key="protected-file-next-day")
        assert next_receipt["budget_day_utc"] == now.date().isoformat()
        assert next_receipt["sequence"] == 1
        assert next_receipt["binding_hash"] != old_hash
        next_bound, next_calendar = await loop._load_bound_loop_environment(
            runtime_settings, root, {}, env_file=config_file
        )
        assert next_bound["NEXUS_AGENT_PAPER_ACCOUNT_ID"] == str(account)
        assert next_bound["NEXUS_AGENT_PAPER_CANDIDATE_BATCH_ID"] == str(batch)
        assert next_calendar == calendar
        async with sessions.begin() as session:
            demand = await protected_demand(session, business_date=now.date(), now=now)
            assert demand is not None and len(demand.future_tasks) == 4
            assert demand.future_usd == Decimal("0.0008")
            preserved_model = await session.get(AgentModelConfigRecord, old_model.id)
            assert preserved_model is not None and preserved_model.pricing_expires_at == old_expiry
            records = (await session.scalars(select(FormalBudgetBindingRecord))).all()
            assert len(records) == 2
            assert {record.business_date for record in records} == {
                now.date(),
                (now - timedelta(days=1)).date(),
            }
            assert old_hash in {record.content_hash for record in records}
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(AuditEvent)
                    .where(AuditEvent.action == "formal_budget_binding_apply")
                )
                == 2
            )
            assert (
                await session.scalar(select(func.count()).select_from(AgentPaperCycleRecord))
                == before_cycles
            )
    finally:
        await engine.dispose()
