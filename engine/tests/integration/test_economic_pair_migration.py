"""Database-only economic identity constraints; these are not lifecycle acceptance."""

import asyncio
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from helpers.paper_api import settings
from nexus.audit.service import append_audit_event
from nexus.paper.portfolio_economic_protocol import register_protocol
from nexus.persistence.models.paper import PaperLedgerAccountRecord, PaperLedgerEntryRecord
from nexus.persistence.session import session_factory
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from engine.tests.integration.test_economic_authorities_pg import seed_pair
from engine.tests.integration.test_portfolio_economic_protocol_pg import protocol_request

ROOT = Path(__file__).resolve().parents[2]
TABLES = ("economic_evaluations", "economic_pair_slots", "economic_lane_cycles")


async def seed_identity(
    url, *, funding_amount=10000, binding_fault=None, reverse_protocol=False, protocol_fault=None
):
    factory = session_factory(settings(url))
    async with factory.begin() as s:
        assert all([await s.scalar(text("SELECT to_regclass(:name)"), {"name": t}) for t in TABLES])
        actual = await s.scalar(text("SELECT current_database()"))
        from nexus.paper.economic_version_binding import ACTOR, bind_versions, decode_binding
        from nexus.paper.portfolio_economic_preflight import _hash

        await seed_pair(s, False)
        request = protocol_request().model_copy(
            update={
                "research_cost_usd": Decimal(0),
                "synthesis_cost_usd": Decimal(0),
            }
        )
        if reverse_protocol:
            request = request.model_copy(
                update={"instruments": tuple(reversed(request.instruments))}
            )
        protocol = await register_protocol(s, request, expected_database=actual)
        binding = await bind_versions(
            s,
            protocol_hash=protocol.payload["content_hash"],
            proposal_hash="b" * 64,
            expected_database=actual,
        )
        assert decode_binding(binding).protocol_hash == protocol.payload["content_hash"]
        if protocol_fault is not None:
            payload = dict(protocol.payload)
            payload[protocol_fault] = False if protocol_fault == "execution_locked" else "invalid"
            protocol = await append_audit_event(
                s,
                actor_id="local-economic-protocol-cli",
                action="portfolio.economic.protocol_registered",
                schema_version="portfolio_economic_protocol_v1",
                payload=payload,
                occurred_at=protocol.occurred_at,
            )
        if binding_fault is not None:
            payload = dict(binding.payload)
            actor = ACTOR
            if binding_fault == "partial":
                payload = {key: payload[key] for key in ("protocol_hash", "content_hash")}
            elif binding_fault == "actor":
                actor = "schema-fixture"
            elif binding_fault == "content_hash":
                payload["shared_model_version"] = "not-the-frozen-model"
            elif binding_fault == "same_prompt":
                payload["challenger_prompt_id"] = payload["incumbent_prompt_id"]
                payload["challenger_prompt_hash"] = payload["incumbent_prompt_hash"]
                payload["content_hash"] = _hash(
                    {k: v for k, v in payload.items() if k != "content_hash"}
                )
            elif binding_fault == "model_hash":
                payload["shared_model_hash"] = "c" * 64
                payload["content_hash"] = _hash(
                    {k: v for k, v in payload.items() if k != "content_hash"}
                )
            elif binding_fault == "expired":
                payload["pricing_expires_at"] = "2026-01-01T00:00:00Z"
                payload["content_hash"] = _hash(
                    {k: v for k, v in payload.items() if k != "content_hash"}
                )
            else:
                raise AssertionError(binding_fault)
            binding = await append_audit_event(
                s,
                actor_id=actor,
                action="portfolio.economic.versions_bound",
                schema_version="portfolio_economic_binding_v1",
                payload=payload,
                occurred_at=binding.occurred_at,
            )
        lanes = []
        for name in ("incumbent", "challenger"):
            a, p, paper, funding = (uuid4() for _ in range(4))
            await s.execute(
                text(
                    "INSERT INTO accounts(id,provider,status) "
                    "VALUES (:id,'internal_paper','ACTIVE')"
                ),
                {"id": a},
            )
            await s.execute(
                text("INSERT INTO portfolios(id,account_id,name) VALUES (:id,:account,:name)"),
                {"id": p, "account": a, "name": name},
            )
            await s.execute(
                text("""INSERT INTO paper_accounts
                (id,account_id,portfolio_id,code,currency,account_model,position_model,initial_capital,capability)
                VALUES (:id,:account,:portfolio,:code,'USD','cash','long_only',
                        10000,'simulation_write_internal')"""),
                {"id": paper, "account": a, "portfolio": p, "code": "economic-" + str(paper)},
            )
            await s.execute(
                text("""INSERT INTO paper_ledger_transactions
                (id,account_id,order_id,transaction_type,reference_type,reference_hash,occurred_at,transaction_hash)
                VALUES (:id,:account,NULL,'funding','initial_funding',
                        :hash,clock_timestamp(),:hash)"""),
                {"id": funding, "account": paper, "hash": "c" * 64},
            )
            for seq, (code, normal_side) in enumerate(
                (
                    ("asset.cash.available.usd", "debit"),
                    ("equity.contributed_capital.usd", "credit"),
                ),
                start=1,
            ):
                ledger_id = uuid4()
                s.add(
                    PaperLedgerAccountRecord(
                        id=ledger_id,
                        paper_account_id=paper,
                        code=code,
                        normal_side=normal_side,
                        currency="USD",
                    )
                )
                await s.flush()
                s.add(
                    PaperLedgerEntryRecord(
                        id=uuid4(),
                        transaction_id=funding,
                        account_id=paper,
                        entry_seq=seq,
                        ledger_account_id=ledger_id,
                        account_code=code,
                        currency="USD",
                        debit_amount=Decimal(funding_amount if seq == 1 else 0),
                        credit_amount=Decimal(funding_amount if seq == 2 else 0),
                    )
                )
                await s.flush()
            lanes.append((paper, p, funding))
        eid = uuid4()
        params = dict(
            id=eid,
            protocol=protocol.id,
            binding=binding.id,
            protocol_hash=protocol.payload["content_hash"],
            binding_hash=binding.payload["content_hash"],
            ia=lanes[0][0],
            ip=lanes[0][1],
            io=lanes[0][2],
            ca=lanes[1][0],
            cp=lanes[1][1],
            co=lanes[1][2],
        )
        await s.execute(
            text("""INSERT INTO economic_evaluations
            (id,protocol_event_id,binding_event_id,protocol_hash,binding_hash,
             incumbent_account_id,incumbent_portfolio_id,incumbent_opening_id,
             challenger_account_id,challenger_portfolio_id,challenger_opening_id)
            VALUES (:id,:protocol,:binding,:protocol_hash,:binding_hash,
                    :ia,:ip,:io,:ca,:cp,:co)"""),
            params,
        )
        slot = protocol.payload["slots"][0]
        sid = uuid4()
        slot_params = dict(
            id=sid,
            evaluation=eid,
            ordinal=slot["ordinal"],
            session=date.fromisoformat(slot["session"]),
            decision=datetime.fromisoformat(slot["decision_at"]),
            close=datetime.fromisoformat(slot["market_close_at"]),
        )
        await s.execute(
            text("""INSERT INTO economic_pair_slots
            (id,evaluation_id,ordinal,session,decision_at,cutoff_at,market_close_at)
            VALUES (:id,:evaluation,:ordinal,CAST(:session AS date),CAST(:decision AS timestamptz),
                    CAST(:decision AS timestamptz),CAST(:close AS timestamptz))"""),
            slot_params,
        )
    return factory, params, slot_params


@pytest.mark.asyncio
async def test_pair_schema_retains_distinct_identity_and_registered_slot(migrated_database_url):
    factory, _, slot = await seed_identity(migrated_database_url)
    async with factory.begin() as s:
        row = (
            await s.execute(
                text("SELECT ordinal,execution_locked FROM economic_pair_slots WHERE id=:id"),
                {"id": slot["id"]},
            )
        ).one()
        assert tuple(row) == (1, True)
        assert await s.scalar(text("SELECT count(*) FROM economic_lane_cycles")) == 0
        assert await s.scalar(text("SELECT count(*) FROM jobs")) == 0
        assert await s.scalar(text("SELECT count(*) FROM paper_orders")) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE economic_evaluations SET binding_hash=repeat('d',64)",
        "DELETE FROM economic_evaluations",
        "UPDATE economic_pair_slots SET ordinal=2",
        "DELETE FROM economic_pair_slots",
        "TRUNCATE economic_evaluations CASCADE",
    ],
)
async def test_pair_facts_are_immutable(migrated_database_url, statement):
    factory, _, _ = await seed_identity(migrated_database_url)
    async with factory.begin() as s:
        with pytest.raises(DBAPIError, match="ECONOMIC_PAIR_IMMUTABLE"):
            async with s.begin_nested():
                await s.execute(text(statement))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "column,value",
    [
        ("ordinal", "2"),
        ("session", "DATE '2027-01-05'"),
        ("decision_at", "TIMESTAMPTZ '2027-01-04 15:01:00+00'"),
        ("cutoff_at", "TIMESTAMPTZ '2027-01-04 15:01:00+00'"),
    ],
)
async def test_slot_cannot_move_to_unregistered_time(migrated_database_url, column, value):
    factory, _, slot = await seed_identity(migrated_database_url)
    async with factory.begin() as s:
        with pytest.raises(DBAPIError, match="ECONOMIC_PAIR_SLOT_INVALID"):
            async with s.begin_nested():
                await s.execute(
                    text(f"""INSERT INTO economic_pair_slots
                    SELECT :id,evaluation_id,{value if column == "ordinal" else "ordinal"},
                    {value if column == "session" else "session"},
                    {value if column == "decision_at" else "decision_at"},
                    {value if column == "cutoff_at" else "cutoff_at"},
                    market_close_at,execution_locked,created_at
                    FROM economic_pair_slots WHERE id=:prior"""),
                    {"id": uuid4(), "prior": slot["id"]},
                )


@pytest.mark.asyncio
async def test_same_account_cannot_appear_in_both_lanes(migrated_database_url):
    factory, identity, _ = await seed_identity(migrated_database_url)
    async with factory.begin() as s:
        with pytest.raises(DBAPIError):
            async with s.begin_nested():
                await s.execute(
                    text("""INSERT INTO economic_evaluations
                    SELECT :id,protocol_event_id,binding_event_id,protocol_hash,binding_hash,
                    incumbent_account_id,incumbent_portfolio_id,incumbent_opening_id,
                    incumbent_account_id,incumbent_portfolio_id,incumbent_opening_id,created_at
                    FROM economic_evaluations WHERE id=:prior"""),
                    {"id": uuid4(), "prior": identity["id"]},
                )


@pytest.mark.asyncio
async def test_unknown_cycle_cannot_be_attached_or_make_jobs(migrated_database_url):
    factory, identity, slot = await seed_identity(migrated_database_url)
    async with factory.begin() as s:
        with pytest.raises(DBAPIError, match="ECONOMIC_PAIR_CYCLE_INVALID"):
            async with s.begin_nested():
                await s.execute(
                    text("""INSERT INTO economic_lane_cycles
                    (id,pair_slot_id,lane,account_id,portfolio_id,cycle_id)
                    VALUES (:id,:pair,'incumbent',:account,:portfolio,:cycle)"""),
                    {
                        "id": uuid4(),
                        "pair": slot["id"],
                        "account": identity["ia"],
                        "portfolio": identity["ip"],
                        "cycle": uuid4(),
                    },
                )
        assert await s.scalar(text("SELECT count(*) FROM economic_lane_cycles")) == 0
        assert await s.scalar(text("SELECT count(*) FROM jobs")) == 0


@pytest.mark.asyncio
async def test_registered_slot_is_unique_and_locked(migrated_database_url):
    factory, _, slot = await seed_identity(migrated_database_url)
    async with factory.begin() as s:
        with pytest.raises(DBAPIError, match="uq_economic_pair_slot_ordinal"):
            async with s.begin_nested():
                await s.execute(
                    text("""INSERT INTO economic_pair_slots
                    SELECT :id,evaluation_id,ordinal,session,decision_at,cutoff_at,
                           market_close_at,execution_locked,created_at
                    FROM economic_pair_slots WHERE id=:prior"""),
                    {"id": uuid4(), "prior": slot["id"]},
                )
        with pytest.raises(DBAPIError, match="execution_locked"):
            async with s.begin_nested():
                await s.execute(
                    text("""INSERT INTO economic_pair_slots
                    SELECT :id,evaluation_id,ordinal,session,decision_at,cutoff_at,
                           market_close_at,false,created_at
                    FROM economic_pair_slots WHERE id=:prior"""),
                    {"id": uuid4(), "prior": slot["id"]},
                )


def test_economic_pair_empty_roundtrip(monkeypatch, temporary_database_url):
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(ROOT / "alembic.ini"))
    command.upgrade(config, "0069_formal_budget_output_merge")
    command.upgrade(config, "0070_economic_pair_identity")
    command.downgrade(config, "0069_formal_budget_output_merge")

    async def verify():
        engine = create_async_engine(temporary_database_url)
        try:
            async with engine.connect() as c:
                assert all(
                    [
                        await c.scalar(text("SELECT to_regclass(:name)"), {"name": t}) is None
                        for t in TABLES
                    ]
                )
        finally:
            await engine.dispose()

    asyncio.run(verify())


def test_economic_pair_populated_rollback_refuses(monkeypatch, migrated_database_url):
    asyncio.run(seed_identity(migrated_database_url))
    monkeypatch.setenv("DATABASE_URL", migrated_database_url)
    with pytest.raises(DBAPIError, match="ECONOMIC_PAIR_ROLLBACK_REQUIRES_EMPTY"):
        command.downgrade(Config(str(ROOT / "alembic.ini")), "0069_formal_budget_output_merge")


@pytest.mark.asyncio
@pytest.mark.parametrize("funding_amount", [9999, 10001])
async def test_opening_cash_must_match_frozen_10000_capital(migrated_database_url, funding_amount):
    with pytest.raises(DBAPIError, match="ECONOMIC_PAIR_OPENING_INVALID"):
        await seed_identity(migrated_database_url, funding_amount=funding_amount)


async def seed_economic_cycle(
    url, *, manifest_fault=None, batch_fault=False, reverse_protocol=False
):
    """Schema fixture only; no claim of product admission or valid research."""
    from nexus.persistence.models.market import UniverseSnapshot
    from nexus.persistence.models.paper import AgentPaperCandidateBatchRecord, AgentPaperCycleRecord

    factory, identity, slot = await seed_identity(url, reverse_protocol=reverse_protocol)
    cycle_id, universe_id, batch_id = (uuid4() for _ in range(3))
    from nexus.paper.candidate_batch import _canonical_hash
    from nexus.paper.portfolio_economic_preflight import _hash

    manifest = [
        {
            **item.model_dump(mode="json"),
            "inclusion_reason": 'schema-fixture 中文 "reason"\n😀',
            "research_snapshot_id": str(uuid4()),
            "prior_daily_price_authority_id": str(uuid4()),
            "decision_quote_id": str(uuid4()),
            "decision_trade_id": None,
            "decision_artifact_id": str(uuid4()),
            "decision_evidence_hash": "a" * 64,
            "risk_query_authority_ids": [str(uuid4()) for _ in range(3)],
        }
        for item in protocol_request().instruments
    ]
    if manifest_fault == "order":
        manifest.reverse()
    elif manifest_fault == "symbol":
        manifest[0]["symbol"] = "AAPL"
    elif manifest_fault == "instrument":
        manifest[0]["instrument_id"] = str(UUID(int=3))
    elif manifest_fault == "missing_candidate":
        manifest.pop()
    elif manifest_fault == "missing_evidence_field":
        manifest[0].pop("decision_artifact_id")
    batch_ids = [item.instrument_id for item in protocol_request().instruments]
    if batch_fault:
        batch_ids[-1] = UUID(int=3)
    async with factory.begin() as s:
        s.add(
            UniverseSnapshot(
                id=universe_id,
                key="economic-schema-fixture",
                version="1",
                effective_from=slot["session"],
                source="schema-fixture",
                content_hash="e" * 64,
            )
        )
        await s.flush()
        s.add(
            AgentPaperCandidateBatchRecord(
                id=batch_id,
                account_id=identity["ia"],
                universe_snapshot_id=universe_id,
                candidate_instrument_ids=batch_ids,
                source="explicit_research_scope",
                batch_hash=_canonical_hash(
                    account_id=identity["ia"],
                    universe_snapshot_id=universe_id,
                    instrument_ids=tuple(batch_ids),
                ),
            )
        )
        await s.flush()
        s.add(
            AgentPaperCycleRecord(
                id=cycle_id,
                account_id=identity["ia"],
                portfolio_id=identity["ip"],
                decision_session=slot["session"],
                decision_time=slot["decision"],
                decision_slot_at=slot["decision"],
                execution_session=slot["session"],
                universe_snapshot_id=universe_id,
                quality_report_id=None,
                candidate_batch_id=batch_id,
                decision_price_policy_id=UUID("4d24df4a-b510-541b-8a6a-fe7db5afaac4"),
                execution_price_policy_id=UUID("cf525f07-4e55-5f84-a839-60d845819ec5"),
                candidate_manifest=manifest,
                candidate_hash="a" * 64
                if manifest_fault == "hash"
                else _hash(sorted(manifest, key=lambda row: UUID(row["instrument_id"]).int)),
                sizing_policy_version="portfolio_allocation_v1",
                sizing_policy_hash="d7b719f47edbf3cb7ebb40dbe555a51f717ca581162a72d05c23391b8b083dd7",
                execution_locked=True,
            )
        )
    return factory, identity, slot, cycle_id


async def insert_lane(s, identity, slot, cycle_id):
    await s.execute(
        text("""INSERT INTO economic_lane_cycles
        (id,pair_slot_id,lane,account_id,portfolio_id,cycle_id)
        VALUES (:id,:slot,'incumbent',:account,:portfolio,:cycle)"""),
        {
            "id": uuid4(),
            "slot": slot["id"],
            "account": identity["ia"],
            "portfolio": identity["ip"],
            "cycle": cycle_id,
        },
    )


async def insert_cycle_job(s, cycle_id):
    from nexus.persistence.models.runtime import Job

    now = datetime.now(UTC)
    s.add(
        Job(
            id=uuid4(),
            job_key=str(uuid4()),
            input_hash="a" * 64,
            status="queued",
            schema_version="economic_schema_test_v1",
            payload={"cycle_id": str(cycle_id)},
            job_type="schema.fixture",
            queue="general",
            scope_kind="global",
            scope_id="fixture",
            effective_at=now,
            priority=0,
            retry_policy="never",
            max_attempts=1,
            available_at=now,
            started_at=None,
            finished_at=None,
        )
    )
    await s.flush()


@pytest.mark.asyncio
async def test_economic_job_requires_prior_lane_binding(migrated_database_url):
    factory, identity, slot, cycle_id = await seed_economic_cycle(migrated_database_url)
    async with factory.begin() as s:
        with pytest.raises(DBAPIError, match="ECONOMIC_PAIR_CYCLE_NOT_BOUND"):
            async with s.begin_nested():
                await insert_cycle_job(s, cycle_id)
        assert await s.scalar(text("SELECT count(*) FROM jobs")) == 0
        await insert_lane(s, identity, slot, cycle_id)
        await insert_cycle_job(s, cycle_id)
    async with factory.begin() as s:
        assert await s.scalar(text("SELECT count(*) FROM economic_lane_cycles")) == 1
        assert await s.scalar(text("SELECT count(*) FROM jobs")) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("producer", ["job", "research_run"])
@pytest.mark.parametrize("commit_binding", [True, False])
async def test_lane_binding_and_job_share_cycle_row_lock(
    migrated_database_url, producer, commit_binding
):
    factory, identity, slot, cycle_id = await seed_economic_cycle(migrated_database_url)
    kwargs = (
        await seed_research_dependencies(factory, cycle_id) if producer == "research_run" else None
    )
    async with factory() as writer, factory() as observer:
        await writer.begin()
        await insert_lane(writer, identity, slot, cycle_id)
        writer_pid = await writer.scalar(text("SELECT pg_backend_pid()"))
        ready = asyncio.Event()
        job_pid = None

        async def create_job():
            nonlocal job_pid
            async with factory.begin() as s:
                job_pid = await s.scalar(text("SELECT pg_backend_pid()"))
                ready.set()
                if kwargs is None:
                    await insert_cycle_job(s, cycle_id)
                else:
                    from nexus.persistence.models.agents import AgentResearchRunRecord

                    s.add(AgentResearchRunRecord(id=uuid4(), **kwargs))
                    await s.flush()

        pending = asyncio.create_task(create_job())
        try:
            await ready.wait()
            blocked = False
            async with asyncio.timeout(10):
                while not pending.done():
                    blockers = await observer.scalar(
                        text("SELECT pg_blocking_pids(:pid)"), {"pid": job_pid}
                    )
                    if writer_pid in blockers:
                        blocked = True
                        break
                    await asyncio.sleep(0.05)
            assert blocked, "Job insert bypassed uncommitted lane binding"
            if commit_binding:
                await writer.commit()
                await pending
            else:
                await writer.rollback()
                with pytest.raises(DBAPIError, match="ECONOMIC_PAIR_CYCLE_NOT_BOUND"):
                    await pending
        finally:
            await writer.rollback()
            if not pending.done():
                pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
    async with factory.begin() as s:
        assert await s.scalar(text("SELECT count(*) FROM economic_lane_cycles")) == int(
            commit_binding
        )
        assert await s.scalar(text("SELECT count(*) FROM jobs")) == (
            1 if commit_binding and producer == "job" else 0
        )
        assert await s.scalar(text("SELECT count(*) FROM agent_research_runs")) == (
            1 if commit_binding and producer == "research_run" else 0
        )


async def seed_research_dependencies(factory, cycle_id, *, uppercase=False):
    """Frozen schema data; does not claim a valid research admission or model call."""
    from datetime import timedelta

    from nexus.persistence.models.agents import AgentModelConfigRecord, AgentPromptVersionRecord
    from nexus.persistence.models.data import DataSnapshot
    from nexus.persistence.models.market import Instrument

    snapshot, prompt, model = (uuid4() for _ in range(3))
    now = datetime.now(UTC)
    async with factory.begin() as s:
        instrument = protocol_request().instruments[0].instrument_id
        if await s.scalar(select(Instrument.id).where(Instrument.id == instrument)) is None:
            s.add(
                Instrument(
                    id=instrument,
                    asset_type="stock",
                    exchange="NASDAQ",
                    currency="USD",
                    risk_bucket="schema-fixture",
                    tick_size=Decimal("0.01"),
                )
            )
        s.add(
            DataSnapshot(
                id=snapshot,
                source="schema-fixture",
                provider_object_id=str(snapshot),
                revision="1",
                observed_at=now,
                available_at=now,
                received_at=now,
                schema_version="economic_schema_test_v1",
                payload={
                    "agent_paper_context": {
                        "cycle_id": str(cycle_id).upper() if uppercase else str(cycle_id)
                    }
                },
            )
        )
        s.add(
            AgentPromptVersionRecord(
                id=prompt,
                role="orchestrator",
                version=str(prompt),
                content="schema fixture",
                content_hash="a" * 64,
            )
        )
        s.add(
            AgentModelConfigRecord(
                id=model,
                model_id="schema-fixture",
                version=str(model),
                config_hash="a" * 64,
                max_input_tokens=1,
                max_tool_calls=0,
                max_output_tokens=1,
                input_usd_per_million=0,
                output_usd_per_million=0,
                pricing_available_at=now,
                pricing_expires_at=now + timedelta(days=1),
            )
        )
    return dict(
        instrument_id=instrument,
        data_snapshot_id=snapshot,
        prompt_version_id=prompt,
        model_config_id=model,
        decision_time=now,
        actor_id="schema-fixture",
        idempotency_key=str(uuid4()),
        status="prepared",
        retry_count=0,
        schema_hash="a" * 64,
        model_hash="a" * 64,
        prompt_hash="a" * 64,
        prompt_manifest={},
        evidence_hash="a" * 64,
        evidence_manifest=[{"schema_fixture": True}],
        tool_hash="a" * 64,
        input_hash="a" * 64,
        reserved_cost_usd=0,
        settled_cost_usd=None,
        pricing_version=str(model),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("uppercase", [False, True])
async def test_native_research_run_and_run_id_job_require_bound_cycle(
    migrated_database_url, uppercase
):
    from nexus.persistence.models.agents import AgentResearchRunRecord
    from nexus.persistence.models.runtime import Job

    factory, identity, slot, cycle_id = await seed_economic_cycle(migrated_database_url)
    kwargs = await seed_research_dependencies(factory, cycle_id, uppercase=uppercase)
    async with factory.begin() as s:
        with pytest.raises(DBAPIError, match="ECONOMIC_PAIR_CYCLE_NOT_BOUND"):
            async with s.begin_nested():
                s.add(AgentResearchRunRecord(id=uuid4(), **kwargs))
                await s.flush()
        assert await s.scalar(text("SELECT count(*) FROM agent_research_runs")) == 0
        await insert_lane(s, identity, slot, cycle_id)
        run = AgentResearchRunRecord(id=uuid4(), **kwargs)
        s.add(run)
        await s.flush()
        await insert_cycle_job(s, cycle_id)
        # Test the actual native Job shape, which carries only run_id.
        # Identity changes are prohibited by the ordinary Job guard, so use a new Job.
        s.add(
            Job(
                id=uuid4(),
                job_key=str(uuid4()),
                input_hash="b" * 64,
                status="queued",
                schema_version="economic_schema_test_v1",
                payload={"run_id": str(run.id)},
                job_type="agent.paper.research",
                queue="general",
                scope_kind="global",
                scope_id="fixture",
                effective_at=kwargs["decision_time"],
                priority=0,
                retry_policy="never",
                max_attempts=1,
                available_at=kwargs["decision_time"],
                started_at=None,
                finished_at=None,
            )
        )
        await s.flush()
    async with factory.begin() as s:
        assert await s.scalar(text("SELECT count(*) FROM agent_research_runs")) == 1
        assert await s.scalar(text("SELECT count(*) FROM jobs")) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "table,column",
    [
        ("agent_portfolio_synthesis_runs", "cycle_id"),
        ("strategy_decisions", "agent_paper_cycle_id"),
    ],
)
async def test_other_native_producers_reject_unbound_economic_cycle(
    migrated_database_url, table, column
):
    factory, _, _, cycle_id = await seed_economic_cycle(migrated_database_url)
    async with factory.begin() as s:
        with pytest.raises(DBAPIError, match="ECONOMIC_PAIR_CYCLE_NOT_BOUND"):
            async with s.begin_nested():
                await s.execute(
                    text(f"INSERT INTO {table}(id,{column}) VALUES (:id,:cycle)"),
                    {"id": uuid4(), "cycle": cycle_id},
                )
        assert await s.scalar(text(f"SELECT count(*) FROM {table}")) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("job_type", ["agent.paper.research", "agent.portfolio_synthesis"])
async def test_native_job_rejects_unresolved_run_reference(migrated_database_url, job_type):
    from nexus.persistence.models.runtime import Job

    factory, _, _, _ = await seed_economic_cycle(migrated_database_url)
    now = datetime.now(UTC)
    async with factory.begin() as s:
        with pytest.raises(DBAPIError, match="ECONOMIC_PAIR_JOB_RUN_UNAVAILABLE"):
            async with s.begin_nested():
                s.add(
                    Job(
                        id=uuid4(),
                        job_key=str(uuid4()),
                        input_hash="a" * 64,
                        status="queued",
                        schema_version="economic_schema_test_v1",
                        payload={"run_id": str(uuid4())},
                        job_type=job_type,
                        queue="general",
                        scope_kind="global",
                        scope_id="fixture",
                        effective_at=now,
                        priority=0,
                        retry_policy="never",
                        max_attempts=1,
                        available_at=now,
                        started_at=None,
                        finished_at=None,
                    )
                )
                await s.flush()
        assert await s.scalar(text("SELECT count(*) FROM jobs")) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "protocol_fault", ["schema_version", "status", "economic_status", "execution_locked"]
)
async def test_identity_rejects_invalid_protocol_literals(migrated_database_url, protocol_fault):
    with pytest.raises(DBAPIError, match="ECONOMIC_PAIR_IDENTITY_INVALID"):
        await seed_identity(migrated_database_url, protocol_fault=protocol_fault)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "binding_fault", ["partial", "actor", "content_hash", "same_prompt", "model_hash", "expired"]
)
async def test_identity_rejects_invalid_version_binding(migrated_database_url, binding_fault):
    with pytest.raises(DBAPIError, match="ECONOMIC_PAIR_IDENTITY_INVALID"):
        await seed_identity(migrated_database_url, binding_fault=binding_fault)
    factory = session_factory(settings(migrated_database_url))
    async with factory.begin() as s:
        assert await s.scalar(text("SELECT count(*) FROM economic_evaluations")) == 0
        assert await s.scalar(text("SELECT count(*) FROM jobs")) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "manifest_fault",
    ["symbol", "instrument", "missing_candidate", "missing_evidence_field", "hash"],
)
async def test_lane_rejects_cycle_manifest_outside_frozen_protocol(
    migrated_database_url, manifest_fault
):
    factory, identity, slot, cycle_id = await seed_economic_cycle(
        migrated_database_url, manifest_fault=manifest_fault
    )
    async with factory.begin() as s:
        with pytest.raises(DBAPIError, match="ECONOMIC_PAIR_CYCLE_INVALID"):
            async with s.begin_nested():
                await insert_lane(s, identity, slot, cycle_id)
        assert await s.scalar(text("SELECT count(*) FROM economic_lane_cycles")) == 0
        assert await s.scalar(text("SELECT count(*) FROM jobs")) == 0


@pytest.mark.asyncio
async def test_lane_rejects_batch_with_different_candidate_identity(migrated_database_url):
    factory, identity, slot, cycle_id = await seed_economic_cycle(
        migrated_database_url, batch_fault=True
    )
    async with factory.begin() as s:
        with pytest.raises(DBAPIError, match="ECONOMIC_PAIR_CYCLE_INVALID"):
            async with s.begin_nested():
                await insert_lane(s, identity, slot, cycle_id)
        assert await s.scalar(text("SELECT count(*) FROM economic_lane_cycles")) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reverse_protocol,reverse_manifest", [(True, False), (False, True), (True, True)]
)
async def test_lane_matches_same_candidates_in_existing_canonical_order(
    migrated_database_url, reverse_protocol, reverse_manifest
):
    factory, identity, slot, cycle_id = await seed_economic_cycle(
        migrated_database_url,
        reverse_protocol=reverse_protocol,
        manifest_fault="order" if reverse_manifest else None,
    )
    async with factory.begin() as s:
        await insert_lane(s, identity, slot, cycle_id)
        assert await s.scalar(text("SELECT count(*) FROM economic_lane_cycles")) == 1
