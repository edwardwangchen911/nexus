"""Real PostgreSQL regressions for additive portfolio-management facts."""

import asyncio
from datetime import date
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from helpers.paper_batch import PAPER_ACCOUNT_ID, PORTFOLIO_ID, UNIVERSE_ID, seed_paper_batch
from nexus.paper.portfolio_policy import PORTFOLIO_ALLOCATION_POLICY_HASH
from nexus.persistence.models.paper import RiskQueryAuthorityRecord
from sqlalchemy import inspect, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine

NEW_TABLES = {
    "agent_paper_candidate_batches",
    "agent_portfolio_synthesis_runs",
    "agent_portfolio_synthesis_outputs",
    "agent_portfolio_synthesis_budget_reservations",
    "paper_initial_target_approvals",
    "paper_target_versions",
}
LINKS = {
    "agent_paper_cycles": "candidate_batch_id",
    "strategy_decisions": "portfolio_synthesis_output_id",
    "paper_order_batches": "target_version_id",
}


async def _execute(url: str, sql: str, **values: object) -> None:
    engine = create_async_engine(url)
    try:
        async with engine.begin() as connection:
            await connection.execute(text(sql), values)
    finally:
        await engine.dispose()


async def _batch(url: str, candidates: list[UUID | None] | None = None) -> UUID:
    batch_id = uuid4()
    await _execute(
        url,
        "INSERT INTO agent_paper_candidate_batches "
        "(id,account_id,universe_snapshot_id,candidate_instrument_ids,source,batch_hash) "
        "VALUES (:id,:account,:universe,:candidates,'explicit_research_scope',:hash)",
        id=batch_id,
        account=PAPER_ACCOUNT_ID,
        universe=UNIVERSE_ID,
        candidates=[UUID(int=1)] if candidates is None else candidates,
        hash=batch_id.hex * 2,
    )
    return batch_id


async def _approval(url: str, **overrides: object) -> UUID:
    values = dict(
        id=uuid4(),
        account=PAPER_ACCOUNT_ID,
        portfolio=PORTFOLIO_ID,
        hash="a" * 64,
        policy_hash=PORTFOLIO_ALLOCATION_POLICY_HASH,
    )
    values.update(overrides)
    await _execute(
        url,
        "INSERT INTO paper_initial_target_approvals "
        "(id,account_id,portfolio_id,projection_as_of,projection_hash,"
        "allocation_policy_version,allocation_policy_hash,approved_by,approved_at,approval_hash) "
        "VALUES (:id,:account,:portfolio,'2026-08-26T21:00:00Z',:hash,"
        "'portfolio_allocation_v1',:policy_hash,'fixture','2026-08-26T21:01:00Z',:hash)",
        **values,
    )
    return values["id"]


async def _target(url: str, decision_id: UUID, **overrides: object) -> UUID:
    values = dict(
        id=uuid4(),
        account=PAPER_ACCOUNT_ID,
        portfolio=PORTFOLIO_ID,
        seq=1,
        decision=decision_id,
        previous=None,
        hash="a" * 64,
    )
    values.update(overrides)
    await _execute(
        url,
        "INSERT INTO paper_target_versions "
        "(id,account_id,portfolio_id,version_seq,strategy_decision_id,"
        "previous_target_version_id,target_content_hash,activated_at,activation_hash) "
        "VALUES (:id,:account,:portfolio,:seq,:decision,:previous,:hash,"
        "'2026-08-26T21:01:00Z',:hash)",
        **values,
    )
    return values["id"]


async def _clone_decision(url: str, decision_id: UUID, **changes: object) -> UUID:
    """Copy a valid synthetic decision while changing only the guard under test."""
    new_id = uuid4()
    changes = {
        "id": new_id,
        "decision_hash": new_id.hex * 2,
        "decision_session": "2026-08-25",
        "cash_weight": Decimal("1"),
        **changes,
    }
    for key, value in changes.items():
        if key.endswith("session") and isinstance(value, str):
            changes[key] = date.fromisoformat(value)
    engine = create_async_engine(url)
    try:
        async with engine.begin() as connection:
            columns = await connection.run_sync(
                lambda c: [x["name"] for x in inspect(c).get_columns("strategy_decisions")]
            )
            select = [f":{c}" if c in changes else c for c in columns]
            await connection.execute(
                text(
                    f"INSERT INTO strategy_decisions ({','.join(columns)}) "
                    f"SELECT {','.join(select)} FROM strategy_decisions WHERE id=:original"
                ),
                {**changes, "original": decision_id},
            )
    finally:
        await engine.dispose()
    return new_id


async def _cycle(url: str, batch_id: UUID | None) -> UUID:
    cycle_id = uuid4()
    await _execute(
        url,
        "INSERT INTO agent_paper_cycles "
        "(id,account_id,portfolio_id,decision_session,decision_time,decision_slot_at,"
        "execution_session,universe_snapshot_id,decision_price_policy_id,"
        "execution_price_policy_id,candidate_manifest,candidate_hash,sizing_policy_version,"
        "sizing_policy_hash,execution_locked,candidate_batch_id) VALUES "
        "(:id,:account,:portfolio,'2026-08-26','2026-08-26T21:00:00Z','2026-08-26T21:00:00Z',"
        "'2026-08-27',:universe,'2573d63a-fb6d-53d3-8d44-207766a78322',"
        "'e99532cc-e24c-52a8-8165-c8076325fbd2','[]',repeat('a',64),:policy,:hash,true,:batch)",
        id=cycle_id,
        account=PAPER_ACCOUNT_ID,
        portfolio=PORTFOLIO_ID,
        universe=UNIVERSE_ID,
        policy="agent_paper_sizing_v1" if batch_id is None else "portfolio_allocation_v1",
        hash="a" * 64 if batch_id is None else PORTFOLIO_ALLOCATION_POLICY_HASH,
        batch=batch_id,
    )
    return cycle_id


async def _synthesis(url: str, *, status: str = "completed") -> tuple[UUID, UUID, UUID]:
    batch = await _batch(url)
    cycle = await _cycle(url, batch)
    prompt, model, run, output = (uuid4() for _ in range(4))
    await _execute(
        url,
        "INSERT INTO agent_prompt_versions (id,role,version,content_hash,content) "
        "VALUES (:id,'portfolio','portfolio-test',repeat('a',64),'fixture')",
        id=prompt,
    )
    await _execute(
        url,
        "INSERT INTO agent_model_configs (id,model_id,version,config_hash,max_input_tokens,"
        "max_tool_calls,max_output_tokens,input_usd_per_million,output_usd_per_million,"
        "pricing_available_at,pricing_expires_at) VALUES "
        "(:id,'fixture','portfolio-test',repeat('b',64),10,0,10,0,0,"
        "'2026-01-01T00:00:00Z','2027-01-01T00:00:00Z')",
        id=model,
    )
    await _execute(
        url,
        "INSERT INTO agent_portfolio_synthesis_runs "
        "(id,cycle_id,candidate_batch_id,research_output_ids,projection_hash,"
        "prompt_version_id,model_config_id,prompt_hash,model_hash,input_hash,"
        "frozen_input,idempotency_key,status,pricing_version,decision_time) VALUES "
        "(:id,:cycle,:batch,:outputs,repeat('a',64),:prompt,:model,repeat('a',64),"
        "repeat('b',64),repeat('c',64),'{}',:key,'prepared','fixture',"
        "'2026-08-26T21:00:00Z')",
        id=run,
        cycle=cycle,
        batch=batch,
        outputs=[uuid4()],
        prompt=prompt,
        model=model,
        key=str(run),
    )
    if status != "prepared":
        await _execute(url, "UPDATE agent_portfolio_synthesis_runs SET status='running'")
        await _execute(
            url, "UPDATE agent_portfolio_synthesis_runs SET status=:status", status=status
        )
        await _execute(
            url,
            "INSERT INTO agent_portfolio_synthesis_outputs "
            "(id,run_id,status,intent,content_hash,binding_hash) VALUES "
            "(:id,:run,:status,CAST(:intent AS jsonb),repeat('a',64),repeat('b',64))",
            id=output,
            run=run,
            status=status,
            intent="{}" if status == "completed" else None,
        )
    return cycle, run, output


async def _proposal(url: str, original: UUID, cycle: UUID, output: UUID) -> UUID:
    return await _clone_decision(
        url,
        original,
        decision_origin="agent_research",
        strategy_id="agent_research",
        strategy_version="agent_portfolio_proposal_v1",
        strategy_definition_id=None,
        registry_event_id=None,
        champion_generation=None,
        agent_paper_cycle_id=cycle,
        portfolio_synthesis_output_id=output,
        decision_session="2026-08-26",
        source_policy_id=UUID("2573d63a-fb6d-53d3-8d44-207766a78322"),
        execution_source_policy_id=UUID("e99532cc-e24c-52a8-8165-c8076325fbd2"),
        quality_report_id=None,
        reason="AGENT_RESEARCH",
        portfolio_projection_hash="a" * 64,
        strategy_parameters_hash=PORTFOLIO_ALLOCATION_POLICY_HASH,
    )


async def _legacy_agent_decision(url: str, original: UUID, cycle: UUID) -> UUID:
    return await _clone_decision(
        url,
        original,
        decision_origin="agent_research",
        strategy_id="agent_research",
        strategy_version="agent_paper_decision_v1",
        strategy_definition_id=None,
        registry_event_id=None,
        champion_generation=None,
        agent_paper_cycle_id=cycle,
        portfolio_synthesis_output_id=None,
        decision_session="2026-08-26",
        source_policy_id=UUID("2573d63a-fb6d-53d3-8d44-207766a78322"),
        execution_source_policy_id=UUID("e99532cc-e24c-52a8-8165-c8076325fbd2"),
        quality_report_id=None,
        reason="AGENT_RESEARCH",
    )


@pytest.mark.asyncio
async def test_0065_adds_portfolio_management_facts(migrated_database_url: str) -> None:
    """Missing tables or non-null legacy links prevent additive rollout."""
    engine = create_async_engine(migrated_database_url)
    try:
        async with engine.connect() as connection:
            risk_constraints = await connection.run_sync(
                lambda c: inspect(c).get_unique_constraints("risk_query_authorities")
            )
            tables, columns = await connection.run_sync(
                lambda connection: (
                    set(inspect(connection).get_table_names()),
                    {
                        table: {
                            column["name"]: column
                            for column in inspect(connection).get_columns(table)
                        }
                        for table in LINKS
                    },
                )
            )
        assert NEW_TABLES <= tables
        expected_risk_key = [
            "source_policy_id",
            "universe_snapshot_id",
            "decision_session",
            "kind",
            "available_at",
            "required_instrument_ids",
        ]
        assert (
            next(
                c["column_names"]
                for c in risk_constraints
                if c["name"] == "uq_risk_query_authorities_scope_available"
            )
            == expected_risk_key
        )
        assert (
            list(
                next(
                    c
                    for c in RiskQueryAuthorityRecord.__table__.constraints
                    if c.name == "uq_risk_query_authorities_scope_available"
                ).columns.keys()
            )
            == expected_risk_key
        )
        for table, column in LINKS.items():
            assert columns[table][column]["nullable"] is True
    finally:
        await engine.dispose()


def test_0065_revision_upgrades_and_empty_downgrade_preserves_0064(
    temporary_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The new revision must be independently deployable and reversible when empty."""
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    command.upgrade(config, "0065_portfolio_management")
    command.downgrade(config, "0064_prompt_release_channel")
    command.upgrade(config, "0065_portfolio_management")


@pytest.mark.parametrize(
    "candidates",
    [[], [UUID(int=1)] * 2, [UUID(int=i) for i in range(1, 7)], [None], [UUID(int=2), UUID(int=1)]],
)
@pytest.mark.asyncio
async def test_candidate_array_rejects_empty_duplicate_overbound_null_or_unsorted_ids(
    migrated_database_url: str,
    candidates: list[UUID | None],
) -> None:
    await seed_paper_batch(migrated_database_url)
    with pytest.raises(IntegrityError):
        await _batch(migrated_database_url, candidates)


@pytest.mark.parametrize("count", [1, 5])
@pytest.mark.asyncio
async def test_candidate_array_accepts_both_inclusive_bounds(
    migrated_database_url: str,
    count: int,
) -> None:
    await seed_paper_batch(migrated_database_url)
    await _batch(migrated_database_url, [UUID(int=i) for i in range(1, count + 1)])


@pytest.mark.asyncio
async def test_initial_approval_cannot_be_replaced_or_duplicated(
    migrated_database_url: str,
) -> None:
    await _approval(migrated_database_url)
    with pytest.raises(DBAPIError):
        await _approval(migrated_database_url)
    for sql in (
        "UPDATE paper_initial_target_approvals SET approved_by='changed'",
        "DELETE FROM paper_initial_target_approvals",
    ):
        with pytest.raises(DBAPIError):
            await _execute(migrated_database_url, sql)


@pytest.mark.asyncio
async def test_target_sequence_and_decision_activation_are_unique(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    await _target(migrated_database_url, seed.decision_id)
    with pytest.raises(DBAPIError):
        await _target(migrated_database_url, seed.decision_id, seq=2)
    other = await _clone_decision(migrated_database_url, seed.decision_id)
    with pytest.raises(DBAPIError):
        await _target(migrated_database_url, other)


@pytest.mark.asyncio
async def test_target_allows_return_to_old_content_but_not_a_previous_fork(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    first = await _target(migrated_database_url, seed.decision_id)
    second_decision = await _clone_decision(migrated_database_url, seed.decision_id)
    second = await _target(
        migrated_database_url, second_decision, seq=2, previous=first, hash="b" * 64
    )
    third_decision = await _clone_decision(
        migrated_database_url, seed.decision_id, decision_session="2026-08-24"
    )
    await _target(migrated_database_url, third_decision, seq=3, previous=second)
    fourth_decision = await _clone_decision(
        migrated_database_url, seed.decision_id, decision_session="2026-08-23"
    )
    with pytest.raises(DBAPIError):
        await _target(migrated_database_url, fourth_decision, seq=4, previous=first)


@pytest.mark.parametrize("table", ["agent_paper_candidate_batches", "paper_target_versions"])
@pytest.mark.asyncio
async def test_portfolio_facts_reject_update_and_delete(
    migrated_database_url: str,
    table: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    await _batch(migrated_database_url)
    await _target(migrated_database_url, seed.decision_id)
    for sql in (f"UPDATE {table} SET id=id", f"DELETE FROM {table}"):
        with pytest.raises(DBAPIError):
            await _execute(migrated_database_url, sql)


def test_downgrade_refuses_new_facts_without_erasing_them(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", migrated_database_url)
    asyncio.run(_approval(migrated_database_url))
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    with pytest.raises(DBAPIError, match="portfolio management facts prevent downgrade"):
        command.downgrade(config, "0064_prompt_release_channel")


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["completed", "abstained", "failed"])
async def test_proposal_requires_completed_synthesis_from_its_own_cycle(
    migrated_database_url: str,
    status: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    cycle, _, output = await _synthesis(migrated_database_url, status=status)
    if status == "completed":
        await _proposal(migrated_database_url, seed.decision_id, cycle, output)
    else:
        with pytest.raises(IntegrityError, match="completed bound synthesis"):
            await _proposal(migrated_database_url, seed.decision_id, cycle, output)


@pytest.mark.asyncio
async def test_new_cycle_rejects_legacy_decision_without_synthesis(
    migrated_database_url: str,
) -> None:
    """A legacy decision must not occupy a new portfolio cycle's only decision slot."""
    seed = await seed_paper_batch(migrated_database_url)
    batch = await _batch(migrated_database_url)
    cycle = await _cycle(migrated_database_url, batch)
    with pytest.raises(IntegrityError, match="cycle decision identity mismatch"):
        await _legacy_agent_decision(migrated_database_url, seed.decision_id, cycle)


@pytest.mark.asyncio
async def test_legacy_cycle_accepts_legacy_decision_without_synthesis(
    migrated_database_url: str,
) -> None:
    """Binding new cycles must preserve the historical Agent decision insertion path."""
    seed = await seed_paper_batch(migrated_database_url)
    cycle = await _cycle(migrated_database_url, None)
    decision = await _legacy_agent_decision(migrated_database_url, seed.decision_id, cycle)
    engine = create_async_engine(migrated_database_url)
    try:
        async with engine.connect() as connection:
            result = await connection.execute(
                text(
                    "SELECT strategy_version,portfolio_synthesis_output_id "
                    "FROM strategy_decisions WHERE id=:id"
                ),
                {"id": decision},
            )
            assert result.one() == ("agent_paper_decision_v1", None)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_synthesis_lifecycle_freezes_authority_and_terminal_usage(
    migrated_database_url: str,
) -> None:
    await seed_paper_batch(migrated_database_url)
    _, run, _ = await _synthesis(migrated_database_url, status="prepared")
    for sql in (
        "UPDATE agent_portfolio_synthesis_runs SET input_hash=repeat('f',64)",
        "UPDATE agent_portfolio_synthesis_runs SET status='completed'",
        "DELETE FROM agent_portfolio_synthesis_runs",
    ):
        with pytest.raises(IntegrityError):
            await _execute(migrated_database_url, sql)
    await _execute(
        migrated_database_url,
        "INSERT INTO agent_portfolio_synthesis_budget_reservations "
        "(id,business_date,run_id,pricing_version,reserved_usd,state) "
        "VALUES (:id,'2026-08-26',:run,'fixture',1,'reserved')",
        id=uuid4(),
        run=run,
    )
    with pytest.raises(IntegrityError):
        await _execute(
            migrated_database_url,
            "UPDATE agent_portfolio_synthesis_budget_reservations SET reserved_usd=2",
        )
    await _execute(
        migrated_database_url,
        "UPDATE agent_portfolio_synthesis_budget_reservations SET state='settled',settled_usd=0.5",
    )
    for sql in (
        "UPDATE agent_portfolio_synthesis_budget_reservations SET settled_usd=0.4",
        "DELETE FROM agent_portfolio_synthesis_budget_reservations",
    ):
        with pytest.raises(IntegrityError):
            await _execute(migrated_database_url, sql)
    await _execute(
        migrated_database_url, "UPDATE agent_portfolio_synthesis_runs SET status='running'"
    )
    await _execute(
        migrated_database_url, "UPDATE agent_portfolio_synthesis_runs SET status='completed'"
    )
    with pytest.raises(IntegrityError):
        await _execute(
            migrated_database_url, "UPDATE agent_portfolio_synthesis_runs SET input_tokens=0"
        )


@pytest.mark.asyncio
async def test_legacy_cycle_decision_and_batch_keep_null_new_links(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    cycle = await _cycle(migrated_database_url, None)
    idempotency, batch = uuid4(), uuid4()
    await _execute(
        migrated_database_url,
        "INSERT INTO idempotency_records "
        "(id,actor_id,route,idempotency_key,request_hash,schema_version,response_payload) "
        "VALUES (:id,'fixture','/paper','legacy-null',repeat('a',64),'v1','{}')",
        id=idempotency,
    )
    await _execute(
        migrated_database_url,
        "INSERT INTO paper_order_batches "
        "(id,account_id,strategy_decision_id,risk_policy_id,idempotency_record_id,request_hash,"
        "projection_hash,risk_input_hash,result_hash,batch_hash,evaluated_at) VALUES "
        "(:id,:account,:decision,'2a3bf2d3-85c2-524b-abe3-d37112685caf',:idem,"
        "repeat('a',64),repeat('b',64),repeat('c',64),repeat('d',64),repeat('e',64),"
        "'2026-08-26T21:01:00Z')",
        id=batch,
        account=PAPER_ACCOUNT_ID,
        decision=seed.decision_id,
        idem=idempotency,
    )
    engine = create_async_engine(migrated_database_url)
    try:
        async with engine.connect() as connection:
            for table, column in LINKS.items():
                assert await connection.scalar(text(f"SELECT {column} FROM {table}")) is None
            assert await connection.scalar(text("SELECT id FROM agent_paper_cycles")) == cycle
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_previous_target_cannot_cross_accounts(migrated_database_url: str) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    first = await _target(migrated_database_url, seed.decision_id)
    account, portfolio, paper = (uuid4() for _ in range(3))
    await _execute(
        migrated_database_url,
        "INSERT INTO accounts (id,provider,status) VALUES (:id,'paper','ACTIVE')",
        id=account,
    )
    await _execute(
        migrated_database_url,
        "INSERT INTO portfolios (id,account_id,name) VALUES (:id,:account,'isolated fixture')",
        id=portfolio,
        account=account,
    )
    await _execute(
        migrated_database_url,
        "INSERT INTO paper_accounts (id,account_id,portfolio_id,code,currency,account_model,"
        "position_model,initial_capital,capability) VALUES "
        "(:id,:account,:portfolio,'fixture-other','USD','cash','long_only',10000,"
        "'simulation_write_internal')",
        id=paper,
        account=account,
        portfolio=portfolio,
    )
    other_decision = await _clone_decision(
        migrated_database_url, seed.decision_id, account_id=paper, portfolio_id=portfolio
    )
    with pytest.raises(IntegrityError, match="fk_paper_target_versions_previous"):
        await _target(
            migrated_database_url,
            other_decision,
            account=paper,
            portfolio=portfolio,
            seq=2,
            previous=first,
        )
    await _target(migrated_database_url, other_decision, account=paper, portfolio=portfolio)


@pytest.mark.asyncio
async def test_proposal_rejects_completed_output_from_another_cycle(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    cycle, _, output = await _synthesis(migrated_database_url)
    other_cycle = uuid4()
    await _execute(
        migrated_database_url,
        "INSERT INTO agent_paper_cycles "
        "SELECT :id,account_id,portfolio_id,decision_session,decision_time,"
        "execution_session,universe_snapshot_id,quality_report_id,decision_price_policy_id,"
        "execution_price_policy_id,candidate_manifest,candidate_hash,sizing_policy_version,"
        "sizing_policy_hash,execution_locked,created_at,decision_slot_at + interval '5 minutes',"
        "candidate_batch_id FROM agent_paper_cycles WHERE id=:original",
        id=other_cycle,
        original=cycle,
    )
    with pytest.raises(IntegrityError, match="completed bound synthesis"):
        await _proposal(migrated_database_url, seed.decision_id, other_cycle, output)


@pytest.mark.asyncio
async def test_synthesis_outputs_cannot_be_rewritten_or_deleted(migrated_database_url: str) -> None:
    await seed_paper_batch(migrated_database_url)
    await _synthesis(migrated_database_url)
    for sql in (
        "UPDATE agent_portfolio_synthesis_outputs SET intent='{}'",
        "DELETE FROM agent_portfolio_synthesis_outputs",
    ):
        with pytest.raises(IntegrityError):
            await _execute(migrated_database_url, sql)


@pytest.mark.asyncio
@pytest.mark.parametrize("bad_hash", ["A" * 64, "a" * 63, "g" * 64])
async def test_approval_rejects_noncanonical_hashes(
    migrated_database_url: str,
    bad_hash: str,
) -> None:
    with pytest.raises(IntegrityError):
        await _approval(migrated_database_url, hash=bad_hash)


@pytest.mark.asyncio
async def test_released_budget_requires_an_explicit_zero_settlement(
    migrated_database_url: str,
) -> None:
    await seed_paper_batch(migrated_database_url)
    _, run, _ = await _synthesis(migrated_database_url, status="prepared")
    await _execute(
        migrated_database_url,
        "INSERT INTO agent_portfolio_synthesis_budget_reservations "
        "(id,business_date,run_id,pricing_version,reserved_usd,state) "
        "VALUES (:id,'2026-08-26',:run,'fixture',1,'reserved')",
        id=uuid4(),
        run=run,
    )
    with pytest.raises(IntegrityError):
        await _execute(
            migrated_database_url,
            "UPDATE agent_portfolio_synthesis_budget_reservations SET state='released'",
        )
    await _execute(
        migrated_database_url,
        "UPDATE agent_portfolio_synthesis_budget_reservations SET state='released',settled_usd=0",
    )
