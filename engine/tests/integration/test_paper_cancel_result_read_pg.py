from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest
from helpers.paper_api import app, headers, settings
from helpers.paper_execution import seed_paper_execution
from nexus.paper.execution_service import (
    advance_internal_paper_order,
    cancel_internal_paper_order,
)
from nexus.paper.service import PaperOrderBatchResult
from nexus.persistence.models.paper import (
    PaperOrderBatchRecord,
    PaperOrderRecord,
    PaperRequestIdempotencyRecord,
)
from nexus.persistence.models.runtime import IdempotencyRecord
from nexus.persistence.session import session_factory
from nexus.risk.canonical import canonical_paper_order_batch_hash
from sqlalchemy import select, text


@pytest.fixture(autouse=True)
def _historical_execution_authority_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "nexus.paper.execution_service._paper_execution_authority_available",
        lambda _source_policy_id: True,
    )


def _cancel_request_hash(*, order_id, expected_event_seq: int) -> str:
    payload = {
        "schema": "paper_cancel_request_sha256_v1",
        "expected_event_seq": expected_event_seq,
        "order_id": str(order_id),
    }
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    ).hexdigest()


async def _database_fingerprint(session) -> tuple[tuple[int, str], ...]:
    tables = (
        "idempotency_records",
        "paper_accounts",
        "agent_portfolio_synthesis_runs",
        "agent_portfolio_synthesis_outputs",
        "agent_portfolio_synthesis_budget_reservations",
        "strategy_decisions",
        "strategy_decision_agent_outputs",
        "strategy_decision_targets",
        "risk_policies",
        "risk_no_change_evaluations",
        "risk_evaluations",
        "risk_evaluation_rules",
        "risk_evaluation_price_inputs",
        "risk_evaluation_sector_inputs",
        "risk_evaluation_query_proofs",
        "paper_orders",
        "paper_order_events",
        "paper_execution_window_authorities",
        "paper_execution_window_members",
        "paper_fills",
        "paper_request_idempotency",
        "paper_reservations",
        "paper_reservation_events",
        "paper_ledger_accounts",
        "paper_ledger_transactions",
        "paper_ledger_entries",
        "paper_position_adjustment_events",
        "paper_position_snapshots",
        "paper_target_versions",
        "paper_order_batches",
    )
    values = []
    for table_name in tables:
        row = (
            await session.execute(
                text(
                    f"SELECT count(*), md5(coalesce(string_agg(to_jsonb(t)::text, '' "
                    f"ORDER BY to_jsonb(t)::text), '')) FROM {table_name} AS t"
                )
            )
        ).one()
        values.append((int(row[0]), str(row[1])))
    return tuple(values)


async def _seed_partial_order(database_url: str):
    seed = await seed_paper_execution(
        database_url,
        window_bar_volumes=(Decimal("40"),),
    )
    factory = session_factory(settings(database_url))
    async with factory() as session, session.begin():
        batch = await session.scalar(
            select(PaperOrderBatchRecord)
            .join(PaperOrderRecord, PaperOrderRecord.batch_id == PaperOrderBatchRecord.id)
            .where(PaperOrderRecord.id == seed.order_id)
        )
        assert batch is not None
        decision_hash = await session.scalar(
            text("SELECT decision_hash FROM strategy_decisions WHERE id=:decision_id"),
            {"decision_id": batch.strategy_decision_id},
        )
        idempotency = await session.get(IdempotencyRecord, batch.idempotency_record_id)
        assert decision_hash is not None and idempotency is not None
        stored = PaperOrderBatchResult.model_validate(idempotency.response_payload)
        batch_hash = canonical_paper_order_batch_hash(
            account_id=stored.account_id,
            decision_hash=decision_hash,
            risk_input_hash=stored.risk_input_hash,
            evaluations=stored.evaluations,
        )
        await session.execute(text("ALTER TABLE paper_order_batches DISABLE TRIGGER USER"))
        batch.batch_hash = batch_hash
        idempotency.response_payload = stored.model_copy(
            update={"decision_hash": decision_hash, "batch_hash": batch_hash}
        ).model_dump(mode="json")
        await session.flush()
        partial = await advance_internal_paper_order(
            session,
            order_id=seed.order_id,
            evaluated_at=seed.evaluation_times[0],
            idempotency_key="cancel-read-partial-fill",
        )
    assert partial.action == "partially_filled"
    return seed, factory, partial.result_event_seq


async def _corrupt_append_only_fact(session, *, table: str, statement: str, order_id) -> None:
    await session.execute(text(f"ALTER TABLE {table} DISABLE TRIGGER USER"))
    try:
        await session.execute(text(statement), {"order_id": order_id})
    finally:
        await session.execute(text(f"ALTER TABLE {table} ENABLE TRIGGER USER"))


async def _remove_one_risk_evaluation(session, *, batch_id) -> None:
    child_tables = (
        "risk_evaluation_rules",
        "risk_evaluation_price_inputs",
        "risk_evaluation_sector_inputs",
        "risk_evaluation_query_proofs",
    )
    for table in (*child_tables, "risk_evaluations"):
        await session.execute(text(f"ALTER TABLE {table} DISABLE TRIGGER USER"))
    try:
        risk_id = await session.scalar(
            text("SELECT id FROM risk_evaluations WHERE batch_id = :batch_id LIMIT 1"),
            {"batch_id": batch_id},
        )
        assert risk_id is not None
        for table in child_tables:
            await session.execute(
                text(f"DELETE FROM {table} WHERE risk_evaluation_id = :risk_id"),
                {"risk_id": risk_id},
            )
        await session.execute(
            text("DELETE FROM risk_evaluations WHERE id = :risk_id"),
            {"risk_id": risk_id},
        )
    finally:
        for table in reversed((*child_tables, "risk_evaluations")):
            await session.execute(text(f"ALTER TABLE {table} ENABLE TRIGGER USER"))


async def _insert_extra_risk_evaluation(session, *, batch_id, orphan: bool) -> None:
    await session.execute(text("ALTER TABLE risk_evaluations DISABLE TRIGGER USER"))
    await session.execute(
        text("ALTER TABLE risk_evaluations DROP CONSTRAINT uq_risk_evaluations_order")
    )
    if orphan:
        await session.execute(
            text("ALTER TABLE risk_evaluations DROP CONSTRAINT fk_risk_evaluations_order")
        )
    original_order_id = await session.scalar(
        text("SELECT order_id FROM risk_evaluations WHERE batch_id = :batch_id LIMIT 1"),
        {"batch_id": batch_id},
    )
    assert original_order_id is not None
    await session.execute(
        text(
            "INSERT INTO risk_evaluations ("
            "id, order_id, batch_id, account_id, risk_policy_id, outcome, "
            "desired_quantity, approved_quantity, target_weight, strategy_priority, "
            "reference_close, worst_price, reserve_amount, projected_available_cash, "
            "projected_cash_ratio, projected_name_ratio, projected_sector_ratios, "
            "input_hash, result_hash, created_at"
            ") SELECT :new_id, :order_id, batch_id, account_id, risk_policy_id, outcome, "
            "desired_quantity, approved_quantity, target_weight, strategy_priority, "
            "reference_close, worst_price, reserve_amount, projected_available_cash, "
            "projected_cash_ratio, projected_name_ratio, projected_sector_ratios, "
            "input_hash, result_hash, created_at "
            "FROM risk_evaluations WHERE batch_id = :batch_id LIMIT 1"
        ),
        {
            "new_id": uuid4(),
            "order_id": uuid4() if orphan else original_order_id,
            "batch_id": batch_id,
        },
    )
    await session.execute(text("ALTER TABLE risk_evaluations ENABLE TRIGGER USER"))


async def _seed_target_associated_plan(database_url: str, monkeypatch: pytest.MonkeyPatch):
    import integration.test_portfolio_synthesis_persistence_pg as seed_module
    from integration.test_paper_target_activation_pg import _include_first
    from integration.test_portfolio_proposal_pg import proposal_source
    from integration.test_portfolio_synthesis_worker_pg import _seed_normal_synthesis
    from nexus.paper.agent_decision_store import persist_portfolio_proposal
    from nexus.paper.target_service import activate_portfolio_target

    original_intent = seed_module._safe_no_trade_intent

    def complete_buy(**kwargs):
        source = original_intent(**kwargs)
        assessment = source.synthesis_assessment.model_copy(
            update={
                "stance": "buy_candidate",
                "review_conclusion": "sufficient",
                "reviews": tuple(
                    review.model_copy(
                        update={
                            "resolved": True,
                            "status": "accepted",
                            "missing_evidence": (),
                            "prevented_conclusion": None,
                        }
                    )
                    for review in source.synthesis_assessment.reviews
                ),
            }
        )
        return source.model_copy(
            update={
                "quality_status": "complete",
                "stance": "buy_candidate",
                "exposure_action": "eligible_for_increase",
                "evidence_audit": "complete",
                "synthesis_assessment": assessment,
                "role_votes": tuple(
                    vote.model_copy(update={"vote": "support"})
                    for vote in source.role_votes
                ),
            }
        )

    monkeypatch.setattr(seed_module, "_safe_no_trade_intent", complete_buy)
    seeded = await _seed_normal_synthesis(database_url, monkeypatch)
    engine, sessions, args = await proposal_source(
        database_url,
        seeded=seeded,
        intent_factory=_include_first,
    )
    async with sessions.begin() as session:
        proposal = await persist_portfolio_proposal(session, **args)
        activated = await activate_portfolio_target(
            session,
            account_id=proposal.account_id,
            strategy_decision_id=proposal.decision_id,
            request_time=args["now"] + timedelta(seconds=1),
            idempotency_key="task10-target-plan",
        )
    assert activated.batch_id is not None
    return engine, sessions, args, proposal, activated


async def _corrupt_target_proposal_authority(session, *, decision_id, corruption: str) -> None:
    async def drop_fk_to(table: str, referenced_table: str) -> None:
        constraint = await session.scalar(
            text(
                "SELECT conname FROM pg_constraint "
                "WHERE conrelid = CAST(:table AS regclass) AND contype = 'f' "
                "AND pg_get_constraintdef(oid) LIKE :reference"
            ),
            {"table": table, "reference": f"%{referenced_table}%"},
        )
        assert constraint is not None and constraint.replace("_", "").isalnum()
        await session.execute(text(f'ALTER TABLE {table} DROP CONSTRAINT "{constraint}"'))

    if corruption == "decision_hash":
        await _corrupt_append_only_fact(
            session,
            table="strategy_decisions",
            statement="UPDATE strategy_decisions SET decision_hash = repeat('a', 64) "
            "WHERE id = :order_id",
            order_id=decision_id,
        )
        return
    if corruption == "proposal":
        await _corrupt_append_only_fact(
            session,
            table="strategy_decisions",
            statement="UPDATE strategy_decisions SET cash_weight = cash_weight + 0.01 "
            "WHERE id = :order_id",
            order_id=decision_id,
        )
        return
    if corruption == "binding":
        await _corrupt_append_only_fact(
            session,
            table="strategy_decision_agent_outputs",
            statement="UPDATE strategy_decision_agent_outputs "
            "SET binding_hash = repeat('a', 64) "
            "WHERE strategy_decision_id = :order_id",
            order_id=decision_id,
        )
        return
    if corruption == "missing_output":
        await session.execute(
            text(
                "ALTER TABLE strategy_decisions DROP CONSTRAINT "
                "fk_strategy_decisions_portfolio_synthesis"
            )
        )
        await session.execute(
            text("ALTER TABLE agent_portfolio_synthesis_outputs DISABLE TRIGGER USER")
        )
        await session.execute(
            text(
                "DELETE FROM agent_portfolio_synthesis_outputs WHERE id = ("
                "SELECT portfolio_synthesis_output_id FROM strategy_decisions "
                "WHERE id = :decision_id)"
            ),
            {"decision_id": decision_id},
        )
        return
    if corruption == "missing_run":
        await drop_fk_to(
            "agent_portfolio_synthesis_outputs",
            "agent_portfolio_synthesis_runs",
        )
        await drop_fk_to(
            "agent_portfolio_synthesis_budget_reservations",
            "agent_portfolio_synthesis_runs",
        )
        for table in (
            "agent_portfolio_synthesis_budget_reservations",
            "agent_portfolio_synthesis_runs",
        ):
            await session.execute(text(f"ALTER TABLE {table} DISABLE TRIGGER USER"))
        run_id = await session.scalar(
            text(
                "SELECT output.run_id FROM strategy_decisions decision "
                "JOIN agent_portfolio_synthesis_outputs output "
                "ON output.id = decision.portfolio_synthesis_output_id "
                "WHERE decision.id = :decision_id"
            ),
            {"decision_id": decision_id},
        )
        assert run_id is not None
        await session.execute(
            text("DELETE FROM agent_portfolio_synthesis_budget_reservations WHERE run_id=:run_id"),
            {"run_id": run_id},
        )
        await session.execute(
            text("DELETE FROM agent_portfolio_synthesis_runs WHERE id=:run_id"),
            {"run_id": run_id},
        )
        return
    raise AssertionError(f"unsupported corruption: {corruption}")


@pytest.mark.asyncio
async def test_cancel_result_not_recorded_is_a_repeatable_pure_read(
    migrated_database_url: str,
) -> None:
    seed, factory, _ = await _seed_partial_order(migrated_database_url)
    path = f"/internal/v1/paper/orders/{seed.order_id}/cancel-result"
    request_headers = headers("GET", path)
    request_headers["Idempotency-Key"] = "cancel-read-not-recorded"
    async with factory() as session:
        before = await _database_fingerprint(session)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        first = await client.get(path, headers=request_headers)
        repeated_headers = headers("GET", path)
        repeated_headers["Idempotency-Key"] = "cancel-read-not-recorded"
        repeated = await client.get(path, headers=repeated_headers)
        invalid_path = f"{path}?idempotency_key=cancel-read-not-recorded"
        invalid = await client.get(
            invalid_path,
            headers=headers("GET", invalid_path) | {"Idempotency-Key": "cancel-read-not-recorded"},
        )
    async with factory() as session:
        after = await _database_fingerprint(session)

    assert first.status_code == repeated.status_code == 200
    assert first.json()["data"] == repeated.json()["data"]
    assert first.json()["data"]["status"] == "NOT_RECORDED"
    assert first.json()["data"]["order"]["status"] == "partially_filled"
    assert first.json()["data"]["reservation"]["released"] is False
    assert before == after
    assert invalid.status_code == 422


@pytest.mark.asyncio
async def test_cancel_result_distinguishes_committed_conflict_and_unknown(
    migrated_database_url: str,
) -> None:
    seed, factory, expected_event_seq = await _seed_partial_order(migrated_database_url)
    committed_key = "cancel-read-committed"
    async with factory() as session, session.begin():
        committed = await cancel_internal_paper_order(
            session,
            order_id=seed.order_id,
            expected_event_seq=expected_event_seq,
            requested_at=datetime(2026, 8, 27, 14, 0, tzinfo=UTC),
            idempotency_key=committed_key,
        )
    async with factory() as session:
        batch_id = await session.scalar(
            select(PaperOrderBatchRecord.id)
            .join(PaperOrderRecord, PaperOrderRecord.batch_id == PaperOrderBatchRecord.id)
            .where(PaperOrderRecord.id == seed.order_id)
        )
    assert batch_id is not None

    path = f"/internal/v1/paper/orders/{seed.order_id}/cancel-result"
    plan_path = f"/internal/v1/paper/plans/{batch_id}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        committed_response = await client.get(
            path,
            headers=headers("GET", path) | {"Idempotency-Key": committed_key},
        )
        plan_response = await client.get(plan_path, headers=headers("GET", plan_path))

    assert committed_response.status_code == 200
    committed_data = committed_response.json()["data"]
    assert committed_data["status"] == "COMMITTED"
    assert committed_data["result_event_seq"] == committed.result_event_seq
    assert committed_data["order"]["status"] == "cancelled"
    assert committed_data["reservation"]["released"] is True
    assert plan_response.status_code == 200, plan_response.text
    plan_times = plan_response.json()["data"]["plan"]
    assert plan_times["reconciliation_as_of"] == committed_data["committed_at"]
    assert plan_times["reconciliation_as_of"] > plan_times["plan_created_at"]

    async with factory() as session, session.begin():
        other_order = await session.scalar(
            select(PaperOrderRecord)
            .where(
                PaperOrderRecord.account_id == seed.account_id,
                PaperOrderRecord.id != seed.order_id,
            )
            .order_by(PaperOrderRecord.id)
        )
        assert other_order is not None
        conflict_key = "cancel-read-conflict"
        session.add(
            PaperRequestIdempotencyRecord(
                id=uuid4(),
                account_id=seed.account_id,
                order_id=other_order.id,
                operation="cancel",
                idempotency_key=conflict_key,
                request_hash="a" * 64,
                result_event_seq=1,
                result_hash="b" * 64,
                created_at=datetime(2026, 8, 27, 14, 1, tzinfo=UTC),
            )
        )
        unknown_key = "cancel-read-unknown"
        session.add(
            PaperRequestIdempotencyRecord(
                id=uuid4(),
                account_id=seed.account_id,
                order_id=seed.order_id,
                operation="cancel",
                idempotency_key=unknown_key,
                request_hash=_cancel_request_hash(
                    order_id=seed.order_id,
                    expected_event_seq=committed.result_event_seq - 1,
                ),
                result_event_seq=None,
                result_hash="c" * 64,
                created_at=datetime(2026, 8, 27, 14, 2, tzinfo=UTC),
            )
        )

    async with factory() as session:
        before_repeated_reads = await _database_fingerprint(session)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        conflict = await client.get(
            path,
            headers=headers("GET", path) | {"Idempotency-Key": conflict_key},
        )
        unknown = await client.get(
            path,
            headers=headers("GET", path) | {"Idempotency-Key": unknown_key},
        )
        committed_repeated = await client.get(
            path,
            headers=headers("GET", path) | {"Idempotency-Key": committed_key},
        )
        conflict_repeated = await client.get(
            path,
            headers=headers("GET", path) | {"Idempotency-Key": conflict_key},
        )
        unknown_repeated = await client.get(
            path,
            headers=headers("GET", path) | {"Idempotency-Key": unknown_key},
        )
    async with factory() as session:
        after_repeated_reads = await _database_fingerprint(session)

    assert conflict.status_code == unknown.status_code == 200
    assert conflict.json()["data"]["status"] == "CONFLICT"
    assert unknown.json()["data"]["status"] == "UNKNOWN"
    assert unknown.json()["data"]["reservation"]["released"] is True
    assert committed_repeated.json()["data"] == committed_data
    assert conflict_repeated.json()["data"] == conflict.json()["data"]
    assert unknown_repeated.json()["data"] == unknown.json()["data"]
    assert before_repeated_reads == after_repeated_reads



@pytest.mark.asyncio
async def test_unknown_cancel_claim_keeps_an_empty_projection_occupied(
    migrated_database_url: str,
) -> None:
    from nexus.paper.projection import load_paper_projection
    from nexus.paper.target_service import (
        approve_initial_target_projection,
        require_activation_projection_unoccupied,
    )
    from nexus.persistence.models.paper import PaperOrderEventRecord

    seed, factory, _ = await _seed_partial_order(migrated_database_url)
    cancel_time = datetime(2026, 8, 27, 14, 0, tzinfo=UTC)
    async with factory() as session, session.begin():
        orders = tuple(
            (
                await session.scalars(
                    select(PaperOrderRecord)
                    .where(PaperOrderRecord.account_id == seed.account_id)
                    .order_by(PaperOrderRecord.id)
                )
            ).all()
        )
        for index, order in enumerate(orders):
            latest = await session.scalar(
                select(PaperOrderEventRecord)
                .where(PaperOrderEventRecord.order_id == order.id)
                .order_by(PaperOrderEventRecord.event_seq.desc())
                .limit(1)
            )
            assert latest is not None
            if latest.to_status in {"submitted", "partially_filled"}:
                await cancel_internal_paper_order(
                    session,
                    order_id=order.id,
                    expected_event_seq=latest.event_seq,
                    requested_at=cancel_time + timedelta(seconds=index),
                    idempotency_key=f"cancel-before-activation-{index}",
                )
        session.add(
            PaperRequestIdempotencyRecord(
                id=uuid4(),
                account_id=seed.account_id,
                order_id=seed.order_id,
                operation="cancel",
                idempotency_key="cancel-before-activation-unknown",
                request_hash="a" * 64,
                result_event_seq=None,
                result_hash="b" * 64,
                created_at=cancel_time + timedelta(minutes=1),
            )
        )

    activation_time = cancel_time + timedelta(minutes=2)
    async with factory() as session:
        projection = await load_paper_projection(
            session,
            account_id=seed.account_id,
            as_of=activation_time,
        )
        assert projection.open_orders == ()
        assert projection.reserved_cash == 0
        with pytest.raises(ValueError, match="^INITIAL_PROJECTION_OCCUPIED_OR_UNKNOWN$"):
            await require_activation_projection_unoccupied(
                session,
                account_id=seed.account_id,
                projection=projection,
            )
        with pytest.raises(ValueError, match="^INITIAL_PROJECTION_OCCUPIED_OR_UNKNOWN$"):
            await approve_initial_target_projection(
                session,
                account_id=seed.account_id,
                projection_as_of=projection.as_of,
                expected_projection_hash=projection.projection_hash,
                approved_by="local-user",
                approved_at=activation_time,
                idempotency_key="approval-with-unknown-cancel",
            )


@pytest.mark.parametrize(
    ("corruption", "table", "statement"),
    (
        (
            "cancel actor",
            "paper_order_events",
            "UPDATE paper_order_events SET actor_kind = 'task19' "
            "WHERE order_id = :order_id AND to_status = 'cancelled'",
        ),
        (
            "cancel reason",
            "paper_order_events",
            "UPDATE paper_order_events SET reason_code = 'paper_timeout_cancelled' "
            "WHERE order_id = :order_id AND to_status = 'cancelled'",
        ),
        (
            "cancel payload",
            "paper_order_events",
            "UPDATE paper_order_events SET payload_hash = repeat('a', 64) "
            "WHERE order_id = :order_id AND to_status = 'cancelled'",
        ),
        (
            "fill payload",
            "paper_fills",
            "UPDATE paper_fills SET payload_hash = repeat('a', 64) "
            "WHERE order_id = :order_id",
        ),
        (
            "reservation payload",
            "paper_reservation_events",
            "UPDATE paper_reservation_events SET payload_hash = repeat('a', 64) "
            "WHERE reservation_id = (SELECT id FROM paper_reservations "
            "WHERE order_id = :order_id) AND event_type = 'released'",
        ),
    ),
)
@pytest.mark.asyncio
async def test_cancel_result_downgrades_corrupt_committed_fact_chain_to_unknown(
    migrated_database_url: str,
    corruption: str,
    table: str,
    statement: str,
) -> None:
    seed, factory, expected_event_seq = await _seed_partial_order(migrated_database_url)
    key = f"cancel-read-corrupt-{corruption.replace(' ', '-')}"
    async with factory() as session, session.begin():
        await cancel_internal_paper_order(
            session,
            order_id=seed.order_id,
            expected_event_seq=expected_event_seq,
            requested_at=datetime(2026, 8, 27, 14, 0, tzinfo=UTC),
            idempotency_key=key,
        )
    async with factory() as session, session.begin():
        await _corrupt_append_only_fact(
            session,
            table=table,
            statement=statement,
            order_id=seed.order_id,
        )

    path = f"/internal/v1/paper/orders/{seed.order_id}/cancel-result"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(
            path,
            headers=headers("GET", path) | {"Idempotency-Key": key},
        )

    assert response.status_code == 200
    assert response.json()["data"]["status"] == "UNKNOWN", corruption
    assert response.json()["data"]["result_event_seq"] is None
    assert response.json()["data"]["committed_at"] is None


@pytest.mark.asyncio
async def test_cancel_result_unknown_order_is_not_found(migrated_database_url: str) -> None:
    path = f"/internal/v1/paper/orders/{uuid4()}/cancel-result"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(
            path,
            headers=headers("GET", path) | {"Idempotency-Key": "unknown-order"},
        )
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_legacy_plan_separates_risk_execution_reservation_and_actual_facts(
    migrated_database_url: str,
) -> None:
    seed, factory, _ = await _seed_partial_order(migrated_database_url)
    async with factory() as session:
        batch_id = await session.scalar(
            select(PaperOrderBatchRecord.id)
            .join(PaperOrderRecord, PaperOrderRecord.batch_id == PaperOrderBatchRecord.id)
            .where(PaperOrderRecord.id == seed.order_id)
        )
    assert batch_id is not None

    path = f"/internal/v1/paper/plans/{batch_id}"
    async with factory() as session:
        before = await _database_fingerprint(session)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
        repeated = await client.get(path, headers=headers("GET", path))
        invalid_path = f"{path}?account_id=hidden"
        invalid = await client.get(invalid_path, headers=headers("GET", invalid_path))
    async with factory() as session:
        after = await _database_fingerprint(session)

    assert response.status_code == repeated.status_code == 200, response.text
    assert response.json()["data"] == repeated.json()["data"]
    assert before == after
    data = response.json()["data"]
    assert data["association_state"] == "legacy_unassociated"
    assert data["target"] is None
    assert data["proposal"]["strategy_decision_id"]
    assert data["proposal"]["as_of"]
    assert data["plan"]["plan_created_at"]
    assert data["plan"]["reconciliation_as_of"]
    assert data["plan"]["reconciliation_as_of"] > data["plan"]["plan_created_at"]
    assert {item["outcome"] for item in data["plan"]["risk"]} == {
        "approved",
        "reduced",
        "rejected",
    }
    partial = next(item for item in data["plan"]["items"] if item["order_id"] == str(seed.order_id))
    assert Decimal(partial["confirmed_fill_quantity"]) == Decimal("2")
    assert len(partial["confirmed_fills"]) == 1
    assert partial["confirmed_fills"][0]["as_of"]
    assert Decimal(partial["confirmed_fills"][0]["notional_usd"]) > 0
    assert Decimal(partial["open_order_coverage_quantity"]) > 0
    assert Decimal(partial["open_order_coverage_amount_usd"]) > 0
    assert Decimal(partial["cash_reservation_outstanding"]) > 0
    assert Decimal(partial["position_reservation_outstanding"]) == 0
    assert Decimal(partial["remaining_target_gap_quantity"]) > 0
    assert Decimal(partial["remaining_target_gap_amount_usd"]) > 0
    assert data["plan"]["occupation_state"] == "occupied"
    assert data["actual"]["as_of"]
    assert Decimal(data["actual"]["reserved_cash"]) > 0
    assert invalid.status_code == 422


@pytest.mark.parametrize(
    ("table", "statement"),
    (
        (
            "paper_order_batches",
            "UPDATE paper_order_batches SET batch_hash = repeat('a', 64) WHERE id = :order_id",
        ),
        (
            "risk_evaluations",
            "UPDATE risk_evaluations SET result_hash = repeat('a', 64) "
            "WHERE batch_id = :order_id",
        ),
        (
            "strategy_decisions",
            "UPDATE strategy_decisions SET decision_hash = repeat('a', 64) "
            "WHERE id = (SELECT strategy_decision_id FROM paper_order_batches "
            "WHERE id = :order_id)",
        ),
    ),
)
@pytest.mark.asyncio
async def test_plan_rejects_corrupt_authority_hashes(
    migrated_database_url: str,
    table: str,
    statement: str,
) -> None:
    seed, factory, _ = await _seed_partial_order(migrated_database_url)
    async with factory() as session:
        batch_id = await session.scalar(
            select(PaperOrderBatchRecord.id)
            .join(PaperOrderRecord, PaperOrderRecord.batch_id == PaperOrderBatchRecord.id)
            .where(PaperOrderRecord.id == seed.order_id)
        )
    assert batch_id is not None
    async with factory() as session, session.begin():
        await _corrupt_append_only_fact(
            session,
            table=table,
            statement=statement,
            order_id=batch_id,
        )

    path = f"/internal/v1/paper/plans/{batch_id}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=headers("GET", path))

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "DATA_PARTIAL"


@pytest.mark.asyncio
async def test_plan_rejects_missing_risk_evaluation_without_fallback(
    migrated_database_url: str,
) -> None:
    seed, factory, _ = await _seed_partial_order(migrated_database_url)
    async with factory() as session:
        batch_id = await session.scalar(
            select(PaperOrderBatchRecord.id)
            .join(PaperOrderRecord, PaperOrderRecord.batch_id == PaperOrderBatchRecord.id)
            .where(PaperOrderRecord.id == seed.order_id)
        )
    assert batch_id is not None
    async with factory() as session, session.begin():
        await _remove_one_risk_evaluation(session, batch_id=batch_id)

    path = f"/internal/v1/paper/plans/{batch_id}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=headers("GET", path))

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "DATA_PARTIAL"


@pytest.mark.parametrize("orphan", (False, True), ids=("duplicate", "orphan"))
@pytest.mark.asyncio
async def test_plan_rejects_duplicate_or_orphan_risk_evaluation(
    migrated_database_url: str,
    orphan: bool,
) -> None:
    seed, factory, _ = await _seed_partial_order(migrated_database_url)
    async with factory() as session:
        batch_id = await session.scalar(
            select(PaperOrderBatchRecord.id)
            .join(PaperOrderRecord, PaperOrderRecord.batch_id == PaperOrderBatchRecord.id)
            .where(PaperOrderRecord.id == seed.order_id)
        )
    assert batch_id is not None
    async with factory() as session, session.begin():
        await _insert_extra_risk_evaluation(
            session,
            batch_id=batch_id,
            orphan=orphan,
        )

    path = f"/internal/v1/paper/plans/{batch_id}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=headers("GET", path))

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "DATA_PARTIAL"


@pytest.mark.asyncio
async def test_plan_preserves_target_and_proposal_identity_without_merging_actual(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine, _, args, proposal, activated = await _seed_target_associated_plan(
        migrated_database_url,
        monkeypatch,
    )
    try:
        path = f"/internal/v1/paper/plans/{activated.batch_id}"
        request_now = args["now"] + timedelta(minutes=1)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app(migrated_database_url, now=request_now)),
            base_url="http://testserver",
        ) as client:
            response = await client.get(path, headers=headers("GET", path))

        assert response.status_code == 200, response.text
        data = response.json()["data"]
        assert data["association_state"] == "target_associated"
        assert data["target"]["target_version_id"] == str(activated.target_version_id)
        assert data["target"]["version_seq"] == 1
        assert data["proposal"]["strategy_decision_id"] == str(proposal.decision_id)
        assert data["proposal"]["decision_hash"] == proposal.proposal.decision_hash
        assert data["target"]["as_of"] != data["actual"]["as_of"]
        assert data["plan"]["reconciliation_as_of"] == data["plan"]["plan_created_at"]
        assert data["plan"]["occupation_state"] == "occupied"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_activation_occupation_binds_the_exact_persisted_plan(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nexus.paper.projection import load_paper_projection
    from nexus.paper.target_service import (
        TargetActivationBlockedError,
        require_activation_projection_unoccupied,
    )

    engine, sessions, args, proposal, activated = await _seed_target_associated_plan(
        migrated_database_url,
        monkeypatch,
    )
    try:
        as_of = args["now"] + timedelta(minutes=1)
        async with sessions() as session:
            projection = await load_paper_projection(
                session,
                account_id=proposal.account_id,
                as_of=as_of,
            )
            with pytest.raises(TargetActivationBlockedError) as caught:
                await require_activation_projection_unoccupied(
                    session,
                    account_id=proposal.account_id,
                    projection=projection,
                )
        assert caught.value.blocking_plan_id == activated.batch_id
    finally:
        await engine.dispose()


@pytest.mark.parametrize(
    "corruption",
    ("decision_hash", "proposal", "binding", "missing_run", "missing_output"),
)
@pytest.mark.asyncio
async def test_target_plan_maps_proposal_authority_corruption_to_pure_read_409(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
    corruption: str,
) -> None:
    engine, sessions, args, proposal, activated = await _seed_target_associated_plan(
        migrated_database_url,
        monkeypatch,
    )
    try:
        async with sessions.begin() as session:
            await _corrupt_target_proposal_authority(
                session,
                decision_id=proposal.decision_id,
                corruption=corruption,
            )
        path = f"/internal/v1/paper/plans/{activated.batch_id}"
        async with sessions() as session:
            before = await _database_fingerprint(session)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(
                app=app(migrated_database_url, now=args["now"] + timedelta(minutes=1))
            ),
            base_url="http://testserver",
        ) as client:
            first = await client.get(path, headers=headers("GET", path))
            repeated = await client.get(path, headers=headers("GET", path))
            orders_path = "/internal/v1/paper/orders?limit=50"
            orders = await client.get(
                orders_path,
                headers=headers("GET", orders_path),
            )
        async with sessions() as session:
            after = await _database_fingerprint(session)

        assert first.status_code == repeated.status_code == 409
        assert first.json()["error"]["code"] == "DATA_PARTIAL"
        assert repeated.json()["error"]["code"] == "DATA_PARTIAL"
        assert orders.status_code == 409
        assert orders.json()["error"]["code"] == "DATA_PARTIAL"
        assert before == after
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_plan_preserves_unknown_actual_projection_without_fabricating_zero(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed, factory, _ = await _seed_partial_order(migrated_database_url)
    async with factory() as session:
        batch_id = await session.scalar(
            select(PaperOrderBatchRecord.id)
            .join(PaperOrderRecord, PaperOrderRecord.batch_id == PaperOrderBatchRecord.id)
            .where(PaperOrderRecord.id == seed.order_id)
        )
    assert batch_id is not None

    async def unavailable_projection(*_args, **_kwargs):
        raise ValueError("projection facts conflict")

    monkeypatch.setattr(
        "nexus.paper.read_service.load_paper_projection",
        unavailable_projection,
    )
    path = f"/internal/v1/paper/plans/{batch_id}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=headers("GET", path))

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "DATA_PARTIAL"
