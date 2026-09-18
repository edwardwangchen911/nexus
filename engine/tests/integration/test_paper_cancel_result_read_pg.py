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
from nexus.persistence.models.paper import (
    PaperOrderBatchRecord,
    PaperOrderRecord,
    PaperRequestIdempotencyRecord,
)
from nexus.persistence.session import session_factory
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
        "paper_orders",
        "paper_order_events",
        "paper_fills",
        "paper_request_idempotency",
        "paper_reservations",
        "paper_reservation_events",
        "paper_ledger_transactions",
        "paper_ledger_entries",
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
        partial = await advance_internal_paper_order(
            session,
            order_id=seed.order_id,
            evaluated_at=seed.evaluation_times[0],
            idempotency_key="cancel-read-partial-fill",
        )
    assert partial.action == "partially_filled"
    return seed, factory, partial.result_event_seq


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

    path = f"/internal/v1/paper/orders/{seed.order_id}/cancel-result"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        committed_response = await client.get(
            path,
            headers=headers("GET", path) | {"Idempotency-Key": committed_key},
        )

    assert committed_response.status_code == 200
    committed_data = committed_response.json()["data"]
    assert committed_data["status"] == "COMMITTED"
    assert committed_data["result_event_seq"] == committed.result_event_seq
    assert committed_data["order"]["status"] == "cancelled"
    assert committed_data["reservation"]["released"] is True

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

    assert response.status_code == repeated.status_code == 200
    assert response.json()["data"] == repeated.json()["data"]
    assert before == after
    data = response.json()["data"]
    assert data["association_state"] == "legacy_unassociated"
    assert data["target"] is None
    assert data["proposal"]["strategy_decision_id"]
    assert data["proposal"]["as_of"]
    assert data["plan"]["as_of"]
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


@pytest.mark.asyncio
async def test_plan_preserves_target_and_proposal_identity_without_merging_actual(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
                    vote.model_copy(update={"vote": "support"}) for vote in source.role_votes
                ),
            }
        )

    monkeypatch.setattr(seed_module, "_safe_no_trade_intent", complete_buy)
    seeded = await _seed_normal_synthesis(migrated_database_url, monkeypatch)
    engine, sessions, args = await proposal_source(
        migrated_database_url,
        seeded=seeded,
        intent_factory=_include_first,
    )
    try:
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
        path = f"/internal/v1/paper/plans/{activated.batch_id}"
        request_now = args["now"] + timedelta(minutes=1)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app(migrated_database_url, now=request_now)),
            base_url="http://testserver",
        ) as client:
            response = await client.get(path, headers=headers("GET", path))

        assert response.status_code == 200
        data = response.json()["data"]
        assert data["association_state"] == "target_associated"
        assert data["target"]["target_version_id"] == str(activated.target_version_id)
        assert data["target"]["version_seq"] == 1
        assert data["proposal"]["strategy_decision_id"] == str(proposal.decision_id)
        assert data["proposal"]["decision_hash"] == proposal.proposal.decision_hash
        assert data["target"]["as_of"] != data["actual"]["as_of"]
        assert data["plan"]["occupation_state"] == "occupied"
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

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["plan"]["occupation_state"] == "unknown"
    assert data["actual"] == {
        "data_state": "UNKNOWN",
        "as_of": response.json()["meta"]["as_of"],
        "available_cash": None,
        "reserved_cash": None,
        "nav": None,
        "positions": None,
    }
