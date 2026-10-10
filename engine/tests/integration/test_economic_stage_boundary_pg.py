"""Stage-specific no-buy/rejection boundaries through real native Paper facts."""

from datetime import timedelta
from decimal import Decimal

import pytest
from nexus.ops import economic_cycle as cli
from nexus.paper.economic_execution import read_economic_pair_execution
from nexus.paper.economic_window import freeze_common_window
from nexus.paper.projection import load_paper_projection
from nexus.persistence.models.economic import (
    EconomicCommonWindowRecord,
    EconomicWindowOrderRecord,
)
from nexus.persistence.models.paper import (
    PaperDrawdownLockEventRecord,
    PaperFillRecord,
    PaperOrderBatchRecord,
    PaperOrderEventRecord,
    PaperOrderRecord,
    RiskEvaluationRecord,
    RiskEvaluationRuleRecord,
)
from nexus.persistence.models.runtime import AuditEvent, IdempotencyRecord, Job
from sqlalchemy import func, select

from engine.tests.integration.test_economic_cycle_pg import (
    test_normal_pair_producer_binds_four_research_jobs_and_exact_budget as seed,
)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    ["one_not_required", "both_not_required", "one_rejected", "both_rejected"],
)
async def test_new_plan_stage_is_not_confused_with_no_trade_or_prior_fills(
    migrated_database_url, monkeypatch, tmp_path, case
):
    await seed(
        migrated_database_url,
        monkeypatch,
        tmp_path,
        "trade",
        True,
        window_test="frozen",
        collection_test="pair_stage_" + case,
    )


async def verify_stage_boundary(
    sessions, clock, pair_id, kwargs, parents, post_sale, collect_child, case, root_result
):
    from nexus.agents.portfolio_synthesis_contracts import canonical_portfolio_synthesis_hash
    from nexus.risk.policy import transition_drawdown

    # A controlled risk INPUT, not a mocked evaluation or fabricated fill. This
    # tests the native deterministic lock rule, not observed investment drawdown.
    rejected_accounts = set()
    if case in {"one_rejected", "both_rejected"}:
        from nexus.persistence.models.economic import EconomicLaneCycleRecord

        async with sessions.begin() as session:
            lanes = tuple(
                await session.scalars(
                    select(EconomicLaneCycleRecord).where(
                        EconomicLaneCycleRecord.pair_slot_id == pair_id
                    )
                )
            )
            rejected_accounts = {
                lane.account_id
                for lane in lanes
                if case == "both_rejected" or lane.lane == "challenger"
            }
            for account_id in rejected_accounts:
                prior = await session.scalar(
                    select(PaperDrawdownLockEventRecord)
                    .where(PaperDrawdownLockEventRecord.account_id == account_id)
                    .order_by(PaperDrawdownLockEventRecord.event_seq.desc())
                    .limit(1)
                )
                assert prior.to_state == "unlocked"
                ratio = Decimal("0.08")
                state = transition_drawdown("unlocked", ratio, user_acknowledged=False)
                assert state == "locked"
                session.add(
                    PaperDrawdownLockEventRecord(
                        account_id=account_id,
                        event_seq=prior.event_seq + 1,
                        from_state="unlocked",
                        to_state=state,
                        drawdown_ratio=ratio,
                        user_acknowledged=False,
                        reason_code="CONTROLLED_DRAWDOWN_THRESHOLD",
                        occurred_at=clock[0],
                        created_at=clock[0],
                        payload_hash=canonical_portfolio_synthesis_hash(
                            {
                                "schema_version": "controlled_drawdown_input_v1",
                                "account_id": account_id,
                                "ratio": ratio,
                                "as_of": clock[0],
                            }
                        ),
                    )
                )
    result = await cli.execute_continue_plans(**kwargs)
    assert result["status"] == "plans_continued" and result["execution_locked"] is True
    expected_statuses = (
        {"incumbent": "created", "challenger": "not_required"}
        if case == "one_not_required"
        else {"incumbent": "not_required", "challenger": "not_required"}
        if case == "both_not_required"
        else {"incumbent": "created", "challenger": "created"}
    )
    assert {lane["lane"]: lane["status"] for lane in result["lanes"]} == expected_statuses
    async with sessions.begin() as session:
        children = tuple(
            await session.scalars(
                select(PaperOrderBatchRecord).where(
                    PaperOrderBatchRecord.previous_batch_id.in_([parent.id for parent in parents])
                )
            )
        )
        orders = tuple(
            await session.scalars(
                select(PaperOrderRecord).where(
                    PaperOrderRecord.batch_id.in_([child.id for child in children])
                )
            )
        )
        assert (
            len(children)
            == len(orders)
            == (0 if case == "both_not_required" else 1 if case == "one_not_required" else 2)
        )
        for order in orders:
            assert order.side == "buy"
            assert order.risk_outcome == (
                "rejected" if order.account_id in rejected_accounts else "approved"
            )
            if order.account_id in rejected_accounts:
                risk = await session.scalar(
                    select(RiskEvaluationRecord).where(RiskEvaluationRecord.order_id == order.id)
                )
                failed = tuple(
                    await session.scalars(
                        select(RiskEvaluationRuleRecord.rule_code).where(
                            RiskEvaluationRuleRecord.risk_evaluation_id == risk.id,
                            RiskEvaluationRuleRecord.passed.is_(False),
                        )
                    )
                )
                assert "DRAWDOWN_LOCKED" in failed
                assert risk.approved_quantity == 0
            else:
                assert order.quantity > 0
        for parent in parents:
            current = await load_paper_projection(
                session, account_id=parent.account_id, as_of=clock[0]
            )
            assert (current.available_cash + current.reserved_cash) == (
                post_sale[parent.account_id].available_cash
                + post_sale[parent.account_id].reserved_cash
            )
        assert await session.scalar(select(func.count()).select_from(PaperFillRecord)) == 8

    tables = (
        AuditEvent,
        IdempotencyRecord,
        Job,
        PaperOrderBatchRecord,
        PaperOrderRecord,
        PaperFillRecord,
        EconomicCommonWindowRecord,
    )

    async def counts():
        async with sessions() as session:
            return [await session.scalar(select(func.count()).select_from(t)) for t in tables]

    before = await counts()
    assert await cli.execute_continue_plans(**kwargs) == result
    assert await counts() == before
    if case == "both_rejected":
        with pytest.raises(ValueError, match="ECONOMIC_WINDOW_NOT_READY"):
            await cli.execute_freeze_next(**kwargs)
        assert await counts() == before
    elif case == "both_not_required":
        none = await cli.execute_freeze_next(**kwargs)
        assert none["status"] == "not_required" and none["window_id"] is None
        assert await counts() == before
    else:
        child_window = await cli.execute_freeze_next(**kwargs)
        assert child_window["status"] == "frozen"
        async with sessions.begin() as session:
            window = await freeze_common_window(session, pair_slot_id=pair_id, window_seq=2)
            row = await session.get(EconomicCommonWindowRecord, window.window_id)
            selected = set(
                await session.scalars(
                    select(EconomicWindowOrderRecord.order_id).where(
                        EconomicWindowOrderRecord.window_id == row.id
                    )
                )
            )
            assert selected == {o.id for o in orders if o.risk_outcome == "approved"}
            assert row.qualification["plan_stage"] == "post_sale_v1"
            assert len(row.qualification["order_states"]) == len(orders)
        clock[0] = window.window_end + timedelta(seconds=46)
        assert (await collect_child(window)).status == "archives_ready"
        finished = await cli.execute_advance(**kwargs)
        assert finished["status"] == ("partial" if case == "one_rejected" else "completed")
        assert finished["comparison_status"] == (
            "incomplete" if case == "one_rejected" else "not_evaluated"
        )
        assert len(finished["orders"]) == 1
        statuses = {lane["lane"]: lane["status"] for lane in finished["lanes"]}
        assert statuses == {
            "incumbent": "filled",
            "challenger": ("risk_rejected" if case == "one_rejected" else "not_required"),
        }
        assert (await cli.execute_status(**kwargs))["lanes"] == finished["lanes"]
        prior = await counts()
        assert await cli.execute_advance(**kwargs) == {**finished, "replayed": True}
        assert await counts() == prior
        async with sessions() as session:
            assert await session.scalar(select(func.count()).select_from(PaperFillRecord)) == 9
            for order in orders:
                state = await session.scalar(
                    select(PaperOrderEventRecord.to_status)
                    .where(PaperOrderEventRecord.order_id == order.id)
                    .order_by(PaperOrderEventRecord.event_seq.desc())
                    .limit(1)
                )
                assert state == ("risk_rejected" if order.risk_outcome == "rejected" else "filled")
                projection = await load_paper_projection(
                    session, account_id=order.account_id, as_of=clock[0]
                )
                assert not projection.open_orders and projection.reserved_cash == 0
    async with sessions() as session:
        historic = await read_economic_pair_execution(
            session,
            pair_slot_id=pair_id,
            window_seq=1,
            expected_database=kwargs["expected_database"],
        )
        assert historic == {**root_result, "replayed": True}
    # Original terminal root and continuation receipt stay readable later;
    # rejection is not replay permission and not a zero-return observation.
    clock[0] += timedelta(days=10)
    final_before = await counts()
    assert await cli.execute_continue_plans(**kwargs) == result
    assert await counts() == final_before
