from datetime import UTC, date, datetime, timedelta
from uuid import UUID
from zoneinfo import ZoneInfo

import pytest
from nexus.paper.economic_budget_hold import HoldRequest, require_protocol_budget_day
from nexus.paper.portfolio_economic_preflight import EconomicPlanRequest, compile_plan


def facts(start=date(2026, 10, 30), ordinal=1):
    plan = compile_plan(
        EconomicPlanRequest(
            evaluation_key="slot-budget-case",
            instruments=(
                {"instrument_id": UUID(int=1), "symbol": "AVGO"},
                {"instrument_id": UUID(int=2), "symbol": "NVDA"},
            ),
            start_session=start,
            research_cost_usd="1",
            synthesis_cost_usd="2",
            global_daily_limit_usd="20",
        ),
        now=datetime(2026, 10, 8, tzinfo=UTC),
    )
    request = HoldRequest(
        evaluation_key=plan.request.evaluation_key,
        protocol_hash=plan.content_hash,
        slot_ordinal=ordinal,
        research_cost_usd="1",
        synthesis_cost_usd="2",
        global_limit_usd="20",
        uncovered_formal_usd="0",
    )
    return plan, request


@pytest.mark.parametrize("ordinal,utc_hour", [(1, 14), (2, 15)])
def test_exact_frozen_slot_across_dst_uses_utc_budget_date(ordinal, utc_hour):
    plan, request = facts(ordinal=ordinal)
    slot = plan.slots[ordinal - 1]
    assert slot.decision_at.hour == utc_hour
    observed = slot.decision_at.astimezone(ZoneInfo("Asia/Shanghai"))
    assert require_protocol_budget_day(plan, request, observed_at=observed) == slot


@pytest.mark.parametrize("delta", [-1, 1])
def test_other_day_cannot_fund_this_protocol_slot(delta):
    plan, request = facts()
    with pytest.raises(ValueError, match="ECONOMIC_TRANSFER_SLOT_DAY_MISMATCH"):
        require_protocol_budget_day(
            plan, request, observed_at=plan.slots[0].decision_at + timedelta(days=delta)
        )


@pytest.mark.parametrize(
    "field,value", [("protocol_hash", "b" * 64), ("evaluation_key", "another-evaluation")]
)
def test_other_protocol_identity_is_not_slot_authority(field, value):
    plan, request = facts()
    with pytest.raises(ValueError, match="ECONOMIC_TRANSFER_SLOT_AUTHORITY_INVALID"):
        require_protocol_budget_day(
            plan, request.model_copy(update={field: value}), observed_at=plan.slots[0].decision_at
        )


def test_naive_clock_cannot_be_interpreted_as_utc():
    plan, request = facts()
    with pytest.raises(ValueError, match="ECONOMIC_TRANSFER_SLOT_AUTHORITY_INVALID"):
        require_protocol_budget_day(plan, request, observed_at=datetime(2026, 10, 30))


def test_local_next_calendar_day_still_uses_same_utc_budget_day():
    plan, request = facts()
    utc_time = datetime(2026, 10, 30, 23, 30, tzinfo=UTC)
    observed = utc_time.astimezone(ZoneInfo("Asia/Shanghai"))
    assert observed.date() != plan.slots[0].session
    assert require_protocol_budget_day(plan, request, observed_at=observed) == plan.slots[0]


def test_wrong_slot_ordinal_cannot_borrow_today_budget():
    plan, request = facts()
    with pytest.raises(ValueError, match="ECONOMIC_TRANSFER_SLOT_DAY_MISMATCH"):
        require_protocol_budget_day(
            plan,
            request.model_copy(update={"slot_ordinal": 2}),
            observed_at=plan.slots[0].decision_at,
        )
