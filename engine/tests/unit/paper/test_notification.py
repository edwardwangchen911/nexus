from copy import deepcopy
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.domain.evidence import EvidenceRef
from nexus.paper.notification import (
    PaperNotificationDecisionContext,
    PaperNotificationError,
    PaperNotificationFill,
    PaperOrderNotificationSnapshot,
    build_risk_rejected_snapshot,
    build_terminal_execution_snapshot,
    decision_context_from_research_intent,
    notification_snapshot_payload,
    parse_notification_snapshot,
)
from nexus.persistence.models.paper import PaperOrderRecord
from nexus.risk.models import (
    PaperPortfolioProjection,
    PaperPositionBalance,
    RiskEvaluation,
    RiskRuleEvidence,
)
from nexus.strategy.intents import (
    ResearchClaim,
    ResearchCondition,
    ResearchIntent,
    RoleVote,
    SynthesisAssessment,
    SynthesisClaimReview,
)
from pydantic import ValidationError

ORDER_ID = UUID("0199d500-1000-7000-8000-000000000101")
CYCLE_ID = UUID("0199d500-1000-7000-8000-000000000102")
INSTRUMENT_ID = UUID("0199d500-1000-7000-8000-000000000103")

FILLED_PAYLOAD: dict[str, object] = {
    "schema_version": "paper_notification_v1",
    "mode": "internal_paper",
    "order_id": str(ORDER_ID),
    "agent_cycle_id": str(CYCLE_ID),
    "occurred_at": "2026-09-16T02:15:00Z",
    "symbol": "AAPL",
    "side": "buy",
    "status": "filled",
    "requested_quantity": "12",
    "approved_quantity": "12",
    "filled_quantity": "12",
    "remaining_quantity": "0",
    "price": {"kind": "average_fill", "value": "187.36", "currency": "USD"},
    "notional": "2248.32",
    "fee": "0",
    "cash": {"kind": "actual_after", "available": "21406.18"},
    "position": {
        "kind": "actual_after",
        "quantity": "42",
        "portfolio_ratio": None,
    },
    "agent_reason": "盈利预期上修，价格突破近期区间。",
    "invalidation": "价格跌破既定阈值。",
    "risk_reason": None,
}

REJECTED_PAYLOAD: dict[str, object] = {
    **FILLED_PAYLOAD,
    "status": "risk_rejected",
    "requested_quantity": "20",
    "approved_quantity": "0",
    "filled_quantity": "0",
    "remaining_quantity": "0",
    "price": {"kind": "reference", "value": "187", "currency": "USD"},
    "notional": "3740",
    "fee": None,
    "cash": {"kind": "unchanged", "available": "23654.50"},
    "position": {
        "kind": "unchanged",
        "quantity": "30",
        "portfolio_ratio": "0.18",
    },
    "risk_reason": "成交后单一标的仓位将超过 20% 上限。",
}


def test_filled_snapshot_parses_exact_decimals_and_requires_actual_values() -> None:
    snapshot = PaperOrderNotificationSnapshot.model_validate(FILLED_PAYLOAD)

    assert snapshot.status == "filled"
    assert snapshot.price.kind == "average_fill"
    assert snapshot.cash.kind == "actual_after"
    assert snapshot.price.value == Decimal("187.36")
    assert snapshot.filled_quantity + snapshot.remaining_quantity == snapshot.approved_quantity
    assert snapshot.occurred_at == datetime(2026, 9, 16, 2, 15, tzinfo=UTC)


def test_rejected_snapshot_keeps_account_values_unchanged() -> None:
    snapshot = PaperOrderNotificationSnapshot.model_validate(REJECTED_PAYLOAD)

    assert snapshot.status == "risk_rejected"
    assert snapshot.approved_quantity == 0
    assert snapshot.cash.kind == "unchanged"
    assert snapshot.position.kind == "unchanged"
    assert snapshot.risk_reason is not None


@pytest.mark.parametrize(
    ("path", "value"),
    (
        (("price", "kind"), "reference"),
        (("cash", "kind"), "projected_after"),
        (("position", "kind"), "projected_after"),
        (("remaining_quantity",), "1"),
        (("occurred_at",), "2026-09-16T10:15:00+08:00"),
        (("requested_quantity",), 12.0),
    ),
)
def test_filled_snapshot_rejects_invalid_bindings_or_inexact_input(
    path: tuple[str, ...],
    value: object,
) -> None:
    payload = deepcopy(FILLED_PAYLOAD)
    target = payload
    for part in path[:-1]:
        nested = target[part]
        assert isinstance(nested, dict)
        target = nested
    target[path[-1]] = value

    with pytest.raises(ValidationError):
        PaperOrderNotificationSnapshot.model_validate(payload)


def test_rejected_snapshot_requires_reason_and_no_fill_or_fee() -> None:
    for field, value in (
        ("risk_reason", None),
        ("filled_quantity", "1"),
        ("fee", "0"),
    ):
        payload = deepcopy(REJECTED_PAYLOAD)
        payload[field] = value
        with pytest.raises(ValidationError):
            PaperOrderNotificationSnapshot.model_validate(payload)


@pytest.mark.parametrize("status", ("filled", "risk_rejected"))
def test_filled_or_rejected_snapshot_requires_positive_notional(status: str) -> None:
    payload = deepcopy(FILLED_PAYLOAD if status == "filled" else REJECTED_PAYLOAD)
    payload["notional"] = "0"

    with pytest.raises(ValidationError):
        PaperOrderNotificationSnapshot.model_validate(payload)


def test_snapshot_is_closed_and_currency_is_required() -> None:
    extra = deepcopy(FILLED_PAYLOAD)
    extra["account_id"] = "account-secret"
    with pytest.raises(ValidationError):
        PaperOrderNotificationSnapshot.model_validate(extra)

    missing_currency = deepcopy(FILLED_PAYLOAD)
    price = missing_currency["price"]
    assert isinstance(price, dict)
    del price["currency"]
    with pytest.raises(ValidationError):
        PaperOrderNotificationSnapshot.model_validate(missing_currency)


def test_snapshot_payload_round_trips_with_decimal_strings() -> None:
    snapshot = PaperOrderNotificationSnapshot.model_validate(FILLED_PAYLOAD)

    payload = notification_snapshot_payload(snapshot)
    raw = payload["notification_snapshot"]
    assert isinstance(raw, dict)
    assert raw["notional"] == "2248.32"
    assert parse_notification_snapshot(payload) == snapshot


def test_snapshot_payload_uses_plain_canonical_decimal_strings() -> None:
    payload = deepcopy(FILLED_PAYLOAD)
    payload["notional"] = Decimal("1E+3")
    snapshot = PaperOrderNotificationSnapshot.model_validate(payload)

    serialized = notification_snapshot_payload(snapshot)["notification_snapshot"]
    assert isinstance(serialized, dict)
    assert serialized["notional"] == "1000"


@pytest.mark.parametrize("filled_quantity", ("0", "5"))
def test_cancelled_snapshot_binds_price_and_fee_to_fill_facts(filled_quantity: str) -> None:
    payload = deepcopy(FILLED_PAYLOAD)
    payload.update(
        status="cancelled",
        filled_quantity=filled_quantity,
        remaining_quantity=str(12 - int(filled_quantity)),
        price={
            "kind": "average_fill" if filled_quantity != "0" else "reference",
            "value": "187.36",
            "currency": "USD",
        },
        fee="0" if filled_quantity != "0" else None,
        notional="100" if filled_quantity != "0" else "0",
        risk_reason="模拟执行窗口已结束。",
    )

    snapshot = PaperOrderNotificationSnapshot.model_validate(payload)

    assert snapshot.status == "cancelled"


@pytest.mark.parametrize(
    ("filled_quantity", "price_kind", "fee"),
    (
        ("5", "reference", "0"),
        ("5", "average_fill", None),
        ("0", "average_fill", None),
        ("0", "reference", "0"),
    ),
)
def test_cancelled_snapshot_rejects_price_or_fee_mismatch(
    filled_quantity: str,
    price_kind: str,
    fee: str | None,
) -> None:
    payload = deepcopy(FILLED_PAYLOAD)
    payload.update(
        status="cancelled",
        filled_quantity=filled_quantity,
        remaining_quantity=str(12 - int(filled_quantity)),
        price={"kind": price_kind, "value": "187.36", "currency": "USD"},
        fee=fee,
        notional="100" if filled_quantity != "0" else "0",
    )

    with pytest.raises(ValidationError):
        PaperOrderNotificationSnapshot.model_validate(payload)


@pytest.mark.parametrize(("filled_quantity", "notional"), (("5", "0"), ("0", "1")))
def test_cancelled_snapshot_binds_notional_to_fill_facts(
    filled_quantity: str,
    notional: str,
) -> None:
    payload = deepcopy(FILLED_PAYLOAD)
    payload.update(
        status="cancelled",
        filled_quantity=filled_quantity,
        remaining_quantity=str(12 - int(filled_quantity)),
        price={
            "kind": "average_fill" if filled_quantity != "0" else "reference",
            "value": "187.36",
            "currency": "USD",
        },
        fee="0" if filled_quantity != "0" else None,
        notional=notional,
    )

    with pytest.raises(ValidationError):
        PaperOrderNotificationSnapshot.model_validate(payload)


def test_terminal_snapshot_aggregates_weighted_fills_and_actual_account_state() -> None:
    order = PaperOrderRecord(
        id=ORDER_ID,
        account_id=UUID("0199d500-1000-7000-8000-000000000105"),
        batch_id=UUID("0199d500-1000-7000-8000-000000000106"),
        instrument_id=INSTRUMENT_ID,
        strategy_decision_id=UUID("0199d500-1000-7000-8000-000000000107"),
        side="buy",
        quantity=Decimal("12"),
        reference_close=Decimal("100"),
        worst_price=Decimal("102"),
        risk_outcome="approved",
        created_at=datetime(2026, 9, 16, 2, tzinfo=UTC),
    )
    context = PaperNotificationDecisionContext(
        symbol="AAPL",
        agent_cycle_id=CYCLE_ID,
        agent_reason="Agent 判断基本面改善。",
        invalidation="研究证据不再有效。",
    )

    snapshot = build_terminal_execution_snapshot(
        order=order,
        requested_quantity=Decimal("15"),
        approved_quantity=Decimal("12"),
        fills=(
            PaperNotificationFill(
                quantity=Decimal("5"),
                notional=Decimal("501.25"),
                fee=Decimal("0.50"),
            ),
            PaperNotificationFill(
                quantity=Decimal("7"),
                notional=Decimal("707.00"),
                fee=Decimal("0.70"),
            ),
        ),
        available_cash=Decimal("18790.55"),
        position_quantity=Decimal("42"),
        context=context,
        status="filled",
        cancellation_reason_code=None,
        occurred_at=datetime(2026, 9, 16, 2, 15, tzinfo=UTC),
    )

    assert snapshot.requested_quantity == 15
    assert snapshot.approved_quantity == 12
    assert snapshot.filled_quantity == 12
    assert snapshot.price.value == Decimal("100.6875")
    assert snapshot.notional == Decimal("1208.25")
    assert snapshot.fee == Decimal("1.20")
    assert snapshot.cash.available == Decimal("18790.55")
    assert snapshot.position.quantity == 42


def test_unfilled_cancel_snapshot_uses_reference_price_and_fixed_reason() -> None:
    order = PaperOrderRecord(
        id=ORDER_ID,
        account_id=UUID("0199d500-1000-7000-8000-000000000105"),
        batch_id=UUID("0199d500-1000-7000-8000-000000000106"),
        instrument_id=INSTRUMENT_ID,
        strategy_decision_id=UUID("0199d500-1000-7000-8000-000000000107"),
        side="buy",
        quantity=Decimal("12"),
        reference_close=Decimal("100"),
        worst_price=Decimal("102"),
        risk_outcome="approved",
        created_at=datetime(2026, 9, 16, 2, tzinfo=UTC),
    )

    snapshot = build_terminal_execution_snapshot(
        order=order,
        requested_quantity=Decimal("12"),
        approved_quantity=Decimal("12"),
        fills=(),
        available_cash=Decimal("20000"),
        position_quantity=Decimal("0"),
        context=PaperNotificationDecisionContext(symbol="AAPL", agent_cycle_id=None),
        status="cancelled",
        cancellation_reason_code="paper_user_cancelled",
        occurred_at=datetime(2026, 9, 16, 2, 15, tzinfo=UTC),
    )

    assert snapshot.price.kind == "reference"
    assert snapshot.price.value == Decimal("100")
    assert snapshot.notional == 0
    assert snapshot.fee is None
    assert snapshot.risk_reason == "用户取消了订单。"


@pytest.mark.parametrize(
    "payload",
    (
        {},
        {"notification_snapshot": "not-an-object"},
        {"notification_snapshot": {**FILLED_PAYLOAD, "status": "submitted"}},
    ),
)
def test_snapshot_parser_returns_value_free_error(payload: dict[str, object]) -> None:
    with pytest.raises(PaperNotificationError, match="^PAPER_NOTIFICATION_INVALID$") as caught:
        parse_notification_snapshot(payload)

    assert "AAPL" not in str(caught.value)


def _research_intent() -> ResearchIntent:
    decision_time = datetime(2026, 9, 16, 2, tzinfo=UTC)
    evidence = EvidenceRef(
        source="fixture",
        object_id="research-1",
        available_at=decision_time - timedelta(minutes=5),
        location="memory://research-1",
        content_hash="a" * 64,
    )
    synthesis_claim = ResearchClaim(
        text="综合证据支持增加风险暴露。",
        evidence_refs=(evidence,),
    )
    thesis = ResearchClaim(
        text="这是仅在缺少综合结论时使用的备选理由。",
        evidence_refs=(evidence,),
    )
    condition = ResearchCondition(
        signal="committee.evidence_valid",
        operator="is_false",
        threshold=True,
        unit="boolean",
        evidence_refs=(evidence,),
    )
    return ResearchIntent(
        synthesis_assessment=SynthesisAssessment(
            stance="buy_candidate",
            claims=(synthesis_claim,),
            reviews=(
                SynthesisClaimReview(
                    claim_id="evidence_audit:0",
                    status="accepted",
                    rationale="冻结证据已完成核对。",
                    evidence_refs=(evidence,),
                    resolved=True,
                    missing_evidence=(),
                    prevented_conclusion=None,
                ),
            ),
            review_conclusion="sufficient",
            uncertainties=(),
        ),
        schema_version="research_intent_v1",
        decision_id=UUID("0199d500-1000-7000-8000-000000000104"),
        instrument_id=INSTRUMENT_ID,
        display_symbol="AAPL",
        decision_time=decision_time,
        data_snapshot_id=UUID("0199d500-1000-7000-8000-000000000105"),
        horizon="medium_term",
        stance="buy_candidate",
        exposure_action="eligible_for_increase",
        theses=(thesis,),
        counter_theses=(),
        uncertainties=(),
        invalidation_conditions=(condition,),
        entry_conditions=(condition,),
        exit_conditions=(condition,),
        valid_until=decision_time + timedelta(days=1),
        quality_status="complete",
        evidence_audit="complete",
        role_votes=(RoleVote(role="orchestrator", vote="support"),),
        disagreement="无重大分歧。",
        strategy_version="agent-v1",
        model_version="fixture",
        prompt_version="fixture-v1",
        data_version="fixture-v1",
        evidence_refs=(evidence,),
    )


def test_agent_decision_context_prefers_synthesis_and_renders_known_invalidation() -> None:
    context = decision_context_from_research_intent(
        _research_intent(),
        agent_cycle_id=CYCLE_ID,
    )

    assert context == PaperNotificationDecisionContext(
        symbol="AAPL",
        agent_cycle_id=CYCLE_ID,
        agent_reason="综合证据支持增加风险暴露。",
        invalidation="研究证据不再有效。",
    )


def test_agent_decision_context_falls_back_to_first_thesis() -> None:
    intent = _research_intent().model_copy(update={"synthesis_assessment": None})

    context = decision_context_from_research_intent(intent, agent_cycle_id=CYCLE_ID)

    assert context.agent_reason == "这是仅在缺少综合结论时使用的备选理由。"


def test_rejected_snapshot_freezes_unchanged_projection_and_first_failed_rule() -> None:
    context = decision_context_from_research_intent(
        _research_intent(),
        agent_cycle_id=CYCLE_ID,
    )
    order = PaperOrderRecord(
        id=ORDER_ID,
        batch_id=UUID("0199d500-1000-7000-8000-000000000106"),
        account_id=UUID("0199d500-1000-7000-8000-000000000107"),
        strategy_decision_id=UUID("0199d500-1000-7000-8000-000000000108"),
        client_order_id="paper-test",
        instrument_id=INSTRUMENT_ID,
        side="buy",
        quantity=Decimal("20"),
        target_weight=Decimal("0.2"),
        reference_close=Decimal("187"),
        worst_price=Decimal("188"),
        risk_outcome="rejected",
    )
    evaluation = RiskEvaluation(
        instrument_id=INSTRUMENT_ID,
        side="buy",
        outcome="rejected",
        desired_quantity=Decimal("20"),
        approved_quantity=Decimal("0"),
        target_weight=Decimal("0.2"),
        strategy_priority=2,
        reference_close=Decimal("187"),
        worst_price=Decimal("188"),
        reserve_amount=Decimal("0"),
        projected_available_cash=Decimal("23654.50"),
        projected_cash_ratio=Decimal("0.47"),
        projected_name_ratio=Decimal("0.18"),
        projected_sector_ratios=(),
        rules=(
            RiskRuleEvidence(
                rule_code="SINGLE_NAME_LIMIT",
                passed=False,
                limit=Decimal("0.20"),
                observed=Decimal("0.25"),
            ),
        ),
        result_hash="b" * 64,
    )
    portfolio = PaperPortfolioProjection(
        account_id=order.account_id,
        portfolio_id=UUID("0199d500-1000-7000-8000-000000000109"),
        as_of=datetime(2026, 9, 16, 2, tzinfo=UTC),
        currency="USD",
        account_model="cash",
        position_model="long_only",
        available_cash=Decimal("23654.50"),
        reserved_cash=Decimal("0"),
        positions=(
            PaperPositionBalance(
                instrument_id=INSTRUMENT_ID,
                quantity=Decimal("30"),
                reserved_quantity=Decimal("0"),
                cost_basis=Decimal("5000"),
                reference_close=Decimal("187"),
            ),
        ),
        open_orders=(),
        nav=Decimal("31182.50"),
        peak_nav=Decimal("31182.50"),
        drawdown_state="unlocked",
        source_facts_hash="c" * 64,
        projection_hash="d" * 64,
    )

    snapshot = build_risk_rejected_snapshot(
        order=order,
        evaluation=evaluation,
        portfolio=portfolio,
        context=context,
        occurred_at=datetime(2026, 9, 16, 2, 15, tzinfo=UTC),
    )

    assert snapshot.status == "risk_rejected"
    assert snapshot.notional == Decimal("3740")
    assert snapshot.cash.available == Decimal("23654.50")
    assert snapshot.position.quantity == Decimal("30")
    assert snapshot.position.portfolio_ratio == Decimal("0.18")
    assert snapshot.risk_reason == "成交后单一标的仓位将超过 20% 上限。"
