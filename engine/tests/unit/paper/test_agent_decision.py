from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.agents.actionable_research import ActionableResearchDecision
from nexus.paper.agent_cycle_models import floor_decision_slot
from nexus.paper.agent_decision import (
    AGENT_PAPER_SIZING_POLICY_HASH,
    AgentPaperDecisionError,
    AgentPaperSizingPolicyV1,
    build_agent_paper_decision,
)
from nexus.persistence.models.paper import (
    PAPER_DECISION_SOURCE_POLICY_ID,
    PAPER_EXECUTION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
    AgentPaperCycleRecord,
)
from nexus.risk.evaluate import derive_order_candidates
from nexus.risk.models import (
    AuthorityQueryProof,
    PaperOpenOrder,
    PaperPortfolioProjection,
    PaperPositionBalance,
    RiskMarketSnapshot,
    RiskPrice,
    RiskSectorExposure,
)

AT = datetime(2026, 9, 11, 20, 15, tzinfo=UTC)
ACCOUNT_ID = UUID(int=1)
PORTFOLIO_ID = UUID(int=2)
REDUCE_ID = UUID(int=101)
INCREASE_ID = UUID(int=102)
HOLD_ID = UUID(int=103)


def _action(
    instrument_id: UUID,
    action: str,
    *,
    output_id: UUID | None = None,
) -> ActionableResearchDecision:
    stance_by_action = {
        "increase": "buy_candidate",
        "reduce": "reduce_candidate",
        "maintain": "hold",
        "none": "watch",
    }
    return ActionableResearchDecision(
        instrument_id=instrument_id,
        output_id=output_id or UUID(int=instrument_id.int + 1000),
        intent_hash=f"{instrument_id.int:064x}",
        stance=stance_by_action[action],
        sizing_action=action,
        evidence_ref_ids=(UUID(int=instrument_id.int + 2000),),
    )


def _cycle() -> AgentPaperCycleRecord:
    return AgentPaperCycleRecord(
        id=UUID(int=10),
        account_id=ACCOUNT_ID,
        portfolio_id=PORTFOLIO_ID,
        decision_session=date(2026, 9, 11),
        decision_time=AT,
        decision_slot_at=floor_decision_slot(AT),
        execution_session=date(2026, 9, 14),
        universe_snapshot_id=UUID(int=5),
        quality_report_id=None,
        decision_price_policy_id=PAPER_DECISION_SOURCE_POLICY_ID,
        execution_price_policy_id=PAPER_EXECUTION_SOURCE_POLICY_ID,
        candidate_manifest=[],
        candidate_hash="a" * 64,
        sizing_policy_version="agent_paper_sizing_v1",
        sizing_policy_hash=AGENT_PAPER_SIZING_POLICY_HASH,
        execution_locked=True,
    )


def _position(instrument_id: UUID, quantity: str, close: str) -> PaperPositionBalance:
    return PaperPositionBalance(
        instrument_id=instrument_id,
        quantity=Decimal(quantity),
        reserved_quantity=Decimal(0),
        cost_basis=Decimal(quantity) * Decimal(close),
        reference_close=Decimal(close),
    )


def _projection(
    *,
    positions: tuple[PaperPositionBalance, ...] = (),
    open_orders: tuple[PaperOpenOrder, ...] = (),
    nav: str = "10000",
) -> PaperPortfolioProjection:
    return PaperPortfolioProjection(
        account_id=ACCOUNT_ID,
        portfolio_id=PORTFOLIO_ID,
        as_of=AT,
        currency="USD",
        account_model="cash",
        position_model="long_only",
        available_cash=Decimal("7000"),
        reserved_cash=Decimal(0),
        positions=positions,
        open_orders=open_orders,
        nav=Decimal(nav),
        peak_nav=Decimal(nav),
        drawdown_state="unlocked",
        source_facts_hash="c" * 64,
        projection_hash="d" * 64,
    )


def _market(
    instruments: tuple[tuple[UUID, str, str], ...],
) -> RiskMarketSnapshot:
    ids = tuple(item[0] for item in instruments)
    proofs = tuple(
        AuthorityQueryProof(
            kind=kind,
            authority_id=UUID(int=3000 + index),
            complete=True,
            blocked=False,
            available_at=AT,
            content_hash="e" * 64,
        )
        for index, kind in enumerate(("corporate_action", "suspension", "delisting"))
    )
    draft = RiskMarketSnapshot(
        decision_session=date(2026, 9, 11),
        execution_session=date(2026, 9, 14),
        decision_time=AT,
        source_policy_id=PAPER_DECISION_SOURCE_POLICY_ID,
        calendar_authority_id=UUID(int=4),
        universe_snapshot_id=UUID(int=5),
        quality_report_id=None,
        required_instrument_ids=ids,
        whitelisted_instrument_ids=ids,
        prices=tuple(
            RiskPrice(
                instrument_id=instrument_id,
                authority_id=UUID(int=instrument_id.int + 4000),
                market="US",
                currency="USD",
                asset_type="common_stock",
                close=Decimal(close),
                source="alpaca",
                delivery_mode="delayed",
                available_at=AT,
                content_hash="f" * 64,
            )
            for instrument_id, close, _ in instruments
        ),
        sector_exposures=tuple(
            RiskSectorExposure(
                instrument_id=instrument_id,
                authority_id=UUID(int=instrument_id.int + 5000),
                sector=sector,
                weight=Decimal(1),
                content_hash="1" * 64,
            )
            for instrument_id, _, sector in instruments
        ),
        query_proofs=proofs,
        authority_hash="0" * 64,
    )
    from nexus.risk.canonical import canonical_risk_market_authority_hash

    return draft.model_copy(update={"authority_hash": canonical_risk_market_authority_hash(draft)})


def test_reduce_precedes_increase_and_weights_equal_one() -> None:
    projection = _projection(
        positions=(
            _position(REDUCE_ID, "20", "100"),
            _position(HOLD_ID, "10", "100"),
        )
    )
    market = _market(
        (
            (REDUCE_ID, "100", "technology"),
            (INCREASE_ID, "50", "health_care"),
            (HOLD_ID, "100", "industrial"),
        )
    )

    decision = build_agent_paper_decision(
        cycle=_cycle(),
        actions=(
            _action(INCREASE_ID, "increase"),
            _action(REDUCE_ID, "reduce"),
            _action(HOLD_ID, "maintain"),
        ),
        projection=projection,
        market=market,
    )

    assert [target.reason for target in decision.targets] == [
        "AGENT_RESEARCH_REDUCE",
        "AGENT_RESEARCH_MAINTAIN",
        "AGENT_RESEARCH_INCREASE",
    ]
    assert decision.cash_weight + sum(
        (target.target_weight for target in decision.targets), Decimal(0)
    ) == Decimal(1)
    assert [(item.output_id, item.sizing_action) for item in decision.bindings] == [
        (UUID(int=1101), "reduce"),
        (UUID(int=1102), "increase"),
        (UUID(int=1103), "maintain"),
    ]


def test_same_session_schwab_policy_keeps_two_percent_sizing() -> None:
    cycle = _cycle()
    cycle.decision_price_policy_id = PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID
    cycle.execution_price_policy_id = PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID
    cycle.execution_session = cycle.decision_session
    market = _market(((INCREASE_ID, "100", "technology"),)).model_copy(
        update={
            "execution_session": cycle.decision_session,
            "source_policy_id": PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
        }
    )
    from nexus.risk.canonical import canonical_risk_market_authority_hash

    market = market.model_copy(
        update={"authority_hash": canonical_risk_market_authority_hash(market)}
    )

    decision = build_agent_paper_decision(
        cycle=cycle,
        actions=(_action(INCREASE_ID, "increase"),),
        projection=_projection(),
        market=market,
    )

    assert decision.execution_session == decision.decision_session
    assert decision.source_policy_id == PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID
    assert decision.execution_source_policy_id == PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID
    assert decision.targets[0].target_weight == Decimal("0.02")


def test_turnover_cap_and_whole_share_derivation_are_deterministic() -> None:
    instruments = tuple(UUID(int=200 + index) for index in range(6))
    projection = _projection()
    market = _market(tuple((instrument_id, "100", "technology") for instrument_id in instruments))
    decision = build_agent_paper_decision(
        cycle=_cycle(),
        actions=tuple(
            _action(instrument_id, "increase") for instrument_id in reversed(instruments)
        ),
        projection=projection,
        market=market,
    )

    assert sum((target.target_weight for target in decision.targets), Decimal(0)) == Decimal("0.10")
    assert decision.targets[-1].target_weight == Decimal(0)
    candidates = derive_order_candidates(decision, projection, market)
    assert [candidate.instrument_id for candidate in candidates] == list(instruments[:5])
    assert all(candidate.desired_quantity == Decimal(2) for candidate in candidates)


def test_open_order_rejects_entire_cycle() -> None:
    projection = _projection(
        open_orders=(
            PaperOpenOrder(
                order_id=UUID(int=900),
                instrument_id=INCREASE_ID,
                side="buy",
                quantity=Decimal(1),
                worst_price=Decimal("101"),
                reserved_cash=Decimal("101"),
                reserved_quantity=Decimal(0),
                status="submitted",
            ),
        )
    )
    with pytest.raises(AgentPaperDecisionError, match="OPEN_ORDER_EXISTS"):
        build_agent_paper_decision(
            cycle=_cycle(),
            actions=(_action(INCREASE_ID, "increase"),),
            projection=projection,
            market=_market(((INCREASE_ID, "100", "technology"),)),
        )


def test_small_position_reduction_exits_and_none_does_not_open_position() -> None:
    projection = _projection(positions=(_position(REDUCE_ID, "2", "100"),))
    market = _market(((REDUCE_ID, "100", "technology"), (INCREASE_ID, "100", "health_care")))
    decision = build_agent_paper_decision(
        cycle=_cycle(),
        actions=(_action(REDUCE_ID, "reduce"), _action(INCREASE_ID, "none")),
        projection=projection,
        market=market,
    )

    observed = [
        (target.instrument_id, target.target_weight, target.exit) for target in decision.targets
    ]
    assert observed == [
        (REDUCE_ID, Decimal(0), True),
        (INCREASE_ID, Decimal(0), False),
    ]


@pytest.mark.parametrize(
    "mutate,code",
    [
        ("account", "PAPER_PROJECTION_INVALID"),
        ("market", "RISK_AUTHORITY_INCOMPLETE"),
        ("duplicate", "DUPLICATE_INSTRUMENT"),
    ],
)
def test_invalid_authority_inputs_fail_closed(mutate: str, code: str) -> None:
    cycle = _cycle()
    projection = _projection()
    market = _market(((INCREASE_ID, "3", "technology"),))
    actions = (_action(INCREASE_ID, "increase"),)
    if mutate == "account":
        projection = projection.model_copy(update={"account_id": UUID(int=999)})
    elif mutate == "market":
        market = market.model_copy(update={"authority_hash": "3" * 64})
    else:
        actions = (actions[0], actions[0].model_copy(update={"output_id": UUID(int=999)}))

    with pytest.raises(AgentPaperDecisionError, match=code):
        build_agent_paper_decision(
            cycle=cycle,
            actions=actions,
            projection=projection,
            market=market,
        )


def test_decimal_weights_are_exact_to_twelve_places() -> None:
    projection = _projection(positions=(_position(HOLD_ID, "1", "1"),), nav="3")
    decision = build_agent_paper_decision(
        cycle=_cycle(),
        actions=(_action(HOLD_ID, "maintain"),),
        projection=projection,
        market=_market(((HOLD_ID, "1", "technology"),)),
    )

    assert decision.targets[0].target_weight == Decimal("0.333333333333")
    assert decision.cash_weight == Decimal("0.666666666667")
    assert derive_order_candidates(
        decision,
        projection,
        _market(((HOLD_ID, "1", "technology"),)),
    ) == ()
    assert all(
        -value.as_tuple().exponent <= 12
        for value in (decision.cash_weight, decision.targets[0].target_weight)
    )


def test_empty_actions_and_binary_float_cash_fail_closed() -> None:
    with pytest.raises(ValueError, match="binary float"):
        AgentPaperSizingPolicyV1(target_step=0.02)
    with pytest.raises(AgentPaperDecisionError, match="NO_ELIGIBLE_CANDIDATE"):
        build_agent_paper_decision(
            cycle=_cycle(),
            actions=(),
            projection=_projection(),
            market=_market(((INCREASE_ID, "100", "technology"),)),
        )
