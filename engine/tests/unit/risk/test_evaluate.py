from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.risk.evaluate import (
    RiskBatchRejected,
    derive_order_candidates,
    evaluate_order_batch,
)
from nexus.risk.models import (
    AuthorityQueryProof,
    OrderCandidate,
    PaperOpenOrder,
    PaperPortfolioProjection,
    PaperPositionBalance,
    RiskMarketSnapshot,
    RiskPrice,
    RiskSectorExposure,
)
from nexus.risk.policy import RiskPolicy
from nexus.strategy.forward import ForwardStrategyDecision, ForwardStrategyTarget

AT = datetime(2026, 8, 21, 20, tzinfo=UTC)
ACCOUNT_ID = UUID(int=1)
PORTFOLIO_ID = UUID(int=2)
TECH_ID = UUID(int=101)
HEALTH_ID = UUID(int=102)
SECOND_TECH_ID = UUID(int=103)


def _sector(
    instrument_id: UUID,
    sector: str,
    weight: Decimal = Decimal("1"),
) -> RiskSectorExposure:
    return RiskSectorExposure(
        instrument_id=instrument_id,
        authority_id=UUID(int=instrument_id.int + 1000),
        sector=sector,
        weight=weight,
        content_hash="a" * 64,
    )


def _candidate(
    instrument_id: UUID = TECH_ID,
    *,
    side: str = "buy",
    quantity: Decimal = Decimal("10"),
    target_weight: Decimal = Decimal("0.125"),
    priority: int = 2,
    close: Decimal = Decimal("100"),
    sector: str = "technology",
) -> OrderCandidate:
    return OrderCandidate(
        instrument_id=instrument_id,
        side=side,
        desired_quantity=quantity,
        target_weight=target_weight,
        strategy_priority=priority,
        reference_close=close,
        asset_type="common_stock",
        currency="USD",
        sector_exposures=(_sector(instrument_id, sector),),
    )


def _proof(kind: str, *, blocked: bool = False) -> AuthorityQueryProof:
    return AuthorityQueryProof(
        kind=kind,
        authority_id=UUID(int=2000 + len(kind)),
        complete=True,
        blocked=blocked,
        available_at=AT,
        content_hash="a" * 64,
    )


def _market(
    instruments: tuple[tuple[UUID, Decimal, str], ...] = ((TECH_ID, Decimal("100"), "technology"),),
    *,
    blocked_kind: str | None = None,
) -> RiskMarketSnapshot:
    ids = tuple(item[0] for item in instruments)
    return RiskMarketSnapshot(
        decision_session=date(2026, 8, 21),
        execution_session=date(2026, 8, 24),
        decision_time=AT,
        source_policy_id=UUID(int=3),
        calendar_authority_id=UUID(int=4),
        universe_snapshot_id=UUID(int=5),
        quality_report_id=UUID(int=6),
        required_instrument_ids=ids,
        whitelisted_instrument_ids=ids,
        prices=tuple(
            RiskPrice(
                instrument_id=instrument_id,
                authority_id=UUID(int=instrument_id.int + 500),
                market="US",
                currency="USD",
                asset_type="common_stock",
                close=close,
                source="fosun",
                delivery_mode="delayed",
                available_at=AT,
                content_hash="b" * 64,
            )
            for instrument_id, close, _ in instruments
        ),
        sector_exposures=tuple(
            _sector(instrument_id, sector) for instrument_id, _, sector in instruments
        ),
        query_proofs=tuple(
            _proof(kind, blocked=kind == blocked_kind)
            for kind in ("corporate_action", "suspension", "delisting")
        ),
        authority_hash="b" * 64,
    )


def _position(
    instrument_id: UUID,
    *,
    quantity: Decimal,
    close: Decimal,
) -> PaperPositionBalance:
    return PaperPositionBalance(
        instrument_id=instrument_id,
        quantity=quantity,
        reserved_quantity=Decimal("0"),
        cost_basis=quantity * close,
        reference_close=close,
    )


def _projection(
    *,
    available_cash: Decimal = Decimal("10000"),
    reserved_cash: Decimal = Decimal("0"),
    positions: tuple[PaperPositionBalance, ...] = (),
    open_orders: tuple[PaperOpenOrder, ...] = (),
    nav: Decimal = Decimal("10000"),
    drawdown_state: str = "unlocked",
) -> PaperPortfolioProjection:
    return PaperPortfolioProjection(
        account_id=ACCOUNT_ID,
        portfolio_id=PORTFOLIO_ID,
        as_of=AT,
        currency="USD",
        account_model="cash",
        position_model="long_only",
        available_cash=available_cash,
        reserved_cash=reserved_cash,
        positions=positions,
        open_orders=open_orders,
        nav=nav,
        peak_nav=nav if drawdown_state == "unlocked" else Decimal("11000"),
        drawdown_state=drawdown_state,
        source_facts_hash="d" * 64,
        projection_hash="c" * 64,
    )


def _decision(
    targets: tuple[ForwardStrategyTarget, ...],
    *,
    cash_weight: Decimal,
) -> ForwardStrategyDecision:
    return ForwardStrategyDecision(
        account_id=ACCOUNT_ID,
        portfolio_id=PORTFOLIO_ID,
        strategy_id="baseline",
        strategy_version="baseline_v1",
        strategy_definition_id=UUID(int=7),
        registry_event_id=UUID(int=8),
        champion_generation=1,
        decision_session=date(2026, 8, 21),
        decision_time=AT,
        execution_session=date(2026, 8, 24),
        source_policy_id=UUID(int=3),
        calendar_authority_id=UUID(int=4),
        universe_snapshot_id=UUID(int=5),
        quality_report_id=UUID(int=6),
        portfolio_projection_hash="c" * 64,
        authority_hash="b" * 64,
        strategy_parameters_hash="d" * 64,
        decision_hash="e" * 64,
        cash_weight=cash_weight,
        reason="WEEKLY_REBALANCE",
        targets=targets,
    )


def _rule_codes(evaluation) -> set[str]:
    return {rule.rule_code for rule in evaluation.rules if not rule.passed}


def test_candidate_derivation_floors_target_quantity_and_builds_exit() -> None:
    decision = _decision(
        (
            ForwardStrategyTarget(
                instrument_id=TECH_ID,
                target_weight=Decimal("0.125"),
                exit=False,
                strategy_priority=1,
                reason="BASELINE_SELECTED",
            ),
            ForwardStrategyTarget(
                instrument_id=HEALTH_ID,
                target_weight=Decimal("0"),
                exit=True,
                strategy_priority=0,
                reason="BASELINE_EXIT",
            ),
        ),
        cash_weight=Decimal("0.875"),
    )
    portfolio = _projection(
        positions=(
            _position(TECH_ID, quantity=Decimal("5"), close=Decimal("90")),
            _position(HEALTH_ID, quantity=Decimal("4"), close=Decimal("50")),
        )
    )
    market = _market(
        (
            (TECH_ID, Decimal("90"), "technology"),
            (HEALTH_ID, Decimal("50"), "health_care"),
        )
    )

    candidates = derive_order_candidates(decision, portfolio, market)

    assert [
        (item.instrument_id, item.side, item.desired_quantity, item.strategy_priority)
        for item in candidates
    ] == [
        (HEALTH_ID, "sell", Decimal("4"), 0),
        (TECH_ID, "buy", Decimal("8"), 1),
    ]


def test_candidate_derivation_omits_no_change() -> None:
    decision = _decision(
        (
            ForwardStrategyTarget(
                instrument_id=TECH_ID,
                target_weight=Decimal("0.10"),
                exit=False,
                strategy_priority=1,
                reason="BASELINE_SELECTED",
            ),
        ),
        cash_weight=Decimal("0.90"),
    )
    portfolio = _projection(
        positions=(_position(TECH_ID, quantity=Decimal("10"), close=Decimal("100")),)
    )

    market = _market()
    candidates = derive_order_candidates(decision, portfolio, market)

    assert candidates == ()
    assert evaluate_order_batch(candidates, portfolio, market, RiskPolicy.v1()) == ()


def test_evaluation_is_independent_of_candidate_input_order() -> None:
    candidates = (
        _candidate(TECH_ID, target_weight=Decimal("0.12"), priority=2),
        _candidate(
            HEALTH_ID,
            target_weight=Decimal("0.11"),
            priority=1,
            sector="health_care",
        ),
        _candidate(
            SECOND_TECH_ID,
            side="sell",
            quantity=Decimal("2"),
            target_weight=Decimal("0"),
            priority=0,
        ),
    )
    portfolio = _projection(
        positions=(_position(SECOND_TECH_ID, quantity=Decimal("2"), close=Decimal("100")),)
    )
    market = _market(
        (
            (TECH_ID, Decimal("100"), "technology"),
            (HEALTH_ID, Decimal("100"), "health_care"),
            (SECOND_TECH_ID, Decimal("100"), "technology"),
        )
    )

    forward = evaluate_order_batch(candidates, portfolio, market, RiskPolicy.v1())
    reverse = evaluate_order_batch(
        tuple(reversed(candidates)),
        portfolio,
        market,
        RiskPolicy.v1(),
    )

    assert forward == reverse
    assert [item.instrument_id for item in forward] == [
        SECOND_TECH_ID,
        HEALTH_ID,
        TECH_ID,
    ]


def test_single_name_limit_reduces_an_oversized_buy() -> None:
    evaluation = evaluate_order_batch(
        (_candidate(quantity=Decimal("30")),),
        _projection(),
        _market(),
        RiskPolicy.v1(),
    )[0]

    assert evaluation.outcome == "reduced"
    assert evaluation.approved_quantity == Decimal("14")
    assert evaluation.projected_name_ratio == Decimal("0.1414")
    assert evaluation.projected_cash_ratio == Decimal("0.8586")
    assert "SINGLE_NAME_LIMIT" in _rule_codes(evaluation)


def test_sector_limit_accounts_for_existing_and_prior_batch_exposure() -> None:
    portfolio = _projection(
        available_cash=Decimal("8000"),
        positions=(_position(SECOND_TECH_ID, quantity=Decimal("20"), close=Decimal("100")),),
    )
    candidates = (
        _candidate(TECH_ID, quantity=Decimal("10"), target_weight=Decimal("0.10")),
        _candidate(
            HEALTH_ID,
            quantity=Decimal("10"),
            target_weight=Decimal("0.10"),
            sector="health_care",
        ),
    )
    market = _market(
        (
            (TECH_ID, Decimal("100"), "technology"),
            (HEALTH_ID, Decimal("100"), "health_care"),
            (SECOND_TECH_ID, Decimal("100"), "technology"),
        )
    )

    results = evaluate_order_batch(candidates, portfolio, market, RiskPolicy.v1())

    assert results[0].instrument_id == TECH_ID
    assert results[0].outcome == "reduced"
    assert results[0].approved_quantity == Decimal("9")
    assert results[0].projected_sector_ratios == (
        ("health_care", Decimal("0")),
        ("technology", Decimal("0.2909")),
    )
    assert results[1].instrument_id == HEALTH_ID
    assert results[1].outcome == "approved"


def test_cash_floor_uses_worst_price_and_rejects_below_minimum_remainder() -> None:
    result = evaluate_order_batch(
        (_candidate(quantity=Decimal("2"), close=Decimal("60")),),
        _projection(available_cash=Decimal("1110"), nav=Decimal("10000")),
        _market(((TECH_ID, Decimal("60"), "technology"),)),
        RiskPolicy.v1(),
    )[0]

    assert result.outcome == "rejected"
    assert result.approved_quantity == Decimal("0")
    assert result.reserve_amount == Decimal("0")
    assert {"CASH_FLOOR", "BELOW_MINIMUM_ORDER"} <= _rule_codes(result)


def test_worst_price_rounds_up_instead_of_under_reserving() -> None:
    close = Decimal("123.456789123401")

    result = evaluate_order_batch(
        (_candidate(quantity=Decimal("1"), close=close),),
        _projection(),
        _market(((TECH_ID, close, "technology"),)),
        RiskPolicy.v1(),
    )[0]

    assert result.outcome == "approved"
    assert result.worst_price == Decimal("124.691357014636")
    assert result.worst_price >= close * Decimal("1.01")
    assert result.reserve_amount == result.worst_price


def test_sell_is_exempt_from_minimum_and_never_creates_short() -> None:
    candidate = _candidate(
        side="sell",
        quantity=Decimal("3"),
        target_weight=Decimal("0"),
        priority=0,
        close=Decimal("20"),
    )
    approved = evaluate_order_batch(
        (candidate,),
        _projection(positions=(_position(TECH_ID, quantity=Decimal("2"), close=Decimal("20")),)),
        _market(((TECH_ID, Decimal("20"), "technology"),)),
        RiskPolicy.v1(),
    )[0]

    assert approved.outcome == "reduced"
    assert approved.approved_quantity == Decimal("2")
    assert approved.reserve_amount == Decimal("2")
    assert "BELOW_MINIMUM_ORDER" not in _rule_codes(approved)
    assert "INSUFFICIENT_POSITION" in _rule_codes(approved)


def test_pending_buy_consumes_cash_and_exposure() -> None:
    open_buy = PaperOpenOrder(
        order_id=UUID(int=3000),
        instrument_id=TECH_ID,
        side="buy",
        quantity=Decimal("10"),
        worst_price=Decimal("101"),
        reserved_cash=Decimal("1010"),
        reserved_quantity=Decimal("0"),
        status="submitted",
    )
    result = evaluate_order_batch(
        (_candidate(quantity=Decimal("10")),),
        _projection(
            available_cash=Decimal("8990"),
            reserved_cash=Decimal("1010"),
            open_orders=(open_buy,),
        ),
        _market(),
        RiskPolicy.v1(),
    )[0]

    assert result.outcome == "reduced"
    assert result.approved_quantity == Decimal("4")
    assert result.projected_name_ratio == Decimal("0.1414")


def test_pending_sell_does_not_release_cash_or_reduce_exposure() -> None:
    open_sell = PaperOpenOrder(
        order_id=UUID(int=3001),
        instrument_id=TECH_ID,
        side="sell",
        quantity=Decimal("5"),
        worst_price=Decimal("100"),
        reserved_cash=Decimal("0"),
        reserved_quantity=Decimal("5"),
        status="submitted",
    )
    result = evaluate_order_batch(
        (
            _candidate(
                HEALTH_ID,
                quantity=Decimal("10"),
                close=Decimal("100"),
                sector="health_care",
            ),
        ),
        _projection(
            available_cash=Decimal("1000"),
            positions=(_position(TECH_ID, quantity=Decimal("10"), close=Decimal("100")),),
            open_orders=(open_sell,),
        ),
        _market(
            (
                (TECH_ID, Decimal("100"), "technology"),
                (HEALTH_ID, Decimal("100"), "health_care"),
            )
        ),
        RiskPolicy.v1(),
    )[0]

    assert result.outcome == "rejected"
    assert result.approved_quantity == Decimal("0")
    assert "CASH_FLOOR" in _rule_codes(result)


def test_drawdown_lock_rejects_buy_but_allows_reduction() -> None:
    portfolio = _projection(
        available_cash=Decimal("9000"),
        positions=(_position(TECH_ID, quantity=Decimal("10"), close=Decimal("100")),),
        nav=Decimal("10000"),
        drawdown_state="locked",
    )
    sell, buy = evaluate_order_batch(
        (
            _candidate(
                HEALTH_ID,
                quantity=Decimal("1"),
                close=Decimal("100"),
                sector="health_care",
            ),
            _candidate(
                TECH_ID,
                side="sell",
                quantity=Decimal("1"),
                target_weight=Decimal("0.09"),
                priority=0,
            ),
        ),
        portfolio,
        _market(
            (
                (TECH_ID, Decimal("100"), "technology"),
                (HEALTH_ID, Decimal("100"), "health_care"),
            )
        ),
        RiskPolicy.v1(),
    )

    assert sell.outcome == "approved"
    assert buy.outcome == "rejected"
    assert "DRAWDOWN_LOCKED" in _rule_codes(buy)


def test_blocked_corporate_action_rejects_the_whole_batch_before_results() -> None:
    with pytest.raises(RiskBatchRejected) as captured:
        evaluate_order_batch(
            (_candidate(),),
            _projection(),
            _market(blocked_kind="corporate_action"),
            RiskPolicy.v1(),
        )

    assert captured.value.reason_code == "RISK_AUTHORITY_INCOMPLETE"
