"""Pure, deterministic hard-risk candidate derivation and batch evaluation."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import ROUND_FLOOR, ROUND_HALF_EVEN, Decimal
from uuid import UUID

from nexus.risk.canonical import canonical_risk_evaluation_result_hash
from nexus.risk.models import (
    OrderCandidate,
    PaperPortfolioProjection,
    RiskEvaluation,
    RiskMarketSnapshot,
    RiskOutcome,
    RiskRuleEvidence,
    RiskSectorExposure,
)
from nexus.risk.policy import (
    RiskPolicy,
    drawdown_allows_candidate,
    verify_policy_content_hash,
)
from nexus.strategy.forward import ForwardStrategyDecision

_QUANTUM = Decimal("0.000000000001")
_ZERO = Decimal(0)


class RiskBatchRejected(ValueError):
    """A stable fail-closed batch error raised before per-candidate results."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


def _q(value: Decimal) -> Decimal:
    return value.quantize(_QUANTUM, rounding=ROUND_HALF_EVEN)


def _floor_shares(value: Decimal) -> Decimal:
    if value <= 0:
        return _ZERO
    return value.to_integral_value(rounding=ROUND_FLOOR)


def _candidate_sort_key(candidate: OrderCandidate) -> tuple[int, Decimal, int]:
    return (
        candidate.strategy_priority,
        -candidate.target_weight,
        candidate.instrument_id.int,
    )


def _market_maps(
    market: RiskMarketSnapshot,
) -> tuple[dict[UUID, Decimal], dict[UUID, tuple[RiskSectorExposure, ...]]]:
    prices = {price.instrument_id: price.close for price in market.prices}
    sectors: dict[UUID, tuple[RiskSectorExposure, ...]] = {}
    for instrument_id in market.required_instrument_ids:
        sectors[instrument_id] = tuple(
            sorted(
                (
                    exposure
                    for exposure in market.sector_exposures
                    if exposure.instrument_id == instrument_id
                ),
                key=lambda item: (item.sector, item.authority_id.int),
            )
        )
    return prices, sectors


def _preflight(
    candidates: Sequence[OrderCandidate],
    portfolio: PaperPortfolioProjection,
    market: RiskMarketSnapshot,
    policy: RiskPolicy,
) -> RiskPolicy:
    try:
        verified = verify_policy_content_hash(policy)
    except ValueError as error:
        raise RiskBatchRejected("RISK_POLICY_INVALID") from error
    if (
        portfolio.currency != verified.supported_currency
        or portfolio.account_model != "cash"
        or portfolio.position_model != "long_only"
    ):
        raise RiskBatchRejected("PAPER_ACCOUNT_INVALID")
    if any(proof.blocked for proof in market.query_proofs):
        raise RiskBatchRejected("RISK_AUTHORITY_INCOMPLETE")
    required = set(market.required_instrument_ids)
    participating = {
        *(candidate.instrument_id for candidate in candidates),
        *(position.instrument_id for position in portfolio.positions),
        *(order.instrument_id for order in portfolio.open_orders),
    }
    if participating != required:
        raise RiskBatchRejected("RISK_AUTHORITY_INCOMPLETE")
    prices, sectors = _market_maps(market)
    for candidate in candidates:
        if candidate.asset_type not in verified.supported_asset_types:
            raise RiskBatchRejected("RISK_AUTHORITY_INCOMPLETE")
        if candidate.currency != verified.supported_currency:
            raise RiskBatchRejected("RISK_AUTHORITY_INCOMPLETE")
        if prices.get(candidate.instrument_id) != candidate.reference_close:
            raise RiskBatchRejected("RISK_AUTHORITY_INCOMPLETE")
        expected = tuple(
            (item.authority_id, item.sector, item.weight)
            for item in sectors[candidate.instrument_id]
        )
        observed = tuple(
            (item.authority_id, item.sector, item.weight)
            for item in sorted(
                candidate.sector_exposures,
                key=lambda item: (item.sector, item.authority_id.int),
            )
        )
        if observed != expected:
            raise RiskBatchRejected("RISK_AUTHORITY_INCOMPLETE")
    return verified


def derive_order_candidates(
    decision: ForwardStrategyDecision,
    portfolio: PaperPortfolioProjection,
    market: RiskMarketSnapshot,
) -> tuple[OrderCandidate, ...]:
    """Convert trusted target weights into whole-share deltas at the PIT close."""
    if (
        decision.account_id != portfolio.account_id
        or decision.portfolio_id != portfolio.portfolio_id
        or decision.decision_session != market.decision_session
        or decision.execution_session != market.execution_session
        or decision.decision_time != market.decision_time
        or decision.source_policy_id != market.source_policy_id
        or decision.calendar_authority_id != market.calendar_authority_id
        or decision.universe_snapshot_id != market.universe_snapshot_id
        or decision.quality_report_id != market.quality_report_id
        or decision.authority_hash != market.authority_hash
        or decision.portfolio_projection_hash != portfolio.projection_hash
    ):
        raise RiskBatchRejected("STRATEGY_DECISION_INVALID")
    prices, sectors = _market_maps(market)
    positions = {position.instrument_id: position.quantity for position in portfolio.positions}
    candidates: list[OrderCandidate] = []
    for target in decision.targets:
        price = prices.get(target.instrument_id)
        if price is None or target.instrument_id not in sectors:
            raise RiskBatchRejected("RISK_AUTHORITY_INCOMPLETE")
        target_quantity = _floor_shares(target.target_weight * portfolio.nav / price)
        current_quantity = positions.get(target.instrument_id, _ZERO)
        delta = target_quantity - current_quantity
        if delta == 0:
            continue
        candidates.append(
            OrderCandidate(
                instrument_id=target.instrument_id,
                side="buy" if delta > 0 else "sell",
                desired_quantity=abs(delta),
                target_weight=target.target_weight,
                strategy_priority=target.strategy_priority,
                reference_close=price,
                asset_type="common_stock",
                currency="USD",
                sector_exposures=sectors[target.instrument_id],
            )
        )
    return tuple(sorted(candidates, key=_candidate_sort_key))


def _initial_exposures(
    portfolio: PaperPortfolioProjection,
    market: RiskMarketSnapshot,
) -> tuple[dict[UUID, Decimal], dict[str, Decimal]]:
    prices, sectors = _market_maps(market)
    name_exposure: dict[UUID, Decimal] = {
        instrument_id: _ZERO for instrument_id in market.required_instrument_ids
    }
    for position in portfolio.positions:
        name_exposure[position.instrument_id] += position.quantity * prices[position.instrument_id]
    for order in portfolio.open_orders:
        if order.side == "buy":
            name_exposure[order.instrument_id] += order.quantity * order.worst_price
    sector_exposure: dict[str, Decimal] = {}
    for instrument_id, notional in name_exposure.items():
        for exposure in sectors[instrument_id]:
            sector_exposure[exposure.sector] = (
                sector_exposure.get(exposure.sector, _ZERO) + notional * exposure.weight
            )
    return name_exposure, sector_exposure


def _available_positions(portfolio: PaperPortfolioProjection) -> dict[UUID, Decimal]:
    open_sell_reservations: dict[UUID, Decimal] = {}
    for order in portfolio.open_orders:
        if order.side == "sell":
            open_sell_reservations[order.instrument_id] = (
                open_sell_reservations.get(order.instrument_id, _ZERO) + order.reserved_quantity
            )
    return {
        position.instrument_id: position.quantity
        - max(
            position.reserved_quantity,
            open_sell_reservations.get(position.instrument_id, _ZERO),
        )
        for position in portfolio.positions
    }


def _rule(
    code: str,
    *,
    passed: bool,
    limit: Decimal | None = None,
    observed: Decimal | None = None,
    remaining: Decimal | None = None,
    authority_ids: tuple[UUID, ...] = (),
) -> RiskRuleEvidence:
    return RiskRuleEvidence(
        rule_code=code,
        passed=passed,
        limit=_q(limit) if limit is not None else None,
        observed=_q(observed) if observed is not None else None,
        remaining=_q(remaining) if remaining is not None else None,
        authority_ids=authority_ids,
    )


def _finalize(evaluation: RiskEvaluation) -> RiskEvaluation:
    digest = canonical_risk_evaluation_result_hash((evaluation,))
    return evaluation.model_copy(update={"result_hash": digest})


def _evaluate_sell(
    candidate: OrderCandidate,
    *,
    portfolio: PaperPortfolioProjection,
    available_positions: dict[UUID, Decimal],
    name_exposure: dict[UUID, Decimal],
    sector_exposure: dict[str, Decimal],
) -> RiskEvaluation:
    available = max(available_positions.get(candidate.instrument_id, _ZERO), _ZERO)
    approved = min(candidate.desired_quantity, available)
    available_positions[candidate.instrument_id] = available - approved
    outcome: RiskOutcome = (
        "approved"
        if approved == candidate.desired_quantity
        else ("reduced" if approved > 0 else "rejected")
    )
    name_ratio = name_exposure.get(candidate.instrument_id, _ZERO) / portfolio.nav
    sector_ratios = tuple(
        (sector, _q(value / portfolio.nav)) for sector, value in sorted(sector_exposure.items())
    )
    rules = (
        _rule("DRAWDOWN_LOCKED", passed=True),
        _rule(
            "INSUFFICIENT_POSITION",
            passed=approved == candidate.desired_quantity,
            limit=available,
            observed=candidate.desired_quantity,
            remaining=available - candidate.desired_quantity,
        ),
        _rule("SINGLE_NAME_LIMIT", passed=True),
        _rule("SECTOR_LIMIT", passed=True),
        _rule("CASH_FLOOR", passed=True),
        _rule("BELOW_MINIMUM_ORDER", passed=True),
    )
    return _finalize(
        RiskEvaluation(
            instrument_id=candidate.instrument_id,
            side=candidate.side,
            outcome=outcome,
            desired_quantity=candidate.desired_quantity,
            approved_quantity=approved,
            target_weight=candidate.target_weight,
            strategy_priority=candidate.strategy_priority,
            reference_close=candidate.reference_close,
            worst_price=candidate.reference_close,
            reserve_amount=approved,
            projected_available_cash=portfolio.available_cash,
            projected_cash_ratio=_q(portfolio.available_cash / portfolio.nav),
            projected_name_ratio=_q(name_ratio),
            projected_sector_ratios=sector_ratios,
            rules=rules,
            result_hash="0" * 64,
        )
    )


def _sector_quantity_cap(
    candidate: OrderCandidate,
    *,
    nav: Decimal,
    worst_price: Decimal,
    sector_exposure: dict[str, Decimal],
    policy: RiskPolicy,
) -> Decimal:
    caps: list[Decimal] = []
    for exposure in candidate.sector_exposures:
        remaining = policy.sector_limit * nav - sector_exposure.get(exposure.sector, _ZERO)
        denominator = worst_price * exposure.weight
        caps.append(_floor_shares(remaining / denominator))
    return min(caps, default=candidate.desired_quantity)


def _evaluate_buy(
    candidate: OrderCandidate,
    *,
    portfolio: PaperPortfolioProjection,
    available_cash: Decimal,
    name_exposure: dict[UUID, Decimal],
    sector_exposure: dict[str, Decimal],
    policy: RiskPolicy,
) -> tuple[RiskEvaluation, Decimal]:
    worst_price = _q(candidate.reference_close * (Decimal(1) + policy.buy_reserve_markup))
    drawdown_allowed = drawdown_allows_candidate(
        portfolio.drawdown_state,
        side="buy",
        is_reduction=False,
    )
    name_remaining = policy.single_name_limit * portfolio.nav - name_exposure.get(
        candidate.instrument_id, _ZERO
    )
    name_cap = _floor_shares(name_remaining / worst_price)
    sector_cap = _sector_quantity_cap(
        candidate,
        nav=portfolio.nav,
        worst_price=worst_price,
        sector_exposure=sector_exposure,
        policy=policy,
    )
    cash_remaining = available_cash - policy.minimum_cash_ratio * portfolio.nav
    cash_cap = _floor_shares((cash_remaining - policy.commission) / worst_price)
    approved = min(
        candidate.desired_quantity,
        name_cap,
        sector_cap,
        cash_cap,
    )
    if not drawdown_allowed:
        approved = _ZERO
    below_minimum = (
        approved > 0 and approved * candidate.reference_close < policy.minimum_buy_notional
    )
    if below_minimum:
        approved = _ZERO
    reserve = _q(approved * worst_price + policy.commission) if approved > 0 else _ZERO
    projected_cash = available_cash - reserve
    added_exposure = approved * worst_price
    projected_name = name_exposure.get(candidate.instrument_id, _ZERO) + added_exposure
    projected_sectors = dict(sector_exposure)
    for exposure in candidate.sector_exposures:
        projected_sectors[exposure.sector] = (
            projected_sectors.get(exposure.sector, _ZERO) + added_exposure * exposure.weight
        )
    outcome: RiskOutcome = (
        "approved"
        if approved == candidate.desired_quantity
        else ("reduced" if approved > 0 else "rejected")
    )
    authority_ids = tuple(
        sorted(
            (exposure.authority_id for exposure in candidate.sector_exposures),
            key=lambda value: value.int,
        )
    )
    rules = (
        _rule("DRAWDOWN_LOCKED", passed=drawdown_allowed),
        _rule("INSUFFICIENT_POSITION", passed=True),
        _rule(
            "SINGLE_NAME_LIMIT",
            passed=name_cap >= candidate.desired_quantity,
            limit=policy.single_name_limit,
            observed=(
                name_exposure.get(candidate.instrument_id, _ZERO)
                + candidate.desired_quantity * worst_price
            )
            / portfolio.nav,
            remaining=name_remaining,
        ),
        _rule(
            "SECTOR_LIMIT",
            passed=sector_cap >= candidate.desired_quantity,
            limit=policy.sector_limit,
            observed=max(
                (
                    (
                        sector_exposure.get(exposure.sector, _ZERO)
                        + candidate.desired_quantity * worst_price * exposure.weight
                    )
                    / portfolio.nav
                    for exposure in candidate.sector_exposures
                ),
                default=_ZERO,
            ),
            authority_ids=authority_ids,
        ),
        _rule(
            "CASH_FLOOR",
            passed=cash_cap >= candidate.desired_quantity,
            limit=policy.minimum_cash_ratio,
            observed=(available_cash - candidate.desired_quantity * worst_price - policy.commission)
            / portfolio.nav,
            remaining=cash_remaining,
        ),
        _rule(
            "BELOW_MINIMUM_ORDER",
            passed=not below_minimum,
            limit=policy.minimum_buy_notional,
            observed=approved * candidate.reference_close,
        ),
    )
    evaluation = _finalize(
        RiskEvaluation(
            instrument_id=candidate.instrument_id,
            side=candidate.side,
            outcome=outcome,
            desired_quantity=candidate.desired_quantity,
            approved_quantity=approved,
            target_weight=candidate.target_weight,
            strategy_priority=candidate.strategy_priority,
            reference_close=candidate.reference_close,
            worst_price=worst_price,
            reserve_amount=reserve,
            projected_available_cash=_q(projected_cash),
            projected_cash_ratio=_q(projected_cash / portfolio.nav),
            projected_name_ratio=_q(projected_name / portfolio.nav),
            projected_sector_ratios=tuple(
                (sector, _q(value / portfolio.nav))
                for sector, value in sorted(projected_sectors.items())
            ),
            rules=rules,
            result_hash="0" * 64,
        )
    )
    if approved > 0:
        name_exposure[candidate.instrument_id] = projected_name
        sector_exposure.clear()
        sector_exposure.update(projected_sectors)
    return evaluation, projected_cash


def evaluate_order_batch(
    candidates: Sequence[OrderCandidate],
    portfolio: PaperPortfolioProjection,
    market: RiskMarketSnapshot,
    policy: RiskPolicy,
) -> tuple[RiskEvaluation, ...]:
    """Evaluate a batch in stable order using worst-case pending-order projection."""
    verified = _preflight(candidates, portfolio, market, policy)
    ordered = tuple(sorted(candidates, key=_candidate_sort_key))
    name_exposure, sector_exposure = _initial_exposures(portfolio, market)
    available_positions = _available_positions(portfolio)
    available_cash = portfolio.available_cash
    evaluations: list[RiskEvaluation] = []
    for candidate in ordered:
        if candidate.side == "sell":
            evaluations.append(
                _evaluate_sell(
                    candidate,
                    portfolio=portfolio,
                    available_positions=available_positions,
                    name_exposure=name_exposure,
                    sector_exposure=sector_exposure,
                )
            )
            continue
        evaluation, available_cash = _evaluate_buy(
            candidate,
            portfolio=portfolio,
            available_cash=available_cash,
            name_exposure=name_exposure,
            sector_exposure=sector_exposure,
            policy=verified,
        )
        evaluations.append(evaluation)
    return tuple(evaluations)
