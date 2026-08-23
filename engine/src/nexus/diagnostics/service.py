"""Deterministic diagnosis entry point with fail-closed readiness precedence."""

from collections import defaultdict
from collections.abc import Iterable
from decimal import Decimal
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from nexus.diagnostics.decision import optimization_need as _optimization_need
from nexus.diagnostics.metrics import (
    annualized_volatility,
    beta_and_correlation_by_session,
    correlation_components,
    days_to_exit,
    maximum_drawdown,
    qualified_session_closes,
    security_hhi,
    top_weights,
)
from nexus.diagnostics.models import (
    DiagnosticActionability,
    DiagnosticHistory,
    DiagnosticItem,
    DiagnosticSeverity,
    MarketEvidence,
    OptimizationNeed,
    PortfolioDiagnosticReport,
)
from nexus.diagnostics.policy import RealAdvisoryPolicy, load_real_advisory_policy_v1
from nexus.diagnostics.repository import load_frozen_snapshot, load_history, persist_report
from nexus.diagnostics.scope import (
    assert_fosun_read_only_scope as _assert_fosun_read_only_scope,
)
from nexus.diagnostics.scope import (
    has_future_session_conflict as _has_future_session_conflict,
)
from nexus.domain.decimal import decimal_to_string
from nexus.providers.broker.base import BrokerPosition, PortfolioSnapshot


def diagnose_portfolio(
    snapshot: PortfolioSnapshot,
    market: MarketEvidence,
    history: DiagnosticHistory,
    policy: RealAdvisoryPolicy,
) -> PortfolioDiagnosticReport:
    """Diagnose frozen broker facts without cross-currency aggregation or LLM input."""
    source_conflict = market.source_conflict or _has_future_session_conflict(snapshot, market)
    if snapshot.status != "READY" or source_conflict:
        return _report(snapshot, policy, "BLOCKED", (_blocked_item(market, source_conflict),))
    if market.quality != "PASS":
        return _report(snapshot, policy, "RESEARCH_REQUIRED", (_market_unknown_item(market),))
    items = _evaluate_currency_scopes(snapshot, market, policy)
    currencies = {item.currency for item in snapshot.cash} | {
        item.currency for item in snapshot.positions
    }
    if len(currencies) > 1:
        items.append(
            _unknown_item(
                "CURRENCY_EXPOSURE",
                "ratio",
                "FX_QUALIFIED_VIEW_MISSING",
                "portfolio:unqualified_fx",
                market,
            )
        )
    elif currencies:
        items.append(
            _threshold_item(
                "CURRENCY_EXPOSURE",
                Decimal(1),
                "ratio",
                policy.currency_warning,
                f"currency:{next(iter(currencies))}",
                market,
                warning=True,
            )
        )
    _assert_fosun_read_only_scope(snapshot, market, items)
    trading_day = None if snapshot.as_of is None else snapshot.as_of.date()
    return _report(
        snapshot,
        policy,
        _optimization_need(items, history, trading_day, market.trading_sessions),
        tuple(items),
    )


async def diagnose_and_persist(
    session: AsyncSession,
    *,
    portfolio_id: UUID,
    snapshot_id: UUID,
) -> PortfolioDiagnosticReport:
    """Production entry point accepts IDs only and fails closed without stored market evidence."""
    policy = await load_real_advisory_policy_v1(session)
    snapshot = await load_frozen_snapshot(
        session, portfolio_id=portfolio_id, snapshot_id=snapshot_id
    )
    history = await load_history(session, portfolio_id=portfolio_id)
    market = MarketEvidence(quality="PARTIAL")
    report = diagnose_portfolio(snapshot, market, history, policy)
    await persist_report(session, portfolio_id=portfolio_id, report=report)
    return report


def _evaluate_currency_scopes(
    snapshot: PortfolioSnapshot, market: MarketEvidence, policy: RealAdvisoryPolicy
) -> list[DiagnosticItem]:
    positions_by_currency: dict[str, list[BrokerPosition]] = defaultdict(list)
    cash_by_currency: dict[str, Decimal] = defaultdict(lambda: Decimal(0))
    for cash in snapshot.cash:
        cash_by_currency[cash.currency] += cash.cash_balance
    for position in snapshot.positions:
        positions_by_currency[position.currency].append(position)
    items: list[DiagnosticItem] = []
    for currency in sorted(set(positions_by_currency) | set(cash_by_currency)):
        scope = f"currency:{currency}"
        values, value_unknown = _position_values(positions_by_currency[currency])
        if value_unknown is not None:
            items.append(
                _unknown_item("POSITION_MARKET_VALUE", currency, value_unknown, scope, market)
            )
        if values:
            items.extend(_concentration_items(values, scope, market, policy))
            items.extend(_exposure_items(values, scope, market, policy))
            items.extend(_liquidity_items(values, scope, market, policy))
            items.extend(_market_risk_items(values, scope, market, policy, snapshot))
        total = cash_by_currency[currency] + sum((value for _, value in values), Decimal(0))
        if total > 0:
            cash_ratio = cash_by_currency[currency] / total
            items.append(
                _threshold_item(
                    "CASH_RATIO",
                    cash_ratio,
                    "ratio",
                    policy.cash_minimum,
                    scope,
                    market,
                    warning=cash_ratio < policy.cash_minimum,
                )
            )
    return items


def _position_values(
    positions: Iterable[BrokerPosition],
) -> tuple[list[tuple[BrokerPosition, Decimal]], str | None]:
    values: list[tuple[BrokerPosition, Decimal]] = []
    unknown: str | None = None
    for position in positions:
        if position.current_price is None:
            unknown = "POSITION_PRICE_MISSING"
            continue
        if position.current_price <= 0:
            unknown = "POSITION_PRICE_INVALID"
            continue
        if position.quantity <= 0:
            unknown = "POSITION_QUANTITY_INVALID"
            continue
        values.append((position, position.quantity * position.current_price))
    return values, unknown


def _concentration_items(
    values: list[tuple[BrokerPosition, Decimal]],
    scope: str,
    market: MarketEvidence,
    policy: RealAdvisoryPolicy,
) -> list[DiagnosticItem]:
    amounts = tuple(value for _, value in values)
    weights = top_weights(amounts)
    hhi = security_hhi(amounts)
    if weights is None or hhi is None:
        return [_unknown_item("CONCENTRATION_HHI", "ratio", "NO_SECURITIES", scope, market)]
    top3 = sum(weights[:3], Decimal(0))
    return [
        _upper_threshold_item(
            "CONCENTRATION_TOP1",
            weights[0],
            policy.single_name_warning,
            policy.single_name_critical,
            scope,
            market,
        ),
        _threshold_item(
            "CONCENTRATION_TOP3",
            top3,
            "ratio",
            policy.top3_warning,
            scope,
            market,
            warning=top3 > policy.top3_warning,
        ),
        _item("CONCENTRATION_HHI", hhi, "ratio", None, scope, "info", "monitor", None, market),
    ]


def _exposure_items(
    values: list[tuple[BrokerPosition, Decimal]],
    scope: str,
    market: MarketEvidence,
    policy: RealAdvisoryPolicy,
) -> list[DiagnosticItem]:
    total = sum((value for _, value in values), Decimal(0))
    buckets: dict[str, Decimal] = defaultdict(lambda: Decimal(0))
    for position, value in values:
        definitions = market.risk_buckets.get(position.instrument_id)
        if not definitions:
            return [
                _unknown_item("RISK_BUCKET_EXPOSURE", "ratio", "RISK_BUCKET_MISSING", scope, market)
            ]
        for bucket, bucket_weight in definitions.items():
            buckets[bucket] += (value / total) * bucket_weight
    return [
        _upper_threshold_item(
            f"RISK_BUCKET_EXPOSURE:{bucket}",
            weight,
            policy.sector_warning,
            policy.sector_critical,
            scope,
            market,
        )
        for bucket, weight in sorted(buckets.items())
    ]


def _liquidity_items(
    values: list[tuple[BrokerPosition, Decimal]],
    scope: str,
    market: MarketEvidence,
    policy: RealAdvisoryPolicy,
) -> list[DiagnosticItem]:
    result: list[DiagnosticItem] = []
    for position, value in values:
        metric = days_to_exit(value, market.adv20.get(position.instrument_id))
        code = f"LIQUIDITY_DAYS_TO_EXIT:{position.instrument_id}"
        if metric is None:
            result.append(_unknown_item(code, "trading_days", "ADV20_MISSING", scope, market))
        else:
            result.append(
                _upper_threshold_item(
                    code,
                    metric,
                    policy.days_to_exit_warning,
                    policy.days_to_exit_critical,
                    scope,
                    market,
                    unit="trading_days",
                )
            )
    return result


def _market_risk_items(
    values: list[tuple[BrokerPosition, Decimal]],
    scope: str,
    market: MarketEvidence,
    policy: RealAdvisoryPolicy,
    snapshot: PortfolioSnapshot,
) -> list[DiagnosticItem]:
    if snapshot.as_of is None:
        return [_unknown_item("MARKET_RISK", "ratio", "SNAPSHOT_AS_OF_MISSING", scope, market)]
    benchmark = qualified_session_closes(market.benchmark, snapshot.as_of)
    returns_by_instrument: dict[UUID, tuple[Decimal, ...]] = {}
    items: list[DiagnosticItem] = []
    for position, _ in values:
        session_closes = qualified_session_closes(
            market.prices.get(position.instrument_id), snapshot.as_of
        )
        if session_closes is None:
            items.append(
                _unknown_item(
                    f"MARKET_RISK:{position.instrument_id}",
                    "ratio",
                    "PRICE_HISTORY_MISSING",
                    scope,
                    market,
                )
            )
            continue
        closes = tuple(session_closes[session] for session in sorted(session_closes))
        volatility20, volatility63, drawdown = (
            annualized_volatility(closes, 20),
            annualized_volatility(closes, 63),
            maximum_drawdown(closes),
        )
        beta_correlation = (
            None
            if benchmark is None
            else beta_and_correlation_by_session(session_closes, benchmark)
        )
        if None in (volatility20, volatility63, drawdown, beta_correlation):
            items.append(
                _unknown_item(
                    f"MARKET_RISK:{position.instrument_id}",
                    "ratio",
                    "MARKET_RISK_COVERAGE_INSUFFICIENT",
                    scope,
                    market,
                )
            )
            continue
        assert volatility20 is not None and volatility63 is not None and drawdown is not None
        assert beta_correlation is not None
        returns_by_instrument[position.instrument_id] = tuple(
            current / prior - Decimal(1)
            for prior, current in zip(closes, closes[1:])
            if prior > 0 and current > 0
        )
        items.extend(
            (
                _item(
                    f"VOLATILITY_20:{position.instrument_id}",
                    volatility20,
                    "annualized_ratio",
                    None,
                    scope,
                    "info",
                    "monitor",
                    None,
                    market,
                ),
                _item(
                    f"VOLATILITY_63:{position.instrument_id}",
                    volatility63,
                    "annualized_ratio",
                    None,
                    scope,
                    "info",
                    "monitor",
                    None,
                    market,
                ),
                _upper_threshold_item(
                    f"DRAWDOWN_252:{position.instrument_id}",
                    drawdown,
                    policy.drawdown_warning,
                    policy.drawdown_critical,
                    scope,
                    market,
                    inclusive=True,
                ),
                _item(
                    f"BETA_126:{position.instrument_id}",
                    beta_correlation[0],
                    "ratio",
                    None,
                    scope,
                    "info",
                    "monitor",
                    None,
                    market,
                ),
                _item(
                    f"CORRELATION_126:{position.instrument_id}",
                    beta_correlation[1],
                    "ratio",
                    None,
                    scope,
                    "info",
                    "monitor",
                    None,
                    market,
                ),
            )
        )
    components = correlation_components(returns_by_instrument)  # type: ignore[arg-type]
    if components:
        amounts = {position.instrument_id: value for position, value in values}
        total = sum(amounts.values(), Decimal(0))
        for component in components:
            weight = sum((amounts[item] for item in component), Decimal(0)) / total
            items.append(
                _threshold_item(
                    "CORRELATION_CLUSTER",
                    weight,
                    "ratio",
                    policy.correlation_cluster_warning,
                    scope,
                    market,
                    warning=weight > policy.correlation_cluster_warning,
                )
            )
    return items


def _upper_threshold_item(
    code: str,
    value: Decimal,
    warning_limit: Decimal,
    critical_limit: Decimal,
    scope: str,
    market: MarketEvidence,
    *,
    unit: str = "ratio",
    inclusive: bool = False,
) -> DiagnosticItem:
    if value >= critical_limit if inclusive else value > critical_limit:
        return _item(
            code, value, unit, critical_limit, scope, "critical", "reduce_only", None, market
        )
    if value >= warning_limit if inclusive else value > warning_limit:
        return _item(code, value, unit, warning_limit, scope, "warning", "research", None, market)
    return _item(code, value, unit, warning_limit, scope, "info", "monitor", None, market)


def _threshold_item(
    code: str,
    value: Decimal,
    unit: str,
    reference_limit: Decimal,
    scope: str,
    market: MarketEvidence,
    *,
    warning: bool,
) -> DiagnosticItem:
    severity: DiagnosticSeverity = "warning" if warning else "info"
    actionability: DiagnosticActionability = "research" if warning else "monitor"
    return _item(
        code,
        value,
        unit,
        reference_limit,
        scope,
        severity,
        actionability,
        None,
        market,
    )


def _unknown_item(
    code: str, unit: str, reason: str, scope: str, market: MarketEvidence
) -> DiagnosticItem:
    return DiagnosticItem(
        diagnostic_code=code,
        metric_value=None,
        unit=unit,
        reference_limit=None,
        policy_scope=scope,
        severity="info",
        actionability="research",
        unknown_reason=reason,
        evidence_refs=market.evidence_refs,
    )


def _item(
    code: str,
    value: Decimal,
    unit: str,
    reference_limit: Decimal | None,
    scope: str,
    severity: DiagnosticSeverity,
    actionability: DiagnosticActionability,
    reason: str | None,
    market: MarketEvidence,
) -> DiagnosticItem:
    return DiagnosticItem(
        diagnostic_code=code,
        metric_value=decimal_to_string(value),
        unit=unit,
        reference_limit=None if reference_limit is None else decimal_to_string(reference_limit),
        policy_scope=scope,
        severity=severity,
        actionability=actionability,
        unknown_reason=reason,
        evidence_refs=market.evidence_refs,
    )


def _report(
    snapshot: PortfolioSnapshot,
    policy: RealAdvisoryPolicy,
    optimization_need: OptimizationNeed,
    items: tuple[DiagnosticItem, ...],
) -> PortfolioDiagnosticReport:
    return PortfolioDiagnosticReport(
        portfolio_snapshot_id=snapshot.id,
        trading_day=None if snapshot.as_of is None else snapshot.as_of.date(),
        policy_version=policy.version,
        optimization_need=optimization_need,
        items=items,
    )


def _blocked_item(market: MarketEvidence, source_conflict: bool) -> DiagnosticItem:
    return _unknown_item(
        "SNAPSHOT_ADMISSION",
        "status",
        "SOURCE_CONFLICT" if source_conflict else "SNAPSHOT_NOT_READY",
        "portfolio",
        market,
    ).model_copy(update={"severity": "critical", "actionability": "blocked"})


def _market_unknown_item(market: MarketEvidence) -> DiagnosticItem:
    return _unknown_item(
        "MARKET_EVIDENCE", "status", "MARKET_EVIDENCE_UNAVAILABLE", "portfolio", market
    )
