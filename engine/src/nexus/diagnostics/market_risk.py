"""Evidence-qualified market-risk diagnostic evaluation."""

from datetime import date
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

from nexus.diagnostics.evidence import EvidenceResolver, combine_refs
from nexus.diagnostics.items import item, threshold_item, unknown_item, upper_threshold_item
from nexus.diagnostics.metrics import (
    annualized_volatility_by_session,
    beta_and_correlation_by_session,
    correlation_components_by_session,
    maximum_drawdown_by_session,
    published_interval_returns,
    qualified_session_closes,
)
from nexus.diagnostics.models import DiagnosticItem, MarketEvidence
from nexus.diagnostics.policy import RealAdvisoryPolicy
from nexus.domain.evidence import EvidenceRef
from nexus.providers.broker.base import BrokerPosition, PortfolioSnapshot


def market_risk_items(
    values: list[tuple[BrokerPosition, Decimal]],
    scope: str,
    market: MarketEvidence,
    policy: RealAdvisoryPolicy,
    snapshot: PortfolioSnapshot,
    resolver: EvidenceResolver,
) -> list[DiagnosticItem]:
    if snapshot.as_of is None:
        return [unknown_item("MARKET_RISK", "ratio", "SNAPSHOT_AS_OF_MISSING", scope, market)]
    benchmark_visible = qualified_session_closes(market.benchmark, snapshot.as_of)
    benchmark_session = None if not benchmark_visible else max(benchmark_visible)
    benchmark_evidence = resolver.resolve(
        market.evidence.benchmark,
        "BENCHMARK",
        expected_session=benchmark_session,
    )
    returns_by_instrument: dict[UUID, dict[date, Decimal]] = {}
    items: list[DiagnosticItem] = []
    for position, _ in values:
        visible_closes = qualified_session_closes(
            market.prices.get(position.instrument_id), snapshot.as_of
        )
        price_session = None if not visible_closes else max(visible_closes)
        price_evidence = resolver.resolve(
            market.evidence.price_series.get(position.instrument_id),
            "PRICE_SERIES",
            expected_session=price_session,
        )
        if visible_closes is None or price_evidence.reason is not None:
            items.extend(
                _unknown_metrics(
                    position.instrument_id,
                    price_evidence.reason or "PRICE_HISTORY_MISSING",
                    scope,
                    market,
                    price_evidence.refs,
                )
            )
            continue
        session_closes = {
            session: visible_closes[session]
            for session in market.trading_sessions
            if session in visible_closes
        }
        returns_by_instrument[position.instrument_id] = published_interval_returns(
            session_closes, market.trading_sessions
        )
        volatility20 = annualized_volatility_by_session(
            session_closes, market.trading_sessions, 20
        )
        volatility63 = annualized_volatility_by_session(
            session_closes, market.trading_sessions, 63
        )
        drawdown = maximum_drawdown_by_session(
            session_closes, market.trading_sessions
        )
        _append_single_series_metrics(
            items,
            position.instrument_id,
            volatility20,
            volatility63,
            drawdown,
            scope,
            market,
            policy,
            price_evidence.refs,
        )
        benchmark = (
            None
            if benchmark_visible is None
            else {
                session: benchmark_visible[session]
                for session in market.trading_sessions
                if session in benchmark_visible
            }
        )
        beta_correlation = (
            None
            if benchmark is None or benchmark_evidence.reason is not None
            else beta_and_correlation_by_session(
                session_closes,
                benchmark,
                published_sessions=market.trading_sessions,
            )
        )
        market_refs = combine_refs(price_evidence.refs, benchmark_evidence.refs)
        if beta_correlation is None:
            reason = (
                benchmark_evidence.reason
                or "BETA_CORRELATION_126_COVERAGE_INSUFFICIENT"
            )
            items.extend(
                unknown_item(
                    f"{family}:{position.instrument_id}",
                    "ratio",
                    reason,
                    scope,
                    market,
                    evidence_refs=market_refs,
                )
                for family in ("BETA_126", "CORRELATION_126")
            )
        else:
            items.extend(
                item(
                    f"{family}:{position.instrument_id}",
                    value,
                    "ratio",
                    None,
                    scope,
                    "info",
                    "monitor",
                    None,
                    market,
                    evidence_refs=market_refs,
                )
                for family, value in zip(
                    ("BETA_126", "CORRELATION_126"), beta_correlation, strict=True
                )
            )
    _append_correlation_clusters(
        items, values, returns_by_instrument, scope, market, policy, resolver
    )
    return items


def _unknown_metrics(
    instrument_id: UUID,
    reason: str,
    scope: str,
    market: MarketEvidence,
    refs: tuple[EvidenceRef, ...],
) -> tuple[DiagnosticItem, ...]:
    return tuple(
        unknown_item(
            f"{family}:{instrument_id}",
            unit,
            reason,
            scope,
            market,
            evidence_refs=refs,
        )
        for family, unit in (
            ("VOLATILITY_20", "annualized_ratio"),
            ("VOLATILITY_63", "annualized_ratio"),
            ("DRAWDOWN_252", "ratio"),
            ("BETA_126", "ratio"),
            ("CORRELATION_126", "ratio"),
        )
    )


def _append_single_series_metrics(
    items: list[DiagnosticItem],
    instrument_id: UUID,
    volatility20: Decimal | None,
    volatility63: Decimal | None,
    drawdown: Decimal | None,
    scope: str,
    market: MarketEvidence,
    policy: RealAdvisoryPolicy,
    refs: tuple[EvidenceRef, ...],
) -> None:
    for family, value in (
        ("VOLATILITY_20", volatility20),
        ("VOLATILITY_63", volatility63),
    ):
        if value is None:
            items.append(
                unknown_item(
                    f"{family}:{instrument_id}",
                    "annualized_ratio",
                    f"{family}_COVERAGE_INSUFFICIENT",
                    scope,
                    market,
                    evidence_refs=refs,
                )
            )
        else:
            items.append(
                item(
                    f"{family}:{instrument_id}",
                    value,
                    "annualized_ratio",
                    None,
                    scope,
                    "info",
                    "monitor",
                    None,
                    market,
                    evidence_refs=refs,
                )
            )
    if drawdown is None:
        items.append(
            unknown_item(
                f"DRAWDOWN_252:{instrument_id}",
                "ratio",
                "DRAWDOWN_252_COVERAGE_INSUFFICIENT",
                scope,
                market,
                evidence_refs=refs,
            )
        )
    else:
        items.append(
            upper_threshold_item(
                f"DRAWDOWN_252:{instrument_id}",
                drawdown,
                policy.drawdown_warning,
                policy.drawdown_critical,
                scope,
                market,
                inclusive=True,
                evidence_refs=refs,
            )
        )


def _append_correlation_clusters(
    items: list[DiagnosticItem],
    values: list[tuple[BrokerPosition, Decimal]],
    returns_by_instrument: dict[UUID, dict[date, Decimal]],
    scope: str,
    market: MarketEvidence,
    policy: RealAdvisoryPolicy,
    resolver: EvidenceResolver,
) -> None:
    amounts = {position.instrument_id: value for position, value in values}
    total = sum(amounts.values(), Decimal(0))
    for component in correlation_components_by_session(returns_by_instrument):
        weight = sum((amounts[instrument_id] for instrument_id in component), Decimal(0)) / total
        refs = combine_refs(
            *(
                resolver.resolve(
                    market.evidence.price_series.get(instrument_id), "PRICE_SERIES"
                ).refs
                for instrument_id in component
            )
        )
        items.append(
            threshold_item(
                "CORRELATION_CLUSTER",
                weight,
                "ratio",
                policy.correlation_cluster_warning,
                correlation_cluster_scope(scope, component),
                market,
                warning=weight > policy.correlation_cluster_warning,
                evidence_refs=refs,
            )
        )


def correlation_cluster_scope(scope: str, component: tuple[UUID, ...]) -> str:
    """Bind a cluster to its currency scope and stable connected-component identity."""
    identity = "|".join(str(instrument_id) for instrument_id in sorted(component, key=str))
    return f"{scope}:correlation-cluster:{sha256(identity.encode()).hexdigest()[:16]}"
