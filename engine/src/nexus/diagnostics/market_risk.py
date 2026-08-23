"""Evidence-qualified market-risk diagnostic evaluation."""

from datetime import date
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

from nexus.diagnostics.evidence import EvidenceResolver, combine_refs
from nexus.diagnostics.items import item, threshold_item, unknown_item, upper_threshold_item
from nexus.diagnostics.metrics import (
    annualized_volatility,
    beta_and_correlation_by_session,
    correlation_components_by_session,
    maximum_drawdown,
    published_interval_returns,
    qualified_session_closes,
)
from nexus.diagnostics.models import DiagnosticItem, MarketEvidence
from nexus.diagnostics.policy import RealAdvisoryPolicy
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
    benchmark = qualified_session_closes(market.benchmark, snapshot.as_of)
    benchmark_session = None if not benchmark else max(benchmark)
    benchmark_evidence = resolver.resolve(
        market.evidence.benchmark,
        "BENCHMARK",
        expected_session=benchmark_session,
    )
    returns_by_instrument: dict[UUID, dict[date, Decimal]] = {}
    items: list[DiagnosticItem] = []
    for position, _ in values:
        session_closes = qualified_session_closes(
            market.prices.get(position.instrument_id), snapshot.as_of
        )
        price_session = None if not session_closes else max(session_closes)
        price_evidence = resolver.resolve(
            market.evidence.price_series.get(position.instrument_id),
            "PRICE_SERIES",
            expected_session=price_session,
        )
        if session_closes is None or price_evidence.reason is not None:
            items.append(
                unknown_item(
                    f"MARKET_RISK:{position.instrument_id}",
                    "ratio",
                    price_evidence.reason or "PRICE_HISTORY_MISSING",
                    scope,
                    market,
                )
            )
            continue
        if benchmark_evidence.reason is not None:
            items.append(
                unknown_item(
                    f"MARKET_RISK:{position.instrument_id}",
                    "ratio",
                    benchmark_evidence.reason,
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
            else beta_and_correlation_by_session(
                session_closes,
                benchmark,
                published_sessions=market.trading_sessions,
            )
        )
        if None in (volatility20, volatility63, drawdown, beta_correlation):
            items.append(
                unknown_item(
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
        returns_by_instrument[position.instrument_id] = published_interval_returns(
            session_closes, market.trading_sessions
        )
        market_refs = combine_refs(price_evidence.refs, benchmark_evidence.refs)
        items.extend(
            (
                item(
                    f"VOLATILITY_20:{position.instrument_id}",
                    volatility20,
                    "annualized_ratio",
                    None,
                    scope,
                    "info",
                    "monitor",
                    None,
                    market,
                    evidence_refs=price_evidence.refs,
                ),
                item(
                    f"VOLATILITY_63:{position.instrument_id}",
                    volatility63,
                    "annualized_ratio",
                    None,
                    scope,
                    "info",
                    "monitor",
                    None,
                    market,
                    evidence_refs=price_evidence.refs,
                ),
                upper_threshold_item(
                    f"DRAWDOWN_252:{position.instrument_id}",
                    drawdown,
                    policy.drawdown_warning,
                    policy.drawdown_critical,
                    scope,
                    market,
                    inclusive=True,
                    evidence_refs=price_evidence.refs,
                ),
                item(
                    f"BETA_126:{position.instrument_id}",
                    beta_correlation[0],
                    "ratio",
                    None,
                    scope,
                    "info",
                    "monitor",
                    None,
                    market,
                    evidence_refs=market_refs,
                ),
                item(
                    f"CORRELATION_126:{position.instrument_id}",
                    beta_correlation[1],
                    "ratio",
                    None,
                    scope,
                    "info",
                    "monitor",
                    None,
                    market,
                    evidence_refs=market_refs,
                ),
            )
        )
    _append_correlation_clusters(
        items, values, returns_by_instrument, scope, market, policy, resolver
    )
    return items


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
