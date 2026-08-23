from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

from nexus.diagnostics.evidence import EvidenceResolver
from nexus.diagnostics.market_risk import market_risk_items
from nexus.diagnostics.models import DiagnosticEvidence, EvidenceBinding, MarketEvidence
from nexus.diagnostics.policy import RealAdvisoryPolicy
from nexus.domain.evidence import EvidenceRef
from nexus.providers.base import PriceBar, PriceSeries
from nexus.providers.broker.base import BrokerPosition, PortfolioSnapshot

SNAPSHOT_AT = datetime(2026, 8, 21, 21, tzinfo=UTC)
FIRST = UUID("0198d500-1000-7000-8000-000000000061")
SECOND = UUID("0198d500-1000-7000-8000-000000000062")


def test_cluster_is_independent_of_beta_and_drawdown_coverage() -> None:
    sessions = tuple(date(2026, 6, 1) + timedelta(days=index) for index in range(61))
    first, second = _position(FIRST), _position(SECOND)
    first_series = _series(FIRST, sessions)
    second_series = _series(SECOND, sessions)
    evidence = DiagnosticEvidence(
        price_series={
            FIRST: _binding("first", sessions[-1]),
            SECOND: _binding("second", sessions[-1]),
        },
        benchmark=_binding("benchmark", sessions[-1]),
    )
    market = MarketEvidence(
        quality="PASS",
        prices={FIRST: first_series, SECOND: second_series},
        benchmark=first_series,
        trading_sessions=sessions,
        evidence=evidence,
    )
    snapshot = PortfolioSnapshot(
        id=UUID("0198d500-1000-7000-8000-000000000063"),
        portfolio_id=UUID("0198d500-1000-7000-8000-000000000064"),
        status="READY",
        cash=(),
        positions=(first, second),
        open_orders=(),
        as_of=SNAPSHOT_AT,
    )

    items = market_risk_items(
        [(first, Decimal("100")), (second, Decimal("100"))],
        "currency:USD",
        market,
        RealAdvisoryPolicy.v1(),
        snapshot,
        EvidenceResolver(SNAPSHOT_AT),
    )
    by_code = {item.diagnostic_code: item for item in items}

    assert "CORRELATION_CLUSTER" in by_code
    assert by_code[f"VOLATILITY_20:{FIRST}"].metric_value is not None
    assert by_code[f"VOLATILITY_63:{FIRST}"].metric_value is not None
    assert by_code[f"DRAWDOWN_252:{FIRST}"].unknown_reason == (
        "DRAWDOWN_252_COVERAGE_INSUFFICIENT"
    )
    assert by_code[f"BETA_126:{FIRST}"].unknown_reason == (
        "BETA_CORRELATION_126_COVERAGE_INSUFFICIENT"
    )
    assert by_code[f"CORRELATION_126:{FIRST}"].unknown_reason == (
        "BETA_CORRELATION_126_COVERAGE_INSUFFICIENT"
    )


def _position(instrument_id: UUID) -> BrokerPosition:
    return BrokerPosition(
        instrument_id=instrument_id,
        currency="USD",
        quantity=Decimal(1),
        available_quantity=Decimal(1),
        frozen_quantity=Decimal(0),
        avg_cost=Decimal("100"),
        diluted_cost=Decimal("100"),
        current_price=Decimal("100"),
        provider_as_of=SNAPSHOT_AT,
    )


def _series(instrument_id: UUID, sessions: tuple[date, ...]) -> PriceSeries:
    bars = tuple(
        PriceBar(
            instrument_id=instrument_id,
            session=session,
            open=Decimal(100 + index),
            high=Decimal(100 + index),
            low=Decimal(100 + index),
            close=Decimal(100 + index),
            volume=Decimal("1000"),
            observed_at=SNAPSHOT_AT,
            available_at=SNAPSHOT_AT,
            received_at=SNAPSHOT_AT,
            revision="fixture-r1",
        )
        for index, session in enumerate(sessions)
    )
    return PriceSeries(
        source="fixture",
        feed="fixture",
        adjustment="all",
        bars=bars,
        mode="fixture",
        delivery_mode="fixture",
        quality="PASS",
    )


def _binding(name: str, session: date) -> EvidenceBinding:
    ref = EvidenceRef(
        source="fixture",
        object_id=name,
        available_at=SNAPSHOT_AT,
        location=f"fixture://{name}",
        content_hash=sha256(name.encode()).hexdigest(),
    )
    return EvidenceBinding(
        available_at=SNAPSHOT_AT,
        as_of=SNAPSHOT_AT,
        session=session,
        refs=(ref,),
    )
