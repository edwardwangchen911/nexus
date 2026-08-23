from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from nexus.diagnostics.models import ComparableVariantEvidence, DiagnosticHistory, MarketEvidence
from nexus.diagnostics.policy import RealAdvisoryPolicy
from nexus.diagnostics.service import diagnose_portfolio
from nexus.providers.base import PriceBar, PriceSeries
from nexus.providers.broker.base import BrokerPosition, CashBalance, PortfolioSnapshot


def _ready_snapshot() -> PortfolioSnapshot:
    return PortfolioSnapshot(
        id=UUID("0198d500-1000-7000-8000-000000000011"),
        portfolio_id=UUID("0198d500-1000-7000-8000-000000000012"),
        status="READY",
        cash=(
            CashBalance(
                currency="USD",
                cash_balance=Decimal("100"),
                available_cash=Decimal("100"),
                provider_as_of=datetime(2026, 8, 23, tzinfo=UTC),
            ),
        ),
        positions=(
            BrokerPosition(
                instrument_id=UUID("0198d500-1000-7000-8000-000000000013"),
                currency="USD",
                quantity=Decimal("1"),
                available_quantity=Decimal("1"),
                frozen_quantity=Decimal("0"),
                avg_cost=Decimal("100"),
                diluted_cost=Decimal("100"),
                current_price=Decimal("100"),
                provider_as_of=datetime(2026, 8, 23, tzinfo=UTC),
            ),
        ),
        open_orders=(),
        as_of=datetime(2026, 8, 23, tzinfo=UTC),
    )


def test_unknown_required_metric_cannot_be_no_action() -> None:
    """Removing ADV unknown propagation must fail this readiness safeguard."""
    report = diagnose_portfolio(
        _ready_snapshot(),
        MarketEvidence.missing_adv(),
        DiagnosticHistory.empty(),
        RealAdvisoryPolicy.v1(),
    )

    assert report.optimization_need == "RESEARCH_REQUIRED"
    assert any(item.unknown_reason == "ADV20_MISSING" for item in report.items)


def test_non_ready_snapshot_is_blocked_before_any_metric_evaluation() -> None:
    """Changing blocked precedence to research-required must fail this admission guard."""
    report = diagnose_portfolio(
        _ready_snapshot().model_copy(update={"status": "RECONCILIATION_REQUIRED"}),
        MarketEvidence(quality="PASS"),
        DiagnosticHistory.empty(),
        RealAdvisoryPolicy.v1(),
    )

    assert report.optimization_need == "BLOCKED"
    assert report.items[0].unknown_reason == "SNAPSHOT_NOT_READY"
    assert report.items[0].actionability == "blocked"


def test_source_conflict_is_blocked_even_for_a_ready_snapshot() -> None:
    report = diagnose_portfolio(
        _ready_snapshot(),
        MarketEvidence(quality="PASS", source_conflict=True),
        DiagnosticHistory.empty(),
        RealAdvisoryPolicy.v1(),
    )

    assert report.optimization_need == "BLOCKED"
    assert report.items[0].unknown_reason == "SOURCE_CONFLICT"


def test_critical_trigger_requires_comparable_non_hold_evidence() -> None:
    """Treating a critical item as a candidate without comparison evidence must fail here."""
    snapshot = _ready_snapshot()
    report = diagnose_portfolio(
        snapshot,
        _complete_market(snapshot),
        DiagnosticHistory.empty(),
        RealAdvisoryPolicy.v1(),
    )

    assert any(item.severity == "critical" for item in report.items)
    assert report.optimization_need == "RESEARCH_REQUIRED"


def test_critical_trigger_becomes_candidate_only_with_non_hold_comparison_evidence() -> None:
    snapshot = _ready_snapshot()
    history = DiagnosticHistory(
        comparable_variants=(
            ComparableVariantEvidence(
                variant="minimum_change",
                proposal_group_id=UUID("0198d500-1000-7000-8000-000000000014"),
            ),
        ),
    )
    report = diagnose_portfolio(
        snapshot,
        _complete_market(snapshot),
        history,
        RealAdvisoryPolicy.v1(),
    )

    assert report.optimization_need == "OPTIMIZATION_CANDIDATE"


def _complete_market(snapshot: PortfolioSnapshot) -> MarketEvidence:
    instrument_id = snapshot.positions[0].instrument_id
    bars = tuple(
        PriceBar(
            instrument_id=instrument_id,
            session=date(2026, 1, 1) + timedelta(days=index),
            open=Decimal("100") + Decimal(index % 2),
            high=Decimal("101") + Decimal(index % 2),
            low=Decimal("99") + Decimal(index % 2),
            close=Decimal("100") + Decimal(index % 2),
            volume=Decimal("1000"),
            available_at=datetime(2026, 8, 23, tzinfo=UTC),
        )
        for index in range(253)
    )
    series = PriceSeries(
        source="fixture",
        feed="fixture",
        adjustment="all",
        bars=bars,
        mode="fixture",
        delivery_mode="fixture",
        quality="PASS",
    )
    return MarketEvidence(
        prices={instrument_id: series},
        adv20={instrument_id: Decimal("1000")},
        quality="PASS",
        risk_buckets={instrument_id: ("equity",)},
        benchmark=series,
    )
