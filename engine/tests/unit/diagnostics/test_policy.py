from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.diagnostics.items import upper_threshold_item as _upper_threshold_item
from nexus.diagnostics.models import (
    ComparableVariantEvidence,
    DiagnosticEvidence,
    DiagnosticHistory,
    DiagnosticItem,
    EvidenceBinding,
    InstrumentIdentity,
    MarketEvidence,
    PortfolioDiagnosticReport,
)
from nexus.diagnostics.policy import RealAdvisoryPolicy
from nexus.diagnostics.service import (
    _assert_fosun_read_only_scope,
    _optimization_need,
    diagnose_portfolio,
)
from nexus.domain.evidence import EvidenceRef
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


def test_days_to_exit_uses_only_its_item_specific_adv_evidence() -> None:
    """Replacing the ADV binding with global evidence must fail this audit boundary."""
    snapshot = _ready_snapshot()
    instrument_id = snapshot.positions[0].instrument_id
    adv_ref = _evidence_ref("adv20", "a")
    market = _complete_market(snapshot).model_copy(
        update={
            "evidence": _complete_market(snapshot).evidence.model_copy(
                update={"adv20": {instrument_id: _binding(adv_ref)}}
            ),
        }
    )

    report = diagnose_portfolio(
        snapshot, market, DiagnosticHistory.empty(), RealAdvisoryPolicy.v1()
    )
    item = next(
        item
        for item in report.items
        if item.diagnostic_code == f"LIQUIDITY_DAYS_TO_EXIT:{instrument_id}"
    )

    assert item.unknown_reason is None
    assert item.evidence_refs == (
        market.evidence.snapshot.refs[0],  # type: ignore[union-attr]
        market.evidence.position_prices[instrument_id].refs[0],
        adv_ref,
    )


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
                evidence_refs=(
                    EvidenceRef(
                        source="fixture",
                        object_id="comparison-1",
                        available_at=datetime(2026, 8, 23, tzinfo=UTC),
                        location="fixture://comparison-1",
                        content_hash="b" * 64,
                    ),
                ),
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


def test_drawdown_thresholds_are_inclusive_at_eight_and_fifteen_percent() -> None:
    market = MarketEvidence(quality="PASS")
    policy = RealAdvisoryPolicy.v1()

    warning = _upper_threshold_item(
        "DRAWDOWN_252",
        Decimal("0.08"),
        policy.drawdown_warning,
        policy.drawdown_critical,
        "currency:USD",
        market,
        inclusive=True,
    )
    critical = _upper_threshold_item(
        "DRAWDOWN_252",
        Decimal("0.15"),
        policy.drawdown_warning,
        policy.drawdown_critical,
        "currency:USD",
        market,
        inclusive=True,
    )

    assert warning.severity == "warning"
    assert critical.severity == "critical"


def test_warning_persistence_requires_the_four_previous_trading_days_and_scope() -> None:
    item = _warning_item()
    history = DiagnosticHistory(
        prior_reports=tuple(
            _prior_report(item, trading_day)
            for trading_day in (
                date(2026, 8, 19),
                date(2026, 8, 20),
                date(2026, 8, 21),
                date(2026, 8, 24),
            )
        ),
        comparable_variants=(_comparison_evidence(),),
    )

    sessions = (
        date(2026, 8, 19),
        date(2026, 8, 20),
        date(2026, 8, 21),
        date(2026, 8, 24),
        date(2026, 8, 25),
    )
    assert (
        _optimization_need((item,), history, date(2026, 8, 25), sessions)
        == "OPTIMIZATION_CANDIDATE"
    )

    wrong_scope = history.model_copy(
        update={
            "prior_reports": history.prior_reports[:-1]
            + (
                _prior_report(
                    item.model_copy(update={"policy_scope": "currency:HKD"}), date(2026, 8, 24)
                ),
            )
        }
    )
    assert _optimization_need((item,), wrong_scope, date(2026, 8, 25), sessions) == "NO_ACTION"

    gap = history.model_copy(
        update={
            "prior_reports": history.prior_reports[:-1] + (_prior_report(item, date(2026, 8, 21)),)
        }
    )
    assert _optimization_need((item,), gap, date(2026, 8, 25), sessions) == "NO_ACTION"


def test_market_risk_fails_closed_below_eighty_percent_sample_coverage() -> None:
    snapshot = _ready_snapshot()
    market = _complete_market(snapshot).model_copy(
        update={"prices": {snapshot.positions[0].instrument_id: _series(snapshot, 201)}}
    )

    report = diagnose_portfolio(
        snapshot, market, DiagnosticHistory.empty(), RealAdvisoryPolicy.v1()
    )

    assert any(item.unknown_reason == "MARKET_RISK_COVERAGE_INSUFFICIENT" for item in report.items)
    assert report.optimization_need == "RESEARCH_REQUIRED"


def test_risk_bucket_weights_are_exact_and_do_not_duplicate_multi_label_exposure() -> None:
    snapshot = _ready_snapshot()
    instrument_id = snapshot.positions[0].instrument_id
    market = _complete_market(snapshot).model_copy(
        update={
            "risk_buckets": {instrument_id: {"equity": Decimal("0.6"), "factor": Decimal("0.4")}}
        }
    )

    report = diagnose_portfolio(
        snapshot, market, DiagnosticHistory.empty(), RealAdvisoryPolicy.v1()
    )
    buckets = {
        item.diagnostic_code: item.metric_value
        for item in report.items
        if item.diagnostic_code.startswith("RISK_BUCKET_EXPOSURE")
    }

    assert buckets == {"RISK_BUCKET_EXPOSURE:equity": "0.6", "RISK_BUCKET_EXPOSURE:factor": "0.4"}


def test_market_evidence_rejects_risk_bucket_weights_that_do_not_sum_to_one() -> None:
    with pytest.raises(ValueError, match="sum to one"):
        MarketEvidence(
            quality="PASS",
            risk_buckets={UUID("0198d500-1000-7000-8000-000000000013"): {"equity": Decimal("0.9")}},
        )


def test_market_evidence_rejects_nested_binary_float_inputs() -> None:
    instrument_id = UUID("0198d500-1000-7000-8000-000000000013")

    with pytest.raises(TypeError, match="binary floats"):
        MarketEvidence(quality="PASS", adv20={instrument_id: 0.1})
    with pytest.raises(TypeError, match="binary floats"):
        MarketEvidence(
            quality="PASS",
            risk_buckets={instrument_id: {"equity": 1.0}},
        )


def test_nonpositive_current_price_is_explicit_unknown_not_a_metric_exception() -> None:
    base_snapshot = _ready_snapshot()
    position = base_snapshot.positions[0].model_copy(update={"current_price": Decimal(0)})
    snapshot = base_snapshot.model_copy(update={"positions": (position,)})

    report = diagnose_portfolio(
        snapshot, _complete_market(snapshot), DiagnosticHistory.empty(), RealAdvisoryPolicy.v1()
    )

    assert report.optimization_need == "RESEARCH_REQUIRED"
    assert any(item.unknown_reason == "POSITION_PRICE_INVALID" for item in report.items)


def test_future_session_with_early_availability_blocks_as_source_conflict() -> None:
    snapshot = _ready_snapshot()
    instrument_id = snapshot.positions[0].instrument_id
    future = PriceBar(
        instrument_id=instrument_id,
        session=date(2026, 8, 24),
        open=Decimal("100"),
        high=Decimal("100"),
        low=Decimal("100"),
        close=Decimal("100"),
        volume=Decimal("1"),
        available_at=snapshot.as_of - timedelta(days=1),
    )
    market = _complete_market(snapshot).model_copy(
        update={
            "prices": {
                instrument_id: _complete_market(snapshot)
                .prices[instrument_id]
                .model_copy(
                    update={
                        "bars": (_complete_market(snapshot).prices[instrument_id].bars[0], future)
                    }
                )
            }
        }
    )

    report = diagnose_portfolio(
        snapshot, market, DiagnosticHistory.empty(), RealAdvisoryPolicy.v1()
    )

    assert report.optimization_need == "BLOCKED"
    assert report.items[0].unknown_reason == "SOURCE_CONFLICT"


def test_00656_hk_guard_rejects_any_future_fundamental_diagnostic() -> None:
    snapshot = _ready_snapshot()
    market = _complete_market(snapshot).model_copy(
        update={
            "identities": {
                snapshot.positions[0].instrument_id: InstrumentIdentity.fosun_read_only(
                    snapshot.positions[0].instrument_id
                )
            }
        }
    )
    prohibited = _warning_item().model_copy(update={"diagnostic_code": "FUNDAMENTAL_BUY"})

    with pytest.raises(ValueError, match="00656.HK"):
        _assert_fosun_read_only_scope(snapshot, market, [prohibited])


def _warning_item() -> DiagnosticItem:
    return DiagnosticItem(
        diagnostic_code="LIQUIDITY_DAYS_TO_EXIT:fixture",
        metric_value="3.000000000000",
        unit="trading_days",
        reference_limit="2.000000000000",
        policy_scope="currency:USD",
        severity="warning",
        actionability="research",
        unknown_reason=None,
        evidence_refs=(),
    )


def _prior_report(item: DiagnosticItem, trading_day: date) -> PortfolioDiagnosticReport:
    return PortfolioDiagnosticReport(
        portfolio_snapshot_id=UUID("0198d500-1000-7000-8000-000000000011"),
        trading_day=trading_day,
        policy_version="real_advisory_policy_v1",
        optimization_need="NO_ACTION",
        items=(item,),
    )


def _comparison_evidence() -> ComparableVariantEvidence:
    return ComparableVariantEvidence(
        variant="minimum_change",
        proposal_group_id=UUID("0198d500-1000-7000-8000-000000000014"),
        evidence_refs=(
            EvidenceRef(
                source="fixture",
                object_id="comparison-2",
                available_at=datetime(2026, 8, 23, tzinfo=UTC),
                location="fixture://comparison-2",
                content_hash="c" * 64,
            ),
        ),
    )


def _evidence_ref(object_id: str, hash_character: str) -> EvidenceRef:
    return EvidenceRef(
        source="fixture",
        object_id=object_id,
        available_at=datetime(2026, 8, 23, tzinfo=UTC),
        location=f"fixture://{object_id}",
        content_hash=hash_character * 64,
    )


def _binding(ref: EvidenceRef, *, session: date | None = None) -> EvidenceBinding:
    return EvidenceBinding(
        available_at=ref.available_at,
        as_of=ref.available_at,
        session=session,
        refs=(ref,),
    )


def _complete_market(snapshot: PortfolioSnapshot) -> MarketEvidence:
    instrument_id = snapshot.positions[0].instrument_id
    series = _series(snapshot, 253)
    last_session = series.bars[-1].session
    return MarketEvidence(
        prices={instrument_id: series},
        adv20={instrument_id: Decimal("1000")},
        quality="PASS",
        risk_buckets={instrument_id: {"equity": Decimal(1)}},
        identities={instrument_id: InstrumentIdentity.standard(instrument_id)},
        trading_sessions=tuple(bar.session for bar in series.bars),
        benchmark=series,
        evidence=DiagnosticEvidence(
            snapshot=_binding(_evidence_ref("snapshot", "1")),
            cash={"USD": _binding(_evidence_ref("cash", "2"))},
            position_prices={instrument_id: _binding(_evidence_ref("position", "3"))},
            adv20={instrument_id: _binding(_evidence_ref("adv20", "4"))},
            risk_buckets={instrument_id: _binding(_evidence_ref("risk-bucket", "5"))},
            price_series={
                instrument_id: _binding(_evidence_ref("prices", "6"), session=last_session)
            },
            benchmark=_binding(_evidence_ref("benchmark", "7"), session=last_session),
            trading_calendar=_binding(
                _evidence_ref("trading-calendar", "8"), session=last_session
            ),
        ),
    )


def _series(snapshot: PortfolioSnapshot, count: int) -> PriceSeries:
    instrument_id = snapshot.positions[0].instrument_id
    bars = tuple(
        PriceBar(
            instrument_id=instrument_id,
            session=snapshot.as_of.date() - timedelta(days=count - index),
            open=Decimal("100") + Decimal(index % 2),
            high=Decimal("101") + Decimal(index % 2),
            low=Decimal("99") + Decimal(index % 2),
            close=Decimal("100") + Decimal(index % 2),
            volume=Decimal("1000"),
            available_at=datetime(2026, 8, 23, tzinfo=UTC),
        )
        for index in range(count)
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
    return series
