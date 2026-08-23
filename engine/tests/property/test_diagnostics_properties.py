from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from hypothesis import assume, given, settings
from hypothesis import strategies as st
from nexus.diagnostics.decision import optimization_need
from nexus.diagnostics.metrics import (
    annualized_volatility,
    beta_and_correlation_by_session,
    correlation_components_by_session,
    days_to_exit,
    maximum_drawdown,
    security_hhi,
    top_weights,
)
from nexus.diagnostics.models import (
    ComparableVariantEvidence,
    DiagnosticHistory,
    DiagnosticItem,
    PortfolioDiagnosticReport,
)
from nexus.domain.evidence import EvidenceRef

VALUES = st.lists(st.integers(min_value=1, max_value=10_000), min_size=1, max_size=20)


@given(VALUES)
def test_top1_top3_and_hhi_match_exact_security_weights(values: list[int]) -> None:
    """Wrong cash inclusion, sort order, or normalization must fail this property."""
    decimals = tuple(Decimal(value) for value in values)
    total = sum(decimals, Decimal(0))
    weights = top_weights(decimals)
    hhi = security_hhi(decimals)

    assert weights is not None and hhi is not None
    assert weights == tuple(sorted((value / total for value in decimals), reverse=True))
    expected_top3 = sum(sorted(decimals, reverse=True)[:3], Decimal(0)) / total
    assert abs(sum(weights[:3], Decimal(0)) - expected_top3) <= Decimal("1e-26")
    assert hhi == sum(((value / total) ** 2 for value in decimals), Decimal(0))
    assert Decimal(1) / Decimal(len(values)) - Decimal("1e-26") <= hhi <= Decimal(1)


@given(
    st.lists(st.integers(min_value=1, max_value=10_000), min_size=64, max_size=80),
    st.integers(min_value=1, max_value=100),
    st.sampled_from((20, 63)),
)
@settings(max_examples=30, deadline=None)
def test_volatility_20_and_63_are_nonnegative_and_scale_invariant(
    closes: list[int], scale: int, window: int
) -> None:
    exact = tuple(Decimal(value) for value in closes)
    scaled = tuple(value * Decimal(scale) for value in exact)

    result = annualized_volatility(exact, window)
    assert result is not None and result >= 0
    assert annualized_volatility(scaled, window) == result


@given(
    st.lists(st.integers(min_value=1, max_value=10_000), min_size=252, max_size=260),
    st.integers(min_value=1, max_value=100),
)
@settings(max_examples=25, deadline=None)
def test_drawdown_252_is_bounded_and_scale_invariant(closes: list[int], scale: int) -> None:
    exact = tuple(Decimal(value) for value in closes)
    result = maximum_drawdown(exact)

    assert result is not None and Decimal(0) <= result < Decimal(1)
    assert maximum_drawdown(tuple(value * Decimal(scale) for value in exact)) == result


@given(
    st.lists(st.integers(min_value=1, max_value=10_000), min_size=127, max_size=135),
    st.lists(st.integers(min_value=1, max_value=10_000), min_size=127, max_size=135),
)
@settings(max_examples=30, deadline=None)
def test_beta_and_correlation_126_are_finite_and_correlation_is_bounded(
    asset: list[int], benchmark: list[int]
) -> None:
    assume(len(set(asset)) > 1 and len(set(benchmark)) > 1)
    start = date(2025, 1, 1)
    sessions = tuple(
        start + timedelta(days=index) for index in range(min(len(asset), len(benchmark)))
    )
    result = beta_and_correlation_by_session(
        dict(zip(sessions, (Decimal(value) for value in asset), strict=False)),
        dict(zip(sessions, (Decimal(value) for value in benchmark), strict=False)),
        published_sessions=sessions,
    )

    assume(result is not None)
    beta, correlation = result
    assert beta.is_finite()
    assert correlation.is_finite()
    assert Decimal(-1) <= correlation <= Decimal(1)


@given(
    st.integers(min_value=1, max_value=10_000),
    st.integers(min_value=15, max_value=31),
)
@settings(max_examples=40, deadline=None)
def test_production_cluster_straddles_exact_point_eight_correlation(
    scale: int, repeats: int,
) -> None:
    left = UUID("0198d500-1000-7000-8000-000000000001")
    right = UUID("0198d500-1000-7000-8000-000000000002")
    sessions = tuple(
        date(2026, 1, 1) + timedelta(days=index) for index in range(repeats * 4)
    )
    base_left = (Decimal(1), Decimal(-1), Decimal(1), Decimal(-1)) * repeats
    orthogonal = (Decimal(1), Decimal(1), Decimal(-1), Decimal(-1)) * repeats
    factor = Decimal(scale)
    left_values = tuple(value * factor for value in base_left)
    exact_values = tuple(
        (Decimal(4) * first + Decimal(3) * other) * factor
        for first, other in zip(base_left, orthogonal, strict=True)
    )
    below_values = tuple(
        (Decimal(3) * first + Decimal(4) * other) * factor
        for first, other in zip(base_left, orthogonal, strict=True)
    )
    above_values = tuple(
        (Decimal(5) * first + Decimal(3) * other) * factor
        for first, other in zip(base_left, orthogonal, strict=True)
    )
    series = dict(zip(sessions, left_values, strict=True))
    exact = dict(zip(sessions, exact_values, strict=True))
    below = dict(zip(sessions, below_values, strict=True))
    above = dict(zip(sessions, above_values, strict=True))

    assert _correlation(left_values, exact_values) == Decimal("0.8")
    assert _correlation(left_values, below_values) < Decimal("0.8")
    assert _correlation(left_values, above_values) > Decimal("0.8")
    assert correlation_components_by_session({left: series, right: exact}) == (
        (left, right),
    )
    assert correlation_components_by_session({left: series, right: below}) == ()
    assert correlation_components_by_session({left: series, right: above}) == (
        (left, right),
    )


@given(st.integers(min_value=1, max_value=10_000))
def test_correlation_cluster_straddles_fifty_nine_and_sixty_overlap(scale: int) -> None:
    left = UUID("0198d500-1000-7000-8000-000000000001")
    right = UUID("0198d500-1000-7000-8000-000000000002")
    sessions = tuple(date(2026, 1, 1) + timedelta(days=index) for index in range(60))
    series = {session: Decimal(index * scale) for index, session in enumerate(sessions)}

    assert correlation_components_by_session(
        {left: dict(tuple(series.items())[:59]), right: dict(tuple(series.items())[:59])}
    ) == ()
    assert correlation_components_by_session({left: series, right: dict(series)}) == (
        (left, right),
    )


def _correlation(left: tuple[Decimal, ...], right: tuple[Decimal, ...]) -> Decimal:
    count = Decimal(len(left))
    left_mean = sum(left, Decimal(0)) / count
    right_mean = sum(right, Decimal(0)) / count
    covariance = sum(
        (
            (first - left_mean) * (second - right_mean)
            for first, second in zip(left, right, strict=True)
        ),
        Decimal(0),
    ) / count
    left_variance = sum(((value - left_mean) ** 2 for value in left), Decimal(0)) / count
    right_variance = sum(((value - right_mean) ** 2 for value in right), Decimal(0)) / count
    return covariance / (left_variance * right_variance).sqrt()


@given(
    st.integers(min_value=1, max_value=10**9),
    st.integers(min_value=1, max_value=10**9),
)
def test_days_to_exit_is_exact_and_monotone(position_value: int, adv20: int) -> None:
    value = Decimal(position_value)
    adv = Decimal(adv20)
    result = days_to_exit(value, adv)

    assert result == value / (adv * Decimal("0.10"))
    assert days_to_exit(value * 2, adv) >= result
    assert days_to_exit(value, adv * 2) <= result


@given(st.dates(min_value=date(2020, 1, 1), max_value=date(2030, 12, 20)))
def test_warning_persistence_uses_five_published_sessions_not_holiday(start: date) -> None:
    holiday = start + timedelta(days=2)
    sessions = _published_sessions(start, holiday)
    warning = _item(severity="warning")
    history = DiagnosticHistory(
        prior_reports=tuple(_report(warning, trading_day) for trading_day in sessions[:-1]),
        comparable_variants=(_comparison(),),
    )

    assert optimization_need((warning,), history, sessions[-1], sessions) == (
        "OPTIMIZATION_CANDIDATE"
    )
    holiday_history = history.model_copy(
        update={
            "prior_reports": history.prior_reports[:-1]
            + (_report(warning, holiday),)
        }
    )
    assert optimization_need((warning,), holiday_history, sessions[-1], sessions) == "NO_ACTION"


@given(
    st.sampled_from(
        (
            "ADV20_EVIDENCE_MISSING",
            "PRICE_SERIES_EVIDENCE_FUTURE",
            "TRADING_CALENDAR_EVIDENCE_INCONSISTENT",
        )
    )
)
def test_unknown_propagation_cannot_be_no_action(reason: str) -> None:
    unknown = _item(unknown_reason=reason)

    assert optimization_need((unknown,), DiagnosticHistory.empty(), None) == "RESEARCH_REQUIRED"


def _item(*, severity: str = "info", unknown_reason: str | None = None) -> DiagnosticItem:
    return DiagnosticItem(
        diagnostic_code="LIQUIDITY_DAYS_TO_EXIT:fixture",
        metric_value=None if unknown_reason else "3",
        unit="trading_days",
        reference_limit="2",
        policy_scope="currency:USD",
        severity=severity,  # type: ignore[arg-type]
        actionability="research",
        unknown_reason=unknown_reason,
        evidence_refs=(_item_ref(),),
    )


def _report(item: DiagnosticItem, trading_day: date) -> PortfolioDiagnosticReport:
    return PortfolioDiagnosticReport(
        portfolio_snapshot_id=UUID("0198d500-1000-7000-8000-000000000011"),
        trading_day=trading_day,
        policy_version="real_advisory_policy_v1",
        optimization_need="NO_ACTION",
        items=(item,),
    )


def _comparison() -> ComparableVariantEvidence:
    return ComparableVariantEvidence(
        variant="minimum_change",
        proposal_group_id=UUID("0198d500-1000-7000-8000-000000000014"),
        evidence_refs=(
            EvidenceRef(
                source="fixture",
                object_id="comparison",
                available_at=datetime(2026, 8, 23, tzinfo=UTC),
                location="fixture://comparison",
                content_hash="a" * 64,
            ),
        ),
    )


def _item_ref() -> EvidenceRef:
    return EvidenceRef(
        source="fixture",
        object_id="diagnostic-input",
        available_at=datetime(2026, 8, 23, tzinfo=UTC),
        location="fixture://diagnostic-input",
        content_hash="b" * 64,
    )


def _published_sessions(start: date, holiday: date) -> tuple[date, ...]:
    sessions: list[date] = []
    candidate = start
    while len(sessions) < 5:
        if candidate.weekday() < 5 and candidate != holiday:
            sessions.append(candidate)
        candidate += timedelta(days=1)
    return tuple(sessions)
