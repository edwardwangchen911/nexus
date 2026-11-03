from datetime import UTC, datetime, timedelta

from nexus.api.price_check_windows import price_check_windows


def test_completed_windows_distinguish_one_missing_record_from_unknown_and_nonmatches():
    start = datetime(2026, 11, 3, 15, tzinfo=UTC)
    step = timedelta(minutes=5)
    observed = {start, start + 2 * step}
    report = price_check_windows([(start, start + 4 * step)], start + 3 * step, observed, [])
    assert report.expected == 3 and report.recorded == 2
    assert report.missing == (start + step,)
    assert report.uncertain == 0


def test_partial_activation_and_uncertain_coverage_do_not_create_false_missing_checks():
    t = datetime(2026, 11, 3, 15, tzinfo=UTC)
    step = timedelta(minutes=5)
    report = price_check_windows(
        [(t + timedelta(seconds=1), t + 4 * step)], t + 3 * step, set(), [(t + step, t + 2 * step)]
    )
    assert report.missing == (t + 2 * step,)
    assert report.uncertain == 2


def test_expired_and_closed_market_windows_are_excluded_and_unverified_checks_remain_visible():
    start = datetime(2026, 11, 3, 15, tzinfo=UTC)
    step = timedelta(minutes=5)
    result = price_check_windows(
        [(start, start + step)], start + 3 * step, {start, start + step}, [], scope_complete=False
    )
    assert result.expected == 1 and result.recorded == 1 and not result.missing
    assert not result.scope_complete and result.recorded_outside_verified_windows == 1


def test_retained_unknown_receipt_and_late_terminal_do_not_hide_earlier_missing_windows():
    from uuid import uuid4

    from nexus.api.price_check_windows import _coverage_periods
    from nexus.persistence.models.agents import AgentResearchRunRecord
    from nexus.persistence.models.monitoring import MonitorCoverageReceipt

    start = datetime(2026, 11, 3, 15, tzinfo=UTC)
    step = timedelta(minutes=5)
    plan_id = uuid4()
    child = AgentResearchRunRecord(id=uuid4(), decision_time=start, created_at=start)
    receipt = MonitorCoverageReceipt(
        run_id=child.id,
        research_as_of=start,
        active_plan_id=plan_id,
        status="unknown",
        observed_at=start + timedelta(minutes=12),
        created_at=start + timedelta(minutes=12),
    )
    covered, uncertain = _coverage_periods(
        plan_id, start, start + 4 * step, [receipt], [child], {child.id: start + 2 * step}
    )
    report = price_check_windows(covered, start + 4 * step, set(), uncertain)
    assert report.missing == (start, start + step, start + 3 * step)
    assert report.uncertain == 1

    receipt.status = "gap"
    receipt.active_plan_id = None
    covered, uncertain = _coverage_periods(
        plan_id, start, start + 4 * step, [receipt], [child], {child.id: start + 2 * step}
    )
    assert covered == [(start, start + timedelta(minutes=12))]
    assert uncertain == [(start + 2 * step, start + timedelta(minutes=12))]


def test_recorded_unknown_in_an_uncertain_window_does_not_change_the_denominator():
    t = datetime(2026, 11, 3, 15, tzinfo=UTC)
    end = t + timedelta(minutes=5)
    periods = [(t, end)]
    empty = price_check_windows(periods, end, set(), periods)
    observed = price_check_windows(periods, end, {t}, periods)
    assert empty.expected == observed.expected == 0
    assert empty.uncertain == observed.uncertain == 1
    assert observed.recorded == 0 and observed.recorded_outside_verified_windows == 1
