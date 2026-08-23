"""Deterministic report-level decision precedence and trading-day persistence."""

from collections.abc import Iterable
from datetime import date

from nexus.diagnostics.models import (
    DiagnosticHistory,
    DiagnosticItem,
    OptimizationNeed,
)


def optimization_need(
    items: Iterable[DiagnosticItem],
    history: DiagnosticHistory,
    current_trading_day: date | None,
    trading_sessions: tuple[date, ...] = (),
) -> OptimizationNeed:
    """Apply the binding precedence after deterministic metric evaluation."""
    item_tuple = tuple(items)
    if any(item.unknown_reason is not None for item in item_tuple):
        return "RESEARCH_REQUIRED"
    warnings = tuple(item for item in item_tuple if item.severity == "warning")
    triggered = any(item.severity == "critical" for item in item_tuple)
    triggered = triggered or len({_risk_domain(item.diagnostic_code) for item in warnings}) >= 2
    triggered = triggered or any(
        _warning_persists(item, history, current_trading_day, trading_sessions) for item in warnings
    )
    if not triggered:
        return "NO_ACTION"
    return "OPTIMIZATION_CANDIDATE" if history.comparable_variants else "RESEARCH_REQUIRED"


def _warning_persists(
    item: DiagnosticItem,
    history: DiagnosticHistory,
    current_trading_day: date | None,
    trading_sessions: tuple[date, ...],
) -> bool:
    if (
        current_trading_day is None
        or len(history.prior_reports) < 4
        or current_trading_day not in trading_sessions
    ):
        return False
    reports = history.prior_reports[-4:]
    if any(report.trading_day is None for report in reports):
        return False
    current_index = trading_sessions.index(current_trading_day)
    if current_index < 4:
        return False
    expected_sessions = tuple(reversed(trading_sessions[current_index - 4 : current_index]))
    for report, expected in zip(reversed(reports), expected_sessions, strict=True):
        if report.trading_day != expected or not any(
            prior_item.diagnostic_code == item.diagnostic_code
            and prior_item.policy_scope == item.policy_scope
            and prior_item.severity == "warning"
            for prior_item in report.items
        ):
            return False
    return True


def _risk_domain(code: str) -> str:
    if code.startswith("CONCENTRATION"):
        return "concentration"
    if code.startswith("RISK_BUCKET"):
        return "exposure"
    if code.startswith(("VOLATILITY", "DRAWDOWN", "BETA", "CORRELATION")):
        return "market"
    if code.startswith("LIQUIDITY"):
        return "liquidity"
    return code.split(":", 1)[0]
