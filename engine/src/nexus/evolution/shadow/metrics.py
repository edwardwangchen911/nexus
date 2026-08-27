"""Exact Decimal Strategy Shadow metrics and deterministic final gates."""

from __future__ import annotations

import hashlib
from datetime import date
from decimal import Decimal, localcontext
from uuid import UUID

from nexus.audit.canonical import canonical_json
from nexus.backtest.canonical import canonical_decimal
from nexus.evolution.shadow.models import (
    EVALUATION_SESSION_COUNT,
    INITIAL_NAV,
    StrategyShadowFinalGateV1,
    StrategyShadowFinalStatus,
    StrategyShadowGateKey,
    StrategyShadowGateResultV1,
    StrategyShadowGateStatus,
    StrategyShadowLegMetricsV1,
    StrategyShadowSessionPointV1,
    StrategyShadowWorstSessionV1,
)


def calculate_strategy_shadow_metrics(
    points: tuple[StrategyShadowSessionPointV1, ...],
    *,
    checkpoint_ordinal: int,
) -> StrategyShadowLegMetricsV1:
    """Calculate one immutable checkpoint from exact ordered session facts."""
    if checkpoint_ordinal not in (5, 21, 63):
        raise ValueError("Strategy Shadow checkpoint must be 5, 21, or 63")
    ordered = tuple(sorted(points, key=lambda item: item.ordinal))
    if len({item.ordinal for item in ordered}) != len(ordered):
        raise ValueError("Strategy Shadow metric ordinals must be unique")
    if ordered and tuple(item.ordinal for item in ordered) != tuple(range(1, len(ordered) + 1)):
        raise ValueError("Strategy Shadow metric ordinals must be contiguous")
    sessions = tuple(item.session for item in ordered)
    if any(left >= right for left, right in zip(sessions, sessions[1:], strict=False)):
        raise ValueError("Strategy Shadow sessions must be strictly increasing")
    complete = len(ordered) == checkpoint_ordinal
    net_return = canonical_decimal(ordered[-1].nav / INITIAL_NAV - Decimal(1)) if complete else None
    returns: list[tuple[date, Decimal]] = []
    peak = INITIAL_NAV
    maximum_drawdown = Decimal(0)
    previous = INITIAL_NAV
    for point in ordered:
        daily_return = point.nav / previous - Decimal(1)
        returns.append((point.session, daily_return))
        previous = point.nav
        peak = max(peak, point.nav)
        maximum_drawdown = min(maximum_drawdown, point.nav / peak - Decimal(1))
    volatility = _annualized_volatility(tuple(item[1] for item in returns)) if complete else None
    worst = tuple(
        StrategyShadowWorstSessionV1(session=session, return_value=value)
        for session, value in sorted(returns, key=lambda item: (item[1], item[0]))[:5]
    )
    payload: dict[str, object] = {
        "schema_version": "strategy_shadow_metrics_v1",
        "checkpoint_ordinal": checkpoint_ordinal,
        "session_count": len(ordered),
        "net_return": net_return,
        "max_drawdown": canonical_decimal(maximum_drawdown) if complete else None,
        "annualized_volatility": (
            canonical_decimal(volatility) if volatility is not None else None
        ),
        "worst_sessions": [item.model_dump(mode="json") for item in worst],
        "cumulative_turnover": canonical_decimal(
            sum((item.turnover for item in ordered), start=Decimal(0))
        ),
        "cumulative_fees": canonical_decimal(
            sum((item.fees for item in ordered), start=Decimal(0))
        ),
        "full_fill_count": sum(item.filled_order_count for item in ordered),
        "hard_risk_violation_count": sum(1 for item in ordered if item.hard_risk_violation),
        "session_authority_hashes": [item.authority_hash for item in ordered],
    }
    provisional = StrategyShadowLegMetricsV1.model_validate({**payload, "metrics_hash": "0" * 64})
    metrics_hash = hashlib.sha256(
        canonical_json(provisional.model_dump(mode="json", exclude={"metrics_hash"})).encode()
    ).hexdigest()
    return provisional.model_copy(update={"metrics_hash": metrics_hash})


def evaluate_strategy_shadow_final_gate(
    *,
    champion: StrategyShadowLegMetricsV1,
    challenger: StrategyShadowLegMetricsV1,
    champion_generation: int,
    active_generation: int,
    evidence_ids: tuple[UUID, ...],
    evidence_hashes: tuple[str, ...],
) -> StrategyShadowFinalGateV1:
    """Evaluate fixed Shadow gates; incomplete evidence is UNKNOWN and fail closed."""
    if len(evidence_ids) != len(set(evidence_ids)):
        raise ValueError("Strategy Shadow final evidence identities must be unique")
    if any(
        len(item) != 64 or any(char not in "0123456789abcdef" for char in item)
        for item in evidence_hashes
    ):
        raise ValueError("Strategy Shadow final evidence hashes must be canonical SHA-256")
    complete = (
        champion.session_count == challenger.session_count == EVALUATION_SESSION_COUNT
        and champion.net_return is not None
        and challenger.net_return is not None
        and champion.max_drawdown is not None
        and challenger.max_drawdown is not None
    )
    gates = (
        _gate(
            "complete_sessions",
            None if not complete else True,
            (
                f"champion={champion.session_count}",
                f"challenger={challenger.session_count}",
            ),
            (f"required={EVALUATION_SESSION_COUNT}",),
            evidence_ids,
            "SHADOW_SESSION_EVIDENCE_INCOMPLETE",
        ),
        _gate(
            "minimum_full_fills",
            None if not complete else challenger.full_fill_count >= 20,
            (f"challenger={challenger.full_fill_count}",),
            ("minimum=20",),
            evidence_ids,
            "SHADOW_FULL_FILL_COUNT_INSUFFICIENT",
        ),
        _gate(
            "hard_risk_clear",
            None if not complete else challenger.hard_risk_violation_count == 0,
            (f"challenger={challenger.hard_risk_violation_count}",),
            ("maximum=0",),
            evidence_ids,
            "SHADOW_HARD_RISK_VIOLATION",
        ),
        _gate(
            "nonnegative_excess_return",
            (
                None if not complete else challenger.net_return >= champion.net_return  # type: ignore[operator]
            ),
            (
                f"champion={champion.net_return}",
                f"challenger={challenger.net_return}",
            ),
            ("minimum_delta=0",),
            evidence_ids,
            "SHADOW_EXCESS_RETURN_NEGATIVE",
        ),
        _gate(
            "maximum_drawdown_degradation",
            (
                None
                if not complete
                else challenger.max_drawdown - champion.max_drawdown  # type: ignore[operator]
                >= Decimal("-0.02")
            ),
            (
                f"champion={champion.max_drawdown}",
                f"challenger={challenger.max_drawdown}",
            ),
            ("minimum_delta=-0.02",),
            evidence_ids,
            "SHADOW_DRAWDOWN_DEGRADATION_EXCEEDED",
        ),
    )
    if champion_generation != active_generation:
        status: StrategyShadowFinalStatus = "STALE"
        reason_code = "STALE_CHAMPION_BASELINE"
    else:
        statuses = {item.status for item in gates}
        status = "UNKNOWN" if "UNKNOWN" in statuses else "FAIL" if "FAIL" in statuses else "PASS"
        reason_code = {
            "PASS": "STRATEGY_SHADOW_GATES_PASSED",
            "FAIL": "STRATEGY_SHADOW_GATES_FAILED",
            "UNKNOWN": "STRATEGY_SHADOW_EVIDENCE_INCOMPLETE",
        }[status]
    authority_identity = {
        "schema_version": "strategy_shadow_final_authority_v1",
        "champion_metrics_hash": champion.metrics_hash,
        "challenger_metrics_hash": challenger.metrics_hash,
        "champion_generation": champion_generation,
        "active_generation": active_generation,
        "evidence_ids": [str(item) for item in evidence_ids],
        "evidence_hashes": list(evidence_hashes),
    }
    final_authority_hash = hashlib.sha256(canonical_json(authority_identity).encode()).hexdigest()
    provisional = StrategyShadowFinalGateV1(
        status=status,
        champion_generation=champion_generation,
        active_generation=active_generation,
        gates=gates,
        reason_code=reason_code,
        evidence_ids=evidence_ids,
        final_authority_hash=final_authority_hash,
        finalization_hash="0" * 64,
    )
    finalization_hash = hashlib.sha256(
        canonical_json(provisional.model_dump(mode="json", exclude={"finalization_hash"})).encode()
    ).hexdigest()
    return provisional.model_copy(update={"finalization_hash": finalization_hash})


def _annualized_volatility(returns: tuple[Decimal, ...]) -> Decimal:
    if len(returns) < 2:
        return Decimal(0)
    with localcontext() as context:
        context.prec = 50
        mean = sum(returns, start=Decimal(0)) / Decimal(len(returns))
        variance = sum((item - mean) ** 2 for item in returns) / Decimal(len(returns) - 1)
        return variance.sqrt() * Decimal(252).sqrt()


def _gate(
    gate: StrategyShadowGateKey,
    passed: bool | None,
    observed: tuple[str, ...],
    thresholds: tuple[str, ...],
    evidence_ids: tuple[UUID, ...],
    failure_reason: str,
) -> StrategyShadowGateResultV1:
    status: StrategyShadowGateStatus = "UNKNOWN" if passed is None else "PASS" if passed else "FAIL"
    reason = (
        f"{gate.upper()}_PASSED"
        if status == "PASS"
        else f"{gate.upper()}_UNKNOWN"
        if status == "UNKNOWN"
        else failure_reason
    )
    return StrategyShadowGateResultV1(
        gate=gate,
        status=status,
        observed=observed,
        thresholds=thresholds,
        evidence_ids=evidence_ids,
        reason_code=reason,
    )
