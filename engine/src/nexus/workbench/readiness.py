"""Small deterministic explanation of persisted facts, never an execution gate."""

from nexus.api.schemas.paper import (
    AgentPaperCycleResponse,
    AgentPaperSessionSummaryResponse,
    PaperOrderResponse,
)
from nexus.api.schemas.workbench import (
    WorkbenchBlocker,
    WorkbenchReadinessResponse,
    WorkbenchStage,
)

# Exact codes only. Broad admission failures deliberately remain unknown.
_BLOCKERS: dict[str, WorkbenchBlocker] = {
    "AGENT_PAPER_CALENDAR_UNAVAILABLE": "data_missing",
    "DATA_STALE": "stale",
    "HANDLER_AUTHORITY_MISMATCH": "unbound",
    "RISK_AUTHORITY_INCOMPLETE": "unbound",
    "PAPER_EXECUTION_AUTHORITY_INCOMPLETE": "unbound",
    "RESEARCH_OUTPUT_NOT_ACTIONABLE": "research_not_actionable",
    "AGENT_PAPER_RESULT_UNKNOWN": "result_unknown",
    "OPEN_ORDER_EXISTS": "none",
}
_STAGES: dict[str, WorkbenchStage] = {
    "prepared": "data",
    "researching": "agent",
    "decision_recorded": "risk",
    "risk_evaluated": "execution",
    "awaiting_market": "data",
    "partially_filled": "execution",
    "completed": "completed",
    "failed": "recovery",
}


def cycle_blocker(cycle: AgentPaperCycleResponse) -> WorkbenchBlocker:
    if cycle.status == "partially_filled" and not cycle.order_ids:
        return "result_unknown"
    if cycle.decision and "rejected" in cycle.decision.risk_outcomes:
        return "risk_rejected"
    if cycle.result_code in _BLOCKERS:
        return _BLOCKERS[cycle.result_code]
    if cycle.status in {"unavailable", "failed"}:
        return "unknown"
    return "none"


def _stage(cycle: AgentPaperCycleResponse) -> WorkbenchStage:
    if cycle.result_code == "OPEN_ORDER_EXISTS":
        return "execution"
    if cycle.result_code == "PAPER_MARKET_WINDOW_UNAVAILABLE":
        return "data"
    blocker = cycle_blocker(cycle)
    if blocker == "result_unknown":
        return "recovery"
    if cycle.status == "unavailable":
        blocked_stages: dict[WorkbenchBlocker, WorkbenchStage] = {
            "data_missing": "data",
            "stale": "data",
            "unbound": "recovery",
            "research_not_actionable": "agent",
        }
        return blocked_stages.get(blocker, "unknown")
    return _STAGES.get(cycle.status, "unknown")


def runtime_readiness(
    cycles: tuple[AgentPaperCycleResponse, ...],
    summary: AgentPaperSessionSummaryResponse,
    orders: tuple[PaperOrderResponse, ...],
    *,
    trigger_failure_code: str | None = None,
) -> WorkbenchReadinessResponse:
    latest = cycles[0] if cycles else None
    previous = cycles[1] if len(cycles) > 1 else None
    stage: WorkbenchStage = _stage(latest) if latest else "unknown"
    blocker: WorkbenchBlocker = cycle_blocker(latest) if latest else "unknown"
    reason = latest.result_code if latest else "CYCLE_MISSING"
    if summary.status == "calendar_unavailable":
        stage, blocker, reason = "data", "data_missing", "CALENDAR_UNAVAILABLE"
    elif summary.status == "closed" and (latest is None or latest.status == "completed"):
        stage = "window"
    elif latest is None and summary.status == "not_started":
        stage = "window"
    elif latest is None and summary.status == "ready":
        stage = "data"
    if latest is None and summary.status != "calendar_unavailable" and trigger_failure_code:
        reason = trigger_failure_code
        trigger_stages: dict[str, tuple[WorkbenchStage, WorkbenchBlocker]] = {
            "SCHWAB_L1_INCOMPLETE": ("data", "data_missing"),
            "SCHWAB_L1_FRESHNESS_UNPROVEN": ("data", "stale"),
            "AGENT_PAPER_RESULT_UNKNOWN": ("recovery", "result_unknown"),
        }
        stage, blocker = trigger_stages.get(trigger_failure_code, ("unknown", "unknown"))
    if any(order.status == "reconciling" for order in orders):
        stage, blocker, reason = "recovery", "result_unknown", "ORDER_RESULT_UNKNOWN"
    changes = []
    if latest and previous:
        if _stage(latest) != _stage(previous):
            changes.append("stage")
        if cycle_blocker(latest) != cycle_blocker(previous):
            changes.append("blocker")
        if latest.result_code != previous.result_code:
            changes.append("result")
        if (latest.decision.risk_outcomes if latest.decision else None) != (
            previous.decision.risk_outcomes if previous.decision else None
        ):
            changes.append("risk")
        if len(latest.order_ids) != len(previous.order_ids):
            changes.append("orders")
    return WorkbenchReadinessResponse.model_validate(
        {
            "stage": stage,
            "blocker": blocker,
            "reason_code": reason,
            "previous_cycle_id": previous.cycle_id if previous else None,
            "previous_result_code": previous.result_code if previous else None,
            "changes": changes,
        }
    )
