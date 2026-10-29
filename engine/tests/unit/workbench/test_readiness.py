"""Read-only interpretation never turns absent evidence into execution readiness."""

from datetime import UTC, datetime
from uuid import UUID

import pytest
from nexus.api.schemas.paper import AgentPaperCycleResponse, AgentPaperSessionSummaryResponse
from nexus.workbench.readiness import cycle_blocker, runtime_readiness

NOW = datetime(2026, 9, 17, 12, tzinfo=UTC)


def cycle(status: str = "completed", code: str = "NO_ORDER_REQUIRED") -> AgentPaperCycleResponse:
    return AgentPaperCycleResponse.model_validate(
        {
            "cycle_id": str(UUID(int=1)),
            "status": status,
            "result_code": code,
            "decision_session": "2026-09-17",
            "decision_time": NOW,
            "decision_slot_at": NOW,
            "execution_session": "2026-09-17",
            "execution_locked": True,
            "candidate_count": 0,
            "candidates": [],
            "decision": None,
            "decision_market": None,
            "execution_market": {
                "source": "schwab",
                "status": "pending",
                "window_minutes": 5,
                "feed": "price_history",
                "delivery_mode": "unknown",
            },
            "order_ids": [],
        }
    )


def summary(status: str = "completed") -> AgentPaperSessionSummaryResponse:
    return AgentPaperSessionSummaryResponse.model_validate(
        {
            "decision_session": "2026-09-17",
            "status": status,
            "cycle_count": 1,
            "completed_count": 1,
            "last_completed_decision_at": NOW,
            "next_eligible_at": NOW,
            "next_eligible_reason": "NEXT_SLOT",
            "execution_locked": True,
        }
    )


@pytest.mark.parametrize(
    ("status", "code", "expected"),
    [
        ("completed", "NO_ORDER_REQUIRED", "none"),
        ("unavailable", "AGENT_PAPER_CALENDAR_UNAVAILABLE", "data_missing"),
        ("unavailable", "RESEARCH_OUTPUT_NOT_ACTIONABLE", "research_not_actionable"),
        ("unavailable", "RISK_INPUT_REQUIRED", "unknown"),
        ("unavailable", "PAPER_MARKET_WINDOW_UNAVAILABLE", "unknown"),
        ("unavailable", "HANDLER_AUTHORITY_MISMATCH", "unbound"),
        ("unavailable", "DATA_STALE", "stale"),
        ("failed", "AGENT_PAPER_RESULT_UNKNOWN", "result_unknown"),
        ("unavailable", "NEW_UNRECOGNIZED_REASON", "unknown"),
        ("failed", "AGENT_DECISION_CONFLICT", "unknown"),
    ],
)
def test_only_explicit_codes_explain_the_blocker(status: str, code: str, expected: str) -> None:
    assert cycle_blocker(cycle(status, code)) == expected


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ("prepared", "data"),
        ("researching", "agent"),
        ("decision_recorded", "risk"),
        ("risk_evaluated", "execution"),
        ("awaiting_market", "data"),
        ("partially_filled", "recovery"),
        ("completed", "completed"),
        ("failed", "recovery"),
        ("unavailable", "unknown"),
    ],
)
def test_stage_is_persisted_progress_not_a_claim_of_online_execution(
    status: str, expected: str
) -> None:
    result = runtime_readiness((cycle(status, "RISK_INPUT_REQUIRED"),), summary("in_progress"), ())
    assert result.stage == expected


def test_window_and_calendar_are_not_fabricated_from_missing_cycles() -> None:
    assert runtime_readiness((), summary("closed"), ()).stage == "window"
    result = runtime_readiness((), summary("calendar_unavailable"), ())
    assert result.stage == "data"
    assert result.blocker == "data_missing"
    assert result.previous_cycle_id is None
    assert result.changes == ()


def test_adjacent_cycle_changes_do_not_claim_performance_improvement() -> None:
    previous = cycle("unavailable", "RESEARCH_OUTPUT_NOT_ACTIONABLE").model_copy(
        update={"cycle_id": UUID(int=2)}
    )
    result = runtime_readiness((cycle(), previous), summary(), ())
    assert result.previous_cycle_id == UUID(int=2)
    assert result.changes == ("stage", "blocker", "result")
    assert result.previous_result_code == "RESEARCH_OUTPUT_NOT_ACTIONABLE"
    assert result.blocker == "none"


def test_open_orders_and_aggregate_rejections_do_not_invent_failure_causes() -> None:
    opened = runtime_readiness((cycle("unavailable", "OPEN_ORDER_EXISTS"),), summary(), ())
    assert opened.stage == "execution"
    assert opened.blocker != "result_unknown"
    for code in ("RISK_INPUT_REQUIRED", "NO_ELIGIBLE_CANDIDATE", "RESEARCH_ADMISSION_UNAVAILABLE"):
        assert cycle_blocker(cycle("unavailable", code)) == "unknown"


def test_attention_does_not_label_broad_admission_rejection_as_data_failure() -> None:
    from nexus.api.schemas.workbench import WorkbenchLoopResponse
    from nexus.workbench.read_service import _attention_items

    items = _attention_items(
        latest=cycle("unavailable", "RESEARCH_ADMISSION_UNAVAILABLE"),
        orders=(),
        loop=WorkbenchLoopResponse(
            status="unknown",
            reason_code="HEARTBEAT_MISSING",
            started_at=None,
            last_seen_at=None,
            stale_after_seconds=30,
        ),
        session=summary(),
    )
    assert items[0].kind == "service"
    assert items[0].title_code == "AGENT_PAPER_BLOCKED"


def test_partial_fill_without_linked_orders_requires_original_result_verification() -> None:
    result = runtime_readiness(
        (cycle("partially_filled", "PAPER_RESULT_INCOMPLETE"),), summary(), ()
    )
    assert result.stage == "recovery"
    assert result.blocker == "result_unknown"


def test_an_eligible_empty_slot_waits_for_data_not_for_a_closed_window() -> None:
    assert runtime_readiness((), summary("ready"), ()).stage == "data"
    assert runtime_readiness((), summary("not_started"), ()).stage == "window"


def test_saved_risk_rejection_takes_priority_over_no_order_result() -> None:
    from nexus.api.schemas.paper import AgentPaperDecisionResponse

    rejected = cycle().model_copy(
        update={
            "decision": AgentPaperDecisionResponse(
                decision_id=UUID(int=3),
                sizing_policy_version="agent_paper_sizing_v1",
                target_cash_weight="1",
                risk_outcomes=("rejected",),
            )
        }
    )
    result = runtime_readiness((rejected, cycle()), summary(), ())
    assert result.blocker == "risk_rejected"
    assert result.changes == ("blocker", "risk")


@pytest.mark.parametrize(
    ("code", "stage", "blocker"),
    [
        ("SCHWAB_L1_INCOMPLETE", "data", "data_missing"),
        ("SCHWAB_L1_FRESHNESS_UNPROVEN", "data", "stale"),
        ("AGENT_PAPER_RESULT_UNKNOWN", "recovery", "result_unknown"),
        ("RISK_INPUT_REQUIRED", "unknown", "unknown"),
    ],
)
def test_precycle_failure_is_explained_without_claiming_a_cycle(code, stage, blocker):
    result = runtime_readiness((), summary("stopped"), (), trigger_failure_code=code)
    assert (result.stage, result.blocker, result.reason_code) == (stage, blocker, code)
    assert result.previous_cycle_id is None
    assert result.changes == ()


def test_precycle_failure_does_not_override_cycle_or_calendar():
    assert (
        runtime_readiness(
            (cycle(),), summary(), (), trigger_failure_code="SCHWAB_L1_INCOMPLETE"
        ).reason_code
        == "NO_ORDER_REQUIRED"
    )
    assert (
        runtime_readiness(
            (), summary("calendar_unavailable"), (), trigger_failure_code="SCHWAB_L1_INCOMPLETE"
        ).reason_code
        == "CALENDAR_UNAVAILABLE"
    )
