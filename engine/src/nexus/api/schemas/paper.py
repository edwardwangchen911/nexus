"""Strict redacted contracts for the internal-paper console."""

from __future__ import annotations

from datetime import date
from typing import Annotated, Literal
from uuid import UUID

from nexus.api.schemas.integrations import SuccessMetaResponse
from nexus.domain.decimal import DecimalString
from nexus.paper.models import InternalOrderStatus
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator


class _StrictSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PaperPerformanceResponse(_StrictSchema):
    as_of: AwareDatetime
    cash: DecimalString
    available_cash: DecimalString
    reserved_cash: DecimalString
    market_value: DecimalString
    nav: DecimalString
    peak_nav: DecimalString
    realized_pnl: DecimalString
    unrealized_pnl: DecimalString
    fees: DecimalString
    drawdown_ratio: DecimalString


class PaperPositionResponse(_StrictSchema):
    instrument_id: UUID
    symbol: str | None = Field(min_length=1, max_length=32)
    quantity: DecimalString
    reserved_quantity: DecimalString
    available_quantity: DecimalString
    cost_basis: DecimalString
    average_cost: DecimalString
    reference_close: DecimalString
    market_value: DecimalString
    unrealized_pnl: DecimalString
    price_source: str = Field(min_length=1, max_length=100)
    price_feed: str = Field(min_length=1, max_length=100)
    delivery_mode: Literal["real_time", "delayed", "fixture", "unknown"]
    price_as_of: AwareDatetime


class PaperAttributionResponse(_StrictSchema):
    dimension: Literal["cash", "instrument", "sector"]
    dimension_key: str = Field(min_length=1, max_length=100)
    market_value: DecimalString
    realized_pnl: DecimalString
    unrealized_pnl: DecimalString
    fees: DecimalString
    slippage: DecimalString
    completeness: Literal["FULL", "PARTIAL"]


class PaperTargetAllocationResponse(_StrictSchema):
    instrument_id: UUID
    target_weight: DecimalString
    exit: bool
    role: Literal["core", "tactical", "none"] | None
    reason_code: str = Field(min_length=1, max_length=100)


class PaperTargetSummaryResponse(_StrictSchema):
    target_version_id: UUID
    version_seq: int = Field(ge=1)
    strategy_decision_id: UUID
    previous_target_version_id: UUID | None
    target_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    activation_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    cash_weight: DecimalString
    targets: tuple[PaperTargetAllocationResponse, ...] = Field(max_length=50)
    as_of: AwareDatetime


class PaperPlanSummaryResponse(_StrictSchema):
    batch_id: UUID
    target_version_id: UUID | None
    strategy_decision_id: UUID
    decision_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    plan_created_at: AwareDatetime
    reconciliation_as_of: AwareDatetime
    occupation_state: Literal["occupied", "released", "unknown"]


class AgentPaperCandidateBatchResponse(_StrictSchema):
    candidate_batch_id: UUID
    universe_snapshot_id: UUID
    candidate_instrument_ids: tuple[UUID, ...] = Field(min_length=1, max_length=5)
    source: Literal["explicit_research_scope"]
    batch_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    as_of: AwareDatetime


class AgentPaperSecurityResearchResponse(_StrictSchema):
    instrument_id: UUID
    research_run_id: UUID
    research_output_id: UUID
    status: Literal["completed"]
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    intent_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    stance: Literal["buy_candidate", "reduce_candidate", "hold", "watch"]
    quality_status: Literal["complete", "partial"]
    as_of: AwareDatetime


class AgentPaperSynthesisChoiceResponse(_StrictSchema):
    instrument_id: UUID
    action: Literal["include", "increase", "reduce", "exit", "maintain", "defer"]
    role: Literal["core", "tactical", "none"]
    rank: int | None = Field(default=None, ge=1, le=5)
    replaces_instrument_id: UUID | None
    depends_on_instrument_ids: tuple[UUID, ...] = Field(max_length=4)
    thesis: str = Field(min_length=1, max_length=2000)
    counter_thesis: str = Field(min_length=1, max_length=2000)
    invalidation: str = Field(min_length=1, max_length=2000)


class AgentPaperSynthesisResponse(_StrictSchema):
    synthesis_run_id: UUID
    synthesis_output_id: UUID
    candidate_batch_id: UUID
    input_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    binding_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    status: Literal["completed", "abstained", "failed"]
    choices: tuple[AgentPaperSynthesisChoiceResponse, ...] = Field(max_length=5)
    cash_rationale: str | None = Field(default=None, min_length=1, max_length=2000)
    as_of: AwareDatetime


class AgentPaperProposalTargetResponse(_StrictSchema):
    instrument_id: UUID
    target_weight: DecimalString
    exit: bool
    role: Literal["core", "tactical", "none"]
    reason_code: str = Field(min_length=1, max_length=100)
    rank: int | None = Field(default=None, ge=1, le=5)
    replaces_instrument_id: UUID | None
    depends_on_instrument_ids: tuple[UUID, ...] = Field(max_length=4)


class AgentPaperPortfolioProposalResponse(_StrictSchema):
    strategy_decision_id: UUID
    synthesis_output_id: UUID
    decision_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    policy_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    baseline_target_version_id: UUID | None
    initial_approval_id: UUID | None
    cash_weight: DecimalString
    targets: tuple[AgentPaperProposalTargetResponse, ...] = Field(max_length=50)
    expires_at: AwareDatetime
    as_of: AwareDatetime


class AgentPaperActivationResultResponse(_StrictSchema):
    result_code: Literal["TARGET_ACTIVATION_BLOCKED", "PLAN_CREATED", "NO_ORDER_REQUIRED"]
    strategy_decision_id: UUID | None
    target_version_id: UUID | None
    batch_id: UUID | None
    as_of: AwareDatetime


class AgentPaperCycleBlockerResponse(_StrictSchema):
    code: Literal[
        "AGENT_PAPER_PROVIDER_CAPACITY_EXCEEDED",
        "POLICY_UNCONFIRMED",
        "PORTFOLIO_PROPOSAL_UNAVAILABLE",
        "TARGET_ACTIVATION_BLOCKED",
    ]
    source_stage: Literal["provider", "policy", "proposal", "activation"]
    category: Literal[
        "provider_capacity",
        "policy_unconfirmed",
        "proposal_rejected",
        "resource_occupied",
    ]
    proposal_rejection_code: (
        Literal[
            "EXIT_TURNOVER_CONFLICT",
            "REDUCTION_TURNOVER_CONFLICT",
            "PORTFOLIO_DEPENDENCY_CONFLICT",
        ]
        | None
    )
    as_of: AwareDatetime

    @model_validator(mode="after")
    def _code_matches_stage_and_category(self) -> AgentPaperCycleBlockerResponse:
        expected = {
            "AGENT_PAPER_PROVIDER_CAPACITY_EXCEEDED": (
                "provider",
                "provider_capacity",
                False,
            ),
            "POLICY_UNCONFIRMED": ("policy", "policy_unconfirmed", False),
            "PORTFOLIO_PROPOSAL_UNAVAILABLE": ("proposal", "proposal_rejected", True),
            "TARGET_ACTIVATION_BLOCKED": ("activation", "resource_occupied", False),
        }[self.code]
        if (self.source_stage, self.category) != expected[:2]:
            raise ValueError("blocker code does not match its source stage and category")
        if (self.proposal_rejection_code is not None) != expected[2]:
            raise ValueError("proposal rejection code does not match blocker category")
        return self


class PaperManagementSummaryResponse(_StrictSchema):
    current_target: PaperTargetSummaryResponse | None
    occupying_plan: PaperPlanSummaryResponse | None
    latest_proposal: AgentPaperPortfolioProposalResponse | None
    read_as_of: AwareDatetime


class PaperComparisonRowResponse(_StrictSchema):
    key: Literal["agent", "hold_current", "baseline_v1", "market_benchmark"]
    data_state: Literal["READY", "PARTIAL", "STALE", "UNAVAILABLE"]
    nav: DecimalString | None
    total_return_ratio: DecimalString | None
    max_drawdown_ratio: DecimalString | None
    turnover_ratio: DecimalString | None
    total_cost: DecimalString | None
    reason_code: Literal[
        "PERSISTED_AGENT_PERFORMANCE",
        "PERSISTED_MARKET_BENCHMARK",
        "CASH_HOLD_CURRENT",
        "SAME_WINDOW_FORWARD_FACTS_UNAVAILABLE",
        "MIXED_STRATEGY_FACTS_UNAVAILABLE",
    ]

    @model_validator(mode="after")
    def _metrics_match_availability(self) -> PaperComparisonRowResponse:
        metrics = (
            self.nav,
            self.total_return_ratio,
            self.max_drawdown_ratio,
            self.turnover_ratio,
            self.total_cost,
        )
        unavailable = self.data_state == "UNAVAILABLE"
        if (unavailable and not all(value is None for value in metrics)) or (
            not unavailable and any(value is None for value in metrics)
        ):
            raise ValueError("comparison metrics must match availability")
        return self


class PaperComparisonResponse(_StrictSchema):
    window_start: AwareDatetime
    as_of: AwareDatetime
    rows: tuple[PaperComparisonRowResponse, ...] = Field(min_length=4, max_length=4)

    @model_validator(mode="after")
    def _rows_are_canonical(self) -> PaperComparisonResponse:
        if self.window_start > self.as_of:
            raise ValueError("comparison window must be ordered")
        if tuple(row.key for row in self.rows) != (
            "agent",
            "hold_current",
            "baseline_v1",
            "market_benchmark",
        ):
            raise ValueError("comparison rows must use canonical order")
        return self


class PaperPortfolioResponse(_StrictSchema):
    portfolio_id: UUID
    name: str = Field(min_length=1, max_length=100)
    mode: Literal["internal_paper"]
    currency: Literal["USD"]
    initial_capital: DecimalString
    data_state: Literal["EMPTY", "READY", "PARTIAL", "STALE"]
    performance: PaperPerformanceResponse | None
    comparison: PaperComparisonResponse | None
    positions: tuple[PaperPositionResponse, ...] = Field(max_length=500)
    attributions: tuple[PaperAttributionResponse, ...] = Field(max_length=500)


class PaperPortfolioEnvelope(_StrictSchema):
    data: PaperPortfolioResponse
    meta: SuccessMetaResponse


class PaperOrderEventResponse(_StrictSchema):
    event_seq: int = Field(ge=1)
    from_status: InternalOrderStatus | None
    to_status: InternalOrderStatus
    actor_kind: Literal["task18", "task19", "user", "reconciliation"]
    reason_code: str = Field(min_length=1, max_length=100)
    occurred_at: AwareDatetime


class PaperFillResponse(_StrictSchema):
    fill_id: UUID
    fill_seq: int = Field(ge=1)
    quantity: DecimalString
    price: DecimalString
    notional: DecimalString
    fee: DecimalString
    occurred_at: AwareDatetime
    source: str = Field(min_length=1, max_length=100)
    feed: str = Field(min_length=1, max_length=100)
    delivery_mode: Literal["real_time", "delayed", "fixture", "unknown"]
    source_as_of: AwareDatetime
    execution_model_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    slippage_bps: Literal[10, 25]
    slippage: DecimalString


class PaperOrderResponse(_StrictSchema):
    order_id: UUID
    instrument_id: UUID
    symbol: str | None = Field(min_length=1, max_length=32)
    side: Literal["buy", "sell"]
    quantity: DecimalString
    filled_quantity: DecimalString
    remaining_quantity: DecimalString
    worst_price: DecimalString
    status: InternalOrderStatus
    cancellable: bool
    current_event_seq: int = Field(ge=1)
    submitted_at: AwareDatetime
    strategy_version: str = Field(min_length=1, max_length=50)
    risk_policy_version: str = Field(min_length=1, max_length=50)
    events: tuple[PaperOrderEventResponse, ...] = Field(max_length=128)
    fills: tuple[PaperFillResponse, ...] = Field(max_length=128)


class PaperOrderPageResponse(_StrictSchema):
    items: tuple[PaperOrderResponse, ...] = Field(max_length=50)
    next_cursor: str | None


class PaperOrderPageEnvelope(_StrictSchema):
    data: PaperOrderPageResponse
    meta: SuccessMetaResponse


class AgentPaperCandidateResponse(_StrictSchema):
    instrument_id: UUID
    symbol: str = Field(min_length=1, max_length=32)
    selection_reasons: tuple[str, ...] = Field(min_length=1, max_length=4)
    research_status: Literal["pending", "completed", "unavailable"]
    research_job_status: (
        Literal["queued", "running", "succeeded", "failed", "skipped", "dead_letter"] | None
    ) = None
    stance: Literal["buy_candidate", "reduce_candidate", "hold", "watch"] | None
    sizing_action: Literal["increase", "reduce", "maintain", "none"] | None
    intent_hash: str | None = Field(pattern=r"^[0-9a-f]{64}$")
    output_id: UUID | None
    rejection_code: str | None = Field(default=None, min_length=1, max_length=100)


class AgentPaperDecisionResponse(_StrictSchema):
    decision_id: UUID
    sizing_policy_version: Literal[
        "agent_paper_sizing_v1",
        "portfolio_allocation_v1",
    ]
    target_cash_weight: DecimalString
    risk_outcomes: tuple[Literal["approved", "reduced", "rejected", "no_change"], ...] = Field(
        max_length=50
    )


class AgentPaperDecisionMarketResponse(_StrictSchema):
    source: Literal["schwab"]
    feed: Literal["l1"]
    delivery_mode: Literal["real_time"]
    delayed: Literal[False]
    quote_at: AwareDatetime


class AgentPaperAlpacaExecutionMarketResponse(_StrictSchema):
    source: Literal["alpaca"]
    feed: Literal["iex"]
    delivery_mode: Literal["real_time"]
    window_minutes: Literal[5]
    status: Literal["pending", "qualified", "probe", "unavailable"]


class AgentPaperSchwabExecutionMarketResponse(_StrictSchema):
    source: Literal["schwab"]
    feed: Literal["price_history"]
    delivery_mode: Literal["unknown"]
    window_minutes: Literal[5]
    status: Literal["pending", "qualified", "probe", "unavailable"]


AgentPaperExecutionMarketResponse = Annotated[
    AgentPaperAlpacaExecutionMarketResponse | AgentPaperSchwabExecutionMarketResponse,
    Field(discriminator="source"),
]


class AgentPaperCycleResponse(_StrictSchema):
    cycle_id: UUID
    status: Literal[
        "prepared",
        "researching",
        "decision_recorded",
        "risk_evaluated",
        "awaiting_market",
        "partially_filled",
        "completed",
        "unavailable",
        "failed",
    ]
    result_code: str = Field(min_length=1, max_length=100)
    decision_session: date
    decision_time: AwareDatetime
    decision_slot_at: AwareDatetime
    execution_session: date
    execution_locked: Literal[True]
    candidate_count: int = Field(ge=0, le=5)
    candidates: tuple[AgentPaperCandidateResponse, ...] = Field(max_length=5)
    decision: AgentPaperDecisionResponse | None
    decision_market: AgentPaperDecisionMarketResponse | None
    execution_market: AgentPaperExecutionMarketResponse
    order_ids: tuple[UUID, ...] = Field(max_length=50)


class AgentPaperCycleEnvelope(_StrictSchema):
    data: AgentPaperCycleResponse
    meta: SuccessMetaResponse


class AgentPaperCycleDetailResponse(_StrictSchema):
    cycle: AgentPaperCycleResponse
    orders: tuple[PaperOrderResponse, ...] = Field(max_length=50)


class AgentPaperManagementCycleResponse(_StrictSchema):
    cycle: AgentPaperCycleResponse
    orders: tuple[PaperOrderResponse, ...] = Field(max_length=50)
    candidate_batch: AgentPaperCandidateBatchResponse | None
    security_research: tuple[AgentPaperSecurityResearchResponse, ...] = Field(max_length=5)
    synthesis: AgentPaperSynthesisResponse | None
    proposal: AgentPaperPortfolioProposalResponse | None
    active_target: PaperTargetSummaryResponse | None
    activation_result: AgentPaperActivationResultResponse | None
    blocker: AgentPaperCycleBlockerResponse | None
    blocking_plan: PaperPlanSummaryResponse | None
    plan_summary: PaperPlanSummaryResponse | None


class AgentPaperCycleDetailEnvelope(_StrictSchema):
    data: AgentPaperCycleDetailResponse
    meta: SuccessMetaResponse


class AgentPaperManagementCycleEnvelope(_StrictSchema):
    data: AgentPaperManagementCycleResponse
    meta: SuccessMetaResponse


class PaperManagementSummaryEnvelope(_StrictSchema):
    data: PaperManagementSummaryResponse
    meta: SuccessMetaResponse


class AgentPaperSessionSummaryResponse(_StrictSchema):
    decision_session: date | None
    status: Literal[
        "calendar_unavailable",
        "not_started",
        "ready",
        "in_progress",
        "completed",
        "stopped",
        "closed",
        "historical",
    ]
    cycle_count: int = Field(ge=0, le=108)
    completed_count: int = Field(ge=0, le=108)
    latest_result_code: str | None = Field(default=None, min_length=1, max_length=100)
    last_completed_decision_at: AwareDatetime | None
    next_eligible_at: AwareDatetime | None
    next_eligible_reason: (
        Literal[
            "CURRENT_SLOT",
            "NEXT_SLOT",
            "SESSION_OPEN",
            "NEXT_SESSION",
            "CURRENT_CYCLE_IN_PROGRESS",
            "CALENDAR_UNAVAILABLE",
        ]
        | None
    )
    execution_locked: Literal[True]


class AgentPaperVersionObservationResponse(_StrictSchema):
    prompt_version: str = Field(min_length=1, max_length=100)
    prompt_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    model_id: str = Field(min_length=1, max_length=100)
    model_version: str = Field(min_length=1, max_length=100)
    model_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    first_decision_at: AwareDatetime
    last_decision_at: AwareDatetime
    observed_sessions: int = Field(ge=1, le=100_000)
    cycle_count: int = Field(ge=1, le=100_000)
    completed_count: int = Field(ge=0, le=100_000)
    unavailable_count: int = Field(ge=0, le=100_000)
    order_count: int = Field(ge=0, le=100_000)
    fill_count: int = Field(ge=0, le=100_000)
    challenger_available: bool
    lifecycle_state: Literal["incumbent_frozen", "historical_frozen"]
    verdict: Literal["continue_observing", "retained_as_frozen_history"]
    reason_codes: tuple[
        Literal[
            "VERSION_FROZEN_AUDIT_HISTORY",
            "VERSION_PERFORMANCE_ATTRIBUTION_UNAVAILABLE",
            "VERSION_ORDER_PATH_UNOBSERVED",
            "VERSION_CHALLENGER_UNAVAILABLE",
            "VERSION_SEQUENTIAL_HISTORY_NOT_COMPARABLE",
        ],
        ...,
    ] = Field(min_length=1, max_length=4)

    @model_validator(mode="after")
    def _facts_are_consistent(self) -> AgentPaperVersionObservationResponse:
        if self.first_decision_at > self.last_decision_at:
            raise ValueError("version observation window must be ordered")
        if self.observed_sessions > self.cycle_count:
            raise ValueError("version observation sessions exceed cycles")
        if self.completed_count + self.unavailable_count > self.cycle_count:
            raise ValueError("version observation terminal counts exceed cycles")
        if self.lifecycle_state == "incumbent_frozen":
            if self.verdict != "continue_observing":
                raise ValueError("incumbent version must continue observing")
            expected_reasons = ["VERSION_PERFORMANCE_ATTRIBUTION_UNAVAILABLE"]
        else:
            if self.verdict != "retained_as_frozen_history":
                raise ValueError("historical version must remain frozen history")
            expected_reasons = [
                "VERSION_FROZEN_AUDIT_HISTORY",
                "VERSION_PERFORMANCE_ATTRIBUTION_UNAVAILABLE",
            ]
        if self.order_count == 0 or self.fill_count == 0:
            expected_reasons.append("VERSION_ORDER_PATH_UNOBSERVED")
        if self.lifecycle_state == "incumbent_frozen":
            if not self.challenger_available:
                expected_reasons.append("VERSION_CHALLENGER_UNAVAILABLE")
        else:
            expected_reasons.append("VERSION_SEQUENTIAL_HISTORY_NOT_COMPARABLE")
        if self.reason_codes != tuple(expected_reasons):
            raise ValueError("version observation reasons do not match facts")
        return self


class AgentVersionLaneMetricsResponse(_StrictSchema):
    nav: DecimalString
    total_return_ratio: DecimalString
    max_drawdown_ratio: DecimalString
    turnover_ratio: DecimalString
    total_cost: DecimalString
    order_count: int = Field(ge=0, le=100_000)
    fill_count: int = Field(ge=0, le=100_000)
    hard_risk_count: int = Field(ge=0, le=100_000)


class AgentVersionComparisonResponse(_StrictSchema):
    key: Literal["hold_current", "baseline_v1", "market_benchmark"]
    data_state: Literal["READY", "UNKNOWN"]
    reason_code: str = Field(min_length=1, max_length=100)


class AgentVersionGateResponse(_StrictSchema):
    key: Literal[
        "authority",
        "oos_pairs",
        "cost_authority",
        "comparison_authority",
        "hard_risk",
        "ledger_conservation",
        "duplicate_execution",
        "action_divergence",
        "challenger_fills",
        "excess_return",
        "positive_excess_sessions",
        "drawdown_degradation",
    ]
    status: Literal["PASS", "FAIL", "UNKNOWN"]
    observed: str | None = Field(default=None, max_length=100)
    threshold: str | None = Field(default=None, max_length=100)
    reason_code: str = Field(min_length=1, max_length=100)


class AgentVersionSlotLaneResponse(_StrictSchema):
    status: Literal[
        "pending",
        "prepared",
        "researching",
        "decision_recorded",
        "risk_evaluated",
        "awaiting_market",
        "partially_filled",
        "completed",
        "unavailable",
        "failed",
    ]
    result_code: str | None = Field(default=None, min_length=1, max_length=100)

    @model_validator(mode="after")
    def _result_matches_status(self) -> AgentVersionSlotLaneResponse:
        if (self.status == "pending") != (self.result_code is None):
            raise ValueError("pending Agent version lane cannot expose a result")
        return self


class AgentVersionSlotResponse(_StrictSchema):
    phase: Literal["train", "oos"]
    decision_slot_at: AwareDatetime
    status: Literal["scheduled", "due", "running", "paired", "attention"]
    incumbent: AgentVersionSlotLaneResponse
    challenger: AgentVersionSlotLaneResponse
    reason_code: str = Field(min_length=1, max_length=100)


class AgentVersionEvaluationResponse(_StrictSchema):
    evaluation_id: UUID
    status: Literal["observing", "PASS", "FAIL", "UNKNOWN"]
    incumbent_prompt_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    incumbent_model_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    challenger_prompt_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    challenger_model_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    train_completed: int = Field(ge=0, le=4)
    oos_completed: int = Field(ge=0, le=20)
    oos_missing: int = Field(ge=0, le=20)
    oos_incomparable: int = Field(ge=0, le=20)
    slots: tuple[AgentVersionSlotResponse, ...] = Field(min_length=24, max_length=24)
    incumbent_metrics: AgentVersionLaneMetricsResponse | None
    challenger_metrics: AgentVersionLaneMetricsResponse | None
    comparisons: tuple[AgentVersionComparisonResponse, ...] = Field(min_length=3, max_length=3)
    gates: tuple[AgentVersionGateResponse, ...] = Field(max_length=12)
    reason_code: str = Field(min_length=1, max_length=100)
    execution_locked: Literal[True]
    broker_write: Literal[False]
    live_write: Literal[False]

    @model_validator(mode="after")
    def _facts_are_consistent(self) -> AgentVersionEvaluationResponse:
        if self.oos_completed + self.oos_missing + self.oos_incomparable != 20:
            raise ValueError("Agent version OOS lifecycle counts must total 20")
        if tuple(row.key for row in self.comparisons) != (
            "hold_current",
            "baseline_v1",
            "market_benchmark",
        ):
            raise ValueError("Agent version comparisons must use the fixed order")
        if tuple(slot.phase for slot in self.slots) != ("train",) * 4 + ("oos",) * 20:
            raise ValueError("Agent version slots must use the fixed phase order")
        if any(
            left.decision_slot_at >= right.decision_slot_at
            for left, right in zip(self.slots, self.slots[1:], strict=False)
        ):
            raise ValueError("Agent version slots must be strictly ordered")
        if self.train_completed != sum(
            slot.phase == "train" and slot.status == "paired" for slot in self.slots
        ) or self.oos_completed != sum(
            slot.phase == "oos" and slot.status == "paired" for slot in self.slots
        ):
            raise ValueError("Agent version slot states must match pair counts")
        if self.status == "observing" and self.gates:
            raise ValueError("observing Agent version evaluation cannot expose final gates")
        if self.status != "observing" and not self.gates:
            raise ValueError("finalized Agent version evaluation requires gates")
        expected_gate_keys = (
            "authority",
            "oos_pairs",
            "cost_authority",
            "comparison_authority",
            "hard_risk",
            "ledger_conservation",
            "duplicate_execution",
            "action_divergence",
            "challenger_fills",
            "excess_return",
            "positive_excess_sessions",
            "drawdown_degradation",
        )
        if self.gates and tuple(gate.key for gate in self.gates) != expected_gate_keys:
            raise ValueError("finalized Agent version gates must use the fixed order")
        if self.status == "PASS" and (
            self.reason_code != "OOS_IMPROVEMENT_PROVEN"
            or any(gate.status != "PASS" for gate in self.gates)
        ):
            raise ValueError("PASS Agent version evaluation requires every gate")
        if self.status == "FAIL" and not any(gate.status == "FAIL" for gate in self.gates):
            raise ValueError("FAIL Agent version evaluation requires a failed gate")
        if self.status == "UNKNOWN" and not any(gate.status == "UNKNOWN" for gate in self.gates):
            raise ValueError("UNKNOWN Agent version evaluation requires an unknown gate")
        return self


class AgentPaperCyclePageResponse(_StrictSchema):
    items: tuple[AgentPaperCycleResponse, ...] = Field(max_length=20)
    summary: AgentPaperSessionSummaryResponse
    version_observation: AgentPaperVersionObservationResponse | None
    version_history: tuple[AgentPaperVersionObservationResponse, ...] = Field(max_length=20)
    version_evaluation: AgentVersionEvaluationResponse | None

    @model_validator(mode="after")
    def _version_history_is_consistent(self) -> AgentPaperCyclePageResponse:
        if self.version_observation is None:
            if self.version_history:
                raise ValueError("version history requires a current observation")
            return self
        if not self.version_history or self.version_history[0] != self.version_observation:
            raise ValueError("current version observation must lead version history")
        if self.version_history[0].lifecycle_state != "incumbent_frozen" or any(
            item.lifecycle_state != "historical_frozen" for item in self.version_history[1:]
        ):
            raise ValueError("version history must contain exactly one leading incumbent")
        identities = [(item.prompt_hash, item.model_hash) for item in self.version_history]
        if len(identities) != len(set(identities)):
            raise ValueError("version history identities must be unique")
        if any(
            current.last_decision_at < following.last_decision_at
            for current, following in zip(
                self.version_history,
                self.version_history[1:],
                strict=False,
            )
        ):
            raise ValueError("version history must be newest first")
        return self


class AgentPaperCyclePageEnvelope(_StrictSchema):
    data: AgentPaperCyclePageResponse
    meta: SuccessMetaResponse


class PaperCancelRequest(_StrictSchema):
    expected_event_seq: int = Field(ge=1)


class PaperCancelResponse(_StrictSchema):
    order_id: UUID
    status: Literal["cancelled"]
    filled_quantity: DecimalString
    remaining_quantity: DecimalString
    result_event_seq: int = Field(ge=1)
    replayed: bool


class PaperCancelEnvelope(_StrictSchema):
    data: PaperCancelResponse
    meta: SuccessMetaResponse


class PaperCancelOrderFactResponse(_StrictSchema):
    order_id: UUID
    status: InternalOrderStatus | None
    current_event_seq: int | None = Field(default=None, ge=1)
    filled_quantity: DecimalString
    remaining_quantity: DecimalString
    as_of: AwareDatetime | None


class PaperCancelReservationFactResponse(_StrictSchema):
    reservation_id: UUID
    reservation_type: Literal["cash", "position"]
    amount: DecimalString
    outstanding_amount: DecimalString | None
    released: bool | None
    as_of: AwareDatetime | None


class PaperCancelResultResponse(_StrictSchema):
    status: Literal["NOT_RECORDED", "COMMITTED", "CONFLICT", "UNKNOWN"]
    order: PaperCancelOrderFactResponse
    reservation: PaperCancelReservationFactResponse | None
    reservation_released: bool | None
    result_event_seq: int | None = Field(default=None, ge=1)
    committed_at: AwareDatetime | None

    @model_validator(mode="after")
    def _committed_fact_pair_is_coherent(self) -> PaperCancelResultResponse:
        has_committed_facts = self.result_event_seq is not None and self.committed_at is not None
        if (self.status == "COMMITTED") != has_committed_facts:
            raise ValueError("committed cancellation requires its result event and time")
        expected_release = None if self.reservation is None else self.reservation.released
        if self.reservation_released != expected_release:
            raise ValueError("cancellation reservation summary must match its fact")
        return self


class PaperCancelResultEnvelope(_StrictSchema):
    data: PaperCancelResultResponse
    meta: SuccessMetaResponse


class PaperPlanTargetResponse(_StrictSchema):
    target_version_id: UUID
    version_seq: int = Field(ge=1)
    target_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    as_of: AwareDatetime


class PaperPlanProposalTargetResponse(_StrictSchema):
    instrument_id: UUID
    target_weight: DecimalString
    exit: bool


class PaperPlanProposalResponse(_StrictSchema):
    strategy_decision_id: UUID
    decision_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    cash_weight: DecimalString
    targets: tuple[PaperPlanProposalTargetResponse, ...] = Field(max_length=50)
    as_of: AwareDatetime


class PaperPlanRiskResponse(_StrictSchema):
    order_id: UUID
    instrument_id: UUID
    outcome: Literal["approved", "reduced", "rejected"]
    desired_quantity: DecimalString
    approved_quantity: DecimalString
    reduced_quantity: DecimalString
    rejected_quantity: DecimalString
    desired_amount_usd: DecimalString
    approved_amount_usd: DecimalString
    reduced_amount_usd: DecimalString
    rejected_amount_usd: DecimalString


class PaperPlanConfirmedFillResponse(_StrictSchema):
    fill_id: UUID
    quantity: DecimalString
    notional_usd: DecimalString
    as_of: AwareDatetime


class PaperPlanItemResponse(_StrictSchema):
    order_id: UUID
    instrument_id: UUID
    side: Literal["buy", "sell"]
    status: InternalOrderStatus | None
    confirmed_fill_quantity: DecimalString
    confirmed_fills: tuple[PaperPlanConfirmedFillResponse, ...] = Field(max_length=128)
    open_order_coverage_quantity: DecimalString
    open_order_coverage_amount_usd: DecimalString
    cash_reservation_outstanding: DecimalString | None
    position_reservation_outstanding: DecimalString | None
    remaining_target_gap_quantity: DecimalString | None
    remaining_target_gap_amount_usd: DecimalString | None
    order_as_of: AwareDatetime | None
    reservation_as_of: AwareDatetime | None


class PaperPlanFactsResponse(_StrictSchema):
    plan_created_at: AwareDatetime
    reconciliation_as_of: AwareDatetime
    occupation_state: Literal["occupied", "released", "unknown"]
    risk: tuple[PaperPlanRiskResponse, ...] = Field(max_length=50)
    items: tuple[PaperPlanItemResponse, ...] = Field(max_length=50)


class PaperPlanActualPositionResponse(_StrictSchema):
    instrument_id: UUID
    symbol: str | None = None
    quantity: DecimalString
    reserved_quantity: DecimalString


class PaperPlanActualResponse(_StrictSchema):
    data_state: Literal["READY", "UNKNOWN"]
    as_of: AwareDatetime | None
    available_cash: DecimalString | None
    reserved_cash: DecimalString | None
    nav: DecimalString | None
    positions: tuple[PaperPlanActualPositionResponse, ...] | None = Field(max_length=500)

    @model_validator(mode="after")
    def _facts_match_state(self) -> PaperPlanActualResponse:
        facts = (self.available_cash, self.reserved_cash, self.nav, self.positions)
        if self.data_state == "READY" and any(value is None for value in facts):
            raise ValueError("ready actual projection requires every fact")
        if self.data_state == "UNKNOWN" and any(value is not None for value in facts):
            raise ValueError("unknown actual projection cannot fabricate facts")
        if self.data_state == "READY" and self.as_of is None:
            raise ValueError("ready actual projection requires its projection cutoff")
        if self.data_state == "UNKNOWN" and self.as_of is not None:
            raise ValueError("unknown actual projection has no authoritative cutoff")
        return self


class PaperActualEnvelope(_StrictSchema):
    data: PaperPlanActualResponse
    meta: SuccessMetaResponse


class PaperPlanResponse(_StrictSchema):
    batch_id: UUID
    currency: Literal["USD"]
    association_state: Literal["target_associated", "legacy_unassociated"]
    target: PaperPlanTargetResponse | None
    proposal: PaperPlanProposalResponse
    plan: PaperPlanFactsResponse
    actual: PaperPlanActualResponse

    @model_validator(mode="after")
    def _target_association_is_coherent(self) -> PaperPlanResponse:
        if (self.association_state == "target_associated") != (self.target is not None):
            raise ValueError("target association must match the exact target object")
        return self


class PaperPlanEnvelope(_StrictSchema):
    data: PaperPlanResponse
    meta: SuccessMetaResponse
