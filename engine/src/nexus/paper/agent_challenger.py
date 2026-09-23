"""Bounded retrospective and immutable prompt-challenger proposal contracts."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from typing import Literal, cast
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    field_validator,
    model_validator,
)
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.committee import ROLE_ORDER, AgentRole
from nexus.agents.portfolio_synthesis_contracts import (
    SYNTHESIS_PROMPT_VERSION,
    PortfolioChoiceV1,
    PortfolioSynthesisIntentV1,
)
from nexus.audit.canonical import canonical_json
from nexus.audit.service import append_audit_event
from nexus.domain.evidence import EvidenceRef
from nexus.domain.ids import new_uuid7
from nexus.jobs.models import job_input_hash
from nexus.paper.agent_cycle_models import CandidateManifestEntry, candidate_manifest_hash
from nexus.paper.management_read_authority import load_synthesis_read_authority
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPortfolioSynthesisOutputRecord,
    AgentPortfolioSynthesisRunRecord,
    AgentPromptVersionRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.market import InstrumentAlias
from nexus.persistence.models.paper import (
    AgentPaperCandidateBatchRecord,
    AgentPaperCycleEventRecord,
    AgentPaperCycleRecord,
    PaperFillRecord,
    PaperLedgerTransactionRecord,
    PaperOrderBatchRecord,
    PaperOrderEventRecord,
    PaperOrderRecord,
    PaperPerformanceSnapshotRecord,
    StrategyDecisionAgentOutputRecord,
    StrategyDecisionRecord,
)
from nexus.persistence.models.runtime import AuditEvent, Job
from nexus.strategy.intents import ResearchIntent

AGENT_CHALLENGER_PROPOSAL_ACTION = "agent.challenger.proposed"
AGENT_CHALLENGER_PUBLICATION_ACTION = "agent.challenger.published"
PROPOSER_PROMPT_VERSION = "agent-challenger-review-v8"
PROPOSER_PROMPT = """You are the bounded NEXUS Agent retrospective reviewer.
Use only the supplied immutable cycle facts, incumbent portfolio prompt, and output contract.
Propose one small portfolio-role prompt addendum that can produce a measurable behavioral
difference from the incumbent. Do not repeat or contradict the incumbent prompt. Do not claim
investment performance from no-trade observations. Do not change the fixed AAPL scope,
deterministic sizing or risk code, broker permissions, scheduling, or execution_locked=true.
Use only existing output fields; do not request a schema change. The addendum must improve
diagnosis or decision criteria, not force a trade. Set target_role to portfolio. Return exactly
the requested structured object. Preserve every required per-claim review: never merge, remove,
or deduplicate reviews, and keep each unresolved review's missing_evidence and prevented_conclusion.
Any deduplication may apply only to top-level uncertainties after all per-review bindings remain
complete. You have no tools."""
PROPOSER_PROMPT_HASH = hashlib.sha256(PROPOSER_PROMPT.encode()).hexdigest()
PROVIDER_PROJECTION_VERSION = "agent-challenger-provider-v4"
PROMPT_CONTRACT_SUFFIX = """Contract preservation requirements:
- Keep every per-claim synthesis review separate and complete, including claim_id, status,
  rationale, evidence_refs, resolved, missing_evidence, and prevented_conclusion.
- Never merge, remove, or deduplicate per-claim reviews.
- Only top-level uncertainties may merge semantically duplicate gaps, and only after every
  per-review binding remains present and unchanged in the structured output."""
PORTFOLIO_OUTPUT_CONTRACT: dict[str, object] = {
    "schema_version": "portfolio_synthesis_contract_v1",
    "target_role": "portfolio",
    "existing_fields_only": True,
    "decision_fields": (
        "stance",
        "exposure_action",
        "uncertainties",
        "disagreement",
        "role_votes",
        "invalidation_conditions",
        "entry_conditions",
        "exit_conditions",
    ),
    "review_fields": (
        "claim_id",
        "status",
        "rationale",
        "evidence_refs",
        "resolved",
        "missing_evidence",
        "prevented_conclusion",
    ),
    "review_rule": (
        "Keep every bear and evidence_audit claim review separate and complete; only "
        "top-level uncertainties may merge semantically duplicate gaps."
    ),
}

Hash64 = str


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)


class AgentChallengerReviewFactV4(_FrozenModel):
    claim_id: str = Field(pattern=r"^(bear|evidence_audit):[0-9]+$")
    status: Literal["accepted", "rejected", "unresolved"]
    rationale: str = Field(min_length=1, max_length=500)
    evidence_refs: tuple[EvidenceRef, ...] = Field(min_length=1, max_length=3)
    resolved: StrictBool
    missing_evidence: tuple[str, ...] = Field(max_length=4)
    prevented_conclusion: str | None = Field(max_length=500)


class AgentChallengerCycleFactV4(_FrozenModel):
    cycle_id: UUID
    decision_slot_at: AwareDatetime
    result_code: str = Field(min_length=1, max_length=100)
    strategy_decision_id: UUID
    decision_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    sizing_actions: tuple[Literal["increase", "reduce", "maintain", "none"], ...] = Field(
        min_length=1, max_length=5
    )
    research_run_ids: tuple[UUID, ...] = Field(min_length=1, max_length=5)
    settled_cost_usd: str = Field(pattern=r"^(0|[0-9]+(\.[0-9]+)?)$")
    stance: Literal["buy_candidate", "reduce_candidate", "hold", "watch"]
    exposure_action: Literal["none", "reduce", "maintain", "eligible_for_increase"]
    quality_status: Literal["complete", "partial"]
    evidence_audit: Literal["complete", "partial", "failed"]
    uncertainties: tuple[str, ...] = Field(max_length=10)
    disagreement: str = Field(min_length=1, max_length=500)
    role_votes: tuple[str, ...] = Field(min_length=1, max_length=8)
    reviews: tuple[AgentChallengerReviewFactV4, ...] = Field(min_length=1, max_length=4)
    invalidation_signals: tuple[str, ...] = Field(max_length=10)
    entry_signals: tuple[str, ...] = Field(max_length=10)
    exit_signals: tuple[str, ...] = Field(max_length=10)

    @field_validator("research_run_ids")
    @classmethod
    def _runs_are_unique(cls, value: tuple[UUID, ...]) -> tuple[UUID, ...]:
        if len(value) != len(set(value)):
            raise ValueError("AGENT_CHALLENGER_RUNS_DUPLICATED")
        return value


class AgentPromptAuthorityV1(_FrozenModel):
    id: UUID
    version: str = Field(min_length=1, max_length=100)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class AgentChallengerRetrospectiveV4(_FrozenModel):
    schema_version: Literal["agent_challenger_retrospective_v4"]
    instrument_id: UUID
    as_of: AwareDatetime
    incumbent_prompt_manifest: dict[AgentRole, AgentPromptAuthorityV1]
    incumbent_prompt_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    incumbent_portfolio_prompt: str = Field(min_length=1, max_length=8000)
    output_contract: dict[str, object]
    output_contract_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    incumbent_model_config_id: UUID
    incumbent_model_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    cycles: tuple[AgentChallengerCycleFactV4, ...] = Field(min_length=4, max_length=20)
    source_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def _complete_manifest(self) -> AgentChallengerRetrospectiveV4:
        if set(self.incumbent_prompt_manifest) != set(ROLE_ORDER):
            raise ValueError("AGENT_CHALLENGER_PROMPT_MANIFEST_INCOMPLETE")
        if (
            hashlib.sha256(self.incumbent_portfolio_prompt.encode()).hexdigest()
            != self.incumbent_prompt_manifest["portfolio"].content_hash
            or _hash(self.output_contract) != self.output_contract_hash
        ):
            raise ValueError("AGENT_CHALLENGER_RETROSPECTIVE_AUTHORITY_INVALID")
        return self


class AgentChallengerSuggestionV4(_FrozenModel):
    source_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    observed_pattern: str = Field(min_length=1, max_length=1000)
    hypothesis: str = Field(min_length=1, max_length=1000)
    target_role: Literal["portfolio"]
    prompt_addendum: str = Field(min_length=1, max_length=4000)
    expected_behavior_changes: tuple[str, ...] = Field(min_length=1, max_length=5)
    invalidation_conditions: tuple[str, ...] = Field(min_length=1, max_length=5)
    execution_locked: Literal[True]
    fixed_aapl_scope: Literal[True]
    deterministic_risk_unchanged: Literal[True]
    broker_writes_forbidden: Literal[True]
    output_schema_unchanged: Literal[True]


class AgentChallengerProposalV4(_FrozenModel):
    schema_version: Literal["agent_challenger_proposal_v4"]
    proposal_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    instrument_id: UUID
    source_cycle_ids: tuple[UUID, ...] = Field(min_length=4, max_length=20)
    incumbent_prompt_manifest: dict[AgentRole, AgentPromptAuthorityV1]
    incumbent_prompt_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    incumbent_model_config_id: UUID
    incumbent_model_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    observed_pattern: str
    hypothesis: str
    target_role: Literal["portfolio"]
    prompt_addendum: str
    expected_behavior_changes: tuple[str, ...]
    invalidation_conditions: tuple[str, ...]
    execution_locked: Literal[True]
    fixed_aapl_scope: Literal[True]
    deterministic_risk_unchanged: Literal[True]
    broker_writes_forbidden: Literal[True]
    output_schema_unchanged: Literal[True]

    @model_validator(mode="after")
    def _complete_manifest(self) -> AgentChallengerProposalV4:
        if set(self.incumbent_prompt_manifest) != set(ROLE_ORDER):
            raise ValueError("AGENT_CHALLENGER_PROMPT_MANIFEST_INCOMPLETE")
        return self


class AgentChallengerJobOutputV4(_FrozenModel):
    schema_version: Literal["agent_challenger_job_output_v4"]
    job_id: UUID
    proposer_prompt_version: str = Field(min_length=1, max_length=100)
    proposer_prompt_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    proposal: AgentChallengerProposalV4
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    provider_calls: Literal[1]
    tool_calls: Literal[0]


class AgentChallengerPublicationV4(_FrozenModel):
    schema_version: Literal["agent_challenger_publication_v4"]
    proposal_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    challenger_prompt_version_id: UUID
    challenger_prompt_manifest: dict[AgentRole, AgentPromptAuthorityV1]
    challenger_prompt_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    challenger_model_config_id: UUID
    replayed: bool
    execution_locked: Literal[True] = True
    evaluation_registered: Literal[False] = False

    @model_validator(mode="after")
    def _complete_manifest(self) -> AgentChallengerPublicationV4:
        if set(self.challenger_prompt_manifest) != set(ROLE_ORDER):
            raise ValueError("AGENT_CHALLENGER_PROMPT_MANIFEST_INCOMPLETE")
        orchestrator = self.challenger_prompt_manifest["orchestrator"]
        if orchestrator.id != self.challenger_prompt_version_id:
            raise ValueError("AGENT_CHALLENGER_ORCHESTRATOR_MISMATCH")
        return self


class AgentChallengerError(ValueError):
    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


class PortfolioChallengerCandidateFactV1(_FrozenModel):
    instrument_id: UUID
    research_run_id: UUID
    research_output_id: UUID
    research_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    research_intent: ResearchIntent
    choice: PortfolioChoiceV1
    sizing_action: Literal["increase", "reduce", "maintain", "none"]

    @model_validator(mode="after")
    def _candidate_bound(self) -> PortfolioChallengerCandidateFactV1:
        if (
            self.research_intent.instrument_id != self.instrument_id
            or self.choice.instrument_id != self.instrument_id
        ):
            raise ValueError("PORTFOLIO_CHALLENGER_SAMPLE_INCOMPLETE")
        return self


class PortfolioChallengerFillFactV1(_FrozenModel):
    fill_id: UUID
    order_id: UUID
    instrument_id: UUID
    side: Literal["buy", "sell"]
    quantity: Decimal = Field(gt=0)
    price: Decimal = Field(gt=0)
    notional: Decimal = Field(gt=0)
    fee: Decimal = Field(ge=0)
    occurred_at: AwareDatetime
    fill_payload_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    ledger_transaction_id: UUID
    ledger_transaction_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class PortfolioChallengerAccountPerformanceV1(_FrozenModel):
    """Account-level observation; it does not attribute NAV change to one cycle."""

    snapshot_id: UUID
    as_of: AwareDatetime
    nav: Decimal = Field(gt=0)
    cash: Decimal = Field(ge=0)
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    fees: Decimal = Field(ge=0)
    drawdown_ratio: Decimal = Field(ge=0, le=1)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class PortfolioChallengerCycleSampleV1(_FrozenModel):
    """One complete, immutable multi-candidate input for a future portfolio review."""

    schema_version: Literal["portfolio_challenger_cycle_sample_v1"]
    cycle_id: UUID
    account_id: UUID
    portfolio_id: UUID
    decision_slot_at: AwareDatetime
    candidate_batch_id: UUID
    candidate_instrument_ids: tuple[UUID, ...] = Field(min_length=2, max_length=5)
    research_run_ids: tuple[UUID, ...] = Field(min_length=2, max_length=5)
    research_output_ids: tuple[UUID, ...] = Field(min_length=2, max_length=5)
    candidate_facts: tuple[PortfolioChallengerCandidateFactV1, ...] = Field(
        min_length=2, max_length=5
    )
    synthesis_run_id: UUID
    synthesis_output_id: UUID
    synthesis_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    synthesis_prompt_version_id: UUID
    synthesis_prompt_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    synthesis_model_config_id: UUID
    synthesis_model_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    synthesis_intent: PortfolioSynthesisIntentV1
    strategy_decision_id: UUID
    decision_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    result_code: str = Field(min_length=1, max_length=100)
    execution_state: Literal["no_order", "orders_terminal"]
    order_ids: tuple[UUID, ...] = Field(max_length=5)
    order_terminal_statuses: tuple[str, ...] = Field(max_length=5)
    fill_ids: tuple[UUID, ...]
    fill_ledger_transaction_ids: tuple[UUID, ...]
    fill_facts: tuple[PortfolioChallengerFillFactV1, ...]
    performance_state: Literal["missing", "account_observed"]
    account_performance: PortfolioChallengerAccountPerformanceV1 | None
    source_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def _complete_frozen_source(self) -> PortfolioChallengerCycleSampleV1:
        if (
            {item.instrument_id for item in self.candidate_facts}
            != set(self.candidate_instrument_ids)
            or {item.choice.instrument_id for item in self.candidate_facts}
            != set(self.candidate_instrument_ids)
            or set(self.synthesis_intent.compared_instrument_ids)
            != set(self.candidate_instrument_ids)
            or {item.fill_id for item in self.fill_facts} != set(self.fill_ids)
            or {item.ledger_transaction_id for item in self.fill_facts}
            != set(self.fill_ledger_transaction_ids)
            or (self.account_performance is None) != (self.performance_state == "missing")
            or _hash(self.model_dump(mode="json", exclude={"source_hash"})) != self.source_hash
        ):
            raise ValueError("PORTFOLIO_CHALLENGER_SAMPLE_INCOMPLETE")
        return self


async def collect_portfolio_challenger_cycle_sample(
    session: AsyncSession, *, cycle_id: UUID, as_of: datetime
) -> PortfolioChallengerCycleSampleV1:
    """Read a completed portfolio cycle without dropping any candidate or authority binding."""
    code = "PORTFOLIO_CHALLENGER_SAMPLE_INCOMPLETE"
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise AgentChallengerError(code)
    cycle = await session.get(AgentPaperCycleRecord, cycle_id)
    if (
        cycle is None
        or cycle.decision_time > as_of
        or not cycle.execution_locked
        or cycle.candidate_batch_id is None
        or cycle.sizing_policy_version != "portfolio_allocation_v1"
    ):
        raise AgentChallengerError(code)
    try:
        manifest = tuple(
            CandidateManifestEntry.model_validate(row) for row in cycle.candidate_manifest
        )
    except ValueError:
        raise AgentChallengerError(code) from None
    instrument_ids = tuple(sorted(item.instrument_id for item in manifest))
    if (
        not 2 <= len(instrument_ids) <= 5
        or len(set(instrument_ids)) != len(instrument_ids)
        or candidate_manifest_hash(manifest) != cycle.candidate_hash
    ):
        raise AgentChallengerError(code)
    batch = await session.get(AgentPaperCandidateBatchRecord, cycle.candidate_batch_id)
    if batch is None or set(batch.candidate_instrument_ids) != set(instrument_ids):
        raise AgentChallengerError(code)
    latest = await session.scalar(
        select(AgentPaperCycleEventRecord)
        .where(AgentPaperCycleEventRecord.cycle_id == cycle_id)
        .order_by(AgentPaperCycleEventRecord.sequence.desc())
        .limit(1)
    )
    if (
        latest is None
        or latest.to_status not in {"completed", "awaiting_market"}
        or latest.occurred_at > as_of
    ):
        raise AgentChallengerError(code)
    decision = await session.scalar(
        select(StrategyDecisionRecord).where(
            StrategyDecisionRecord.agent_paper_cycle_id == cycle_id,
            StrategyDecisionRecord.decision_origin == "agent_research",
            StrategyDecisionRecord.strategy_version == "agent_portfolio_proposal_v1",
        )
    )
    if (
        decision is None
        or decision.portfolio_synthesis_output_id is None
        or decision.account_id != cycle.account_id
        or decision.portfolio_id != cycle.portfolio_id
        or decision.universe_snapshot_id != cycle.universe_snapshot_id
        or decision.decision_time != cycle.decision_time
    ):
        raise AgentChallengerError(code)
    synthesis_output = await session.get(
        AgentPortfolioSynthesisOutputRecord, decision.portfolio_synthesis_output_id
    )
    if synthesis_output is None or synthesis_output.status != "completed":
        raise AgentChallengerError(code)
    synthesis_run = await session.get(AgentPortfolioSynthesisRunRecord, synthesis_output.run_id)
    if (
        synthesis_run is None
        or synthesis_run.status != "completed"
        or synthesis_run.cycle_id != cycle_id
        or synthesis_run.candidate_batch_id != batch.id
        or synthesis_run.completed_at is None
        or synthesis_run.completed_at > as_of
    ):
        raise AgentChallengerError(code)
    synthesis_prompt = await session.get(AgentPromptVersionRecord, synthesis_run.prompt_version_id)
    synthesis_model = await session.get(AgentModelConfigRecord, synthesis_run.model_config_id)
    if (
        synthesis_prompt is None
        or synthesis_prompt.role != "portfolio_decision"
        or synthesis_prompt.version != SYNTHESIS_PROMPT_VERSION
        or synthesis_prompt.release_channel != "active"
        or hashlib.sha256(synthesis_prompt.content.encode()).hexdigest()
        != synthesis_run.prompt_hash
        or synthesis_prompt.content_hash != synthesis_run.prompt_hash
        or synthesis_model is None
        or synthesis_model.config_hash != synthesis_run.model_hash
    ):
        raise AgentChallengerError(code)
    try:
        synthesis_authority = await load_synthesis_read_authority(
            session, cycle=cycle, read_as_of=as_of
        )
    except ValueError:
        raise AgentChallengerError(code) from None
    if (
        synthesis_authority is None
        or synthesis_authority.output.id != synthesis_output.id
        or synthesis_authority.intent is None
    ):
        raise AgentChallengerError(code)
    synthesis_intent = synthesis_authority.intent
    bindings = tuple(
        await session.scalars(
            select(StrategyDecisionAgentOutputRecord).where(
                StrategyDecisionAgentOutputRecord.strategy_decision_id == decision.id
            )
        )
    )
    output_ids = tuple(binding.agent_research_output_id for binding in bindings)
    if (
        len(output_ids) != len(instrument_ids)
        or len(set(output_ids)) != len(output_ids)
        or set(output_ids) != set(synthesis_run.research_output_ids)
    ):
        raise AgentChallengerError(code)
    rows = (
        await session.execute(
            select(AgentResearchOutputRecord, AgentResearchRunRecord)
            .join(
                AgentResearchRunRecord,
                AgentResearchRunRecord.id == AgentResearchOutputRecord.run_id,
            )
            .where(AgentResearchOutputRecord.id.in_(output_ids))
        )
    ).all()
    prepared = tuple(
        await session.scalars(
            select(AgentPaperCycleEventRecord).where(
                AgentPaperCycleEventRecord.cycle_id == cycle_id,
                AgentPaperCycleEventRecord.result_code == "RESEARCH_JOBS_PREPARED",
            )
        )
    )
    raw_run_ids = prepared[0].fact_ids.get("research_run_ids") if len(prepared) == 1 else None
    if not isinstance(raw_run_ids, list) or not all(isinstance(item, str) for item in raw_run_ids):
        raise AgentChallengerError(code)
    try:
        prepared_run_ids = tuple(UUID(item) for item in raw_run_ids)
    except ValueError:
        raise AgentChallengerError(code) from None
    if (
        len(rows) != len(instrument_ids)
        or {run.instrument_id for _, run in rows} != set(instrument_ids)
        or len(prepared_run_ids) != len(instrument_ids)
        or set(prepared_run_ids) != {run.id for _, run in rows}
        or any(
            output.status != "completed"
            or run.status != "completed"
            or run.decision_time != cycle.decision_time
            for output, run in rows
        )
    ):
        raise AgentChallengerError(code)
    manifest_by_instrument = {item.instrument_id: item for item in manifest}
    choice_by_instrument = {item.instrument_id: item for item in synthesis_intent.choices}
    binding_by_output = {item.agent_research_output_id: item for item in bindings}
    if set(choice_by_instrument) != set(instrument_ids):
        raise AgentChallengerError(code)
    candidate_facts: list[PortfolioChallengerCandidateFactV1] = []
    for output, run in sorted(rows, key=lambda pair: pair[1].instrument_id):
        if output.intent is None:
            raise AgentChallengerError(code)
        try:
            intent = ResearchIntent.model_validate(output.intent)
        except ValueError:
            raise AgentChallengerError(code) from None
        content = {
            "status": output.status,
            "role": output.role,
            "payload": output.payload,
            "intent": output.intent,
        }
        content_hash = hashlib.sha256(
            json.dumps(content, sort_keys=True, separators=(",", ":"), default=str).encode()
        ).hexdigest()
        if (
            output.content_hash != content_hash
            or intent.model_dump(mode="json") != output.intent
            or intent.instrument_id != run.instrument_id
            or intent.data_snapshot_id != run.data_snapshot_id
            or intent.decision_time != run.decision_time
            or intent.display_symbol != manifest_by_instrument[run.instrument_id].symbol
        ):
            raise AgentChallengerError(code)
        sizing_action = binding_by_output[output.id].sizing_action
        if sizing_action not in {"increase", "reduce", "maintain", "none"}:
            raise AgentChallengerError(code)
        candidate_facts.append(
            PortfolioChallengerCandidateFactV1(
                instrument_id=run.instrument_id,
                research_run_id=run.id,
                research_output_id=output.id,
                research_content_hash=output.content_hash,
                research_intent=intent,
                choice=choice_by_instrument[run.instrument_id],
                sizing_action=cast(
                    Literal["increase", "reduce", "maintain", "none"], sizing_action
                ),
            )
        )
    order_batch = await session.scalar(
        select(PaperOrderBatchRecord).where(
            PaperOrderBatchRecord.strategy_decision_id == decision.id
        )
    )
    orders: tuple[PaperOrderRecord, ...] = ()
    if order_batch is not None:
        if order_batch.account_id != cycle.account_id:
            raise AgentChallengerError(code)
        orders = tuple(
            await session.scalars(
                select(PaperOrderRecord)
                .where(PaperOrderRecord.batch_id == order_batch.id)
                .order_by(PaperOrderRecord.id)
            )
        )
    execution_state: Literal["no_order", "orders_terminal"]
    statuses: tuple[str, ...] = ()
    fills: tuple[PaperFillRecord, ...] = ()
    ledger_by_id: dict[UUID, PaperLedgerTransactionRecord] = {}
    if orders:
        if latest.to_status != "awaiting_market" or not 1 <= len(orders) <= 5:
            raise AgentChallengerError(code)
        order_ids = tuple(order.id for order in orders)
        events = tuple(
            await session.scalars(
                select(PaperOrderEventRecord)
                .where(PaperOrderEventRecord.order_id.in_(order_ids))
                .order_by(PaperOrderEventRecord.order_id, PaperOrderEventRecord.event_seq.desc())
            )
        )
        latest_orders: dict[UUID, PaperOrderEventRecord] = {}
        for event in events:
            latest_orders.setdefault(event.order_id, event)
        if len(latest_orders) != len(orders) or any(
            event.to_status not in {"filled", "cancelled", "rejected", "risk_rejected"}
            or event.occurred_at > as_of
            for event in latest_orders.values()
        ):
            raise AgentChallengerError(code)
        statuses = tuple(latest_orders[order.id].to_status for order in orders)
        fills = tuple(
            await session.scalars(
                select(PaperFillRecord)
                .where(PaperFillRecord.order_id.in_(order_ids))
                .order_by(PaperFillRecord.id)
            )
        )
        fill_event_ids = {event.source_fill_id for event in events if event.source_fill_id}
        if (
            any(fill.occurred_at > as_of or fill.account_id != cycle.account_id for fill in fills)
            or {fill.id for fill in fills} != fill_event_ids
            or any(
                sum((fill.quantity for fill in fills if fill.order_id == order.id), Decimal(0))
                != order.quantity
                for order in orders
                if latest_orders[order.id].to_status == "filled"
            )
        ):
            raise AgentChallengerError(code)
        ledger_ids = tuple(fill.ledger_transaction_id for fill in fills)
        ledgers = tuple(
            await session.scalars(
                select(PaperLedgerTransactionRecord).where(
                    PaperLedgerTransactionRecord.id.in_(ledger_ids)
                )
            )
        )
        ledger_by_id = {ledger.id: ledger for ledger in ledgers}
        if len(ledger_by_id) != len(fills) or any(
            ledger_by_id[fill.ledger_transaction_id].account_id != cycle.account_id
            or ledger_by_id[fill.ledger_transaction_id].order_id != fill.order_id
            or ledger_by_id[fill.ledger_transaction_id].transaction_type != "fill"
            for fill in fills
        ):
            raise AgentChallengerError(code)
        execution_state = "orders_terminal"
    else:
        if latest.to_status != "completed" or latest.result_code != "NO_ORDER_REQUIRED":
            raise AgentChallengerError(code)
        execution_state = "no_order"
    order_by_id = {order.id: order for order in orders}
    fill_facts = tuple(
        PortfolioChallengerFillFactV1(
            fill_id=fill.id,
            order_id=fill.order_id,
            instrument_id=fill.instrument_id,
            side=cast(Literal["buy", "sell"], order_by_id[fill.order_id].side),
            quantity=fill.quantity,
            price=fill.price,
            notional=fill.notional,
            fee=fill.fee,
            occurred_at=fill.occurred_at,
            fill_payload_hash=fill.payload_hash,
            ledger_transaction_id=fill.ledger_transaction_id,
            ledger_transaction_hash=ledger_by_id[fill.ledger_transaction_id].transaction_hash,
        )
        for fill in fills
    )
    latest_execution_at = max((fill.occurred_at for fill in fills), default=cycle.decision_time)
    execution_calendar = await session.scalar(
        select(ExchangeCalendarSessionRecord).where(
            ExchangeCalendarSessionRecord.calendar_authority_id == decision.calendar_authority_id,
            ExchangeCalendarSessionRecord.session == cycle.execution_session,
        )
    )
    performance = None
    if execution_calendar is not None and execution_calendar.market_close_at > latest_execution_at:
        performance = await session.scalar(
            select(PaperPerformanceSnapshotRecord).where(
                PaperPerformanceSnapshotRecord.account_id == cycle.account_id,
                PaperPerformanceSnapshotRecord.as_of == execution_calendar.market_close_at,
                PaperPerformanceSnapshotRecord.as_of <= as_of,
                PaperPerformanceSnapshotRecord.created_at <= as_of,
            )
        )
    account_performance = (
        None
        if performance is None
        else PortfolioChallengerAccountPerformanceV1(
            snapshot_id=performance.id,
            as_of=performance.as_of,
            nav=performance.nav,
            cash=performance.cash,
            realized_pnl=performance.realized_pnl,
            unrealized_pnl=performance.unrealized_pnl,
            fees=performance.fees,
            drawdown_ratio=performance.drawdown_ratio,
            content_hash=performance.content_hash,
        )
    )
    payload = {
        "schema_version": "portfolio_challenger_cycle_sample_v1",
        "cycle_id": str(cycle_id),
        "account_id": str(cycle.account_id),
        "portfolio_id": str(cycle.portfolio_id),
        "decision_slot_at": cycle.decision_slot_at.isoformat().replace("+00:00", "Z"),
        "candidate_batch_id": str(batch.id),
        "candidate_instrument_ids": [str(value) for value in instrument_ids],
        "research_run_ids": [str(run.id) for _, run in sorted(rows, key=lambda pair: pair[1].id)],
        "research_output_ids": [str(value) for value in sorted(output_ids)],
        "candidate_facts": [item.model_dump(mode="json") for item in candidate_facts],
        "synthesis_run_id": str(synthesis_run.id),
        "synthesis_output_id": str(synthesis_output.id),
        "synthesis_content_hash": synthesis_output.content_hash,
        "synthesis_prompt_version_id": str(synthesis_prompt.id),
        "synthesis_prompt_hash": synthesis_prompt.content_hash,
        "synthesis_model_config_id": str(synthesis_model.id),
        "synthesis_model_hash": synthesis_model.config_hash,
        "synthesis_intent": synthesis_intent.model_dump(mode="json"),
        "strategy_decision_id": str(decision.id),
        "decision_hash": decision.decision_hash,
        "result_code": latest.result_code,
        "execution_state": execution_state,
        "order_ids": [str(order.id) for order in orders],
        "order_terminal_statuses": list(statuses),
        "fill_ids": [str(fill.id) for fill in fills],
        "fill_ledger_transaction_ids": [str(fill.ledger_transaction_id) for fill in fills],
        "fill_facts": [item.model_dump(mode="json") for item in fill_facts],
        "performance_state": "missing" if performance is None else "account_observed",
        "account_performance": (
            None if account_performance is None else account_performance.model_dump(mode="json")
        ),
    }
    return PortfolioChallengerCycleSampleV1.model_validate(
        {**payload, "source_hash": _hash(payload)}
    )


class PortfolioChallengerRetrospectiveV1(_FrozenModel):
    """Exact portfolio-decision incumbent and bounded historical observations."""

    schema_version: Literal["portfolio_challenger_retrospective_v1"]
    account_id: UUID
    portfolio_id: UUID
    as_of: AwareDatetime
    incumbent_prompt_version_id: UUID
    incumbent_prompt_version: str = Field(min_length=1, max_length=100)
    incumbent_prompt_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    incumbent_prompt_content: str = Field(min_length=1)
    incumbent_model_config_id: UUID
    incumbent_model_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    cycles: tuple[PortfolioChallengerCycleSampleV1, ...] = Field(min_length=1, max_length=20)
    source_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def _bound_history(self) -> PortfolioChallengerRetrospectiveV1:
        if (
            hashlib.sha256(self.incumbent_prompt_content.encode()).hexdigest()
            != self.incumbent_prompt_hash
            or len({item.cycle_id for item in self.cycles}) != len(self.cycles)
            or tuple(sorted(self.cycles, key=lambda item: (item.decision_slot_at, item.cycle_id)))
            != self.cycles
            or any(
                item.account_id != self.account_id
                or item.portfolio_id != self.portfolio_id
                or item.synthesis_prompt_version_id != self.incumbent_prompt_version_id
                or item.synthesis_prompt_hash != self.incumbent_prompt_hash
                or item.synthesis_model_config_id != self.incumbent_model_config_id
                or item.synthesis_model_hash != self.incumbent_model_hash
                or item.decision_slot_at > self.as_of
                for item in self.cycles
            )
            or _hash(self.model_dump(mode="json", exclude={"source_hash"})) != self.source_hash
        ):
            raise ValueError("PORTFOLIO_CHALLENGER_RETROSPECTIVE_INVALID")
        return self


async def collect_portfolio_challenger_retrospective(
    session: AsyncSession,
    *,
    account_id: UUID,
    as_of: datetime,
    limit: int = 20,
) -> PortfolioChallengerRetrospectiveV1:
    """Freeze up to twenty settled portfolio cycles under one actual synthesis prompt."""
    if as_of.tzinfo is None or as_of.utcoffset() is None or not 1 <= limit <= 20:
        raise AgentChallengerError("PORTFOLIO_CHALLENGER_REQUEST_INVALID")
    cycle_ids = tuple(
        await session.scalars(
            select(AgentPaperCycleRecord.id)
            .where(
                AgentPaperCycleRecord.account_id == account_id,
                AgentPaperCycleRecord.candidate_batch_id.is_not(None),
                AgentPaperCycleRecord.decision_time <= as_of,
            )
            .order_by(
                AgentPaperCycleRecord.decision_slot_at.desc(), AgentPaperCycleRecord.id.desc()
            )
            .limit(100)
        )
    )
    samples: list[PortfolioChallengerCycleSampleV1] = []
    anchor: PortfolioChallengerCycleSampleV1 | None = None
    for cycle_id in cycle_ids:
        try:
            sample = await collect_portfolio_challenger_cycle_sample(
                session, cycle_id=cycle_id, as_of=as_of
            )
        except AgentChallengerError as error:
            if error.reason_code != "PORTFOLIO_CHALLENGER_SAMPLE_INCOMPLETE":
                raise
            continue
        if anchor is None:
            anchor = sample
        if (
            sample.portfolio_id == anchor.portfolio_id
            and sample.synthesis_prompt_version_id == anchor.synthesis_prompt_version_id
            and sample.synthesis_prompt_hash == anchor.synthesis_prompt_hash
            and sample.synthesis_model_config_id == anchor.synthesis_model_config_id
            and sample.synthesis_model_hash == anchor.synthesis_model_hash
        ):
            samples.append(sample)
        if len(samples) == limit:
            break
    if anchor is None:
        raise AgentChallengerError("PORTFOLIO_CHALLENGER_HISTORY_INSUFFICIENT")
    prompt = await session.get(AgentPromptVersionRecord, anchor.synthesis_prompt_version_id)
    if (
        prompt is None
        or prompt.role != "portfolio_decision"
        or prompt.version != SYNTHESIS_PROMPT_VERSION
        or prompt.release_channel != "active"
        or prompt.content_hash != anchor.synthesis_prompt_hash
        or hashlib.sha256(prompt.content.encode()).hexdigest() != prompt.content_hash
    ):
        raise AgentChallengerError("PORTFOLIO_CHALLENGER_PROMPT_INVALID")
    ordered = tuple(sorted(samples, key=lambda item: (item.decision_slot_at, item.cycle_id)))
    payload = {
        "schema_version": "portfolio_challenger_retrospective_v1",
        "account_id": str(account_id),
        "portfolio_id": str(anchor.portfolio_id),
        "as_of": as_of.isoformat().replace("+00:00", "Z"),
        "incumbent_prompt_version_id": str(prompt.id),
        "incumbent_prompt_version": prompt.version,
        "incumbent_prompt_hash": prompt.content_hash,
        "incumbent_prompt_content": prompt.content,
        "incumbent_model_config_id": str(anchor.synthesis_model_config_id),
        "incumbent_model_hash": anchor.synthesis_model_hash,
        "cycles": [item.model_dump(mode="json") for item in ordered],
    }
    return PortfolioChallengerRetrospectiveV1.model_validate(
        {**payload, "source_hash": _hash(payload)}
    )


def freeze_agent_challenger_retrospective_v4(
    *,
    instrument_id: UUID,
    as_of: datetime,
    incumbent_prompt_manifest: dict[AgentRole, dict[str, object]],
    incumbent_prompt_hash: Hash64,
    incumbent_portfolio_prompt: str,
    output_contract: dict[str, object],
    incumbent_model_config_id: UUID,
    incumbent_model_hash: Hash64,
    cycles: tuple[AgentChallengerCycleFactV4, ...],
) -> AgentChallengerRetrospectiveV4:
    ordered = tuple(sorted(cycles, key=lambda item: (item.decision_slot_at, item.cycle_id)))
    payload = {
        "schema_version": "agent_challenger_retrospective_v4",
        "instrument_id": str(instrument_id),
        "as_of": as_of.isoformat(),
        "incumbent_prompt_manifest": incumbent_prompt_manifest,
        "incumbent_prompt_hash": incumbent_prompt_hash,
        "incumbent_portfolio_prompt": incumbent_portfolio_prompt,
        "output_contract": output_contract,
        "output_contract_hash": _hash(output_contract),
        "incumbent_model_config_id": str(incumbent_model_config_id),
        "incumbent_model_hash": incumbent_model_hash,
        "cycles": [item.model_dump(mode="json") for item in ordered],
    }
    return AgentChallengerRetrospectiveV4.model_validate({**payload, "source_hash": _hash(payload)})


def verify_agent_challenger_retrospective_v4(
    retrospective: AgentChallengerRetrospectiveV4,
) -> None:
    payload = {
        "schema_version": retrospective.schema_version,
        "instrument_id": str(retrospective.instrument_id),
        "as_of": retrospective.as_of.isoformat(),
        "incumbent_prompt_manifest": {
            role: item.model_dump(mode="json")
            for role, item in retrospective.incumbent_prompt_manifest.items()
        },
        "incumbent_prompt_hash": retrospective.incumbent_prompt_hash,
        "incumbent_portfolio_prompt": retrospective.incumbent_portfolio_prompt,
        "output_contract": retrospective.output_contract,
        "output_contract_hash": retrospective.output_contract_hash,
        "incumbent_model_config_id": str(retrospective.incumbent_model_config_id),
        "incumbent_model_hash": retrospective.incumbent_model_hash,
        "cycles": [item.model_dump(mode="json") for item in retrospective.cycles],
    }
    if _hash(payload) != retrospective.source_hash:
        raise ValueError("AGENT_CHALLENGER_RETROSPECTIVE_INVALID")


def freeze_agent_challenger_proposal_v4(
    retrospective: AgentChallengerRetrospectiveV4,
    suggestion: AgentChallengerSuggestionV4,
) -> AgentChallengerProposalV4:
    if suggestion.source_hash != retrospective.source_hash:
        raise ValueError("AGENT_CHALLENGER_SOURCE_MISMATCH")
    prompt_addendum = f"{suggestion.prompt_addendum.rstrip()}\n\n{PROMPT_CONTRACT_SUFFIX}"
    payload = {
        "schema_version": "agent_challenger_proposal_v4",
        "source_hash": retrospective.source_hash,
        "instrument_id": str(retrospective.instrument_id),
        "source_cycle_ids": [str(item.cycle_id) for item in retrospective.cycles],
        "incumbent_prompt_manifest": {
            role: item.model_dump(mode="json")
            for role, item in retrospective.incumbent_prompt_manifest.items()
        },
        "incumbent_prompt_hash": retrospective.incumbent_prompt_hash,
        "incumbent_model_config_id": str(retrospective.incumbent_model_config_id),
        "incumbent_model_hash": retrospective.incumbent_model_hash,
        **suggestion.model_dump(mode="json", exclude={"source_hash"}),
        "prompt_addendum": prompt_addendum,
    }
    return AgentChallengerProposalV4.model_validate({**payload, "proposal_hash": _hash(payload)})


async def collect_agent_challenger_retrospective(
    session: AsyncSession,
    *,
    instrument_id: UUID,
    as_of: datetime,
    limit: int = 20,
) -> AgentChallengerRetrospectiveV4:
    """Freeze 4-20 completed cycles for the current active prompt/model authority."""
    if not 4 <= limit <= 20:
        raise AgentChallengerError("AGENT_CHALLENGER_LIMIT_INVALID")
    if not await _is_aapl_at(session, instrument_id=instrument_id, on_date=as_of.date()):
        raise AgentChallengerError("AGENT_CHALLENGER_SCOPE_INVALID")
    from nexus.agents.persistence import load_complete_prompt_authority

    active = await load_complete_prompt_authority(session)
    if active is None:
        raise AgentChallengerError("AGENT_CHALLENGER_ACTIVE_AUTHORITY_MISSING")
    active_manifest: dict[AgentRole, dict[str, object]] = {
        role: {
            "id": str(active[role].id),
            "version": active[role].version,
            "content_hash": active[role].content_hash,
        }
        for role in ROLE_ORDER
    }
    active_prompt_hash = hashlib.sha256(
        json.dumps(active_manifest, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    rows = (
        await session.execute(
            select(
                AgentPaperCycleRecord,
                StrategyDecisionRecord,
                StrategyDecisionAgentOutputRecord,
                AgentResearchOutputRecord,
                AgentResearchRunRecord,
            )
            .join(
                StrategyDecisionRecord,
                StrategyDecisionRecord.agent_paper_cycle_id == AgentPaperCycleRecord.id,
            )
            .join(
                StrategyDecisionAgentOutputRecord,
                StrategyDecisionAgentOutputRecord.strategy_decision_id == StrategyDecisionRecord.id,
            )
            .join(
                AgentResearchOutputRecord,
                AgentResearchOutputRecord.id
                == StrategyDecisionAgentOutputRecord.agent_research_output_id,
            )
            .join(
                AgentResearchRunRecord,
                AgentResearchRunRecord.id == AgentResearchOutputRecord.run_id,
            )
            .where(
                AgentResearchRunRecord.instrument_id == instrument_id,
                AgentResearchRunRecord.status == "completed",
                StrategyDecisionRecord.decision_origin == "agent_research",
                AgentPaperCycleRecord.execution_locked.is_(True),
                AgentPaperCycleRecord.decision_time <= as_of,
            )
            .order_by(
                AgentPaperCycleRecord.decision_slot_at.desc(),
                AgentPaperCycleRecord.id.desc(),
                AgentResearchRunRecord.id,
            )
            .limit(200)
        )
    ).all()
    if not rows:
        raise AgentChallengerError("AGENT_CHALLENGER_HISTORY_INSUFFICIENT")
    by_cycle: dict[
        UUID,
        list[
            tuple[
                AgentPaperCycleRecord,
                StrategyDecisionRecord,
                StrategyDecisionAgentOutputRecord,
                AgentResearchOutputRecord,
                AgentResearchRunRecord,
            ]
        ],
    ] = defaultdict(list)
    ordered_cycle_ids: list[UUID] = []
    for row in rows:
        cycle = row[0]
        if cycle.id not in by_cycle:
            ordered_cycle_ids.append(cycle.id)
        by_cycle[cycle.id].append((row[0], row[1], row[2], row[3], row[4]))
    latest_events: dict[UUID, AgentPaperCycleEventRecord] = {}
    for event in await session.scalars(
        select(AgentPaperCycleEventRecord)
        .where(AgentPaperCycleEventRecord.cycle_id.in_(ordered_cycle_ids))
        .order_by(
            AgentPaperCycleEventRecord.cycle_id,
            AgentPaperCycleEventRecord.sequence.desc(),
        )
    ):
        latest_events.setdefault(event.cycle_id, event)

    anchor_model_id: UUID | None = None
    anchor_model_hash: str | None = None
    facts: list[AgentChallengerCycleFactV4] = []
    for cycle_id in ordered_cycle_ids:
        grouped = by_cycle[cycle_id]
        cycle, decision = grouped[0][0], grouped[0][1]
        latest = latest_events.get(cycle_id)
        if latest is None or latest.to_status != "completed":
            continue
        try:
            manifest = tuple(
                CandidateManifestEntry.model_validate(item) for item in cycle.candidate_manifest
            )
        except ValueError:
            continue
        if (
            len(manifest) != 1
            or manifest[0].instrument_id != instrument_id
            or manifest[0].symbol != "AAPL"
            or candidate_manifest_hash(manifest) != cycle.candidate_hash
        ):
            continue
        outputs = {item[3].id: item[3] for item in grouped}
        runs = {item[4].id: item[4] for item in grouped}
        bindings = tuple(item[2] for item in grouped)
        if len(runs) != 1 or len(outputs) != 1 or len(bindings) != 1:
            continue
        if any(
            run.prompt_manifest != active_manifest or run.prompt_hash != active_prompt_hash
            for run in runs.values()
        ):
            continue
        run = next(iter(runs.values()))
        current_model = await session.get(AgentModelConfigRecord, run.model_config_id)
        if (
            current_model is None
            or current_model.config_hash != run.model_hash
            or current_model.version != run.pricing_version
            or run.settled_cost_usd is None
        ):
            continue
        current_model_id = current_model.id
        current_model_hash = current_model.config_hash
        if anchor_model_id is None:
            anchor_model_id, anchor_model_hash = current_model_id, current_model_hash
        if current_model_id != anchor_model_id or current_model_hash != anchor_model_hash:
            continue
        output = next(iter(outputs.values()))
        if output.intent is None:
            continue
        try:
            intent = ResearchIntent.model_validate(output.intent)
        except ValueError:
            continue
        settled = sum(
            (cast(Decimal, item.settled_cost_usd) for item in runs.values()), Decimal("0")
        )
        assessment = intent.synthesis_assessment
        if assessment is None or not assessment.reviews:
            continue
        reviews = tuple(
            AgentChallengerReviewFactV4.model_validate(review.model_dump(mode="json"))
            for review in assessment.reviews
        )
        facts.append(
            AgentChallengerCycleFactV4(
                cycle_id=cycle.id,
                decision_slot_at=cycle.decision_slot_at,
                result_code=latest.result_code,
                strategy_decision_id=decision.id,
                decision_hash=decision.decision_hash,
                sizing_actions=tuple(
                    cast(Literal["increase", "reduce", "maintain", "none"], action)
                    for action in sorted(binding.sizing_action for binding in bindings)
                ),
                research_run_ids=tuple(sorted(runs)),
                settled_cost_usd=format(settled, "f"),
                stance=intent.stance,
                exposure_action=intent.exposure_action,
                quality_status=intent.quality_status,
                evidence_audit=intent.evidence_audit,
                uncertainties=intent.uncertainties,
                disagreement=intent.disagreement,
                role_votes=tuple(f"{vote.role}:{vote.vote}" for vote in intent.role_votes),
                reviews=reviews,
                invalidation_signals=tuple(
                    condition.signal for condition in intent.invalidation_conditions
                ),
                entry_signals=tuple(condition.signal for condition in intent.entry_conditions),
                exit_signals=tuple(condition.signal for condition in intent.exit_conditions),
            )
        )
        if len(facts) == limit:
            break
    if len(facts) < 4 or anchor_model_id is None or anchor_model_hash is None:
        raise AgentChallengerError("AGENT_CHALLENGER_HISTORY_INSUFFICIENT")
    model = await session.get(AgentModelConfigRecord, anchor_model_id)
    if model is None or model.config_hash != anchor_model_hash:
        raise AgentChallengerError("AGENT_CHALLENGER_MODEL_AUTHORITY_MISSING")
    output_contract = PORTFOLIO_OUTPUT_CONTRACT
    return freeze_agent_challenger_retrospective_v4(
        instrument_id=instrument_id,
        as_of=as_of,
        incumbent_prompt_manifest=active_manifest,
        incumbent_prompt_hash=active_prompt_hash,
        incumbent_portfolio_prompt=active["portfolio"].content,
        output_contract=output_contract,
        incumbent_model_config_id=model.id,
        incumbent_model_hash=model.config_hash,
        cycles=tuple(facts),
    )


async def publish_agent_challenger(
    session: AsyncSession,
    *,
    proposal_hash: str,
    actor_id: str,
    now: datetime,
) -> AgentChallengerPublicationV4:
    """Publish one confirmed proposal without changing the active prompt authority."""
    if len(proposal_hash) != 64 or any(char not in "0123456789abcdef" for char in proposal_hash):
        raise ValueError("AGENT_CHALLENGER_PROPOSAL_HASH_INVALID")
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 1421))"),
        {"scope": f"agent-challenger-publish:{proposal_hash}"},
    )
    publication_event = await session.scalar(
        select(AuditEvent).where(
            AuditEvent.action == AGENT_CHALLENGER_PUBLICATION_ACTION,
            AuditEvent.schema_version == "agent_challenger_publication_v4",
            AuditEvent.payload["proposal_hash"].as_string() == proposal_hash,
        )
    )
    if publication_event is not None:
        replay = AgentChallengerPublicationV4.model_validate(publication_event.payload)
        replay_manifest: dict[AgentRole, dict[str, object]] = {}
        for role, item in replay.challenger_prompt_manifest.items():
            prompt = await session.get(AgentPromptVersionRecord, item.id)
            if (
                prompt is None
                or prompt.role != role
                or prompt.version != item.version
                or prompt.content_hash != item.content_hash
                or hashlib.sha256(prompt.content.encode()).hexdigest() != item.content_hash
                or prompt.release_channel != "challenger"
            ):
                raise ValueError("AGENT_CHALLENGER_PUBLICATION_INVALID")
            replay_manifest[role] = item.model_dump(mode="json")
        replay_hash = hashlib.sha256(
            json.dumps(replay_manifest, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        if replay_hash != replay.challenger_prompt_hash:
            raise ValueError("AGENT_CHALLENGER_PUBLICATION_INVALID")
        return replay.model_copy(update={"replayed": True})

    proposal_event = await session.scalar(
        select(AuditEvent).where(
            AuditEvent.action == AGENT_CHALLENGER_PROPOSAL_ACTION,
            AuditEvent.schema_version == "agent_challenger_job_output_v4",
            AuditEvent.payload["proposal"]["proposal_hash"].as_string() == proposal_hash,
        )
    )
    if proposal_event is None:
        raise ValueError("AGENT_CHALLENGER_PROPOSAL_UNKNOWN")
    output = AgentChallengerJobOutputV4.model_validate(proposal_event.payload)
    proposal = output.proposal
    unhashed = proposal.model_dump(mode="json", exclude={"proposal_hash"})
    if _hash(unhashed) != proposal.proposal_hash:
        raise ValueError("AGENT_CHALLENGER_PROPOSAL_INVALID")
    job = await session.get(Job, output.job_id)
    if (
        job is None
        or job.job_type != "agent.challenger_propose"
        or job.schema_version != "agent_challenger_job_v4"
        or job.status != "succeeded"
        or job.result_code != "AGENT_CHALLENGER_PROPOSED"
        or job.queue != "general"
        or job.scope_kind != "instrument"
        or job.scope_id != str(proposal.instrument_id)
        or job.retry_policy != "never"
        or job.max_attempts != 1
        or job.attempt_count != 1
        or job.input_hash != job_input_hash(job.payload)
        or job.payload.get("proposer_prompt_version") != PROPOSER_PROMPT_VERSION
        or job.payload.get("proposer_prompt_hash") != PROPOSER_PROMPT_HASH
        or job.payload.get("provider_projection_version") != PROVIDER_PROJECTION_VERSION
        or output.proposer_prompt_version != PROPOSER_PROMPT_VERSION
        or output.proposer_prompt_hash != PROPOSER_PROMPT_HASH
    ):
        raise ValueError("AGENT_CHALLENGER_JOB_AUTHORITY_INVALID")
    try:
        retrospective = AgentChallengerRetrospectiveV4.model_validate(job.payload["retrospective"])
    except (KeyError, ValueError) as error:
        raise ValueError("AGENT_CHALLENGER_JOB_AUTHORITY_INVALID") from error
    try:
        verify_agent_challenger_retrospective_v4(retrospective)
        history_limit_value = job.payload["history_limit"]
        if type(history_limit_value) is not int:
            raise TypeError("history_limit must be an integer")
        history_limit = history_limit_value
        request_hash = job_input_hash(
            {
                "schema_version": "agent_challenger_request_v4",
                "instrument_id": str(retrospective.instrument_id),
                "history_limit": history_limit,
            }
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("AGENT_CHALLENGER_JOB_AUTHORITY_INVALID") from error
    if (
        not 4 <= history_limit <= 20
        or job.payload.get("request_hash") != request_hash
        or retrospective.source_hash != proposal.source_hash
        or tuple(item.cycle_id for item in retrospective.cycles) != proposal.source_cycle_ids
        or retrospective.instrument_id != proposal.instrument_id
    ):
        raise ValueError("AGENT_CHALLENGER_JOB_AUTHORITY_INVALID")
    if not await _is_aapl_at(session, instrument_id=proposal.instrument_id, on_date=now.date()):
        raise ValueError("AGENT_CHALLENGER_SCOPE_INVALID")

    # Import here to keep the pure proposal contracts cheap to import in tests.
    from nexus.agents.persistence import load_complete_prompt_authority

    active = await load_complete_prompt_authority(session)
    if active is None:
        raise ValueError("AGENT_CHALLENGER_ACTIVE_AUTHORITY_MISSING")
    active_manifest = {
        role: {
            "id": str(active[role].id),
            "version": active[role].version,
            "content_hash": active[role].content_hash,
        }
        for role in ROLE_ORDER
    }
    active_prompt_hash = hashlib.sha256(
        json.dumps(active_manifest, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    proposal_manifest = {
        role: item.model_dump(mode="json")
        for role, item in proposal.incumbent_prompt_manifest.items()
    }
    if active_manifest != proposal_manifest or active_prompt_hash != proposal.incumbent_prompt_hash:
        raise ValueError("AGENT_CHALLENGER_INCUMBENT_CHANGED")
    model = await session.get(AgentModelConfigRecord, proposal.incumbent_model_config_id)
    if model is None or model.config_hash != proposal.incumbent_model_hash:
        raise ValueError("AGENT_CHALLENGER_MODEL_AUTHORITY_CHANGED")

    version = f"agent-challenger-{proposal.proposal_hash[:16]}"
    bundle: dict[AgentRole, AgentPromptVersionRecord] = {}
    for role in ROLE_ORDER:
        incumbent = active[role]
        content = incumbent.content
        if role == proposal.target_role:
            content = (
                f"{content.rstrip()}\n\n"
                "## Frozen challenger addendum\n"
                f"{proposal.prompt_addendum.strip()}\n"
            )
        content_hash = hashlib.sha256(content.encode()).hexdigest()
        prompt = await session.scalar(
            select(AgentPromptVersionRecord).where(
                AgentPromptVersionRecord.role == role,
                AgentPromptVersionRecord.version == version,
            )
        )
        if prompt is None:
            prompt = AgentPromptVersionRecord(
                id=new_uuid7(),
                role=role,
                version=version,
                release_channel="challenger",
                content_hash=content_hash,
                content=content,
            )
            session.add(prompt)
            await session.flush()
        elif (
            prompt.release_channel != "challenger"
            or prompt.content_hash != content_hash
            or prompt.content != content
        ):
            raise ValueError("AGENT_CHALLENGER_PROMPT_VERSION_CONFLICT")
        bundle[role] = prompt
    bundle_manifest: dict[AgentRole, dict[str, object]] = {
        role: {
            "id": str(bundle[role].id),
            "version": bundle[role].version,
            "content_hash": bundle[role].content_hash,
        }
        for role in ROLE_ORDER
    }
    bundle_hash = hashlib.sha256(
        json.dumps(bundle_manifest, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    publication_manifest = {
        role: AgentPromptAuthorityV1.model_validate(item) for role, item in bundle_manifest.items()
    }
    publication = AgentChallengerPublicationV4(
        schema_version="agent_challenger_publication_v4",
        proposal_hash=proposal.proposal_hash,
        challenger_prompt_version_id=bundle["orchestrator"].id,
        challenger_prompt_manifest=publication_manifest,
        challenger_prompt_hash=bundle_hash,
        challenger_model_config_id=model.id,
        replayed=False,
    )
    await append_audit_event(
        session,
        actor_id=actor_id,
        action=AGENT_CHALLENGER_PUBLICATION_ACTION,
        schema_version=publication.schema_version,
        payload=publication.model_dump(mode="json"),
        occurred_at=now,
    )
    return publication


def _hash(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


async def _is_aapl_at(
    session: AsyncSession,
    *,
    instrument_id: UUID,
    on_date: date,
) -> bool:
    alias_id = await session.scalar(
        select(InstrumentAlias.id)
        .where(
            InstrumentAlias.instrument_id == instrument_id,
            InstrumentAlias.symbol == "AAPL",
            InstrumentAlias.alias_type == "ticker",
            InstrumentAlias.valid_from <= on_date,
            (InstrumentAlias.valid_to.is_(None) | (InstrumentAlias.valid_to >= on_date)),
        )
        .limit(1)
    )
    return alias_id is not None
