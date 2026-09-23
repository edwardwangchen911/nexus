"""One bounded, tool-free Codex retrospective for an Agent prompt challenger."""

from __future__ import annotations

import hashlib
import json
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

from nexus.agents.codex_sdk import (
    MODEL_ID,
    codex_input_allowance,
    codex_output_schema,
    codex_request_wire,
    invoke_codex,
)
from nexus.agents.codex_token_budget import INPUT_RESERVE
from nexus.agents.responses_budget import UsageSnapshot
from nexus.paper.agent_challenger import (
    PORTFOLIO_PROPOSER_PROMPT,
    PORTFOLIO_PROPOSER_PROMPT_HASH,
    PORTFOLIO_PROPOSER_PROMPT_VERSION,
    PORTFOLIO_PROVIDER_PROJECTION_VERSION,
    PROPOSER_PROMPT,
    PROPOSER_PROMPT_HASH,
    PROPOSER_PROMPT_VERSION,
    PROVIDER_PROJECTION_VERSION,
    AgentChallengerRetrospectiveV4,
    AgentChallengerSuggestionV4,
    PortfolioChallengerRetrospectiveV1,
    PortfolioChallengerSuggestionV1,
)
from nexus.persistence.models.agents import AgentModelConfigRecord

CHALLENGER_MAX_EXPLICIT_INPUT_UNITS = 10_000
CHALLENGER_MAX_REPORTED_INPUT_TOKENS = 16_000


class AgentChallengerSuggestionWireV4(BaseModel):
    """Small provider contract; deterministic code adds every safety boundary."""

    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    source_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    observed_pattern: str = Field(min_length=1, max_length=1000)
    hypothesis: str = Field(min_length=1, max_length=1000)
    target_role: Literal["portfolio"]
    prompt_addendum: str = Field(min_length=1, max_length=4000)
    expected_behavior_changes: tuple[str, ...] = Field(min_length=1, max_length=5)
    invalidation_conditions: tuple[str, ...] = Field(min_length=1, max_length=5)


class PortfolioChallengerSuggestionWireV1(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    source_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    observed_pattern: str = Field(min_length=1, max_length=1000)
    hypothesis: str = Field(min_length=1, max_length=1000)
    target_role: Literal["portfolio_decision"]
    prompt_addendum: str = Field(min_length=1, max_length=4000)
    expected_behavior_changes: tuple[str, ...] = Field(min_length=1, max_length=5)
    invalidation_conditions: tuple[str, ...] = Field(min_length=1, max_length=5)


class PortfolioChallengerRuntime(Protocol):
    async def generate(
        self, retrospective: PortfolioChallengerRetrospectiveV1
    ) -> tuple[PortfolioChallengerSuggestionV1, UsageSnapshot]: ...


def compact_portfolio_challenger_retrospective(
    retrospective: PortfolioChallengerRetrospectiveV1,
) -> dict[str, object]:
    """Expose bounded decision semantics while retaining exact source-hash authority."""
    cycles: list[dict[str, object]] = []
    for cycle in retrospective.cycles:
        candidates: list[dict[str, object]] = []
        for fact in cycle.candidate_facts:
            assessment = fact.research_intent.synthesis_assessment
            candidates.append(
                {
                    "instrument_id": str(fact.instrument_id),
                    "symbol": fact.research_intent.display_symbol,
                    "research_content_hash": fact.research_content_hash,
                    "stance": fact.research_intent.stance,
                    "exposure_action": fact.research_intent.exposure_action,
                    "quality_status": fact.research_intent.quality_status,
                    "evidence_audit": fact.research_intent.evidence_audit,
                    "uncertainties": list(fact.research_intent.uncertainties),
                    "review_conclusion": None
                    if assessment is None
                    else assessment.review_conclusion,
                    "reviews": (
                        []
                        if assessment is None
                        else [
                            {
                                "claim_id": review.claim_id,
                                "status": review.status,
                                "resolved": review.resolved,
                                "missing_evidence": list(review.missing_evidence),
                                "prevented_conclusion": review.prevented_conclusion,
                            }
                            for review in assessment.reviews
                        ]
                    ),
                    "choice": fact.choice.model_dump(mode="json"),
                    "sizing_action": fact.sizing_action,
                }
            )
        cycles.append(
            {
                "cycle_id": str(cycle.cycle_id),
                "source_hash": cycle.source_hash,
                "result_code": cycle.result_code,
                "compared_instrument_ids": [str(value) for value in cycle.candidate_instrument_ids],
                "candidates": candidates,
                "cash_rationale": cycle.synthesis_intent.cash_rationale.model_dump(mode="json"),
                "execution_state": cycle.execution_state,
                "order_terminal_statuses": list(cycle.order_terminal_statuses),
                "fills": [
                    {
                        "instrument_id": str(fill.instrument_id),
                        "side": fill.side,
                        "quantity": str(fill.quantity),
                        "price": str(fill.price),
                        "fee": str(fill.fee),
                    }
                    for fill in cycle.fill_facts
                ],
                "performance_state": cycle.performance_state,
                "account_performance": (
                    None
                    if cycle.account_performance is None
                    else {
                        "as_of": cycle.account_performance.as_of.isoformat(),
                        "nav": str(cycle.account_performance.nav),
                        "drawdown_ratio": str(cycle.account_performance.drawdown_ratio),
                        "fees": str(cycle.account_performance.fees),
                        "account_level_only": True,
                    }
                ),
            }
        )
    return {
        "schema_version": PORTFOLIO_PROVIDER_PROJECTION_VERSION,
        "source_hash": retrospective.source_hash,
        "incumbent_prompt_content": retrospective.incumbent_prompt_content,
        "incumbent_prompt_hash": retrospective.incumbent_prompt_hash,
        "performance_attribution": "not_established",
        "cycles": cycles,
    }


class CodexPortfolioChallengerRuntime:
    """One tool-free model proposal for the actual portfolio-decision prompt."""

    def __init__(self, model: AgentModelConfigRecord) -> None:
        if (
            model.model_id != MODEL_ID
            or model.max_input_tokens < CHALLENGER_MAX_REPORTED_INPUT_TOKENS
            or model.max_output_tokens < 8_192
            or model.input_usd_per_million != 0
            or model.output_usd_per_million != 0
        ):
            raise ValueError("PORTFOLIO_CHALLENGER_MODEL_UNSUPPORTED")
        self._model_id = model.id
        self._model_hash = model.config_hash
        self._max_input_tokens = model.max_input_tokens
        self._attempted = False

    async def generate(
        self, retrospective: PortfolioChallengerRetrospectiveV1
    ) -> tuple[PortfolioChallengerSuggestionV1, UsageSnapshot]:
        if self._attempted:
            raise ValueError("PORTFOLIO_CHALLENGER_REPLAY_FORBIDDEN")
        if (
            retrospective.incumbent_model_config_id != self._model_id
            or retrospective.incumbent_model_hash != self._model_hash
        ):
            raise ValueError("PORTFOLIO_CHALLENGER_MODEL_AUTHORITY_CHANGED")
        self._attempted = True
        payload = {
            "schema_version": "portfolio_challenger_codex_input_v1",
            "instructions": PORTFOLIO_PROPOSER_PROMPT,
            "proposer_prompt_version": PORTFOLIO_PROPOSER_PROMPT_VERSION,
            "proposer_prompt_hash": PORTFOLIO_PROPOSER_PROMPT_HASH,
            "retrospective": compact_portfolio_challenger_retrospective(retrospective),
        }
        wire = codex_request_wire(payload)
        allowance = codex_input_allowance(
            wire, codex_output_schema(PortfolioChallengerSuggestionWireV1)
        )
        if allowance > min(
            self._max_input_tokens, CHALLENGER_MAX_EXPLICIT_INPUT_UNITS + INPUT_RESERVE
        ):
            raise ValueError("PORTFOLIO_CHALLENGER_INPUT_TOO_LARGE")
        raw, usage = await invoke_codex(payload, output_model=PortfolioChallengerSuggestionWireV1)
        wire_suggestion = PortfolioChallengerSuggestionWireV1.model_validate_json(raw)
        suggestion = PortfolioChallengerSuggestionV1(
            **wire_suggestion.model_dump(),
            execution_locked=True,
            deterministic_risk_unchanged=True,
            broker_writes_forbidden=True,
            output_schema_unchanged=True,
            human_review_required=True,
        )
        if suggestion.source_hash != retrospective.source_hash:
            raise ValueError("PORTFOLIO_CHALLENGER_SOURCE_MISMATCH")
        if (
            usage.input_tokens > CHALLENGER_MAX_REPORTED_INPUT_TOKENS
            or usage.provider_calls != 1
            or usage.tool_calls != 0
        ):
            raise ValueError("PORTFOLIO_CHALLENGER_USAGE_INVALID")
        return suggestion, usage


class AgentChallengerRuntime(Protocol):
    async def generate(
        self, retrospective: AgentChallengerRetrospectiveV4
    ) -> tuple[AgentChallengerSuggestionV4, UsageSnapshot]: ...


def compact_agent_challenger_retrospective(
    retrospective: AgentChallengerRetrospectiveV4,
) -> dict[str, object]:
    """Project frozen facts into a lossless per-review, deduplicated provider view."""
    text_catalog: list[str] = []
    text_indexes: dict[str, int] = {}

    def text_index(value: str) -> int:
        if value not in text_indexes:
            text_indexes[value] = len(text_catalog)
            text_catalog.append(value)
        return text_indexes[value]

    cycles: list[list[object]] = []
    for cycle in retrospective.cycles:
        reviews: list[list[object]] = []
        for review in cycle.reviews:
            evidence_payload = [
                evidence_ref.model_dump(mode="json") for evidence_ref in review.evidence_refs
            ]
            evidence_binding_hash = hashlib.sha256(
                json.dumps(evidence_payload, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
            reviews.append(
                [
                    review.claim_id,
                    review.status,
                    review.resolved,
                    hashlib.sha256(review.rationale.encode()).hexdigest(),
                    evidence_binding_hash,
                    [text_index(value) for value in review.missing_evidence],
                    (
                        None
                        if review.prevented_conclusion is None
                        else text_index(review.prevented_conclusion)
                    ),
                ]
            )
        cycles.append(
            [
                str(cycle.cycle_id),
                cycle.result_code,
                list(cycle.sizing_actions),
                cycle.stance,
                cycle.exposure_action,
                cycle.quality_status,
                cycle.evidence_audit,
                [text_index(value) for value in cycle.uncertainties],
                reviews,
            ]
        )
    return {
        "schema_version": PROVIDER_PROJECTION_VERSION,
        "source_hash": retrospective.source_hash,
        "instrument_id": str(retrospective.instrument_id),
        "incumbent_portfolio_prompt": retrospective.incumbent_portfolio_prompt,
        "output_contract": retrospective.output_contract,
        "cycle_tuple_fields": [
            "cycle_id",
            "result",
            "sizing_actions",
            "stance",
            "exposure_action",
            "quality_status",
            "evidence_audit",
            "uncertainty_text_indexes",
            "reviews",
        ],
        "review_tuple_fields": [
            "claim_id",
            "status",
            "resolved",
            "rationale_hash",
            "evidence_binding_hash",
            "missing_evidence_text_indexes",
            "prevented_conclusion_text_index",
        ],
        "text_catalog": text_catalog,
        "cycles": cycles,
    }


class CodexAgentChallengerRuntime:
    """Invoke the existing isolated Codex SDK exactly once with no tools."""

    def __init__(self, model: AgentModelConfigRecord) -> None:
        if (
            model.model_id != MODEL_ID
            or model.max_input_tokens < CHALLENGER_MAX_REPORTED_INPUT_TOKENS
            or model.max_output_tokens < 8_192
            or model.input_usd_per_million != 0
            or model.output_usd_per_million != 0
        ):
            raise ValueError("AGENT_CHALLENGER_MODEL_UNSUPPORTED")
        self._model_id = model.id
        self._model_hash = model.config_hash
        self._max_input_tokens = model.max_input_tokens
        self._attempted = False

    async def generate(
        self, retrospective: AgentChallengerRetrospectiveV4
    ) -> tuple[AgentChallengerSuggestionV4, UsageSnapshot]:
        if self._attempted:
            raise ValueError("AGENT_CHALLENGER_REPLAY_FORBIDDEN")
        if (
            retrospective.incumbent_model_config_id != self._model_id
            or retrospective.incumbent_model_hash != self._model_hash
        ):
            raise ValueError("AGENT_CHALLENGER_MODEL_AUTHORITY_CHANGED")
        self._attempted = True
        payload = {
            "schema_version": "agent_challenger_codex_input_v4",
            "instructions": PROPOSER_PROMPT,
            "proposer_prompt_version": PROPOSER_PROMPT_VERSION,
            "proposer_prompt_hash": PROPOSER_PROMPT_HASH,
            "retrospective": compact_agent_challenger_retrospective(retrospective),
        }
        wire = codex_request_wire(payload)
        allowance = codex_input_allowance(
            wire, codex_output_schema(AgentChallengerSuggestionWireV4)
        )
        if allowance > min(
            self._max_input_tokens,
            CHALLENGER_MAX_EXPLICIT_INPUT_UNITS + INPUT_RESERVE,
        ):
            raise ValueError("AGENT_CHALLENGER_INPUT_TOO_LARGE")
        raw, usage = await invoke_codex(payload, output_model=AgentChallengerSuggestionWireV4)
        wire_suggestion = AgentChallengerSuggestionWireV4.model_validate_json(raw)
        suggestion = AgentChallengerSuggestionV4(
            **wire_suggestion.model_dump(),
            execution_locked=True,
            fixed_aapl_scope=True,
            deterministic_risk_unchanged=True,
            broker_writes_forbidden=True,
            output_schema_unchanged=True,
        )
        if suggestion.source_hash != retrospective.source_hash:
            raise ValueError("AGENT_CHALLENGER_SOURCE_MISMATCH")
        if (
            usage.input_tokens > CHALLENGER_MAX_REPORTED_INPUT_TOKENS
            or usage.provider_calls != 1
            or usage.tool_calls != 0
        ):
            if usage.input_tokens > CHALLENGER_MAX_REPORTED_INPUT_TOKENS:
                raise ValueError("AGENT_CHALLENGER_INPUT_TOKENS_EXCEEDED")
            raise ValueError("AGENT_CHALLENGER_USAGE_INVALID")
        return suggestion, usage
