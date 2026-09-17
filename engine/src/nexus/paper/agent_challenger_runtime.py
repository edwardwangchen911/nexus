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
    PROPOSER_PROMPT,
    PROPOSER_PROMPT_HASH,
    PROPOSER_PROMPT_VERSION,
    PROVIDER_PROJECTION_VERSION,
    AgentChallengerRetrospectiveV4,
    AgentChallengerSuggestionV4,
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
