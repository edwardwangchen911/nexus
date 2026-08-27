"""Fixed Strategy R&D Agent request and strict Responses runtime."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from nexus.agents.responses import ResponsesClient, ResponsesModelConfig
from nexus.agents.responses_budget import RunUsageAllowance, UsageSnapshot
from nexus.agents.responses_transport import ResponseShapeError, normalize_response
from nexus.domain.time import require_utc
from nexus.evolution.agent_tools import StrategyRNDTools, strategy_rnd_function_tools
from nexus.evolution.models import StrategyExperimentProposalV1


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class StrategyRNDRequestV1(_FrozenModel):
    """All server-frozen authority identities available to one Agent invocation."""

    schema_version: str = Field(pattern=r"^strategy_rnd_request_v1$")
    parent_strategy_definition_id: UUID
    champion_generation: int = Field(ge=1)
    research_lineage_id: UUID
    evolution_policy_id: UUID
    training_validation_summary_id: UUID
    data_snapshot_id: UUID
    evidence_refs: tuple[UUID, ...] = Field(min_length=1, max_length=32)
    as_of: datetime

    @field_validator("as_of")
    @classmethod
    def _as_of_is_utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @model_validator(mode="after")
    def _evidence_is_unique(self) -> StrategyRNDRequestV1:
        if len(self.evidence_refs) != len(set(self.evidence_refs)):
            raise ValueError("frozen evidence references must be unique")
        return self


class StrategyRNDPromptAsset(_FrozenModel):
    role: str = Field(pattern=r"^strategy_rnd$")
    version: str = Field(pattern=r"^strategy_rnd_v1$")
    content: str = Field(min_length=1)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class StrategyRNDRuntime(Protocol):
    @property
    def configuration(self) -> ResponsesModelConfig: ...

    @property
    def usage(self) -> UsageSnapshot: ...

    async def generate(
        self, request: StrategyRNDRequestV1, tools: StrategyRNDTools
    ) -> StrategyExperimentProposalV1: ...


def load_strategy_rnd_prompt() -> StrategyRNDPromptAsset:
    """Load the one released Strategy R&D prompt and derive its immutable identity."""
    path = Path(__file__).parents[1] / "agents" / "prompts" / "strategy_rnd-v1.md"
    content = path.read_text(encoding="utf-8").strip()
    return StrategyRNDPromptAsset(
        role="strategy_rnd",
        version="strategy_rnd_v1",
        content=content,
        content_hash=hashlib.sha256(content.encode()).hexdigest(),
    )


class ResponsesStrategyRNDRuntime:
    """Generate one strict proposal using only four custom read-only functions."""

    def __init__(self, *, client: ResponsesClient, config: ResponsesModelConfig) -> None:
        if config.absolute_timeout_seconds <= 0:
            raise ValueError("Strategy R&D absolute timeout must be positive")
        if config.max_tool_calls > 4:
            raise ValueError("Strategy R&D tool-call authority is capped at four")
        self._client = client
        self._config = config
        self._prompt = load_strategy_rnd_prompt()
        self._allowance = RunUsageAllowance(
            max_input_tokens=config.max_input_tokens,
            max_output_tokens=config.max_output_tokens,
            max_tool_calls=config.max_tool_calls,
        )

    @property
    def usage(self) -> UsageSnapshot:
        return self._allowance.usage

    @property
    def configuration(self) -> ResponsesModelConfig:
        return self._config

    @property
    def worst_case_cost_usd(self) -> Decimal:
        return self._config.worst_case_cost_usd

    async def generate(
        self, request: StrategyRNDRequestV1, tools: StrategyRNDTools
    ) -> StrategyExperimentProposalV1:
        self._require_frozen_tool_context(request, tools)
        definitions = strategy_rnd_function_tools()
        if tuple(tools.names) != tuple(definitions):
            raise ValueError("fixed Strategy R&D tool registry required")
        responses = getattr(self._client, "responses")
        create = getattr(responses, "create")
        input_items: list[Mapping[str, object]] = [
            {"role": "system", "content": self._prompt.content},
            {
                "role": "user",
                "content": json.dumps(
                    request.model_dump(mode="json"),
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            },
        ]
        previous_response_id: str | None = None
        loop = asyncio.get_running_loop()
        deadline = loop.time() + float(self._config.absolute_timeout_seconds)
        async with asyncio.timeout_at(deadline):
            while True:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise TimeoutError("Strategy R&D Responses deadline exceeded")
                text_format = {
                    "format": {
                        "type": "json_schema",
                        "name": "strategy_experiment_proposal_v1",
                        "strict": True,
                        "schema": StrategyExperimentProposalV1.model_json_schema(),
                    }
                }
                request_payload: dict[str, object] = {
                    "model": self._config.model_id,
                    "input": input_items,
                    "tools": list(definitions.values()),
                    "tool_choice": "auto",
                    "text": text_format,
                }
                if previous_response_id is not None:
                    request_payload["previous_response_id"] = previous_response_id
                allowance = self._allowance.authorize(request_payload)
                create_kwargs: dict[str, object] = {
                    "model": self._config.model_id,
                    "timeout": remaining,
                    "max_output_tokens": allowance.max_output_tokens,
                    "input": input_items,
                    "tools": list(definitions.values()),
                    "tool_choice": "auto",
                    "text": text_format,
                }
                if previous_response_id is not None:
                    create_kwargs["previous_response_id"] = previous_response_id
                result = await create(**create_kwargs)
                try:
                    normalized = normalize_response(result)
                except ResponseShapeError as error:
                    if error.usage is not None:
                        self._allowance.record_usage(allowance, error.usage)
                    raise ValueError(str(error)) from error
                self._allowance.record_usage(allowance, normalized.usage)
                calls = tuple(
                    item for item in normalized.output if item.get("type") == "function_call"
                )
                if calls:
                    self._allowance.record_tool_calls(len(calls))
                    continuation: list[Mapping[str, object]] = []
                    for call in calls:
                        continuation.append(await self._execute_tool_call(call, tools))
                    if normalized.response_id is None:
                        input_items.extend(normalized.output)
                        input_items.extend(continuation)
                    else:
                        previous_response_id = normalized.response_id
                        input_items = continuation
                    continue
                if not isinstance(normalized.output_text, str):
                    raise ValueError("Strategy R&D structured output text is required")
                try:
                    proposal = StrategyExperimentProposalV1.model_validate_json(
                        normalized.output_text
                    )
                except ValidationError as error:
                    raise ValueError("Strategy R&D structured output is invalid") from error
                self._require_proposal_identity(request, proposal)
                return proposal

    async def _execute_tool_call(
        self, call: Mapping[str, object], tools: StrategyRNDTools
    ) -> Mapping[str, object]:
        name, arguments, call_id = (
            call.get("name"),
            call.get("arguments"),
            call.get("call_id"),
        )
        if not isinstance(name, str) or not isinstance(arguments, str):
            raise ValueError("Strategy R&D function call is malformed")
        if not isinstance(call_id, str) or not call_id:
            raise ValueError("Strategy R&D function call id is required")
        try:
            payload = json.loads(arguments)
        except json.JSONDecodeError as error:
            raise ValueError("Strategy R&D function arguments are invalid JSON") from error
        if not isinstance(payload, Mapping):
            raise ValueError("Strategy R&D function arguments must be an object")
        result = await tools.execute(name, payload)
        return {
            "type": "function_call_output",
            "call_id": call_id,
            "output": json.dumps(result, sort_keys=True, separators=(",", ":")),
        }

    @staticmethod
    def _require_frozen_tool_context(
        request: StrategyRNDRequestV1, tools: StrategyRNDTools
    ) -> None:
        context = tools.context
        if (
            context.parent_strategy_definition_id
            != request.parent_strategy_definition_id
            or context.champion_generation != request.champion_generation
            or context.research_lineage_id != request.research_lineage_id
            or context.evolution_policy_id != request.evolution_policy_id
            or context.training_validation_summary_id
            != request.training_validation_summary_id
            or context.as_of != request.as_of
        ):
            raise ValueError("Strategy R&D tool context identity does not match the request")

    @staticmethod
    def _require_proposal_identity(
        request: StrategyRNDRequestV1, proposal: StrategyExperimentProposalV1
    ) -> None:
        if (
            proposal.parent_strategy_definition_id
            != request.parent_strategy_definition_id
            or proposal.champion_generation != request.champion_generation
            or proposal.research_lineage_id != request.research_lineage_id
            or not set(proposal.evidence_refs) <= set(request.evidence_refs)
        ):
            raise ValueError("Strategy R&D proposal identity does not match frozen input")
