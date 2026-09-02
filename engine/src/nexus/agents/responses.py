"""Bounded official-Responses-shaped adapter with an injected transport seam."""

import asyncio
import hashlib
import json
from collections.abc import Mapping
from decimal import Decimal
from pathlib import Path
from typing import Literal, Protocol, cast

from openai import pydantic_function_tool
from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

from nexus.agents.committee import AgentRole, CommitteeRoleOutput, PromptAsset
from nexus.agents.models import ResearchRequest
from nexus.agents.responses_budget import RunUsageAllowance, UsageSnapshot
from nexus.agents.responses_transport import ResponseShapeError, normalize_response
from nexus.agents.tools import ReadOnlyToolRegistry, response_function_tools
from nexus.domain.evidence import EvidenceRef
from nexus.strategy.intents import ResearchClaim


class ResponsesClient(Protocol):
    @property
    def responses(self) -> object: ...


class ResponsesModelConfig(BaseModel):
    """Frozen model, token, deadline, and immutable pricing authority."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    model_id: str
    pricing_version: str
    max_input_tokens: int
    max_output_tokens: int
    max_tool_calls: int
    absolute_timeout_seconds: Decimal
    input_usd_per_million: Decimal
    output_usd_per_million: Decimal

    @field_validator("model_id", "pricing_version")
    @classmethod
    def nonempty_text(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("model and pricing identifiers are required")
        return value

    @field_validator("max_input_tokens", "max_output_tokens")
    @classmethod
    def positive_token_limit(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("token limits must be positive")
        return value

    @field_validator("max_tool_calls")
    @classmethod
    def bounded_tool_limit(cls, value: int) -> int:
        if value < 0 or value > 8:
            raise ValueError("tool call limit must be between zero and eight")
        return value

    @field_validator("absolute_timeout_seconds", "input_usd_per_million", "output_usd_per_million")
    @classmethod
    def finite_decimal(cls, value: Decimal) -> Decimal:
        if not value.is_finite() or value < 0:
            raise ValueError("pricing and timeout values must be finite and non-negative")
        return value

    @property
    def worst_case_cost_usd(self) -> Decimal:
        return (
            Decimal(self.max_input_tokens) * self.input_usd_per_million
            + Decimal(self.max_output_tokens) * self.output_usd_per_million
        ) / Decimal(1_000_000)


class ResponsesResearchModel:
    """Run strict custom functions and parse strict schema output within one deadline."""

    def __init__(self, *, client: ResponsesClient, config: ResponsesModelConfig) -> None:
        if config.absolute_timeout_seconds <= 0:
            raise ValueError("absolute timeout must be positive")
        self._client = client
        self._config = config

    @property
    def worst_case_cost_usd(self) -> Decimal:
        return self._config.worst_case_cost_usd

    async def respond(
        self, request: ResearchRequest, tools: ReadOnlyToolRegistry
    ) -> Mapping[str, object]:
        definitions = response_function_tools()
        if tuple(tools.names) != tuple(definitions):
            raise ValueError("fixed tool registry required")
        responses = getattr(self._client, "responses")
        create = getattr(responses, "create")
        input_items: list[Mapping[str, object]] = [
            {"role": "user", "content": _wire_json(request.model_dump(mode="json"))}
        ]
        allowance_ledger = RunUsageAllowance(
            max_input_tokens=self._config.max_input_tokens,
            max_output_tokens=self._config.max_output_tokens,
            max_tool_calls=self._config.max_tool_calls,
        )
        loop = asyncio.get_running_loop()
        deadline = loop.time() + float(self._config.absolute_timeout_seconds)
        async with asyncio.timeout_at(deadline):
            while True:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise TimeoutError("Responses absolute deadline exceeded")
                request_payload: dict[str, object] = {
                    "model": self._config.model_id,
                    "input": input_items,
                    "tools": list(definitions.values()),
                    "tool_choice": "auto",
                    "store": False,
                    "include": ["reasoning.encrypted_content"],
                    "text": {
                        "format": {
                            "type": "json_schema",
                            "name": "research_intent_v1",
                            "strict": True,
                            "schema": _intent_schema(),
                        }
                    },
                }
                call_allowance = allowance_ledger.authorize(request_payload)
                create_kwargs = dict(
                    model=self._config.model_id,
                    timeout=remaining,
                    max_output_tokens=call_allowance.max_output_tokens,
                    input=input_items,
                    tools=list(definitions.values()),
                    tool_choice="auto",
                    text=request_payload["text"],
                    store=False,
                    include=["reasoning.encrypted_content"],
                )
                result = await create(**create_kwargs)
                try:
                    normalized = normalize_response(result)
                except ResponseShapeError as error:
                    if error.usage is not None:
                        allowance_ledger.record_usage(call_allowance, error.usage)
                    raise ValueError(str(error)) from error
                allowance_ledger.record_usage(call_allowance, normalized.usage)
                calls = tuple(
                    item for item in normalized.output if item.get("type") == "function_call"
                )
                if calls:
                    allowance_ledger.record_tool_calls(len(calls))
                    continuation: list[Mapping[str, object]] = []
                    for call in calls:
                        name = call.get("name")
                        arguments = call.get("arguments")
                        call_id = call.get("call_id")
                        if not isinstance(name, str) or not isinstance(arguments, str):
                            raise ValueError("Responses function call is malformed")
                        if not isinstance(call_id, str) or not call_id:
                            raise ValueError("Responses function call id is required")
                        try:
                            raw_payload = json.loads(arguments)
                        except json.JSONDecodeError as error:
                            raise ValueError(
                                "Responses function arguments are invalid JSON"
                            ) from error
                        if not isinstance(raw_payload, Mapping):
                            raise ValueError("Responses function arguments must be an object")
                        tool_result = await tools.execute(name, raw_payload)
                        continuation.append(
                            {
                                "type": "function_call_output",
                                "call_id": call_id,
                                "output": json.dumps(
                                    tool_result, sort_keys=True, separators=(",", ":"), default=str
                                ),
                            }
                        )
                    input_items.extend(normalized.output)
                    input_items.extend(continuation)
                    continue
                raw_text = normalized.output_text
                if not isinstance(raw_text, str):
                    raise ValueError("Responses structured output text is required")
                try:
                    intent = json.loads(raw_text)
                except json.JSONDecodeError as error:
                    raise ValueError("Responses structured output is invalid JSON") from error
                if not isinstance(intent, Mapping):
                    raise ValueError("Responses structured output must be an object")
                usage = allowance_ledger.usage
                cost = (
                    Decimal(usage.input_tokens) * self._config.input_usd_per_million
                    + Decimal(usage.output_tokens) * self._config.output_usd_per_million
                ) / Decimal(1_000_000)
                return {
                    "intent": dict(intent),
                    "cost_usd": format(cost, "f"),
                    "input_tokens": usage.input_tokens,
                    "output_tokens": usage.output_tokens,
                    "pricing_version": self._config.pricing_version,
                    "model_hash": hashlib.sha256(
                        json.dumps(
                            self._config.model_dump(mode="json"),
                            sort_keys=True,
                            separators=(",", ":"),
                        ).encode()
                    ).hexdigest(),
                }


class _CommitteePayload(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    vote: Literal["support", "oppose", "abstain"]
    claims: tuple[ResearchClaim, ...]
    conflicting_facts: bool = False


class ResponsesCommittee:
    """Bounded eight-role adapter using only strict custom read-only functions."""

    def __init__(
        self,
        *,
        client: ResponsesClient,
        config: ResponsesModelConfig,
        tools: ReadOnlyToolRegistry,
    ) -> None:
        if config.absolute_timeout_seconds <= 0:
            raise ValueError("absolute timeout must be positive")
        if tuple(tools.names) != tuple(response_function_tools()):
            raise ValueError("fixed tool registry required")
        self._client = client
        self._config = config
        self._tools = tools
        self._allowance = RunUsageAllowance(
            max_input_tokens=config.max_input_tokens,
            max_output_tokens=config.max_output_tokens,
            max_tool_calls=config.max_tool_calls,
        )

    @property
    def usage(self) -> UsageSnapshot:
        return self._allowance.usage

    async def analyze(
        self,
        role: AgentRole,
        request: ResearchRequest,
        evidence_refs: tuple[EvidenceRef, ...],
        prompt: PromptAsset,
    ) -> CommitteeRoleOutput:
        definitions = response_function_tools()
        responses = getattr(self._client, "responses")
        create = getattr(responses, "create")
        input_items: list[Mapping[str, object]] = [
            {"role": "system", "content": prompt.content},
            {
                "role": "user",
                "content": _wire_json(
                    {
                        "role": role,
                        "request": request.model_dump(mode="json"),
                        "evidence_refs": [item.model_dump(mode="json") for item in evidence_refs],
                    }
                ),
            },
        ]
        usage_before = self.usage
        loop = asyncio.get_running_loop()
        deadline = loop.time() + float(self._config.absolute_timeout_seconds)
        async with asyncio.timeout_at(deadline):
            while True:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise TimeoutError("Responses absolute deadline exceeded")
                request_payload: dict[str, object] = {
                    "model": self._config.model_id,
                    "input": input_items,
                    "tools": list(definitions.values()),
                    "tool_choice": "auto",
                    "store": False,
                    "include": ["reasoning.encrypted_content"],
                    "text": {
                        "format": {
                            "type": "json_schema",
                            "name": "committee_role_output_v1",
                            "strict": True,
                            "schema": pydantic_function_tool(_CommitteePayload)["function"][
                                "parameters"
                            ],
                        }
                    },
                }
                allowance = self._allowance.authorize(request_payload)
                create_kwargs = dict(
                    model=self._config.model_id,
                    timeout=remaining,
                    max_output_tokens=allowance.max_output_tokens,
                    input=input_items,
                    tools=list(definitions.values()),
                    tool_choice="auto",
                    text=request_payload["text"],
                    store=False,
                    include=["reasoning.encrypted_content"],
                )
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
                        name = call.get("name")
                        arguments = call.get("arguments")
                        call_id = call.get("call_id")
                        if not isinstance(name, str) or not isinstance(arguments, str):
                            raise ValueError("Responses function call is malformed")
                        if not isinstance(call_id, str) or not call_id:
                            raise ValueError("Responses function call id is required")
                        try:
                            payload = json.loads(arguments)
                        except json.JSONDecodeError as error:
                            raise ValueError(
                                "Responses function arguments are invalid JSON"
                            ) from error
                        if not isinstance(payload, Mapping):
                            raise ValueError("Responses function arguments must be an object")
                        tool_result = await self._tools.execute(name, payload)
                        continuation.append(
                            {
                                "type": "function_call_output",
                                "call_id": call_id,
                                "output": json.dumps(
                                    tool_result, sort_keys=True, separators=(",", ":"), default=str
                                ),
                            }
                        )
                    input_items.extend(normalized.output)
                    input_items.extend(continuation)
                    continue
                raw_text = normalized.output_text
                if not isinstance(raw_text, str):
                    raise ValueError("Responses structured output text is required")
                try:
                    payload = _CommitteePayload.model_validate_json(raw_text)
                except ValidationError as error:
                    raise ValueError("Responses structured output is invalid") from error
                current_usage = self.usage
                return CommitteeRoleOutput(
                    role=role,
                    vote=payload.vote,
                    claims=payload.claims,
                    prompt_hash=prompt.content_hash,
                    input_tokens=current_usage.input_tokens - usage_before.input_tokens,
                    output_tokens=current_usage.output_tokens - usage_before.output_tokens,
                    conflicting_facts=payload.conflicting_facts,
                )


def _intent_schema() -> Mapping[str, object]:
    return cast(
        Mapping[str, object],
        json.loads(
            (
                Path(__file__).resolve().parents[4]
                / "contracts/agent/research-intent-v1.schema.json"
            ).read_text(encoding="utf-8")
        ),
    )


def _wire_json(value: Mapping[str, object]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
