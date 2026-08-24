"""Bounded official-Responses-shaped adapter with an injected transport seam."""

import asyncio
import hashlib
import json
from collections.abc import Mapping
from decimal import Decimal
from pathlib import Path
from typing import Protocol, cast

from pydantic import BaseModel, ConfigDict, field_validator

from nexus.agents.models import ResearchRequest
from nexus.agents.tools import ReadOnlyToolRegistry, response_function_tools


class _ResponsesClient(Protocol):
    responses: object


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

    def __init__(self, *, client: _ResponsesClient, config: ResponsesModelConfig) -> None:
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
            {"role": "user", "content": request.model_dump(mode="json")}
        ]
        input_tokens = 0
        output_tokens = 0
        tool_calls = 0
        loop = asyncio.get_running_loop()
        deadline = loop.time() + float(self._config.absolute_timeout_seconds)
        async with asyncio.timeout_at(deadline):
            while True:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise TimeoutError("Responses absolute deadline exceeded")
                result = await create(
                    model=self._config.model_id,
                    timeout=remaining,
                    max_output_tokens=self._config.max_output_tokens,
                    input=input_items,
                    tools=list(definitions.values()),
                    tool_choice="auto",
                    text={
                        "format": {
                            "type": "json_schema",
                            "name": "research_intent_v1",
                            "strict": True,
                            "schema": _intent_schema(),
                        }
                    },
                )
                if not isinstance(result, Mapping):
                    raise ValueError("Responses result must be a mapping at the transport seam")
                usage = result.get("usage")
                if not isinstance(usage, Mapping):
                    raise ValueError("Responses usage is required")
                input_tokens += _token_count(usage.get("input_tokens"))
                output_tokens += _token_count(usage.get("output_tokens"))
                if input_tokens > self._config.max_input_tokens:
                    raise ValueError("aggregate input token limit exceeded")
                if output_tokens > self._config.max_output_tokens:
                    raise ValueError("aggregate output token limit exceeded")
                calls = _function_calls(result.get("output"))
                if calls:
                    tool_calls += len(calls)
                    if tool_calls > self._config.max_tool_calls:
                        raise ValueError("aggregate tool call limit exceeded")
                    output = result.get("output")
                    assert isinstance(output, list)
                    input_items.extend(cast(list[Mapping[str, object]], output))
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
                        input_items.append(
                            {
                                "type": "function_call_output",
                                "call_id": call_id,
                                "output": json.dumps(
                                    tool_result, sort_keys=True, separators=(",", ":"), default=str
                                ),
                            }
                        )
                    continue
                raw_text = result.get("output_text")
                if not isinstance(raw_text, str):
                    raise ValueError("Responses structured output text is required")
                try:
                    intent = json.loads(raw_text)
                except json.JSONDecodeError as error:
                    raise ValueError("Responses structured output is invalid JSON") from error
                if not isinstance(intent, Mapping):
                    raise ValueError("Responses structured output must be an object")
                cost = (
                    Decimal(input_tokens) * self._config.input_usd_per_million
                    + Decimal(output_tokens) * self._config.output_usd_per_million
                ) / Decimal(1_000_000)
                return {
                    "intent": dict(intent),
                    "cost_usd": format(cost, "f"),
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "pricing_version": self._config.pricing_version,
                    "model_hash": hashlib.sha256(
                        json.dumps(
                            self._config.model_dump(mode="json"),
                            sort_keys=True,
                            separators=(",", ":"),
                        ).encode()
                    ).hexdigest(),
                }


def _function_calls(value: object) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, list):
        raise ValueError("Responses output items are required")
    if not all(isinstance(item, Mapping) for item in value):
        raise ValueError("Responses output item is malformed")
    return tuple(item for item in value if item.get("type") == "function_call")


def _token_count(value: object) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError("Responses token usage must be a non-negative integer")
    return value


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
