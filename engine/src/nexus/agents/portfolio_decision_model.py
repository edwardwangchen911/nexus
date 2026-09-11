"""Exactly-two-turn Responses adapter for bounded portfolio interpretation."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, replace
from decimal import Decimal

from openai import pydantic_function_tool
from pydantic import BaseModel, ValidationError

from nexus.agents.codex_versions import (
    PORTFOLIO_DECISION_MAX_INPUT_TOKENS,
    PORTFOLIO_DECISION_MAX_INPUT_TOKENS_PER_TURN,
    PORTFOLIO_DECISION_MAX_OUTPUT_TOKENS,
    PORTFOLIO_DECISION_MAX_OUTPUT_TOKENS_PER_TURN,
    PORTFOLIO_DECISION_MAX_TOOL_CALLS,
    PORTFOLIO_DECISION_MAX_WIRE_BYTES,
)
from nexus.agents.portfolio_decision_contracts import (
    AgentScenarioProjectionV1,
    EvaluateAdjustmentScenarioToolInputV1,
    PortfolioDecisionInput,
    PortfolioDecisionModelContextV1,
    PortfolioResearchIntentV1,
    canonical_portfolio_decision_hash,
    validate_portfolio_intent,
)
from nexus.agents.portfolio_decision_tools import TOOL_NAME, PortfolioDecisionToolRegistry
from nexus.agents.responses import ResponsesClient, ResponsesModelConfig
from nexus.agents.responses_budget import RunUsageAllowance, UsageSnapshot
from nexus.agents.responses_transport import (
    NormalizedResponse,
    NormalizedUsage,
    ResponseShapeError,
    normalize_response,
)


class PortfolioDecisionModelError(ValueError):
    """Stable fail-closed model protocol error."""

    def __init__(
        self, code: str, *, usage: UsageSnapshot | None = None, usage_known: bool = True
    ) -> None:
        super().__init__(code)
        self.code = code
        self.usage = usage or UsageSnapshot()
        self.usage_known = usage_known


@dataclass(frozen=True)
class PortfolioDecisionFirstTurn:
    projection: AgentScenarioProjectionV1
    continuation: tuple[Mapping[str, object], ...]
    tool_output: Mapping[str, object]
    context_hash: str
    prompt_hash: str


@dataclass(frozen=True)
class PortfolioDecisionModelResult:
    intent: PortfolioResearchIntentV1
    usage: UsageSnapshot


CodexInvoke = Callable[..., Awaitable[tuple[str, UsageSnapshot]]]


class BoundPortfolioResponsesClient:
    """Bind a provider transport to one complete frozen model authority."""

    max_retries = 0

    def __init__(self, *, client: object, config: ResponsesModelConfig) -> None:
        if getattr(client, "max_retries", None) != 0:
            raise ValueError("portfolio decision transport retries must be disabled")
        responses = getattr(client, "responses", None)
        if responses is None:
            raise ValueError("portfolio decision Responses transport is required")
        self.responses = responses
        self._config = config

    def supports_model_config(self, config: ResponsesModelConfig) -> bool:
        return config == self._config


class CodexPortfolioResponsesClient:
    """Map two strict Codex SDK outputs onto the existing no-retry model protocol."""

    max_retries = 0

    def __init__(
        self,
        *,
        model_id: str,
        pricing_version: str,
        invoke: CodexInvoke | None = None,
    ) -> None:
        if not model_id or not pricing_version:
            raise ValueError("portfolio decision Codex model is required")
        if invoke is None:
            from nexus.agents.codex_sdk import invoke_codex_portfolio

            invoke = invoke_codex_portfolio
        self._model_id = model_id
        self._pricing_version = pricing_version
        self._invoke = invoke
        self._stage = 0
        self.responses = self

    def supports_model_config(self, config: ResponsesModelConfig) -> bool:
        return (
            config.model_id == self._model_id
            and config.pricing_version == self._pricing_version
            and config.max_input_tokens == PORTFOLIO_DECISION_MAX_INPUT_TOKENS
            and config.max_output_tokens == PORTFOLIO_DECISION_MAX_OUTPUT_TOKENS
            and config.max_tool_calls == PORTFOLIO_DECISION_MAX_TOOL_CALLS
            and config.input_usd_per_million == 0
            and config.output_usd_per_million == 0
        )

    async def create(self, **kwargs: object) -> Mapping[str, object]:
        if kwargs.get("model") != self._model_id or kwargs.get("store") is not False:
            raise ValueError("portfolio decision Codex authority is invalid")
        model, stage = self._output_model(kwargs)
        raw_input = kwargs.get("input")
        if not isinstance(raw_input, list):
            raise ValueError("portfolio decision Codex input is invalid")
        raw, usage = await self._invoke(
            {
                "schema_version": "codex_portfolio_decision_transport_v1",
                "stage": stage,
                "instructions": (
                    "Return only the objective allowed by the requested schema; the host will "
                    "execute the deterministic scenario tool exactly once."
                    if stage == "scenario_request"
                    else "Return only the final read-only PortfolioResearchIntentV1 JSON using "
                    "the supplied deterministic tool projection."
                ),
                "input": raw_input,
            },
            output_model=model,
            max_wire_bytes=PORTFOLIO_DECISION_MAX_WIRE_BYTES,
        )
        if usage.input_tokens > PORTFOLIO_DECISION_MAX_INPUT_TOKENS_PER_TURN:
            raise ResponseShapeError(
                "portfolio decision Codex turn input exceeds limit",
                usage=NormalizedUsage(
                    input_tokens=usage.input_tokens,
                    output_tokens=usage.output_tokens,
                ),
            )
        if usage.output_tokens > PORTFOLIO_DECISION_MAX_OUTPUT_TOKENS_PER_TURN:
            raise ResponseShapeError(
                "portfolio decision Codex turn output exceeds limit",
                usage=NormalizedUsage(
                    input_tokens=usage.input_tokens,
                    output_tokens=usage.output_tokens,
                ),
            )
        try:
            parsed = model.model_validate_json(raw)
        except ValidationError as error:
            raise ResponseShapeError(
                "portfolio decision Codex structured output is invalid",
                usage=NormalizedUsage(
                    input_tokens=usage.input_tokens,
                    output_tokens=usage.output_tokens,
                ),
            ) from error
        canonical = parsed.model_dump_json()
        response_usage = {
            "input_tokens": usage.input_tokens,
            "output_tokens": usage.output_tokens,
        }
        self._stage += 1
        if stage == "scenario_request":
            return {
                "output": [
                    {
                        "type": "function_call",
                        "call_id": "codex-portfolio-scenario-1",
                        "name": TOOL_NAME,
                        "arguments": canonical,
                    }
                ],
                "usage": response_usage,
            }
        return {
            "output": [{"type": "message"}],
            "output_text": canonical,
            "usage": response_usage,
        }

    def _output_model(
        self, kwargs: Mapping[str, object]
    ) -> tuple[type[BaseModel], str]:
        tool_choice = kwargs.get("tool_choice")
        tools = kwargs.get("tools")
        if self._stage == 0:
            definition = tools[0] if isinstance(tools, list) and len(tools) == 1 else None
            function = definition.get("function") if isinstance(definition, Mapping) else None
            if (
                tool_choice != "required"
                or not isinstance(function, Mapping)
                or function.get("name") != TOOL_NAME
            ):
                raise ValueError("portfolio decision Codex tool request is invalid")
            return EvaluateAdjustmentScenarioToolInputV1, "scenario_request"
        if self._stage == 1:
            if tool_choice != "none" or tools != []:
                raise ValueError("portfolio decision Codex intent request is invalid")
            return PortfolioResearchIntentV1, "research_intent"
        raise ValueError("portfolio decision Codex turn limit exceeded")


class ResponsesPortfolioDecisionModel:
    """Use one forced tool turn and one tool-free structured-output turn."""

    def __init__(self, *, client: ResponsesClient, config: ResponsesModelConfig) -> None:
        if getattr(client, "max_retries", None) != 0:
            raise ValueError("portfolio decision transport retries must be disabled")
        if config.max_tool_calls < 1:
            raise ValueError("portfolio decision model requires a tool allowance")
        if config.absolute_timeout_seconds <= 0:
            raise ValueError("absolute timeout must be positive")
        self._client = client
        self._config = config
        self._allowance = RunUsageAllowance(
            max_input_tokens=config.max_input_tokens,
            max_output_tokens=config.max_output_tokens,
            max_tool_calls=1,
        )
        self._deadline: float | None = None
        self._second_started = False
        self._finished = False

    @property
    def usage(self) -> UsageSnapshot:
        return self._allowance.usage

    async def request_scenario(
        self,
        *,
        context: PortfolioDecisionModelContextV1,
        prompt: str,
        tools: PortfolioDecisionToolRegistry,
    ) -> PortfolioDecisionFirstTurn:
        if self._deadline is not None or self._finished:
            raise self._error("PORTFOLIO_DECISION_MODEL_STATE_INVALID")
        if tuple(tools.names) != (TOOL_NAME,):
            raise ValueError("fixed portfolio decision tool registry required")
        loop = asyncio.get_running_loop()
        self._deadline = loop.time() + float(self._config.absolute_timeout_seconds)
        input_items = _initial_input(context, prompt)
        definition = pydantic_function_tool(
            EvaluateAdjustmentScenarioToolInputV1,
            name=TOOL_NAME,
        )
        payload: dict[str, object] = {
            "model": self._config.model_id,
            "input": input_items,
            "tools": [definition],
            "tool_choice": "required",
            "parallel_tool_calls": False,
            "store": False,
            "include": ["reasoning.encrypted_content"],
        }
        normalized = await self._create(payload)
        calls = tuple(item for item in normalized.output if item.get("type") == "function_call")
        if not calls:
            raise self._error("PORTFOLIO_DECISION_TOOL_REQUIRED")
        if len(calls) != 1:
            raise self._error("PORTFOLIO_DECISION_TOOL_LIMIT_EXCEEDED")
        if normalized.output_text is not None or any(
            item.get("type") not in {"reasoning", "function_call"} for item in normalized.output
        ):
            raise self._error("PORTFOLIO_DECISION_TOOL_REQUIRED")
        self._allowance.record_tool_calls(1)
        call = calls[0]
        name, arguments, call_id = call.get("name"), call.get("arguments"), call.get("call_id")
        if (
            not isinstance(name, str)
            or name != TOOL_NAME
            or not isinstance(arguments, str)
            or not isinstance(call_id, str)
        ):
            raise self._error("PORTFOLIO_DECISION_OUTPUT_INVALID")
        try:
            raw_tool_input = json.loads(arguments)
            tool_input = EvaluateAdjustmentScenarioToolInputV1.model_validate(raw_tool_input)
        except (json.JSONDecodeError, TypeError, ValidationError) as error:
            raise self._error("PORTFOLIO_DECISION_OUTPUT_INVALID") from error
        try:
            projection = AgentScenarioProjectionV1.model_validate(
                await tools.execute(name, tool_input.model_dump(mode="python"))
            )
        except (TypeError, ValueError, ValidationError) as error:
            raise self._error("PORTFOLIO_DECISION_TOOL_RESULT_MISMATCH") from error
        tool_output = {
            "type": "function_call_output",
            "call_id": call_id,
            "output": json.dumps(
                projection.model_dump(mode="json"),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
        }
        return PortfolioDecisionFirstTurn(
            projection=projection,
            continuation=normalized.output,
            tool_output=tool_output,
            context_hash=canonical_portfolio_decision_hash(context),
            prompt_hash=hashlib.sha256(prompt.encode()).hexdigest(),
        )

    async def request_intent(
        self,
        *,
        context: PortfolioDecisionModelContextV1,
        prompt: str,
        frozen: PortfolioDecisionInput,
        first_turn: PortfolioDecisionFirstTurn,
    ) -> PortfolioDecisionModelResult:
        if self._deadline is None or self._second_started or self._finished:
            raise self._error("PORTFOLIO_DECISION_MODEL_STATE_INVALID")
        if (
            first_turn.context_hash != canonical_portfolio_decision_hash(context)
            or first_turn.prompt_hash != hashlib.sha256(prompt.encode()).hexdigest()
        ):
            raise self._error("PORTFOLIO_DECISION_MODEL_STATE_INVALID")
        try:
            _validate_context(context, frozen)
        except PortfolioDecisionModelError as error:
            raise self._error(error.code) from error
        try:
            continuation, tool_output = _validate_first_turn(first_turn)
        except (TypeError, ValueError, ValidationError) as error:
            raise self._error("PORTFOLIO_DECISION_MODEL_STATE_INVALID") from error
        input_items = [
            *_initial_input(context, prompt),
            *continuation,
            tool_output,
        ]
        payload: dict[str, object] = {
            "model": self._config.model_id,
            "input": input_items,
            "tools": [],
            "tool_choice": "none",
            "store": False,
            "include": ["reasoning.encrypted_content"],
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": "portfolio_research_intent_v1",
                    "strict": True,
                    "schema": PortfolioResearchIntentV1.model_json_schema(mode="serialization"),
                }
            },
        }
        self._second_started = True
        normalized = await self._create(payload)
        if any(item.get("type") == "function_call" for item in normalized.output):
            raise self._error("PORTFOLIO_DECISION_TOOL_LIMIT_EXCEEDED")
        if not isinstance(normalized.output_text, str):
            raise self._error("PORTFOLIO_DECISION_OUTPUT_INVALID")
        try:
            intent = PortfolioResearchIntentV1.model_validate_json(normalized.output_text)
            if intent.run_id != context.run_id:
                raise ValueError("intent run does not match context")
            validate_portfolio_intent(
                intent,
                frozen=frozen,
                projection=first_turn.projection,
                allowed_citation_ids=frozenset(context.citation_ids),
            )
        except (TypeError, ValueError, ValidationError) as error:
            raise self._error("PORTFOLIO_DECISION_OUTPUT_INVALID") from error
        self._finished = True
        return PortfolioDecisionModelResult(intent=intent, usage=self.usage)

    async def _create(self, payload: Mapping[str, object]) -> NormalizedResponse:
        assert self._deadline is not None
        loop = asyncio.get_running_loop()
        remaining = self._deadline - loop.time()
        if remaining <= 0:
            raise self._error("PORTFOLIO_DECISION_MODEL_FAILED")
        try:
            create = getattr(getattr(self._client, "responses"), "create")
            if not callable(create):
                raise TypeError("Responses create transport is not callable")
        except (AttributeError, TypeError) as error:
            raise self._error("PORTFOLIO_DECISION_MODEL_FAILED") from error
        try:
            allowance = self._allowance.authorize(payload)
        except ValueError as error:
            raise self._error("PORTFOLIO_DECISION_MODEL_FAILED") from error
        allowance = replace(
            allowance,
            # Provider usage includes transport instructions and output schemas that
            # are absent from the business payload byte estimate. The aggregate
            # model limit remains the authoritative post-call ceiling.
            input_token_ceiling=self._config.max_input_tokens - self.usage.input_tokens,
            max_output_tokens=min(
                allowance.max_output_tokens,
                PORTFOLIO_DECISION_MAX_OUTPUT_TOKENS_PER_TURN,
            ),
        )
        kwargs = dict(payload)
        kwargs["timeout"] = remaining
        kwargs["max_output_tokens"] = allowance.max_output_tokens
        try:
            async with asyncio.timeout_at(self._deadline):
                result = await create(**kwargs)
            normalized = normalize_response(result)
        except ResponseShapeError as error:
            if error.usage is not None:
                try:
                    self._allowance.record_usage(allowance, error.usage)
                except ValueError:
                    pass
            raise self._error(
                "PORTFOLIO_DECISION_MODEL_FAILED",
                usage_known=error.usage is not None,
            ) from error
        except Exception as error:
            raise self._error("PORTFOLIO_DECISION_MODEL_FAILED", usage_known=False) from error
        try:
            self._allowance.record_usage(allowance, normalized.usage)
        except ValueError as error:
            raise self._error("PORTFOLIO_DECISION_MODEL_FAILED") from error
        return normalized

    def _error(self, code: str, *, usage_known: bool = True) -> PortfolioDecisionModelError:
        return PortfolioDecisionModelError(
            code,
            usage=self.usage,
            usage_known=usage_known,
        )


def _initial_input(
    context: PortfolioDecisionModelContextV1, prompt: str
) -> list[Mapping[str, object]]:
    return [
        {"role": "system", "content": prompt},
        {
            "role": "user",
            "content": json.dumps(
                context.model_dump(mode="json"),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
        },
    ]


def _validate_context(
    context: PortfolioDecisionModelContextV1, frozen: PortfolioDecisionInput
) -> None:
    if (
        context.portfolio_id != frozen.portfolio_id
        or context.portfolio_snapshot_id != frozen.portfolio_snapshot_id
        or context.diagnostic_report_id != frozen.diagnostic_report_id
        or context.decision_time != frozen.decision_time
        or context.objective != frozen.objective
        or context.constraints.objective != frozen.objective
        or context.constraints.max_turnover != Decimal(frozen.max_turnover)
        or context.constraints.minimum_cash != Decimal(frozen.minimum_cash)
        or context.constraints.universe_policy != frozen.universe_policy
        or context.constraints.allow_new_positions != frozen.allow_new_positions
    ):
        raise PortfolioDecisionModelError("PORTFOLIO_DECISION_MODEL_STATE_INVALID")


def _validate_first_turn(
    first_turn: PortfolioDecisionFirstTurn,
) -> tuple[tuple[Mapping[str, object], ...], Mapping[str, object]]:
    continuation: list[Mapping[str, object]] = []
    calls: list[Mapping[str, object]] = []
    for raw_item in first_turn.continuation:
        if not isinstance(raw_item, Mapping):
            raise ValueError("first-turn continuation item is not a mapping")
        item = _json_copy(raw_item)
        item_type = item.get("type")
        if item_type == "reasoning":
            if set(item) - {"id", "type", "summary", "encrypted_content", "status"}:
                raise ValueError("reasoning continuation contains unknown fields")
            if "id" in item and not isinstance(item["id"], str):
                raise ValueError("reasoning continuation id is invalid")
            if "summary" in item and not isinstance(item["summary"], list):
                raise ValueError("reasoning continuation summary is invalid")
            if "encrypted_content" in item and not isinstance(item["encrypted_content"], str):
                raise ValueError("reasoning continuation content is invalid")
            if "status" in item and item["status"] != "completed":
                raise ValueError("reasoning continuation status is invalid")
        elif item_type == "function_call":
            if set(item) - {"id", "type", "call_id", "name", "arguments", "status"}:
                raise ValueError("function continuation contains unknown fields")
            call_id, name, arguments = (
                item.get("call_id"),
                item.get("name"),
                item.get("arguments"),
            )
            if (
                not isinstance(call_id, str)
                or not call_id
                or name != TOOL_NAME
                or not isinstance(arguments, str)
            ):
                raise ValueError("function continuation identity is invalid")
            EvaluateAdjustmentScenarioToolInputV1.model_validate_json(arguments)
            if "status" in item and item["status"] != "completed":
                raise ValueError("function continuation status is invalid")
            calls.append(item)
        else:
            raise ValueError("first-turn continuation type is invalid")
        continuation.append(item)
    if len(calls) != 1:
        raise ValueError("first-turn continuation must contain one tool call")

    raw_output = first_turn.tool_output
    if not isinstance(raw_output, Mapping) or set(raw_output) != {
        "type",
        "call_id",
        "output",
    }:
        raise ValueError("first-turn tool output shape is invalid")
    output = _json_copy(raw_output)
    output_text = output.get("output")
    if (
        output.get("type") != "function_call_output"
        or output.get("call_id") != calls[0].get("call_id")
        or not isinstance(output_text, str)
    ):
        raise ValueError("first-turn tool output identity is invalid")
    projection = AgentScenarioProjectionV1.model_validate_json(output_text)
    if projection != first_turn.projection:
        raise ValueError("first-turn tool output changed")
    output["output"] = json.dumps(
        first_turn.projection.model_dump(mode="json"),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return tuple(continuation), output


def _json_copy(value: Mapping[str, object]) -> dict[str, object]:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    decoded = json.loads(encoded)
    if not isinstance(decoded, dict):
        raise ValueError("continuation value is not a JSON object")
    return decoded
