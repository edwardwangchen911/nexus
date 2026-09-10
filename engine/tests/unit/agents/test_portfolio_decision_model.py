import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.agents import portfolio_decision_model
from nexus.agents.portfolio_decision_contracts import (
    AgentScenarioProjectionV1,
    EvaluateAdjustmentScenarioToolInputV1,
    PortfolioDecisionInputV1,
    PortfolioDecisionModelContextV1,
    PortfolioResearchIntentV1,
)
from nexus.agents.portfolio_decision_model import (
    PortfolioDecisionModelError,
    ResponsesPortfolioDecisionModel,
)
from nexus.agents.responses import ResponsesModelConfig
from nexus.agents.responses_budget import UsageSnapshot

NOW = datetime(2026, 9, 10, 1, 2, 3, tzinfo=UTC)
RUN_ID = UUID("018f0000-0000-7000-8000-000000000001")
PORTFOLIO_ID = UUID("018f0000-0000-7000-8000-000000000002")
SNAPSHOT_ID = UUID("018f0000-0000-7000-8000-000000000003")
REPORT_ID = UUID("018f0000-0000-7000-8000-000000000004")
CID = "a" * 64
RESULT_HASH = "b" * 64


@pytest.mark.asyncio
async def test_codex_transport_maps_two_structured_turns_to_existing_protocol() -> None:
    transport_type = getattr(portfolio_decision_model, "CodexPortfolioResponsesClient", None)
    assert transport_type is not None
    calls: list[tuple[dict[str, object], type[object], int]] = []

    async def invoke(
        payload: dict[str, object], *, output_model: type[object], max_wire_bytes: int
    ) -> tuple[str, UsageSnapshot]:
        calls.append((payload, output_model, max_wire_bytes))
        body = (
            {"objective": "reduce_security_concentration"}
            if len(calls) == 1
            else _intent()
        )
        return json.dumps(body), UsageSnapshot(input_tokens=50, output_tokens=25)

    client = transport_type(
        invoke=invoke,
        model_id="configured-model",
        pricing_version="pricing-v1",
    )
    model = ResponsesPortfolioDecisionModel(client=client, config=_config())
    first = await model.request_scenario(
        context=_context(), prompt="bounded prompt", tools=_tools()
    )
    result = await model.request_intent(
        context=_context(), prompt="bounded prompt", frozen=_frozen(), first_turn=first
    )

    assert result.intent.scenario_result_hash == RESULT_HASH
    assert [call[0]["stage"] for call in calls] == ["scenario_request", "research_intent"]
    assert "execute the deterministic scenario tool exactly once" in str(
        calls[0][0]["instructions"]
    )
    assert calls[0][1] is EvaluateAdjustmentScenarioToolInputV1
    assert calls[1][1] is PortfolioResearchIntentV1
    assert [call[2] for call in calls] == [64_000, 64_000]
    assert result.usage == UsageSnapshot(
        input_tokens=100,
        output_tokens=50,
        tool_calls=1,
        provider_calls=2,
    )


@pytest.mark.asyncio
async def test_codex_transport_rejects_per_turn_output_over_8192_without_retry() -> None:
    calls = 0

    async def invoke(
        payload: dict[str, object], **kwargs: object
    ) -> tuple[str, UsageSnapshot]:
        nonlocal calls
        calls += 1
        del payload
        del kwargs
        return json.dumps({"objective": "reduce_security_concentration"}), UsageSnapshot(
            input_tokens=50,
            output_tokens=8_193,
        )

    client = portfolio_decision_model.CodexPortfolioResponsesClient(
        invoke=invoke,
        model_id="configured-model",
        pricing_version="pricing-v1",
    )
    config = _config().model_copy(update={"max_output_tokens": 16_384})
    model = ResponsesPortfolioDecisionModel(client=client, config=config)

    with pytest.raises(PortfolioDecisionModelError) as raised:
        await model.request_scenario(
            context=_context(), prompt="bounded prompt", tools=_tools()
        )

    assert raised.value.usage_known is True
    assert raised.value.usage.output_tokens == 8_193
    assert calls == 1


@pytest.mark.asyncio
async def test_codex_transport_rejects_second_turn_output_over_8192_without_retry() -> None:
    calls = 0

    async def invoke(
        payload: dict[str, object], **kwargs: object
    ) -> tuple[str, UsageSnapshot]:
        nonlocal calls
        calls += 1
        del payload
        del kwargs
        body = {"objective": "reduce_security_concentration"} if calls == 1 else _intent()
        output_tokens = 25 if calls == 1 else 8_193
        return json.dumps(body), UsageSnapshot(input_tokens=50, output_tokens=output_tokens)

    client = portfolio_decision_model.CodexPortfolioResponsesClient(
        invoke=invoke,
        model_id="configured-model",
        pricing_version="pricing-v1",
    )
    config = _config().model_copy(update={"max_output_tokens": 16_384})
    model = ResponsesPortfolioDecisionModel(client=client, config=config)
    first = await model.request_scenario(
        context=_context(), prompt="bounded prompt", tools=_tools()
    )

    with pytest.raises(PortfolioDecisionModelError) as raised:
        await model.request_intent(
            context=_context(), prompt="bounded prompt", frozen=_frozen(), first_turn=first
        )

    assert raised.value.usage_known is True
    assert raised.value.usage.output_tokens == 8_218
    assert calls == 2


def test_bound_transport_requires_the_exact_frozen_model_config() -> None:
    transport_type = getattr(portfolio_decision_model, "BoundPortfolioResponsesClient", None)
    assert transport_type is not None
    expected = _config()
    delegate = _Client(_intent())
    client = transport_type(client=delegate, config=expected)

    assert client.responses is delegate.responses
    assert client.supports_model_config(expected) is True
    assert client.supports_model_config(
        expected.model_copy(update={"pricing_version": "another-pricing-version"})
    ) is False


@pytest.mark.asyncio
async def test_codex_transport_preserves_known_usage_for_invalid_structured_output() -> None:
    async def invoke(
        payload: dict[str, object], **kwargs: object
    ) -> tuple[str, UsageSnapshot]:
        del payload
        del kwargs
        return "not-json", UsageSnapshot(input_tokens=50, output_tokens=25)

    client = portfolio_decision_model.CodexPortfolioResponsesClient(
        invoke=invoke,
        model_id="configured-model",
        pricing_version="pricing-v1",
    )
    model = ResponsesPortfolioDecisionModel(client=client, config=_config())

    with pytest.raises(PortfolioDecisionModelError) as raised:
        await model.request_scenario(
            context=_context(), prompt="bounded prompt", tools=_tools()
        )

    assert raised.value.usage_known is True
    assert raised.value.usage.input_tokens == 50
    assert raised.value.usage.output_tokens == 25


@pytest.mark.asyncio
async def test_codex_transport_accepts_reported_sdk_overhead_within_aggregate_limit() -> None:
    async def invoke(
        payload: dict[str, object], **kwargs: object
    ) -> tuple[str, UsageSnapshot]:
        del payload
        del kwargs
        return json.dumps({"objective": "reduce_security_concentration"}), UsageSnapshot(
            input_tokens=8_795,
            output_tokens=44,
        )

    client = portfolio_decision_model.CodexPortfolioResponsesClient(
        invoke=invoke,
        model_id="configured-model",
        pricing_version="pricing-v1",
    )
    config = _config().model_copy(
        update={"max_input_tokens": 128_000, "max_output_tokens": 16_384}
    )
    model = ResponsesPortfolioDecisionModel(client=client, config=config)

    first = await model.request_scenario(
        context=_context(), prompt="bounded prompt", tools=_tools()
    )

    assert first.projection.result_hash == RESULT_HASH
    assert model.usage.input_tokens == 8_795
    assert model.usage.output_tokens == 44


@pytest.mark.asyncio
async def test_codex_transport_rejects_first_turn_input_over_64000_with_known_usage() -> None:
    calls = 0

    async def invoke(
        payload: dict[str, object], **kwargs: object
    ) -> tuple[str, UsageSnapshot]:
        nonlocal calls
        calls += 1
        del payload
        del kwargs
        return json.dumps({"objective": "reduce_security_concentration"}), UsageSnapshot(
            input_tokens=64_001,
            output_tokens=44,
        )

    client = portfolio_decision_model.CodexPortfolioResponsesClient(
        invoke=invoke,
        model_id="configured-model",
        pricing_version="pricing-v1",
    )
    model = ResponsesPortfolioDecisionModel(
        client=client,
        config=_config().model_copy(
            update={"max_input_tokens": 128_000, "max_output_tokens": 16_384}
        ),
    )

    with pytest.raises(PortfolioDecisionModelError) as raised:
        await model.request_scenario(
            context=_context(), prompt="bounded prompt", tools=_tools()
        )

    assert raised.value.usage_known is True
    assert raised.value.usage.input_tokens == 64_001
    assert calls == 1


@pytest.mark.asyncio
async def test_codex_transport_rejects_second_turn_input_over_64000_with_known_usage() -> None:
    calls = 0

    async def invoke(
        payload: dict[str, object], **kwargs: object
    ) -> tuple[str, UsageSnapshot]:
        nonlocal calls
        calls += 1
        del payload
        del kwargs
        body = {"objective": "reduce_security_concentration"} if calls == 1 else _intent()
        input_tokens = 1_000 if calls == 1 else 64_001
        return json.dumps(body), UsageSnapshot(input_tokens=input_tokens, output_tokens=44)

    client = portfolio_decision_model.CodexPortfolioResponsesClient(
        invoke=invoke,
        model_id="configured-model",
        pricing_version="pricing-v1",
    )
    model = ResponsesPortfolioDecisionModel(
        client=client,
        config=_config().model_copy(
            update={"max_input_tokens": 128_000, "max_output_tokens": 16_384}
        ),
    )
    first = await model.request_scenario(
        context=_context(), prompt="bounded prompt", tools=_tools()
    )

    with pytest.raises(PortfolioDecisionModelError) as raised:
        await model.request_intent(
            context=_context(), prompt="bounded prompt", frozen=_frozen(), first_turn=first
        )

    assert raised.value.usage_known is True
    assert raised.value.usage.input_tokens == 65_001
    assert calls == 2


@pytest.mark.asyncio
async def test_two_turn_model_calls_one_tool_then_returns_bound_intent() -> None:
    client = _Client(_intent())
    model = ResponsesPortfolioDecisionModel(client=client, config=_config())
    first = await model.request_scenario(
        context=_context(), prompt="bounded prompt", tools=_tools()
    )
    result = await model.request_intent(
        context=_context(),
        prompt="bounded prompt",
        frozen=_frozen(),
        first_turn=first,
    )

    assert first.projection.result_hash == RESULT_HASH
    assert result.intent.scenario_result_hash == RESULT_HASH
    assert (result.usage.input_tokens, result.usage.output_tokens, result.usage.tool_calls) == (
        150,
        100,
        1,
    )
    assert len(client.responses.calls) == 2
    assert client.responses.calls[0]["parallel_tool_calls"] is False
    assert client.responses.calls[0]["tool_choice"] == "required"
    assert client.responses.calls[1]["tools"] == []
    assert client.responses.calls[1]["tool_choice"] == "none"
    assert client.responses.calls[1]["input"][-1]["type"] == "function_call_output"


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ("no_tool", "two_tools", "second_tool"))
async def test_two_turn_model_fails_closed_on_tool_protocol_violation(mode: str) -> None:
    client = _Client(_intent(), mode=mode)
    model = ResponsesPortfolioDecisionModel(client=client, config=_config())
    if mode == "no_tool":
        with pytest.raises(PortfolioDecisionModelError, match="TOOL_REQUIRED") as raised:
            await model.request_scenario(
                context=_context(), prompt="bounded prompt", tools=_tools()
            )
        assert raised.value.usage_known is True
        assert (raised.value.usage.input_tokens, raised.value.usage.output_tokens) == (100, 50)
        return
    if mode == "two_tools":
        with pytest.raises(PortfolioDecisionModelError, match="TOOL_LIMIT_EXCEEDED"):
            await model.request_scenario(
                context=_context(), prompt="bounded prompt", tools=_tools()
            )
        return
    first = await model.request_scenario(
        context=_context(), prompt="bounded prompt", tools=_tools()
    )
    with pytest.raises(PortfolioDecisionModelError, match="TOOL_LIMIT_EXCEEDED"):
        await model.request_intent(
            context=_context(), prompt="bounded prompt", frozen=_frozen(), first_turn=first
        )


@pytest.mark.asyncio
async def test_two_turn_model_rejects_unsafe_arguments_and_unbound_intent() -> None:
    bad_arguments = ResponsesPortfolioDecisionModel(
        client=_Client(_intent(), mode="bad_arguments"), config=_config()
    )
    with pytest.raises(PortfolioDecisionModelError, match="OUTPUT_INVALID"):
        await bad_arguments.request_scenario(
            context=_context(), prompt="bounded prompt", tools=_tools()
        )

    payload = _intent()
    payload["run_id"] = str(UUID("018f0000-0000-7000-8000-000000000099"))
    client = _Client(payload)
    model = ResponsesPortfolioDecisionModel(client=client, config=_config())
    first = await model.request_scenario(
        context=_context(), prompt="bounded prompt", tools=_tools()
    )
    with pytest.raises(PortfolioDecisionModelError, match="OUTPUT_INVALID"):
        await model.request_intent(
            context=_context(), prompt="bounded prompt", frozen=_frozen(), first_turn=first
        )


@pytest.mark.asyncio
async def test_transport_failure_marks_usage_unknown_without_retry() -> None:
    client = _Client(_intent(), mode="transport_error")
    model = ResponsesPortfolioDecisionModel(client=client, config=_config())
    with pytest.raises(PortfolioDecisionModelError, match="MODEL_FAILED") as raised:
        await model.request_scenario(context=_context(), prompt="bounded prompt", tools=_tools())
    assert raised.value.usage_known is False
    assert raised.value.usage.provider_calls == 1
    assert len(client.responses.calls) == 1


@pytest.mark.asyncio
async def test_failed_second_turn_cannot_be_replayed() -> None:
    client = _Client(_intent(), mode="second_tool")
    model = ResponsesPortfolioDecisionModel(client=client, config=_config())
    first = await model.request_scenario(
        context=_context(), prompt="bounded prompt", tools=_tools()
    )

    with pytest.raises(PortfolioDecisionModelError, match="TOOL_LIMIT_EXCEEDED"):
        await model.request_intent(
            context=_context(), prompt="bounded prompt", frozen=_frozen(), first_turn=first
        )
    with pytest.raises(PortfolioDecisionModelError, match="MODEL_STATE_INVALID"):
        await model.request_intent(
            context=_context(), prompt="bounded prompt", frozen=_frozen(), first_turn=first
        )

    assert len(client.responses.calls) == 2


@pytest.mark.asyncio
async def test_second_turn_rejects_tampered_first_turn_payload() -> None:
    client = _Client(_intent())
    model = ResponsesPortfolioDecisionModel(client=client, config=_config())
    first = await model.request_scenario(
        context=_context(), prompt="bounded prompt", tools=_tools()
    )
    call = dict(first.continuation[-1])
    call["arguments"] = json.dumps(
        {"objective": "reduce_security_concentration", "target_weight": "0.2"}
    )
    tampered = replace(first, continuation=(call,))

    with pytest.raises(PortfolioDecisionModelError, match="MODEL_STATE_INVALID"):
        await model.request_intent(
            context=_context(), prompt="bounded prompt", frozen=_frozen(), first_turn=tampered
        )

    assert len(client.responses.calls) == 1


@pytest.mark.asyncio
async def test_second_turn_rejects_tampered_tool_output() -> None:
    client = _Client(_intent())
    model = ResponsesPortfolioDecisionModel(client=client, config=_config())
    first = await model.request_scenario(
        context=_context(), prompt="bounded prompt", tools=_tools()
    )
    changed_projection = _projection().model_copy(update={"result_hash": "f" * 64})
    tampered = replace(
        first,
        tool_output={
            **first.tool_output,
            "output": json.dumps(changed_projection.model_dump(mode="json")),
        },
    )

    with pytest.raises(PortfolioDecisionModelError, match="MODEL_STATE_INVALID"):
        await model.request_intent(
            context=_context(), prompt="bounded prompt", frozen=_frozen(), first_turn=tampered
        )

    assert len(client.responses.calls) == 1


@pytest.mark.asyncio
async def test_deadline_exhaustion_before_second_call_keeps_usage_known() -> None:
    client = _Client(_intent())
    config = _config().model_copy(update={"absolute_timeout_seconds": Decimal("0.1")})
    model = ResponsesPortfolioDecisionModel(client=client, config=config)
    first = await model.request_scenario(
        context=_context(), prompt="bounded prompt", tools=_tools()
    )
    await asyncio.sleep(0.11)

    with pytest.raises(PortfolioDecisionModelError, match="MODEL_FAILED") as raised:
        await model.request_intent(
            context=_context(), prompt="bounded prompt", frozen=_frozen(), first_turn=first
        )

    assert raised.value.usage_known is True
    assert raised.value.usage.provider_calls == 1
    assert len(client.responses.calls) == 1


def test_model_requires_transport_with_retries_disabled() -> None:
    with pytest.raises(ValueError, match="retries must be disabled"):
        ResponsesPortfolioDecisionModel(client=_Client(_intent(), max_retries=2), config=_config())


@pytest.mark.asyncio
async def test_invalid_client_shape_uses_stable_model_failure() -> None:
    model = ResponsesPortfolioDecisionModel(client=_BrokenClient(), config=_config())

    with pytest.raises(PortfolioDecisionModelError, match="MODEL_FAILED") as raised:
        await model.request_scenario(context=_context(), prompt="bounded prompt", tools=_tools())

    assert raised.value.usage_known is True
    assert raised.value.usage.provider_calls == 0


def _config() -> ResponsesModelConfig:
    return ResponsesModelConfig(
        model_id="configured-model",
        pricing_version="pricing-v1",
        max_input_tokens=100_000,
        max_output_tokens=400,
        max_tool_calls=1,
        absolute_timeout_seconds=Decimal("12"),
        input_usd_per_million=Decimal("1"),
        output_usd_per_million=Decimal("2"),
    )


def _known(value: str) -> dict[str, object]:
    return {"status": "known", "value": value, "reason": None}


def _metrics() -> dict[str, object]:
    return {
        "security_top1": _known("0.4"),
        "security_top3": _known("0.8"),
        "security_hhi": _known("0.22"),
        "cash_weight": _known("0.1"),
        "one_way_turnover": _known("0.05"),
        "estimated_cost": {
            "status": "unknown",
            "value": None,
            "reason": "COST_EVIDENCE_INCOMPLETE",
        },
    }


def _projection() -> AgentScenarioProjectionV1:
    return AgentScenarioProjectionV1.model_validate(
        {
            "schema_version": "agent_scenario_projection_v1",
            "objective": "reduce_security_concentration",
            "objective_status": "improved_not_met",
            "analysis_status": "partial",
            "baseline": _metrics(),
            "candidate": _metrics(),
            "unknown_metrics": ["estimated_cost"],
            "hard_constraint_reasons": [],
            "changed_position_count": 1,
            "result_hash": RESULT_HASH,
            "citation_ids": [CID],
            "execution_locked": True,
        }
    )


class _Tools:
    names = ("evaluate_adjustment_scenario",)

    async def execute(self, name: str, payload: dict[str, object]) -> dict[str, object]:
        assert name == self.names[0]
        assert payload == {"objective": "reduce_security_concentration"}
        return _projection().model_dump(mode="json")


def _tools() -> _Tools:
    return _Tools()


def _context() -> PortfolioDecisionModelContextV1:
    diagnostics = [
        {
            "code": code,
            "severity": "warning",
            "metric": _known(value),
            "unit": "ratio",
            "citation_ids": [CID],
        }
        for code, value in (
            ("CONCENTRATION_TOP1", "0.4"),
            ("CONCENTRATION_TOP3", "0.8"),
            ("CONCENTRATION_HHI", "0.22"),
        )
    ]
    return PortfolioDecisionModelContextV1.model_validate(
        {
            "schema_version": "portfolio_decision_model_context_v1",
            "run_id": str(RUN_ID),
            "portfolio_id": str(PORTFOLIO_ID),
            "portfolio_snapshot_id": str(SNAPSHOT_ID),
            "diagnostic_report_id": str(REPORT_ID),
            "decision_time": NOW.isoformat(),
            "objective": "reduce_security_concentration",
            "constraints": {
                "schema_version": "adjustment_scenario_constraints_v1",
                "objective": "reduce_security_concentration",
                "max_turnover": "0.05",
                "minimum_cash": "0.1",
                "universe_policy": "current_holdings_only",
                "allow_new_positions": False,
            },
            "current_diagnostics": diagnostics,
            "citation_ids": [CID],
            "execution_locked": True,
        }
    )


def _frozen() -> PortfolioDecisionInputV1:
    return PortfolioDecisionInputV1.model_validate(
        {
            "schema_version": "portfolio_decision_input_v1",
            "portfolio_id": str(PORTFOLIO_ID),
            "portfolio_snapshot_id": str(SNAPSHOT_ID),
            "diagnostic_report_id": str(REPORT_ID),
            "decision_time": NOW.isoformat(),
            "objective": "reduce_security_concentration",
            "max_turnover": "0.05",
            "minimum_cash": "0.1",
            "universe_policy": "current_holdings_only",
            "allow_new_positions": False,
            "scenario_request_hash": "c" * 64,
            "expected_scenario_result_hash": RESULT_HASH,
            "diagnostic_input_hash": "d" * 64,
            "policy_version": "real_advisory_policy_v1",
            "evidence_hash": "e" * 64,
        }
    )


def _intent() -> dict[str, object]:
    return {
        "schema_version": "portfolio_research_intent_v1",
        "run_id": str(RUN_ID),
        "portfolio_id": str(PORTFOLIO_ID),
        "portfolio_snapshot_id": str(SNAPSHOT_ID),
        "diagnostic_report_id": str(REPORT_ID),
        "decision_time": NOW.isoformat(),
        "assessment": "explore_adjustment",
        "objective": "reduce_security_concentration",
        "horizon": "medium_term",
        "confidence": "medium",
        "rationale": [{"text": "Concentration improves.", "citation_ids": [CID]}],
        "invalidation_conditions": [
            {
                "signal": "portfolio.scenario_objective_status",
                "operator": "eq",
                "threshold": "unchanged",
                "unit": "state",
                "citation_ids": [CID],
            }
        ],
        "limitations": [{"code": "estimated_cost", "text": "Estimated cost is unknown."}],
        "scenario_result_hash": RESULT_HASH,
        "citation_ids": [CID],
        "execution_locked": True,
    }


class _Responses:
    def __init__(self, intent: dict[str, object], *, mode: str = "ok") -> None:
        self.intent = intent
        self.mode = mode
        self.calls: list[dict[str, object]] = []

    async def create(self, **kwargs: object) -> dict[str, object]:
        self.calls.append(kwargs)
        if self.mode == "transport_error":
            raise TimeoutError("provider timeout")
        call = {
            "type": "function_call",
            "call_id": "call-1",
            "name": "evaluate_adjustment_scenario",
            "arguments": json.dumps({"objective": "reduce_security_concentration"}),
        }
        if self.mode == "bad_arguments":
            call["arguments"] = json.dumps(
                {"objective": "reduce_security_concentration", "target_weight": "0.2"}
            )
        if len(self.calls) == 1:
            if self.mode == "no_tool":
                return {
                    "output": [{"type": "message"}],
                    "output_text": "{}",
                    "usage": {"input_tokens": 100, "output_tokens": 50},
                }
            output = [call, {**call, "call_id": "call-2"}] if self.mode == "two_tools" else [call]
            return {"output": output, "usage": {"input_tokens": 100, "output_tokens": 50}}
        if self.mode == "second_tool":
            return {"output": [call], "usage": {"input_tokens": 50, "output_tokens": 50}}
        return {
            "output": [{"type": "message"}],
            "output_text": json.dumps(self.intent),
            "usage": {"input_tokens": 50, "output_tokens": 50},
        }


class _Client:
    def __init__(
        self, intent: dict[str, object], *, mode: str = "ok", max_retries: int = 0
    ) -> None:
        self.max_retries = max_retries
        self.responses = _Responses(intent, mode=mode)


class _BrokenClient:
    max_retries = 0
    responses = object()
