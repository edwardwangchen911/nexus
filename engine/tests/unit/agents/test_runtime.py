"""Fail-closed Agent runtime mode and official Responses transport tests."""

import asyncio
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal
from json import dumps
from uuid import UUID

import pytest
from nexus.agents.models import ResearchRequest
from nexus.agents.responses import ResponsesModelConfig, ResponsesResearchModel
from nexus.agents.tools import ReadOnlyTools
from nexus.settings import EngineSettings, Environment
from pydantic import ValidationError


def test_non_fixture_runtime_requires_key_and_both_configured_model_ids() -> None:
    """A missing production model configuration must fail startup, not silently use a default."""
    with pytest.raises(ValidationError, match="Agent non-fixture mode requires"):
        EngineSettings(
            environment=Environment.PRODUCTION,
            database_url="postgresql+asyncpg://fixture",
            agent_runtime_mode="responses",
        )


def test_non_fixture_runtime_accepts_primary_openai_model_environment_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Using only legacy AGENT_* model names would make the documented configuration fail."""
    monkeypatch.setenv("AGENT_RUNTIME_MODE", "responses")
    monkeypatch.setenv("OPENAI_API_KEY", "fixture-key")
    monkeypatch.setenv("OPENAI_EXTRACTION_MODEL", "configured-extraction")
    monkeypatch.setenv("OPENAI_REASONING_MODEL", "configured-reasoning")
    settings = EngineSettings(environment=Environment.TEST)
    assert settings.openai_extraction_model == "configured-extraction"
    assert settings.openai_reasoning_model == "configured-reasoning"


@pytest.mark.asyncio
async def test_responses_adapter_uses_strict_schema_and_configured_model_with_fake_sdk() -> None:
    """The adapter must execute typed custom calls then parse strict structured output."""
    request = _request()
    intent = _intent_payload(request)
    fake = _FakeClient(intent)
    observed_payloads: list[Mapping[str, object]] = []

    async def snapshot(payload: Mapping[str, object]) -> Mapping[str, object]:
        observed_payloads.append(payload)
        return {"symbol": "ABC", "quality": "complete"}

    model = ResponsesResearchModel(client=fake, config=_config())
    result = await model.respond(
        request, ReadOnlyTools(executors={"get_security_snapshot": snapshot})
    )
    assert result["intent"] == intent
    assert result["cost_usd"] == "0.00035"
    assert result["input_tokens"] == 150
    assert result["output_tokens"] == 100
    assert observed_payloads == [
        {
            "instrument_id": UUID("0198d500-1000-7000-8000-000000000112"),
            "data_snapshot_id": UUID("0198d500-1000-7000-8000-000000000113"),
            "as_of": datetime(2026, 8, 24, tzinfo=UTC),
        }
    ]
    assert len(fake.calls) == 2
    assert fake.calls[0]["model"] == "configured-reasoning"
    assert fake.calls[0]["max_output_tokens"] == 400
    assert fake.calls[0]["text"]["format"]["strict"] is True
    assert fake.calls[0]["tools"] and fake.calls[0]["tools"][0]["type"] == "function"
    assert fake.calls[1]["input"][-1]["type"] == "function_call_output"


@pytest.mark.asyncio
async def test_responses_adapter_enforces_aggregate_tool_and_token_caps() -> None:
    """Loop limits apply across Responses calls, not separately to each transport call."""
    request = _request()
    tools = ReadOnlyTools(executors={"get_security_snapshot": lambda payload: {"ok": True}})
    with pytest.raises(ValueError, match="tool call limit"):
        await ResponsesResearchModel(
            client=_FakeClient(_intent_payload(request), repeat_tool=True),
            config=_config(max_tool_calls=1),
        ).respond(request, tools)
    with pytest.raises(ValueError, match="output token limit"):
        await ResponsesResearchModel(
            client=_FakeClient(_intent_payload(request), excess_output=True),
            config=_config(max_output_tokens=40),
        ).respond(request, tools)


@pytest.mark.asyncio
async def test_responses_adapter_uses_one_absolute_deadline_across_the_loop() -> None:
    """A provider cannot reset the timeout after each custom-function result."""

    class SlowResponses(_FakeResponses):
        async def create(self, **kwargs: object) -> Mapping[str, object]:
            await asyncio.sleep(0.02)
            return await super().create(**kwargs)

    request = _request()
    fake = _FakeClient(_intent_payload(request))
    fake.responses = SlowResponses(_intent_payload(request))
    config = _config().model_copy(update={"absolute_timeout_seconds": Decimal("0.03")})
    with pytest.raises(TimeoutError):
        await ResponsesResearchModel(client=fake, config=config).respond(
            request,
            ReadOnlyTools(executors={"get_security_snapshot": lambda payload: {"ok": True}}),
        )


class _FakeResponses:
    def __init__(
        self,
        intent: Mapping[str, object],
        *,
        repeat_tool: bool = False,
        excess_output: bool = False,
    ) -> None:
        self.intent = intent
        self.repeat_tool = repeat_tool
        self.excess_output = excess_output
        self.calls: list[dict[str, object]] = []

    async def create(self, **kwargs: object) -> Mapping[str, object]:
        self.calls.append(kwargs)
        if len(self.calls) == 1 or self.repeat_tool:
            return {
                "output": [
                    {
                        "type": "function_call",
                        "call_id": f"call-{len(self.calls)}",
                        "name": "get_security_snapshot",
                        "arguments": dumps(
                            {
                                "instrument_id": "0198d500-1000-7000-8000-000000000112",
                                "data_snapshot_id": "0198d500-1000-7000-8000-000000000113",
                                "as_of": "2026-08-24T00:00:00Z",
                            }
                        ),
                    }
                ],
                "usage": {"input_tokens": 100, "output_tokens": 50},
            }
        return {
            "output": [{"type": "message"}],
            "output_text": dumps(self.intent),
            "usage": {
                "input_tokens": 50,
                "output_tokens": 401 if self.excess_output else 50,
            },
        }


class _FakeClient:
    def __init__(
        self,
        intent: Mapping[str, object],
        *,
        repeat_tool: bool = False,
        excess_output: bool = False,
    ) -> None:
        self.responses = _FakeResponses(
            intent, repeat_tool=repeat_tool, excess_output=excess_output
        )

    @property
    def calls(self) -> list[dict[str, object]]:
        return self.responses.calls


def _request() -> ResearchRequest:
    return ResearchRequest(
        instrument_id=UUID("0198d500-1000-7000-8000-000000000112"),
        data_snapshot_id=UUID("0198d500-1000-7000-8000-000000000113"),
        decision_time=datetime(2026, 8, 24, tzinfo=UTC),
    )


def _config(*, max_tool_calls: int = 2, max_output_tokens: int = 400) -> ResponsesModelConfig:
    return ResponsesModelConfig(
        model_id="configured-reasoning",
        pricing_version="fixture-pricing-v1",
        max_input_tokens=1000,
        max_output_tokens=max_output_tokens,
        max_tool_calls=max_tool_calls,
        absolute_timeout_seconds=Decimal("12"),
        input_usd_per_million=Decimal("1"),
        output_usd_per_million=Decimal("2"),
    )


def _intent_payload(request: ResearchRequest) -> dict[str, object]:
    evidence = {
        "source": "fixture",
        "object_id": "e1",
        "available_at": "2026-08-23T00:00:00Z",
        "location": "fixture:e1",
        "content_hash": "a" * 64,
    }
    return {
        "schema_version": "research_intent_v1",
        "decision_id": "0198d500-1000-7000-8000-000000000111",
        "instrument_id": str(request.instrument_id),
        "display_symbol": "ABC",
        "decision_time": "2026-08-24T00:00:00Z",
        "data_snapshot_id": str(request.data_snapshot_id),
        "horizon": "medium_term",
        "stance": "watch",
        "exposure_action": "none",
        "theses": [{"text": "durable demand", "evidence_refs": [evidence]}],
        "counter_theses": [],
        "uncertainties": ["margin pressure"],
        "invalidation_conditions": [],
        "entry_conditions": [],
        "exit_conditions": [],
        "valid_until": "2026-09-01T00:00:00Z",
        "quality_status": "complete",
        "evidence_audit": "complete",
        "role_votes": [{"role": "fundamental", "vote": "abstain"}],
        "disagreement": "none",
        "strategy_version": "baseline_v1",
        "model_version": "fixture_v1",
        "prompt_version": "fixture_v1",
        "data_version": "fixture_v1",
        "evidence_refs": [evidence],
    }
