"""Fail-closed Agent runtime mode and official Responses transport tests."""

import asyncio
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal
from json import dumps
from typing import cast
from uuid import UUID

import pytest
from nexus.agents.committee import load_prompt_assets
from nexus.agents.models import ResearchRequest
from nexus.agents.responses import ResponsesCommittee, ResponsesModelConfig, ResponsesResearchModel
from nexus.agents.tools import ReadOnlyTools
from nexus.domain.evidence import EvidenceRef
from nexus.jobs.models import JobType
from nexus.settings import EngineSettings, Environment
from nexus.worker import _general_registry, run_worker
from openai.types.responses.response import Response
from openai.types.responses.response_function_tool_call import ResponseFunctionToolCall
from openai.types.responses.response_reasoning_item import ResponseReasoningItem
from pydantic import ValidationError


def test_non_fixture_runtime_requires_key_and_both_configured_model_ids() -> None:
    """A missing production model configuration must fail startup, not silently use a default."""
    with pytest.raises(ValidationError, match="Agent non-fixture mode requires"):
        EngineSettings(
            environment=Environment.PRODUCTION,
            database_url="postgresql+asyncpg://fixture",
            agent_runtime_mode="responses",
        )


@pytest.mark.asyncio
async def test_production_worker_never_starts_with_implicit_or_explicit_fixture_runtime() -> None:
    """Production must not silently select the deterministic CI committee."""
    for kwargs in ({}, {"agent_runtime_mode": "fixture"}):
        settings = EngineSettings(
            environment=Environment.PRODUCTION,
            database_url="postgresql+asyncpg://fixture",
            object_store_endpoint="https://object-store.invalid",
            object_store_region="us-east-1",
            object_store_bucket="fixture",
            object_store_access_key_id="fixture-access",
            object_store_secret_access_key="fixture-secret",
            engine_hmac_current_key_id="fixture-current",
            engine_hmac_current_secret="fixture-hmac",
            **kwargs,
        )
        with pytest.raises(RuntimeError, match="requires explicit responses runtime"):
            await run_worker(settings)


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


def test_general_worker_registers_the_configured_audit_manifest_handler() -> None:
    registry = _general_registry(
        EngineSettings.for_test(),
        responses_client=None,
        tools=None,
    )
    assert registry.resolve(JobType.AUDIT_MANIFEST.value) is not None
    assert registry.resolve(JobType.COLLECT_MARKET.value) is None
    assert registry.resolve(JobType.COLLECT_SEC_MACRO.value) is None
    assert registry.resolve(JobType.COLLECT_INTRADAY_SNAPSHOT.value) is None


def test_general_worker_never_registers_snapshot_handler() -> None:
    registry = _general_registry(
        EngineSettings.for_test(),
        responses_client=None,
        tools=None,
    )
    assert registry.resolve(JobType.COLLECT_INTRADAY_SNAPSHOT.value) is None


def test_general_worker_never_registers_provider_handlers_when_stages_are_enabled() -> None:
    registry = _general_registry(
        EngineSettings(environment=Environment.TEST, provider_jobs_enabled=True),
        responses_client=None,
        tools=None,
    )

    for job_type in (JobType.COLLECT_MARKET, JobType.COLLECT_SEC_MACRO):
        assert registry.resolve(job_type.value) is None
    assert registry.resolve(JobType.COLLECT_INTRADAY_SNAPSHOT.value) is None


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


@pytest.mark.asyncio
async def test_responses_committee_returns_frozen_role_output_with_fake_transport() -> None:
    evidence = EvidenceRef(
        source="fixture",
        object_id="e1",
        available_at=datetime(2026, 8, 23, tzinfo=UTC),
        location="fixture:e1",
        content_hash="a" * 64,
    )
    fake = _CommitteeClient(evidence)
    output = await ResponsesCommittee(
        client=fake,
        config=_config(max_tool_calls=0),
        tools=ReadOnlyTools(),
    ).analyze("bull", _request(), (evidence,), load_prompt_assets()["bull"])

    assert (output.role, output.vote) == ("bull", "support")
    assert output.claims[0].evidence_refs == (evidence,)
    assert output.prompt_hash == load_prompt_assets()["bull"].content_hash
    assert (output.input_tokens, output.output_tokens) == (12, 7)
    call = fake.responses.calls[0]
    assert call["model"] == "configured-reasoning"
    assert call["text"]["format"]["name"] == "committee_role_output_v1"
    assert {tool["type"] for tool in call["tools"]} == {"function"}
    schemas = [call["text"]["format"]["schema"], *(tool["parameters"] for tool in call["tools"])]
    for schema in schemas:
        assert set(schema["required"]) == set(schema["properties"])
        assert schema["additionalProperties"] is False


@pytest.mark.asyncio
async def test_responses_committee_accepts_official_typed_sdk_response() -> None:
    evidence = _evidence()
    client = _TypedCommitteeClient(evidence, include_reasoning=True)
    output = await ResponsesCommittee(
        client=client,
        config=_config(max_tool_calls=0),
        tools=ReadOnlyTools(),
    ).analyze("bull", _request(), (evidence,), load_prompt_assets()["bull"])

    assert (output.vote, output.input_tokens, output.output_tokens) == ("support", 12, 7)


@pytest.mark.asyncio
async def test_official_typed_function_call_completes_through_fixed_tool_loop() -> None:
    evidence = _evidence()
    observed: list[Mapping[str, object]] = []

    async def snapshot(payload: Mapping[str, object]) -> Mapping[str, object]:
        observed.append(payload)
        return {"status": "ok"}

    client = _TypedToolCommitteeClient(evidence)
    output = await ResponsesCommittee(
        client=client,
        config=_config(max_tool_calls=1),
        tools=ReadOnlyTools(executors={"get_security_snapshot": snapshot}),
    ).analyze("bull", _request(), (evidence,), load_prompt_assets()["bull"])

    assert output.vote == "support"
    assert observed == [
        {
            "instrument_id": _request().instrument_id,
            "data_snapshot_id": _request().data_snapshot_id,
            "as_of": _request().decision_time,
        }
    ]
    calls = cast(_TypedToolCommitteeResponses, client.responses).calls
    assert isinstance(calls[0]["input"][0]["content"], str)
    assert isinstance(calls[0]["input"][1]["content"], str)
    assert "previous_response_id" not in calls[0]
    assert all(call.get("store") is False for call in calls)
    assert all("previous_response_id" not in call for call in calls)
    assert calls[1]["input"][-1:] == [
        {
            "type": "function_call_output",
            "call_id": "call_fixture",
            "output": '{"status":"ok"}',
        }
    ]
    assert any(item.get("type") == "function_call" for item in calls[1]["input"])
    assert any(
        item.get("type") == "message" and item.get("role") == "assistant" and item.get("content")
        for item in calls[1]["input"]
    )
    assert any(
        item.get("encrypted_content") == "opaque-test-reasoning" for item in calls[1]["input"]
    )


@pytest.mark.asyncio
async def test_responses_committee_shrinks_run_allowance_and_blocks_before_second_call() -> None:
    evidence = _evidence()
    client = _CommitteeClient(evidence)
    committee = ResponsesCommittee(
        client=client,
        config=_config(max_tool_calls=0, max_output_tokens=7),
        tools=ReadOnlyTools(),
    )
    await committee.analyze("bull", _request(), (evidence,), load_prompt_assets()["bull"])
    with pytest.raises(ValueError, match="output allowance exhausted"):
        await committee.analyze("bear", _request(), (evidence,), load_prompt_assets()["bear"])

    assert len(client.responses.calls) == 1


@pytest.mark.asyncio
async def test_responses_committee_bounds_eight_roles_plus_retry_to_one_run_allowance() -> None:
    evidence = _evidence()
    client = _CommitteeClient(evidence, input_tokens=1, output_tokens=1)
    committee = ResponsesCommittee(
        client=client,
        config=_config(max_tool_calls=0, max_output_tokens=9),
        tools=ReadOnlyTools(),
    )
    roles = (*load_prompt_assets(), "orchestrator")
    for role in roles:
        await committee.analyze(role, _request(), (evidence,), load_prompt_assets()[role])

    assert [call["max_output_tokens"] for call in client.responses.calls] == list(range(9, 0, -1))
    assert committee.usage.output_tokens == 9


@pytest.mark.asyncio
async def test_responses_committee_retains_usage_when_structured_output_is_invalid() -> None:
    evidence = _evidence()
    client = _CommitteeClient(evidence, invalid_json=True)
    committee = ResponsesCommittee(
        client=client,
        config=_config(max_tool_calls=0),
        tools=ReadOnlyTools(),
    )
    with pytest.raises(ValueError, match="structured output"):
        await committee.analyze("bull", _request(), (evidence,), load_prompt_assets()["bull"])

    assert (committee.usage.input_tokens, committee.usage.output_tokens) == (12, 7)


class _CommitteeResponses:
    def __init__(
        self,
        evidence: EvidenceRef,
        *,
        input_tokens: int = 12,
        output_tokens: int = 7,
        invalid_json: bool = False,
    ) -> None:
        self.evidence = evidence
        self.input_tokens = input_tokens
        self.output_tokens = output_tokens
        self.invalid_json = invalid_json
        self.calls: list[dict[str, object]] = []

    async def create(self, **kwargs: object) -> Mapping[str, object]:
        self.calls.append(kwargs)
        return {
            "output": [{"type": "message"}],
            "output_text": "{"
            if self.invalid_json
            else dumps(
                {
                    "vote": "support",
                    "claims": [
                        {
                            "text": "Bounded evidence supports the role vote.",
                            "evidence_refs": [self.evidence.model_dump(mode="json")],
                        }
                    ],
                    "conflicting_facts": False,
                }
            ),
            "usage": {
                "input_tokens": self.input_tokens,
                "output_tokens": self.output_tokens,
            },
        }


class _CommitteeClient:
    def __init__(self, evidence: EvidenceRef, **kwargs: object) -> None:
        self.responses = _CommitteeResponses(evidence, **kwargs)


class _TypedCommitteeResponses(_CommitteeResponses):
    def __init__(self, evidence: EvidenceRef, *, include_reasoning: bool = False) -> None:
        super().__init__(evidence)
        self.include_reasoning = include_reasoning

    async def create(self, **kwargs: object) -> Response:
        raw = await super().create(**kwargs)
        response = _official_response(raw)
        if self.include_reasoning:
            response.output.insert(
                0,
                ResponseReasoningItem.model_validate(
                    {
                        "id": "reasoning_fixture",
                        "summary": [],
                        "type": "reasoning",
                        "status": "completed",
                        "encrypted_content": "opaque-test-reasoning",
                    }
                ),
            )
        return response


class _TypedCommitteeClient:
    def __init__(self, evidence: EvidenceRef, *, include_reasoning: bool = False) -> None:
        self.responses = _TypedCommitteeResponses(evidence, include_reasoning=include_reasoning)


class _TypedToolCommitteeResponses(_TypedCommitteeResponses):
    def __init__(self, evidence: EvidenceRef) -> None:
        super().__init__(evidence, include_reasoning=True)

    async def create(self, **kwargs: object) -> Response:
        response = await super().create(**kwargs)
        if len(self.calls) == 1:
            return response.model_copy(
                update={
                    "output": [
                        response.output[0],
                        response.output[1],
                        ResponseFunctionToolCall.model_validate(
                            {
                                "id": "fc_fixture",
                                "call_id": "call_fixture",
                                "name": "get_security_snapshot",
                                "arguments": dumps(
                                    {
                                        "instrument_id": str(_request().instrument_id),
                                        "data_snapshot_id": str(_request().data_snapshot_id),
                                        "as_of": "2026-08-24T00:00:00Z",
                                    }
                                ),
                                "status": "completed",
                                "type": "function_call",
                            }
                        ),
                    ]
                }
            )
        return response


class _TypedToolCommitteeClient:
    def __init__(self, evidence: EvidenceRef) -> None:
        self.responses = _TypedToolCommitteeResponses(evidence)


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
        max_input_tokens=100_000,
        max_output_tokens=max_output_tokens,
        max_tool_calls=max_tool_calls,
        absolute_timeout_seconds=Decimal("12"),
        input_usd_per_million=Decimal("1"),
        output_usd_per_million=Decimal("2"),
    )


def _evidence() -> EvidenceRef:
    return EvidenceRef(
        source="fixture",
        object_id="e1",
        available_at=datetime(2026, 8, 23, tzinfo=UTC),
        location="fixture:e1",
        content_hash="a" * 64,
    )


def _official_response(raw: Mapping[str, object]) -> Response:
    output_text = raw.get("output_text")
    usage = raw["usage"]
    assert isinstance(output_text, str) and isinstance(usage, Mapping)
    return Response.model_validate(
        {
            "id": "resp_fixture",
            "created_at": 0.0,
            "error": None,
            "incomplete_details": None,
            "instructions": None,
            "metadata": None,
            "model": "configured-reasoning",
            "object": "response",
            "output": [
                {
                    "id": "msg_fixture",
                    "content": [
                        {
                            "annotations": [],
                            "text": output_text,
                            "type": "output_text",
                            "logprobs": [],
                        }
                    ],
                    "role": "assistant",
                    "status": "completed",
                    "type": "message",
                }
            ],
            "parallel_tool_calls": False,
            "temperature": None,
            "tool_choice": "auto",
            "tools": [],
            "top_p": None,
            "background": False,
            "conversation": None,
            "max_output_tokens": 400,
            "max_tool_calls": None,
            "previous_response_id": None,
            "prompt": None,
            "prompt_cache_key": None,
            "reasoning": None,
            "safety_identifier": None,
            "service_tier": "default",
            "status": "completed",
            "text": None,
            "top_logprobs": 0,
            "truncation": "disabled",
            "usage": {
                "input_tokens": usage["input_tokens"],
                "input_tokens_details": {"cached_tokens": 0},
                "output_tokens": usage["output_tokens"],
                "output_tokens_details": {"reasoning_tokens": 0},
                "total_tokens": int(usage["input_tokens"]) + int(usage["output_tokens"]),
            },
            "user": None,
        }
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
