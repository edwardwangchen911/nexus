"""Strict Strategy R&D Agent request, prompt, and Responses runtime."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.agents.responses import ResponsesModelConfig
from nexus.agents.responses_budget import UsageSnapshot
from nexus.evolution.agent import (
    ResponsesStrategyRNDRuntime,
    StrategyRNDRequestV1,
    load_strategy_rnd_prompt,
)
from nexus.evolution.agent_tools import StrategyRNDToolContext, StrategyRNDTools
from nexus.evolution.service import _calendar_quarter_after
from pydantic import ValidationError

NOW = datetime(2026, 8, 27, 12, tzinfo=UTC)
PARENT_ID = UUID("019d0000-0000-7000-8000-000000004101")
LINEAGE_ID = UUID("019d0000-0000-7000-8000-000000004102")
POLICY_ID = UUID("019d0000-0000-7000-8000-000000004103")
SUMMARY_ID = UUID("019d0000-0000-7000-8000-000000004104")
SNAPSHOT_ID = UUID("019d0000-0000-7000-8000-000000004105")
EVIDENCE_ID = UUID("019d0000-0000-7000-8000-000000004106")


def _request() -> StrategyRNDRequestV1:
    return StrategyRNDRequestV1(
        schema_version="strategy_rnd_request_v1",
        parent_strategy_definition_id=PARENT_ID,
        champion_generation=1,
        research_lineage_id=LINEAGE_ID,
        evolution_policy_id=POLICY_ID,
        training_validation_summary_id=SUMMARY_ID,
        data_snapshot_id=SNAPSHOT_ID,
        evidence_refs=(EVIDENCE_ID,),
        as_of=NOW,
    )


def _proposal() -> dict[str, object]:
    return {
        "schema_version": "strategy_experiment_proposal_v1",
        "parent_strategy_definition_id": str(PARENT_ID),
        "champion_generation": 1,
        "research_lineage_id": str(LINEAGE_ID),
        "hypothesis": "A registered liquidity threshold may improve robustness.",
        "expected_mechanism": "The frozen paired validation should reduce unstable names.",
        "invalidation_conditions": ["Paired validation does not improve robustness."],
        "evidence_refs": [str(EVIDENCE_ID)],
        "mutation_axes": [{"axis": "min_close", "values": ["3", "10"]}],
    }


def _tools() -> StrategyRNDTools:
    return StrategyRNDTools(
        context=StrategyRNDToolContext(
            parent_strategy_definition_id=PARENT_ID,
            champion_generation=1,
            research_lineage_id=LINEAGE_ID,
            evolution_policy_id=POLICY_ID,
            training_validation_summary_id=SUMMARY_ID,
            as_of=NOW,
            sealed_experiment_ids=(),
            current_family_experiment_ids=(),
        )
    )


def test_request_is_frozen_strict_and_requires_unique_frozen_evidence() -> None:
    with pytest.raises(ValidationError):
        StrategyRNDRequestV1.model_validate(
            {**_request().model_dump(), "evidence_refs": [EVIDENCE_ID, EVIDENCE_ID]}
        )
    with pytest.raises(ValidationError):
        StrategyRNDRequestV1.model_validate({**_request().model_dump(), "url": "forbidden"})
    with pytest.raises(ValidationError):
        _request().as_of = NOW  # type: ignore[misc]


def test_prompt_asset_is_versioned_hashed_and_explicitly_nonexecuting() -> None:
    prompt = load_strategy_rnd_prompt()

    assert prompt.role == "strategy_rnd"
    assert prompt.version == "strategy_rnd_v1"
    assert len(prompt.content_hash) == 64
    assert "never place orders" in prompt.content.lower()
    assert "registered mutation" in prompt.content.lower()


def test_family_cooldown_uses_three_calendar_months() -> None:
    assert _calendar_quarter_after(datetime(2027, 1, 31, 12, tzinfo=UTC)) == datetime(
        2027, 4, 30, 12, tzinfo=UTC
    )
    assert _calendar_quarter_after(datetime(2027, 11, 30, 12, tzinfo=UTC)) == datetime(
        2028, 2, 29, 12, tzinfo=UTC
    )


@pytest.mark.asyncio
async def test_responses_runtime_uses_only_strict_schema_and_configured_model() -> None:
    client = _FakeClient(_proposal())
    runtime = ResponsesStrategyRNDRuntime(
        client=client,
        config=ResponsesModelConfig(
            model_id="configured-strategy-model",
            pricing_version="fixture-v1",
            max_input_tokens=10_000,
            max_output_tokens=1_000,
            max_tool_calls=0,
            absolute_timeout_seconds=Decimal("5"),
            input_usd_per_million=Decimal("1"),
            output_usd_per_million=Decimal("2"),
        ),
    )

    proposal = await runtime.generate(_request(), _tools())

    assert proposal.parent_strategy_definition_id == PARENT_ID
    assert proposal.evidence_refs == (EVIDENCE_ID,)
    assert runtime.usage.input_tokens == 25
    assert runtime.usage.output_tokens == 10
    call = client.responses.calls[0]
    assert call["model"] == "configured-strategy-model"
    assert call["text"]["format"]["strict"] is True
    assert call["text"]["format"]["name"] == "strategy_experiment_proposal_v1"
    assert {tool["name"] for tool in call["tools"]} == set(_tools().names)


@pytest.mark.asyncio
async def test_runtime_rejects_proposal_identity_or_evidence_drift() -> None:
    drifted = {**_proposal(), "champion_generation": 2}
    runtime = ResponsesStrategyRNDRuntime(client=_FakeClient(drifted), config=_config())

    with pytest.raises(ValueError, match="identity"):
        await runtime.generate(_request(), _tools())


def _config() -> ResponsesModelConfig:
    return ResponsesModelConfig(
        model_id="configured-strategy-model",
        pricing_version="fixture-v1",
        max_input_tokens=10_000,
        max_output_tokens=1_000,
        max_tool_calls=0,
        absolute_timeout_seconds=Decimal("5"),
        input_usd_per_million=Decimal("1"),
        output_usd_per_million=Decimal("2"),
    )


class _FakeResponses:
    def __init__(self, proposal: dict[str, object]) -> None:
        self.proposal = proposal
        self.calls: list[dict[str, object]] = []

    async def create(self, **kwargs: object) -> dict[str, object]:
        self.calls.append(dict(kwargs))
        return {
            "output": [{"type": "message"}],
            "output_text": json.dumps(self.proposal),
            "usage": {"input_tokens": 25, "output_tokens": 10},
        }


class _FakeClient:
    def __init__(self, proposal: dict[str, object]) -> None:
        self.responses = _FakeResponses(proposal)


def _codex_config(**updates) -> ResponsesModelConfig:
    from nexus.agents.codex_sdk import MAX_INPUT_TOKENS, MAX_OUTPUT_TOKENS, MODEL_ID
    from nexus.evolution.codex import CODEX_RND_CONFIG_VERSION

    values = {
        "model_id": MODEL_ID,
        "pricing_version": CODEX_RND_CONFIG_VERSION,
        "max_input_tokens": MAX_INPUT_TOKENS,
        "max_output_tokens": MAX_OUTPUT_TOKENS,
        "max_tool_calls": 0,
        "absolute_timeout_seconds": Decimal("5"),
        "input_usd_per_million": Decimal("0"),
        "output_usd_per_million": Decimal("0"),
    }
    return ResponsesModelConfig(**{**values, **updates})


def _codex_tools(*, training_return="0.05", sealed=()) -> StrategyRNDTools:
    context = _tools().context.model_copy(update={"sealed_experiment_ids": sealed})
    return StrategyRNDTools(
        context=context,
        executors={
            "get_strategy_parent": lambda _: {
                "strategy_definition_id": str(PARENT_ID),
                "strategy_version": "synthetic-v1",
                "expression_hash": "a" * 64,
                "registered_parameters": {
                    "min_adv20_usd": "25000000",
                    "min_close": "3",
                    "target_weight": "0.1",
                },
                "ranking_feature": "return_126",
                "positive_return_filter_enabled": True,
            },
            "get_evolution_search_space": lambda _: {
                "policy_id": str(POLICY_ID),
                "policy_version": "evolution_search_space_v1",
                "content_hash": "b" * 64,
                "axes": [{"axis": "min_close", "values": ["3", "10"]}],
            },
            "get_training_validation_summary": lambda _: {
                "summary_id": str(SUMMARY_ID),
                "authority_hash": "c" * 64,
                "quality_status": "PASS",
                "training_window_count": 2,
                "validation_window_count": 1,
                "training_return": training_return,
                "validation_return": "0.03",
                "training_sharpe": "0.8",
                "validation_sharpe": "0.6",
                "maximum_drawdown": "0.2",
                "evidence_refs": [str(EVIDENCE_ID)],
            },
            "get_sealed_experiment_summary": lambda payload: {
                "experiment_id": str(payload["experiment_id"]),
                "candidate_hash": "d" * 64,
                "summary_hash": "e" * 64,
                "status": "BACKTEST_FAILED",
                "gate_status": "FAIL",
                "evidence_refs": [str(EVIDENCE_ID)],
            },
        },
    )


@pytest.mark.asyncio
async def test_codex_rnd_freezes_typed_tools_and_uses_one_strict_sdk_turn(monkeypatch):
    from nexus.evolution import codex
    from nexus.evolution.models import StrategyExperimentProposalV1

    sealed = (UUID(int=11), UUID(int=12))
    invoke = AsyncMock(
        return_value=(
            json.dumps(_proposal()),
            UsageSnapshot(input_tokens=300, output_tokens=80, provider_calls=1),
        )
    )
    monkeypatch.setattr(codex, "invoke_codex", invoke)
    runtime = codex.CodexStrategyRNDRuntime(config=_codex_config())
    proposal = await runtime.generate(_request(), _codex_tools(sealed=sealed))
    assert proposal == StrategyExperimentProposalV1.model_validate(_proposal())
    payload = invoke.call_args.args[0]
    assert payload["request"] == _request().model_dump(mode="json")
    assert payload["prompt"]["content_hash"] == load_strategy_rnd_prompt().content_hash
    assert payload["frozen_inputs"]["strategy_parent"]["strategy_definition_id"] == str(PARENT_ID)
    assert payload["frozen_inputs"]["evolution_search_space"]["policy_id"] == str(POLICY_ID)
    assert payload["frozen_inputs"]["training_validation"]["summary_id"] == str(SUMMARY_ID)
    assert [item["experiment_id"] for item in payload["frozen_inputs"]["sealed_experiments"]] == [
        str(item) for item in sealed
    ]
    assert "no tools" in payload["instructions"].lower()
    assert invoke.call_args.kwargs["output_model"] is StrategyExperimentProposalV1
    assert runtime.usage == UsageSnapshot(input_tokens=300, output_tokens=80, provider_calls=1)
    with pytest.raises(ValueError, match="ALREADY_ATTEMPTED"):
        await runtime.generate(_request(), _codex_tools())
    invoke.assert_awaited_once()


@pytest.mark.parametrize(
    "updates",
    [
        {"model_id": "wrong-model"},
        {"pricing_version": "codex-sdk-chatgpt-v1"},
        {"max_tool_calls": 1},
        {"max_input_tokens": 10000},
        {"max_output_tokens": 1000},
        {"input_usd_per_million": Decimal("1")},
        {"output_usd_per_million": Decimal("1")},
        {"absolute_timeout_seconds": Decimal("31")},
    ],
)
def test_codex_rnd_rejects_mismatched_runtime_authority(updates):
    from nexus.evolution.codex import CodexStrategyRNDRuntime

    with pytest.raises(ValueError, match="CONFIG_INVALID"):
        CodexStrategyRNDRuntime(config=_codex_config(**updates))


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["context", "tool_output", "input_size", "sealed_limit"])
async def test_codex_rnd_invalid_frozen_input_never_reaches_sdk(monkeypatch, invalid):
    from nexus.evolution import codex

    invoke = AsyncMock()
    monkeypatch.setattr(codex, "invoke_codex", invoke)
    runtime = codex.CodexStrategyRNDRuntime(config=_codex_config())
    request = _request()
    tools = _codex_tools()
    if invalid == "context":
        request = request.model_copy(update={"champion_generation": 2})
    elif invalid == "tool_output":
        tools = StrategyRNDTools(
            context=tools.context, executors={"get_strategy_parent": lambda _: {}}
        )
    elif invalid == "input_size":
        tools = _codex_tools(training_return="1" * 64000)
    else:
        tools = _codex_tools(sealed=tuple(UUID(int=i) for i in range(1, 34)))
    with pytest.raises(ValueError):
        await runtime.generate(request, tools)
    invoke.assert_not_awaited()
    assert runtime.usage == UsageSnapshot()


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["schema", "identity", "evidence", "usage", "tools"])
async def test_codex_rnd_rejects_invalid_response_without_retry(monkeypatch, invalid):
    from nexus.evolution import codex

    document = _proposal()
    usage = UsageSnapshot(input_tokens=300, output_tokens=80, provider_calls=1)
    if invalid == "schema":
        document["unexpected"] = "not allowed"
    elif invalid == "identity":
        document["champion_generation"] = 2
    elif invalid == "evidence":
        document["evidence_refs"] = [str(UUID(int=99))]
    elif invalid == "usage":
        usage = UsageSnapshot(input_tokens=64001, output_tokens=80, provider_calls=1)
    else:
        usage = UsageSnapshot(input_tokens=300, output_tokens=80, tool_calls=1, provider_calls=1)
    invoke = AsyncMock(return_value=(json.dumps(document), usage))
    monkeypatch.setattr(codex, "invoke_codex", invoke)
    runtime = codex.CodexStrategyRNDRuntime(config=_codex_config())
    with pytest.raises(ValueError):
        await runtime.generate(_request(), _codex_tools())
    with pytest.raises(ValueError, match="ALREADY_ATTEMPTED"):
        await runtime.generate(_request(), _codex_tools())
    assert runtime.usage == usage
    invoke.assert_awaited_once()


@pytest.mark.asyncio
async def test_codex_rnd_timeout_preserves_nonzero_usage_bound_without_replay(monkeypatch):
    from nexus.evolution import codex

    async def stall(*args, **kwargs):
        await asyncio.sleep(1)

    invoke = AsyncMock(side_effect=stall)
    monkeypatch.setattr(codex, "invoke_codex", invoke)
    config = _codex_config(absolute_timeout_seconds=Decimal("0.01"))
    runtime = codex.CodexStrategyRNDRuntime(config=config)
    with pytest.raises(TimeoutError):
        await runtime.generate(_request(), _codex_tools())
    assert runtime.usage == UsageSnapshot(
        input_tokens=config.max_input_tokens,
        output_tokens=config.max_output_tokens,
        provider_calls=1,
    )
    with pytest.raises(ValueError, match="ALREADY_ATTEMPTED"):
        await runtime.generate(_request(), _codex_tools())
    invoke.assert_awaited_once()


@pytest.mark.asyncio
async def test_codex_rnd_deadline_includes_local_evidence_freezing(monkeypatch):
    from nexus.evolution import codex

    tools = _codex_tools()
    execute = tools.execute

    async def delayed_lookup(name, payload):
        await asyncio.sleep(0.03)
        return await execute(name, payload)

    monkeypatch.setattr(tools, "execute", delayed_lookup)
    invoke = AsyncMock(
        return_value=(
            json.dumps(_proposal()),
            UsageSnapshot(input_tokens=300, output_tokens=80, provider_calls=1),
        )
    )
    monkeypatch.setattr(codex, "invoke_codex", invoke)
    runtime = codex.CodexStrategyRNDRuntime(
        config=_codex_config(absolute_timeout_seconds=Decimal("0.01"))
    )
    with pytest.raises(TimeoutError):
        await runtime.generate(_request(), tools)
    invoke.assert_not_awaited()
    assert runtime.usage == UsageSnapshot()
