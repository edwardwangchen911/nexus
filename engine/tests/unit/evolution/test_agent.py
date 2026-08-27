"""Strict Strategy R&D Agent request, prompt, and Responses runtime."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.agents.responses import ResponsesModelConfig
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
    runtime = ResponsesStrategyRNDRuntime(
        client=_FakeClient(drifted), config=_config()
    )

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
