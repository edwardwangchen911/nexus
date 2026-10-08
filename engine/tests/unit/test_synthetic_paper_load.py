"""Synthetic load remains visibly non-authoritative and cannot alter public evidence."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.ops.synthetic_paper_load import SyntheticPaperLoadTools


@pytest.mark.asyncio
async def test_paper_load_is_marked_synthetic_without_changing_base_snapshot():
    result = {"items": [{"source": "sec", "provider_object_id": "public"}]}
    base = AsyncMock()
    base.names = ("get_security_snapshot",)
    base.filing_forms = ("10-K", "8-K/A")
    base.execute.return_value = result
    tools = SyntheticPaperLoadTools(
        base, instrument_id=UUID(int=1), decision_time=datetime(2026, 10, 8, tzinfo=UTC)
    )
    observed = await tools.execute("get_security_snapshot", {"scope": "already validated by base"})
    assert observed["items"][0] == result["items"][0]
    assert observed["items"] == result["items"]
    assert len(result["items"]) == 1
    item = observed["diagnostic_synthetic_paper_load"]
    assert item["source"] == "diagnostic_synthetic"
    assert item["agent_paper_context"]["execution_locked"] is True
    assert item["agent_paper_context"]["decision_quote"]["source"] == "diagnostic_synthetic"
    assert "account_id" not in str(item)
    assert tools.filing_forms == base.filing_forms
    assert tools.diagnostic_synthetic_paper_load is True


@pytest.mark.asyncio
async def test_other_tools_and_validation_are_forwarded_unchanged():
    base = AsyncMock()
    base.names = ("get_price_features",)
    base.execute.return_value = {"items": []}
    tools = SyntheticPaperLoadTools(
        base, instrument_id=UUID(int=1), decision_time=datetime(2026, 10, 8, tzinfo=UTC)
    )
    assert await tools.execute("get_price_features", {"test": 1}) == {"items": []}
    base.execute.assert_awaited_once_with("get_price_features", {"test": 1})
    base.execute.side_effect = ValueError("invalid base scope")
    with pytest.raises(ValueError, match="invalid base scope"):
        await tools.execute("get_security_snapshot", {})


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["research", "critique", "synthesis"])
async def test_normal_compiler_labels_synthetic_load_and_keeps_real_paper_path(stage):
    from nexus.agents.codex_staged import StructuredStagedCodexCommittee
    from unit.agents.test_codex_sdk import _authority, _tools

    request, evidence, prompts = _authority()
    tools = SyntheticPaperLoadTools(
        _tools(), instrument_id=request.instrument_id, decision_time=request.decision_time
    )
    committee = StructuredStagedCodexCommittee(
        prompts=prompts, tools=tools, paper_stage_scope=True, inline_citations=True
    )
    inputs = await committee._inputs(request)
    payload, _ = committee.build_stage_request(
        stage=stage, request=request, evidence_refs=evidence, frozen_inputs=inputs, previous=[]
    )
    assert "diagnostic synthetic Paper load" in payload["instructions"]
    assert "not a Schwab quote" in payload["instructions"]
    assert "decision_quote is the current Schwab quote" not in payload["instructions"]
    ordinary = StructuredStagedCodexCommittee(
        prompts=prompts, tools=_tools(), paper_stage_scope=True
    )
    ordinary_payload, _ = ordinary.build_stage_request(
        stage=stage,
        request=request,
        evidence_refs=evidence,
        frozen_inputs={"security_snapshot": {"items": [{"agent_paper_context": {}}]}},
        previous=[],
    )
    assert "decision_quote is the current Schwab quote" in ordinary_payload["instructions"]
    assert "diagnostic synthetic Paper load" not in ordinary_payload["instructions"]


def test_timing_receipt_cannot_leak_model_payload_or_untyped_phase():
    from types import SimpleNamespace

    from nexus.ops.research_budget_diagnostic import timing_metadata

    record = SimpleNamespace(
        payload={
            "secret_model_text": "must not appear",
            "stage_timings": {
                "research": {
                    "allocated_timeout_ms": 50000,
                    "elapsed_ms": 11000,
                    "phase_elapsed_ms": {"turn_run": 10000},
                }
            },
        }
    )
    value = timing_metadata([record])
    assert value["stage_timings"]["research"]["elapsed_ms"] == 11000
    assert "must not appear" not in str(value)
    record.payload["stage_timings"]["research"]["phase_elapsed_ms"] = {"private raw error": 1}
    with pytest.raises(ValueError, match="^DIAGNOSTIC_TIMING_INVALID$"):
        timing_metadata([record])
