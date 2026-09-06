"""Synthetic source indexes remain exact across model requests and claim binding."""

import json

import pytest
from nexus.agents import codex_staged
from nexus.agents.committee import ROLE_ORDER
from nexus.agents.responses_budget import UsageSnapshot
from unit.agents.test_codex_sdk import _authority, _staged_batch, _tools


@pytest.mark.asyncio
@pytest.mark.parametrize("selected_source", ["sec", "exchange_calendars"])
@pytest.mark.parametrize("sources", [("sec", "exchange_calendars"), ("exchange_calendars", "sec")])
async def test_explicit_source_index_binds_returned_citation_without_reordering(
    monkeypatch, sources, selected_source
):
    request, original, prompts = _authority()
    evidence = tuple(
        original[0].model_copy(update={"source": source, "object_id": f"synthetic:{source}"})
        for source in sources
    )
    selected_index = sources.index(selected_source)

    async def invoke(payload, *, output_model):
        assert payload["evidence"] == [
            {"index": i, **ref.model_dump(mode="json")} for i, ref in enumerate(evidence)
        ]
        assert "source, object_id and content_hash" in payload["instructions"]
        batch = _staged_batch("research", ROLE_ORDER[:5])
        for role in batch["roles"]:
            for claim in role["claims"]:
                claim["citations"] = [selected_index]
        return json.dumps(batch), UsageSnapshot(input_tokens=100, output_tokens=50)

    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    model = codex_staged.StructuredStagedCodexCommittee(prompts=prompts, tools=_tools())
    for role in ROLE_ORDER[:5]:
        output = await model.analyze(role, request, evidence, prompts[role])
        assert all(claim.evidence_refs == (evidence[selected_index],) for claim in output.claims)
