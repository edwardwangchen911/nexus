"""Complete exhibit text survives the actual staged SDK wire, without changing authority."""

import json
from copy import deepcopy

import pytest
from nexus.agents.codex_staged import StagedCodexCommittee, StructuredStagedCodexCommittee
from nexus.agents.committee import ROLE_ORDER
from nexus.agents.tools import ReadOnlyTools
from unit.agents.test_codex_sdk import (
    _assessment_payload,
    _authority,
    _restore_view,
    _sdk,
    _staged_batch,
)
from unit.research.test_sec_exhibits import build


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [2, 3])
async def test_complete_exhibit_is_preserved_in_every_actual_staged_request(monkeypatch, version):
    report = build()
    request, evidence, prompts = _authority()
    request = request.model_copy(
        update={
            "instrument_id": report.instrument_id,
            "decision_time": report.available_at,
        }
    )
    scope = {
        "schema_version": "agent_read_only_tool_result_v1",
        "instrument_id": str(request.instrument_id),
        "data_snapshot_id": str(request.data_snapshot_id),
        "as_of": request.decision_time.isoformat(),
        "evidence_hash": "a" * 64,
        "portfolio_scope": None,
    }
    disclosure = {
        **scope,
        "status": "ok",
        "reason": None,
        "items": [{"disclosure": report.model_dump(mode="json")}],
    }
    original = deepcopy(disclosure)
    empty = {**scope, "status": "unavailable", "reason": "missing", "items": []}
    tools = ReadOnlyTools(
        executors={
            name: (lambda payload: disclosure)
            if name == "search_filing_evidence"
            else (lambda payload: empty)
            for name in ReadOnlyTools.names
        }
    )
    _, thread, result, _ = _sdk(monkeypatch)
    sent = []

    async def run(wire, **options):
        payload = json.loads(wire)
        sent.append(payload)
        stage = payload["stage"]
        result.final_response = json.dumps(
            _assessment_payload(payload["previous_claims"])
            if version == 3 and stage == "synthesis"
            else _staged_batch(stage, payload["requested_roles"])
        )
        return result

    thread.run.side_effect = run
    cls = StructuredStagedCodexCommittee if version == 3 else StagedCodexCommittee
    committee = cls(prompts=prompts, tools=tools)
    for role in ROLE_ORDER:
        await committee.analyze(role, request, evidence, prompts[role])
    assert len(sent) == 3 and disclosure == original
    for payload in sent:
        restored = _restore_view(payload["frozen_inputs"])["filing_disclosures"]
        assert len(restored["items"]) == 1 and restored == original
        excerpt = restored["items"][0]["disclosure"]["exhibit"]["excerpt"]
        assert len(excerpt) > 12000 and "GAAP operating cash flow" in excerpt
