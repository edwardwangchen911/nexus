"""Workload metadata must distinguish Paper context without exporting its values."""

import json

from nexus.ops.research_workload_inspection import summarize_inputs


def test_stage_summary_measures_full_wire_and_schema_without_exporting_contents():
    from nexus.agents.codex_sdk import codex_output_schema, codex_request_wire
    from nexus.ops.research_workload_inspection import summarize_stage_request
    from pydantic import BaseModel

    class Response(BaseModel):
        result: str

    payload = {"stage": "critique", "previous_claims": [{"text": "PRIVATE_SENTINEL"}]}
    summary = summarize_stage_request(payload, Response)
    assert summary["wire_bytes"] == len(codex_request_wire(payload).encode())
    assert summary["schema_bytes"] == len(
        json.dumps(
            codex_output_schema(Response), sort_keys=True, ensure_ascii=False, separators=(",", ":")
        ).encode()
    )
    assert summary["input_allowance_units"] > 0
    assert summary["server_token_count"] is None
    assert "PRIVATE_SENTINEL" not in str(summary)


def test_paper_context_changes_workload_metadata_without_exporting_facts():
    plain = {"security_snapshot": {"items": [{"display_symbol": "SYNTHETIC"}]}}
    paper = {
        "security_snapshot": {
            "items": [
                {
                    "display_symbol": "SYNTHETIC",
                    "agent_paper_context": {"paper_portfolio": {"cash_weight": "PRIVATE_SENTINEL"}},
                }
            ]
        }
    }
    before, after = summarize_inputs(plain), summarize_inputs(paper)
    assert before["paper_context_present"] is False
    assert after["paper_context_present"] is True
    assert after["frozen_inputs_bytes"] > before["frozen_inputs_bytes"]
    assert before["frozen_inputs_sha256"] != after["frozen_inputs_sha256"]
    assert "PRIVATE_SENTINEL" not in str(after)
    assert "SYNTHETIC" not in str(after)
    assert after["model_calls"] == 0
    assert after["formal_acceptance"] is False


def test_missing_or_malformed_security_context_is_unknown_not_absent():
    for value in (
        {},
        {"security_snapshot": None},
        {"security_snapshot": {"items": [None]}},
        {"security_snapshot": {"items": [{"agent_paper_context": "damaged"}]}},
    ):
        assert summarize_inputs(value)["paper_context_present"] is None
