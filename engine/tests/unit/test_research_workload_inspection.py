"""Workload metadata must distinguish Paper context without exporting its values."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
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


@pytest.mark.asyncio
async def test_formal_inspection_uses_frozen_profile_without_invoking_committee(monkeypatch):
    from nexus.agents.codex_versions import FORMAL_STAGE_CAPACITY_CONFIG_VERSION
    from nexus.ops import research_workload_inspection as inspection

    committee = SimpleNamespace(analyze=AsyncMock())
    factory = AsyncMock(return_value=committee)
    monkeypatch.setattr(inspection, "_committee", factory)
    model = SimpleNamespace(version=FORMAL_STAGE_CAPACITY_CONFIG_VERSION)
    result = await inspection._inspection_committee(None, None, None, model=model)

    assert result is committee
    assert factory.call_args.kwargs["formal_stage_capacity"] is True
    assert factory.call_args.kwargs["deadline"] is not None
    assert factory.call_args.kwargs["responses_client"] is None
    assert factory.call_args.kwargs["tools"] is None
    committee.analyze.assert_not_awaited()


@pytest.mark.asyncio
async def test_legacy_inspection_does_not_enable_formal_profile(monkeypatch):
    from nexus.ops import research_workload_inspection as inspection

    factory = AsyncMock()
    monkeypatch.setattr(inspection, "_committee", factory)
    await inspection._inspection_committee(
        None, None, None, model=SimpleNamespace(version="codex-sdk-chatgpt-v6-stage-time-budget")
    )
    assert factory.call_args.kwargs["formal_stage_capacity"] is False
    assert factory.call_args.kwargs["deadline"] is None


def test_wire_limit_summary_preserves_over_limit_request_and_does_not_claim_readiness():
    from nexus.ops.research_workload_inspection import summarize_stage_request
    from pydantic import BaseModel

    class Response(BaseModel):
        result: str

    payload = {"stage": "research", "text": "PRIVATE_SENTINEL" * 5000}
    before = json.dumps(payload)
    summary = summarize_stage_request(payload, Response, max_wire_bytes=64000)
    assert summary["wire_limit_status"] == "exceeded"
    assert summary["wire_limit_bytes"] == 64000
    assert summary["wire_bytes"] > 64000
    assert json.dumps(payload) == before
    assert "PRIVATE_SENTINEL" not in str(summary)
    assert "ready" not in summary.values()


def test_component_sizes_only_publish_fixed_component_names():
    inputs = {
        "filing_disclosures": {"private": "PRIVATE_SENTINEL"},
        "PRIVATE_UNKNOWN_COMPONENT": {"private": "PRIVATE_SENTINEL"},
    }
    summary = summarize_inputs(inputs)
    assert summary["component_bytes"] == {
        "filing_disclosures": len(
            json.dumps(
                inputs["filing_disclosures"],
                sort_keys=True,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode()
        )
    }
    assert "PRIVATE" not in str(summary)


def test_critique_readback_checks_prior_research_not_later_stage(monkeypatch):
    import hashlib
    from datetime import UTC, datetime
    from uuid import uuid4

    from nexus.agents.committee import ROLE_ORDER, CommitteeRoleOutput
    from nexus.domain.evidence import EvidenceRef
    from nexus.ops import research_workload_inspection as inspection

    ref = EvidenceRef(
        source="fixture",
        object_id="fixture",
        location="memory://fixture",
        available_at=datetime(2026, 1, 1, tzinfo=UTC),
        content_hash="a" * 64,
    )
    inputs = {"security_snapshot": {"items": []}}
    frozen_hash = inspection.summarize_inputs(inputs)["frozen_inputs_sha256"]
    run = SimpleNamespace(id=uuid4(), evidence_hash="a" * 64)
    records = []
    for role in ROLE_ORDER[:5]:
        output = CommitteeRoleOutput(
            role=role,
            vote="support",
            claims=(),
            prompt_hash="b" * 64,
            input_tokens=0,
            output_tokens=0,
            stage="research",
            stage_input_hash="c" * 64,
            frozen_inputs_hash=frozen_hash,
        )
        payload = output.model_dump(mode="json")
        content = {"status": "running", "role": role, "payload": payload, "intent": None}
        records.append(
            SimpleNamespace(
                run_id=run.id,
                content_hash=hashlib.sha256(
                    json.dumps(content, sort_keys=True, separators=(",", ":"), default=str).encode()
                ).hexdigest(),
                **content,
            )
        )
    later = {
        "status": "running",
        "role": "portfolio",
        "payload": {"stage": "synthesis"},
        "intent": None,
    }
    records.append(
        SimpleNamespace(
            run_id=run.id,
            content_hash=hashlib.sha256(
                json.dumps(later, sort_keys=True, separators=(",", ":"), default=str).encode()
            ).hexdigest(),
            **later,
        )
    )

    def verify(_run, rows, _refs, _prompts):
        return SimpleNamespace(
            status="verified" if [row.role for row in rows] == list(ROLE_ORDER[:5]) else "invalid"
        )

    monkeypatch.setattr(inspection, "validate_stage_trace", verify)
    committee = SimpleNamespace(
        _max_wire_bytes=64000,
        _prompts={role: SimpleNamespace(content_hash="b" * 64) for role in ROLE_ORDER},
        build_stage_request=lambda **_kwargs: ({}, CommitteeRoleOutput),
    )
    result = inspection.inspect_critique_request(
        committee,
        run=run,
        request=None,
        evidence_refs=(ref,),
        inputs=inputs,
        records=tuple(records),
    )
    assert result["status"] == "current_code_readback_only"
    records[0].content_hash = "f" * 64
    assert (
        inspection.inspect_critique_request(
            committee,
            run=run,
            request=None,
            evidence_refs=(ref,),
            inputs=inputs,
            records=tuple(records),
        )["status"]
        == "unavailable"
    )


def test_readonly_cli_candidate_is_explicit_and_does_not_enable_runtime(monkeypatch, capsys):
    import sys

    from nexus.ops import research_workload_inspection as inspection

    observed = {}

    async def read(_settings, *, run_id, financial_table_candidate=False):
        observed.update(candidate=financial_table_candidate, run_id=run_id)
        return {"status": "workload_observed", "model_calls": 0, "business_writes": 0}

    monkeypatch.setattr(inspection, "local_settings", lambda *_a, **_k: object())
    monkeypatch.setattr(inspection, "inspect_run", read)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "inspection",
            "--repo-root",
            ".",
            "--run-id",
            "00000000-0000-0000-0000-000000000001",
            "--financial-table-candidate",
        ],
    )
    assert inspection.main() == 0
    assert observed["candidate"] is True
    assert json.loads(capsys.readouterr().out)["model_calls"] == 0
