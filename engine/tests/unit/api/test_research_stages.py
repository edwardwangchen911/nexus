"""Saved stage lineage remains inspectable after failures without accepting corrupt claims."""

import hashlib
import json
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from nexus.agents.codex_staged import STAGES
from nexus.agents.committee import CommitteeRoleOutput
from nexus.api.research_stages import validate_stage_trace
from nexus.domain.evidence import EvidenceRef
from nexus.strategy.intents import ResearchClaim


def _case():
    run = SimpleNamespace(id=uuid4(), evidence_hash="a" * 64)
    evidence = EvidenceRef(
        source="sec",
        object_id="filing",
        available_at=datetime(2026, 1, 1, tzinfo=UTC),
        location="memory://sha256/" + "a" * 64,
        content_hash="a" * 64,
    )
    records, prompts = [], {}
    for stage, roles in STAGES:
        for role in roles:
            reviewed = (
                ()
                if stage == "research"
                else ("bull:0",)
                if stage == "critique"
                else ("bear:0", "evidence_audit:0")
            )
            output = CommitteeRoleOutput(
                role=role,
                vote="abstain",
                claims=(ResearchClaim(text="证据不足，保留观察。", evidence_refs=(evidence,)),),
                prompt_hash="b" * 64,
                input_tokens=10,
                output_tokens=5,
                stage=stage,
                reviewed_claim_ids=reviewed,
            )
            record = SimpleNamespace(
                run_id=run.id,
                status="running",
                role=role,
                prompt_hash=output.prompt_hash,
                payload=output.model_dump(mode="json"),
                intent=None,
            )
            _sign(record)
            records.append(record)
            prompts[role] = output.prompt_hash
    return run, records, (evidence,), prompts


def _sign(record):
    content = {key: getattr(record, key) for key in ("status", "role", "payload", "intent")}
    record.content_hash = hashlib.sha256(
        json.dumps(content, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def test_complete_stages_keep_citations_review_links_and_usage():
    trace = validate_stage_trace(*_case())
    assert trace.status == "verified"
    assert [s.status for s in trace.stages] == ["complete"] * 3
    assert [s.input_tokens for s in trace.stages] == [50, 20, 10]
    assert trace.stages[2].roles[0].reviewed_claim_ids == ("bear:0", "evidence_audit:0")


@pytest.mark.parametrize(
    "count,statuses",
    [
        (0, ["unavailable"] * 3),
        (5, ["complete", "unavailable", "unavailable"]),
        (6, ["complete", "partial", "unavailable"]),
    ],
)
def test_failure_preserves_only_saved_stages(count, statuses):
    run, records, evidence, prompts = _case()
    trace = validate_stage_trace(run, records[:count], evidence, prompts)
    assert trace.status == "verified"
    assert [s.status for s in trace.stages] == statuses


@pytest.mark.parametrize("drift", [None, "stage", "frozen", "missing"])
def test_stage_input_fingerprints_cannot_drift_within_one_run(drift):
    run, records, evidence, prompts = _case()
    for item in records:
        item.payload["stage_input_hash"] = hashlib.sha256(
            item.payload["stage"].encode()
        ).hexdigest()
        item.payload["frozen_inputs_hash"] = "d" * 64
    if drift == "stage":
        records[1].payload["stage_input_hash"] = "f" * 64
    elif drift == "frozen":
        records[-1].payload["frozen_inputs_hash"] = "f" * 64
    elif drift == "missing":
        records[-1].payload["stage_input_hash"] = None
    for item in records:
        _sign(item)
    trace = validate_stage_trace(run, records, evidence, prompts)
    assert trace.status == ("verified" if drift is None else "invalid")
    if drift is None:
        assert {s.frozen_inputs_hash for s in trace.stages} == {"d" * 64}
        assert all(s.input_hash for s in trace.stages)


@pytest.mark.parametrize(
    "damage",
    [
        "digest",
        "citation",
        "review",
        "role",
        "prompt",
        "future_review",
        "missing_bear",
        "cross_run",
    ],
)
def test_damaged_trace_does_not_display_unverified_model_text(damage):
    run, records, evidence, prompts = _case()
    item = records[-1]
    if damage == "digest":
        item.content_hash = "0" * 64
    elif damage == "citation":
        item.payload["claims"][0]["evidence_refs"][0]["content_hash"] = "c" * 64
    elif damage == "review":
        item.payload["reviewed_claim_ids"] = ["bear:99", "evidence_audit:0"]
    elif damage == "role":
        records.reverse()
    elif damage == "prompt":
        item.prompt_hash = "c" * 64
    elif damage == "future_review":
        records[5].payload["reviewed_claim_ids"] = ["evidence_audit:0"]
        _sign(records[5])
    elif damage == "missing_bear":
        item.payload["reviewed_claim_ids"] = ["bull:0", "evidence_audit:0"]
    elif damage == "cross_run":
        item.run_id = uuid4()
    if damage != "digest":
        _sign(item)
    trace = validate_stage_trace(run, records, evidence, prompts)
    assert trace.status == "invalid"
    assert not trace.stages
