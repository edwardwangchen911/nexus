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
    run = SimpleNamespace(
        id=uuid4(), evidence_hash="a" * 64, pricing_version="codex-sdk-chatgpt-v2"
    )
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


@pytest.mark.parametrize(
    "version",
    [
        "codex-sdk-chatgpt-v3",
        "codex-sdk-chatgpt-v7-stage-capacity-candidate",
        "codex-sdk-chatgpt-v8-stage-capacity",
        "codex-sdk-chatgpt-v9-full-input-capacity",
    ],
)
@pytest.mark.parametrize("damage", [None, "missing", "citation", "coverage"])
def test_structured_trace_checks_and_preserves_full_assessment(damage, version):
    run, records, evidence, prompts = _case()
    run.pricing_version = version
    assessment = {
        "stance": "hold",
        "claims": records[-1].payload["claims"],
        "reviews": [
            {
                "claim_id": claim_id,
                "status": "accepted",
                "rationale": "证据缺口仍在。",
                "evidence_refs": [evidence[0].model_dump(mode="json")],
                "resolved": False,
                "missing_evidence": ["同口径价格"],
                "prevented_conclusion": "不能比较。",
            }
            for claim_id in ("bear:0", "evidence_audit:0")
        ],
        "review_conclusion": "limited",
        "uncertainties": [],
    }
    records[-1].payload["synthesis_assessment"] = assessment
    if damage == "missing":
        del records[-1].payload["synthesis_assessment"]
    elif damage == "citation":
        assessment["reviews"][0]["evidence_refs"][0]["content_hash"] = "f" * 64
    elif damage == "coverage":
        assessment["reviews"].pop()
    _sign(records[-1])
    trace = validate_stage_trace(run, records, evidence, prompts)
    assert trace.status == ("verified" if damage is None else "invalid")
    if damage is None:
        assert trace.stages[-1].roles[0].synthesis_assessment.model_dump(mode="json") == assessment


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "version", ["codex-sdk-chatgpt-v8-stage-capacity", "codex-sdk-chatgpt-v9-full-input-capacity"]
)
async def test_capacity_reader_loads_and_validates_persisted_trace(monkeypatch, version):
    from unittest.mock import AsyncMock

    from nexus.api import research_stages

    run, records, evidence, prompts = _case()
    run.pricing_version = version
    session = AsyncMock()
    session.scalars.return_value = records[:5]
    monkeypatch.setattr(research_stages, "load_frozen_evidence", AsyncMock(return_value=evidence))
    monkeypatch.setattr(research_stages, "domain_evidence", lambda value: value)
    monkeypatch.setattr(
        research_stages,
        "load_frozen_prompts",
        AsyncMock(
            return_value={
                role: SimpleNamespace(content_hash=value) for role, value in prompts.items()
            }
        ),
    )
    trace = await research_stages.read_stage_trace(session, run)
    assert trace is not None
    assert trace.status == "verified"
    assert [stage.status for stage in trace.stages] == ["complete", "unavailable", "unavailable"]


@pytest.mark.parametrize("damage", [None, "missing", "citation", "coverage"])
def test_platform_trace_uses_existing_structured_validation(monkeypatch, damage):
    from nexus.agents.comparison_manifest import PLATFORM_ACTOR
    from nexus.agents.platform_usage import CURRENT_PLATFORM_PRICE_VERSION

    original = validate_stage_trace

    def verified(run, records, evidence, prompts):
        run.actor_id = PLATFORM_ACTOR
        return original(run, records, evidence, prompts, comparison_semantics_verified=True)

    monkeypatch.setitem(globals(), "validate_stage_trace", verified)
    test_structured_trace_checks_and_preserves_full_assessment(
        damage, CURRENT_PLATFORM_PRICE_VERSION
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing_model", "profile", "run_hash", "binding"])
async def test_platform_reader_rejects_invalid_native_authority(monkeypatch, damage):
    from unittest.mock import AsyncMock

    from nexus.agents import comparison_authority
    from nexus.agents.platform_profile import platform_model_fields
    from nexus.api import research_stages

    run, _, _, _ = _case()
    model = SimpleNamespace(**platform_model_fields())
    run.model_config_id = uuid4()
    run.model_hash = model.config_hash
    run.pricing_version = model.version
    if damage == "profile":
        model.max_output_tokens += 1
    elif damage == "run_hash":
        run.model_hash = "f" * 64
    session = AsyncMock()
    session.get.return_value = None if damage == "missing_model" else model
    binding = AsyncMock(return_value=damage != "binding")
    monkeypatch.setattr(comparison_authority, "comparison_context_qualified", binding)
    trace = await research_stages.read_stage_trace(session, run)
    assert trace is not None and trace.status == "invalid" and trace.stages == ()
    session.scalars.assert_not_called()
    if damage == "binding":
        binding.assert_awaited_once_with(session, run)
