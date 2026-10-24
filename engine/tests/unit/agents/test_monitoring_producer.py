"""Versioned monitoring proposals traverse the real three-stage producer."""

import json
from datetime import timedelta
from uuid import UUID

import pytest
from nexus.agents import codex_staged
from nexus.agents.codex_sdk import codex_request_hash
from nexus.agents.codex_versions import BOUNDED_STAGE_BUDGET_CONFIG_VERSION
from nexus.agents.committee import ROLE_ORDER, CommitteeRoleOutput, load_portfolio_synthesis_prompt
from nexus.agents.responses_budget import UsageSnapshot
from nexus.agents.synthesis import synthesize_intent
from nexus.monitoring.contracts import PriceMonitoringProposal
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.strategy.intents import ResearchIntent
from unit.agents.test_codex_sdk import (
    _assessment_payload,
    _authority,
    _restore_view,
    _staged_batch,
    _tools,
)
from unit.agents.test_security_research_scope import _wire

V7_HASHES = {
    ("research", False): "6c11eb3c05a3f89580171f63ec25af34ef81e536c177e3382d817bd04867de09",
    ("research", True): "f41032a132ccdc29610ed0df451a40740863f21fc53ca77f9f4d17a75e69469f",
    ("critique", False): "b0b821ab5eef956a91d129b2e07289ffddae24aae193dce12079104833849c7f",
    ("critique", True): "d9824b4be4582526b274cc96cba805f2284c65183d3491d9ed2d3dea6f0434d8",
    ("synthesis", False): "0a7ac2426148807893dc4065420953d3edd74d41ba6a2695c5d21a332c7bcd0b",
    ("synthesis", True): "8bb9d377ea7159c6e6b4d2db9d2878339aac6cc79cf3e55f7d502f9e8979da15",
}


@pytest.mark.parametrize("stage,capacity", V7_HASHES)
def test_v7_wire_remains_byte_identical(stage, capacity):
    assert (
        codex_request_hash(
            *_wire(stage, new=True, claim_review=True, inference_review=True, capacity=capacity)
        )
        == V7_HASHES[stage, capacity]
    )


def proposal_wire(now):
    return {
        "condition": {
            "signal": "schwab.l1.bid_ask_midpoint",
            "operator": "lte",
            "threshold": "123.45",
            "unit": "currency",
            "currency": "USD",
            "purpose": "review",
            "source": "schwab",
            "feed": "l1",
            "price_basis": "unadjusted_bid_ask_midpoint",
            "max_age_seconds": 60,
        },
        "rationale": "价格低于冻结证据所支持的复核阈值时重新评估。",
        "citations": [0],
        "review_due_at": (now + timedelta(days=1)).isoformat(),
        "valid_until": (now + timedelta(days=2)).isoformat(),
    }


async def produce(
    monkeypatch,
    *,
    rejected=None,
    vote="support",
    adoption="exact",
    citation=0,
    missing_review=None,
    review_hash="exact",
    absent=False,
    expired=False,
    inline=False,
    capture=None,
):
    request, evidence, prompts = _authority()
    prompts = dict(prompts)
    prompts["portfolio"] = load_portfolio_synthesis_prompt(
        security_research_scope=True,
        claim_review_scope=True,
        inference_review_scope=True,
        monitoring_scope=True,
    )
    seen = []
    proposal = None

    async def invoke(payload, *, output_model, **kwargs):
        nonlocal proposal
        payload = _restore_view(payload)
        seen.append(payload)
        stage = payload["stage"]
        if stage == "research":
            body = _staged_batch(stage, payload["requested_roles"])
            for row in body["roles"]:
                row["monitoring_proposal"] = None
                if row["role"] == "market_state" and not absent:
                    row["monitoring_proposal"] = proposal_wire(request.decision_time)
                    row["monitoring_proposal"]["citations"] = [citation]
                    if expired:
                        row["monitoring_proposal"]["review_due_at"] = (
                            request.decision_time.isoformat()
                        )
            wire = proposal_wire(request.decision_time)
            if expired:
                wire["review_due_at"] = request.decision_time.isoformat()
            proposal = (
                None
                if absent
                else PriceMonitoringProposal(
                    **{k: v for k, v in wire.items() if k != "citations"}, evidence_refs=evidence
                )
            )
        elif stage == "critique":
            if absent:
                assert payload["monitoring_proposal"] is None
            else:
                assert payload["monitoring_proposal"]["content_hash"] == proposal.content_hash
                assert payload["monitoring_proposal"]["condition"]["threshold"] == "123.45"
            body = _staged_batch(stage, payload["requested_roles"])
            for row in body["roles"]:
                row["vote"] = vote if row["role"] == "evidence_audit" else "support"
                row["monitoring_review"] = (
                    None
                    if absent or row["role"] == missing_review
                    else {
                        "proposal_hash": proposal.content_hash
                        if review_hash == "exact"
                        else "f" * 64,
                        "decision": "rejected" if row["role"] == rejected else "accepted",
                        "rationale": "核对价格口径、期限与支持证据。",
                        "citations": [0],
                    }
                )
        else:
            assert set(payload["monitoring_reviews"]) == (
                set() if absent else {"bear", "evidence_audit"} - {missing_review}
            )
            body = _assessment_payload(payload["previous_claims"])
            body["synthesis_assessment"].update(stance="watch", review_conclusion="sufficient")
            for review in body["synthesis_assessment"]["reviews"]:
                review.update(resolved=True, missing_evidence=[], prevented_conclusion=None)
            body["monitoring_adoption_hash"] = (
                proposal.content_hash
                if adoption == "exact" and proposal is not None
                else "f" * 64
                if adoption == "foreign"
                else None
            )
        return json.dumps(body), UsageSnapshot(input_tokens=300, output_tokens=100)

    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    committee = codex_staged.StructuredStagedCodexCommittee(
        prompts=prompts, tools=_tools(), paper_stage_scope=True, inline_citations=inline
    )
    original_inputs = committee._inputs

    async def inputs(req):
        value = await original_inputs(req)
        value["security_snapshot"] = {
            "items": [
                {
                    "source": evidence[0].source,
                    "provider_object_id": evidence[0].object_id,
                    "content_hash": evidence[0].content_hash,
                    "available_at": evidence[0].model_dump(mode="json")["available_at"],
                    "evidence_ref_id": "fixture-evidence",
                    "agent_paper_context": {"execution_locked": True},
                }
            ]
        }
        return value

    monkeypatch.setattr(committee, "_inputs", inputs)
    outputs = [
        await committee.analyze(role, request, evidence, prompts[role]) for role in ROLE_ORDER
    ]
    if capture is not None:
        capture["inputs"] = committee._frozen_inputs
        capture["request"] = request
        capture["evidence"] = evidence
        capture["prompts"] = prompts
    intent = synthesize_intent(
        AgentResearchRunRecord(id=UUID(int=3), **request.model_dump()),
        "ABC",
        DataSnapshot(id=request.data_snapshot_id, revision="synthetic"),
        {
            r: AgentPromptVersionRecord(
                role=r, version=p.version, content=p.content, content_hash=p.content_hash
            )
            for r, p in prompts.items()
        },
        AgentModelConfigRecord(model_id="fixture", version=BOUNDED_STAGE_BUDGET_CONFIG_VERSION),
        evidence,
        outputs,
        price_authority_valid=True,
    )
    return intent, outputs, seen


@pytest.mark.asyncio
@pytest.mark.parametrize("inline", [False, True])
async def test_three_calls_bind_original_proposal_reviews_adoption_and_final_intent(
    monkeypatch, inline
):
    intent, outputs, seen = await produce(monkeypatch, inline=inline)
    assert [p["stage"] for p in seen] == ["research", "critique", "synthesis"]
    proposal = outputs[2].monitoring_proposal
    assert intent.monitoring_proposal == proposal
    assert intent.stance == "watch" and intent.exposure_action == "none"
    assert intent.quality_status == "complete"
    assert outputs[5].monitoring_review.proposal_hash == proposal.content_hash
    assert outputs[6].monitoring_review.proposal_hash == proposal.content_hash
    assert (
        ResearchIntent.model_validate_json(intent.model_dump_json()).monitoring_proposal == proposal
    )
    for output in outputs:
        assert CommitteeRoleOutput.model_validate_json(output.model_dump_json()) == output


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kwargs",
    [
        dict(rejected="bear"),
        dict(rejected="evidence_audit"),
        dict(vote="oppose"),
        dict(adoption="none"),
        dict(adoption="foreign"),
        dict(missing_review="bear"),
        dict(missing_review="evidence_audit"),
        dict(review_hash="foreign"),
        dict(absent=True),
        dict(expired=True),
    ],
)
async def test_synthesis_cannot_override_original_rejection_or_missing_adoption(
    monkeypatch, kwargs
):
    intent, _, seen = await produce(monkeypatch, **kwargs)
    assert intent.monitoring_proposal is None
    assert "monitoring_proposal" not in intent.model_dump(mode="json")
    assert len(seen) == 3


@pytest.mark.asyncio
async def test_monitoring_citation_must_bind_frozen_evidence(monkeypatch):
    with pytest.raises(ValueError, match="CODEX_CITATION_INVALID"):
        await produce(monkeypatch, citation=99)


def test_new_fields_are_absent_from_old_output_and_intent():
    from unit.agents.test_execution import _synthesize

    intent = _synthesize(("support",) * 8, actionable=True)
    assert "monitoring_proposal" not in intent.model_dump(mode="json")
    row = CommitteeRoleOutput(
        role="market_state",
        vote="support",
        claims=intent.theses,
        prompt_hash="a" * 64,
        input_tokens=1,
        output_tokens=1,
    )
    assert not any(key.startswith("monitoring_") for key in row.model_dump(mode="json"))


def test_monitoring_schema_hash_selects_only_exact_frozen_v8():
    import hashlib
    from pathlib import Path

    from nexus.agents.persistence import _schema_hash

    root = Path(__file__).parents[4]
    legacy = hashlib.sha256(
        (root / "contracts/agent/research-intent-v1.schema.json").read_bytes()
    ).hexdigest()
    assert legacy == "b47296825d97470f50857cdad7919d6ca90013847ccea175b168c6bbb7911871"
    assert _schema_hash() == legacy
    assert (
        _schema_hash(portfolio_prompt_version="local-portfolio-synthesis-v7-inference-review")
        == legacy
    )
    assert (
        _schema_hash(portfolio_prompt_version="local-portfolio-synthesis-v8-price-monitor")
        != legacy
    )


@pytest.mark.asyncio
async def test_v8_artifact_validates_full_intent_and_keeps_closed_contract(monkeypatch):
    from pathlib import Path

    from jsonschema import Draft202012Validator

    intent, _, _ = await produce(monkeypatch)
    path = (
        Path(__file__).parents[4] / "contracts/agent/research-intent-v8-price-monitor.schema.json"
    )
    assert path.is_file(), "v8 admission needs its own immutable schema artifact"
    validator = Draft202012Validator(json.loads(path.read_text()))
    document = intent.model_dump(mode="json")
    assert not list(validator.iter_errors(document))
    assert list(validator.iter_errors({**document, "target_weight": "0.1"}))
    document["monitoring_proposal"]["evidence_refs"][0]["location"] = "https://private.invalid"
    assert list(validator.iter_errors(document))


@pytest.mark.asyncio
async def test_domain_role_and_intent_reject_forged_monitor_ownership_and_evidence(monkeypatch):
    intent, outputs, _ = await produce(monkeypatch)
    market = outputs[2].model_dump(mode="json")
    with pytest.raises(ValueError, match="MONITOR_ROLE_INVALID"):
        CommitteeRoleOutput.model_validate({**market, "role": "bull"})
    document = intent.model_dump(mode="json")
    document["monitoring_proposal"]["evidence_refs"][0]["content_hash"] = "f" * 64
    with pytest.raises(ValueError, match="MONITOR_PROPOSAL_BINDING_INVALID"):
        ResearchIntent.model_validate(document)


@pytest.mark.asyncio
async def test_typed_monitoring_objects_cannot_bypass_existing_safe_text_policy(monkeypatch):
    intent, outputs, _ = await produce(monkeypatch)
    unsafe = intent.monitoring_proposal.model_copy(update={"rationale": "https://private.invalid"})
    with pytest.raises(ValueError, match="unsafe text"):
        CommitteeRoleOutput(**{**outputs[2].model_dump(), "monitoring_proposal": unsafe})
    with pytest.raises(ValueError, match="unsafe text"):
        ResearchIntent(**{**intent.model_dump(), "monitoring_proposal": unsafe})


def test_v8_requires_structured_synthesis_before_any_model_call():
    _, _, prompts = _authority()
    prompts = dict(prompts)
    prompts["portfolio"] = load_portfolio_synthesis_prompt(
        security_research_scope=True,
        claim_review_scope=True,
        inference_review_scope=True,
        monitoring_scope=True,
    )
    with pytest.raises(ValueError, match="MONITORING_REQUIRES_STRUCTURED_SYNTHESIS"):
        codex_staged.StagedCodexCommittee(prompts=prompts, tools=_tools(), paper_stage_scope=True)


@pytest.mark.parametrize(
    "field", ["monitoring_proposal", "monitoring_review", "monitoring_adoption_hash"]
)
def test_v7_stage_schema_cannot_emit_v8_monitoring_fields(field):
    stage = (
        "research"
        if field == "monitoring_proposal"
        else "critique"
        if field == "monitoring_review"
        else "synthesis"
    )
    payload, model = _wire(stage, new=True, claim_review=True, inference_review=True)
    payload = _restore_view(payload)
    if stage == "synthesis":
        response = _assessment_payload(payload["previous_claims"])
        response[field] = None
    else:
        response = _staged_batch(stage, payload["requested_roles"])
        response["roles"][0][field] = None
    with pytest.raises(ValueError, match="Extra inputs are not permitted"):
        model.model_validate(response)


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["critique", "synthesis"])
async def test_v8_readonly_inspection_restores_original_monitoring_context(monkeypatch, stage):
    import hashlib
    from types import SimpleNamespace

    from nexus.ops.research_workload_inspection import inspect_critique_request

    captured = {}
    _, outputs, _ = await produce(monkeypatch, capture=captured)
    request, evidence, prompts = (captured[key] for key in ("request", "evidence", "prompts"))
    committee = codex_staged.StructuredStagedCodexCommittee(
        prompts=prompts, tools=_tools(), paper_stage_scope=True, inline_citations=False
    )
    run = SimpleNamespace(
        id=UUID(int=3), evidence_hash="b" * 64, pricing_version=BOUNDED_STAGE_BUDGET_CONFIG_VERSION
    )
    records = []
    for index, output in enumerate(outputs):
        content = dict(
            status="running", role=output.role, payload=output.model_dump(mode="json"), intent=None
        )
        records.append(
            SimpleNamespace(
                **content,
                run_id=run.id,
                sequence=index + 1,
                prompt_hash=output.prompt_hash,
                content_hash=hashlib.sha256(
                    json.dumps(content, sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest(),
            )
        )
    result = inspect_critique_request(
        committee,
        run=run,
        request=request,
        evidence_refs=evidence,
        inputs=captured["inputs"],
        records=tuple(records),
        stage=stage,
    )
    assert result["status"] == "request_hash_verified"
    assert result["original_request_hash_matched"] is True
    assert committee._outputs == {}  # Readback cannot alter a live producer's state.
