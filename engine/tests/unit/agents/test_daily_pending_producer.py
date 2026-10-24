"""Daily pending adjudication traverses all three existing calls without clearing facts."""

import hashlib
import json
from uuid import UUID

import pytest
from nexus.agents import codex_staged
from nexus.agents.codex_sdk import codex_request_hash
from nexus.agents.codex_versions import BOUNDED_STAGE_BUDGET_CONFIG_VERSION
from nexus.agents.committee import ROLE_ORDER, load_portfolio_synthesis_prompt
from nexus.agents.responses_budget import UsageSnapshot
from nexus.agents.synthesis import synthesize_intent
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from unit.agents.test_codex_sdk import (
    _assessment_payload,
    _authority,
    _restore_view,
    _staged_batch,
    _tools,
)


def context(n=2):
    return {
        "purpose": "daily_monitor_context_v1",
        "trigger_checks": [{"trigger_id": str(UUID(int=100 + i))} for i in range(n)],
    }


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def prompts_for(*, daily=True):
    prompts = dict(_authority()[2])
    options = dict(
        security_research_scope=True,
        claim_review_scope=True,
        inference_review_scope=True,
        monitoring_scope=True,
    )
    if daily:
        options["daily_pending_scope"] = True
    prompts["portfolio"] = load_portfolio_synthesis_prompt(**options)
    return prompts


def test_v9_is_explicit_and_v8_remains_available():
    assert prompts_for()["portfolio"].version == "local-portfolio-synthesis-v9-daily-pending"
    assert (
        prompts_for(daily=False)["portfolio"].version
        == "local-portfolio-synthesis-v8-price-monitor"
    )


async def produce(
    monkeypatch, *, mutation=None, inline=False, pending=True, n=2, capture=None, gap=False
):
    request, evidence, _ = _authority()
    evidence = tuple(
        ref.model_copy(update={"source": "nexus_agent_paper_context"}) for ref in evidence
    )
    if mutation in {"proposal_no_context_ref", "review_no_context_ref"}:
        evidence += (evidence[0].model_copy(update={"source": "sec", "object_id": "other"}),)
    prompts = prompts_for()
    frozen = context(n) if pending else None
    paper = {"execution_locked": True}
    if frozen is not None:
        paper["pending_monitoring"] = frozen
    if gap:
        assert frozen is None
        paper["pending_monitoring_gap"] = {
            "purpose": "daily_monitor_gap_v1",
            "reason": "MONITOR_PENDING_OVERFLOW",
            "pending_count": 33,
            "max_pending": 32,
        }
    seen = []
    expected = None
    if capture is not None:
        capture["seen"] = seen

    async def invoke(payload, *, output_model, **kwargs):
        nonlocal expected
        payload = _restore_view(payload)
        seen.append(payload)
        stage = payload["stage"]
        if stage == "research":
            body = _staged_batch(stage, payload["requested_roles"])
            for row in body["roles"]:
                row.update(monitoring_proposal=None, daily_pending_proposal=None)
                if row["role"] == "orchestrator" and frozen is not None:
                    row["daily_pending_proposal"] = {
                        "context_hash": digest(frozen),
                        "items": [
                            {
                                "trigger_id": t["trigger_id"],
                                "status": "addressed" if i % 2 == 0 else "unresolved",
                                "rationale": "已核对原触发及父研究，当前证据支持复核结论。",
                                "citations": [0],
                            }
                            for i, t in enumerate(frozen["trigger_checks"])
                        ],
                    }
                    if mutation == "context":
                        row["daily_pending_proposal"]["context_hash"] = "f" * 64
                    if mutation == "missing_member":
                        row["daily_pending_proposal"]["items"].pop()
                    if mutation == "foreign_member":
                        row["daily_pending_proposal"]["items"][0]["trigger_id"] = str(UUID(int=999))
                    if mutation == "duplicate":
                        row["daily_pending_proposal"]["items"][-1] = dict(
                            row["daily_pending_proposal"]["items"][0]
                        )
                    if mutation == "proposal_no_context_ref":
                        row["daily_pending_proposal"]["items"][0]["citations"] = [1]
                    if mutation == "citation":
                        row["daily_pending_proposal"]["items"][0]["citations"] = [99]
        elif stage == "critique":
            expected = payload["daily_pending_proposal"]
            body = _staged_batch(stage, payload["requested_roles"])
            for row in body["roles"]:
                row.update(monitoring_review=None, daily_pending_review=None)
                row["vote"] = (
                    "oppose"
                    if mutation == "audit_vote" and row["role"] == "evidence_audit"
                    else "support"
                )
                if expected is not None and mutation != "missing_review":
                    row["daily_pending_review"] = {
                        "proposal_hash": "f" * 64
                        if mutation == "review_hash"
                        else expected["content_hash"],
                        "decision": "rejected" if mutation == row["role"] else "accepted",
                        "rationale": "逐项核对冻结触发、原论点与处理理由。",
                        "citations": [1] if mutation == "review_no_context_ref" else [0],
                    }
                if mutation == "unexpected_review":
                    row["daily_pending_review"] = {
                        "proposal_hash": "f" * 64,
                        "decision": "accepted",
                        "rationale": "并无原提案。",
                        "citations": [0],
                    }
        else:
            body = _assessment_payload(payload["previous_claims"])
            body["synthesis_assessment"].update(stance="watch", review_conclusion="sufficient")
            for review in body["synthesis_assessment"]["reviews"]:
                review.update(resolved=True, missing_evidence=[], prevented_conclusion=None)
            body["monitoring_adoption_hash"] = None
            body["daily_pending_adoption_hash"] = (
                "f" * 64
                if mutation == "adoption"
                else expected["content_hash"]
                if expected
                else None
            )
            assert set(payload["daily_pending_reviews"]) == (
                {"bear", "evidence_audit"} if expected and mutation != "missing_review" else set()
            )
        return json.dumps(body), UsageSnapshot(input_tokens=300, output_tokens=100)

    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    committee = codex_staged.StructuredStagedCodexCommittee(
        prompts=prompts, tools=_tools(), paper_stage_scope=True, inline_citations=inline
    )
    original = committee._inputs

    async def inputs(req):
        value = await original(req)
        value["security_snapshot"] = {
            "items": [
                {
                    "source": evidence[0].source,
                    "provider_object_id": evidence[0].object_id,
                    "content_hash": evidence[0].content_hash,
                    "available_at": evidence[0].model_dump(mode="json")["available_at"],
                    "agent_paper_context": paper,
                    "evidence_ref_id": "fixture",
                }
            ]
        }
        return value

    monkeypatch.setattr(committee, "_inputs", inputs)
    outputs = [
        await committee.analyze(role, request, evidence, prompts[role]) for role in ROLE_ORDER
    ]
    if capture is not None:
        capture.update(
            request=request, evidence=evidence, prompts=prompts, inputs=committee._frozen_inputs
        )
    intent = synthesize_intent(
        AgentResearchRunRecord(id=UUID(int=3), **request.model_dump()),
        "ABC",
        DataSnapshot(
            id=request.data_snapshot_id,
            revision="synthetic",
            payload={"agent_paper_context": paper},
        ),
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
    return intent, outputs, seen, committee


@pytest.mark.asyncio
@pytest.mark.parametrize("inline", [False, True])
async def test_daily_proposal_and_independent_reviews_survive_three_calls(monkeypatch, inline):
    intent, outputs, seen, _ = await produce(monkeypatch, inline=inline)
    proposal = outputs[0].daily_pending_proposal
    assert intent.daily_pending_proposal == proposal
    assert [i.status for i in proposal.items] == ["addressed", "unresolved"]
    assert all(
        outputs[i].daily_pending_review.proposal_hash == proposal.content_hash for i in (5, 6)
    )
    assert outputs[-1].daily_pending_adoption_hash == proposal.content_hash
    assert [p["stage"] for p in seen] == ["research", "critique", "synthesis"]
    assert intent.stance == "watch" and intent.exposure_action == "none"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "bear",
        "evidence_audit",
        "audit_vote",
        "review_hash",
        "missing_review",
        "review_no_context_ref",
        "adoption",
    ],
)
async def test_no_coverage_from_non_exact_or_rejected_original(monkeypatch, mutation):
    intent, _, seen, _ = await produce(monkeypatch, mutation=mutation)
    assert intent.daily_pending_proposal is None
    assert "daily_pending_proposal" not in intent.model_dump(mode="json")
    assert len(seen) == 3


@pytest.mark.asyncio
async def test_absent_pending_context_has_no_adoption(monkeypatch):
    intent, _, _, _ = await produce(monkeypatch, pending=False)
    assert intent.daily_pending_proposal is None


@pytest.mark.asyncio
async def test_daily_capacity_gap_remains_unreviewed_through_all_stages(monkeypatch):
    intent, outputs, seen, _ = await produce(monkeypatch, pending=False, gap=True)
    assert len(seen) == 3
    assert all(payload["daily_pending_context_hash"] is None for payload in seen)
    assert all("not reviewed or consumed" in payload["instructions"] for payload in seen)
    assert all(row.daily_pending_proposal is None for row in outputs)
    assert all(row.daily_pending_review is None for row in outputs)
    assert all(row.daily_pending_adoption_hash is None for row in outputs)
    assert intent.daily_pending_proposal is None
    assert intent.stance == "watch"


@pytest.mark.asyncio
async def test_daily_capacity_gap_rejects_isolated_adoption_hash(monkeypatch):
    with pytest.raises(ValueError, match="DAILY_PENDING_ADOPTION_WITHOUT_CONTEXT"):
        await produce(monkeypatch, pending=False, gap=True, mutation="adoption")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "citation",
        "duplicate",
        "context",
        "missing_member",
        "foreign_member",
        "proposal_no_context_ref",
    ],
)
async def test_invalid_daily_citations_or_duplicates_are_rejected(monkeypatch, mutation):
    captured = {}
    with pytest.raises(ValueError):
        await produce(monkeypatch, mutation=mutation, capture=captured)
    assert len(captured["seen"]) == 1


@pytest.mark.asyncio
async def test_daily_all_32_members_remain_explicit(monkeypatch):
    intent, _, _, _ = await produce(monkeypatch, n=32)
    assert len(intent.daily_pending_proposal.items) == 32


V8_HASHES = {
    ("research", False): "aeb8a69932d9af9b60ac24e632f2dae493069872e3c1c93d6f6858daa1f604a8",
    ("research", True): "809fbff83fbcc1490a09495dcfa4d20e9a9ee427e99e4871e039fc5029c721c0",
    ("critique", False): "e87833f3cf755bce44a5c78630e701a589194b33eee75552c8ddb83fe05472ce",
    ("critique", True): "298475251f3f56e1769480e2d2f14b6835125bd970515b71030540081db3a7f3",
    ("synthesis", False): "0618fc6681abc828a968ced4874bd39c8a4a16ebc843e456797b157744dc6ac9",
    ("synthesis", True): "eff44e067b594484a59acceba129bbddebbfd3ebe179a7a467d8c653343c565f",
}


@pytest.mark.parametrize("stage,capacity", V8_HASHES)
def test_v8_entire_wire_remains_byte_identical(stage, capacity):
    from nexus.agents.models import ResearchRequest
    from nexus.domain.evidence import EvidenceRef
    from unit.agents.test_security_research_scope import _wire

    payload, _ = _wire(stage, new=True, claim_review=True, inference_review=True, capacity=False)
    committee = codex_staged.StructuredStagedCodexCommittee(
        prompts=prompts_for(daily=False),
        tools=_tools(),
        paper_stage_scope=True,
        inline_citations=capacity,
    )
    wire = committee.build_stage_request(
        stage=stage,
        request=ResearchRequest.model_validate(payload["request"]),
        evidence_refs=tuple(
            EvidenceRef.model_validate({k: v for k, v in e.items() if k != "index"})
            for e in payload["evidence"]
        ),
        frozen_inputs=payload["frozen_inputs"],
        previous=payload["previous_claims"],
        financial_table_candidate=capacity,
    )
    assert codex_request_hash(*wire) == V8_HASHES[stage, capacity]


def test_v9_capacity_bounds_32_original_items_and_independent_reviews():
    from nexus.agents.codex_capacity import candidate_capacity_envelope
    from nexus.agents.codex_sdk import codex_request_wire
    from nexus.agents.committee import CommitteeRoleOutput
    from nexus.monitoring.daily_contracts import (
        DailyPendingItem,
        DailyPendingProposal,
        DailyPendingReview,
    )
    from nexus.strategy.intents import ResearchClaim
    from unit.agents.test_monitoring_capacity import capacity_fixture, with_maximal_monitoring

    model, args = capacity_fixture()
    model._prompts = prompts_for()
    frozen = context(32)
    args["inputs"]["security_snapshot"]["items"][0]["agent_paper_context"]["pending_monitoring"] = (
        frozen
    )
    bound = candidate_capacity_envelope(model, **args)
    populated = with_maximal_monitoring(model, args)
    refs = (args["evidence_refs"][-1],) * 3
    proposal = DailyPendingProposal(
        context_hash=digest(frozen),
        items=tuple(
            DailyPendingItem(
                trigger_id=t["trigger_id"],
                status="unresolved",
                rationale="\x00" * 160,
                evidence_refs=refs,
            )
            for t in frozen["trigger_checks"]
        ),
    )
    populated._outputs["orchestrator"] = CommitteeRoleOutput(
        role="orchestrator",
        vote="support",
        claims=(ResearchClaim(text="边界", evidence_refs=refs),),
        prompt_hash=model._prompts["orchestrator"].content_hash,
        input_tokens=0,
        output_tokens=0,
        daily_pending_proposal=proposal,
    )
    for role in ("bear", "evidence_audit"):
        populated._outputs[role] = populated._outputs[role].model_copy(
            update={
                "daily_pending_review": DailyPendingReview(
                    proposal_hash=proposal.content_hash,
                    decision="accepted",
                    rationale="\x00" * 500,
                    evidence_refs=(refs[-1],) * 4,
                )
            }
        )
    for stage, roles in codex_staged.STAGES:
        previous = [
            dict(
                claim_id=f"{role}:{index}",
                role=role,
                vote="abstain",
                text="\x00" * 400,
                citations=[len(args["evidence_refs"]) - 1] * 3,
            )
            for role in ROLE_ORDER[: ROLE_ORDER.index(roles[0])]
            for index in range(2)
        ]
        payload, _ = populated.build_stage_request(
            stage=stage,
            request=args["request"],
            evidence_refs=args["evidence_refs"],
            frozen_inputs=args["inputs"],
            previous=previous,
            financial_table_candidate=True,
        )
        assert next(row for row in bound["stages"] if row["stage"] == stage)[
            "wire_upper_bytes"
        ] >= len(codex_request_wire(payload).encode())
    assert model._outputs == {} and model.usage.provider_calls == 0


@pytest.mark.asyncio
async def test_v9_schema_and_old_fields_round_trip(monkeypatch):
    from pathlib import Path

    from jsonschema import Draft202012Validator
    from nexus.agents.persistence import _schema_hash
    from nexus.strategy.intents import ResearchIntent

    intent, outputs, _, _ = await produce(monkeypatch)
    root = Path(__file__).parents[4]
    path = root / "contracts/agent/research-intent-v9-daily-pending.schema.json"
    validator = Draft202012Validator(json.loads(path.read_text()))
    assert not list(validator.iter_errors(intent.model_dump(mode="json")))
    assert ResearchIntent.model_validate_json(intent.model_dump_json()) == intent
    assert (
        _schema_hash(portfolio_prompt_version="local-portfolio-synthesis-v9-daily-pending")
        == hashlib.sha256(path.read_bytes()).hexdigest()
    )
    for output in outputs:
        assert type(output).model_validate_json(output.model_dump_json()) == output


@pytest.mark.asyncio
async def test_no_context_cannot_manufacture_review_for_later_stage(monkeypatch):
    captured = {}
    with pytest.raises(ValueError):
        await produce(monkeypatch, pending=False, mutation="unexpected_review", capture=captured)
    assert len(captured["seen"]) == 2


@pytest.mark.asyncio
async def test_33_context_members_fail_before_any_call(monkeypatch):
    captured = {}
    with pytest.raises(ValueError, match="DAILY_PENDING_CONTEXT_INVALID"):
        await produce(monkeypatch, n=33, capture=captured)
    assert captured["seen"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["critique", "synthesis"])
async def test_v9_readback_uses_original_proposal_and_both_reviews(monkeypatch, stage):
    from types import SimpleNamespace

    from nexus.ops.research_workload_inspection import inspect_critique_request

    captured = {}
    _, outputs, _, _ = await produce(monkeypatch, capture=captured)
    committee = codex_staged.StructuredStagedCodexCommittee(
        prompts=captured["prompts"], tools=_tools(), paper_stage_scope=True
    )
    run = SimpleNamespace(
        id=UUID(int=3), evidence_hash="b" * 64, pricing_version=BOUNDED_STAGE_BUDGET_CONFIG_VERSION
    )
    records = []
    for index, output in enumerate(outputs):
        value = dict(
            status="running", role=output.role, payload=output.model_dump(mode="json"), intent=None
        )
        records.append(
            SimpleNamespace(
                **value,
                run_id=run.id,
                sequence=index + 1,
                prompt_hash=output.prompt_hash,
                content_hash=digest(value),
            )
        )
    result = inspect_critique_request(
        committee,
        run=run,
        request=captured["request"],
        evidence_refs=captured["evidence"],
        inputs=captured["inputs"],
        records=tuple(records),
        stage=stage,
    )
    assert result["status"] == "request_hash_verified"
    assert result["original_request_hash_matched"] is True
    assert committee._outputs == {}


@pytest.mark.asyncio
async def test_v9_preflight_preserves_limit_and_rejects_large_context_before_call(monkeypatch):
    from unittest.mock import AsyncMock

    from nexus.agents import codex_capacity
    from unit.agents.test_monitoring_capacity import capacity_fixture

    model, args = capacity_fixture()
    model._prompts = prompts_for()
    args["inputs"]["security_snapshot"]["items"][0]["agent_paper_context"]["pending_monitoring"] = (
        context(32)
    )
    args["inputs"]["security_snapshot"]["padding"] = "x" * 30000
    model._inputs = AsyncMock(return_value=args["inputs"])
    monkeypatch.setattr(codex_capacity, "codex_input_allowance", lambda *_args: 100)
    assert model._max_wire_bytes == 100479
    with pytest.raises(ValueError, match="CODEX_CAPACITY_POOL_WIRE_EXCEEDED"):
        await codex_capacity.preflight_capacity_pool(
            [codex_capacity.CapacityPoolMember(model, args["request"], args["evidence_refs"])],
            expected_instrument_ids=(args["request"].instrument_id,),
        )
    assert model.usage.provider_calls == 0 and model._capacity_pool_manifest is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,source_role,wrong_role",
    [
        ("daily_pending_proposal", 0, "market_state"),
        ("daily_pending_review", 5, "bull"),
        ("daily_pending_adoption_hash", 7, "orchestrator"),
    ],
)
async def test_daily_fields_are_owned_by_exact_roles(monkeypatch, field, source_role, wrong_role):
    from nexus.agents.committee import CommitteeRoleOutput

    _, outputs, _, _ = await produce(monkeypatch)
    source = outputs[source_role].model_dump(mode="json")
    source.update(role=wrong_role)
    with pytest.raises(ValueError, match="MONITOR_ROLE_INVALID"):
        CommitteeRoleOutput.model_validate(source)
    assert field in source
