"""Daily pending adjudication traverses all three existing calls without clearing facts."""

import hashlib
import json
from uuid import UUID

import pytest
from nexus.agents import codex_staged
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
        "purpose": "durable_sec_pending_v1",
        "events": [{"event_id": str(UUID(int=100 + i))} for i in range(n)],
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
    monkeypatch,
    *,
    mutation=None,
    inline=False,
    pending=True,
    n=2,
    capture=None,
    gap=False,
    v2=False,
):
    request, evidence, _ = _authority()
    evidence = tuple(
        ref.model_copy(update={"source": "nexus_agent_paper_context"}) for ref in evidence
    )
    if mutation in {"proposal_no_context_ref", "review_no_context_ref"}:
        evidence += (evidence[0].model_copy(update={"source": "sec", "object_id": "other"}),)
    prompts = prompts_for()
    frozen = context(n) if pending else None
    if frozen is not None and v2:
        frozen = {**frozen, "purpose": "daily_monitor_context_v2", "materials": []}
    paper = {"execution_locked": True}
    if frozen is not None:
        paper["pending_durable_sec_v2" if v2 else "pending_durable_sec"] = frozen
    if gap:
        assert frozen is None
        paper["pending_durable_sec_gap"] = {
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
                row.update(
                    monitoring_proposal=None,
                    daily_pending_proposal=None,
                    durable_sec_pending_proposal=None,
                )
                if row["role"] == "orchestrator" and frozen is not None:
                    row["durable_sec_pending_proposal"] = {
                        "context_hash": digest(frozen),
                        "items": [
                            {
                                "event_id": t["event_id"],
                                "status": "addressed" if i % 2 == 0 else "unresolved",
                                "rationale": "已核对原触发及父研究，当前证据支持复核结论。",
                                "citations": [0],
                            }
                            for i, t in enumerate(frozen["events"])
                        ],
                    }
                    if mutation == "context":
                        row["durable_sec_pending_proposal"]["context_hash"] = "f" * 64
                    if mutation == "missing_member":
                        row["durable_sec_pending_proposal"]["items"].pop()
                    if mutation == "foreign_member":
                        row["durable_sec_pending_proposal"]["items"][0]["event_id"] = str(
                            UUID(int=999)
                        )
                    if mutation == "duplicate":
                        row["durable_sec_pending_proposal"]["items"][-1] = dict(
                            row["durable_sec_pending_proposal"]["items"][0]
                        )
                    if mutation == "proposal_no_context_ref":
                        row["durable_sec_pending_proposal"]["items"][0]["citations"] = [1]
                    if mutation == "citation":
                        row["durable_sec_pending_proposal"]["items"][0]["citations"] = [99]
        elif stage == "critique":
            expected = payload["durable_sec_pending_proposal"]
            body = _staged_batch(stage, payload["requested_roles"])
            for row in body["roles"]:
                row.update(
                    monitoring_review=None,
                    daily_pending_review=None,
                    durable_sec_pending_review=None,
                )
                row["vote"] = (
                    "oppose"
                    if mutation == "audit_vote" and row["role"] == "evidence_audit"
                    else "support"
                )
                if expected is not None and mutation != "missing_review":
                    row["durable_sec_pending_review"] = {
                        "proposal_hash": "f" * 64
                        if mutation == "review_hash"
                        else expected["content_hash"],
                        "decision": "rejected" if mutation == row["role"] else "accepted",
                        "rationale": "逐项核对冻结触发、原论点与处理理由。",
                        "citations": [1] if mutation == "review_no_context_ref" else [0],
                    }
                if mutation == "unexpected_review":
                    row["durable_sec_pending_review"] = {
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
            body["daily_pending_adoption_hash"] = None
            body["durable_sec_pending_adoption_hash"] = (
                "f" * 64
                if mutation == "adoption"
                else expected["content_hash"]
                if expected
                else None
            )
            assert set(payload["durable_sec_pending_reviews"]) == (
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
async def test_foundation_ids_and_dual_review_cross_normal_three_stage_compiler(
    monkeypatch, inline
):
    intent, outputs, seen, _ = await produce(monkeypatch, inline=inline)
    assert len(seen) == 3
    assert intent.daily_pending_proposal is None
    assert intent.durable_sec_pending_proposal is not None
    assert {x.event_id for x in intent.durable_sec_pending_proposal.items} == {
        UUID(int=100),
        UUID(int=101),
    }
    assert [x.status for x in intent.durable_sec_pending_proposal.items] == [
        "addressed",
        "unresolved",
    ]
    assert "trigger_id" not in intent.durable_sec_pending_proposal.model_dump_json()
    assert {x.role for x in outputs if x.durable_sec_pending_review} == {"bear", "evidence_audit"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "context",
        "missing_member",
        "foreign_member",
        "duplicate",
        "review_hash",
        "bear",
        "evidence_audit",
        "audit_vote",
        "adoption",
        "missing_review",
    ],
)
async def test_affected_or_unreviewed_foundation_never_adopts(monkeypatch, mutation):
    try:
        intent, _, _, _ = await produce(monkeypatch, mutation=mutation)
    except ValueError:
        return
    assert intent.durable_sec_pending_proposal is None
