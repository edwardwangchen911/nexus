"""Mixed auxiliary decisions travel through exactly the original three SDK stages."""

import hashlib
import json
from uuid import UUID

import pytest
from helpers.mixed_stage import add_mixed_fields
from nexus.agents import codex_staged
from nexus.agents.codex_sdk import codex_request_hash
from nexus.agents.committee import ROLE_ORDER, load_portfolio_synthesis_prompt
from nexus.agents.responses_budget import UsageSnapshot
from nexus.monitoring.mixed_contracts import compile_mixed_proposal
from nexus.strategy.intents import ResearchIntent
from unit.agents.test_codex_sdk import (
    _assessment_payload,
    _authority,
    _restore_view,
    _staged_batch,
    _tools,
)


def prompts():
    result = dict(_authority()[2])
    result["portfolio"] = load_portfolio_synthesis_prompt(
        security_research_scope=True,
        claim_review_scope=True,
        inference_review_scope=True,
        monitoring_scope=True,
        daily_pending_scope=True,
        disclosure_scope=True,
    )
    return result


def legacy_wire(stage, inline):
    request, refs, _ = _authority()
    request = request.model_copy(
        update={"instrument_id": UUID(int=1), "data_snapshot_id": UUID(int=2)}
    )
    committee = codex_staged.StructuredStagedCodexCommittee(
        prompts=prompts(),
        tools=_tools(),
        paper_stage_scope=True,
        inline_citations=inline,
    )
    inputs = {
        "security_snapshot": {
            "items": [
                {
                    "source": refs[0].source,
                    "provider_object_id": refs[0].object_id,
                    "content_hash": refs[0].content_hash,
                    "available_at": refs[0].model_dump(mode="json")["available_at"],
                    "evidence_ref_id": "fixture",
                    "agent_paper_context": {"execution_locked": True},
                }
            ]
        },
        "financial_facts": {"items": []},
    }
    previous = (
        []
        if stage == "research"
        else [
            {
                "claim_id": f"{role}:0",
                "role": role,
                "vote": "support",
                "text": "冻结原件支持当前研究。",
                "citations": [0],
            }
            for role in ("orchestrator", "bear", "evidence_audit")
        ]
    )
    return committee.build_stage_request(
        stage=stage,
        request=request,
        evidence_refs=refs,
        frozen_inputs=inputs,
        previous=previous,
        financial_table_candidate=inline,
    )


async def produce(monkeypatch, *, mutation=None, inline=False):
    request, refs, _ = _authority()
    refs = (
        refs[0].model_copy(update={"source": "nexus_monitor_increment_v2"}),
        refs[0].model_copy(
            update={"source": "sec", "object_id": "other", "content_hash": "b" * 64}
        ),
    )
    context = {
        "purpose": "monitor_increment_v2",
        "trigger_checks": [
            {"trigger_id": str(UUID(int=10)), "kind": "price", "event_key": ""},
            {
                "trigger_id": str(UUID(int=11)),
                "kind": "disclosure",
                "event_key": "sec:0000123456:0000123456-26-000002",
            },
        ],
        "materials": {
            "contents": [
                {
                    "snapshot_id": str(UUID(int=20)),
                    "archive": {
                        "issuer_cik": "0000123456",
                        "entry": {"accession_number": "0000123456-26-000002"},
                    },
                }
            ]
        },
    }
    seen = []

    async def invoke(payload, *, output_model, **kwargs):
        payload = _restore_view(payload)
        seen.append(payload)
        stage = payload["stage"]
        if stage == "synthesis":
            body = _assessment_payload(payload["previous_claims"])
            body.update(
                monitoring_adoption_hash=None,
                daily_pending_adoption_hash=None,
                disclosure_adoption_hash=None,
            )
        else:
            body = _staged_batch(stage, payload["requested_roles"])
            for row in body["roles"]:
                if stage == "research":
                    row.update(
                        monitoring_proposal=None,
                        daily_pending_proposal=None,
                        disclosure_proposal=None,
                    )
                else:
                    row["vote"] = "support"
                    row.update(
                        monitoring_review=None, daily_pending_review=None, disclosure_review=None
                    )
        add_mixed_fields(body, payload)
        if stage == "research":
            proposal = body["roles"][2]["mixed_event_proposal"]
            if mutation == "context":
                proposal["context_hash"] = "f" * 64
            if mutation == "missing":
                proposal["items"].pop()
            if mutation == "duplicate":
                proposal["items"][1] = dict(proposal["items"][0])
            if mutation == "content":
                proposal["items"][1]["content_snapshot_id"] = str(UUID(int=999))
            if mutation == "price_content":
                proposal["items"][0]["content_snapshot_id"] = str(UUID(int=20))
            if mutation == "event_key":
                proposal["items"][1]["event_key"] = "foreign"
            if mutation == "citation":
                proposal["items"][0]["citations"] = [999]
            if mutation == "context_citation":
                proposal["items"][0]["citations"] = [1]
            if mutation == "available":
                proposal["items"][0]["status"] = "available"
            if mutation == "unknown":
                proposal["items"][0]["status"] = "unknown"
            if mutation == "wrong_role":
                body["roles"][0]["mixed_event_proposal"] = proposal
            if mutation == "null":
                body["roles"][2]["mixed_event_proposal"] = None
            if mutation == "unresolved":
                proposal["items"][1]["status"] = "unresolved"
        elif stage == "critique":
            if mutation == "review_context_citation":
                body["roles"][0]["mixed_event_review"]["citations"] = [1]
            if mutation == "review_hash":
                body["roles"][0]["mixed_event_review"]["proposal_hash"] = "f" * 64
            if mutation == "rejected":
                body["roles"][0]["mixed_event_review"]["decision"] = "rejected"
            if mutation == "unreviewed":
                body["roles"][0]["mixed_event_review"] = None
            if mutation == "audit_veto":
                body["roles"][1]["vote"] = "oppose"
        return json.dumps(body), UsageSnapshot(input_tokens=300, output_tokens=100)

    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    committee = codex_staged.StructuredStagedCodexCommittee(
        prompts=prompts(),
        tools=_tools(),
        paper_stage_scope=True,
        inline_citations=inline,
    )
    original = committee._inputs

    async def inputs(req):
        value = await original(req)
        value["security_snapshot"] = {
            "purpose": "monitor_increment_v2",
            "items": [
                {
                    "source": refs[0].source,
                    "provider_object_id": refs[0].object_id,
                    "content_hash": refs[0].content_hash,
                    "available_at": refs[0].model_dump(mode="json")["available_at"],
                    "evidence_ref_id": "fixture",
                    "monitor_context": context,
                }
            ],
        }
        return value

    monkeypatch.setattr(committee, "_inputs", inputs)
    outputs = [await committee.analyze(role, request, refs, prompts()[role]) for role in ROLE_ORDER]
    return outputs, seen, refs, context


@pytest.mark.asyncio
@pytest.mark.parametrize("inline", [False, True])
async def test_complete_proposal_and_reviews_survive_exactly_three_calls(monkeypatch, inline):
    outputs, seen, refs, context = await produce(monkeypatch, inline=inline)
    proposal = outputs[2].mixed_event_proposal
    assert [p["stage"] for p in seen] == ["research", "critique", "synthesis"]
    assert all(p["mixed_event_context_hash"] == refs[0].content_hash for p in seen)
    assert seen[1]["mixed_event_proposal"]["content_hash"] == proposal.content_hash
    assert seen[2]["mixed_event_proposal"] == seen[1]["mixed_event_proposal"]
    assert set(seen[2]["mixed_event_reviews"]) == {"bear", "evidence_audit"}
    assert (
        compile_mixed_proposal(
            proposal,
            {row.role: row.mixed_event_review for row in outputs[5:7]},
            context=context,
            evidence_refs=refs,
            audit_vote=outputs[6].vote,
            adoption_hash=outputs[7].mixed_event_adoption_hash,
        )
        == proposal
    )
    assert "mixed_event" not in json.dumps(ResearchIntent.model_json_schema())
    for row in outputs:
        assert type(row).model_validate_json(row.model_dump_json()) == row


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    [
        "context",
        "missing",
        "duplicate",
        "content",
        "price_content",
        "event_key",
        "citation",
        "available",
        "unknown",
        "wrong_role",
        "null",
        "review_hash",
        "context_citation",
        "review_context_citation",
    ],
)
async def test_mixed_wire_cannot_claim_a_different_or_incomplete_frozen_event(
    monkeypatch, mutation
):
    with pytest.raises(ValueError):
        await produce(monkeypatch, mutation=mutation)


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["rejected", "unreviewed", "audit_veto"])
async def test_synthesis_self_adoption_cannot_override_original_audit(monkeypatch, mutation):
    outputs, _, refs, context = await produce(monkeypatch, mutation=mutation)
    with pytest.raises(ValueError, match="MIXED_EVENT_ADOPTION_INVALID"):
        compile_mixed_proposal(
            outputs[2].mixed_event_proposal,
            {
                row.role: row.mixed_event_review
                for row in outputs[5:7]
                if row.mixed_event_review is not None
            },
            context=context,
            evidence_refs=refs,
            audit_vote=outputs[6].vote,
            adoption_hash=outputs[7].mixed_event_adoption_hash,
        )


@pytest.mark.asyncio
async def test_unresolved_event_remains_unresolved_after_acceptance(monkeypatch):
    outputs, _, _, _ = await produce(monkeypatch, mutation="unresolved")
    assert outputs[2].mixed_event_proposal.items[1].status == "unresolved"


V10_WIRE_HASHES = {
    ("research", False): "8ad1151634a236a2eb956e0b187c2c62827c3da9afceeb3b336cafecac7cc5f3",
    ("research", True): "992c4c46cbb5bd32ec19d26bf1c9625e94894666b06ea6bc1de98be59e23b4d8",
    ("critique", False): "92158e97bcc315011540794a4ead08d106f515241a1f88bcb2c1a174b10b6130",
    ("critique", True): "dd85850c1a003c5f6a39fe2eb7709481f8d86de10d840423cb20c45b0d8d39de",
    ("synthesis", False): "48636c3ceec16c8b9af64e570ecf3a42d3aeed90547d100d24e9848685df377e",
    ("synthesis", True): "82396bf5d73f59128f12dd81579055f026efef6cb5101a489d328c377c1f6d7f",
}


@pytest.mark.parametrize("stage,inline", V10_WIRE_HASHES)
def test_existing_v10_wire_and_output_schema_remain_byte_identical(stage, inline):
    payload, model = legacy_wire(stage, inline)
    assert codex_request_hash(payload, model) == V10_WIRE_HASHES[stage, inline]
    assert "mixed_event" not in json.dumps(model.model_json_schema())
    assert "mixed_event" not in json.dumps(payload)


def test_existing_research_intent_schema_remains_identical():
    raw = json.dumps(ResearchIntent.model_json_schema(), sort_keys=True, separators=(",", ":"))
    assert (
        hashlib.sha256(raw.encode()).hexdigest()
        == "661efb0aab6b3ba85e75cdb0afa2783f048529ee43da0d2f23e54d5320268a5f"
    )


@pytest.mark.asyncio
async def test_auxiliary_role_ownership_and_legacy_none_serialization(monkeypatch):
    outputs, _, _, _ = await produce(monkeypatch)
    market, bear, portfolio = outputs[2], outputs[5], outputs[7]
    for role, field, value in (
        ("bull", "mixed_event_proposal", market.mixed_event_proposal),
        ("portfolio", "mixed_event_review", bear.mixed_event_review),
        ("bear", "mixed_event_adoption_hash", portfolio.mixed_event_adoption_hash),
    ):
        values = outputs[0].model_dump()
        values.update(role=role, **{field: value})
        with pytest.raises(ValueError, match="MONITOR_ROLE_INVALID"):
            type(market).model_validate(values)
    for row in outputs:
        wire = row.model_dump(mode="json")
        for field in ("mixed_event_proposal", "mixed_event_review", "mixed_event_adoption_hash"):
            assert (field in wire) == (getattr(row, field) is not None)
