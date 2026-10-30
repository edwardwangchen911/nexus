"""Independent monitor research never becomes formal execution authority."""

import hashlib
import json
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID

import pytest
from nexus.agents.research_only import require_formal_research

AT = datetime(2026, 10, 27, 3, tzinfo=UTC)
PURPOSE = "independent_monitor_research_v1"
SOURCE = "nexus_independent_monitor_research"


class SnapshotSession:
    def __init__(self, snapshot):
        self.snapshot = snapshot

    async def get(self, _model, _identity):
        return self.snapshot


@pytest.mark.asyncio
@pytest.mark.parametrize("marker", ["source", "schema_version", "payload"])
async def test_independent_source_cannot_enter_formal_execution_even_if_other_markers_are_corrupt(
    marker,
):
    snapshot = SimpleNamespace(
        source="nexus_research", schema_version="research_filings_v1", payload={}
    )
    if marker == "payload":
        snapshot.payload["purpose"] = PURPOSE
    else:
        setattr(snapshot, marker, SOURCE if marker == "source" else PURPOSE)
    run = SimpleNamespace(
        actor_id="local-research", data_snapshot_id=UUID(int=1), evidence_manifest=[]
    )
    with pytest.raises(ValueError, match="INDEPENDENT_MONITOR_FORMAL_CONSUMPTION_FORBIDDEN"):
        await require_formal_research(SnapshotSession(snapshot), run)


def context_payload():
    return dict(
        purpose=PURPOSE,
        request_id=str(UUID(int=1)),
        request_hash="a" * 64,
        account_id=str(UUID(int=2)),
        candidate_batch_id=str(UUID(int=3)),
        batch_hash="b" * 64,
        candidate_ids=[str(UUID(int=i)) for i in (4, 5, 6)],
        instrument_id=str(UUID(int=4)),
        decision_time=AT.isoformat(),
        source_snapshot_id=str(UUID(int=7)),
        source_snapshot_hash="c" * 64,
        predecessors=[dict(kind=k, plan_id=None, generation=None) for k in ("price", "disclosure")],
        disclosure_monitoring=dict(
            instrument_id=str(UUID(int=4)),
            research_as_of=AT.isoformat(),
            baseline=None,
            gap_reason="SEC_BASELINE_MISSING",
        ),
    )


def test_independent_context_requires_both_explicit_predecessors_and_exact_membership():
    from nexus.monitoring.independent_context import IndependentMonitorContext

    value = context_payload()
    context = IndependentMonitorContext.model_validate_json(json.dumps(value))
    assert context.predecessor("price").plan_id is None
    assert context.execution_locked is True
    assert context.formal_consumption_allowed is False
    for patch in [
        {"predecessors": value["predecessors"][:1]},
        {"predecessors": [value["predecessors"][0]] * 2},
        {"instrument_id": str(UUID(int=8))},
        {"candidate_ids": [str(UUID(int=4))] * 3},
        {"candidate_ids": []},
        {"execution_locked": False},
        {"formal_consumption_allowed": True},
        {"daily_monitoring": {"trigger_ids": []}},
        {
            "predecessors": [
                dict(kind="price", plan_id=str(UUID(int=9)), generation=None),
                value["predecessors"][1],
            ]
        },
        {
            "disclosure_monitoring": dict(
                value["disclosure_monitoring"], instrument_id=str(UUID(int=5))
            )
        },
    ]:
        with pytest.raises(ValueError):
            IndependentMonitorContext.model_validate_json(json.dumps(value | patch))


def test_independent_snapshot_cannot_be_relabelled_or_contain_formal_context():
    from nexus.monitoring.independent_context import require_independent_snapshot

    payload = {
        "purpose": PURPOSE,
        "as_of": AT.isoformat(),
        "instrument_id": str(UUID(int=4)),
        "independent_monitor_context": context_payload(),
    }
    snapshot = SimpleNamespace(
        source=SOURCE,
        schema_version=PURPOSE,
        payload=payload,
        revision=hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest(),
        provider_object_id=str(UUID(int=4)),
        observed_at=AT,
        available_at=AT,
        received_at=AT,
    )
    assert require_independent_snapshot(snapshot).request_id == UUID(int=1)
    for field, value in [
        ("source", "nexus_research_only"),
        ("schema_version", "research_only_v2"),
        ("revision", "0" * 64),
    ]:
        corrupted = SimpleNamespace(**(vars(snapshot) | {field: value}))
        with pytest.raises(ValueError):
            require_independent_snapshot(corrupted)
    for forbidden in [
        "agent_paper_context",
        "portfolio_context",
        "monitor_context",
        "daily_monitoring",
    ]:
        changed = payload | {forbidden: {}}
        corrupt = SimpleNamespace(
            **(
                vars(snapshot)
                | {
                    "payload": changed,
                    "revision": hashlib.sha256(
                        json.dumps(changed, sort_keys=True).encode()
                    ).hexdigest(),
                }
            )
        )
        with pytest.raises(ValueError):
            require_independent_snapshot(corrupt)


def test_new_request_contract_has_distinct_identity_and_cannot_claim_formal_mode():
    from nexus.paper.manual_request import _validate_event, manual_request_hash

    args = dict(account_id=UUID(int=1), batch_id=UUID(int=2))
    legacy = manual_request_hash(**args, mode="research_only", contract_version=2)
    independent = manual_request_hash(**args, mode="research_and_monitor", contract_version=3)
    assert independent != legacy
    for mode, version in [
        ("research_and_monitor", 1),
        ("research_and_monitor", 2),
        ("research_only", 3),
        ("research_and_paper", 3),
    ]:
        with pytest.raises(ValueError):
            manual_request_hash(**args, mode=mode, contract_version=version)
    row = SimpleNamespace(mode="research_and_monitor", contract_version=3, requested_at=AT)
    prior = SimpleNamespace(status="preparing", occurred_at=AT, cycle_id=None)
    event = SimpleNamespace(
        status="active", code="MANUAL_RESEARCH_STARTED", occurred_at=AT, cycle_id=None
    )
    _validate_event(row, event, prior)
    event.cycle_id = UUID(int=20)
    with pytest.raises(ValueError):
        _validate_event(row, event, prior)


def test_historical_independent_context_never_seeds_price_episode():
    from nexus.monitoring.independent_context import IndependentMonitorContext
    from nexus.monitoring.installation import consumed_baseline

    context = IndependentMonitorContext.model_validate_json(json.dumps(context_payload()))
    assert consumed_baseline(SimpleNamespace(), context) == "unknown"


def test_disclosure_stage_reads_only_exact_independent_context_without_daily_authority():
    from nexus.monitoring.disclosure_contracts import disclosure_from_inputs
    from nexus.monitoring.independent_context import IndependentMonitorContext

    value = IndependentMonitorContext.model_validate_json(json.dumps(context_payload())).model_dump(
        mode="json"
    )
    security = dict(purpose=PURPOSE, independent_monitor_context=value, items=[])
    assert disclosure_from_inputs({"security_snapshot": security}) == value["disclosure_monitoring"]
    security["items"] = [
        {"agent_paper_context": {"disclosure_monitoring": value["disclosure_monitoring"]}}
    ]
    with pytest.raises(ValueError, match="DISCLOSURE_CONTEXT_INVALID"):
        disclosure_from_inputs({"security_snapshot": security})


def test_independent_stage_schema_cannot_adopt_daily_pending_events():
    from nexus.agents.codex_staged import _assessment_model, _stage_model_for_prior_claims
    from pydantic import ValidationError
    from unit.agents.test_codex_sdk import _assessment_payload, _staged_batch

    response = _staged_batch(
        "research", ["orchestrator", "fundamental", "market_state", "macro_sector", "bull"]
    )
    for row in response["roles"]:
        row.update(monitoring_proposal=None, disclosure_proposal=None, daily_pending_proposal=None)
    model = _stage_model_for_prior_claims(
        "research", (), monitoring=True, disclosure=True, daily=False
    )
    model.model_validate(response)
    response["roles"][0]["daily_pending_proposal"] = {"context_hash": "a" * 64, "items": []}
    with pytest.raises(ValidationError):
        model.model_validate(response)
    assessment = _assessment_payload([])
    assessment.update(
        monitoring_adoption_hash=None,
        disclosure_adoption_hash=None,
        daily_pending_adoption_hash=None,
    )
    model = _assessment_model((), monitoring=True, disclosure=True, daily=False)
    model.model_validate(assessment)
    assessment["daily_pending_adoption_hash"] = "a" * 64
    with pytest.raises(ValidationError):
        model.model_validate(assessment)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "version", ["independent-monitor-research-v1", "independent-monitor-research-v2"]
)
async def test_independent_missing_raw_price_basis_drops_only_price_not_audited_sec(
    monkeypatch, version
):
    from nexus.agents.codex_versions import BOUNDED_STAGE_BUDGET_CONFIG_VERSION
    from nexus.agents.synthesis import synthesize_intent
    from nexus.persistence.models.agents import (
        AgentModelConfigRecord,
        AgentPromptVersionRecord,
        AgentResearchRunRecord,
    )
    from nexus.persistence.models.data import DataSnapshot
    from unit.agents.test_monitoring_producer import produce

    captured = {}
    original, outputs, _ = await produce(monkeypatch, disclosure=True, capture=captured)
    assert original.monitoring_proposal is not None and original.disclosure_proposal is not None
    request = captured["request"]
    context = captured["inputs"]["security_snapshot"]["items"][0]["agent_paper_context"]
    intent = synthesize_intent(
        AgentResearchRunRecord(id=UUID(int=3), **request.model_dump()),
        "ABC",
        DataSnapshot(
            id=request.data_snapshot_id,
            source=SOURCE,
            schema_version=PURPOSE,
            revision="synthetic",
            payload={"purpose": PURPOSE, "independent_monitor_context": context},
        ),
        {
            role: AgentPromptVersionRecord(
                role=role, version=version, content=p.content, content_hash=p.content_hash
            )
            for role, p in captured["prompts"].items()
        },
        AgentModelConfigRecord(model_id="fixture", version=BOUNDED_STAGE_BUDGET_CONFIG_VERSION),
        captured["evidence"],
        outputs,
        price_authority_valid=False,
    )
    assert intent.monitoring_proposal is None
    assert intent.disclosure_proposal == original.disclosure_proposal
    assert intent.daily_pending_proposal is None


@pytest.mark.asyncio
@pytest.mark.parametrize("prior_mode", ["empty", "max"])
@pytest.mark.parametrize(
    "prior_text",
    ["\x00" * 400, "".join(chr(0x10000 + i) for i in range(400))],
    ids=["c0", "distinct-four-byte"],
)
async def test_independent_capacity_bounds_sec_and_price_prior_outputs_before_sdk(
    monkeypatch, prior_mode, prior_text
):
    from nexus.agents import codex_capacity
    from nexus.agents.codex_sdk import codex_request_wire
    from nexus.agents.codex_staged import STAGES
    from nexus.agents.committee import ROLE_ORDER
    from nexus.agents.independent_monitor import INSTRUCTIONS, PROMPT_VERSION
    from nexus.monitoring.independent_context import IndependentMonitorContext
    from unit.agents.test_full_input_capacity_v9 import committee
    from unit.agents.test_monitoring_producer import produce

    captured = {}
    await produce(monkeypatch, disclosure=True, capture=captured)
    prompts = {
        role: p.model_copy(
            update=dict(
                version=PROMPT_VERSION,
                content=p.content + "\n\n" + INSTRUCTIONS,
                content_hash=hashlib.sha256(
                    (p.content + "\n\n" + INSTRUCTIONS).encode()
                ).hexdigest(),
            )
        )
        for role, p in captured["prompts"].items()
    }
    model = committee(prompts=prompts)
    request = captured["request"]
    value = context_payload()
    value.update(
        instrument_id=str(request.instrument_id),
        candidate_ids=[str(request.instrument_id)],
        decision_time=request.decision_time.isoformat(),
    )
    value["disclosure_monitoring"] = captured["inputs"]["security_snapshot"]["items"][0][
        "agent_paper_context"
    ]["disclosure_monitoring"]
    context = IndependentMonitorContext.model_validate_json(json.dumps(value))
    inputs = {
        "security_snapshot": {
            "purpose": PURPOSE,
            "items": [],
            "independent_monitor_context": context.model_dump(mode="json"),
        },
        "financial_facts": {"items": []},
    }
    evidence = captured["evidence"]
    envelope = codex_capacity.candidate_capacity_envelope(
        model, request=request, evidence_refs=evidence, inputs=inputs
    )
    maximum = codex_capacity._monitoring_capacity_view(model, evidence, inputs)
    assert maximum._outputs["market_state"].disclosure_proposal is not None
    assert maximum._outputs["market_state"].monitoring_proposal is not None
    for role in ("bear", "evidence_audit"):
        assert maximum._outputs[role].disclosure_review is not None
    for stage, roles in STAGES:
        previous = (
            [
                dict(
                    claim_id=f"{role}:{index}",
                    role=role,
                    vote="abstain",
                    text=prior_text,
                    citations=[len(evidence) - 1] * 3,
                )
                for role in ROLE_ORDER[: ROLE_ORDER.index(roles[0])]
                for index in range(2)
            ]
            if prior_mode == "max"
            else []
        )
        payload, _ = maximum.build_stage_request(
            stage=stage,
            request=request,
            evidence_refs=evidence,
            frozen_inputs=inputs,
            previous=previous,
            financial_table_candidate=True,
        )
        bound = next(row for row in envelope["stages"] if row["stage"] == stage)
        assert len(codex_request_wire(payload).encode()) <= bound["wire_upper_bytes"]
    assert model._outputs == {} and model.usage.provider_calls == 0


@pytest.mark.asyncio
async def test_native_independent_sec_reference_is_required_for_exact_audited_adoption(monkeypatch):
    from nexus.monitoring.disclosure_contracts import compile_disclosure_proposal
    from unit.agents.test_monitoring_producer import produce

    captured = {}
    intent, outputs, _ = await produce(monkeypatch, disclosure=True, capture=captured)
    original = intent.disclosure_proposal
    native_ref = captured["evidence"][1].model_copy(update={"source": SOURCE})
    proposal = original.model_copy(update={"evidence_refs": (native_ref,)})
    reviews = {
        r.role: r.disclosure_review.model_copy(
            update={"proposal_hash": proposal.content_hash, "evidence_refs": (native_ref,)}
        )
        for r in outputs
        if r.role in {"bear", "evidence_audit"}
    }
    args = dict(
        context=captured["inputs"]["security_snapshot"]["items"][0]["agent_paper_context"][
            "disclosure_monitoring"
        ],
        instrument_id=intent.instrument_id,
        now=intent.decision_time,
        evidence_refs=(captured["evidence"][0], native_ref),
        audit_vote="support",
        adoption_hash=proposal.content_hash,
    )
    assert compile_disclosure_proposal(proposal, reviews, **args) == proposal
    with pytest.raises(ValueError, match="DISCLOSURE_PROPOSAL_NOT_APPROVED"):
        compile_disclosure_proposal(original, reviews, **args)
    with pytest.raises(ValueError, match="DISCLOSURE_PROPOSAL_NOT_APPROVED"):
        compile_disclosure_proposal(proposal, reviews, **(args | {"audit_vote": "oppose"}))


@pytest.mark.asyncio
async def test_compact_independent_version_is_explicit_and_preserves_legacy_wire(monkeypatch):
    from nexus.agents.codex_sdk import codex_request_wire
    from nexus.agents.independent_monitor import INSTRUCTIONS
    from nexus.monitoring.independent_context import IndependentMonitorContext
    from unit.agents.test_full_input_capacity_v9 import committee
    from unit.agents.test_monitoring_producer import produce

    captured = {}
    await produce(monkeypatch, disclosure=True, capture=captured)
    prompts = {
        role: p.model_copy(
            update={
                "version": "independent-monitor-research-v1",
                "content": p.content + "\n\n" + INSTRUCTIONS,
                "content_hash": hashlib.sha256(
                    (p.content + "\n\n" + INSTRUCTIONS).encode()
                ).hexdigest(),
            }
        )
        for role, p in captured["prompts"].items()
    }
    request = captured["request"]
    value = context_payload()
    value.update(
        instrument_id=str(request.instrument_id),
        candidate_ids=[str(request.instrument_id)],
        decision_time=request.decision_time.isoformat(),
    )
    value["disclosure_monitoring"] = captured["inputs"]["security_snapshot"]["items"][0][
        "agent_paper_context"
    ]["disclosure_monitoring"]
    context = IndependentMonitorContext.model_validate_json(json.dumps(value))
    inputs = {
        "security_snapshot": {
            "purpose": PURPOSE,
            "items": [],
            "independent_monitor_context": context.model_dump(mode="json"),
        },
        "financial_facts": {"items": []},
    }
    kwargs = dict(
        stage="research",
        request=request,
        evidence_refs=captured["evidence"],
        frozen_inputs=inputs,
        previous=[],
        financial_table_candidate=True,
    )
    legacy = committee(prompts=prompts)
    old, old_schema = legacy.build_stage_request(**kwargs)
    original = codex_request_wire(old)
    compact = committee(
        prompts={
            role: p.model_copy(update={"version": "independent-monitor-research-v2"})
            for role, p in prompts.items()
        }
    )
    new, new_schema = compact.build_stage_request(**kwargs)
    from nexus.agents.independent_input_view import restore_independent_input

    assert new["$iv2"]["version"] == 2
    assert restore_independent_input(new) == old
    assert new_schema.model_json_schema() == old_schema.model_json_schema()
    assert codex_request_wire(legacy.build_stage_request(**kwargs)[0]) == original
    assert "$iv2" not in old
