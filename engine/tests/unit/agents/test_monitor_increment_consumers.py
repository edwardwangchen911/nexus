"""Monitor-only consumers preserve original context and single-security scope."""

import hashlib
import json
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from nexus.agents import codex_staged, database_tools, execution_authority
from nexus.agents.committee import load_portfolio_synthesis_prompt
from nexus.agents.trailing_valuation import _decision_quote
from nexus.monitoring.context import FrozenMonitorContextStatus as Status
from unit.agents.test_codex_sdk import _authority, _restore_view, _tools

SOURCE = "nexus_price_monitor_increment"
PURPOSE = "price_monitor_increment_v1"
AT = datetime(2026, 10, 24, 14, tzinfo=UTC)


@pytest.fixture
def context_contract(monkeypatch):
    from nexus.monitoring import context

    monkeypatch.setattr(context, "read_monitor_context", AsyncMock(return_value=None))
    monkeypatch.setattr(context, "revalidate_monitor_context", AsyncMock(return_value=Status.VALID))
    return context


def monitor_document():
    return {
        "instrument_id": str(UUID(int=1)),
        "decision_time": AT.isoformat(),
        "parent_output_hash": "a" * 64,
        "parent_intent": {"theses": ["原论点，尚待复核"]},
        "trigger_checks": [{"cause": "initial_match", "observation": {"bid": "90"}}],
        "decision_quote": {
            "source": "schwab",
            "feed": "l1",
            "delivery_mode": "real_time",
            "evidence_status": "verified",
            "price_basis": "unadjusted_bid_ask_midpoint",
            "currency": "USD",
            "bid": "99",
            "ask": "101",
            "observed_at": AT.isoformat(),
            "available_at": AT.isoformat(),
            "received_at": AT.isoformat(),
        },
    }


def test_increment_valuation_uses_current_quote_not_parent_or_trigger(context_contract):
    value = monitor_document()
    security = {
        "status": "ok",
        "purpose": PURPOSE,
        "items": [
            {"source": SOURCE, "evidence_ref_id": "monitor-ref", "monitor_context": value},
        ],
    }
    assert _decision_quote(security) == (99, 101, AT.isoformat(), "monitor-ref")
    value["decision_quote"]["price_basis"] = "trade"
    assert _decision_quote(security) is None


def test_increment_wire_is_single_security_and_preserves_parent_and_trigger(context_contract):
    request, evidence, prompts = _authority()
    prompts = dict(prompts)
    prompts["portfolio"] = load_portfolio_synthesis_prompt(
        security_research_scope=True,
        claim_review_scope=True,
        inference_review_scope=True,
        monitoring_scope=True,
    )
    committee = codex_staged.StructuredStagedCodexCommittee(
        prompts=prompts,
        tools=_tools(),
        paper_stage_scope=True,
    )
    value = monitor_document()
    inputs = {
        "security_snapshot": {
            "purpose": PURPOSE,
            "items": [
                {"source": SOURCE, "monitor_context": value},
            ],
        }
    }
    payload, _ = committee.build_stage_request(
        stage="research",
        request=request,
        evidence_refs=evidence,
        frozen_inputs=inputs,
        previous=[],
    )
    payload = _restore_view(payload)
    assert "single-security" in payload["instructions"]
    assert "parent_intent" in payload["instructions"]
    assert "trigger_checks" in payload["instructions"]
    assert "not execution" in payload["instructions"]
    assert payload["frozen_inputs"] == inputs


@pytest.mark.parametrize("bad", ["legacy_prompt", "missing", "paper_context", "wrong_scope"])
def test_increment_wire_rejects_ambiguous_or_legacy_scope(context_contract, bad):
    request, evidence, prompts = _authority()
    prompts = dict(prompts)
    if bad != "legacy_prompt":
        prompts["portfolio"] = load_portfolio_synthesis_prompt(
            security_research_scope=True,
            claim_review_scope=True,
            inference_review_scope=True,
            monitoring_scope=True,
        )
    committee = codex_staged.StructuredStagedCodexCommittee(
        prompts=prompts,
        tools=_tools(),
        paper_stage_scope=bad != "wrong_scope",
    )
    item = {"source": SOURCE, "monitor_context": monitor_document()}
    if bad == "paper_context":
        item["agent_paper_context"] = {}
    inputs = {
        "security_snapshot": {"purpose": PURPOSE, "items": [] if bad == "missing" else [item]}
    }
    with pytest.raises(ValueError, match="^MONITOR_CONTEXT_SCOPE_INVALID$"):
        committee.build_stage_request(
            stage="research",
            request=request,
            evidence_refs=evidence,
            frozen_inputs=inputs,
            previous=[],
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("available", [True, False])
async def test_security_tool_projects_only_bound_monitor_original(
    monkeypatch, context_contract, available
):
    document = monitor_document()
    evidence = SimpleNamespace(
        id=UUID(int=4),
        source=SOURCE,
        provider_object_id="monitor:1",
        revision="a" * 64,
        available_at=AT,
        content_hash="a" * 64,
    )
    context = SimpleNamespace(model_dump=lambda **_: document)
    context_contract.read_monitor_context.return_value = (context, evidence) if available else None
    session = SimpleNamespace(
        get=AsyncMock(return_value=SimpleNamespace(payload={"purpose": PURPOSE}))
    )
    run = SimpleNamespace(
        id=UUID(int=3),
        instrument_id=UUID(int=1),
        data_snapshot_id=UUID(int=2),
        decision_time=AT,
        evidence_hash="a" * 64,
    )
    bound = database_tools._BoundDatabaseExecutors(
        session,
        run,
        (),
        (evidence,),
        allow_fixture_price_evidence=False,
        portfolio_context=None,
    )
    monkeypatch.setattr(bound, "_require_snapshot_scope", AsyncMock())
    monkeypatch.setattr(bound, "_current_evidence_authority_valid", AsyncMock(return_value=True))
    for name in (
        "read_distributions",
        "read_fund_observations",
        "read_nav_histories",
        "read_action_observations",
        "read_company_profiles",
        "read_security_classifications",
        "read_identity_bindings",
        "read_news_evidence",
    ):
        monkeypatch.setattr(database_tools, name, AsyncMock(return_value={}))
    monkeypatch.setattr(
        database_tools, "read_agent_paper_research_context", AsyncMock(return_value=None)
    )
    result = await bound.security_snapshot({})
    if available:
        assert result["purpose"] == PURPOSE
        assert result["items"][0]["monitor_context"] == document
        assert "agent_paper_context" not in result["items"][0]
    else:
        assert result["status"] == "unavailable"


@pytest.mark.asyncio
async def test_frozen_monitor_reference_revalidates_original_not_only_its_hash(context_contract):
    row = SimpleNamespace(
        id=UUID(int=4),
        source=SOURCE,
        provider_object_id="monitor:1",
        revision="a" * 64,
        available_at=AT,
        location="memory://monitor",
        content_hash="a" * 64,
    )
    manifest = [
        {
            "id": str(row.id),
            "source": row.source,
            "provider_object_id": row.provider_object_id,
            "revision": row.revision,
            "available_at": AT.isoformat().replace("+00:00", "Z"),
            "location": row.location,
            "content_hash": row.content_hash,
        }
    ]
    run = SimpleNamespace(
        evidence_manifest=manifest,
        decision_time=AT,
        evidence_hash=hashlib.sha256(
            json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    )
    session = SimpleNamespace(scalars=AsyncMock(return_value=[row]))
    context_contract.revalidate_monitor_context.return_value = Status.INVALID
    assert (
        await execution_authority.load_frozen_evidence(session, run, allow_fixture_evidence=False)
        == ()
    )
    context_contract.revalidate_monitor_context.assert_awaited_once()


@pytest.mark.asyncio
async def test_increment_capacity_requires_binding_before_committee_and_single_member(
    monkeypatch, context_contract
):
    from nexus import worker
    from nexus.agents import codex_capacity
    from nexus.monitoring import admission, capacity

    request, evidence, prompts = _authority()
    run = SimpleNamespace(
        id=UUID(int=3),
        instrument_id=request.instrument_id,
        data_snapshot_id=request.data_snapshot_id,
        decision_time=request.decision_time,
    )
    monkeypatch.setattr(
        admission,
        "require_increment_binding",
        AsyncMock(side_effect=ValueError("MONITOR_BINDING_INVALID")),
    )
    committee = codex_staged.StructuredStagedCodexCommittee(prompts=prompts, tools=_tools())
    factory = AsyncMock(return_value=committee)
    monkeypatch.setattr(worker, "_committee", factory)
    with pytest.raises(ValueError, match="MONITOR_BINDING_INVALID"):
        await capacity.prepare_monitor_capacity_pool(None, None, run, deadline=1000)
    factory.assert_not_awaited()
    admission.require_increment_binding.side_effect = None
    admission.require_increment_binding.return_value = SimpleNamespace(
        run_id=run.id, instrument_id=run.instrument_id, research_as_of=run.decision_time
    )
    monkeypatch.setattr(
        capacity, "load_frozen_evidence", AsyncMock(return_value=(SimpleNamespace(),))
    )
    monkeypatch.setattr(capacity, "domain_evidence", lambda _: evidence[0])
    preflight = AsyncMock()
    monkeypatch.setattr(codex_capacity, "preflight_capacity_pool", preflight)
    result = await capacity.prepare_monitor_capacity_pool(None, None, run, deadline=1000)
    assert result is committee
    factory.assert_awaited_once()
    assert factory.await_args.kwargs["formal_stage_capacity"] is True
    members = preflight.await_args.args[0]
    assert len(members) == 1
    assert preflight.await_args.kwargs["expected_instrument_ids"] == (run.instrument_id,)


@pytest.mark.asyncio
@pytest.mark.parametrize("formal", [True, False])
async def test_normal_capacity_routes_only_explicit_monitor_formal_scope(monkeypatch, formal):
    from nexus.agents import capacity_cycle, codex_candidate
    from nexus.agents.codex_versions import BOUNDED_STAGE_BUDGET_CONFIG_VERSION
    from nexus.monitoring import capacity
    from nexus.settings import EngineSettings

    run = SimpleNamespace(
        data_snapshot_id=UUID(int=2), pricing_version=BOUNDED_STAGE_BUDGET_CONFIG_VERSION
    )
    session = SimpleNamespace(
        get=AsyncMock(return_value=SimpleNamespace(payload={"purpose": PURPOSE})),
        scalar=AsyncMock(return_value="nexus_test_monitor"),
    )
    settings = EngineSettings(database_url="postgresql+asyncpg://localhost/nexus_test_monitor")
    selected = object()
    prepare = AsyncMock(return_value=selected)
    monkeypatch.setattr(capacity, "prepare_monitor_capacity_pool", prepare)
    monkeypatch.setattr(codex_candidate, "require_candidate_database", AsyncMock())
    if formal:
        assert (
            await capacity_cycle.prepare_cycle_capacity_pool(
                session,
                settings,
                run,
                deadline=1000,
                formal_stage_capacity=True,
            )
            is selected
        )
        prepare.assert_awaited_once_with(session, settings, run, deadline=1000)
    else:
        with pytest.raises(ValueError, match="^CODEX_CAPACITY_POOL_BINDING_INVALID$"):
            await capacity_cycle.prepare_cycle_capacity_pool(session, settings, run, deadline=1000)
        prepare.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "initial,final",
    [
        (Status.INVALID, Status.INVALID),
        (Status.VALID, Status.INVALID),
        (Status.VALID, Status.MISSING),
        (Status.MISSING, Status.VALID),
        (Status.VALID, Status.VALID),
        (Status.MISSING, Status.MISSING),
    ],
)
async def test_execution_revalidates_monitor_before_and_after_roles(
    monkeypatch, context_contract, initial, final
):
    from nexus.agents import execution
    from nexus.agents.committee import FixtureCommittee
    from nexus.agents.paper_context import FrozenAgentPaperContextStatus
    from nexus.agents.price_authority import FrozenPriceAuthorityStatus
    from nexus.persistence.models.agents import AgentResearchRunRecord

    request, evidence, prompts = _authority()
    run = SimpleNamespace(
        id=UUID(int=3),
        status="prepared",
        model_config_id=UUID(int=5),
        instrument_id=request.instrument_id,
        data_snapshot_id=request.data_snapshot_id,
        decision_time=request.decision_time,
    )

    async def get(model, *args, **kwargs):
        return run if model is AgentResearchRunRecord else SimpleNamespace()

    session = SimpleNamespace(get=get, flush=AsyncMock())
    row = SimpleNamespace(source=SOURCE)
    for name in ("_append_event", "_append_output", "_advance", "require_reserved_daily_budget"):
        monkeypatch.setattr(execution, name, AsyncMock())
    monkeypatch.setattr(execution, "load_frozen_evidence", AsyncMock(return_value=(row,)))
    monkeypatch.setattr(execution, "domain_evidence", lambda _: evidence[0])
    monkeypatch.setattr(execution, "load_frozen_symbol", AsyncMock(return_value="SYNTH"))
    monkeypatch.setattr(execution, "load_frozen_prompts", AsyncMock(return_value=prompts))
    monkeypatch.setattr(
        execution,
        "revalidate_frozen_price_authority",
        AsyncMock(return_value=FrozenPriceAuthorityStatus.MISSING),
    )
    monkeypatch.setattr(
        execution,
        "revalidate_frozen_agent_paper_context",
        AsyncMock(return_value=FrozenAgentPaperContextStatus.MISSING),
    )
    context_contract.revalidate_monitor_context.side_effect = [initial, final]
    stopped_before, stopped_after = (
        AsyncMock(return_value="before"),
        AsyncMock(return_value="after"),
    )
    monkeypatch.setattr(execution, "_terminal_without_usage", stopped_before)
    monkeypatch.setattr(execution, "_terminal_after_committee", stopped_after)
    fixture = FixtureCommittee()

    async def analyze(_session, _run, *, role, prompt, **kwargs):
        return await fixture.analyze(role, request, evidence, prompt)

    analyzer = AsyncMock(side_effect=analyze)
    monkeypatch.setattr(execution, "_analyze_with_one_retry", analyzer)
    monkeypatch.setattr(execution, "_settle_or_hold_usage", AsyncMock(return_value=True))
    monkeypatch.setattr(execution, "_committee_usage", lambda *_: None)
    monkeypatch.setattr(execution, "read_portfolio_context", AsyncMock(return_value=None))
    monkeypatch.setattr(execution, "_bind_final_evidence", lambda *_: None)
    synthesized = Mock(side_effect=RuntimeError("synthesis reached"))
    monkeypatch.setattr(execution, "synthesize_intent", synthesized)
    if initial is Status.INVALID:
        assert (
            await execution.execute_research_run(session, run_id=run.id, committee=fixture)
            == "before"
        )
        assert stopped_before.await_args.kwargs["reason"] == "monitor_context_invalid"
        analyzer.assert_not_awaited()
    elif final is Status.INVALID or final is not initial:
        assert (
            await execution.execute_research_run(session, run_id=run.id, committee=fixture)
            == "after"
        )
        assert stopped_after.await_args.kwargs["reason"] == "monitor_context_invalid"
        assert analyzer.await_count == 8
        synthesized.assert_not_called()
    else:
        with pytest.raises(RuntimeError, match="synthesis reached"):
            await execution.execute_research_run(session, run_id=run.id, committee=fixture)
        assert synthesized.call_args.kwargs["price_authority_valid"] is (final is Status.VALID)
