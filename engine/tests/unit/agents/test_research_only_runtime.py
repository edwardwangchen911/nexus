"""Research observations must never become current decision authority."""

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run, load_actionable_research_output
from nexus.agents.snapshots import prepare_research_snapshot

NOW = datetime(2026, 10, 22, 3, tzinfo=UTC)


@pytest.mark.asyncio
async def test_pure_snapshot_rejects_portfolio_context_before_reading():
    with pytest.raises(ValueError, match="RESEARCH_ONLY_CONTEXT_FORBIDDEN"):
        await prepare_research_snapshot(
            object(), UUID(int=1), as_of=NOW, research_only=True, portfolio_snapshot_id=UUID(int=2)
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("extra", [{}, {"formal_stage_capacity": True}, {"research_only": 1}])
async def test_pure_admission_requires_exact_formal_scope_before_access(extra):
    args = dict(research_only=True, **{k: v for k, v in extra.items() if k != "research_only"})
    if "research_only" in extra:
        args["research_only"] = extra["research_only"]
    with pytest.raises(ValueError, match="RESEARCH_ONLY_ADMISSION_ARGUMENT_INVALID"):
        await admit_research_run(
            object(),
            actor_id="test",
            route="/test",
            idempotency_key="test",
            request_hash="a" * 64,
            request=ResearchRequest(
                instrument_id=UUID(int=1), data_snapshot_id=UUID(int=2), decision_time=NOW
            ),
            prompt_version_id=None,
            model_config_id=None,
            daily_limit_usd=Decimal(1),
            now=NOW,
            **args,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("marker", ["purpose", "source", "schema", "evidence"])
async def test_formal_consumer_rejects_pure_output_before_requalification(marker, monkeypatch):
    run = SimpleNamespace(
        actor_id="fixture-research-only", data_snapshot_id=UUID(int=2), evidence_manifest=[]
    )
    snapshot = SimpleNamespace(
        source="nexus_research", schema_version="research_filings_v1", payload={}
    )
    if marker == "purpose":
        snapshot.payload["purpose"] = "research_only_v2"
    if marker == "source":
        snapshot.source = "nexus_research_only"
    if marker == "schema":
        snapshot.schema_version = "research_only_v2"
    if marker == "evidence":
        run.evidence_manifest = [{"source": "research_observation_identity"}]
    session = SimpleNamespace(get=AsyncMock(side_effect=[run, snapshot]))
    from nexus.agents.persistence import ActionableResearchError

    async def forbidden(*args, **kwargs):
        raise AssertionError("research-only reached formal qualification")

    monkeypatch.setattr("nexus.agents.codex_formal_stage.formal_stage_output_qualified", forbidden)
    with pytest.raises(ActionableResearchError):
        await load_actionable_research_output(
            session, run_id=UUID(int=1), decision_time=NOW, formal_stage_capacity=True
        )


@pytest.mark.asyncio
async def test_optimizer_direct_consumer_rejects_pure_run():
    from nexus.optimizer.formal_inputs import FormalResearchOutputReference, freeze_formal_research

    run = SimpleNamespace(
        actor_id="fixture-research-only", data_snapshot_id=UUID(int=2), evidence_manifest=[]
    )
    snapshot = SimpleNamespace(
        source="nexus_research_only",
        schema_version="research_only_v2",
        payload={"purpose": "research_only_v2"},
    )
    session = SimpleNamespace(get=AsyncMock(side_effect=[run, snapshot]))
    with pytest.raises(ValueError, match="RESEARCH_ONLY_FORMAL_CONSUMPTION_FORBIDDEN"):
        await freeze_formal_research(
            session,
            FormalResearchOutputReference(run_id=UUID(int=1), output_id=UUID(int=3)),
            portfolio_snapshot_id=UUID(int=4),
            diagnostic_report_id=UUID(int=5),
            diagnostic_input_hash="a" * 64,
            valuation_time=NOW,
            decision_cutoff_at=NOW,
        )


@pytest.mark.asyncio
async def test_frozen_observation_rejects_forged_formal_snapshot(monkeypatch):
    import hashlib
    import json

    from nexus.agents.execution_authority import load_frozen_evidence
    from nexus.persistence.models.research import ResearchEvidenceRef

    evidence = ResearchEvidenceRef(
        id=UUID(int=3),
        source="research_observation_identity",
        provider_object_id="test",
        revision="v1",
        available_at=NOW,
        created_at=NOW,
        location="memory://test",
        content_hash="a" * 64,
    )
    manifest = [
        {
            "id": str(evidence.id),
            "source": evidence.source,
            "provider_object_id": evidence.provider_object_id,
            "revision": evidence.revision,
            "available_at": NOW.isoformat().replace("+00:00", "Z"),
            "location": evidence.location,
            "content_hash": evidence.content_hash,
        }
    ]
    run = SimpleNamespace(
        actor_id="unit-test",
        evidence_manifest=manifest,
        evidence_hash=hashlib.sha256(
            json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        data_snapshot_id=UUID(int=2),
        instrument_id=UUID(int=1),
        decision_time=NOW,
    )
    snapshot = SimpleNamespace(
        source="nexus_research", schema_version="research_filings_v1", payload={}
    )
    session = SimpleNamespace(
        scalars=AsyncMock(return_value=[evidence]), get=AsyncMock(return_value=snapshot)
    )
    assert await load_frozen_evidence(session, run, allow_fixture_evidence=False) == ()


@pytest.mark.parametrize("marker", ["prompt", "evidence"])
def test_direct_actionable_consumer_rejects_observation_authority(marker):
    from nexus.agents.actionable_research import (
        ActionableResearchError,
        admit_actionable_research_output,
    )
    from unit.agents.test_actionable_research import _case

    case = _case()
    if marker == "prompt":
        case["run"].prompt_manifest = {"orchestrator": {"version": "research-only-v2-abc"}}
    else:
        case["bound_evidence"][0].source = "research_observation_identity"
    with pytest.raises(ActionableResearchError):
        admit_actionable_research_output(**case)


@pytest.mark.parametrize("stage", ["research", "critique", "synthesis"])
def test_three_stage_wire_preserves_observation_scope(stage):
    from nexus.agents.codex_staged import StructuredStagedCodexCommittee
    from unit.agents.test_codex_sdk import _authority, _tools

    request, evidence, prompts = _authority()
    committee = StructuredStagedCodexCommittee(
        prompts=prompts, tools=_tools(), inline_citations=True
    )
    payload, _ = committee.build_stage_request(
        stage=stage,
        request=request,
        evidence_refs=evidence,
        frozen_inputs={"security_snapshot": {"purpose": "research_only_v2", "items": []}},
        previous=[],
    )
    assert "remote latest status is unverified" in payload["instructions"]
    assert "No portfolio or Paper context applies" in payload["instructions"]
    assert "No allocation" in payload["instructions"]


@pytest.mark.asyncio
async def test_v11_pool_routes_pure_manifest_without_reading_paper(monkeypatch):
    from nexus.agents import capacity_cycle
    from nexus.agents.codex_versions import BOUNDED_STAGE_BUDGET_CONFIG_VERSION
    from nexus.settings import EngineSettings

    run = SimpleNamespace(
        pricing_version=BOUNDED_STAGE_BUDGET_CONFIG_VERSION,
        data_snapshot_id=UUID(int=2),
        instrument_id=UUID(int=1),
        decision_time=NOW,
    )
    snapshot = SimpleNamespace(
        source="nexus_research_only",
        schema_version="research_only_v2",
        payload={"purpose": "research_only_v2"},
    )
    session = SimpleNamespace(
        get=AsyncMock(return_value=snapshot), scalar=AsyncMock(return_value="nexus_test_pure")
    )

    async def forbidden(*args, **kwargs):
        raise AssertionError("pure research attempted Paper context")

    monkeypatch.setattr("nexus.agents.paper_context.read_agent_paper_research_context", forbidden)
    # A missing request manifest must be refused by the pure adapter, not by Paper context.
    with pytest.raises(ValueError, match="RESEARCH_ONLY_POOL_BINDING_INVALID"):
        await capacity_cycle.prepare_cycle_capacity_pool(
            session,
            EngineSettings(database_url="postgresql+asyncpg://localhost/nexus_test_pure"),
            run,
            deadline=100,
            formal_stage_capacity=True,
        )
