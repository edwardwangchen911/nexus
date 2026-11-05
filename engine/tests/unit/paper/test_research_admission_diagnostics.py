from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from nexus.agents import persistence
from nexus.agents.research_stages import routes
from nexus.paper import agent_cycle


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "phase,code,expected",
    [
        ("prompt_selection", "private response secret=canary", "UNKNOWN"),
        (
            "snapshot_preparation",
            "AGENT_PAPER_RESEARCH_CONTEXT_INVALID",
            "AGENT_PAPER_RESEARCH_CONTEXT_INVALID",
        ),
        (
            "research_admission",
            "SINGLE_SOL_NATURAL_BUDGET_PROTECTED",
            "SINGLE_SOL_NATURAL_BUDGET_PROTECTED",
        ),
        ("stage_registration", "PLATFORM_NATIVE_INPUT_CHANGED", "PLATFORM_NATIVE_INPUT_CHANGED"),
    ],
)
async def test_admission_failure_retains_safe_stage_only_after_pool_rollback(
    monkeypatch, phase, code, expected
):
    candidate = agent_cycle.CandidateManifestEntry(
        instrument_id=UUID(int=2),
        symbol="AAPL",
        inclusion_reason="fixture",
        research_snapshot_id=UUID(int=3),
        prior_daily_price_authority_id=UUID(int=4),
        decision_quote_id=UUID(int=5),
        decision_trade_id=None,
        decision_artifact_id=UUID(int=6),
        decision_evidence_hash="a" * 64,
        risk_query_authority_ids=(UUID(int=7), UUID(int=8), UUID(int=9)),
    )
    cycle = SimpleNamespace(
        id=UUID(int=1), candidate_manifest=[], decision_price_policy_id=UUID(int=10)
    )
    rolled_back = []

    @asynccontextmanager
    async def nested():
        try:
            yield
        except ValueError:
            rolled_back.append(True)
            raise

    async def get(model, identity, **kwargs):
        if identity == cycle.id:
            return cycle
        return SimpleNamespace(prompt_manifest={"portfolio": {}})

    session = SimpleNamespace(begin_nested=nested, get=AsyncMock(side_effect=get))
    monkeypatch.setattr(agent_cycle, "parse_candidate_manifest", lambda *a, **k: (candidate,))
    monkeypatch.setattr(
        agent_cycle, "_require_cycle_candidate_profile", AsyncMock(return_value=UUID(int=11))
    )
    prompt = AsyncMock(return_value=(None, None, None))
    snapshot = AsyncMock(return_value=SimpleNamespace(id=UUID(int=3)))
    admit = AsyncMock(return_value=SimpleNamespace(run_id=UUID(int=12)))
    register = AsyncMock()
    operations = {
        "prompt_selection": prompt,
        "snapshot_preparation": snapshot,
        "research_admission": admit,
        "stage_registration": register,
    }
    operations[phase].side_effect = ValueError(code)
    monkeypatch.setattr(persistence, "resolve_research_prompt_authority", prompt)
    monkeypatch.setattr(agent_cycle, "prepare_agent_paper_research_snapshot", snapshot)
    monkeypatch.setattr(agent_cycle, "admit_agent_paper_research", admit)
    monkeypatch.setattr(routes, "register_new_prepared_runs", register)
    facts = []

    async def append(_session, _cycle, before, after, result_code, details):
        if result_code == "RESEARCH_ADMISSION_UNAVAILABLE":
            assert rolled_back == [True]
            assert (before, after) == ("prepared", "unavailable")
            facts.append(details)

    monkeypatch.setattr(agent_cycle, "_append_event", AsyncMock(side_effect=append))
    monkeypatch.setattr(agent_cycle, "_events", AsyncMock(return_value=()))
    monkeypatch.setattr(agent_cycle, "_result", Mock(return_value="terminal"))
    now = datetime(2026, 11, 5, 15, tzinfo=UTC)
    result = await agent_cycle._admit_cycle_research(
        session,
        cycle=cycle,
        authority=SimpleNamespace(
            decision_time=now, decision_session=now.date(), execution_session=now.date()
        ),
        projection=object(),
        daily_limit_usd=Decimal(13),
        version_evaluation_id=None,
        version_lane=None,
        version_source_run_id=None,
        version_prompt_id=None,
        version_model_id=None,
        formal_stage_capacity=True,
    )
    assert result == "terminal"
    assert len(facts) == 1
    assert facts[0]["failure_phase"] == phase
    assert facts[0]["failure_code"] == expected
    assert "canary" not in repr(facts)
    assert operations[phase].await_count == 1


@pytest.mark.parametrize(
    "error,expected",
    [
        (ValueError("SINGLE_SOL_NATURAL_BUDGET_PROTECTED"), "SINGLE_SOL_NATURAL_BUDGET_PROTECTED"),
        (ValueError("SINGLE_SOL_NATURAL_BUDGET_PROTECTED", "canary"), "UNKNOWN"),
        (ValueError("SINGLE_SOL_NATURAL_BUDGET_PROTECTED secret=canary"), "UNKNOWN"),
        (ValueError({"private": "canary"}), "UNKNOWN"),
    ],
)
def test_safe_codes_never_use_prefixes_or_arbitrary_exception_text(error, expected):
    from nexus.paper.research_admission_diagnostics import (
        admission_failure_facts,
        read_admission_failure,
    )

    facts = admission_failure_facts(error, phase="research_admission", instrument_id=None)
    assert facts["failure_code"] == expected and "canary" not in repr(facts)
    assert read_admission_failure(facts) == {
        "phase": "research_admission",
        "code": expected,
        "instrument_id": None,
    }


@pytest.mark.parametrize(
    "change",
    [
        {},
        {"admission_failure_version": True},
        {"admission_failure_version": 2},
        {"failure_code": "private-canary"},
        {"failure_phase": "private-canary"},
        {"failure_instrument_id": "private-canary"},
    ],
)
def test_missing_and_invalid_diagnostics_do_not_invent_legacy_facts(change):
    from nexus.paper.research_admission_diagnostics import (
        admission_failure_facts,
        read_admission_failure,
    )

    facts = admission_failure_facts(
        ValueError("UNKNOWN"), phase="pool_capacity", instrument_id=None
    )
    assert read_admission_failure(facts | change if change else {}) is None


@pytest.mark.parametrize(
    "stored_phase,public_phase",
    [("prompt_selection", "definition_selection"), ("prompt_binding", "definition_binding")],
)
def test_internal_stage_names_have_public_aliases_without_rewriting_frozen_facts(
    stored_phase, public_phase
):
    from nexus.api.schemas.paper import ResearchAdmissionFailureResponse
    from nexus.paper.research_admission_diagnostics import (
        admission_failure_facts,
        read_admission_failure,
    )

    facts = admission_failure_facts(
        ValueError("UNKNOWN"), phase=stored_phase, instrument_id=UUID(int=2)
    )
    original = dict(facts)
    failure = read_admission_failure(facts)
    assert failure is not None and failure["phase"] == public_phase
    response = ResearchAdmissionFailureResponse.model_validate(failure)
    assert response.phase == public_phase and response.instrument_id == UUID(int=2)
    assert response.code == "UNKNOWN"
    assert facts == original and facts["failure_phase"] == stored_phase
