"""Normal reader derives formal proof and keeps independent content/price rejection."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from nexus.agents import codex_formal_stage, persistence
from nexus.agents.actionable_research import ActionableResearchError
from nexus.agents.codex_versions import FORMAL_STAGE_CAPACITY_CONFIG_VERSION
from nexus.agents.paper_context import FrozenAgentPaperContextStatus
from nexus.agents.price_authority import FrozenPriceAuthorityStatus
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.market import Instrument
from unit.agents.test_actionable_research import _case


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode, proof, price_valid",
    [
        (True, True, True),
        (False, True, True),
        (True, False, True),
        (True, True, False),
    ],
)
async def test_reader_consumes_only_qualified_formal_content(monkeypatch, mode, proof, price_valid):
    case = _case()
    run = case["run"]
    run.pricing_version = FORMAL_STAGE_CAPACITY_CONFIG_VERSION
    run.created_at = run.decision_time
    output = case["output"]
    output.created_at = run.decision_time
    snapshot = SimpleNamespace(
        available_at=run.decision_time,
        received_at=run.decision_time,
        source="nexus_research",
        schema_version="research_filings_v1",
        payload={},
    )

    async def get(model, *_args, **_kwargs):
        return (
            run
            if model is AgentResearchRunRecord
            else SimpleNamespace()
            if model is Instrument
            else snapshot
        )

    session = SimpleNamespace(
        get=AsyncMock(side_effect=get),
        scalars=AsyncMock(side_effect=[case["terminal_outputs"], case["bound_evidence"]]),
    )
    binding = case["bound_evidence"][0]
    evidence = SimpleNamespace(
        id=binding.evidence_ref_id,
        **{
            name: getattr(binding, name)
            for name in (
                "source",
                "provider_object_id",
                "revision",
                "available_at",
                "location",
                "content_hash",
            )
        },
    )
    qualify = AsyncMock(return_value=proof)
    monkeypatch.setattr(codex_formal_stage, "formal_stage_output_qualified", qualify)
    monkeypatch.setattr(persistence, "load_frozen_evidence", AsyncMock(return_value=[evidence]))
    monkeypatch.setattr(
        persistence, "load_frozen_symbol", AsyncMock(return_value=case["intent"].display_symbol)
    )
    monkeypatch.setattr(
        persistence,
        "revalidate_frozen_price_authority",
        AsyncMock(
            return_value=FrozenPriceAuthorityStatus.VALID
            if price_valid
            else FrozenPriceAuthorityStatus.INVALID
        ),
    )
    monkeypatch.setattr(
        persistence,
        "revalidate_frozen_agent_paper_context",
        AsyncMock(return_value=FrozenAgentPaperContextStatus.VALID),
    )
    options = dict(run_id=run.id, decision_time=case["decision_time"], formal_stage_capacity=mode)
    if mode and proof and price_valid:
        assert (
            await persistence.load_actionable_research_output(session, **options)
        ).stance == "buy_candidate"
    else:
        with pytest.raises(ActionableResearchError):
            await persistence.load_actionable_research_output(session, **options)
    if mode:
        qualify.assert_awaited_once_with(session, run.id, decision_time=case["decision_time"])
    else:
        qualify.assert_not_awaited()


@pytest.mark.asyncio
async def test_reader_rejects_mixed_modes_before_database_read():
    session = SimpleNamespace(get=AsyncMock(side_effect=AssertionError("database read")))
    with pytest.raises(ActionableResearchError):
        await persistence.load_actionable_research_output(
            session,
            run_id=_case()["run"].id,
            decision_time=_case()["decision_time"],
            formal_stage_capacity=True,
            candidate_stage_capacity=True,
        )
    session.get.assert_not_awaited()
