"""Formal capacity uses reviewed actionable semantics, without granting execution."""

import pytest
from nexus.agents.codex_versions import FORMAL_STAGE_CAPACITY_CONFIG_VERSION
from nexus.agents.synthesis import staged_intent_semantics_valid
from unit.agents.test_actionable_research import _case


@pytest.mark.parametrize("valid", [True, False])
def test_formal_stage_semantics_require_structured_review(valid):
    intent = _case()["intent"]
    if not valid:
        intent = intent.model_copy(update={"synthesis_assessment": None})
    assert staged_intent_semantics_valid(intent, FORMAL_STAGE_CAPACITY_CONFIG_VERSION) is valid


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "flags",
    [
        {"formal_stage_capacity": 1},
        {"formal_stage_capacity": "true"},
        {"formal_stage_capacity": True, "candidate_stage_capacity": True},
        {"formal_stage_capacity": True, "candidate_stage_capacity": 0},
    ],
)
async def test_formal_synthesis_invalid_modes_precede_reads(flags):
    from unittest.mock import AsyncMock

    from nexus.agents.portfolio_synthesis_persistence import admit_portfolio_synthesis

    session = AsyncMock()
    with pytest.raises(ValueError, match="CODEX_FORMAL_CYCLE_PROFILE_INVALID"):
        await admit_portfolio_synthesis(
            session,
            cycle_id=None,
            research_output_ids=(),
            active_target_version_id=None,
            projection_hash="",
            daily_limit_usd=0,
            now=None,
            **flags,
        )
    assert session.mock_calls == []
