"""Malformed or mixed formal cycle requests fail before any persistence access."""

from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.paper.agent_cycle import (
    _admit_cycle_research,
    _start_cycle,
    start_schwab_agent_paper_cycle,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", [start_schwab_agent_paper_cycle, _start_cycle])
@pytest.mark.parametrize(
    "change",
    [
        {"formal_stage_capacity": 1},
        {"formal_stage_capacity": "true"},
        {"formal_model_config_id": None},
        {"formal_model_config_id": "invalid"},
        {"candidate_stage_capacity": True},
        {"candidate_stage_capacity": 0},
        {"formal_stage_capacity": False},
    ],
)
async def test_formal_cycle_invalid_arguments_do_not_read_or_write(entry, change):
    session = AsyncMock()
    options = dict(
        authority=None,
        decision_slot_at=None,
        idempotency_key="bad-formal-cycle",
        requested_instrument_ids=(),
        candidate_batch_id=UUID(int=1),
        formal_stage_capacity=True,
        formal_model_config_id=UUID(int=2),
    )
    if entry is _start_cycle:
        options.pop("idempotency_key")
        options["daily_limit_usd"] = 1
    with pytest.raises(ValueError, match="CODEX_FORMAL_CYCLE_PROFILE_INVALID"):
        await entry(session, **(options | change))
    assert session.mock_calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "entry", [start_schwab_agent_paper_cycle, _start_cycle, _admit_cycle_research]
)
@pytest.mark.parametrize(
    "version_arg", ["version_lane", "version_prompt_id", "version_source_run_id"]
)
async def test_formal_cycle_cannot_inherit_evaluation_arguments(entry, version_arg):
    session = AsyncMock()
    if entry is _admit_cycle_research:
        options = dict(cycle=None, authority=None, projection=None, daily_limit_usd=1)
    else:
        options = dict(
            authority=None,
            decision_slot_at=None,
            requested_instrument_ids=(),
            candidate_batch_id=UUID(int=1),
            formal_model_config_id=UUID(int=2),
        )
        if entry is _start_cycle:
            options["daily_limit_usd"] = 1
        else:
            options["idempotency_key"] = "no-inherit"
    options.update(formal_stage_capacity=True)
    options[version_arg] = "incumbent" if version_arg == "version_lane" else UUID(int=3)
    with pytest.raises(ValueError, match="CODEX_FORMAL_CYCLE_PROFILE_INVALID"):
        await entry(session, **options)
    assert session.mock_calls == []
