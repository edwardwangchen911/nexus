"""Formal consumers reject mixed/non-boolean modes before connection or mutation."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.paper.agent_cycle import resume_schwab_agent_paper_cycle
from nexus.paper.agent_decision_store import load_portfolio_proposal, persist_portfolio_proposal
from nexus.paper.service import load_paper_order_batch_result
from nexus.paper.target_service import activate_portfolio_target

AT = datetime(2026, 10, 9, 14, tzinfo=UTC)
KEY = UUID(int=1)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"formal_stage_capacity": 1},
        {"formal_stage_capacity": "true"},
        {"formal_stage_capacity": True, "candidate_stage_capacity": True},
        {"formal_stage_capacity": True, "candidate_stage_capacity": 0},
    ],
)
@pytest.mark.parametrize(
    "entry,options",
    [
        (resume_schwab_agent_paper_cycle, dict(cycle_id=KEY, idempotency_key="test", now=AT)),
        (load_portfolio_proposal, dict(account_id=KEY, decision_id=KEY)),
        (persist_portfolio_proposal, dict(cycle_id=KEY, synthesis_output_id=KEY, now=AT)),
        (
            activate_portfolio_target,
            dict(account_id=KEY, strategy_decision_id=KEY, request_time=AT, idempotency_key="test"),
        ),
        (load_paper_order_batch_result, dict(account_id=KEY, strategy_decision_id=KEY)),
    ],
)
async def test_formal_consumer_arguments_fail_before_queries(entry, options, changes):
    session = AsyncMock()
    with pytest.raises(ValueError, match="CODEX_FORMAL_CYCLE_PROFILE_INVALID"):
        await entry(session, **(options | changes))
    assert session.mock_calls == []
