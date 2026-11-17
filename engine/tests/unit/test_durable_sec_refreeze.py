from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.monitoring.durable_sec_refreeze import batch_unsubmitted, require_current_batch


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,run,increment,allowed",
    [
        ("pending", None, None, True),
        ("blocked", None, None, True),
        ("unknown", None, None, False),
        ("pending", UUID(int=3), None, False),
        ("pending", None, UUID(int=4), False),
        ("completed", UUID(int=3), UUID(int=4), False),
    ],
)
async def test_only_no_run_known_unsubmitted_batch_can_refreeze(status, run, increment, allowed):
    session = SimpleNamespace(
        scalar=AsyncMock(return_value=increment),
        scalars=AsyncMock(return_value=[SimpleNamespace(status=status, research_run_id=run)]),
    )
    assert await batch_unsubmitted(session, UUID(int=1)) is allowed


@pytest.mark.asyncio
async def test_superseded_is_a_current_permission_failure_not_a_mutation():
    session = SimpleNamespace(scalar=AsyncMock(return_value=UUID(int=7)))
    with pytest.raises(ValueError, match="DURABLE_SEC_RESEARCH_BATCH_SUPERSEDED"):
        await require_current_batch(session, UUID(int=1))
