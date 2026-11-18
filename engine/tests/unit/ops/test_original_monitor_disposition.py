from datetime import UTC, datetime
from uuid import uuid4

import pytest


@pytest.mark.asyncio
async def test_unrelated_member_cannot_use_original_occupancy_disposition():
    from nexus.monitoring import original_occupancy

    class Unavailable:
        def __getattr__(self, name):
            raise AssertionError(f"Unrelated member accessed {name}")

    assert not await original_occupancy.accepted(
        Unavailable(), run_id=uuid4(), account_id=uuid4(), now=datetime.now(UTC)
    )
