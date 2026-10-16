"""Seal and normal admission agree on verified prior preparation dispositions."""

import pytest
from integration.test_manual_partial_history_seal_pg import (
    test_explicit_manual_seal_retains_known_lower_bound_and_closes_unused_jobs as seal_case,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("same_day", [False, True])
@pytest.mark.parametrize(
    "fault", ["valid", "unclosed", "preparing", "active", "audit-invalid", "late-claim"]
)
async def test_seal_revalidates_prior_preparation_without_settling_unknown_usage(
    migrated_database_url,
    monkeypatch,
    same_day,
    fault,
):
    await seal_case(
        migrated_database_url,
        monkeypatch,
        "research",
        same_day,
        prior_preparation=fault,
    )
