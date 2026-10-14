import pytest
from integration.test_formal_history_seal_pg import (
    test_formal_failure_seal_keeps_unknown_usage_and_unlocks_only_settled_account as formal_case,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("version", ["v9", "v10"])
@pytest.mark.parametrize(
    "fault",
    [
        "missing-invocation",
        "live-lease",
        "missing-confirmation",
        "cycle-binding-hash",
        "cycle-formal-flag",
        "future-clock",
        "default-date-gate",
        "prepared-run",
        "process-live",
    ],
)
async def test_same_day_close_preserves_unknown_usage_and_all_existing_guards(
    migrated_database_url, monkeypatch, version, fault
):
    await formal_case(migrated_database_url, monkeypatch, version, fault, same_day=True)
