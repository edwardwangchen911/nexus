"""Core migration cases, defined in one non-collected helper module."""

import asyncio
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from helpers.migration_core_cases import (
    test_0003_backfills_admission_state_after_the_released_0002_schema as test_0003_backfills_admission_state_after_the_released_0002_schema,  # noqa: E501
)
from helpers.migration_core_cases import (
    test_0003_fail_closed_backfills_disabled_admission_epoch as test_0003_fail_closed_backfills_disabled_admission_epoch,  # noqa: E501
)
from helpers.migration_core_cases import (
    test_0003_legacy_read_only_with_old_admission_fails_closed_across_round_trip as test_0003_legacy_read_only_with_old_admission_fails_closed_across_round_trip,  # noqa: E501
)
from helpers.migration_core_cases import (
    test_0005_upgrades_and_round_trips_the_committed_0004_schema as test_0005_upgrades_and_round_trips_the_committed_0004_schema,  # noqa: E501
)
from helpers.migration_core_cases import (
    test_0006_preserves_existing_admission_and_allows_same_contract_across_scopes as test_0006_preserves_existing_admission_and_allows_same_contract_across_scopes,  # noqa: E501
)
from helpers.migration_core_cases import (
    test_0006_publishes_and_exactly_removes_the_quote_domain_policy as test_0006_publishes_and_exactly_removes_the_quote_domain_policy,  # noqa: E501
)
from helpers.migration_core_cases import (
    test_initial_migration_creates_an_exact_reversible_schema as test_initial_migration_creates_an_exact_reversible_schema,  # noqa: E501
)
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


def test_0065_preserves_a_cycle_created_under_the_released_0064_schema(
    temporary_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Upgrade adds only a null link to a pre-existing immutable cycle."""
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    command.upgrade(config, "0064_prompt_release_channel")

    async def seed_or_read(*, seed: bool) -> dict[str, object]:
        engine = create_async_engine(temporary_database_url)
        try:
            async with engine.begin() as connection:
                if seed:
                    await connection.execute(
                        text(
                            "INSERT INTO universe_snapshots "
                            "(id,key,version,effective_from,source,content_hash,published_at) "
                            "VALUES "
                            "('65000000-0000-0000-0000-000000000001','fixture','v1','2026-01-01',"
                            "'fixture',repeat('a',64),'2026-01-01T00:00:00Z')"
                        )
                    )
                    await connection.execute(
                        text(
                            "INSERT INTO agent_paper_cycles "
                            "(id,account_id,portfolio_id,decision_session,decision_time,decision_slot_at,"
                            "execution_session,universe_snapshot_id,decision_price_policy_id,"
                            "execution_price_policy_id,candidate_manifest,candidate_hash,"
                            "sizing_policy_version,sizing_policy_hash,execution_locked) VALUES "
                            "('65000000-0000-0000-0000-000000000002',"
                            "'d31672c0-3922-5ebd-8fdb-cd3174266731',"
                            "'ee2ff673-eaf6-5076-bbab-f4c9cedfc74e','2026-08-26',"
                            "'2026-08-26T21:00:00Z','2026-08-26T21:00:00Z','2026-08-27',"
                            "'65000000-0000-0000-0000-000000000001',"
                            "'2573d63a-fb6d-53d3-8d44-207766a78322',"
                            "'e99532cc-e24c-52a8-8165-c8076325fbd2','[]',repeat('b',64),"
                            "'agent_paper_sizing_v1',repeat('c',64),true)"
                        )
                    )
                result = await connection.execute(text("SELECT * FROM agent_paper_cycles"))
                return dict(result.mappings().one())
        finally:
            await engine.dispose()

    before = asyncio.run(seed_or_read(seed=True))
    command.upgrade(config, "0065_portfolio_management")
    after = asyncio.run(seed_or_read(seed=False))
    assert after.pop("candidate_batch_id") is None
    assert after == before
    command.downgrade(config, "0064_prompt_release_channel")
    assert asyncio.run(seed_or_read(seed=False)) == before
