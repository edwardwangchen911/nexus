"""Reuse normal owner/Worker/closeout contracts with an explicitly selected new profile.

Only profile selection and provider transport are test-controlled. Real owner,
schema, SQL, shared reservations, stage protocol and terminal readers stay active.
"""

import pytest
from integration import test_sol_native_stage_pg as cases
from nexus.agents import platform_profile
from nexus.agents.platform_schema import COMPACT_PLATFORM_UNITS_VERSION
from unit.agents.test_platform_compact_output import GUIDANCE


@pytest.fixture
def compact_registration(monkeypatch):
    original = platform_profile.register_sol_business_profile

    async def selected(session, *, price_record, source_evidence):
        return await original(
            session,
            price_record=price_record,
            source_evidence=source_evidence,
            units_version=COMPACT_PLATFORM_UNITS_VERSION,
        )

    monkeypatch.setattr(platform_profile, "register_sol_business_profile", selected)
    transport = cases.install_sol_http

    def checked_transport(monkeypatch, store, **kwargs):
        callback = kwargs.get("on_request")

        async def inspect(payload):
            assert payload["instructions"].endswith(GUIDANCE)
            assert payload["instructions"].count(GUIDANCE) == 1
            if callback is not None:
                await callback(payload)

        return transport(monkeypatch, store, **(kwargs | {"on_request": inspect}))

    monkeypatch.setattr(cases, "install_sol_http", checked_transport)


@pytest.mark.asyncio
@pytest.mark.parametrize("audit_veto", [False, True])
async def test_compact_independent_three_stock_settlement_and_next_admission(
    migrated_database_url, tmp_path, monkeypatch, compact_registration, audit_veto
):
    await cases.test_normal_worker_sends_native_sol_three_stages_and_settles_original_price(
        migrated_database_url, tmp_path, monkeypatch, audit_veto
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["formal", "increment"])
async def test_compact_cycle_and_affected_stock_keep_original_owners_and_gates(
    migrated_database_url, monkeypatch, compact_registration, family
):
    if family == "formal":
        await cases.test_native_formal_worker_retains_original_cycle_and_output_gate(
            migrated_database_url, monkeypatch
        )
    else:
        await cases.test_native_increment_worker_settles_only_child_and_retains_parent(
            migrated_database_url, monkeypatch
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "timeout",
        "cache_unknown",
        "incomplete_known",
        "output_limit",
        "verification_ack_lost",
        "authorization_ack_lost",
    ],
)
async def test_compact_failure_keeps_known_settlement_unknown_holds_and_one_attempt(
    migrated_database_url, tmp_path, monkeypatch, compact_registration, fault
):
    await cases.test_native_worker_failure_never_retries_or_falls_back_and_preserves_accounting(
        migrated_database_url, tmp_path, monkeypatch, fault
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["complete", "timeout"])
async def test_compact_budgeted_single_entry_and_bff_keep_exact_previous_results(
    migrated_database_url, tmp_path, monkeypatch, capsys, compact_registration, outcome
):
    from integration.test_budgeted_single_sol_pg import (
        test_same_day_successor_uses_own_reservation_and_one_day_guard as scenario,
    )

    await scenario(migrated_database_url, tmp_path, monkeypatch, capsys, outcome)


@pytest.mark.asyncio
async def test_compact_single_request_cannot_spend_natural_slot_protection(
    migrated_database_url, tmp_path, monkeypatch, capsys, compact_registration
):
    from integration.test_budgeted_single_sol_pg import (
        test_new_request_cannot_spend_remaining_natural_obligations as scenario,
    )

    await scenario(migrated_database_url, tmp_path, monkeypatch, capsys)


@pytest.mark.asyncio
async def test_compact_natural_three_stock_cycle_and_portfolio_consume_own_protection(
    migrated_database_url, tmp_path, monkeypatch, compact_registration
):
    from integration.test_single_sol_budget_pg import (
        test_natural_research_and_synthesis_convert_their_own_native_reservations as scenario,
    )

    await scenario(migrated_database_url, monkeypatch, tmp_path, "local-budgeted-single-sol-v1")


@pytest.mark.asyncio
async def test_compact_formal_intent_keeps_frozen_model_during_normal_conversion(
    migrated_database_url, monkeypatch, compact_registration
):
    from integration.test_formal_intraday_intent_pg import (
        test_registered_intent_is_durable_without_work_or_second_start as scenario,
    )

    await scenario(migrated_database_url, monkeypatch, "sol_admit")


@pytest.mark.asyncio
@pytest.mark.parametrize("known", [False, True])
async def test_compact_orphan_settlement_uses_original_price_and_keeps_unknown(
    migrated_database_url, tmp_path, compact_registration, known
):
    await cases.test_sol_normal_and_orphan_consume_identical_original_price(
        migrated_database_url, tmp_path, orphan=True, known=known
    )
