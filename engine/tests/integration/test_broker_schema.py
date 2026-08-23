import pytest
from nexus.persistence.models.portfolio import (
    Account,
    AccountExternalIdEnvelope,
    BrokerCashFlowWatermark,
    BrokerSyncRun,
    PortfolioSnapshotRecord,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text


def test_account_and_external_id_envelope_constraints_are_declared() -> None:
    assert Account.__table__.name == "accounts"
    constraints = {
        constraint.name for constraint in AccountExternalIdEnvelope.__table__.constraints
    }
    assert "ck_account_external_id_envelopes_aad_version_allowed" in constraints
    assert "uq_account_external_id_envelopes_version" in constraints


@pytest.mark.asyncio
async def test_account_schema_migration_is_runnable(migrated_database_url: str) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        tables = set(
            await session.scalars(
                text(
                    "SELECT tablename FROM pg_tables WHERE schemaname = 'public' "
                    "AND tablename IN ('accounts', 'account_external_id_envelopes')"
                )
            )
        )
    assert tables == {"accounts", "account_external_id_envelopes"}


def test_sync_and_snapshot_models_declare_active_and_stale_invariants() -> None:
    assert BrokerSyncRun.__table__.name == "broker_sync_runs"
    constraint_names = {
        constraint.name for constraint in PortfolioSnapshotRecord.__table__.constraints
    }
    assert "ck_portfolio_snapshots_stale_source_required" in constraint_names


@pytest.mark.asyncio
async def test_sync_snapshot_schema_migration_is_runnable(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        tables = set(
            await session.scalars(
                text(
                    "SELECT tablename FROM pg_tables WHERE schemaname = 'public' "
                    "AND tablename IN ('broker_sync_runs', 'portfolio_snapshots', "
                    "'cash_snapshots', 'position_snapshots', 'broker_order_snapshots')"
                )
            )
        )
    assert tables == {
        "broker_sync_runs",
        "portfolio_snapshots",
        "cash_snapshots",
        "position_snapshots",
        "broker_order_snapshots",
    }


@pytest.mark.asyncio
async def test_cashflow_schema_and_monotonic_trigger_are_runnable(
    migrated_database_url: str,
) -> None:
    assert BrokerCashFlowWatermark.__table__.name == "broker_cash_flow_watermarks"
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        triggers = set(
            await session.scalars(
                text(
                    "SELECT trigger_name FROM information_schema.triggers "
                    "WHERE trigger_name IN ('trg_broker_cash_flow_watermarks_monotonic', "
                    "'trg_provider_capabilities_scope_lock')"
                )
            )
        )
    assert triggers == {
        "trg_broker_cash_flow_watermarks_monotonic",
        "trg_provider_capabilities_scope_lock",
    }
