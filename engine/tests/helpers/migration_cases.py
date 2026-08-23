"""Reusable migration cases; collected through small domain test modules."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from nexus.persistence.models import data, market, portfolio, runtime  # noqa: F401
from nexus.quality.source_policy import (
    QUOTE_SOURCE_POLICY_ID,
    QUOTE_SOURCE_POLICY_PAYLOAD,
)
from sqlalchemy.exc import IntegrityError

from helpers.migration_helpers import (
    _assert_legacy_read_only_is_effectively_disabled,
    _assert_schema,
    _delivery_mode_rows,
    _insert_provider_capability,
    _integration_status_columns,
    _integration_status_row,
    _provider_capability_scopes,
    _quote_source_policy_record,
    _seed_0001_integration_status,
    _seed_0005_delivery_rows,
    _set_fixture_delivery_mode,
    _table_columns,
    _table_names,
)

EXPECTED_TABLES = {
    "provider_runs",
    "provider_capabilities",
    "source_policies",
    "data_snapshots",
    "quality_reports",
    "data_artifacts",
    "price_series_manifests",
    "macro_observations",
    "integration_status",
    "jobs",
    "job_leases",
    "outbox_events",
    "idempotency_records",
    "audit_events",
    "used_nonces",
    "instruments",
    "instrument_aliases",
    "universe_snapshots",
    "universe_members",
    "sector_exposures",
    "latest_quotes",
    "watchlists",
    "watchlist_members",
    "accounts",
    "portfolios",
    "broker_sync_runs",
    "broker_sync_attempts",
    "portfolio_snapshots",
    "cash_snapshots",
    "position_snapshots",
    "broker_order_snapshots",
    "broker_cash_flows",
    "broker_cash_flow_watermarks",
    "broker_sync_requests",
    "account_external_id_envelopes",
}

EXPECTED_FOREIGN_KEYS = {
    "quality_reports": "fk_quality_reports_data_snapshot_id_data_snapshots",
    "price_series_manifests": "fk_price_series_manifests_data_artifact_id_data_artifacts",
    "job_leases": "fk_job_leases_job_id_jobs",
    "instrument_aliases": "fk_instrument_aliases_instrument_id_instruments",
    "universe_members": {
        "fk_universe_members_instrument_id_instruments",
        "fk_universe_members_universe_snapshot_id_universe_snapshots",
    },
    "sector_exposures": "fk_sector_exposures_instrument_id_instruments",
    "latest_quotes": "fk_latest_quotes_instrument_id_instruments",
    "watchlist_members": {
        "fk_watchlist_members_instrument_id_instruments",
        "fk_watchlist_members_watchlist_id_watchlists",
    },
    "portfolios": "fk_portfolios_account_id_accounts",
    "broker_sync_runs": {
        "fk_broker_sync_runs_account_id_accounts",
        "fk_broker_sync_runs_portfolio_id_portfolios",
    },
    "broker_sync_attempts": "fk_broker_sync_attempts_sync_run_id_runs",
    "portfolio_snapshots": {
        "fk_portfolio_snapshots_portfolio_id_portfolios",
        "fk_portfolio_snapshots_stale_source_same_portfolio",
        "fk_portfolio_snapshots_sync_run_id_runs",
    },
    "cash_snapshots": "fk_cash_snapshots_portfolio_snapshot_id_snapshots",
    "position_snapshots": {
        "fk_position_snapshots_instrument_id_instruments",
        "fk_position_snapshots_portfolio_snapshot_id_snapshots",
    },
    "broker_order_snapshots": {
        "fk_broker_order_snapshots_instrument_id_instruments",
        "fk_broker_order_snapshots_portfolio_snapshot_id_snapshots",
    },
    "broker_cash_flows": {
        "fk_broker_cash_flows_account_id_accounts",
        "fk_broker_cash_flows_sync_run_id_runs",
    },
    "broker_cash_flow_watermarks": "fk_cash_flow_watermarks_account_id_accounts",
    "broker_sync_requests": {
        "fk_broker_sync_requests_sync_run_id_runs",
        "fk_broker_sync_requests_idempotency_record_id_records",
        "fk_broker_sync_requests_account_id_accounts",
    },
    "account_external_id_envelopes": ("fk_account_external_id_envelopes_account_id_accounts"),
}


def test_initial_migration_creates_an_exact_reversible_schema(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    """An empty isolated database upgrades, downgrades, and upgrades without schema drift."""
    engine_root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(engine_root / "alembic.ini"))

    command.upgrade(config, "head")
    _assert_schema(temporary_database_url)
    command.downgrade(config, "base")
    assert _table_names(temporary_database_url) == {"alembic_version"}
    command.upgrade(config, "head")
    _assert_schema(temporary_database_url)


def test_0005_upgrades_and_round_trips_the_committed_0004_schema(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    engine_root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(engine_root / "alembic.ini"))

    command.upgrade(config, "0004_market_master")
    assert "published_at" not in _table_columns(temporary_database_url, "universe_snapshots")
    command.upgrade(config, "0005_market_integrity")
    assert "published_at" in _table_columns(temporary_database_url, "universe_snapshots")
    command.downgrade(config, "0004_market_master")
    assert "published_at" not in _table_columns(temporary_database_url, "universe_snapshots")
    command.upgrade(config, "head")
    _assert_schema(temporary_database_url)


def test_0007_round_trips_the_final_broker_schema(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    engine_root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(engine_root / "alembic.ini"))

    command.upgrade(config, "0006_market_delivery_authority")
    assert "accounts" not in _table_names(temporary_database_url)
    command.upgrade(config, "0007_portfolio_snapshots")
    assert {
        "broker_sync_requests",
        "account_external_id_envelopes",
    }.issubset(_table_names(temporary_database_url))
    _assert_schema(temporary_database_url)
    command.downgrade(config, "0006_market_delivery_authority")
    assert "accounts" not in _table_names(temporary_database_url)
    command.upgrade(config, "head")
    _assert_schema(temporary_database_url)


def test_0006_preserves_existing_admission_and_allows_same_contract_across_scopes(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    """One manifest hash must independently authorize quotes and bars without duplicates."""
    engine_root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(engine_root / "alembic.ini"))
    contract_hash = "a" * 64

    command.upgrade(config, "0005_market_integrity")
    _insert_provider_capability(
        temporary_database_url,
        contract_hash=contract_hash,
        operation="fosun_get_bars",
    )
    command.upgrade(config, "0006_market_delivery_authority")
    assert "delivery_mode" in _table_columns(temporary_database_url, "latest_quotes")
    assert "delivery_mode" in _table_columns(temporary_database_url, "price_series_manifests")

    assert _provider_capability_scopes(temporary_database_url) == [
        (contract_hash, "fosun_get_bars", "US")
    ]
    _insert_provider_capability(
        temporary_database_url,
        contract_hash=contract_hash,
        operation="fosun_get_quotes",
    )
    assert _provider_capability_scopes(temporary_database_url) == [
        (contract_hash, "fosun_get_bars", "US"),
        (contract_hash, "fosun_get_quotes", "US"),
    ]

    with pytest.raises(IntegrityError):
        _insert_provider_capability(
            temporary_database_url,
            contract_hash=contract_hash,
            operation="fosun_get_quotes",
        )
    assert len(_provider_capability_scopes(temporary_database_url)) == 2

    with pytest.raises(RuntimeError, match="scoped duplicates exist"):
        command.downgrade(config, "0005_market_integrity")
    assert len(_provider_capability_scopes(temporary_database_url)) == 2


def test_0006_publishes_and_exactly_removes_the_quote_domain_policy(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    """Quote IEX authority is independent of the committed delayed-SIP bar policy."""
    engine_root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(engine_root / "alembic.ini"))

    command.upgrade(config, "0005_market_integrity")
    _seed_0005_delivery_rows(temporary_database_url)
    assert _quote_source_policy_record(temporary_database_url) is None
    command.upgrade(config, "0006_market_delivery_authority")
    assert _quote_source_policy_record(temporary_database_url) == {
        "id": QUOTE_SOURCE_POLICY_ID,
        "key": "market_quote",
        "version": "quote_source_policy_v1",
        "schema_version": "1",
        "payload": QUOTE_SOURCE_POLICY_PAYLOAD,
    }
    assert _delivery_mode_rows(temporary_database_url, "latest_quotes") == [
        ("alpaca", "unknown"),
        ("fixture", "fixture"),
    ]
    assert _delivery_mode_rows(temporary_database_url, "price_series_manifests") == [
        ("alpaca", "unknown"),
        ("fixture", "fixture"),
    ]
    for table_name in ("latest_quotes", "price_series_manifests"):
        with pytest.raises(IntegrityError, match="fixture_delivery_mode"):
            _set_fixture_delivery_mode(
                temporary_database_url, table_name, delivery_mode="real_time"
            )
    command.downgrade(config, "0005_market_integrity")
    assert _quote_source_policy_record(temporary_database_url) is None


def test_0003_backfills_admission_state_after_the_released_0002_schema(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    """A released 0002 database upgrades admission state without rewriting its version history."""
    engine_root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(engine_root / "alembic.ini"))
    created_at = datetime(2026, 8, 22, 11, 30, tzinfo=UTC)

    command.upgrade(config, "0001_runtime_and_data")
    _seed_0001_integration_status(temporary_database_url, created_at)
    command.upgrade(config, "0002_integration_status_version")
    released_0002 = _integration_status_row(temporary_database_url, columns=("version",))
    assert released_0002 == {
        "provider": "fosun",
        "status": "fixture",
        "version": 1,
    }
    assert _integration_status_columns(temporary_database_url) == {
        "id",
        "provider",
        "status",
        "last_success_at",
        "created_at",
        "version",
    }

    command.upgrade(config, "head")
    upgraded = _integration_status_row(
        temporary_database_url,
        columns=(
            "version",
            "mode_changed_at",
            "capability_invalidated_at",
            "admitted_contract_hash",
        ),
    )
    assert upgraded == {
        "provider": "fosun",
        "status": "fixture",
        "version": 1,
        "mode_changed_at": created_at,
        "capability_invalidated_at": None,
        "admitted_contract_hash": None,
    }

    command.downgrade(config, "0002_integration_status_version")
    downgraded = _integration_status_row(temporary_database_url, columns=("version",))
    assert downgraded == released_0002
    assert _integration_status_columns(temporary_database_url) == {
        "id",
        "provider",
        "status",
        "last_success_at",
        "created_at",
        "version",
    }

    command.upgrade(config, "head")
    assert _integration_status_row(temporary_database_url) == upgraded


def test_0003_fail_closed_backfills_disabled_admission_epoch(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    """A legacy disabled row cannot reuse an admission from before migration time."""
    engine_root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(engine_root / "alembic.ini"))
    created_at = datetime(2026, 8, 22, 11, 30, tzinfo=UTC)

    command.upgrade(config, "0001_runtime_and_data")
    _seed_0001_integration_status(temporary_database_url, created_at, status="disabled")
    command.upgrade(config, "0002_integration_status_version")
    command.upgrade(config, "head")
    upgraded = _integration_status_row(
        temporary_database_url,
        columns=("mode_changed_at", "capability_invalidated_at", "admitted_contract_hash"),
    )

    assert upgraded["mode_changed_at"] == created_at
    assert isinstance(upgraded["capability_invalidated_at"], datetime)
    assert upgraded["admitted_contract_hash"] is None


def test_0003_legacy_read_only_with_old_admission_fails_closed_across_round_trip(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    """0003 cannot infer an old admission hash for a legacy read-only status row."""
    engine_root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(engine_root / "alembic.ini"))
    created_at = datetime(2026, 8, 22, 11, 30, tzinfo=UTC)
    projection_now = created_at + timedelta(hours=1)

    command.upgrade(config, "0001_runtime_and_data")
    _seed_0001_integration_status(
        temporary_database_url,
        created_at,
        status="read_only",
        with_valid_capability=True,
    )
    command.upgrade(config, "0002_integration_status_version")
    command.upgrade(config, "head")

    upgraded = _integration_status_row(temporary_database_url)
    assert upgraded == {
        "provider": "fosun",
        "status": "read_only",
        "version": 1,
        "mode_changed_at": created_at,
        "capability_invalidated_at": None,
        "admitted_contract_hash": None,
    }
    _assert_legacy_read_only_is_effectively_disabled(temporary_database_url, projection_now)
    command.downgrade(config, "0002_integration_status_version")
    assert _integration_status_row(temporary_database_url, columns=("version",)) == {
        "provider": "fosun",
        "status": "read_only",
        "version": 1,
    }
    command.upgrade(config, "head")

    assert _integration_status_row(temporary_database_url) == upgraded
    _assert_legacy_read_only_is_effectively_disabled(temporary_database_url, projection_now)
