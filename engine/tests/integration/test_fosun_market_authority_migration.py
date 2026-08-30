"""Migration authority for immutable Fosun market fact bindings."""

import asyncio
import json
import re
import threading
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from nexus.persistence.base import Base
from nexus.persistence.models import data as data_models
from nexus.persistence.models import market as market_models
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

assert data_models is not None
assert market_models is not None

SOURCE_POLICY_V2_ID = UUID("f780fa7f-9135-5004-ab21-d665ae7be24b")
QUOTE_POLICY_V2_ID = UUID("15afb077-46c6-5eea-8307-a7d203d11c2d")
INTRADAY_POLICY_V2_ID = UUID("9bd1eb52-5486-5f40-a3d8-06bd56f80b8d")


@pytest.mark.parametrize(
    ("revision", "tables"),
    (
        pytest.param(
            "0027_fosun_market_authority.py",
            (
                "data_artifacts",
                "integration_status",
                "intraday_bar_set_authorities",
                "latest_quotes",
                "price_bar_set_authorities",
                "price_series_manifests",
                "provider_capabilities",
                "source_policies",
            ),
            id="0027",
        ),
        pytest.param(
            "0028_fosun_authority_hardening.py",
            (
                "agent_data_snapshot_evidence",
                "corporate_actions",
                "data_artifacts",
                "integration_status",
                "intraday_bar_set_authorities",
                "latest_quotes",
                "price_bar_records",
                "price_bar_set_authorities",
                "price_series_manifests",
                "provider_capabilities",
                "source_policies",
            ),
            id="0028",
        ),
        pytest.param(
            "0029_fosun_bar_transport_freshness.py",
            (
                "agent_data_snapshot_evidence",
                "data_artifacts",
                "exchange_calendar_authorities",
                "intraday_bar_set_authorities",
                "latest_quotes",
                "price_bar_records",
                "price_bar_set_authorities",
                "price_series_manifests",
            ),
            id="0029",
        ),
    ),
)
def test_fosun_authority_downgrades_lock_fact_tables_before_inspection(
    revision: str,
    tables: tuple[str, ...],
) -> None:
    source = Path(f"engine/alembic/versions/{revision}").read_text()
    downgrade = source.split("def downgrade() -> None:", maxsplit=1)[1]
    inspection = downgrade.index("DO $$ BEGIN")
    lock_prefix = downgrade[:inspection]

    assert "LOCK TABLE" in lock_prefix
    assert "IN ACCESS EXCLUSIVE MODE" in lock_prefix
    positions = [lock_prefix.index(f"public.{table}") for table in tables]
    assert positions == sorted(positions)


@pytest.mark.parametrize(
    ("upgrade_revision", "downgrade_revision"),
    (
        pytest.param(
            "0027_fosun_market_authority",
            "0026_intraday_review_hardening",
            id="0027",
        ),
        pytest.param(
            "0028_fosun_authority_hardening",
            "0027_fosun_market_authority",
            id="0028",
        ),
        pytest.param(
            "0029_fosun_bar_lineage",
            "0028_fosun_authority_hardening",
            id="0029",
        ),
    ),
)
def test_fosun_authority_downgrade_rechecks_after_concurrent_bound_quote(
    temporary_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
    upgrade_revision: str,
    downgrade_revision: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    command.upgrade(Config("engine/alembic.ini"), upgrade_revision)

    error_code = asyncio.run(
        _race_bound_quote_against_downgrade(
            temporary_database_url,
            downgrade_revision=downgrade_revision,
        )
    )

    assert error_code == "P3D01"


def test_alembic_downgrade_forces_read_committed_over_database_default(
    temporary_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    asyncio.run(
        _set_database_default_isolation(
            temporary_database_url,
            "repeatable read",
        )
    )
    assert asyncio.run(_connection_isolation(temporary_database_url)) == "repeatable read"
    command.upgrade(Config("engine/alembic.ini"), "0027_fosun_market_authority")

    error_code = asyncio.run(
        _race_bound_quote_against_downgrade(
            temporary_database_url,
            downgrade_revision="0026_intraday_review_hardening",
        )
    )

    assert error_code == "P3D01"


def test_0031_is_single_head_and_metadata_exposes_complete_binding() -> None:
    scripts = ScriptDirectory.from_config(Config("engine/alembic.ini"))

    assert scripts.get_heads() == ["0032_ticker_alias_authority"]
    admission = scripts.get_revision("0031_intraday_schema_admission")
    assert admission is not None
    assert admission.down_revision == "0030_fosun_daily_staging"
    staging = scripts.get_revision("0030_fosun_daily_staging")
    assert staging is not None
    assert staging.down_revision == "0029_fosun_bar_lineage"
    lineage = scripts.get_revision("0029_fosun_bar_lineage")
    assert lineage is not None
    assert lineage.down_revision == "0028_fosun_authority_hardening"
    hardening = scripts.get_revision("0028_fosun_authority_hardening")
    assert hardening is not None
    assert hardening.down_revision == "0027_fosun_market_authority"
    revision = scripts.get_revision("0027_fosun_market_authority")
    assert revision is not None
    assert revision.down_revision == "0026_intraday_review_hardening"
    provider_binding = {
        "provider_capability_id",
        "provider_status_id",
        "provider_status_version",
        "provider_contract_hash",
        "provider_authorized_at",
    }
    manifest_lineage = {
        "gateway_transport_artifact_id",
        "daily_calendar_authority_id",
        "gateway_request_start_at",
        "gateway_request_end_at",
    }
    assert provider_binding | manifest_lineage <= set(
        Base.metadata.tables["price_series_manifests"].columns.keys()
    )
    assert provider_binding <= set(Base.metadata.tables["latest_quotes"].columns.keys())
    assert {
        "root_job_id",
        "fetch_job_id",
        "instrument_id",
        "record_kind",
        "raw_artifact_id",
        "raw_artifact_identity_sha256",
        "provider_authorized_at",
    } <= set(Base.metadata.tables["fosun_daily_bar_staging"].columns.keys())


def test_0028_pins_authority_function_search_paths(
    migrated_database_url: str,
) -> None:
    configurations = asyncio.run(_authority_function_configurations(migrated_database_url))

    assert configurations
    assert set(configurations.values()) == {("search_path=pg_catalog, public, pg_temp",)}


def test_0028_installs_two_minimal_provider_trigger_definers(
    migrated_database_url: str,
) -> None:
    snapshot = asyncio.run(_provider_publication_security_snapshot(migrated_database_url))

    assert snapshot == {
        "functions": [
            {
                "function_name": "nexus_guard_provider_price_bar_frozen_insert",
                "identity_arguments": "",
                "security_definer": True,
                "leakproof": False,
                "configuration": ("search_path=pg_catalog, public, pg_temp",),
                "trusted_owner": True,
                "public_execute": False,
                "non_owner_execute": False,
                "language": "plpgsql",
                "returns_trigger": True,
                "volatility": "v",
                "parallel_safety": "u",
            },
            {
                "function_name": "nexus_mark_provider_backtest_authority_publication",
                "identity_arguments": "",
                "security_definer": True,
                "leakproof": False,
                "configuration": ("search_path=pg_catalog, public, pg_temp",),
                "trusted_owner": True,
                "public_execute": False,
                "non_owner_execute": False,
                "language": "plpgsql",
                "returns_trigger": True,
                "volatility": "v",
                "parallel_safety": "u",
            },
        ],
        "triggers": [
            {
                "trigger_name": "trg_corporate_actions_backtest_publication",
                "schema_name": "public",
                "table_name": "corporate_actions",
                "trigger_type": 5,
                "enabled": "O",
                "internal": False,
                "unconditional": True,
                "non_constraint": True,
                "non_deferrable": True,
                "initially_immediate": True,
                "no_arguments": True,
                "not_partition_clone": True,
            },
            {
                "trigger_name": "trg_00_backtest_price_bar_frozen_insert",
                "schema_name": "public",
                "table_name": "price_bar_records",
                "trigger_type": 7,
                "enabled": "O",
                "internal": False,
                "unconditional": True,
                "non_constraint": True,
                "non_deferrable": True,
                "initially_immediate": True,
                "no_arguments": True,
                "not_partition_clone": True,
            },
            {
                "trigger_name": "trg_price_bar_set_authorities_backtest_publication",
                "schema_name": "public",
                "table_name": "price_bar_set_authorities",
                "trigger_type": 5,
                "enabled": "O",
                "internal": False,
                "unconditional": True,
                "non_constraint": True,
                "non_deferrable": True,
                "initially_immediate": True,
                "no_arguments": True,
                "not_partition_clone": True,
            },
        ],
    }


def test_0028_rejects_pg_temp_shadow_binding_bypass(
    migrated_database_url: str,
) -> None:
    with pytest.raises(DBAPIError, match="Fosun market authority") as error:
        asyncio.run(_attempt_pg_temp_shadow_binding_bypass(migrated_database_url))

    assert error.value.orig.sqlstate == "23514"


def test_0028_rejects_pg_temp_shadow_frozen_bar_growth(
    migrated_database_url: str,
) -> None:
    authority = asyncio.run(
        _insert_fosun_bar_authority(
            migrated_database_url,
            interval="1Day",
            delivery_mode="real_time",
        )
    )

    with pytest.raises(DBAPIError, match="frozen price series cannot grow") as error:
        asyncio.run(
            _attempt_pg_temp_shadow_bar_growth(
                migrated_database_url,
                manifest_id=authority["manifest_id"],
            )
        )

    assert error.value.orig.sqlstate == "23514"


def test_0028_rejects_pg_temp_shadow_alpaca_raw_artifact_bypass(
    migrated_database_url: str,
) -> None:
    with pytest.raises(DBAPIError, match="raw artifact") as error:
        asyncio.run(_attempt_pg_temp_shadow_alpaca_raw_artifact_bypass(migrated_database_url))

    assert error.value.orig.sqlstate == "23514"


def test_0028_rejects_pg_temp_shadow_intraday_review_bypass(
    migrated_database_url: str,
) -> None:
    with pytest.raises(DBAPIError, match="intraday bar review authority") as error:
        asyncio.run(_attempt_pg_temp_shadow_intraday_review_bypass(migrated_database_url))

    assert error.value.orig.sqlstate == "23514"


def test_0027_inserts_exact_immutable_v2_policies(migrated_database_url: str) -> None:
    policies = asyncio.run(_v2_policies(migrated_database_url))

    assert policies == [
        (SOURCE_POLICY_V2_ID, "market_price", "source_policy_v2", "2"),
        (QUOTE_POLICY_V2_ID, "market_quote", "quote_source_policy_v2", "2"),
        (
            INTRADAY_POLICY_V2_ID,
            "intraday_monitor",
            "intraday_monitor_policy_v2",
            "2",
        ),
    ]


def test_0027_accepts_complete_fosun_quote_binding(
    migrated_database_url: str,
) -> None:
    quote_id = asyncio.run(_insert_fosun_quote(migrated_database_url))

    assert isinstance(quote_id, UUID)


@pytest.mark.parametrize(
    "missing",
    [
        "provider_capability_id",
        "provider_status_id",
        "provider_status_version",
        "provider_contract_hash",
        "provider_authorized_at",
    ],
)
def test_0027_rejects_partial_fosun_binding(
    migrated_database_url: str,
    missing: str,
) -> None:
    with pytest.raises(DBAPIError, match="Fosun market authority") as error:
        asyncio.run(_insert_fosun_quote(migrated_database_url, missing=missing))

    assert error.value.orig.sqlstate == "23514"


def test_0027_rejects_future_created_capability(
    migrated_database_url: str,
) -> None:
    with pytest.raises(DBAPIError, match="Fosun market authority") as error:
        asyncio.run(
            _insert_fosun_quote(
                migrated_database_url,
                capability_created_offset=timedelta(minutes=1),
            )
        )

    assert error.value.orig.sqlstate == "23514"


@pytest.mark.parametrize(
    "authority_overrides",
    [
        pytest.param({"status_value": "disabled"}, id="status"),
        pytest.param({"status_version": 2}, id="status-version"),
        pytest.param(
            {"capability_hash": "b" * 64, "status_hash": "b" * 64},
            id="binding-hash",
        ),
        pytest.param({"status_hash": "b" * 64}, id="status-hash"),
        pytest.param({"operation": "fosun_get_bars"}, id="operation"),
        pytest.param({"market_scope": "HK"}, id="market-scope"),
        pytest.param({"delivery_mode": "real_time"}, id="delivery-mode"),
        pytest.param(
            {"capability_expires_offset": timedelta(seconds=-1)},
            id="expired",
        ),
        pytest.param(
            {"capability_invalidated_offset": timedelta(minutes=-30)},
            id="invalidated",
        ),
        pytest.param(
            {"mode_changed_offset": timedelta(minutes=1)},
            id="future-mode",
        ),
    ],
)
def test_0027_rejects_inconsistent_fosun_quote_authority(
    migrated_database_url: str,
    authority_overrides: dict[str, object],
) -> None:
    with pytest.raises(DBAPIError, match="Fosun market authority") as error:
        asyncio.run(
            _insert_fosun_quote(
                migrated_database_url,
                authority_overrides=authority_overrides,
            )
        )

    assert error.value.orig.sqlstate == "23514"


def test_0027_provider_capabilities_are_append_only(
    migrated_database_url: str,
) -> None:
    capability_id = asyncio.run(_seed_fosun_authority(migrated_database_url))[0]

    with pytest.raises(DBAPIError, match="provider capability is append-only") as error:
        asyncio.run(_mutate_capability(migrated_database_url, capability_id))

    assert error.value.orig.sqlstate == "23514"


def test_0027_bound_fosun_manifest_is_fully_immutable(
    migrated_database_url: str,
) -> None:
    seeded = asyncio.run(
        _seed_fosun_bar_candidate(
            migrated_database_url,
            interval="1Day",
            delivery_mode="real_time",
        )
    )

    with pytest.raises(DBAPIError, match="immutable") as error:
        asyncio.run(
            _mutate_bound_manifest(
                migrated_database_url,
                seeded["manifest_id"],
            )
        )

    assert error.value.orig.sqlstate == "23514"


def test_0029_rejects_incomplete_or_mismatched_transport_lineage(
    migrated_database_url: str,
) -> None:
    cases = (
        ("missing_raw", "Fosun transport lineage is invalid"),
        ("missing_calendar", "Fosun daily calendar lineage is invalid"),
        ("missing_request_end", "Fosun daily request lineage is invalid"),
        ("revision_mismatch", "Fosun bar transport revision is invalid"),
    )

    for lineage_case, message in cases:
        with pytest.raises(DBAPIError, match=message) as error:
            asyncio.run(
                _seed_fosun_bar_candidate(
                    migrated_database_url,
                    interval="1Day",
                    delivery_mode="real_time",
                    lineage_case=lineage_case,
                )
            )

        assert error.value.orig.sqlstate == "23514"


def test_0029_weekend_freshness_uses_market_session_and_not_db_created_clock(
    migrated_database_url: str,
) -> None:
    sunday_decision = datetime.now(UTC)
    friday_close = sunday_decision - timedelta(days=2)
    seeded = asyncio.run(
        _seed_fosun_bar_candidate(
            migrated_database_url,
            interval="1Day",
            delivery_mode="real_time",
            authorized_at_override=sunday_decision - timedelta(minutes=1),
            observed_at_override=friday_close,
            artifact_created_at=sunday_decision,
        )
    )

    valid = asyncio.run(
        _fosun_bar_valid_at(
            migrated_database_url,
            manifest_id=seeded["manifest_id"],
            as_of=sunday_decision,
        )
    )

    assert valid is True


def test_0029_daily_lineage_uses_utc_dates_under_non_utc_database_timezone(
    migrated_database_url: str,
) -> None:
    authorized_at = datetime.now(UTC) - timedelta(minutes=1)
    observed_date = authorized_at.date() - timedelta(days=2)
    observed_at = datetime.combine(observed_date, datetime.min.time(), tzinfo=UTC)
    observed_at += timedelta(minutes=30)
    seeded = asyncio.run(
        _seed_fosun_bar_candidate(
            migrated_database_url,
            interval="1Day",
            delivery_mode="real_time",
            authorized_at_override=authorized_at,
            observed_at_override=observed_at,
            session_timezone="America/New_York",
        )
    )

    valid = asyncio.run(
        _fosun_bar_valid_at(
            migrated_database_url,
            manifest_id=seeded["manifest_id"],
            as_of=authorized_at,
            session_timezone="America/New_York",
        )
    )

    assert valid is True


@pytest.mark.parametrize("finalized", (False, True), ids=("manifest", "authority"))
def test_0029_daily_calendar_rejects_append_after_fosun_publication(
    migrated_database_url: str,
    finalized: bool,
) -> None:
    if finalized:
        seeded = asyncio.run(
            _insert_fosun_bar_authority(
                migrated_database_url,
                interval="1Day",
                delivery_mode="real_time",
            )
        )
    else:
        seeded = asyncio.run(
            _seed_fosun_bar_candidate(
                migrated_database_url,
                interval="1Day",
                delivery_mode="real_time",
            )
        )

    with pytest.raises(DBAPIError, match="calendar authority set is sealed") as error:
        asyncio.run(_append_fosun_calendar_session(migrated_database_url, seeded))

    assert error.value.orig.sqlstate == "23514"
    assert (
        asyncio.run(
            _fosun_bar_valid_at(
                migrated_database_url,
                manifest_id=seeded["manifest_id"],
                as_of=seeded["provider_authorized_at"],  # type: ignore[arg-type]
            )
        )
        is True
    )


def test_0029_daily_validation_serializes_with_calendar_append(
    migrated_database_url: str,
) -> None:
    seeded = asyncio.run(
        _seed_fosun_bar_candidate(
            migrated_database_url,
            interval="1Day",
            delivery_mode="real_time",
        )
    )

    with pytest.raises(DBAPIError) as error:
        asyncio.run(
            _contend_fosun_calendar_validation_and_append(
                migrated_database_url,
                seeded,
            )
        )

    assert error.value.orig.sqlstate == "55P03", str(error.value)


def test_0027_price_prepare_holds_manifest_advisory_lock(
    migrated_database_url: str,
) -> None:
    seeded = asyncio.run(
        _seed_fosun_bar_candidate(
            migrated_database_url,
            interval="1Day",
            delivery_mode="real_time",
        )
    )

    with pytest.raises(DBAPIError) as error:
        asyncio.run(
            _contend_with_price_prepare_manifest_lock(
                migrated_database_url,
                seeded["manifest_id"],
            )
        )

    assert error.value.orig.sqlstate == "55P03", str(error.value)


def test_0027_binding_guard_holds_artifact_advisory_lock(
    migrated_database_url: str,
) -> None:
    with pytest.raises(DBAPIError) as error:
        asyncio.run(_contend_with_quote_artifact_lock(migrated_database_url))

    assert error.value.orig.sqlstate == "55P03", str(error.value)


@pytest.mark.parametrize(
    "lock_prefix",
    ("price-series-manifest:", "intraday-price-series-manifest:"),
)
def test_0028_fosun_manifest_mutation_holds_both_authority_locks(
    migrated_database_url: str,
    lock_prefix: str,
) -> None:
    seeded = asyncio.run(
        _seed_fosun_bar_candidate(
            migrated_database_url,
            interval="1Min",
            delivery_mode="unknown",
        )
    )

    with pytest.raises(DBAPIError) as error:
        asyncio.run(
            _contend_with_fosun_manifest_mutation_lock(
                migrated_database_url,
                seeded["manifest_id"],
                lock_prefix=lock_prefix,
            )
        )

    assert error.value.orig.sqlstate == "55P03", str(error.value)


@pytest.mark.parametrize(
    ("interval", "delivery_mode", "expected_hash_version", "expected_policy_version"),
    [
        (
            "1Day",
            "real_time",
            "price_bar_set_sha256_v2",
            "agent_price_authority_v2",
        ),
        (
            "1Min",
            "unknown",
            "intraday_bar_set_sha256_v2",
            "intraday_monitor_authority_v2",
        ),
    ],
)
def test_0027_creates_replayable_fosun_v2_bar_authority(
    migrated_database_url: str,
    interval: str,
    delivery_mode: str,
    expected_hash_version: str,
    expected_policy_version: str,
) -> None:
    result = asyncio.run(
        _insert_fosun_bar_authority(
            migrated_database_url,
            interval=interval,
            delivery_mode=delivery_mode,
        )
    )

    assert result["hash_version"] == expected_hash_version
    assert result["policy_version"] == expected_policy_version
    assert result["available_at"] == result["provider_authorized_at"]
    manifest = json.loads(str(result["canonical"]))["manifest"]
    assert manifest["provider_capability_id"] == str(result["provider_capability_id"])
    assert manifest["provider_status_id"] == str(result["provider_status_id"])
    assert manifest["provider_status_version"] == 1
    assert manifest["provider_contract_hash"] == "a" * 64
    assert manifest["provider_authorized_at"].endswith("Z")


def test_0027_daily_unknown_delivery_abstains_from_price_authority(
    migrated_database_url: str,
) -> None:
    prepared = asyncio.run(
        _prepare_fosun_bar_candidate(
            migrated_database_url,
            interval="1Day",
            delivery_mode="unknown",
        )
    )

    assert prepared == []


def test_0027_rejects_mixed_v2_hash_and_v1_policy_tuple(
    migrated_database_url: str,
) -> None:
    with pytest.raises(DBAPIError) as error:
        asyncio.run(
            _insert_fosun_bar_authority(
                migrated_database_url,
                interval="1Day",
                delivery_mode="real_time",
                policy_version="agent_price_authority_v1",
            )
        )

    assert error.value.orig.sqlstate == "23514"
    assert "price bar set authority is invalid" in str(error.value)


def test_0027_agent_snapshot_accepts_exact_daily_v2_authority(
    migrated_database_url: str,
) -> None:
    result = asyncio.run(
        _insert_fosun_bar_authority(
            migrated_database_url,
            interval="1Day",
            delivery_mode="delayed",
        )
    )

    binding_id = asyncio.run(_insert_agent_price_binding(migrated_database_url, result))

    assert isinstance(binding_id, UUID)


def test_0027_empty_schema_round_trips_without_residue(
    temporary_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config("engine/alembic.ini")
    command.upgrade(config, "0026_intraday_review_hardening")
    before = asyncio.run(_v2_schema(temporary_database_url))

    command.upgrade(config, "head")
    upgraded = asyncio.run(_v2_schema(temporary_database_url))
    command.downgrade(config, "0026_intraday_review_hardening")
    downgraded = asyncio.run(_v2_schema(temporary_database_url))
    command.upgrade(config, "head")
    upgraded_again = asyncio.run(_v2_schema(temporary_database_url))

    assert before == downgraded
    assert upgraded == upgraded_again
    assert upgraded["functions"] == {
        "nexus_price_bar_set_canonical_v2",
        "nexus_prepare_price_bar_set_v2",
        "nexus_intraday_bar_set_canonical_v2",
        "nexus_prepare_intraday_bar_set_v2",
    }
    assert upgraded["constraints"] == {
        "ck_price_bar_set_authorities_hash_policy_version_tuple",
        "ck_intraday_bar_set_authorities_hash_policy_version_tuple",
    }


def test_0029_empty_downgrade_restores_calendar_append_guard(
    temporary_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config("engine/alembic.ini")
    command.upgrade(config, "0028_fosun_authority_hardening")
    before = asyncio.run(_calendar_append_function_state(temporary_database_url))

    command.upgrade(config, "head")
    installed = asyncio.run(_calendar_append_function_state(temporary_database_url))
    command.downgrade(config, "0028_fosun_authority_hardening")
    restored = asyncio.run(_calendar_append_function_state(temporary_database_url))

    assert installed[1] == ("search_path=pg_catalog, public, pg_temp",)
    assert installed != before
    assert restored == before


def test_0027_downgrade_refuses_bound_fosun_fact(
    temporary_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config("engine/alembic.ini")
    command.upgrade(config, "0027_fosun_market_authority")
    asyncio.run(_insert_fosun_quote(temporary_database_url))

    with pytest.raises(DBAPIError, match="Fosun market authority prevents downgrade") as error:
        command.downgrade(config, "0026_intraday_review_hardening")

    assert error.value.orig.sqlstate == "P3D01"


def test_0028_refuses_to_remove_search_path_guard_with_bound_facts(
    temporary_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config("engine/alembic.ini")
    command.upgrade(config, "0028_fosun_authority_hardening")
    asyncio.run(_insert_fosun_quote(temporary_database_url))

    with pytest.raises(DBAPIError, match="hardening prevents downgrade") as error:
        command.downgrade(config, "0027_fosun_market_authority")

    assert error.value.orig.sqlstate == "P3D01"
    assert set(
        asyncio.run(_authority_function_configurations(temporary_database_url)).values()
    ) == {("search_path=pg_catalog, public, pg_temp",)}


@pytest.mark.parametrize(
    "fact_kind",
    ("manifest", "latest_quote", "price_authority", "intraday_authority", "agent"),
)
def test_0029_refuses_downgrade_for_preexisting_0027_fosun_fact(
    temporary_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
    fact_kind: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config("engine/alembic.ini")
    command.upgrade(config, "0027_fosun_market_authority")
    if fact_kind == "manifest":
        asyncio.run(
            _seed_fosun_bar_candidate(
                temporary_database_url,
                interval="1Day",
                delivery_mode="real_time",
            )
        )
    elif fact_kind == "latest_quote":
        asyncio.run(_insert_fosun_quote(temporary_database_url))
    elif fact_kind == "price_authority":
        asyncio.run(
            _insert_fosun_bar_authority(
                temporary_database_url,
                interval="1Day",
                delivery_mode="real_time",
            )
        )
    elif fact_kind == "intraday_authority":
        asyncio.run(
            _insert_fosun_bar_authority(
                temporary_database_url,
                interval="1Min",
                delivery_mode="real_time",
            )
        )
    else:
        authority = asyncio.run(
            _insert_fosun_bar_authority(
                temporary_database_url,
                interval="1Day",
                delivery_mode="real_time",
            )
        )
        asyncio.run(_insert_agent_price_binding(temporary_database_url, authority))
    command.upgrade(config, "head")

    with pytest.raises(DBAPIError, match="Fosun transport lineage prevents downgrade") as error:
        command.downgrade(config, "0028_fosun_authority_hardening")

    assert error.value.orig.sqlstate == "P3D01"


async def _v2_policies(database_url: str) -> list[tuple[UUID, str, str, str]]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            rows = (
                await connection.execute(
                    text(
                        "SELECT id,key,version,schema_version FROM source_policies "
                        "WHERE id IN (:daily,:quote,:intraday) "
                        "ORDER BY CASE id "
                        "WHEN :daily THEN 1 WHEN :quote THEN 2 ELSE 3 END"
                    ),
                    {
                        "daily": SOURCE_POLICY_V2_ID,
                        "quote": QUOTE_POLICY_V2_ID,
                        "intraday": INTRADAY_POLICY_V2_ID,
                    },
                )
            ).tuples()
            return list(rows)
    finally:
        await engine.dispose()


async def _authority_function_configurations(
    database_url: str,
) -> dict[str, tuple[str, ...]]:
    names = (
        "nexus_price_bar_set_canonical_v1",
        "nexus_prepare_price_bar_set",
        "nexus_intraday_bar_set_canonical_v1",
        "nexus_prepare_intraday_bar_set",
        "nexus_price_bar_set_canonical_v2",
        "nexus_prepare_price_bar_set_v2",
        "nexus_intraday_bar_set_canonical_v2",
        "nexus_prepare_intraday_bar_set_v2",
        "nexus_validate_price_bar_set_authority",
        "nexus_validate_intraday_bar_set_authority",
        "nexus_guard_price_bar_set_mutation",
        "nexus_validate_latest_quote_raw_artifact",
        "nexus_validate_intraday_review_authority",
        "nexus_prevent_frozen_backtest_authority_change",
        "nexus_lock_fosun_market_input_change",
        "nexus_validate_agent_snapshot_evidence",
        "nexus_prevent_provider_capability_change",
        "nexus_validate_fosun_market_binding",
        "nexus_prevent_fosun_market_binding_change",
    )
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            rows = (
                await connection.execute(
                    text(
                        "SELECT proname, proconfig FROM pg_proc "
                        "WHERE pronamespace='public'::regnamespace "
                        "AND proname = ANY(CAST(:names AS text[]))"
                    ),
                    {"names": list(names)},
                )
            ).tuples()
            return {name: tuple(configuration or ()) for name, configuration in rows}
    finally:
        await engine.dispose()


async def _calendar_append_function_state(
    database_url: str,
) -> tuple[str, tuple[str, ...]]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            row = (
                await connection.execute(
                    text(
                        "SELECT prosrc,proconfig FROM pg_proc "
                        "WHERE oid='nexus_prevent_sealed_backtest_append()'::regprocedure"
                    )
                )
            ).one()
            return str(row.prosrc), tuple(row.proconfig or ())
    finally:
        await engine.dispose()


async def _provider_publication_security_snapshot(database_url: str) -> dict[str, object]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            function_rows = (
                (
                    await connection.execute(
                        text(
                            "SELECT procedure.proname AS function_name, "
                            "pg_get_function_identity_arguments(procedure.oid) "
                            "AS identity_arguments, procedure.prosecdef AS security_definer, "
                            "procedure.proleakproof AS leakproof, "
                            "procedure.proconfig AS configuration, "
                            "procedure.proowner = marks.relowner AS trusted_owner, "
                            "EXISTS (SELECT 1 FROM aclexplode(COALESCE(procedure.proacl,"
                            "acldefault('f',procedure.proowner))) AS public_acl "
                            "WHERE public_acl.privilege_type='EXECUTE' "
                            "AND public_acl.grantee=0) AS public_execute, "
                            "EXISTS (SELECT 1 FROM aclexplode(COALESCE("
                            "procedure.proacl,acldefault('f',procedure.proowner))) AS acl "
                            "WHERE acl.privilege_type='EXECUTE' "
                            "AND acl.grantee NOT IN (0,procedure.proowner)) AS non_owner_execute, "
                            "language.lanname AS language, "
                            "procedure.prorettype='trigger'::regtype AS returns_trigger, "
                            "procedure.provolatile::text AS volatility, "
                            "procedure.proparallel::text AS parallel_safety "
                            "FROM pg_proc AS procedure "
                            "JOIN pg_language AS language ON language.oid=procedure.prolang "
                            "JOIN pg_class AS marks ON marks.oid="
                            "'public.backtest_authority_publication_marks'::regclass "
                            "WHERE procedure.pronamespace='public'::regnamespace "
                            "AND procedure.prosecdef ORDER BY procedure.proname"
                        )
                    )
                )
                .mappings()
                .all()
            )
            trigger_rows = (
                (
                    await connection.execute(
                        text(
                            "SELECT trigger.tgname AS trigger_name, "
                            "namespace.nspname AS schema_name, relation.relname AS table_name, "
                            "trigger.tgtype AS trigger_type, "
                            "trigger.tgenabled::text AS enabled, "
                            "trigger.tgisinternal AS internal, "
                            "trigger.tgqual IS NULL AS unconditional, "
                            "trigger.tgconstraint = 0 AS non_constraint, "
                            "NOT trigger.tgdeferrable AS non_deferrable, "
                            "NOT trigger.tginitdeferred AS initially_immediate, "
                            "trigger.tgnargs = 0 AS no_arguments, "
                            "trigger.tgparentid = 0 AS not_partition_clone "
                            "FROM pg_trigger AS trigger "
                            "JOIN pg_proc AS procedure ON procedure.oid=trigger.tgfoid "
                            "JOIN pg_class AS relation ON relation.oid=trigger.tgrelid "
                            "JOIN pg_namespace AS namespace "
                            "ON namespace.oid=relation.relnamespace "
                            "WHERE procedure.pronamespace='public'::regnamespace "
                            "AND procedure.proname IN ("
                            "'nexus_mark_provider_backtest_authority_publication',"
                            "'nexus_guard_provider_price_bar_frozen_insert') "
                            "ORDER BY relation.relname,trigger.tgname"
                        )
                    )
                )
                .mappings()
                .all()
            )
            return {
                "functions": [_publication_function_dict(row) for row in function_rows],
                "triggers": [dict(row) for row in trigger_rows],
            }
    finally:
        await engine.dispose()


def _publication_function_dict(row: Mapping[str, object]) -> dict[str, object]:
    values = dict(row)
    values["configuration"] = tuple(values["configuration"] or ())
    return values


async def _attempt_pg_temp_shadow_binding_bypass(database_url: str) -> None:
    (
        capability_id,
        status_id,
        artifact_id,
        instrument_id,
        authorized_at,
        received_at,
    ) = await _seed_fosun_authority(database_url, status_value="disabled")
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            for table_name in (
                "integration_status",
                "provider_capabilities",
                "data_artifacts",
                "source_policies",
            ):
                await connection.execute(
                    text(
                        f"CREATE TEMP TABLE {table_name} AS TABLE public.{table_name} WITH NO DATA"
                    )
                )
            await connection.execute(
                text(
                    "INSERT INTO pg_temp.integration_status "
                    "SELECT * FROM public.integration_status WHERE id=:status_id"
                ),
                {"status_id": status_id},
            )
            await connection.execute(
                text(
                    "UPDATE pg_temp.integration_status SET status='read_only' WHERE id=:status_id"
                ),
                {"status_id": status_id},
            )
            await connection.execute(
                text(
                    "INSERT INTO pg_temp.provider_capabilities "
                    "SELECT * FROM public.provider_capabilities WHERE id=:capability_id"
                ),
                {"capability_id": capability_id},
            )
            await connection.execute(
                text(
                    "INSERT INTO pg_temp.data_artifacts "
                    "SELECT * FROM public.data_artifacts WHERE id=:artifact_id"
                ),
                {"artifact_id": artifact_id},
            )
            await connection.execute(
                text("INSERT INTO pg_temp.source_policies SELECT * FROM public.source_policies")
            )
            await connection.execute(text("SET LOCAL search_path = pg_temp, public"))
            await connection.execute(
                text(
                    "INSERT INTO public.latest_quotes "
                    "(id,instrument_id,source,feed,delivery_mode,bid,ask,last,observed_at,"
                    "available_at,received_at,revision,quality_flags,event_kind,content_hash,"
                    "evidence_status,data_artifact_id,provider_capability_id,provider_status_id,"
                    "provider_status_version,provider_contract_hash,provider_authorized_at,"
                    "created_at) VALUES "
                    "(:id,:instrument_id,'fosun','l1','unknown',NULL,NULL,200,:received,"
                    ":received,:received,repeat('b',64),"
                    '\'["FOSUN_PROVIDER_TIMESTAMP_UNAVAILABLE",'
                    "\"INTRADAY_BID_ASK_UNAVAILABLE\"]'::jsonb,'snapshot',repeat('c',64),"
                    "'verified',:artifact_id,:capability_id,:status_id,1,repeat('a',64),"
                    ":authorized,:received)"
                ),
                {
                    "id": uuid4(),
                    "instrument_id": instrument_id,
                    "artifact_id": artifact_id,
                    "capability_id": capability_id,
                    "status_id": status_id,
                    "authorized": authorized_at,
                    "received": received_at,
                },
            )
    finally:
        await engine.dispose()


async def _attempt_pg_temp_shadow_bar_growth(
    database_url: str,
    *,
    manifest_id: object,
) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            for table_name in (
                "price_bar_set_authorities",
                "intraday_bar_set_authorities",
            ):
                await connection.execute(
                    text(
                        f"CREATE TEMP TABLE {table_name} AS TABLE public.{table_name} WITH NO DATA"
                    )
                )
            await connection.execute(text("SET LOCAL search_path = pg_temp, public"))
            await connection.execute(
                text(
                    "INSERT INTO public.price_bar_records "
                    "(id,price_series_manifest_id,instrument_id,session,open,high,low,close,"
                    "volume,observed_at,available_at,received_at,revision,quality_flags,"
                    "created_at) "
                    "SELECT :id,price_series_manifest_id,instrument_id,session + 1,open,high,"
                    "low,close,volume,observed_at + interval '1 day',"
                    "available_at + interval '1 day',received_at + interval '1 day',"
                    "revision,quality_flags,clock_timestamp() "
                    "FROM public.price_bar_records WHERE price_series_manifest_id=:manifest_id"
                ),
                {"id": uuid4(), "manifest_id": manifest_id},
            )
    finally:
        await engine.dispose()


async def _attempt_pg_temp_shadow_alpaca_raw_artifact_bypass(
    database_url: str,
) -> None:
    engine = create_async_engine(database_url)
    instrument_id = uuid4()
    artifact_id = uuid4()
    observed_at = datetime(2026, 8, 28, 14, 32, tzinfo=UTC)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO public.instruments "
                    "(id,asset_type,exchange,currency,tick_size,risk_bucket) "
                    "VALUES (:id,'common_stock','XNYS','USD',0.01,'EQUITY')"
                ),
                {"id": instrument_id},
            )
            await connection.execute(
                text(
                    "INSERT INTO public.data_artifacts "
                    "(id,uri,sha256,byte_size,mime_type,schema_version,source,observed_at,"
                    "available_at,received_at) VALUES "
                    "(:id,'memory://invalid',repeat('a',64),2,'application/json','test_v1',"
                    "'fosun',:observed_at,:observed_at,:observed_at)"
                ),
                {"id": artifact_id, "observed_at": observed_at},
            )
            await connection.execute(
                text("CREATE TEMP TABLE data_artifacts AS TABLE public.data_artifacts WITH NO DATA")
            )
            await connection.execute(
                text(
                    "INSERT INTO pg_temp.data_artifacts SELECT * "
                    "FROM public.data_artifacts WHERE id=:artifact_id"
                ),
                {"artifact_id": artifact_id},
            )
            await connection.execute(
                text(
                    "UPDATE pg_temp.data_artifacts SET source='alpaca', sha256=repeat('b',64) "
                    "WHERE id=:artifact_id"
                ),
                {"artifact_id": artifact_id},
            )
            await connection.execute(text("SET LOCAL search_path = pg_temp, public"))
            await connection.execute(
                text(
                    "INSERT INTO public.latest_quotes "
                    "(id,instrument_id,source,feed,delivery_mode,bid,ask,last,observed_at,"
                    "available_at,received_at,revision,quality_flags,event_kind,content_hash,"
                    "evidence_status,data_artifact_id) VALUES "
                    "(:id,:instrument_id,'alpaca','iex','real_time',NULL,NULL,100,"
                    ":observed_at,:observed_at,:observed_at,repeat('b',64),'[]'::jsonb,"
                    "'trade',repeat('c',64),'verified',:artifact_id)"
                ),
                {
                    "id": uuid4(),
                    "instrument_id": instrument_id,
                    "artifact_id": artifact_id,
                    "observed_at": observed_at,
                },
            )
    finally:
        await engine.dispose()


async def _attempt_pg_temp_shadow_intraday_review_bypass(
    database_url: str,
) -> None:
    engine = create_async_engine(database_url)
    instrument_id = uuid4()
    artifact_id = uuid4()
    manifest_evidence_id = uuid4()
    authority_evidence_id = uuid4()
    manifest_id = uuid4()
    observed_at = datetime(2026, 8, 28, 14, 31, tzinfo=UTC)
    received_at = observed_at + timedelta(seconds=30)
    digest = "d" * 64
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO public.instruments "
                    "(id,asset_type,exchange,currency,tick_size,risk_bucket) "
                    "VALUES (:id,'common_stock','XNYS','USD',0.01,'EQUITY')"
                ),
                {"id": instrument_id},
            )
            await connection.execute(
                text(
                    "INSERT INTO public.data_artifacts "
                    "(id,uri,sha256,byte_size,mime_type,schema_version,source,observed_at,"
                    "available_at,received_at) VALUES "
                    "(:id,:uri,:digest,1,'application/json','test_v1','alpaca',"
                    ":observed_at,:received_at,:received_at)"
                ),
                {
                    "id": artifact_id,
                    "uri": f"memory://sha256/{digest}",
                    "digest": digest,
                    "observed_at": observed_at,
                    "received_at": received_at,
                },
            )
            await connection.execute(
                text(
                    "INSERT INTO public.research_evidence_refs "
                    "(id,source,provider_object_id,revision,available_at,location,"
                    "content_hash,byte_length) VALUES "
                    "(:id,'alpaca',:object_id,:revision,:received_at,"
                    "'memory://sha256/0000000000000000000000000000000000000000000000000000000000000000',"
                    "repeat('0',64),1)"
                ),
                {
                    "id": manifest_evidence_id,
                    "object_id": f"price-series-manifest:{manifest_id}",
                    "revision": "0" * 64,
                    "received_at": received_at,
                },
            )
            await connection.execute(
                text(
                    "INSERT INTO public.price_series_manifests "
                    "(id,evidence_ref_id,data_artifact_id,instrument,source,feed,delivery_mode,"
                    "adjustment,interval,start_at,end_at,row_count) VALUES "
                    "(:id,:evidence_id,:artifact_id,:instrument,'alpaca','iex','real_time',"
                    "'raw','1Min',:observed_at,:observed_at,1)"
                ),
                {
                    "id": manifest_id,
                    "evidence_id": manifest_evidence_id,
                    "artifact_id": artifact_id,
                    "instrument": str(instrument_id),
                    "observed_at": observed_at,
                },
            )
            await connection.execute(
                text(
                    "INSERT INTO public.price_bar_records "
                    "(id,price_series_manifest_id,instrument_id,session,open,high,low,close,"
                    "volume,observed_at,available_at,received_at,revision,quality_flags) "
                    "VALUES (:id,:manifest_id,:instrument_id,:session,100,101,99,100,1000,"
                    ":observed_at,:received_at,:received_at,'revision-v1','[]'::jsonb)"
                ),
                {
                    "id": uuid4(),
                    "manifest_id": manifest_id,
                    "instrument_id": instrument_id,
                    "session": observed_at.date(),
                    "observed_at": observed_at,
                    "received_at": received_at,
                },
            )
            prepared = (
                (
                    await connection.execute(
                        text("SELECT * FROM public.nexus_prepare_intraday_bar_set(:manifest_id)"),
                        {"manifest_id": manifest_id},
                    )
                )
                .mappings()
                .one()
            )
            await connection.execute(
                text(
                    "INSERT INTO public.research_evidence_refs "
                    "(id,source,provider_object_id,revision,available_at,location,"
                    "content_hash,byte_length) VALUES "
                    "(:id,'alpaca',:object_id,:digest,:available_at,:location,"
                    ":digest,:byte_length)"
                ),
                {
                    "id": authority_evidence_id,
                    "object_id": f"intraday-bar-set:{manifest_id}",
                    "digest": prepared["content_hash"],
                    "available_at": prepared["available_at"],
                    "byte_length": prepared["byte_length"],
                    "location": f"memory://sha256/{prepared['content_hash']}",
                },
            )
            await connection.execute(
                text(
                    "CREATE TEMP TABLE price_bar_records AS "
                    "TABLE public.price_bar_records WITH NO DATA"
                )
            )
            await connection.execute(text("SET LOCAL search_path = pg_temp, public"))
            await connection.execute(
                text(
                    "INSERT INTO public.intraday_bar_set_authorities "
                    "(id,price_series_manifest_id,evidence_ref_id,source_policy_id,source,"
                    "provider_object_id,revision,hash_version,policy_version,content_hash,"
                    "byte_length,bar_count,first_observed_at,last_observed_at,available_at) "
                    "VALUES (:id,:manifest_id,:evidence_id,:policy_id,'alpaca',:object_id,"
                    ":digest,'intraday_bar_set_sha256_v1','intraday_monitor_authority_v1',"
                    ":digest,:byte_length,:bar_count,:first_observed,:last_observed,"
                    ":available_at)"
                ),
                {
                    "id": uuid4(),
                    "manifest_id": manifest_id,
                    "evidence_id": authority_evidence_id,
                    "policy_id": UUID("8fab80c9-5b6a-5b2b-8f36-3b22319b54ef"),
                    "object_id": f"intraday-bar-set:{manifest_id}",
                    "digest": prepared["content_hash"],
                    "byte_length": prepared["byte_length"],
                    "bar_count": prepared["bar_count"],
                    "first_observed": prepared["first_observed_at"],
                    "last_observed": prepared["last_observed_at"],
                    "available_at": prepared["available_at"],
                },
            )
    finally:
        await engine.dispose()


async def _seed_fosun_authority(
    database_url: str,
    *,
    capability_created_offset: timedelta = timedelta(hours=-1),
    capability_expires_offset: timedelta = timedelta(hours=1),
    capability_hash: str = "a" * 64,
    status_hash: str | None = None,
    status_value: str = "read_only",
    status_version: int = 1,
    mode_changed_offset: timedelta = timedelta(hours=-2),
    capability_invalidated_offset: timedelta | None = None,
    operation: str = "fosun_get_quotes",
    market_scope: str = "US",
    delivery_mode: str = "unknown",
) -> tuple[UUID, UUID, UUID, UUID, datetime, datetime]:
    engine = create_async_engine(database_url)
    capability_id = uuid4()
    status_id = uuid4()
    artifact_id = uuid4()
    instrument_id = uuid4()
    authorized_at = datetime.now(UTC) - timedelta(seconds=1)
    received_at = authorized_at - timedelta(seconds=1)
    status_hash = capability_hash if status_hash is None else status_hash
    capability_invalidated_at = (
        None
        if capability_invalidated_offset is None
        else authorized_at + capability_invalidated_offset
    )
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO provider_capabilities "
                    "(id,provider,capability,contract_hash,expires_at,schema_version,payload,"
                    "created_at) VALUES "
                    "(:id,'fosun','READ_ONLY',:hash,:expires,'1',"
                    "jsonb_build_object('operation',CAST(:operation AS text),"
                    "'market_scope',CAST(:market_scope AS text),"
                    "'delivery_mode',CAST(:delivery_mode AS text)),:created)"
                ),
                {
                    "id": capability_id,
                    "hash": capability_hash,
                    "expires": authorized_at + capability_expires_offset,
                    "created": authorized_at + capability_created_offset,
                    "operation": operation,
                    "market_scope": market_scope,
                    "delivery_mode": delivery_mode,
                },
            )
            await connection.execute(
                text(
                    "INSERT INTO integration_status "
                    "(id,provider,status,last_success_at,version,mode_changed_at,"
                    "capability_invalidated_at,admitted_contract_hash,created_at) VALUES "
                    "(:id,'fosun',:status,:received,:version,:changed,:invalidated,:hash,"
                    ":changed)"
                ),
                {
                    "id": status_id,
                    "status": status_value,
                    "received": received_at,
                    "version": status_version,
                    "changed": authorized_at + mode_changed_offset,
                    "invalidated": capability_invalidated_at,
                    "hash": status_hash,
                },
            )
            await connection.execute(
                text(
                    "INSERT INTO data_artifacts "
                    "(id,uri,sha256,byte_size,mime_type,schema_version,source,observed_at,"
                    "available_at,received_at,created_at) VALUES "
                    "(:id,'memory://sha256/' || repeat('b',64),repeat('b',64),2,"
                    "'application/json','fosun_gateway_market_snapshot_v1','fosun',"
                    ":received,:received,:received,:received)"
                ),
                {"id": artifact_id, "received": received_at},
            )
            await connection.execute(
                text(
                    "INSERT INTO instruments "
                    "(id,asset_type,exchange,currency,tick_size,risk_bucket,created_at) "
                    "VALUES (:id,'common_stock','XNAS','USD',0.01,'EQUITY',:received)"
                ),
                {"id": instrument_id, "received": received_at},
            )
        return (
            capability_id,
            status_id,
            artifact_id,
            instrument_id,
            authorized_at,
            received_at,
        )
    finally:
        await engine.dispose()


async def _insert_fosun_quote(
    database_url: str,
    *,
    missing: str | None = None,
    capability_created_offset: timedelta = timedelta(hours=-1),
    authority_overrides: dict[str, object] | None = None,
) -> UUID:
    (
        capability_id,
        status_id,
        artifact_id,
        instrument_id,
        authorized_at,
        received_at,
    ) = await _seed_fosun_authority(
        database_url,
        capability_created_offset=capability_created_offset,
        **(authority_overrides or {}),
    )
    binding: dict[str, object | None] = {
        "provider_capability_id": capability_id,
        "provider_status_id": status_id,
        "provider_status_version": 1,
        "provider_contract_hash": "a" * 64,
        "provider_authorized_at": authorized_at,
    }
    if missing is not None:
        binding[missing] = None
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            quote_id = await _insert_fosun_quote_on_connection(
                connection,
                capability_id=capability_id,
                status_id=status_id,
                artifact_id=artifact_id,
                instrument_id=instrument_id,
                authorized_at=authorized_at,
                received_at=received_at,
                binding=binding,
            )
        return quote_id
    finally:
        await engine.dispose()


async def _insert_fosun_quote_on_connection(
    connection: AsyncConnection,
    *,
    capability_id: UUID,
    status_id: UUID,
    artifact_id: UUID,
    instrument_id: UUID,
    authorized_at: datetime,
    received_at: datetime,
    binding: Mapping[str, object | None] | None = None,
) -> UUID:
    quote_id = uuid4()
    resolved_binding: Mapping[str, object | None] = binding or {
        "provider_capability_id": capability_id,
        "provider_status_id": status_id,
        "provider_status_version": 1,
        "provider_contract_hash": "a" * 64,
        "provider_authorized_at": authorized_at,
    }
    await connection.execute(
        text(
            "INSERT INTO latest_quotes "
            "(id,instrument_id,source,feed,delivery_mode,bid,ask,last,observed_at,"
            "available_at,received_at,revision,quality_flags,event_kind,content_hash,"
            "evidence_status,data_artifact_id,provider_capability_id,provider_status_id,"
            "provider_status_version,provider_contract_hash,provider_authorized_at,"
            "created_at) VALUES "
            "(:id,:instrument_id,'fosun','l1','unknown',NULL,NULL,200,:received,"
            ":received,:received,repeat('b',64),"
            '\'["FOSUN_PROVIDER_TIMESTAMP_UNAVAILABLE",'
            "\"INTRADAY_BID_ASK_UNAVAILABLE\"]'::jsonb,'snapshot',repeat('c',64),"
            "'verified',:artifact_id,:provider_capability_id,:provider_status_id,"
            ":provider_status_version,:provider_contract_hash,"
            ":provider_authorized_at,:received)"
        ),
        {
            "id": quote_id,
            "instrument_id": instrument_id,
            "artifact_id": artifact_id,
            "received": received_at,
            **resolved_binding,
        },
    )
    return quote_id


async def _race_bound_quote_against_downgrade(
    database_url: str,
    *,
    downgrade_revision: str,
) -> str | None:
    (
        capability_id,
        status_id,
        artifact_id,
        instrument_id,
        authorized_at,
        received_at,
    ) = await _seed_fosun_authority(database_url)
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as writer:
            transaction = await writer.begin()
            await _insert_fosun_quote_on_connection(
                writer,
                capability_id=capability_id,
                status_id=status_id,
                artifact_id=artifact_id,
                instrument_id=instrument_id,
                authorized_at=authorized_at,
                received_at=received_at,
            )
            downgrade_started = threading.Event()

            def downgrade() -> str | None:
                downgrade_started.set()
                try:
                    command.downgrade(Config("engine/alembic.ini"), downgrade_revision)
                except DBAPIError as error:
                    return error.orig.sqlstate
                return None

            downgrade_task = asyncio.create_task(asyncio.to_thread(downgrade))
            try:
                for _ in range(500):
                    if downgrade_started.is_set():
                        break
                    if downgrade_task.done():
                        return await downgrade_task
                    await asyncio.sleep(0.01)
                else:
                    raise AssertionError("Fosun authority downgrade worker did not start")

                async with engine.connect() as observer:
                    for _ in range(500):
                        downgrade_lock_is_waiting = await observer.scalar(
                            text(
                                "SELECT EXISTS ("
                                "SELECT 1 FROM pg_catalog.pg_locks "
                                "WHERE database = ("
                                "SELECT oid FROM pg_catalog.pg_database "
                                "WHERE datname = pg_catalog.current_database()"
                                ") AND relation IN ("
                                "'public.data_artifacts'::pg_catalog.regclass,"
                                "'public.integration_status'::pg_catalog.regclass,"
                                "'public.latest_quotes'::pg_catalog.regclass,"
                                "'public.price_series_manifests'::pg_catalog.regclass"
                                ") "
                                "AND mode = 'AccessExclusiveLock' AND NOT granted"
                                ")"
                            )
                        )
                        if downgrade_lock_is_waiting:
                            break
                        if downgrade_task.done():
                            return await downgrade_task
                        await asyncio.sleep(0.01)
                    else:
                        raise AssertionError(
                            "Fosun authority downgrade did not reach the fact-table lock"
                        )

                await transaction.commit()
                return await asyncio.wait_for(downgrade_task, timeout=5)
            finally:
                if transaction.is_active:
                    await transaction.rollback()
                if not downgrade_task.done():
                    downgrade_task.cancel()
                    await asyncio.gather(downgrade_task, return_exceptions=True)
    finally:
        await engine.dispose()


async def _set_database_default_isolation(database_url: str, isolation: str) -> None:
    if isolation not in {"read committed", "repeatable read"}:
        raise ValueError("unsupported test isolation")
    parsed = make_url(database_url)
    database_name = parsed.database
    if database_name is None or re.fullmatch(r"nexus_test_[0-9a-f]{32}", database_name) is None:
        raise ValueError("unexpected temporary database name")
    engine = create_async_engine(
        parsed.set(database="postgres"),
        isolation_level="AUTOCOMMIT",
    )
    try:
        async with engine.connect() as connection:
            await connection.execute(
                text(
                    f'ALTER DATABASE "{database_name}" '
                    f"SET default_transaction_isolation TO '{isolation}'"
                )
            )
    finally:
        await engine.dispose()


async def _connection_isolation(database_url: str) -> str:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            return str(await connection.scalar(text("SHOW transaction_isolation")))
    finally:
        await engine.dispose()


async def _mutate_capability(database_url: str, capability_id: UUID) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "UPDATE provider_capabilities SET expires_at=expires_at + interval '1 hour' "
                    "WHERE id=:id"
                ),
                {"id": capability_id},
            )
    finally:
        await engine.dispose()


async def _mutate_bound_manifest(database_url: str, manifest_id: object) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text("UPDATE price_series_manifests SET row_count=row_count + 1 WHERE id=:id"),
                {"id": manifest_id},
            )
    finally:
        await engine.dispose()


async def _contend_with_price_prepare_manifest_lock(
    database_url: str,
    manifest_id: object,
) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as holder:
            transaction = await holder.begin()
            try:
                await holder.execute(
                    text("SELECT * FROM nexus_prepare_price_bar_set_v2(:id)"),
                    {"id": manifest_id},
                )
                async with engine.connect() as contender:
                    await contender.execute(text("SET lock_timeout='1ms'"))
                    await contender.execute(
                        text(
                            "SELECT pg_advisory_xact_lock(hashtextextended("
                            "'price-series-manifest:' || CAST(:id AS text), 0))"
                        ),
                        {"id": str(manifest_id)},
                    )
            finally:
                await transaction.rollback()
    finally:
        await engine.dispose()


async def _contend_with_quote_artifact_lock(database_url: str) -> None:
    (
        capability_id,
        status_id,
        artifact_id,
        instrument_id,
        authorized_at,
        received_at,
    ) = await _seed_fosun_authority(database_url)
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as holder:
            transaction = await holder.begin()
            try:
                await holder.execute(
                    text(
                        "INSERT INTO latest_quotes "
                        "(id,instrument_id,source,feed,delivery_mode,bid,ask,last,observed_at,"
                        "available_at,received_at,revision,quality_flags,event_kind,content_hash,"
                        "evidence_status,data_artifact_id,provider_capability_id,provider_status_id,"
                        "provider_status_version,provider_contract_hash,provider_authorized_at,"
                        "created_at) VALUES "
                        "(:id,:instrument_id,'fosun','l1','unknown',NULL,NULL,200,:received,"
                        ":received,:received,repeat('b',64),"
                        '\'["FOSUN_PROVIDER_TIMESTAMP_UNAVAILABLE",'
                        "\"INTRADAY_BID_ASK_UNAVAILABLE\"]'::jsonb,'snapshot',repeat('c',64),"
                        "'verified',:artifact_id,:capability_id,:status_id,1,repeat('a',64),"
                        ":authorized,:received)"
                    ),
                    {
                        "id": uuid4(),
                        "instrument_id": instrument_id,
                        "artifact_id": artifact_id,
                        "capability_id": capability_id,
                        "status_id": status_id,
                        "authorized": authorized_at,
                        "received": received_at,
                    },
                )
                async with engine.connect() as contender:
                    await contender.execute(text("SET lock_timeout='1ms'"))
                    await contender.execute(
                        text(
                            "SELECT pg_advisory_xact_lock(hashtextextended("
                            "'fosun-data-artifact:' || CAST(:id AS text), 0))"
                        ),
                        {"id": str(artifact_id)},
                    )
            finally:
                await transaction.rollback()
    finally:
        await engine.dispose()


async def _contend_with_fosun_manifest_mutation_lock(
    database_url: str,
    manifest_id: object,
    *,
    lock_prefix: str,
) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as holder:
            transaction = await holder.begin()
            try:
                await holder.execute(
                    text("SELECT pg_advisory_xact_lock(hashtextextended(:lock_scope, 0))"),
                    {"lock_scope": f"{lock_prefix}{manifest_id}"},
                )
                async with engine.connect() as contender:
                    await contender.execute(text("SET lock_timeout='1ms'"))
                    await contender.execute(
                        text("UPDATE price_series_manifests SET row_count=row_count WHERE id=:id"),
                        {"id": manifest_id},
                    )
            finally:
                await transaction.rollback()
    finally:
        await engine.dispose()


async def _seed_fosun_bar_candidate(
    database_url: str,
    *,
    interval: str,
    delivery_mode: str,
    lineage_case: str | None = None,
    authorized_at_override: datetime | None = None,
    observed_at_override: datetime | None = None,
    artifact_created_at: datetime | None = None,
    session_timezone: str = "UTC",
) -> dict[str, object]:
    engine = create_async_engine(database_url)
    capability_id = uuid4()
    status_id = uuid4()
    artifact_id = uuid4()
    raw_artifact_id = uuid4()
    calendar_id = uuid4()
    evidence_id = uuid4()
    instrument_id = uuid4()
    manifest_id = uuid4()
    authorized_at = authorized_at_override or datetime.now(UTC) - timedelta(seconds=1)
    received_at = authorized_at - timedelta(seconds=1)
    observed_at = observed_at_override or (
        received_at - (timedelta(days=1) if interval == "1Day" else timedelta(minutes=2))
    )
    artifact_created_at = artifact_created_at or received_at
    artifact_schema = "price_bars_v1" if interval == "1Day" else "intraday_price_bars_v1"
    artifact_digest = "d" * 64
    raw_digest = "e" * 64
    contract_hash = "a" * 64
    request_end_at = observed_at
    request_start_at = request_end_at - timedelta(days=400)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text("SELECT set_config('TimeZone', :timezone, true)"),
                {"timezone": session_timezone},
            )
            await connection.execute(
                text(
                    "INSERT INTO provider_capabilities "
                    "(id,provider,capability,contract_hash,expires_at,schema_version,payload,"
                    "created_at) VALUES "
                    "(:id,'fosun','READ_ONLY',:hash,:expires,'1',"
                    "jsonb_build_object('operation','fosun_get_bars','market_scope','US',"
                    "'delivery_mode',CAST(:delivery_mode AS text)),:created)"
                ),
                {
                    "id": capability_id,
                    "hash": contract_hash,
                    "expires": authorized_at + timedelta(hours=1),
                    "created": authorized_at - timedelta(hours=1),
                    "delivery_mode": delivery_mode,
                },
            )
            await connection.execute(
                text(
                    "INSERT INTO data_artifacts "
                    "(id,uri,sha256,byte_size,mime_type,schema_version,source,observed_at,"
                    "available_at,received_at,created_at) VALUES "
                    "(:id,'memory://raw/' || :digest,:digest,9,'application/json',"
                    ":schema,'fosun',:received,:received,:received,:created)"
                ),
                {
                    "id": raw_artifact_id,
                    "digest": raw_digest,
                    "schema": (
                        "fosun_gateway_market_bars_v1"
                        if interval == "1Day"
                        else "fosun_gateway_market_snapshot_v1"
                    ),
                    "received": received_at,
                    "created": artifact_created_at,
                },
            )
            if interval == "1Day":
                await connection.execute(
                    text(
                        "INSERT INTO exchange_calendar_authorities "
                        "(id,exchange,version,source,range_start,range_end,query_parameters,"
                        "completeness_status,content_hash,published_at,created_at) VALUES "
                        "(:id,'XNYS',:version,'fixture',:range_start,:range_end,'{}'::jsonb,"
                        "'complete',repeat('f',64),:published,:published)"
                    ),
                    {
                        "id": calendar_id,
                        "version": f"fosun-lineage-{calendar_id}",
                        "range_start": request_start_at.date(),
                        "range_end": authorized_at.date(),
                        "published": observed_at - timedelta(days=2),
                    },
                )
                await connection.execute(
                    text(
                        "INSERT INTO exchange_calendar_sessions "
                        "(calendar_authority_id,session,decision_at,market_open_at,"
                        "market_close_at,created_at) VALUES "
                        "(:calendar_id,:session,:decision,:market_open,:market_close,:created)"
                    ),
                    {
                        "calendar_id": calendar_id,
                        "session": observed_at.date(),
                        "decision": observed_at + timedelta(hours=1),
                        "market_open": observed_at - timedelta(hours=6, minutes=30),
                        "market_close": observed_at,
                        "created": observed_at - timedelta(days=2),
                    },
                )
            await connection.execute(
                text(
                    "INSERT INTO integration_status "
                    "(id,provider,status,last_success_at,version,mode_changed_at,"
                    "capability_invalidated_at,admitted_contract_hash,created_at) VALUES "
                    "(:id,'fosun','read_only',:received,1,:changed,NULL,:hash,:changed)"
                ),
                {
                    "id": status_id,
                    "received": received_at,
                    "changed": authorized_at - timedelta(hours=2),
                    "hash": contract_hash,
                },
            )
            await connection.execute(
                text(
                    "INSERT INTO data_artifacts "
                    "(id,uri,sha256,byte_size,mime_type,schema_version,source,observed_at,"
                    "available_at,received_at,created_at) VALUES "
                    "(:id,'memory://sha256/' || :digest,:digest,8,"
                    "'application/vnd.apache.parquet',:schema,'fosun',"
                    ":observed,:received,:received,:created)"
                ),
                {
                    "id": artifact_id,
                    "digest": artifact_digest,
                    "schema": artifact_schema,
                    "observed": observed_at,
                    "received": received_at,
                    "created": artifact_created_at,
                },
            )
            await connection.execute(
                text(
                    "INSERT INTO instruments "
                    "(id,asset_type,exchange,currency,tick_size,risk_bucket,created_at) "
                    "VALUES (:id,'common_stock','XNAS','USD',0.01,'EQUITY',:created)"
                ),
                {"id": instrument_id, "created": observed_at},
            )
            await connection.execute(
                text(
                    "INSERT INTO research_evidence_refs "
                    "(id,source,provider_object_id,revision,available_at,location,"
                    "content_hash,byte_length,created_at) VALUES "
                    "(:id,'fosun',:object_id,:digest,:received,:uri,:digest,8,:received)"
                ),
                {
                    "id": evidence_id,
                    "object_id": f"price-series-manifest:{manifest_id}",
                    "digest": artifact_digest,
                    "uri": f"memory://sha256/{artifact_digest}",
                    "received": received_at,
                },
            )
            has_transport_lineage = bool(
                await connection.scalar(
                    text(
                        "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
                        "WHERE table_schema=current_schema() "
                        "AND table_name='price_series_manifests' "
                        "AND column_name='gateway_transport_artifact_id')"
                    )
                )
            )
            await connection.execute(
                text(
                    "INSERT INTO price_series_manifests "
                    "(id,evidence_ref_id,data_artifact_id,instrument,source,feed,"
                    "delivery_mode,adjustment,interval,start_at,end_at,row_count,"
                    "provider_capability_id,provider_status_id,provider_status_version,"
                    "provider_contract_hash,provider_authorized_at"
                    + (
                        ",gateway_transport_artifact_id,daily_calendar_authority_id,"
                        "gateway_request_start_at,gateway_request_end_at"
                        if has_transport_lineage
                        else ""
                    )
                    + ",created_at) VALUES "
                    "(:id,:evidence_id,:artifact_id,:instrument,'fosun','l1',"
                    ":delivery_mode,'raw',:interval,:observed,:observed,1,"
                    ":capability_id,:status_id,1,:hash,:authorized"
                    + (
                        ",:raw_artifact_id,:calendar_id,:request_start,:request_end"
                        if has_transport_lineage
                        else ""
                    )
                    + ",:received)"
                ),
                {
                    "id": manifest_id,
                    "evidence_id": evidence_id,
                    "artifact_id": artifact_id,
                    "instrument": str(instrument_id),
                    "delivery_mode": delivery_mode,
                    "interval": interval,
                    "observed": observed_at,
                    "capability_id": capability_id,
                    "status_id": status_id,
                    "hash": contract_hash,
                    "authorized": authorized_at,
                    "raw_artifact_id": (None if lineage_case == "missing_raw" else raw_artifact_id),
                    "calendar_id": (
                        calendar_id
                        if interval == "1Day" and lineage_case != "missing_calendar"
                        else None
                    ),
                    "request_start": (request_start_at if interval == "1Day" else None),
                    "request_end": (
                        request_end_at
                        if interval == "1Day" and lineage_case != "missing_request_end"
                        else None
                    ),
                    "received": received_at,
                },
            )
            await connection.execute(
                text(
                    "INSERT INTO price_bar_records "
                    "(id,price_series_manifest_id,instrument_id,session,open,high,low,close,"
                    "volume,observed_at,available_at,received_at,revision,quality_flags,"
                    "created_at) VALUES "
                    "(:id,:manifest_id,:instrument_id,:session,100,102,99,101,1000,"
                    ":observed,:received,:received,:revision,'[]'::jsonb,:received)"
                ),
                {
                    "id": uuid4(),
                    "manifest_id": manifest_id,
                    "instrument_id": instrument_id,
                    "session": observed_at.date(),
                    "observed": observed_at,
                    "received": received_at,
                    "revision": ("0" * 64 if lineage_case == "revision_mismatch" else raw_digest),
                },
            )
        return {
            "manifest_id": manifest_id,
            "instrument_id": instrument_id,
            "provider_capability_id": capability_id,
            "provider_status_id": status_id,
            "provider_authorized_at": authorized_at,
            "gateway_transport_artifact_id": raw_artifact_id,
            "daily_calendar_authority_id": (calendar_id if interval == "1Day" else None),
            "interval": interval,
        }
    finally:
        await engine.dispose()


async def _prepare_fosun_bar_candidate(
    database_url: str,
    *,
    interval: str,
    delivery_mode: str,
) -> list[dict[str, object]]:
    seeded = await _seed_fosun_bar_candidate(
        database_url,
        interval=interval,
        delivery_mode=delivery_mode,
    )
    function = (
        "nexus_prepare_price_bar_set_v2"
        if interval == "1Day"
        else "nexus_prepare_intraday_bar_set_v2"
    )
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            rows = (
                await connection.execute(
                    text(f"SELECT * FROM {function}(:manifest_id)"),
                    {"manifest_id": seeded["manifest_id"]},
                )
            ).mappings()
            return [dict(row) for row in rows]
    finally:
        await engine.dispose()


async def _fosun_bar_valid_at(
    database_url: str,
    *,
    manifest_id: object,
    as_of: datetime,
    session_timezone: str = "UTC",
) -> bool:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            await connection.execute(
                text("SELECT set_config('TimeZone', :timezone, true)"),
                {"timezone": session_timezone},
            )
            return bool(
                await connection.scalar(
                    text("SELECT nexus_fosun_price_bar_set_valid_at(:manifest_id, :as_of)"),
                    {"manifest_id": manifest_id, "as_of": as_of},
                )
            )
    finally:
        await engine.dispose()


async def _append_fosun_calendar_session(
    database_url: str,
    seeded: Mapping[str, object],
) -> None:
    authorized_at = seeded["provider_authorized_at"]
    assert isinstance(authorized_at, datetime)
    calendar_id = seeded["daily_calendar_authority_id"]
    assert calendar_id is not None
    market_close_at = authorized_at - timedelta(minutes=31)
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO exchange_calendar_sessions "
                    "(calendar_authority_id,session,decision_at,market_open_at,"
                    "market_close_at,created_at) VALUES "
                    "(:calendar_id,:session,:decision,:market_open,:market_close,:created)"
                ),
                {
                    "calendar_id": calendar_id,
                    "session": authorized_at.date(),
                    "decision": market_close_at + timedelta(hours=1),
                    "market_open": market_close_at - timedelta(hours=6, minutes=30),
                    "market_close": market_close_at,
                    "created": market_close_at - timedelta(days=1),
                },
            )
    finally:
        await engine.dispose()


async def _contend_fosun_calendar_validation_and_append(
    database_url: str,
    seeded: Mapping[str, object],
) -> None:
    engine = create_async_engine(database_url)
    now = datetime.now(UTC)
    try:
        async with engine.connect() as validator:
            async with validator.begin():
                valid = await validator.scalar(
                    text("SELECT nexus_fosun_price_bar_set_valid_at(:manifest_id,:as_of)"),
                    {
                        "manifest_id": seeded["manifest_id"],
                        "as_of": seeded["provider_authorized_at"],
                    },
                )
                assert valid is True
                async with engine.begin() as appender:
                    await appender.execute(text("SET LOCAL lock_timeout = '50ms'"))
                    await appender.execute(
                        text(
                            "INSERT INTO exchange_calendar_sessions "
                            "(calendar_authority_id,session,decision_at,market_open_at,"
                            "market_close_at,created_at) VALUES "
                            "(:calendar_id,:session,:decision,:market_open,:market_close,"
                            ":created)"
                        ),
                        {
                            "calendar_id": seeded["daily_calendar_authority_id"],
                            "session": now.date() + timedelta(days=1),
                            "decision": now,
                            "market_open": now - timedelta(hours=7),
                            "market_close": now - timedelta(minutes=30),
                            "created": now - timedelta(days=1),
                        },
                    )
    finally:
        await engine.dispose()


async def _insert_fosun_bar_authority(
    database_url: str,
    *,
    interval: str,
    delivery_mode: str,
    policy_version: str | None = None,
) -> dict[str, object]:
    seeded = await _seed_fosun_bar_candidate(
        database_url,
        interval=interval,
        delivery_mode=delivery_mode,
    )
    daily = interval == "1Day"
    prepare_function = (
        "nexus_prepare_price_bar_set_v2" if daily else "nexus_prepare_intraday_bar_set_v2"
    )
    canonical_function = (
        "nexus_price_bar_set_canonical_v2" if daily else "nexus_intraday_bar_set_canonical_v2"
    )
    authority_table = "price_bar_set_authorities" if daily else "intraday_bar_set_authorities"
    source_policy_id = SOURCE_POLICY_V2_ID if daily else INTRADAY_POLICY_V2_ID
    hash_version = "price_bar_set_sha256_v2" if daily else "intraday_bar_set_sha256_v2"
    expected_policy_version = (
        "agent_price_authority_v2" if daily else "intraday_monitor_authority_v2"
    )
    provider_object_prefix = "price-bar-set" if daily else "intraday-bar-set"
    evidence_id = uuid4()
    authority_id = uuid4()
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            prepared = (
                (
                    await connection.execute(
                        text(f"SELECT * FROM {prepare_function}(:manifest_id)"),
                        {"manifest_id": seeded["manifest_id"]},
                    )
                )
                .mappings()
                .one()
            )
            digest = str(prepared["content_hash"])
            provider_object_id = f"{provider_object_prefix}:{seeded['manifest_id']}"
            location = f"db://price-bar-set/{seeded['manifest_id']}@sha256:{digest}"
            await connection.execute(
                text(
                    "INSERT INTO research_evidence_refs "
                    "(id,source,provider_object_id,revision,available_at,location,"
                    "content_hash,byte_length,created_at) VALUES "
                    "(:id,'fosun',:object_id,:digest,:available,:location,:digest,"
                    ":byte_length,:available)"
                ),
                {
                    "id": evidence_id,
                    "object_id": provider_object_id,
                    "digest": digest,
                    "available": prepared["available_at"],
                    "location": location,
                    "byte_length": prepared["byte_length"],
                },
            )
            if daily:
                await connection.execute(
                    text(
                        "INSERT INTO price_bar_set_authorities "
                        "(id,price_series_manifest_id,evidence_ref_id,source_policy_id,"
                        "source,provider_object_id,revision,hash_version,policy_version,"
                        "content_hash,byte_length,bar_count,first_session,last_session,"
                        "available_at,created_at) VALUES "
                        "(:id,:manifest_id,:evidence_id,:policy_id,'fosun',:object_id,"
                        ":digest,:hash_version,:policy_version,:digest,:byte_length,"
                        ":bar_count,:first_boundary,:last_boundary,:available,:available)"
                    ),
                    {
                        "id": authority_id,
                        "manifest_id": seeded["manifest_id"],
                        "evidence_id": evidence_id,
                        "policy_id": source_policy_id,
                        "object_id": provider_object_id,
                        "digest": digest,
                        "hash_version": hash_version,
                        "policy_version": policy_version or expected_policy_version,
                        "byte_length": prepared["byte_length"],
                        "bar_count": prepared["bar_count"],
                        "first_boundary": prepared["first_session"],
                        "last_boundary": prepared["last_session"],
                        "available": prepared["available_at"],
                    },
                )
            else:
                await connection.execute(
                    text(
                        "INSERT INTO intraday_bar_set_authorities "
                        "(id,price_series_manifest_id,evidence_ref_id,source_policy_id,"
                        "source,provider_object_id,revision,hash_version,policy_version,"
                        "content_hash,byte_length,bar_count,first_observed_at,"
                        "last_observed_at,available_at,created_at) VALUES "
                        "(:id,:manifest_id,:evidence_id,:policy_id,'fosun',:object_id,"
                        ":digest,:hash_version,:policy_version,:digest,:byte_length,"
                        ":bar_count,:first_boundary,:last_boundary,:available,:available)"
                    ),
                    {
                        "id": authority_id,
                        "manifest_id": seeded["manifest_id"],
                        "evidence_id": evidence_id,
                        "policy_id": source_policy_id,
                        "object_id": provider_object_id,
                        "digest": digest,
                        "hash_version": hash_version,
                        "policy_version": policy_version or expected_policy_version,
                        "byte_length": prepared["byte_length"],
                        "bar_count": prepared["bar_count"],
                        "first_boundary": prepared["first_observed_at"],
                        "last_boundary": prepared["last_observed_at"],
                        "available": prepared["available_at"],
                    },
                )
            canonical = await connection.scalar(
                text(f"SELECT {canonical_function}(:manifest_id)"),
                {"manifest_id": seeded["manifest_id"]},
            )
        return {
            **seeded,
            "authority_id": authority_id,
            "evidence_id": evidence_id,
            "hash_version": hash_version,
            "policy_version": policy_version or expected_policy_version,
            "content_hash": digest,
            "byte_length": int(prepared["byte_length"]),
            "available_at": prepared["available_at"],
            "provider_object_id": provider_object_id,
            "location": location,
            "canonical": canonical,
            "authority_table": authority_table,
        }
    finally:
        await engine.dispose()


async def _insert_agent_price_binding(
    database_url: str,
    authority: dict[str, object],
) -> UUID:
    snapshot_id = uuid4()
    selected_at = authority["available_at"] + timedelta(seconds=1)  # type: ignore[operator]
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO data_snapshots "
                    "(id,source,provider_object_id,revision,observed_at,available_at,"
                    "received_at,schema_version,payload,created_at) VALUES "
                    "(:id,'fixture','fosun-v2-agent-snapshot','v1',:selected,:selected,"
                    ":selected,'1','{}'::jsonb,:selected)"
                ),
                {"id": snapshot_id, "selected": selected_at},
            )
            await connection.execute(
                text(
                    "INSERT INTO agent_data_snapshot_evidence "
                    "(data_snapshot_id,instrument_id,evidence_ref_id,authority_kind,selected_at,"
                    "source,provider_object_id,revision,available_at,location,content_hash,"
                    "created_at) VALUES "
                    "(:snapshot_id,:instrument_id,:evidence_id,'price',:selected,'fosun',"
                    ":object_id,:digest,:available,:location,:digest,:selected)"
                ),
                {
                    "snapshot_id": snapshot_id,
                    "instrument_id": authority["instrument_id"],
                    "evidence_id": authority["evidence_id"],
                    "selected": selected_at,
                    "object_id": authority["provider_object_id"],
                    "digest": authority["content_hash"],
                    "available": authority["available_at"],
                    "location": authority["location"],
                },
            )
        return snapshot_id
    finally:
        await engine.dispose()


async def _v2_schema(database_url: str) -> dict[str, set[str]]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            functions = set(
                (
                    await connection.execute(
                        text(
                            "SELECT proname FROM pg_proc "
                            "WHERE pronamespace=current_schema()::regnamespace "
                            "AND proname IN ('nexus_price_bar_set_canonical_v2',"
                            "'nexus_prepare_price_bar_set_v2',"
                            "'nexus_intraday_bar_set_canonical_v2',"
                            "'nexus_prepare_intraday_bar_set_v2')"
                        )
                    )
                ).scalars()
            )
            constraints = set(
                (
                    await connection.execute(
                        text(
                            "SELECT conname FROM pg_constraint "
                            "WHERE conrelid IN ('price_bar_set_authorities'::regclass,"
                            "'intraday_bar_set_authorities'::regclass) "
                            "AND conname LIKE '%hash_policy_version_tuple'"
                        )
                    )
                ).scalars()
            )
            columns = set(
                (
                    await connection.execute(
                        text(
                            "SELECT table_name || '.' || column_name "
                            "FROM information_schema.columns "
                            "WHERE table_schema=current_schema() "
                            "AND table_name IN ('price_series_manifests','latest_quotes') "
                            "AND column_name LIKE 'provider_%'"
                        )
                    )
                ).scalars()
            )
            return {
                "functions": {str(value) for value in functions},
                "constraints": {str(value) for value in constraints},
                "columns": {str(value) for value in columns},
            }
    finally:
        await engine.dispose()
