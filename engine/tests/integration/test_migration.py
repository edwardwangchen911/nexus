import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from nexus.integrations.service import IntegrationStatusService
from nexus.persistence.base import Base
from nexus.persistence.models import data, market, runtime  # noqa: F401
from nexus.persistence.session import session_factory
from nexus.quality.source_policy import (
    QUOTE_SOURCE_POLICY_ID,
    QUOTE_SOURCE_POLICY_PAYLOAD,
)
from nexus.settings import EngineSettings
from sqlalchemy import DateTime, inspect, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine

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
    assert "delivery_mode" in _table_columns(
        temporary_database_url, "price_series_manifests"
    )

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


def _seed_0001_integration_status(
    database_url: str,
    created_at: datetime,
    *,
    status: str = "fixture",
    with_valid_capability: bool = False,
) -> None:
    asyncio.run(
        _seed_0001_integration_status_async(
            database_url,
            created_at,
            status,
            with_valid_capability,
        )
    )


async def _seed_0001_integration_status_async(
    database_url: str,
    created_at: datetime,
    status: str,
    with_valid_capability: bool,
) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO integration_status "
                    "(id, provider, status, last_success_at, created_at) "
                    "VALUES (:id, :provider, :status, :last_success_at, :created_at)"
                ),
                {
                    "id": uuid4(),
                    "provider": "fosun",
                    "status": status,
                    "last_success_at": created_at,
                    "created_at": created_at,
                },
            )
            if with_valid_capability:
                await connection.execute(
                    text(
                        "INSERT INTO provider_capabilities "
                        "(id, provider, capability, contract_hash, expires_at, "
                        "schema_version, payload, created_at) "
                        "VALUES (:id, :provider, :capability, :contract_hash, :expires_at, "
                        ":schema_version, CAST(:payload AS jsonb), :created_at)"
                    ),
                    {
                        "id": uuid4(),
                        "provider": "fosun",
                        "capability": "READ_ONLY",
                        "contract_hash": "a" * 64,
                        "expires_at": created_at + timedelta(days=1),
                        "schema_version": "1",
                        "payload": "{}",
                        "created_at": created_at,
                    },
                )
    finally:
        await engine.dispose()


def _assert_legacy_read_only_is_effectively_disabled(database_url: str, now: datetime) -> None:
    asyncio.run(_assert_legacy_read_only_is_effectively_disabled_async(database_url, now))


async def _assert_legacy_read_only_is_effectively_disabled_async(
    database_url: str, now: datetime
) -> None:
    factory = session_factory(EngineSettings(database_url=database_url))
    async with factory() as session:
        service = IntegrationStatusService(session, now)
        persisted = await service.find_status("fosun")
        capability = await service.capability("fosun")
        response = service.response("fosun", persisted, capability)
    assert response.mode == "disabled"
    assert response.quality == "FAIL"
    assert response.contract_hash == "a" * 64
    assert response.capability_expires_at == now + timedelta(hours=23)
    assert response.next_action == "A valid capability admission is required."


def _integration_status_row(
    database_url: str,
    *,
    columns: tuple[str, ...] = (
        "version",
        "mode_changed_at",
        "capability_invalidated_at",
        "admitted_contract_hash",
    ),
) -> dict[str, object]:
    return asyncio.run(_integration_status_row_async(database_url, columns))


def _insert_provider_capability(
    database_url: str,
    *,
    contract_hash: str,
    operation: str,
) -> None:
    asyncio.run(
        _insert_provider_capability_async(
            database_url,
            contract_hash=contract_hash,
            operation=operation,
        )
    )


async def _insert_provider_capability_async(
    database_url: str,
    *,
    contract_hash: str,
    operation: str,
) -> None:
    now = datetime(2026, 8, 23, 12, tzinfo=UTC)
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO provider_capabilities "
                    "(id, provider, capability, contract_hash, expires_at, "
                    "schema_version, payload, created_at) "
                    "VALUES (:id, 'fosun', 'READ_ONLY', :contract_hash, :expires_at, "
                    "'1', CAST(:payload AS jsonb), :created_at)"
                ),
                {
                    "id": uuid4(),
                    "contract_hash": contract_hash,
                    "expires_at": now + timedelta(hours=1),
                    "payload": json.dumps({"operation": operation, "market_scope": "US"}),
                    "created_at": now,
                },
            )
    finally:
        await engine.dispose()


def _provider_capability_scopes(database_url: str) -> list[tuple[str, str, str]]:
    return asyncio.run(_provider_capability_scopes_async(database_url))


async def _provider_capability_scopes_async(
    database_url: str,
) -> list[tuple[str, str, str]]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            rows = (
                await connection.execute(
                    text(
                        "SELECT contract_hash, payload ->> 'operation', "
                        "payload ->> 'market_scope' FROM provider_capabilities "
                        "ORDER BY payload ->> 'operation'"
                    )
                )
            ).tuples()
            return [(row[0], row[1], row[2]) for row in rows]
    finally:
        await engine.dispose()


async def _integration_status_row_async(
    database_url: str, columns: tuple[str, ...]
) -> dict[str, object]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            row = (
                await connection.execute(
                    text(
                        "SELECT provider, status"
                        + "".join(f", {column}" for column in columns)
                        + " "
                        "FROM integration_status"
                    )
                )
            ).mappings().one()
            return dict(row)
    finally:
        await engine.dispose()


def _integration_status_columns(database_url: str) -> set[str]:
    return asyncio.run(_integration_status_columns_async(database_url))


async def _integration_status_columns_async(database_url: str) -> set[str]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            return await connection.run_sync(
                lambda sync_connection: {
                    column["name"]
                    for column in inspect(sync_connection).get_columns("integration_status")
                }
            )
    finally:
        await engine.dispose()


def _assert_schema(database_url: str) -> None:
    asyncio.run(_assert_schema_async(database_url))


async def _assert_schema_async(database_url: str) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            await connection.run_sync(_assert_reflected_schema)
    finally:
        await engine.dispose()


def _assert_reflected_schema(connection: Connection) -> None:
    inspector = inspect(connection)
    assert set(inspector.get_table_names()) == EXPECTED_TABLES | {"alembic_version"}
    assert {inspector.get_pk_constraint(table_name)["name"] for table_name in EXPECTED_TABLES} == {
        f"pk_{table_name}" for table_name in EXPECTED_TABLES
    }
    assert {constraint["name"] for constraint in inspector.get_unique_constraints("jobs")} == {
        "uq_jobs_job_key"
    }
    idempotency_constraints = inspector.get_unique_constraints("idempotency_records")
    assert {constraint["name"] for constraint in idempotency_constraints} == {
        "uq_idempotency_records_actor_route_key"
    }
    nonce_constraints = inspector.get_unique_constraints("used_nonces")
    assert {constraint["name"] for constraint in nonce_constraints} == {
        "uq_used_nonces_key_nonce"
    }
    for table_name, foreign_key_names in EXPECTED_FOREIGN_KEYS.items():
        expected_names = (
            foreign_key_names if isinstance(foreign_key_names, set) else {foreign_key_names}
        )
        assert {
            foreign_key["name"] for foreign_key in inspector.get_foreign_keys(table_name)
        } == expected_names

    artifacts = {column["name"]: column for column in inspector.get_columns("data_artifacts")}
    assert isinstance(artifacts["id"]["type"], postgresql.UUID)
    assert artifacts["uri"]["nullable"] is False
    observed_at_type = artifacts["observed_at"]["type"]
    assert isinstance(observed_at_type, DateTime)
    assert observed_at_type.timezone is True
    integration_status = {
        column["name"]: column for column in inspector.get_columns("integration_status")
    }
    assert integration_status["version"]["nullable"] is False
    assert isinstance(integration_status["version"]["type"], postgresql.INTEGER)
    assert integration_status["mode_changed_at"]["nullable"] is False
    assert isinstance(integration_status["mode_changed_at"]["type"], DateTime)
    assert integration_status["mode_changed_at"]["type"].timezone is True
    assert integration_status["capability_invalidated_at"]["nullable"] is True
    assert isinstance(integration_status["capability_invalidated_at"]["type"], DateTime)
    assert integration_status["capability_invalidated_at"]["type"].timezone is True
    assert isinstance(integration_status["admitted_contract_hash"]["type"], postgresql.VARCHAR)
    macro_columns = {
        column["name"]: column for column in inspector.get_columns("macro_observations")
    }
    macro_value = macro_columns["value"]
    assert isinstance(macro_value["type"], postgresql.NUMERIC)
    assert macro_value["type"].precision == 38
    assert macro_value["type"].scale == 12
    for table_name in EXPECTED_TABLES:
        assert _created_at_trigger_exists(connection, table_name)

    context = MigrationContext.configure(connection)
    assert compare_metadata(context, Base.metadata) == []


def _created_at_trigger_exists(connection: Connection, table_name: str) -> bool:
    trigger_name = f"trg_{table_name}_created_at_immutable"
    return bool(
        connection.execute(
            text(
                "SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = :trigger_name "
                "AND NOT tgisinternal)"
            ),
            {"trigger_name": trigger_name},
        ).scalar_one()
    )


def _table_names(database_url: str) -> set[str]:
    return asyncio.run(_table_names_async(database_url))


async def _table_names_async(database_url: str) -> set[str]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            return await connection.run_sync(
                lambda sync_connection: set(inspect(sync_connection).get_table_names())
            )
    finally:
        await engine.dispose()


def _table_columns(database_url: str, table_name: str) -> set[str]:
    async def inspect_columns() -> set[str]:
        engine = create_async_engine(database_url)
        try:
            async with engine.connect() as connection:
                return await connection.run_sync(
                    lambda sync_connection: {
                        column["name"]
                        for column in inspect(sync_connection).get_columns(table_name)
                    }
                )
        finally:
            await engine.dispose()

    return asyncio.run(inspect_columns())


def _quote_source_policy_record(database_url: str) -> dict[str, object] | None:
    async def load_record() -> dict[str, object] | None:
        engine = create_async_engine(database_url)
        try:
            async with engine.connect() as connection:
                row = (
                    await connection.execute(
                        text(
                            "SELECT id, key, version, schema_version, payload "
                            "FROM source_policies WHERE id = :id"
                        ),
                        {"id": QUOTE_SOURCE_POLICY_ID},
                    )
                ).mappings().one_or_none()
                return None if row is None else dict(row)
        finally:
            await engine.dispose()

    return asyncio.run(load_record())


def _seed_0005_delivery_rows(database_url: str) -> None:
    async def seed_rows() -> None:
        engine = create_async_engine(database_url)
        instrument_id = uuid4()
        fixture_artifact_id, alpaca_artifact_id = uuid4(), uuid4()
        observed_at = datetime(2026, 8, 22, tzinfo=UTC)
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "INSERT INTO instruments "
                        "(id, asset_type, exchange, currency, tick_size, risk_bucket) "
                        "VALUES (:id, 'common_stock', 'XNYS', 'USD', 0.01, 'EQUITY')"
                    ),
                    {"id": instrument_id},
                )
                for source, feed in (("fixture", "fixture"), ("alpaca", "iex")):
                    await connection.execute(
                        text(
                            "INSERT INTO latest_quotes "
                            "(id, instrument_id, source, feed, bid, ask, last, observed_at, "
                            "available_at, received_at, revision, quality_flags) VALUES "
                            "(:id, :instrument_id, :source, :feed, NULL, NULL, 100, "
                            ":observed_at, :observed_at, :observed_at, '1', '[]'::jsonb)"
                        ),
                        {
                            "id": uuid4(),
                            "instrument_id": instrument_id,
                            "source": source,
                            "feed": feed,
                            "observed_at": observed_at,
                        },
                    )
                for artifact_id, source, digest in (
                    (fixture_artifact_id, "fixture", "a" * 64),
                    (alpaca_artifact_id, "alpaca", "b" * 64),
                ):
                    await connection.execute(
                        text(
                            "INSERT INTO data_artifacts "
                            "(id, uri, sha256, byte_size, mime_type, schema_version, source, "
                            "observed_at, available_at, received_at) VALUES "
                            "(:id, :uri, :sha256, 1, 'application/parquet', '1', :source, "
                            ":observed_at, :observed_at, :observed_at)"
                        ),
                        {
                            "id": artifact_id,
                            "uri": f"memory://{source}",
                            "sha256": digest,
                            "source": source,
                            "observed_at": observed_at,
                        },
                    )
                    await connection.execute(
                        text(
                            "INSERT INTO price_series_manifests "
                            "(id, data_artifact_id, instrument, source, feed, adjustment, "
                            "start_at, end_at, row_count) VALUES "
                            "(:id, :artifact_id, :instrument, :source, :feed, 'all', "
                            ":observed_at, :observed_at, 1)"
                        ),
                        {
                            "id": uuid4(),
                            "artifact_id": artifact_id,
                            "instrument": str(instrument_id),
                            "source": source,
                            "feed": "fixture" if source == "fixture" else "sip",
                            "observed_at": observed_at,
                        },
                    )
        finally:
            await engine.dispose()

    asyncio.run(seed_rows())


def _delivery_mode_rows(database_url: str, table_name: str) -> list[tuple[str, str]]:
    if table_name not in {"latest_quotes", "price_series_manifests"}:
        raise ValueError("unsupported delivery-mode table")

    async def load_rows() -> list[tuple[str, str]]:
        engine = create_async_engine(database_url)
        try:
            async with engine.connect() as connection:
                rows = await connection.execute(
                    text(f"SELECT source, delivery_mode FROM {table_name} ORDER BY source")
                )
                return [(str(source), str(mode)) for source, mode in rows]
        finally:
            await engine.dispose()

    return asyncio.run(load_rows())


def _set_fixture_delivery_mode(
    database_url: str, table_name: str, *, delivery_mode: str
) -> None:
    if table_name not in {"latest_quotes", "price_series_manifests"}:
        raise ValueError("unsupported delivery-mode table")

    async def update_row() -> None:
        engine = create_async_engine(database_url)
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        f"UPDATE {table_name} SET delivery_mode = :delivery_mode "
                        "WHERE source = 'fixture'"
                    ),
                    {"delivery_mode": delivery_mode},
                )
        finally:
            await engine.dispose()

    asyncio.run(update_row())
