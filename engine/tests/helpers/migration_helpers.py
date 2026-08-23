"""Shared PostgreSQL migration assertions and data setup."""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from nexus.integrations.service import IntegrationStatusService
from nexus.persistence.base import Base
from nexus.persistence.models import data, market, portfolio, runtime  # noqa: F401
from nexus.persistence.session import session_factory
from nexus.quality.source_policy import QUOTE_SOURCE_POLICY_ID
from nexus.settings import EngineSettings
from sqlalchemy import DateTime, inspect, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from helpers.migration_schema_expectations import EXPECTED_FOREIGN_KEYS, EXPECTED_TABLES


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
                (
                    await connection.execute(
                        text(
                            "SELECT provider, status"
                            + "".join(f", {column}" for column in columns)
                            + " "
                            "FROM integration_status"
                        )
                    )
                )
                .mappings()
                .one()
            )
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
    actual_tables = set(inspector.get_table_names()) - {"alembic_version"}
    diagnostic_tables = {
        "diagnostic_policies",
        "portfolio_diagnostic_reports",
        "portfolio_diagnostic_items",
        "portfolio_diagnostic_evidence_refs",
    }
    expected_tables = (
        EXPECTED_TABLES
        if diagnostic_tables <= actual_tables
        else EXPECTED_TABLES - diagnostic_tables
    )
    assert actual_tables == expected_tables
    assert {inspector.get_pk_constraint(table_name)["name"] for table_name in expected_tables} == {
        f"pk_{table_name}" for table_name in expected_tables
    }
    assert {constraint["name"] for constraint in inspector.get_unique_constraints("jobs")} == {
        "uq_jobs_job_key"
    }
    idempotency_constraints = inspector.get_unique_constraints("idempotency_records")
    assert {constraint["name"] for constraint in idempotency_constraints} == {
        "uq_idempotency_records_actor_route_key"
    }
    nonce_constraints = inspector.get_unique_constraints("used_nonces")
    assert {constraint["name"] for constraint in nonce_constraints} == {"uq_used_nonces_key_nonce"}
    for table_name, foreign_key_names in EXPECTED_FOREIGN_KEYS.items():
        if table_name not in actual_tables:
            continue
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
    for table_name in expected_tables:
        assert _created_at_trigger_exists(connection, table_name)
    if diagnostic_tables <= actual_tables:
        evidence_columns = {
            column["name"]: column
            for column in inspector.get_columns("portfolio_diagnostic_evidence_refs")
        }
        available_at = evidence_columns["available_at"]["type"]
        assert isinstance(available_at, DateTime)
        assert available_at.timezone is True

    context = MigrationContext.configure(connection)
    if diagnostic_tables <= actual_tables:
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
                    (
                        await connection.execute(
                            text(
                                "SELECT id, key, version, schema_version, payload "
                                "FROM source_policies WHERE id = :id"
                            ),
                            {"id": QUOTE_SOURCE_POLICY_ID},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
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


def _set_fixture_delivery_mode(database_url: str, table_name: str, *, delivery_mode: str) -> None:
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
