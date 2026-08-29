"""Migration authority for immutable Fosun market fact bindings."""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from nexus.persistence.base import Base
from nexus.persistence.models import data as data_models
from nexus.persistence.models import market as market_models
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

assert data_models is not None
assert market_models is not None

SOURCE_POLICY_V2_ID = UUID("f780fa7f-9135-5004-ab21-d665ae7be24b")
QUOTE_POLICY_V2_ID = UUID("15afb077-46c6-5eea-8307-a7d203d11c2d")
INTRADAY_POLICY_V2_ID = UUID("9bd1eb52-5486-5f40-a3d8-06bd56f80b8d")


def test_0027_is_single_head_and_metadata_exposes_complete_binding() -> None:
    scripts = ScriptDirectory.from_config(Config("engine/alembic.ini"))

    assert scripts.get_heads() == ["0027_fosun_market_authority"]
    revision = scripts.get_revision("0027_fosun_market_authority")
    assert revision is not None
    assert revision.down_revision == "0026_intraday_review_hardening"
    expected = {
        "provider_capability_id",
        "provider_status_id",
        "provider_status_version",
        "provider_contract_hash",
        "provider_authorized_at",
    }
    assert expected <= set(Base.metadata.tables["price_series_manifests"].columns.keys())
    assert expected <= set(Base.metadata.tables["latest_quotes"].columns.keys())


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


def test_0027_price_prepare_holds_manifest_share_lock(
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

    assert error.value.orig.sqlstate == "55P03"


def test_0027_binding_guard_holds_artifact_share_lock(
    migrated_database_url: str,
) -> None:
    with pytest.raises(DBAPIError) as error:
        asyncio.run(_contend_with_quote_artifact_lock(migrated_database_url))

    assert error.value.orig.sqlstate == "55P03"


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


def test_0027_downgrade_refuses_bound_fosun_fact(
    temporary_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config("engine/alembic.ini")
    command.upgrade(config, "head")
    asyncio.run(_insert_fosun_quote(temporary_database_url))

    with pytest.raises(DBAPIError, match="Fosun market authority prevents downgrade") as error:
        command.downgrade(config, "0026_intraday_review_hardening")

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
    quote_id = uuid4()
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
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
                    "'[\"FOSUN_PROVIDER_TIMESTAMP_UNAVAILABLE\","
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
                    **binding,
                },
            )
        return quote_id
    finally:
        await engine.dispose()


async def _mutate_capability(database_url: str, capability_id: UUID) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text("UPDATE provider_capabilities SET expires_at=expires_at + interval '1 hour' "
                     "WHERE id=:id"),
                {"id": capability_id},
            )
    finally:
        await engine.dispose()


async def _mutate_bound_manifest(database_url: str, manifest_id: object) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "UPDATE price_series_manifests SET row_count=row_count + 1 "
                    "WHERE id=:id"
                ),
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
                    await contender.execute(
                        text(
                            "SELECT id FROM price_series_manifests WHERE id=:id "
                            "FOR UPDATE NOWAIT"
                        ),
                        {"id": manifest_id},
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
                        "'[\"FOSUN_PROVIDER_TIMESTAMP_UNAVAILABLE\","
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
                    await contender.execute(
                        text(
                            "SELECT id FROM data_artifacts WHERE id=:id "
                            "FOR NO KEY UPDATE NOWAIT"
                        ),
                        {"id": artifact_id},
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
) -> dict[str, object]:
    engine = create_async_engine(database_url)
    capability_id = uuid4()
    status_id = uuid4()
    artifact_id = uuid4()
    evidence_id = uuid4()
    instrument_id = uuid4()
    manifest_id = uuid4()
    authorized_at = datetime.now(UTC) - timedelta(seconds=1)
    received_at = authorized_at - timedelta(seconds=1)
    observed_at = received_at - (
        timedelta(days=1) if interval == "1Day" else timedelta(minutes=2)
    )
    artifact_schema = (
        "price_bars_v1" if interval == "1Day" else "intraday_price_bars_v1"
    )
    artifact_digest = "d" * 64
    contract_hash = "a" * 64
    try:
        async with engine.begin() as connection:
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
                    ":observed,:received,:received,:received)"
                ),
                {
                    "id": artifact_id,
                    "digest": artifact_digest,
                    "schema": artifact_schema,
                    "observed": observed_at,
                    "received": received_at,
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
            await connection.execute(
                text(
                    "INSERT INTO price_series_manifests "
                    "(id,evidence_ref_id,data_artifact_id,instrument,source,feed,"
                    "delivery_mode,adjustment,interval,start_at,end_at,row_count,"
                    "provider_capability_id,provider_status_id,provider_status_version,"
                    "provider_contract_hash,provider_authorized_at,created_at) VALUES "
                    "(:id,:evidence_id,:artifact_id,:instrument,'fosun','l1',"
                    ":delivery_mode,'raw',:interval,:observed,:observed,1,"
                    ":capability_id,:status_id,1,:hash,:authorized,:received)"
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
                    ":observed,:received,:received,'gateway-v1','[]'::jsonb,:received)"
                ),
                {
                    "id": uuid4(),
                    "manifest_id": manifest_id,
                    "instrument_id": instrument_id,
                    "session": observed_at.date(),
                    "observed": observed_at,
                    "received": received_at,
                },
            )
        return {
            "manifest_id": manifest_id,
            "instrument_id": instrument_id,
            "provider_capability_id": capability_id,
            "provider_status_id": status_id,
            "provider_authorized_at": authorized_at,
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
        "nexus_prepare_price_bar_set_v2"
        if daily
        else "nexus_prepare_intraday_bar_set_v2"
    )
    canonical_function = (
        "nexus_price_bar_set_canonical_v2"
        if daily
        else "nexus_intraday_bar_set_canonical_v2"
    )
    authority_table = (
        "price_bar_set_authorities" if daily else "intraday_bar_set_authorities"
    )
    source_policy_id = SOURCE_POLICY_V2_ID if daily else INTRADAY_POLICY_V2_ID
    hash_version = (
        "price_bar_set_sha256_v2" if daily else "intraday_bar_set_sha256_v2"
    )
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
                await connection.execute(
                    text(f"SELECT * FROM {prepare_function}(:manifest_id)"),
                    {"manifest_id": seeded["manifest_id"]},
                )
            ).mappings().one()
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
