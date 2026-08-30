"""Read-only PostgreSQL coverage for the Provider startup catalog probe."""

from datetime import UTC, date, datetime
from hashlib import sha256
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from helpers.price_bar_authority import INSTRUMENT_ID, seed_fosun_price_candidate
from nexus.jobs.daily_payloads import instrument_alias_identity_sha256
from nexus.jobs.provider_collection import _require_frozen_instrument_alias
from nexus.market.price_bar_authority import (
    HASH_VERSION_V2,
    POLICY_VERSION_V2,
    SOURCE_POLICY_V2_ID,
    prepare_price_bar_set,
)
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.data import PriceSeriesManifest
from nexus.persistence.models.market import InstrumentAlias
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.persistence.session import session_factory
from nexus.provider_worker import (
    _PROVIDER_PRIVILEGES,
    _assert_provider_database_role,
    _assert_provider_intraday_bucket_index,
    _assert_provider_migration_head,
    _assert_provider_ticker_alias_constraints,
    _load_provider_trigger_rows,
    _provider_trigger_topology_violation,
)
from nexus.settings import EngineSettings
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.sql.elements import TextClause


@pytest.mark.asyncio
async def test_provider_role_probe_queries_catalogs_before_rejecting_admin_role(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))

    async with factory() as session:
        with pytest.raises(RuntimeError, match="PROVIDER_DATABASE_"):
            await _assert_provider_database_role(session)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("role_mutation", "expected_error"),
    (
        ("NOLOGIN", "PROVIDER_DATABASE_SESSION_ROLE_INVALID"),
        ("DATABASE_CREATE", "PROVIDER_DATABASE_CREATE_PRIVILEGE_INVALID"),
    ),
)
async def test_provider_role_probe_rejects_login_or_database_create_drift(
    migrated_database_url: str,
    role_mutation: str,
    expected_error: str,
) -> None:
    admin_engine = create_async_engine(migrated_database_url)
    role_name = f"nexus_provider_gate_{uuid4().hex}"
    password = "synthetic_provider_gate_only"
    quoted_role = f'"{role_name}"'
    provider_url = make_url(migrated_database_url).set(
        username=role_name,
        password=password,
    )
    provider_engine = create_async_engine(provider_url)
    try:
        async with admin_engine.begin() as connection:
            await connection.execute(text(f"CREATE ROLE {quoted_role} LOGIN PASSWORD '{password}'"))
            await connection.execute(text(f"GRANT USAGE ON SCHEMA public TO {quoted_role}"))

        async with provider_engine.connect() as provider_connection:
            await provider_connection.execute(text("SELECT 1"))
            async with admin_engine.begin() as admin_connection:
                if role_mutation == "NOLOGIN":
                    await admin_connection.execute(text(f"ALTER ROLE {quoted_role} NOLOGIN"))
                else:
                    database_name = await admin_connection.scalar(text("SELECT current_database()"))
                    assert isinstance(database_name, str)
                    quoted_database = '"' + database_name.replace('"', '""') + '"'
                    await admin_connection.execute(
                        text(f"GRANT CREATE ON DATABASE {quoted_database} TO {quoted_role}")
                    )

            async with AsyncSession(bind=provider_connection) as session:
                with pytest.raises(RuntimeError, match=expected_error):
                    await _assert_provider_database_role(session)
    finally:
        await provider_engine.dispose()
        async with admin_engine.begin() as connection:
            await connection.execute(text(f"DROP OWNED BY {quoted_role}"))
            await connection.execute(text(f"DROP ROLE {quoted_role}"))
        await admin_engine.dispose()


@pytest.mark.asyncio
async def test_fresh_migration_matches_frozen_provider_trigger_topology(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))

    async with factory() as session:
        rows = await _load_provider_trigger_rows(session)

    # The migration fixture connects as the object owner; the direct-login Provider
    # acceptance gate separately requires this field to be false.
    for row in rows:
        row["provider_is_owner_member"] = False
    assert _provider_trigger_topology_violation(rows) is None


@pytest.mark.asyncio
async def test_provider_probe_accepts_exact_ticker_alias_exclusion_constraints(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))

    async with factory() as session:
        await _assert_provider_ticker_alias_constraints(session)


@pytest.mark.asyncio
async def test_provider_probe_rejects_a_dropped_ticker_alias_exclusion_constraint(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await session.execute(
            text(
                "ALTER TABLE instrument_aliases DROP CONSTRAINT "
                "ex_instrument_aliases_ticker_symbol_period"
            )
        )

    async with factory() as session:
        with pytest.raises(
            RuntimeError,
            match="PROVIDER_DATABASE_TICKER_ALIAS_CONSTRAINT_INVALID",
        ):
            await _assert_provider_ticker_alias_constraints(session)


@pytest.mark.asyncio
async def test_provider_probe_accepts_exact_intraday_bucket_index(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))

    async with factory() as session:
        await _assert_provider_intraday_bucket_index(session)


@pytest.mark.asyncio
async def test_provider_probe_rejects_a_dropped_intraday_bucket_index(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await session.execute(text("DROP INDEX public.uq_jobs_intraday_v2_bucket"))

    async with factory() as session:
        with pytest.raises(
            RuntimeError,
            match="PROVIDER_DATABASE_INTRADAY_BUCKET_INDEX_INVALID",
        ):
            await _assert_provider_intraday_bucket_index(session)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    ("disable_status_lock", "rename_capability_lock", "drift_append_only_source"),
)
async def test_provider_trigger_probe_rejects_authority_input_trigger_drift(
    migrated_database_url: str,
    mutation: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    statements = {
        "disable_status_lock": (
            "ALTER TABLE integration_status DISABLE TRIGGER "
            "trg_00_integration_status_fosun_authority_lock"
        ),
        "rename_capability_lock": (
            "ALTER TRIGGER trg_provider_capabilities_scope_lock "
            "ON provider_capabilities RENAME TO trg_unreviewed_capability_scope_lock"
        ),
        "drift_append_only_source": (
            "CREATE OR REPLACE FUNCTION nexus_prevent_provider_capability_change() "
            "RETURNS trigger LANGUAGE plpgsql "
            "SET search_path TO pg_catalog, public, pg_temp AS $$ "
            "BEGIN RETURN OLD; END; $$"
        ),
    }
    async with factory() as session, session.begin():
        await session.execute(text(statements[mutation]))
    async with factory() as session:
        rows = await _load_provider_trigger_rows(session)

    for row in rows:
        row["provider_is_owner_member"] = False
    assert (
        _provider_trigger_topology_violation(rows) == "PROVIDER_DATABASE_TRIGGER_TOPOLOGY_INVALID"
    )


@pytest.mark.asyncio
async def test_restricted_provider_role_can_only_select_alembic_head(
    migrated_database_url: str,
) -> None:
    engine = create_async_engine(migrated_database_url)
    role_name = f"nexus_provider_head_{uuid4().hex}"
    quoted_role = f'"{role_name}"'
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f"CREATE ROLE {quoted_role} NOLOGIN"))
            await connection.execute(text(f"GRANT USAGE ON SCHEMA public TO {quoted_role}"))
            await connection.execute(
                text(f"GRANT SELECT ON TABLE public.alembic_version TO {quoted_role}")
            )

        async with engine.begin() as connection:
            await connection.execute(text(f"SET LOCAL ROLE {quoted_role}"))
            await _assert_provider_migration_head(connection)  # type: ignore[arg-type]
            assert (
                await connection.scalar(text("SELECT version_num FROM public.alembic_version"))
                == CURRENT_MIGRATION_HEAD
            )
            for statement in (
                "INSERT INTO public.alembic_version(version_num) VALUES ('forbidden')",
                "UPDATE public.alembic_version SET version_num=version_num",
                "DELETE FROM public.alembic_version",
            ):
                with pytest.raises(DBAPIError) as denied:
                    async with connection.begin_nested():
                        await connection.execute(text(statement))
                assert denied.value.orig.sqlstate == "42501"
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f"DROP OWNED BY {quoted_role}"))
            await connection.execute(text(f"DROP ROLE {quoted_role}"))
        await engine.dispose()


@pytest.mark.asyncio
async def test_restricted_provider_role_can_revalidate_frozen_daily_alias(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await seed_fosun_price_candidate(factory)
    alias_id = uuid4()
    created_at = datetime(2026, 8, 24, 19, tzinfo=UTC)
    async with factory() as session, session.begin():
        session.add(
            InstrumentAlias(
                id=alias_id,
                instrument_id=INSTRUMENT_ID,
                symbol="AAPL",
                alias_type="ticker",
                valid_from=date(2020, 1, 1),
                valid_to=None,
                created_at=created_at,
            )
        )

    identity = instrument_alias_identity_sha256(
        alias_id=str(alias_id),
        instrument_id=str(INSTRUMENT_ID),
        symbol="AAPL",
        alias_type="ticker",
        valid_from=date(2020, 1, 1),
        valid_to=None,
        created_at=created_at,
    )
    engine = create_async_engine(migrated_database_url)
    role_name = f"nexus_provider_alias_{uuid4().hex}"
    quoted_role = f'"{role_name}"'
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f"CREATE ROLE {quoted_role} NOLOGIN"))
            await connection.execute(text(f"GRANT USAGE ON SCHEMA public TO {quoted_role}"))
            await connection.execute(
                text(f"GRANT SELECT ON public.instrument_aliases TO {quoted_role}")
            )
        async with engine.connect() as connection:
            transaction = await connection.begin()
            try:
                await connection.execute(text(f"SET LOCAL ROLE {quoted_role}"))
                async with AsyncSession(bind=connection) as session:
                    await _require_frozen_instrument_alias(
                        session,
                        SimpleNamespace(
                            session="2026-08-28",
                            instrument_id=str(INSTRUMENT_ID),
                            instrument_alias_id=str(alias_id),
                            instrument_symbol="AAPL",
                            instrument_alias_identity_sha256=identity,
                        ),  # type: ignore[arg-type]
                    )
            finally:
                await transaction.rollback()
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f"DROP OWNED BY {quoted_role}"))
            await connection.execute(text(f"DROP ROLE {quoted_role}"))
        await engine.dispose()


@pytest.mark.asyncio
async def test_restricted_provider_role_publishes_only_trigger_scoped_marks(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    first_manifest_id = uuid4()
    second_manifest_id = uuid4()
    await seed_fosun_price_candidate(factory, manifest_id=first_manifest_id)
    await seed_fosun_price_candidate(factory, manifest_id=second_manifest_id)
    first = await _prepare_price_authority(factory, first_manifest_id)
    second = await _prepare_price_authority(factory, second_manifest_id)
    provider_manifest_id = uuid4()
    provider_bar_id = uuid4()
    provider_manifest_evidence_id = uuid4()
    provider_manifest_digest = sha256(str(provider_manifest_id).encode()).hexdigest()
    async with factory() as session:
        original_manifest = await session.get(PriceSeriesManifest, first_manifest_id)
        assert original_manifest is not None
        provider_manifest = {
            "id": provider_manifest_id,
            "evidence_id": provider_manifest_evidence_id,
            "artifact_id": original_manifest.data_artifact_id,
            "gateway_artifact_id": original_manifest.gateway_transport_artifact_id,
            "calendar_id": original_manifest.daily_calendar_authority_id,
            "request_start": original_manifest.gateway_request_start_at,
            "request_end": original_manifest.gateway_request_end_at,
            "instrument": original_manifest.instrument,
            "source": original_manifest.source,
            "feed": original_manifest.feed,
            "delivery_mode": original_manifest.delivery_mode,
            "adjustment": original_manifest.adjustment,
            "interval": original_manifest.interval,
            "start_at": original_manifest.start_at,
            "end_at": original_manifest.end_at,
            "row_count": original_manifest.row_count,
            "capability_id": original_manifest.provider_capability_id,
            "status_id": original_manifest.provider_status_id,
            "status_version": original_manifest.provider_status_version,
            "contract_hash": original_manifest.provider_contract_hash,
            "authorized_at": original_manifest.provider_authorized_at,
        }
    action_id, action_evidence_id = uuid4(), uuid4()
    action_digest = sha256(str(action_id).encode()).hexdigest()
    now = datetime(2026, 8, 30, 12, tzinfo=UTC)
    async with factory() as session, session.begin():
        session.add(
            ResearchEvidenceRef(
                id=action_evidence_id,
                source="alpaca",
                provider_object_id=f"provider-action:{action_id}",
                revision="1",
                available_at=now,
                location=f"memory://sha256/{action_digest}",
                content_hash=action_digest,
                byte_length=1,
            )
        )

    engine = create_async_engine(migrated_database_url)
    role_name = f"nexus_provider_test_{uuid4().hex}"
    quoted_role = f'"{role_name}"'
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f"CREATE ROLE {quoted_role} NOLOGIN"))
            await connection.execute(text(f"GRANT USAGE ON SCHEMA public TO {quoted_role}"))
            await connection.execute(
                text(
                    "GRANT SELECT ON TABLE "
                    + ",".join(
                        f"public.{table_name}" for table_name in sorted(_PROVIDER_PRIVILEGES)
                    )
                    + f" TO {quoted_role}"
                )
            )
            await connection.execute(
                text(
                    "GRANT INSERT ON TABLE public.price_bar_set_authorities,"
                    "public.corporate_actions,public.research_evidence_refs,"
                    "public.price_series_manifests,public.price_bar_records "
                    f"TO {quoted_role}"
                )
            )
        async with engine.begin() as connection:
            await connection.execute(text(f"SET LOCAL ROLE {quoted_role}"))
            with pytest.raises(DBAPIError) as denied:
                async with connection.begin_nested():
                    await connection.execute(
                        text(
                            "INSERT INTO public.backtest_authority_publication_marks "
                            "(domain,record_id,created_at) VALUES "
                            "('price',:record_id,clock_timestamp())"
                        ),
                        {"record_id": uuid4()},
                    )
            assert denied.value.orig.sqlstate == "42501"

        async with engine.begin() as connection:
            await connection.execute(text(f"SET LOCAL ROLE {quoted_role}"))
            await connection.execute(
                text(
                    "INSERT INTO public.research_evidence_refs "
                    "(id,source,provider_object_id,revision,available_at,location,"
                    "content_hash,byte_length) VALUES "
                    "(:id,'fosun',:object_id,:digest,:available_at,:location,:digest,1)"
                ),
                {
                    "id": provider_manifest_evidence_id,
                    "object_id": f"restricted-manifest:{provider_manifest_id}",
                    "digest": provider_manifest_digest,
                    "available_at": provider_manifest["authorized_at"],
                    "location": f"memory://sha256/{provider_manifest_digest}",
                },
            )
            await connection.execute(
                text(
                    "INSERT INTO public.price_series_manifests "
                    "(id,evidence_ref_id,data_artifact_id,gateway_transport_artifact_id,"
                    "daily_calendar_authority_id,gateway_request_start_at,"
                    "gateway_request_end_at,instrument,source,feed,delivery_mode,adjustment,"
                    "interval,start_at,end_at,row_count,provider_capability_id,"
                    "provider_status_id,provider_status_version,provider_contract_hash,"
                    "provider_authorized_at) VALUES "
                    "(:id,:evidence_id,:artifact_id,:gateway_artifact_id,:calendar_id,"
                    ":request_start,:request_end,:instrument,:source,:feed,:delivery_mode,"
                    ":adjustment,:interval,:start_at,:end_at,:row_count,:capability_id,"
                    ":status_id,:status_version,:contract_hash,:authorized_at)"
                ),
                provider_manifest,
            )
            await connection.execute(
                text(
                    "INSERT INTO public.price_bar_records "
                    "(id,price_series_manifest_id,instrument_id,session,open,high,low,"
                    "close,volume,observed_at,available_at,received_at,revision,"
                    "quality_flags) SELECT :bar_id,:provider_manifest_id,instrument_id,"
                    "session,open,high,low,close,volume,observed_at,available_at,"
                    "received_at,revision,quality_flags FROM public.price_bar_records "
                    "WHERE price_series_manifest_id=:source_manifest_id "
                    "ORDER BY observed_at,revision LIMIT 1"
                ),
                {
                    "bar_id": provider_bar_id,
                    "provider_manifest_id": provider_manifest_id,
                    "source_manifest_id": first_manifest_id,
                },
            )
            await connection.execute(_authority_insert(), first)
            await connection.execute(
                text(
                    "INSERT INTO public.corporate_actions "
                    "(id,evidence_ref_id,source,provider_event_id,action_type,status,"
                    "process_date,revision) VALUES "
                    "(:id,:evidence_id,'alpaca',:provider_event_id,'cash_dividend',"
                    "'complete',:process_date,'1')"
                ),
                {
                    "id": action_id,
                    "evidence_id": action_evidence_id,
                    "provider_event_id": f"provider-action:{action_id}",
                    "process_date": date(2026, 8, 30),
                },
            )

        async with engine.begin() as connection:
            await connection.execute(text(f"SET LOCAL ROLE {quoted_role}"))
            await connection.execute(
                text(
                    "CREATE TEMP TABLE backtest_authority_publication_marks "
                    "(domain text NOT NULL,record_id uuid NOT NULL,created_at timestamptz "
                    "NOT NULL,PRIMARY KEY(domain,record_id))"
                )
            )
            await connection.execute(text("SET LOCAL search_path=pg_temp,public"))
            await connection.execute(_authority_insert(), second)
            assert (
                await connection.scalar(
                    text("SELECT count(*) FROM pg_temp.backtest_authority_publication_marks")
                )
                == 0
            )
            await connection.execute(
                text("DROP TABLE pg_temp.backtest_authority_publication_marks")
            )

        async with engine.connect() as connection:
            marks = (
                (
                    await connection.execute(
                        text(
                            "SELECT domain,record_id FROM "
                            "public.backtest_authority_publication_marks "
                            "WHERE record_id IN (:first,:second,:action) "
                            "ORDER BY domain,record_id"
                        ),
                        {
                            "first": first["id"],
                            "second": second["id"],
                            "action": action_id,
                        },
                    )
                )
                .tuples()
                .all()
            )
            assert set(marks) == {
                ("price", first["id"]),
                ("price", second["id"]),
                ("corporate_action", action_id),
            }
            assert (
                await connection.scalar(
                    text("SELECT count(*) FROM public.price_series_manifests WHERE id=:id"),
                    {"id": provider_manifest_id},
                )
                == 1
            )
            assert (
                await connection.scalar(
                    text("SELECT count(*) FROM public.price_bar_records WHERE id=:id"),
                    {"id": provider_bar_id},
                )
                == 1
            )
    finally:
        async with engine.begin() as connection:
            await connection.execute(text(f"DROP OWNED BY {quoted_role}"))
            await connection.execute(text(f"DROP ROLE {quoted_role}"))
        await engine.dispose()


async def _prepare_price_authority(
    factory: async_sessionmaker[AsyncSession],
    manifest_id: UUID,
) -> dict[str, object]:
    async with factory() as session, session.begin():
        manifest = await session.get(PriceSeriesManifest, manifest_id)
        assert manifest is not None
        prepared = await prepare_price_bar_set(
            session,
            manifest_id,
            as_of=manifest.provider_authorized_at,
        )
        assert prepared is not None
        evidence = ResearchEvidenceRef(
            source=manifest.source,
            provider_object_id=f"price-bar-set:{manifest_id}",
            revision=prepared.content_hash,
            available_at=prepared.available_at,
            location=(f"db://price-bar-set/{manifest_id}@sha256:{prepared.content_hash}"),
            content_hash=prepared.content_hash,
            byte_length=prepared.byte_length,
        )
        session.add(evidence)
        await session.flush()
        return {
            "id": uuid4(),
            "manifest_id": manifest_id,
            "evidence_id": evidence.id,
            "policy_id": SOURCE_POLICY_V2_ID,
            "source": manifest.source,
            "provider_object_id": evidence.provider_object_id,
            "digest": prepared.content_hash,
            "hash_version": HASH_VERSION_V2,
            "policy_version": POLICY_VERSION_V2,
            "byte_length": prepared.byte_length,
            "bar_count": prepared.bar_count,
            "first_session": prepared.first_session,
            "last_session": prepared.last_session,
            "available_at": prepared.available_at,
        }


def _authority_insert() -> TextClause:
    return text(
        "INSERT INTO public.price_bar_set_authorities "
        "(id,price_series_manifest_id,evidence_ref_id,source_policy_id,source,"
        "provider_object_id,revision,hash_version,policy_version,content_hash,byte_length,"
        "bar_count,first_session,last_session,available_at) VALUES "
        "(:id,:manifest_id,:evidence_id,:policy_id,:source,:provider_object_id,:digest,"
        ":hash_version,:policy_version,:digest,:byte_length,:bar_count,:first_session,"
        ":last_session,:available_at)"
    )
