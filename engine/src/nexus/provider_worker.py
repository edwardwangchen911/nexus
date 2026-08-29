"""Provider-only composition root with no Agent or broker capability."""

from __future__ import annotations

import asyncio
import re
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol, cast

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from nexus.domain.ids import new_uuid7
from nexus.jobs.daily_profiles import PRIMARY_DAILY_PROFILE
from nexus.jobs.intraday_collection import (
    IntradaySnapshotProvider,
    build_intraday_collection_registry,
)
from nexus.jobs.intraday_schedule import schedule_due_intraday_snapshot
from nexus.jobs.models import QueueName
from nexus.jobs.provider_collection import RawEvidenceStore, build_provider_collection_registry
from nexus.jobs.registry import JobHandlerSpec, JobRegistry
from nexus.jobs.worker import run_job_worker_once
from nexus.market.ingestion import MarketObjectStore
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.providers.composition import (
    FosunGatewayFactory,
    build_provider_bundle,
)
from nexus.providers.http import AllowedOrigin, FixedOriginHttpClient, HttpRequestPolicy
from nexus.providers.market.fosun_gateway import FosunMarketGatewayClient
from nexus.providers.settings import (
    IntradayMonitorMode,
    ProviderRuntimeMode,
    ProviderSettings,
    ProviderWorkerSettings,
)

_PROCESS_VERSION = "nexus-provider-phase3-intraday"
_PROVIDER_PRIVILEGES: dict[str, frozenset[str]] = {
    "jobs": frozenset({"SELECT", "INSERT", "UPDATE"}),
    "job_leases": frozenset({"SELECT", "INSERT", "UPDATE"}),
    "job_attempts": frozenset({"SELECT", "INSERT", "UPDATE"}),
    "job_dependencies": frozenset({"SELECT"}),
    "worker_heartbeats": frozenset({"SELECT", "INSERT", "UPDATE"}),
    "integration_status": frozenset({"SELECT"}),
    "provider_capabilities": frozenset({"SELECT"}),
    "source_policies": frozenset({"SELECT"}),
    "instruments": frozenset({"SELECT"}),
    "instrument_aliases": frozenset({"SELECT"}),
    "universe_snapshots": frozenset({"SELECT"}),
    "universe_members": frozenset({"SELECT"}),
    "watchlists": frozenset({"SELECT"}),
    "watchlist_members": frozenset({"SELECT"}),
    "exchange_calendar_authorities": frozenset({"SELECT"}),
    "exchange_calendar_sessions": frozenset({"SELECT"}),
    "data_artifacts": frozenset({"SELECT", "INSERT"}),
    "research_evidence_refs": frozenset({"SELECT", "INSERT"}),
    "price_series_manifests": frozenset({"SELECT", "INSERT"}),
    "price_bar_records": frozenset({"SELECT", "INSERT"}),
    "price_bar_set_authorities": frozenset({"SELECT", "INSERT"}),
    "intraday_bar_set_authorities": frozenset({"SELECT", "INSERT"}),
    "latest_quotes": frozenset({"SELECT", "INSERT"}),
    "filings": frozenset({"SELECT", "INSERT"}),
    "financial_facts": frozenset({"SELECT", "INSERT"}),
    "corporate_actions": frozenset({"SELECT", "INSERT"}),
    "macro_observations": frozenset({"SELECT", "INSERT"}),
}
_ROW_SCOPED_PROVIDER_TABLES = frozenset(
    {"jobs", "job_leases", "job_attempts", "job_dependencies", "worker_heartbeats"}
)


@dataclass(frozen=True)
class _PolicyFunctionSpec:
    identity_arguments: str
    source: str
    volatility: str


_PROVIDER_POLICY_FUNCTIONS = {
    "nexus_provider_job_visible": _PolicyFunctionSpec(
        identity_arguments="p_queue text, p_job_type text",
        source=(
            "SELECT (p_queue = 'provider' AND p_job_type IN "
            "('collect.market','collect.sec_macro','collect.intraday_snapshot')) "
            "OR (p_queue = 'general' AND p_job_type = 'daily.pipeline')"
        ),
        volatility="i",
    ),
    "nexus_provider_job_writable": _PolicyFunctionSpec(
        identity_arguments="p_queue text, p_job_type text",
        source=(
            "SELECT p_queue = 'provider' AND p_job_type IN "
            "('collect.market','collect.sec_macro','collect.intraday_snapshot')"
        ),
        volatility="i",
    ),
    "nexus_provider_job_owned": _PolicyFunctionSpec(
        identity_arguments="p_job_id uuid",
        source=(
            "SELECT EXISTS (SELECT 1 FROM public.jobs AS job WHERE job.id = p_job_id "
            "AND public.nexus_provider_job_writable(job.queue, job.job_type))"
        ),
        volatility="s",
    ),
    "nexus_provider_queue": _PolicyFunctionSpec(
        identity_arguments="p_queue text",
        source="SELECT p_queue = 'provider'",
        volatility="i",
    ),
}
_PROVIDER_POLICY_EXPRESSIONS = {
    "jobs": (
        "nexus_provider_job_visible(queue, job_type)",
        "nexus_provider_job_writable(queue, job_type)",
    ),
    "job_leases": (
        "nexus_provider_job_owned(job_id)",
        "nexus_provider_job_owned(job_id)",
    ),
    "job_attempts": (
        "nexus_provider_job_owned(job_id)",
        "nexus_provider_job_owned(job_id)",
    ),
    "job_dependencies": (
        "nexus_provider_job_owned(job_id)",
        "nexus_provider_job_owned(job_id)",
    ),
    "worker_heartbeats": (
        "nexus_provider_queue(queue)",
        "nexus_provider_queue(queue)",
    ),
}


class ProviderStore(MarketObjectStore, RawEvidenceStore, Protocol):
    """Combined create-only provider evidence boundary."""


@asynccontextmanager
async def _provider_registry(
    *,
    provider_settings: ProviderSettings,
    provider_http_client: httpx.AsyncClient,
    store: ProviderStore,
    fosun_gateway_factory: FosunGatewayFactory = FosunMarketGatewayClient,
) -> AsyncIterator[JobRegistry]:
    """Compose only provider jobs; construction performs no external request."""

    def http_factory(
        *,
        provider_name: str,
        origin: AllowedOrigin,
        policy: HttpRequestPolicy,
    ) -> FixedOriginHttpClient:
        return FixedOriginHttpClient(
            provider_name=provider_name,
            origin=origin,
            policy=policy,
            client=provider_http_client,
        )

    bundle = build_provider_bundle(
        provider_settings,
        http_client_factory=http_factory,
        fosun_gateway_factory=fosun_gateway_factory,
    )
    try:
        provider = build_provider_collection_registry(
            bundle,
            store,
            store,
            lambda: datetime.now(UTC),
            expected_profile_id=PRIMARY_DAILY_PROFILE,
        )
        intraday_specs: tuple[JobHandlerSpec, ...] = ()
        if provider_settings.intraday_monitor_mode is IntradayMonitorMode.PRODUCTION:

            def snapshot_provider(session: object) -> IntradaySnapshotProvider:
                return cast(
                    IntradaySnapshotProvider,
                    bundle.snapshot_provider(session),
                )

            intraday_specs = build_intraday_collection_registry(
                snapshot_provider,
                store,
                lambda: datetime.now(UTC),
                settings=provider_settings,
            ).specs
        yield JobRegistry((*provider.specs, *intraday_specs))
    finally:
        await bundle.aclose()


async def run_provider_worker(
    runtime_settings: ProviderWorkerSettings | None = None,
    provider_settings: ProviderSettings | None = None,
    *,
    run_forever: bool = False,
) -> None:
    """Poll only the provider queue and schedule provider-owned data jobs."""
    runtime = runtime_settings or ProviderWorkerSettings.model_validate({})
    configured = provider_settings or ProviderSettings.model_validate({})
    if configured.runtime_mode is not ProviderRuntimeMode.PRODUCTION:
        raise RuntimeError("PROVIDER_RUNTIME_PRODUCTION_REQUIRED")
    engine = create_async_engine(
        runtime.database_url,
        pool_pre_ping=True,
        connect_args={"server_settings": {"search_path": "public,pg_catalog"}},
    )
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            await _assert_provider_database_role(session)
        if not run_forever:
            await asyncio.sleep(0)
            return
        store = S3ProviderEvidenceStore(runtime)
        async with httpx.AsyncClient(follow_redirects=False, timeout=None) as client:
            async with _provider_registry(
                provider_settings=configured,
                provider_http_client=client,
                store=store,
            ) as registry:
                worker_id = f"provider-{new_uuid7()}"
                started_at = datetime.now(UTC)
                while True:
                    now = datetime.now(UTC)
                    if configured.intraday_monitor_mode is IntradayMonitorMode.PRODUCTION:
                        async with factory() as session, session.begin():
                            await schedule_due_intraday_snapshot(
                                session,
                                now=now,
                                settings=configured,
                            )
                    worked = await run_job_worker_once(
                        factory,
                        registry=registry,
                        queue=QueueName.PROVIDER,
                        worker_id=worker_id,
                        worker_started_at=started_at,
                        process_version=_PROCESS_VERSION,
                        migration_head=CURRENT_MIGRATION_HEAD,
                        clock=lambda: datetime.now(UTC),
                        claim_registered_only=True,
                    )
                    if not worked:
                        await asyncio.sleep(0.5)
    finally:
        await engine.dispose()


def main() -> None:
    asyncio.run(run_provider_worker(run_forever=True))


async def _assert_provider_database_role(session: AsyncSession) -> None:
    role = (
        await session.execute(
            text(
                "SELECT current_user AS role_name, role.rolsuper AS superuser, "
                "role.rolbypassrls AS bypass_rls, role.rolcreatedb AS create_db, "
                "role.rolcreaterole AS create_role, role.rolreplication AS replication, "
                "has_schema_privilege(current_user, 'public', 'USAGE') AS schema_usage, "
                "has_schema_privilege(current_user, 'public', 'CREATE') AS schema_create, "
                "EXISTS (SELECT 1 FROM pg_roles AS granted_role "
                "WHERE granted_role.oid <> current_user::regrole "
                "AND pg_has_role(current_user::regrole, granted_role.oid, 'MEMBER')) "
                "AS has_role_memberships "
                "FROM pg_roles AS role WHERE role.rolname = current_user"
            )
        )
    ).mappings().one()
    rows = (
        await session.execute(
            text(
                "SELECT relation.relname AS table_name, relation.relrowsecurity AS row_security, "
                "relation.relforcerowsecurity AS force_row_security, "
                "has_table_privilege(current_user, relation.oid, 'SELECT') AS can_select, "
                "has_table_privilege(current_user, relation.oid, 'INSERT') AS can_insert, "
                "has_table_privilege(current_user, relation.oid, 'UPDATE') AS can_update, "
                "has_table_privilege(current_user, relation.oid, 'DELETE') AS can_delete, "
                "has_table_privilege(current_user, relation.oid, 'TRUNCATE') AS can_truncate, "
                "has_table_privilege(current_user, relation.oid, 'REFERENCES') AS can_references, "
                "has_table_privilege(current_user, relation.oid, 'TRIGGER') AS can_trigger, "
                "has_table_privilege(current_user, relation.oid, 'MAINTAIN') AS can_maintain, "
                "has_any_column_privilege(current_user, relation.oid, 'SELECT') "
                "AS can_column_select, "
                "has_any_column_privilege(current_user, relation.oid, 'INSERT') "
                "AS can_column_insert, "
                "has_any_column_privilege(current_user, relation.oid, 'UPDATE') "
                "AS can_column_update, "
                "has_any_column_privilege(current_user, relation.oid, 'REFERENCES') "
                "AS can_column_references "
                "FROM pg_class AS relation "
                "WHERE relation.relnamespace = 'public'::regnamespace "
                "AND relation.relkind IN ('r','p')"
            )
        )
    ).mappings().all()
    policy_rows = (
        await session.execute(
            text(
                "SELECT policy.tablename AS table_name, policy.policyname AS policy_name, "
                "policy.permissive, policy.roles, policy.cmd AS command, "
                "policy.qual, policy.with_check "
                "FROM pg_policies AS policy WHERE policy.schemaname = 'public' "
                "AND policy.tablename IN ('jobs','job_leases','job_attempts',"
                "'job_dependencies','worker_heartbeats')"
            )
        )
    ).mappings().all()
    function_rows = (
        await session.execute(
            text(
                "SELECT procedure.proname AS function_name, "
                "pg_get_function_identity_arguments(procedure.oid) AS identity_arguments, "
                "procedure.prosrc AS source, procedure.provolatile AS volatility, "
                "procedure.proparallel AS parallel_safety, "
                "procedure.prosecdef AS security_definer, "
                "COALESCE('search_path=public, pg_catalog' = ANY(procedure.proconfig), false) "
                "AS search_path_fixed, language.lanname AS language, "
                "procedure.prorettype = 'boolean'::regtype AS returns_boolean, "
                "procedure.proowner = jobs_table.relowner AS trusted_owner, "
                "pg_has_role(current_user::regrole, procedure.proowner, 'MEMBER') "
                "AS provider_is_owner_member, "
                "has_function_privilege(current_user, procedure.oid, 'EXECUTE') "
                "AS provider_execute, "
                "EXISTS (SELECT 1 FROM aclexplode(COALESCE(procedure.proacl, "
                "acldefault('f', procedure.proowner))) AS acl "
                "WHERE acl.privilege_type = 'EXECUTE' AND acl.grantee = 0) "
                "AS public_execute, "
                "EXISTS (SELECT 1 FROM aclexplode(COALESCE(procedure.proacl, "
                "acldefault('f', procedure.proowner))) AS acl "
                "WHERE acl.privilege_type = 'EXECUTE' "
                "AND acl.grantee = current_user::regrole AND acl.is_grantable) "
                "AS provider_execute_grantable, "
                "NOT EXISTS (SELECT 1 FROM aclexplode(COALESCE(procedure.proacl, "
                "acldefault('f', procedure.proowner))) AS acl "
                "WHERE acl.privilege_type = 'EXECUTE' AND acl.grantee NOT IN "
                "(procedure.proowner, current_user::regrole)) "
                "AS acl_only_owner_and_provider, "
                "NOT EXISTS (SELECT 1 FROM aclexplode(COALESCE(procedure.proacl, "
                "acldefault('f', procedure.proowner))) AS acl "
                "WHERE acl.privilege_type = 'EXECUTE' "
                "AND acl.grantor <> procedure.proowner) AS acl_grantor_only_owner "
                "FROM pg_proc AS procedure "
                "JOIN pg_language AS language ON language.oid = procedure.prolang "
                "JOIN pg_class AS jobs_table ON jobs_table.oid = 'public.jobs'::regclass "
                "WHERE procedure.pronamespace = 'public'::regnamespace "
                "AND procedure.proname IN ("
                "'nexus_provider_job_visible','nexus_provider_job_writable',"
                "'nexus_provider_job_owned','nexus_provider_queue')"
            )
        )
    ).mappings().all()
    violation = _provider_privilege_violation(
        superuser=bool(role["superuser"]),
        bypass_rls=bool(role["bypass_rls"]),
        create_db=bool(role["create_db"]),
        create_role=bool(role["create_role"]),
        replication=bool(role["replication"]),
        role_memberships=bool(role["has_role_memberships"]),
        schema_usage=bool(role["schema_usage"]),
        schema_create=bool(role["schema_create"]),
        rows=[dict(row) for row in rows],
    )
    if violation is not None:
        raise RuntimeError(violation)
    policy_violation = _provider_policy_violation(
        [dict(row) for row in policy_rows],
        provider_role=str(role["role_name"]),
    )
    if policy_violation is not None:
        raise RuntimeError(policy_violation)
    function_violation = _provider_policy_function_violation(
        [dict(row) for row in function_rows]
    )
    if function_violation is not None:
        raise RuntimeError(function_violation)


def _provider_privilege_violation(
    *,
    superuser: bool,
    bypass_rls: bool,
    create_db: bool,
    create_role: bool,
    replication: bool,
    role_memberships: bool = False,
    schema_usage: bool,
    schema_create: bool,
    rows: Sequence[Mapping[str, object]],
) -> str | None:
    if superuser:
        return "PROVIDER_DATABASE_ROLE_SUPERUSER"
    if bypass_rls:
        return "PROVIDER_DATABASE_ROLE_BYPASSRLS"
    if create_db or create_role or replication:
        return "PROVIDER_DATABASE_ROLE_PRIVILEGED"
    if role_memberships:
        return "PROVIDER_DATABASE_ROLE_MEMBERSHIP"
    if not schema_usage or schema_create:
        return "PROVIDER_DATABASE_SCHEMA_PRIVILEGES_INVALID"
    table_rows = {str(row["table_name"]): row for row in rows}
    for table_name, required in _PROVIDER_PRIVILEGES.items():
        row = table_rows.get(table_name)
        if row is None:
            return "PROVIDER_DATABASE_SCHEMA_INCOMPLETE"
        actual = {
            privilege
            for privilege, column in (
                ("SELECT", "can_select"),
                ("INSERT", "can_insert"),
                ("UPDATE", "can_update"),
                ("DELETE", "can_delete"),
                ("TRUNCATE", "can_truncate"),
                ("REFERENCES", "can_references"),
                ("TRIGGER", "can_trigger"),
                ("MAINTAIN", "can_maintain"),
            )
            if bool(row[column])
        }
        if actual != required:
            return "PROVIDER_DATABASE_PRIVILEGES_INVALID"
        if any(
            bool(row[column]) and privilege not in required
            for privilege, column in (
                ("SELECT", "can_column_select"),
                ("INSERT", "can_column_insert"),
                ("UPDATE", "can_column_update"),
                ("REFERENCES", "can_column_references"),
            )
        ):
            return "PROVIDER_DATABASE_PRIVILEGES_INVALID"
        if table_name in _ROW_SCOPED_PROVIDER_TABLES and not (
            bool(row["row_security"]) and bool(row["force_row_security"])
        ):
            return "PROVIDER_DATABASE_ROW_SCOPE_REQUIRED"
    for table_name, row in table_rows.items():
        if table_name in _PROVIDER_PRIVILEGES:
            continue
        if any(
            bool(row[column])
            for column in (
                "can_select",
                "can_insert",
                "can_update",
                "can_delete",
                "can_truncate",
                "can_references",
                "can_trigger",
                "can_maintain",
                "can_column_select",
                "can_column_insert",
                "can_column_update",
                "can_column_references",
            )
        ):
            return "PROVIDER_DATABASE_PRIVILEGES_TOO_BROAD"
    return None


def _provider_policy_violation(
    rows: Sequence[Mapping[str, object]],
    *,
    provider_role: str,
) -> str | None:
    expected: dict[tuple[str, str], tuple[tuple[str, ...], str, str]] = {}
    escaped_role = provider_role.replace("'", "''")
    bypass = f"NOT pg_has_role(CURRENT_USER, '{escaped_role}', 'MEMBER')"
    for table_name, (using, with_check) in _PROVIDER_POLICY_EXPRESSIONS.items():
        expected[(table_name, f"nexus_provider_{table_name}_scope")] = (
            (provider_role,),
            using,
            with_check,
        )
        expected[(table_name, f"nexus_non_provider_{table_name}_bypass")] = (
            ("public",),
            bypass,
            bypass,
        )
    actual = {
        (str(row["table_name"]), str(row["policy_name"])): row for row in rows
    }
    if len(actual) != len(rows) or set(actual) != set(expected):
        return "PROVIDER_DATABASE_POLICY_SET_INVALID"
    for key, (roles, using, with_check) in expected.items():
        row = actual[key]
        actual_roles = row["roles"]
        if not isinstance(actual_roles, Sequence) or isinstance(actual_roles, str):
            return "PROVIDER_DATABASE_POLICY_SET_INVALID"
        if (
            tuple(str(role) for role in actual_roles) != roles
            or row["permissive"] != "PERMISSIVE"
            or row["command"] != "ALL"
            or _normalize_policy_expression(row["qual"])
            != _normalize_policy_expression(using)
            or _normalize_policy_expression(row["with_check"])
            != _normalize_policy_expression(with_check)
        ):
            return "PROVIDER_DATABASE_POLICY_SET_INVALID"
    return None


def _provider_policy_function_violation(
    rows: Sequence[Mapping[str, object]],
) -> str | None:
    actual = {str(row["function_name"]): row for row in rows}
    if len(actual) != len(rows) or set(actual) != set(_PROVIDER_POLICY_FUNCTIONS):
        return "PROVIDER_DATABASE_POLICY_FUNCTION_INVALID"
    for name, expected in _PROVIDER_POLICY_FUNCTIONS.items():
        row = actual[name]
        if (
            row["identity_arguments"] != expected.identity_arguments
            or _normalize_sql_source(row["source"]) != _normalize_sql_source(expected.source)
            or row["volatility"] != expected.volatility
            or row["parallel_safety"] != "s"
            or bool(row["security_definer"])
            or not bool(row["search_path_fixed"])
            or row["language"] != "sql"
            or not bool(row["returns_boolean"])
            or not bool(row["trusted_owner"])
            or bool(row["provider_is_owner_member"])
            or not bool(row["provider_execute"])
            or bool(row["public_execute"])
            or bool(row["provider_execute_grantable"])
            or not bool(row["acl_only_owner_and_provider"])
            or not bool(row["acl_grantor_only_owner"])
        ):
            return "PROVIDER_DATABASE_POLICY_FUNCTION_INVALID"
    return None


def _normalize_policy_expression(value: object) -> str:
    if not isinstance(value, str):
        return ""
    normalized = value.lower().replace('"', "").replace("public.", "")
    normalized = re.sub(r"::(?:text|character varying|name)", "", normalized)
    return re.sub(r"[\s()]", "", normalized)


def _normalize_sql_source(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return re.sub(r"\s+", " ", value.strip().rstrip(";")).lower()
