"""Provider worker capability and queue isolation."""

from types import SimpleNamespace

import httpx
import pytest
from nexus.jobs.models import JobType, QueueName
from nexus.provider_worker import (
    _PROVIDER_POLICY_EXPRESSIONS,
    _PROVIDER_POLICY_FUNCTIONS,
    _PROVIDER_PRIVILEGES,
    _provider_policy_function_violation,
    _provider_policy_violation,
    _provider_privilege_violation,
    _provider_registry,
    run_provider_worker,
)
from nexus.providers.settings import ProviderSettings, ProviderWorkerSettings


def _settings(*, intraday: bool) -> ProviderSettings:
    return ProviderSettings(
        runtime_mode="production",
        intraday_monitor_mode="production" if intraday else "disabled",
        alpaca_key_id="key-canary",
        alpaca_secret_key="secret-canary",
        alpaca_feed="sip",
        alpaca_delivery_mode="delayed",
        alpaca_intraday_feed="iex" if intraday else None,
        alpaca_intraday_delivery_mode="real_time" if intraday else None,
        sec_contact="owner@example.com",
        fred_api_key="fred-canary",
    )


@pytest.mark.asyncio
async def test_provider_registry_contains_only_provider_capabilities_without_http() -> None:
    requests = 0

    def fail_if_requested(_request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(599)

    async with httpx.AsyncClient(transport=httpx.MockTransport(fail_if_requested)) as client:
        registry = _provider_registry(
            provider_settings=_settings(intraday=True),
            provider_http_client=client,
            store=SimpleNamespace(),
        )

    assert registry.job_types == {
        JobType.COLLECT_MARKET.value,
        JobType.COLLECT_SEC_MACRO.value,
        JobType.COLLECT_INTRADAY_SNAPSHOT.value,
    }
    assert all(spec.queue is QueueName.PROVIDER for spec in registry.specs)
    assert requests == 0


@pytest.mark.asyncio
async def test_provider_worker_rejects_fixture_runtime_before_opening_resources() -> None:
    runtime = ProviderWorkerSettings(
        database_url="postgresql+asyncpg://fixture.invalid/fixture",
        object_store_endpoint="https://object-store.invalid",
        object_store_region="us-east-1",
        object_store_bucket="fixture",
        object_store_access_key_id="access-canary",
        object_store_secret_access_key="secret-canary",
    )

    with pytest.raises(RuntimeError, match="PROVIDER_RUNTIME_PRODUCTION_REQUIRED"):
        await run_provider_worker(
            runtime,
            ProviderSettings(runtime_mode="fixture"),
            run_forever=False,
        )


def test_provider_database_role_rejects_superuser_broad_access_and_missing_rls() -> None:
    assert (
        _provider_privilege_violation(
            superuser=True,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            schema_usage=True,
            schema_create=False,
            rows=[],
        )
        == "PROVIDER_DATABASE_ROLE_SUPERUSER"
    )
    assert (
        _provider_privilege_violation(
            superuser=False,
            bypass_rls=True,
            create_db=False,
            create_role=False,
            replication=False,
            schema_usage=True,
            schema_create=False,
            rows=[],
        )
        == "PROVIDER_DATABASE_ROLE_BYPASSRLS"
    )
    assert (
        _provider_privilege_violation(
            superuser=False,
            bypass_rls=False,
            create_db=True,
            create_role=False,
            replication=False,
            schema_usage=True,
            schema_create=False,
            rows=[],
        )
        == "PROVIDER_DATABASE_ROLE_PRIVILEGED"
    )
    assert (
        _provider_privilege_violation(
            superuser=False,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            role_memberships=True,
            schema_usage=True,
            schema_create=False,
            rows=[],
        )
        == "PROVIDER_DATABASE_ROLE_MEMBERSHIP"
    )
    assert (
        _provider_privilege_violation(
            superuser=False,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            schema_usage=True,
            schema_create=True,
            rows=[],
        )
        == "PROVIDER_DATABASE_SCHEMA_PRIVILEGES_INVALID"
    )
    rows = [
        {
            "table_name": table_name,
            "can_select": "SELECT" in privileges,
            "can_insert": "INSERT" in privileges,
            "can_update": "UPDATE" in privileges,
            "can_delete": "DELETE" in privileges,
            "can_truncate": False,
            "can_references": False,
            "can_trigger": False,
            "can_maintain": False,
            "can_column_select": "SELECT" in privileges,
            "can_column_insert": "INSERT" in privileges,
            "can_column_update": "UPDATE" in privileges,
            "can_column_references": False,
            "row_security": table_name != "jobs",
            "force_row_security": table_name != "jobs",
        }
        for table_name, privileges in _PROVIDER_PRIVILEGES.items()
    ]

    assert (
        _provider_privilege_violation(
            superuser=False,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            schema_usage=True,
            schema_create=False,
            rows=rows,
        )
        == "PROVIDER_DATABASE_ROW_SCOPE_REQUIRED"
    )

    for row in rows:
        row["row_security"] = True
        row["force_row_security"] = True
    assert (
        _provider_privilege_violation(
            superuser=False,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            schema_usage=True,
            schema_create=False,
            rows=rows,
        )
        is None
    )

    rows.append(
        {
            "table_name": "agent_research_runs",
            "can_select": True,
            "can_insert": False,
            "can_update": False,
            "can_delete": False,
            "can_truncate": False,
            "can_references": False,
            "can_trigger": False,
            "can_maintain": False,
            "can_column_select": True,
            "can_column_insert": False,
            "can_column_update": False,
            "can_column_references": False,
            "row_security": False,
            "force_row_security": False,
        }
    )
    assert (
        _provider_privilege_violation(
            superuser=False,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            schema_usage=True,
            schema_create=False,
            rows=rows,
        )
        == "PROVIDER_DATABASE_PRIVILEGES_TOO_BROAD"
    )

    rows[-1]["can_select"] = False
    rows[0]["can_trigger"] = True
    assert (
        _provider_privilege_violation(
            superuser=False,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            schema_usage=True,
            schema_create=False,
            rows=rows,
        )
        == "PROVIDER_DATABASE_PRIVILEGES_INVALID"
    )

    rows[0]["can_trigger"] = False
    dependencies = next(row for row in rows if row["table_name"] == "job_dependencies")
    dependencies["can_column_update"] = True
    assert (
        _provider_privilege_violation(
            superuser=False,
            bypass_rls=False,
            create_db=False,
            create_role=False,
            replication=False,
            schema_usage=True,
            schema_create=False,
            rows=rows,
        )
        == "PROVIDER_DATABASE_PRIVILEGES_INVALID"
    )

    dependencies["can_column_update"] = False


def _valid_policy_rows(provider_role: str = "provider_runtime") -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    escaped_role = provider_role.replace("'", "''")
    bypass = f"NOT pg_has_role(CURRENT_USER, '{escaped_role}', 'MEMBER')"
    for table_name, (using, with_check) in _PROVIDER_POLICY_EXPRESSIONS.items():
        rows.extend(
            (
                {
                    "table_name": table_name,
                    "policy_name": f"nexus_provider_{table_name}_scope",
                    "permissive": "PERMISSIVE",
                    "roles": [provider_role],
                    "command": "ALL",
                    "qual": using,
                    "with_check": with_check,
                },
                {
                    "table_name": table_name,
                    "policy_name": f"nexus_non_provider_{table_name}_bypass",
                    "permissive": "PERMISSIVE",
                    "roles": ["public"],
                    "command": "ALL",
                    "qual": bypass,
                    "with_check": bypass,
                },
            )
        )
    return rows


def test_provider_database_role_rejects_extra_or_mutated_applicable_policy() -> None:
    provider_role = "provider_runtime"
    rows = _valid_policy_rows(provider_role)
    assert _provider_policy_violation(rows, provider_role=provider_role) is None

    rows.append(
        {
            "table_name": "jobs",
            "policy_name": "unexpected_public_bypass",
            "permissive": "PERMISSIVE",
            "roles": ["public"],
            "command": "ALL",
            "qual": "true",
            "with_check": "true",
        }
    )
    assert (
        _provider_policy_violation(rows, provider_role=provider_role)
        == "PROVIDER_DATABASE_POLICY_SET_INVALID"
    )

    rows = _valid_policy_rows(provider_role)
    bypass = next(row for row in rows if row["policy_name"] == "nexus_non_provider_jobs_bypass")
    bypass["qual"] = "true"
    assert (
        _provider_policy_violation(rows, provider_role=provider_role)
        == "PROVIDER_DATABASE_POLICY_SET_INVALID"
    )


def test_provider_database_role_rejects_mutated_policy_function() -> None:
    rows = [
        {
            "function_name": name,
            "identity_arguments": spec.identity_arguments,
            "source": spec.source,
            "volatility": spec.volatility,
            "parallel_safety": "s",
            "security_definer": False,
            "search_path_fixed": True,
            "trusted_owner": True,
            "provider_is_owner_member": False,
            "provider_execute": True,
            "public_execute": False,
            "provider_execute_grantable": False,
            "acl_only_owner_and_provider": True,
            "acl_grantor_only_owner": True,
            "language": "sql",
            "returns_boolean": True,
        }
        for name, spec in _PROVIDER_POLICY_FUNCTIONS.items()
    ]
    assert _provider_policy_function_violation(rows) is None

    rows[0]["source"] = "SELECT true"
    assert (
        _provider_policy_function_violation(rows)
        == "PROVIDER_DATABASE_POLICY_FUNCTION_INVALID"
    )

    rows[0]["source"] = _PROVIDER_POLICY_FUNCTIONS[
        str(rows[0]["function_name"])
    ].source
    rows[0]["trusted_owner"] = False
    assert (
        _provider_policy_function_violation(rows)
        == "PROVIDER_DATABASE_POLICY_FUNCTION_INVALID"
    )

    rows[0]["trusted_owner"] = True
    rows[0]["acl_only_owner_and_provider"] = False
    assert (
        _provider_policy_function_violation(rows)
        == "PROVIDER_DATABASE_POLICY_FUNCTION_INVALID"
    )

    rows[0]["acl_only_owner_and_provider"] = True
    rows[0]["provider_execute_grantable"] = True
    assert (
        _provider_policy_function_violation(rows)
        == "PROVIDER_DATABASE_POLICY_FUNCTION_INVALID"
    )

    rows[0]["provider_execute_grantable"] = False
    rows.append(
        {
            **rows[0],
            "identity_arguments": "p_queue character varying, p_job_type character varying",
        }
    )
    assert (
        _provider_policy_function_violation(rows)
        == "PROVIDER_DATABASE_POLICY_FUNCTION_INVALID"
    )
