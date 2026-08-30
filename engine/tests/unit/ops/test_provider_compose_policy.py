"""Compose capability separation for offline and production data providers."""

import re
from pathlib import Path

from nexus.ops.local_stack import LocalStackAction, build_local_stack_commands

ROOT = Path(__file__).parents[4]


def _service_block(compose: str, service: str) -> str:
    match = re.search(
        rf"(?ms)^  {re.escape(service)}:\n.*?(?=^  [a-z0-9-]+:\n|\Z)",
        compose,
    )
    assert match is not None
    return match.group(0)


def test_app_worker_has_no_provider_capability_or_secrets() -> None:
    compose = (ROOT / "infra/compose.yaml").read_text()
    worker = _service_block(compose, "worker")

    assert "NEXUS_PROVIDER_RUNTIME_MODE" not in worker
    assert "ALPACA_API_KEY_ID" not in worker
    assert "ALPACA_API_SECRET_KEY" not in worker
    assert "FRED_API_KEY" not in worker
    assert "NEXUS_MARKET_PROVIDER=${NEXUS_MARKET_PROVIDER:-fosun}" in worker


def test_provider_worker_is_a_separate_explicit_profile_and_secret_domain() -> None:
    compose = (ROOT / "infra/compose.yaml").read_text()
    provider = _service_block(compose, "provider-worker")

    assert 'profiles: ["provider"]' in provider
    assert 'command: ["nexus-engine-provider-worker"]' in provider
    assert "NEXUS_PROVIDER_RUNTIME_MODE=production" in provider
    assert "- PROVIDER_DATABASE_URL" in provider
    assert "- DATABASE_URL" not in provider
    assert "AGENT_RUNTIME_MODE" not in provider
    assert "OPENAI_" not in provider
    assert "NEXUS_ENGINE_HMAC" not in provider
    assert "NEXUS_MARKET_PROVIDER=fosun" in provider
    assert "NEXUS_ALPACA_MARKET_MODE=disabled" in provider
    for name in (
        "PROVIDER_OBJECT_STORE_ENDPOINT",
        "PROVIDER_OBJECT_STORE_REGION",
        "PROVIDER_OBJECT_STORE_BUCKET",
        "PROVIDER_OBJECT_STORE_ACCESS_KEY_ID",
        "PROVIDER_OBJECT_STORE_SECRET_ACCESS_KEY",
    ):
        assert f"- {name}" in provider
    assert "- PROVIDER_OBJECT_STORE_PREFIX=provider/market" in provider
    assert not re.search(r"(?m)^      - OBJECT_STORE_", provider)
    assert "object-store-init:" not in provider
    for name in (
        "FOSUN_MARKET_GATEWAY_BASE_URL",
        "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_KEY_ID",
        "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_SECRET",
        "FOSUN_MARKET_GATEWAY_CLIENT_CERT",
        "FOSUN_MARKET_GATEWAY_CLIENT_KEY",
        "FOSUN_MARKET_GATEWAY_CLIENT_CA",
    ):
        assert f"- {name}" in provider
    assert "NEXUS_BROKER_EXTERNAL_ID_" not in provider
    assert "- FOSUN_GATEWAY_BASE_URL" not in provider
    assert "- FOSUN_GATEWAY_CLIENT_CERT" not in provider
    for name in (
        "ALPACA_API_KEY_ID",
        "ALPACA_API_SECRET_KEY",
        "ALPACA_DATA_FEED",
        "ALPACA_DELIVERY_MODE",
        "ALPACA_INTRADAY_FEED",
        "ALPACA_INTRADAY_DELIVERY_MODE",
    ):
        assert f"- {name}" not in provider
    for name in (
        "SEC_CONTACT",
        "FRED_API_KEY",
    ):
        assert f"- {name}" in provider


def test_alpaca_provider_is_an_explicit_mutually_exclusive_secret_domain() -> None:
    compose = (ROOT / "infra/compose.yaml").read_text()
    provider = _service_block(compose, "provider-worker-alpaca")

    assert 'profiles: ["provider-alpaca"]' in provider
    assert 'command: ["nexus-engine-provider-worker"]' in provider
    assert "NEXUS_PROVIDER_RUNTIME_MODE=production" in provider
    assert "NEXUS_MARKET_PROVIDER=alpaca" in provider
    assert "NEXUS_ALPACA_MARKET_MODE=enabled" in provider
    for name in (
        "PROVIDER_OBJECT_STORE_ENDPOINT",
        "PROVIDER_OBJECT_STORE_REGION",
        "PROVIDER_OBJECT_STORE_BUCKET",
        "PROVIDER_OBJECT_STORE_ACCESS_KEY_ID",
        "PROVIDER_OBJECT_STORE_SECRET_ACCESS_KEY",
    ):
        assert f"- {name}" in provider
    assert "- PROVIDER_OBJECT_STORE_PREFIX=provider/market" in provider
    assert not re.search(r"(?m)^      - OBJECT_STORE_", provider)
    assert "object-store-init:" not in provider
    for name in (
        "ALPACA_API_KEY_ID",
        "ALPACA_API_SECRET_KEY",
        "ALPACA_DATA_FEED",
        "ALPACA_DELIVERY_MODE",
        "ALPACA_INTRADAY_FEED",
        "ALPACA_INTRADAY_DELIVERY_MODE",
        "SEC_CONTACT",
        "FRED_API_KEY",
    ):
        assert f"- {name}" in provider
    assert "FOSUN_MARKET_" not in provider
    assert "FOSUN_GATEWAY_" not in provider
    assert "FSOPENAPI_" not in provider
    assert "NEXUS_BROKER_EXTERNAL_ID_" not in provider


def test_fosun_gateway_serves_provider_and_broker_profiles_with_separate_principals() -> None:
    compose = (ROOT / "infra/compose.yaml").read_text()
    gateway = _service_block(compose, "fosun-gateway")

    assert 'profiles: ["provider", "broker"]' in gateway
    assert "replicas: 1" in gateway
    assert "- FOSUN_GATEWAY_REPLICA_COUNT=1" in gateway
    assert (
        "- FOSUN_GATEWAY_REPLAY_STORE_PATH=/var/lib/nexus-fosun-gateway/replay.sqlite3"
    ) in gateway
    assert "nexus-fosun-gateway-ready" in gateway
    assert "fosun-gateway-replay:/var/lib/nexus-fosun-gateway" in gateway
    for name in (
        "FOSUN_GATEWAY_HMAC_CURRENT_KEY_ID",
        "FOSUN_GATEWAY_HMAC_CURRENT_SECRET",
        "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_KEY_ID",
        "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_SECRET",
        "FOSUN_MARKET_GATEWAY_HMAC_PREVIOUS_KEY_ID",
        "FOSUN_MARKET_GATEWAY_HMAC_PREVIOUS_SECRET",
    ):
        assert f"- {name}" in gateway

    for service in ("api", "worker"):
        block = _service_block(compose, service)
        assert "FOSUN_MARKET_GATEWAY_" not in block
        assert "FOSUN_GATEWAY_CLIENT_" not in block

    broker = _service_block(compose, "broker-worker")
    assert "FOSUN_MARKET_GATEWAY_" not in broker
    assert "fosun-gateway:" in broker
    assert "condition: service_healthy" in broker
    provider = _service_block(compose, "provider-worker")
    assert "fosun-gateway:" in provider
    assert "condition: service_healthy" in provider
    assert "external: true" in compose
    assert "name: nexus-fosun-gateway-replay-v1" in compose


def test_provider_worker_cannot_materialize_a_second_daily_root() -> None:
    source = (ROOT / "engine/src/nexus/provider_worker.py").read_text()

    assert "schedule_due_daily_pipeline" not in source
    assert "build_daily_pipeline_registry" not in source
    assert '"search_path": "pg_catalog,public,pg_temp"' in source


def test_provider_database_grant_template_is_force_scoped_and_secret_free() -> None:
    grants = (ROOT / "infra/postgres/provider-runtime-grants.sql").read_text(encoding="utf-8")

    assert "PASSWORD" not in grants.upper()
    assert "SET LOCAL search_path TO pg_catalog, public, pg_temp;" in grants
    assert "REVOKE CREATE ON SCHEMA public" in grants
    for table_name in (
        "jobs",
        "job_leases",
        "job_attempts",
        "job_dependencies",
        "worker_heartbeats",
        "fosun_daily_bar_staging",
    ):
        assert f"ALTER TABLE public.{table_name} FORCE ROW LEVEL SECURITY" in grants
    assert "p_job_type = 'daily.pipeline'" in grants
    assert "'collect.fosun_daily_instrument'" in grants
    assert "'publish.fosun_daily'" in grants
    assert "('alembic_version')" in grants
    assert "nexus_provider_job_writable(queue, job_type)" in grants
    assert "nexus_provider_job_owned(job_id)" in grants
    assert "nexus_provider_job_owned(fetch_job_id)" in grants
    assert "nexus_provider_fosun_daily_bar_staging_scope" in grants
    assert "nexus_non_provider_fosun_daily_bar_staging_bypass" in grants
    assert "SELECT EXISTS (SELECT 1 FROM public.jobs AS job" in grants
    assert "nexus_provider_queue(queue)" in grants
    assert "unexpected shared-runtime RLS policy requires manual review" in grants
    assert "(policy.tablename, policy.policyname) NOT IN" in grants
    assert "unexpected provider policy function overload requires manual review" in grants
    assert "provider role memberships require manual review" in grants
    assert "provider role has forbidden role attributes" in grants
    assert grants.count("provider role must be a direct login role") == 2
    assert grants.count("provider role has database CREATE privilege") == 2
    assert grants.count("NOT provider.rolcanlogin") == 2
    assert grants.count("has_database_privilege(provider.oid, current_database(), 'CREATE')") == 2
    assert "provider role must not own the shared runtime schema" in grants
    assert "pg_roles%%ROWTYPE" in grants
    assert "membership.member = provider.oid OR membership.roleid = provider.oid" in grants
    assert "REVOKE SET, ALTER SYSTEM ON PARAMETER session_replication_role" in grants
    assert "provider role has forbidden parameter privileges" in grants
    assert "provider role has forbidden parameter defaults" in grants
    assert "provider role has forbidden grant options" in grants
    assert "provider role has forbidden non-public schema privileges" in grants
    assert "PUBLIC relation privileges require manual review" in grants
    assert "PUBLIC sequence privileges require manual review" in grants
    assert "provider role has forbidden non-public sequence privileges" in grants
    assert "REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public" in grants
    assert grants.count("FROM pg_sequence AS sequence") == 3
    assert "unexpected non-system security definer requires manual review" in grants
    assert "relation.relkind IN ('r','p','v','m','f')" in grants
    assert "nexus_guard_provider_price_bar_frozen_insert" in grants
    assert "OWNER TO CURRENT_USER" in grants
    assert "REVOKE ALL PRIVILEGES ON FUNCTION" in grants
    assert grants.count("BEGIN;") == 1
    assert grants.count("COMMIT;") == 1


def test_local_stack_never_starts_the_provider_profile(repo_root: Path) -> None:
    root = repo_root
    env = root / "infra/.env.local"
    commands = build_local_stack_commands(
        LocalStackAction.UP,
        repo_root=root,
        env_file=env,
        project_name="nexus-phase3-fixture",
    )

    assert all("provider" not in command.argv for command in commands)
    assert all("broker" not in command.argv for command in commands)


def test_provider_profiles_are_both_static_validated_in_ci() -> None:
    workflow = (ROOT / ".github/workflows/ci.yml").read_text()

    assert "--profile provider config --quiet" in workflow
    assert "--profile provider-alpaca config --quiet" in workflow


def test_local_runbook_requires_mutually_exclusive_provider_workers() -> None:
    runbook = (ROOT / "docs/runbooks/local-development.md").read_text()

    assert "provider-alpaca" in runbook
    assert "不得同时启动" in runbook
    assert "先停止" in runbook
