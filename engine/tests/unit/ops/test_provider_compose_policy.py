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
    for name in (
        "ALPACA_API_KEY_ID",
        "ALPACA_API_SECRET_KEY",
        "ALPACA_DATA_FEED",
        "ALPACA_DELIVERY_MODE",
        "SEC_CONTACT",
        "FRED_API_KEY",
    ):
        assert f"- {name}" in provider


def test_provider_worker_cannot_materialize_a_second_daily_root() -> None:
    source = (ROOT / "engine/src/nexus/provider_worker.py").read_text()

    assert "schedule_due_daily_pipeline" not in source
    assert "build_daily_pipeline_registry" not in source
    assert '"search_path": "public,pg_catalog"' in source


def test_provider_database_grant_template_is_force_scoped_and_secret_free() -> None:
    grants = (ROOT / "infra/postgres/provider-runtime-grants.sql").read_text(
        encoding="utf-8"
    )

    assert "PASSWORD" not in grants.upper()
    assert "REVOKE CREATE ON SCHEMA public" in grants
    for table_name in (
        "jobs",
        "job_leases",
        "job_attempts",
        "job_dependencies",
        "worker_heartbeats",
    ):
        assert f"ALTER TABLE {table_name} FORCE ROW LEVEL SECURITY" in grants
    assert "p_job_type = 'daily.pipeline'" in grants
    assert "nexus_provider_job_writable(queue, job_type)" in grants
    assert "nexus_provider_job_owned(job_id)" in grants
    assert "nexus_provider_queue(queue)" in grants
    assert "unexpected shared-runtime RLS policy requires manual review" in grants
    assert "(policy.tablename, policy.policyname) NOT IN" in grants
    assert "unexpected provider policy function overload requires manual review" in grants
    assert "provider role memberships require manual review" in grants
    assert "provider role has forbidden role attributes" in grants
    assert "provider role must not own the shared runtime schema" in grants
    assert "pg_has_role(provider.oid, granted_role.oid, 'MEMBER')" in grants
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
