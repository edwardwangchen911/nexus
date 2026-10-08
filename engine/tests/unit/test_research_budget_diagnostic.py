from types import SimpleNamespace
from uuid import uuid4

import pytest
from nexus.agents.codex_versions import DEADLINE_BUDGET_CONFIG_VERSION
from nexus.ops.research_budget_diagnostic import require_diagnostic_url, require_fresh_identity


def test_business_database_is_rejected() -> None:
    with pytest.raises(ValueError, match="DIAGNOSTIC_DATABASE_REQUIRED"):
        require_diagnostic_url("postgresql+asyncpg://localhost/nexus")


@pytest.mark.parametrize("host", ["localhost", "127.0.0.1", "::1"])
def test_only_explicit_local_diagnostic_database_is_allowed(host: str) -> None:
    host = f"[{host}]" if ":" in host else host
    require_diagnostic_url(f"postgresql+asyncpg://{host}/nexus_diag_budget")


@pytest.mark.parametrize(
    "field,value",
    [
        ("pricing_version", "codex-sdk-chatgpt-v5-paper-scope"),
        ("status", "failed"),
        ("idempotency_key", "formal-paper-slot"),
    ],
)
def test_old_or_formal_run_is_rejected(field: str, value: str) -> None:
    run = SimpleNamespace(
        status="prepared",
        pricing_version=DEADLINE_BUDGET_CONFIG_VERSION,
        actor_id="local-research-cli",
        idempotency_key="budget-diagnostic:one",
    )
    setattr(run, field, value)
    job = SimpleNamespace(payload={"run_id": str(uuid4())})
    with pytest.raises(ValueError, match="DIAGNOSTIC_FRESH_RUN_REQUIRED"):
        require_fresh_identity(run, job, uuid4())


@pytest.mark.parametrize(
    "field,value",
    [
        ("job_type", "agent.paper.research"),
        ("status", "failed"),
        ("attempt_count", 1),
        ("max_attempts", 2),
        ("manual_retry_count", 1),
        ("retry_policy", "manual_safe"),
        ("queue", "broker"),
    ],
)
def test_replay_or_other_worker_job_is_rejected(field: str, value: object) -> None:
    run_id = uuid4()
    run = SimpleNamespace(
        status="prepared",
        pricing_version=DEADLINE_BUDGET_CONFIG_VERSION,
        actor_id="local-research-cli",
        idempotency_key="budget-diagnostic:one",
    )
    job = SimpleNamespace(
        job_type="agent.research",
        queue="general",
        status="queued",
        attempt_count=0,
        manual_retry_count=0,
        max_attempts=1,
        retry_policy="automatic_safe",
        payload={"run_id": str(run_id)},
    )
    require_fresh_identity(run, job, run_id)
    setattr(job, field, value)
    with pytest.raises(ValueError, match="DIAGNOSTIC_FRESH_JOB_REQUIRED"):
        require_fresh_identity(run, job, run_id)


def test_private_config_and_symlink_guard(tmp_path) -> None:
    from nexus.ops.research_budget_diagnostic import read_database_url

    config = tmp_path / "database-url"
    config.write_text("postgresql+asyncpg://localhost/nexus_diag_budget")
    config.chmod(0o644)
    with pytest.raises(ValueError, match="DIAGNOSTIC_CONFIG_UNSAFE"):
        read_database_url(config)
    config.chmod(0o600)
    assert read_database_url(config).endswith("nexus_diag_budget")
    link = tmp_path / "link"
    link.symlink_to(config)
    with pytest.raises(OSError):
        read_database_url(link)


@pytest.mark.parametrize(
    "settled,input_tokens,output_tokens,expected",
    [
        (None, 300, 80, False),
        (None, 0, 0, False),
        ("0", 300, 80, True),
        ("0.00046", 300, 80, True),
        ("0", None, 80, False),
        ("0", 300, None, False),
    ],
)
def test_diagnostic_does_not_report_unsettled_lower_bound_as_known(
    settled, input_tokens, output_tokens, expected
):
    from decimal import Decimal

    from nexus.ops.research_budget_diagnostic import usage_is_known

    run = SimpleNamespace(
        settled_cost_usd=None if settled is None else Decimal(settled),
        input_tokens=input_tokens,
        output_tokens=output_tokens,
    )
    assert usage_is_known(run) is expected
