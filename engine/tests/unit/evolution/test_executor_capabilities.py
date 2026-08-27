"""The production Strategy R&D composition must remain capability-isolated."""

import ast
from pathlib import Path

import pytest
from nexus.evolution.executor import build_strategy_rnd_executor
from nexus.evolution.jobs import build_strategy_rnd_registry
from nexus.jobs.models import JobType, QueueName
from nexus.settings import AgentRuntimeMode, EngineSettings, Environment

_MODULES = ("executor.py", "authority.py", "validation.py", "worker.py")
_FORBIDDEN_IMPORT_PARTS = ("broker", "fosun", "account", "order_create", "live_write")


@pytest.mark.parametrize("module_name", _MODULES)
def test_executor_modules_do_not_import_external_execution_capabilities(
    module_name: str,
) -> None:
    source = (
        Path(__file__).parents[3] / "src" / "nexus" / "evolution" / module_name
    ).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported.append(node.module)
    assert not any(
        forbidden in module.lower()
        for module in imported
        for forbidden in _FORBIDDEN_IMPORT_PARTS
    )


def test_registry_contains_only_the_strategy_rnd_queue() -> None:
    executor = build_strategy_rnd_executor(EngineSettings.for_test())
    registry = build_strategy_rnd_registry(executor=executor)

    assert {spec.queue for spec in registry.specs} == {QueueName.STRATEGY_RND}
    assert registry.job_types == {JobType.STRATEGY_RND.value}


def test_production_executor_requires_explicit_responses_authority() -> None:
    with pytest.raises(ValueError, match="explicit Responses authority"):
        build_strategy_rnd_executor(
            EngineSettings(
                environment=Environment.PRODUCTION,
                agent_runtime_mode=AgentRuntimeMode.FIXTURE,
                database_url="postgresql+asyncpg://fixture:fixture@db/nexus",
                object_store_endpoint="https://object.invalid",
                object_store_region="us-east-1",
                object_store_bucket="nexus",
                object_store_access_key_id="fixture",
                object_store_secret_access_key="fixture",
                engine_hmac_current_key_id="fixture",
                engine_hmac_current_secret="fixture",
            )
        )
