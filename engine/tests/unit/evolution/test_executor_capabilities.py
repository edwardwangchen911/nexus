"""The production Strategy R&D composition must remain capability-isolated."""

import ast
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.evolution.authority import StrategyRNDAuthorityError
from nexus.evolution.executor import (
    StrategyRNDExecutor,
    _model_configuration,
    build_strategy_rnd_executor,
)
from nexus.evolution.jobs import build_strategy_rnd_job_payload, build_strategy_rnd_registry
from nexus.jobs.models import JobType, QueueName
from nexus.jobs.registry import JobHandlerContext
from nexus.settings import AgentRuntimeMode, EngineSettings, Environment

_MODULES = ("executor.py", "authority.py", "validation.py", "worker.py")
_FORBIDDEN_IMPORT_PARTS = ("broker", "fosun", "account", "order_create", "live_write")


@pytest.mark.parametrize(
    ("operation", "method_name"),
    (
        ("generate_family", "_generate_family"),
        ("evaluate_candidate", "_evaluate_candidate"),
        ("advance_shadow", "_advance_shadow"),
        ("finalize_promotion", "_finalize_promotion"),
    ),
)
@pytest.mark.asyncio
async def test_executor_abstains_before_dispatch_without_current_adjusted_authority(
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    method_name: str,
) -> None:
    executor = StrategyRNDExecutor(
        authority_resolver=cast(object, SimpleNamespace()),
        runtime_mode=AgentRuntimeMode.FIXTURE,
        reasoning_model=None,
        responses_client=None,
    )

    async def forbidden_dispatch(*_args: object) -> None:
        raise AssertionError("executor dispatched without adjusted authority")

    monkeypatch.setattr(executor, method_name, forbidden_dispatch)
    family_id = None if operation == "generate_family" else UUID(int=1)
    experiment_id = None if operation == "generate_family" else UUID(int=2)
    payload = build_strategy_rnd_job_payload(
        operation=operation,
        family_id=family_id,
        experiment_id=experiment_id,
        expected_champion_generation=1,
        candidate_state="ADMITTED" if operation == "evaluate_candidate" else None,
        candidate_version=1 if operation == "evaluate_candidate" else None,
    )
    context = cast(JobHandlerContext, SimpleNamespace(session=object()))

    result = await executor(context, payload)

    assert (result.outcome, result.result_code, result.error_code) == (
        "skipped",
        "BACKTEST_ADJUSTED_AUTHORITY_UNAVAILABLE",
        None,
    )


@pytest.mark.parametrize(
    ("operation", "loader_name"),
    (
        ("generate_family", "load_generation"),
        ("evaluate_candidate", "load_candidate"),
    ),
)
@pytest.mark.asyncio
async def test_executor_normalizes_resolver_gate_closure_to_skipped(
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    loader_name: str,
) -> None:
    async def deny_adjusted_authority(*_args: object, **_kwargs: object) -> None:
        raise StrategyRNDAuthorityError("BACKTEST_ADJUSTED_AUTHORITY_UNAVAILABLE")

    resolver = SimpleNamespace(**{loader_name: deny_adjusted_authority})
    executor = StrategyRNDExecutor(
        authority_resolver=cast(object, resolver),
        runtime_mode=AgentRuntimeMode.FIXTURE,
        reasoning_model=None,
        responses_client=None,
    )
    monkeypatch.setattr(
        "nexus.evolution.executor.current_strategy_rnd_adjusted_authority_available",
        lambda: True,
    )
    payload = build_strategy_rnd_job_payload(
        operation=operation,
        family_id=None if operation == "generate_family" else UUID(int=1),
        experiment_id=None if operation == "generate_family" else UUID(int=2),
        expected_champion_generation=1,
        candidate_state="ADMITTED" if operation == "evaluate_candidate" else None,
        candidate_version=1 if operation == "evaluate_candidate" else None,
    )
    context = cast(
        JobHandlerContext,
        SimpleNamespace(
            session=object(),
            finished_at=lambda: datetime(2026, 8, 30, 12, tzinfo=UTC),
        ),
    )

    result = await executor(context, payload)

    assert (result.outcome, result.result_code, result.error_code) == (
        "skipped",
        "BACKTEST_ADJUSTED_AUTHORITY_UNAVAILABLE",
        None,
    )


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


def test_responses_deadline_fits_inside_strategy_job_timeout() -> None:
    executor = build_strategy_rnd_executor(EngineSettings.for_test())
    spec = build_strategy_rnd_registry(executor=executor).resolve(
        JobType.STRATEGY_RND.value
    )
    assert spec is not None
    config = _model_configuration(
        SimpleNamespace(
            model=SimpleNamespace(
                model_id="fixture-model",
                version="fixture-v1",
                max_input_tokens=10_000,
                max_output_tokens=1_000,
                max_tool_calls=8,
                input_usd_per_million=Decimal("1"),
                output_usd_per_million=Decimal("2"),
            )
        )
    )

    assert float(config.absolute_timeout_seconds) < spec.timeout_seconds


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


def test_local_codex_worker_entry_rejects_before_starting_queue_or_database(monkeypatch):
    from nexus.evolution import worker

    settings = EngineSettings(environment="development", agent_runtime_mode="codex_sdk")
    run = AsyncMock()
    monkeypatch.setattr(worker, "EngineSettings", lambda: settings)
    monkeypatch.setattr(worker, "run_strategy_rnd_worker", run)
    with pytest.raises(ValueError, match="Strategy R&D runtime.*fixture or responses"):
        worker.main()
    run.assert_not_called()


@pytest.mark.parametrize("mode", [AgentRuntimeMode.CODEX_SDK, None, "future_backend"])
def test_generation_selector_never_falls_back_for_unhandled_modes(mode):
    executor = StrategyRNDExecutor(
        authority_resolver=SimpleNamespace(),
        runtime_mode=mode,
        reasoning_model=None,
        responses_client=None,
    )
    with pytest.raises(StrategyRNDAuthorityError) as error:
        executor._generation_runtime(_runtime_authority())
    assert error.value.reason_code == "GENERATION_RUNTIME_UNSUPPORTED"


@pytest.mark.parametrize("mode", [AgentRuntimeMode.FIXTURE, AgentRuntimeMode.RESPONSES])
def test_explicit_supported_runtime_keeps_its_selected_backend(mode):
    from nexus.evolution.agent import ResponsesStrategyRNDRuntime
    from nexus.evolution.executor import _FixedStrategyRNDRuntime

    settings = EngineSettings(
        agent_runtime_mode=mode,
        openai_api_key="synthetic-key",
        openai_extraction_model="synthetic-extraction",
        openai_reasoning_model="synthetic-reasoning",
    )
    client = SimpleNamespace(responses=SimpleNamespace(create=AsyncMock()))
    runtime = build_strategy_rnd_executor(settings, responses_client=client)._generation_runtime(
        _runtime_authority()
    )
    expected = (
        _FixedStrategyRNDRuntime
        if mode is AgentRuntimeMode.FIXTURE
        else ResponsesStrategyRNDRuntime
    )
    assert isinstance(runtime, expected)
    client.responses.create.assert_not_called()


def _runtime_authority():
    return SimpleNamespace(
        model=SimpleNamespace(
            model_id="synthetic-reasoning",
            version="synthetic-v1",
            max_input_tokens=10000,
            max_output_tokens=1000,
            max_tool_calls=4,
            input_usd_per_million=Decimal("1"),
            output_usd_per_million=Decimal("2"),
        )
    )


@pytest.mark.parametrize(
    "updates,client",
    [
        ({"agent_runtime_mode": None}, None),
        (
            {"agent_runtime_mode": AgentRuntimeMode.RESPONSES, "openai_reasoning_model": None},
            object(),
        ),
        (
            {
                "agent_runtime_mode": AgentRuntimeMode.RESPONSES,
                "openai_reasoning_model": "synthetic",
            },
            None,
        ),
    ],
)
def test_factory_rechecks_missing_runtime_configuration_before_client_creation(
    updates, client, monkeypatch
):
    def forbidden(**kwargs):
        raise AssertionError("incomplete settings must not construct a model client")

    monkeypatch.setattr("nexus.evolution.executor.AsyncOpenAI", forbidden)
    settings = EngineSettings.for_test().model_copy(update=updates)
    with pytest.raises(ValueError):
        build_strategy_rnd_executor(settings, responses_client=client)


@pytest.mark.asyncio
async def test_registered_generation_rejects_injected_unsupported_executor_without_agent_output(
    monkeypatch,
):
    from nexus.evolution import executor as executor_module
    from nexus.evolution import jobs

    monkeypatch.setattr(
        executor_module, "current_strategy_rnd_adjusted_authority_available", lambda: True
    )
    monkeypatch.setattr(jobs, "_preflight_strategy_rnd_job", AsyncMock(return_value=None))
    audit = AsyncMock()
    monkeypatch.setattr(jobs, "_append_strategy_operation_audit", audit)
    model_run = AsyncMock()
    monkeypatch.setattr(executor_module, "execute_strategy_rnd", model_run)
    executor = StrategyRNDExecutor(
        authority_resolver=SimpleNamespace(
            load_generation=AsyncMock(return_value=_runtime_authority())
        ),
        runtime_mode=AgentRuntimeMode.CODEX_SDK,
        reasoning_model=None,
        responses_client=None,
    )
    payload = build_strategy_rnd_job_payload(
        operation="generate_family",
        family_id=None,
        experiment_id=None,
        expected_champion_generation=1,
    )
    context = SimpleNamespace(
        payload=payload,
        claim=SimpleNamespace(job_id=UUID(int=1)),
        session=SimpleNamespace(
            get=AsyncMock(return_value=SimpleNamespace(input_hash=payload.input_hash))
        ),
        finished_at=lambda: datetime(2026, 9, 3, tzinfo=UTC),
    )
    spec = build_strategy_rnd_registry(executor=executor).resolve(JobType.STRATEGY_RND.value)
    result = await spec.handler(context)
    assert (result.outcome, result.error_code) == ("failed", "GENERATION_RUNTIME_UNSUPPORTED")
    model_run.assert_not_called()
    assert audit.await_args.kwargs["result"] == result
