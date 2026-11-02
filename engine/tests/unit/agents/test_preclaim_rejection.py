from types import SimpleNamespace

import pytest
from nexus import worker
from nexus.agents.codex_versions import BOUNDED_STAGE_BUDGET_CONFIG_VERSION


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "code", ["CODEX_CAPACITY_POOL_WIRE_EXCEEDED", "CODEX_CAPACITY_POOL_INPUT_EXCEEDED"]
)
async def test_only_native_preclaim_allowlist_is_typed(monkeypatch, code):
    from nexus.paper.research_failure import CapacityPreclaimRejected

    async def rejected(*args, **kwargs):
        raise ValueError(code)

    monkeypatch.setattr("nexus.agents.capacity_cycle.prepare_cycle_capacity_pool", rejected)
    with pytest.raises(CapacityPreclaimRejected) as captured:
        await worker._prepare_preclaim_capacity_pool(
            None, None, SimpleNamespace(pricing_version=BOUNDED_STAGE_BUDGET_CONFIG_VERSION)
        )
    assert captured.value.code == code
    assert captured.value.__cause__.args == (code,)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        ValueError("private raw text"),
        RuntimeError("CODEX_CAPACITY_POOL_WIRE_EXCEEDED"),
        TimeoutError("private"),
    ],
)
async def test_untyped_or_unknown_preclaim_failure_is_unchanged(monkeypatch, error):
    async def failed(*args, **kwargs):
        raise error

    monkeypatch.setattr("nexus.agents.capacity_cycle.prepare_cycle_capacity_pool", failed)
    with pytest.raises(type(error)) as captured:
        await worker._prepare_preclaim_capacity_pool(
            None, None, SimpleNamespace(pricing_version=BOUNDED_STAGE_BUDGET_CONFIG_VERSION)
        )
    assert captured.value is error


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "code",
    [
        "PLATFORM_HTTP_CREDENTIAL_CHANGED",
        "PLATFORM_PRICE_WINDOW_INVALID",
        "PLATFORM_NATIVE_INPUT_CHANGED",
        "PLATFORM_NATIVE_ROUTE_INVALID",
    ],
)
async def test_native_platform_preclaim_has_fixed_safe_receipt(monkeypatch, code):
    from nexus.paper.research_failure import CapacityPreclaimRejected
    from nexus.settings import AgentRuntimeMode

    async def rejected(*a, **kw):
        raise ValueError(code)

    monkeypatch.setattr("nexus.agents.platform_native.prepare_native_pool", rejected)
    with pytest.raises(CapacityPreclaimRejected) as caught:
        await worker._prepare_preclaim_capacity_pool(
            None,
            SimpleNamespace(agent_runtime_mode=AgentRuntimeMode.RESPONSES),
            SimpleNamespace(pricing_version="native-sol"),
            formal_stage_capacity=True,
        )
    assert caught.value.code == code and caught.value.__cause__.args == (code,)
