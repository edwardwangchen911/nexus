"""Protection must not turn an immediate stop into a database dependency."""

from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest
from nexus.ops import agent_paper_loop as loop
from nexus.ops import formal_budget_protection as binding_entry


@pytest.mark.asyncio
@pytest.mark.parametrize("stop", ["false", "0", "no", "off", ""])
@pytest.mark.parametrize(
    "action", [loop.AgentPaperLoopAction.TRIGGER, loop.AgentPaperLoopAction.PREPARE_INPUTS]
)
async def test_protected_action_immediate_stop_needs_no_today_binding(
    monkeypatch,
    tmp_path: Path,
    stop,
    action,
):
    environment = {
        "NEXUS_AGENT_PAPER_CYCLE_ENABLED": stop,
        "NEXUS_FORMAL_BUDGET_PROTECTION_ENABLED": "true",
    }
    monkeypatch.setattr(loop, "load_agent_paper_service_environment", lambda *_a, **_k: environment)

    async def no_binding_or_database(*_a, **_k):
        raise AssertionError("immediate stop must not read the new-day binding")

    monkeypatch.setattr(binding_entry, "bind_runtime_environment", no_binding_or_database)
    result = await loop.execute_agent_paper_loop_action(
        loop.EngineSettings(formal_budget_protection_enabled=True),
        account_id=UUID(int=1),
        repo_root=tmp_path,
        action=action,
        snapshot=loop.AgentPaperLoopSnapshot(now=datetime(2026, 10, 9, tzinfo=UTC)),
        env_file=tmp_path / "infra/.env.test",
    )
    assert result == loop.AgentPaperLoopAction.STOP_DISABLED
