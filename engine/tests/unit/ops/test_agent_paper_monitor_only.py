"""Observe restricted host failures without weakening any refusal."""

import asyncio
import json
import subprocess
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock
from uuid import uuid4

import pytest
from nexus.ops import agent_paper_loop
from nexus.ops import agent_paper_monitor_only as monitor
from nexus.settings import EngineSettings


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "phase,error,code",
    [
        (
            "activation",
            subprocess.TimeoutExpired("private-process-arguments", 5),
            "DEPENDENCY_TIMEOUT",
        ),
        ("configuration", ValueError("private configuration detail"), "VALIDATION_REJECTED"),
        ("heartbeat", ValueError("MANUAL_HOST_NOT_READY"), "MANUAL_HOST_NOT_READY"),
    ],
)
async def test_pulse_reports_failed_phase_without_advancing(
    phase, error, code, monkeypatch, tmp_path, capsys
):
    account_id = uuid4()
    settings = EngineSettings(
        agent_paper_account_id=account_id,
        agent_paper_cycle_enabled=True,
        release_revision="a" * 40,
    )
    started_at = datetime.now(UTC)
    operation = uuid4()
    _, version = agent_paper_loop.agent_paper_host_identity(
        settings, worker_id=f"agent-paper-loop-{operation}", account_id=account_id
    )
    activation = Mock(side_effect=[None, error if phase == "activation" else None])
    runtime = AsyncMock(side_effect=[settings, error if phase == "configuration" else settings])
    heartbeat = AsyncMock(side_effect=error if phase == "heartbeat" else None)
    coordinator = SimpleNamespace(kick=Mock(), close=AsyncMock())
    monkeypatch.setattr("nexus.ops.research_monitor.load_host_settings", runtime)
    monkeypatch.setattr(agent_paper_loop, "heartbeat_agent_paper_loop", heartbeat)
    monkeypatch.setattr(monitor, "PriceMonitorCoordinator", Mock(return_value=coordinator))
    marker = object()
    monkeypatch.setattr(monitor, "lock_portfolio_account", AsyncMock())
    monkeypatch.setattr(monitor, "upsert_worker_heartbeat", AsyncMock())
    monkeypatch.setattr(monitor, "publish_monitor_only_start", AsyncMock(return_value=marker))
    monkeypatch.setattr(monitor, "latest_probe_state", AsyncMock(return_value=marker))
    stored = SimpleNamespace(
        started_at=started_at,
        seen_at=started_at,
        process_version=version,
        migration_head=monitor.CURRENT_MIGRATION_HEAD,
    )
    transaction = MagicMock()
    transaction.__aenter__.return_value = SimpleNamespace(
        execute=AsyncMock(),
        scalar=AsyncMock(side_effect=[monitor.CURRENT_MIGRATION_HEAD, stored]),
    )
    factory = SimpleNamespace(
        kw={"bind": SimpleNamespace(dispose=AsyncMock())},
        begin=Mock(return_value=transaction),
    )
    monkeypatch.setattr(monitor, "session_factory", Mock(return_value=factory))

    with pytest.raises(type(error)) as caught:
        await monitor.run_monitor_only(
            settings,
            account_id=account_id,
            calendar_authority_id=uuid4(),
            repo_root=tmp_path,
            env_file=tmp_path / ".env.local",
            operation_id=operation,
            verify_activation=activation,
            started_at=started_at,
            clock=lambda: started_at,
            shutdown=asyncio.Event(),
            wait_sleep=AsyncMock(),
        )

    assert caught.value is error
    output = capsys.readouterr().err
    report = json.loads(output)
    assert report["event"] == "agent_paper.monitor_only.pulse_failed"
    assert report["phase"] == phase and report["code"] == code
    assert report["phase_elapsed_ms"] >= 0
    assert "private" not in output
    assert heartbeat.await_count == int(phase == "heartbeat")
    coordinator.kick.assert_not_called()
    coordinator.close.assert_awaited_once()
