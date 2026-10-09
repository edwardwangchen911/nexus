"""Status is observation only and must never dispatch the apply writer."""

import json
import sys
from uuid import UUID

import pytest
from nexus.ops import formal_budget_protection as entry
from nexus.settings import EngineSettings


@pytest.mark.parametrize("ready", [True, False, None])
def test_status_cli_never_calls_apply(monkeypatch, capsys, ready):
    async def observed(_root):
        return {"status": "observed", "budget_binding_ready": ready, "execution_locked": True}

    async def forbidden_write(*_args, **_kwargs):
        raise AssertionError("status cannot apply protection")

    monkeypatch.setattr(entry, "inspect_protection", observed)
    monkeypatch.setattr(entry, "apply", forbidden_write)
    monkeypatch.setattr(sys, "argv", ["formal-budget", "--status"])
    assert entry.main() == (0 if ready is True else 1)
    assert json.loads(capsys.readouterr().out)["budget_binding_ready"] is ready


def test_status_cli_masks_raw_failure(monkeypatch, capsys):
    async def unknown(_root):
        raise RuntimeError("private-config-canary")

    monkeypatch.setattr(entry, "inspect_protection", unknown)
    monkeypatch.setattr(sys, "argv", ["formal-budget", "--status"])
    assert entry.main() == 1
    assert json.loads(capsys.readouterr().out) == {
        "status": "unavailable",
        "code": "FORMAL_BUDGET_STATUS_UNAVAILABLE",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode,enabled,status", [(False, True, "disabled"), (True, False, "producer_stopped")]
)
async def test_disabled_status_needs_no_binding_database(
    monkeypatch, tmp_path, mode, enabled, status
):
    settings = EngineSettings(NEXUS_FORMAL_BUDGET_PROTECTION_ENABLED=mode)
    monkeypatch.setattr(
        entry,
        "configuration",
        lambda _root: (settings, UUID(int=1), UUID(int=2), enabled, "a" * 64),
    )

    def forbidden_database(*_args, **_kwargs):
        raise AssertionError("disabled producer must not need a binding database")

    monkeypatch.setattr(entry, "session_factory", forbidden_database)
    result = await entry.inspect_protection(tmp_path)
    assert result["status"] == status and result["budget_binding_ready"] is None
    assert result["economic_activation_permitted"] is False
