"""Registered local source commands use configured settings and safe error replies."""

import json
import sys
from uuid import UUID

import pytest
from nexus.ops import sec_index_local as module


@pytest.mark.parametrize("action", ["collect", "import", "inspect"])
def test_normal_cli_routes_exact_arguments_without_starting_other_work(monkeypatch, capsys, action):
    calls = []

    async def operation(settings, **kwargs):
        calls.append((settings, kwargs))
        return {"status": "verified", "execution_locked": True}

    monkeypatch.setattr(module, "collect_index", operation)
    monkeypatch.setattr(module, "import_index", operation)
    monkeypatch.setattr(module, "inspect_index", operation)
    monkeypatch.setattr(
        module,
        "load_local_environment",
        lambda *args: {
            "DATABASE_URL": "postgresql+asyncpg://synthetic@127.0.0.1:55439/synthetic",
            "SEC_CONTACT": "test@example.invalid",
        },
    )
    identifier = str(UUID(int=1))
    args = ["sec-index", action]
    if action == "inspect":
        args += ["--snapshot-id", identifier]
    else:
        args += ["--identity-snapshot-id", identifier, "--archive-dir", "/private/synthetic"]
    monkeypatch.setattr(sys, "argv", args)
    module.main()
    assert len(calls) == 1 and calls[0][0].database_url.endswith("/synthetic")
    assert calls[0][1]["snapshot_id" if action == "inspect" else "identity_snapshot_id"] == UUID(
        int=1
    )
    if action == "collect":
        assert calls[0][1]["contact"] == "test@example.invalid"
    assert json.loads(capsys.readouterr().out) == {"status": "verified", "execution_locked": True}


def test_normal_cli_never_exposes_unexpected_source_error(monkeypatch, capsys):
    async def failed(*args, **kwargs):
        raise RuntimeError("sensitive upstream detail")

    monkeypatch.setattr(module, "inspect_index", failed)
    monkeypatch.setattr(module, "load_local_environment", lambda *args: {})
    monkeypatch.setattr(sys, "argv", ["sec-index", "inspect", "--snapshot-id", str(UUID(int=1))])
    with pytest.raises(SystemExit):
        module.main()
    reply = json.loads(capsys.readouterr().out)
    assert reply == {
        "status": "unavailable",
        "error_code": "SEC_INDEX_UNAVAILABLE",
        "execution_locked": True,
    }
