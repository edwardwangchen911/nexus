"""Normal identity preparation reports original failures without collecting again."""

import asyncio
import json
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.ops import agent_paper_identity as identity


@pytest.mark.parametrize("initial", [True, False])
@pytest.mark.parametrize(
    "failure,expected",
    [
        ("DEADLINE_EXCEEDED", "TIMEOUT"),
        ("IDENTITY_COLLECTION_DEADLINE_EXCEEDED", "TIMEOUT"),
        ("STATUS_REJECTED", "COLLECTION_UNAVAILABLE"),
    ],
)
def test_original_collection_failure_is_preserved_and_never_retried(
    monkeypatch, tmp_path, capsys, initial, failure, expected
):
    day = date(2026, 11, 16)
    scope = SimpleNamespace(
        day=day,
        ready=False,
        evidence_ready=False,
        capacity_exhausted=False,
        candidates=((UUID(int=1), "NVDA", UUID(int=2)),),
        bindings=(),
    )
    monkeypatch.setattr(identity, "_read_scope", AsyncMock(return_value=scope))
    parent = identity._private_parent(tmp_path)
    directory = parent / "20261116"
    payload = {
        "status": "unavailable",
        "code": "IDENTITY_COLLECTION_FAILED",
        "stage": "directory",
        "failure_phase": "request",
        "failure_code": failure,
        "elapsed_ms": 30006,
        "requests": 1,
        "authority_registered": False,
        "execution_locked": True,
    }

    def save():
        directory.mkdir(mode=0o700)
        p = directory / "result.json"
        p.write_text(json.dumps(payload))
        p.chmod(0o600)

    if not initial:
        save()
    calls = []

    async def collect(**kwargs):
        calls.append(kwargs)
        assert initial
        save()
        raise ValueError("IDENTITY_COLLECTION_FAILED")

    monkeypatch.setattr(identity, "collect_identity_sources", collect)
    args = dict(
        account_id=UUID(int=3),
        batch_id=UUID(int=4),
        expected_calendar_authority_id=UUID(int=5),
        repo_root=tmp_path,
        contact="isolated@invalid.example",
    )
    assert (
        asyncio.run(identity.prepare_agent_paper_identity_once(SimpleNamespace(), **args))
        == "unavailable"
    )
    before = (directory / "result.json").read_bytes()
    assert (
        asyncio.run(identity.prepare_agent_paper_identity_once(SimpleNamespace(), **args))
        == "unavailable"
    )
    assert (directory / "result.json").read_bytes() == before
    reports = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    assert len(reports) == 2 and all(r["failure_code"] == expected for r in reports)
    assert len(calls) == int(initial)
