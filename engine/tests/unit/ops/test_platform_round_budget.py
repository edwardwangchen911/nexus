"""A task-bound operating limit is neither a new consent nor a provider balance."""

import json
from datetime import UTC, datetime, timedelta

import pytest
from unit.ops.test_platform_credentials import private_file
from unit.ops.test_platform_explicit_spend import check_authorization

pytest_plugins = ["unit.ops.test_platform_explicit_spend"]


@pytest.mark.asyncio
async def test_maintenance_rejects_new_round_before_files_or_database(monkeypatch):
    from argparse import Namespace

    from nexus.agents import research_maintenance
    from nexus.ops.platform_round_registration import register_round

    monkeypatch.setattr(research_maintenance, "IMAGE_MAINTENANCE", True)
    # Neither object offers any usable input: the build-fixed fence must run first.
    with pytest.raises(ValueError, match="^RESEARCH_MAINTENANCE$"):
        await register_round(None, Namespace())


def test_stable_round_budget_uses_task_policy_without_old_round_deadline(pc, authorization):
    root, path, body = authorization
    body.update(
        kind="platform_qualification_budget",
        purpose="platform-qualification-v1",
        configuration_hash="a" * 64,
        units_version="platform-structured-http-utf8-plus8192-v2",
        continuation_evidence_sha256="b" * 64,
    )
    body["task_policy_file"] = body.pop("authorization_file")
    body["task_policy_sha256"] = body.pop("authorization_sha256")
    body["registered_at"] = body.pop("authorized_at")
    now = datetime(2027, 1, 2, tzinfo=UTC)
    body.update(
        registered_at=now.isoformat(),
        window_start=now.isoformat(),
        window_end=(now + timedelta(hours=1)).isoformat(),
    )
    private_file(path, json.dumps(body).encode())
    result, _ = check_authorization(
        pc,
        (root, path, body),
        purpose="platform-qualification-v1",
        now=now,
        required_valid_until=now + timedelta(minutes=20),
    )
    assert result.available_gift_usd is None
    assert pc.load_platform_funding_proof(path).record.kind == "platform_qualification_budget"
    body["task_policy_sha256"] = "0" * 64
    private_file(path, json.dumps(body).encode())
    with pytest.raises(pc.PlatformCredentialError, match="PLATFORM_PROOF_INVALID"):
        pc.load_platform_funding_proof(path)


def test_changed_template_cannot_generate_a_round(tmp_path):
    import argparse

    from nexus.ops.platform_round_registration import _freeze
    from unit.ops.test_platform_credentials import NOW

    args = argparse.Namespace(
        source=private_file(tmp_path / "source.json", b"{}"),
        template=private_file(tmp_path / "template.json", b"{}"),
    )
    with pytest.raises(ValueError, match="PLATFORM_ROUND_TEMPLATE_CHANGED"):
        _freeze(args, NOW, {"template_sha256": "0" * 64})


def test_normal_funding_check_supports_operating_budget_without_claiming_balance(
    pc, authorization, monkeypatch, capsys
):
    import sys

    from unit.ops.test_platform_credentials import NOW
    from unit.ops.test_platform_explicit_spend import EXPERIMENT, INPUTS

    _, path, body = authorization
    body.update(
        kind="platform_qualification_budget",
        purpose="platform-qualification-v1",
        configuration_hash="a" * 64,
        units_version="platform-structured-http-utf8-plus8192-v2",
        continuation_evidence_sha256="b" * 64,
    )
    body["task_policy_file"] = body.pop("authorization_file")
    body["task_policy_sha256"] = body.pop("authorization_sha256")
    body["registered_at"] = body.pop("authorized_at")
    private_file(path, json.dumps(body).encode())
    monkeypatch.setattr(pc, "_now", lambda: NOW)
    args = [
        "nexus-platform-local",
        "check",
        "--proof-file",
        str(path),
        "--required-usd",
        "6.414912",
        "--local-held-usd",
        "2",
        "--local-spent-since-proof-usd",
        "1",
        "--valid-until",
        (NOW + timedelta(minutes=20)).isoformat(),
        "--experiment-id",
        str(EXPERIMENT),
    ]
    for value in INPUTS:
        args += ["--input-hash", value]
    monkeypatch.setattr(sys, "argv", args)
    assert pc.main() == 0
    result = json.loads(capsys.readouterr().out)
    assert result["funding_kind"] == "platform_qualification_budget"
    assert result["balance_verified"] is False and result["dispatch_authority"] is False
    assert result["model_requests"] == result["database_writes"] == result["provider_requests"] == 0
