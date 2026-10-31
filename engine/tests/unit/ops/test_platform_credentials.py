"""Offline Platform identity and gift-only admission, using synthetic private files."""

import getpass
import hashlib
import importlib
import io
import json
import os
import socket
import sys
import warnings
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from nexus.ops import openai_local

KEY = "synthetic-opaque-platform-credential"
NOW = datetime(2026, 10, 31, 8, tzinfo=UTC)
ENDPOINT = "https://api.openai.com/v1/responses"
PURPOSE = "platform-gifted-terra-v1-20261031"


def test_platform_credentials_has_a_normal_module_entry():
    assert importlib.util.find_spec("nexus.ops.platform_credentials") is not None


@pytest.fixture
def pc(tmp_path, monkeypatch):
    assert importlib.util.find_spec("nexus.ops.platform_credentials") is not None
    module = importlib.import_module("nexus.ops.platform_credentials")
    monkeypatch.setattr(os, "environ", {})
    monkeypatch.chdir(tmp_path)
    (tmp_path / "engine").mkdir()
    (tmp_path / "engine/pyproject.toml").write_text("# synthetic repository\n")

    def forbidden(*args, **kwargs):
        pytest.fail("Platform credential checks must have zero external effects")

    for name in ("prepare_database", "verify_connection", "session_factory", "AsyncOpenAI"):
        monkeypatch.setattr(openai_local, name, forbidden)
    monkeypatch.setattr(openai_local.httpx, "AsyncClient", forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket.socket, "connect_ex", forbidden)
    return module


def private_file(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    path.chmod(0o600)
    return path


@pytest.fixture
def admission(pc, tmp_path):
    private_file(tmp_path / pc.PLATFORM_KEY_PATH, (KEY + "\n").encode())
    original = b"synthetic manual console evidence: no personal account data"
    artifact = private_file(tmp_path / ".secrets/proof/console.txt", original)
    proof = {
        "version": 1,
        "kind": "manual_platform_console",
        "purpose": PURPOSE,
        "endpoint": ENDPOINT,
        "credential_fingerprint": pc.credential_fingerprint(KEY),
        "organization_id": "org-synthetic",
        "project_id": "proj-synthetic",
        "key_ownership_confirmed": True,
        "gift_claim_active": True,
        "gift_balance_usd": "10.00",
        "gift_expires_at": "2026-11-01T00:00:00Z",
        "purchased_balance_usd": "0",
        "automatic_recharge_enabled": False,
        "other_paid_fallback_enabled": False,
        "organization_exclusive_window_confirmed": True,
        "other_usage_bounded": True,
        "other_usage_upper_bound_usd": "1.00",
        "inflight_usage_bounded": True,
        "unknown_inflight_present": True,
        "unreflected_usage_upper_bound_usd": "1.50",
        "verified_at": "2026-10-31T07:59:00Z",
        "window_start": "2026-10-31T07:55:00Z",
        "window_end": "2026-10-31T08:30:00Z",
        "console_evidence_file": artifact.name,
        "console_evidence_sha256": hashlib.sha256(original).hexdigest(),
    }
    path = artifact.with_name("funding.json")
    private_file(path, json.dumps(proof).encode())
    return tmp_path, path, proof


def credential(pc, root):
    return pc.resolve_platform_credential(
        root,
        expected_fingerprint=pc.credential_fingerprint(KEY),
        organization_id="org-synthetic",
        project_id="proj-synthetic",
        endpoint=ENDPOINT,
    )


def check(pc, admission, **overrides):
    root, path, _ = admission
    arguments = {
        "purpose": PURPOSE,
        "now": NOW,
        "required_usd": Decimal("2"),
        "local_held_usd": Decimal("1"),
        "required_valid_until": NOW + timedelta(minutes=5),
        "local_spent_since_proof_usd": Decimal("0"),
    }
    arguments.update(overrides)
    return pc.check_platform_eligibility(
        credential(pc, root), pc.load_platform_funding_proof(path), **arguments
    )


def test_identity_and_gift_check_bind_actual_key_without_exposing_it(pc, admission):
    resolved = credential(pc, admission[0])
    result = check(pc, admission)
    assert resolved.request_headers() == {
        "Authorization": "Bearer " + KEY,
        "OpenAI-Organization": "org-synthetic",
        "OpenAI-Project": "proj-synthetic",
    }
    assert result.available_gift_usd == Decimal("6.50")
    assert result.credential_binding_hash == resolved.binding_hash
    assert result.funding_proof_hash == pc.load_platform_funding_proof(admission[1]).proof_hash
    assert KEY not in repr(resolved) + repr(result)
    assert not hasattr(resolved, "identity_verified")


@pytest.mark.parametrize(
    "field,value",
    [
        ("organization_id", "org-other"),
        ("project_id", "proj-other"),
        ("credential_fingerprint", "a" * 64),
        ("endpoint", "https://elsewhere.invalid/v1/responses"),
        ("purpose", "old-experiment"),
        ("key_ownership_confirmed", False),
        ("gift_claim_active", False),
        ("gift_balance_usd", "4.49"),
        ("gift_expires_at", "2026-10-31T08:04:59Z"),
        ("purchased_balance_usd", "0.01"),
        ("automatic_recharge_enabled", True),
        ("other_paid_fallback_enabled", True),
        ("organization_exclusive_window_confirmed", False),
        ("other_usage_bounded", False),
        ("inflight_usage_bounded", False),
        ("unreflected_usage_upper_bound_usd", "0"),
        ("window_start", "2026-10-31T08:01:00Z"),
        ("window_end", "2026-10-31T08:04:59Z"),
        ("verified_at", "2026-10-31T08:01:00Z"),
        ("gift_balance_usd", "NaN"),
        ("gift_balance_usd", 10.00),
        ("other_usage_upper_bound_usd", "-1"),
        ("key_ownership_confirmed", "true"),
        ("unrecognized_authority", True),
    ],
)
def test_unqualified_manual_proof_never_admits(pc, admission, field, value):
    _, path, proof = admission
    proof[field] = value
    path.write_text(json.dumps(proof))
    with pytest.raises(pc.PlatformCredentialError):
        check(pc, admission)


@pytest.mark.parametrize("field", ["gift_claim_active", "project_id", "inflight_usage_bounded"])
def test_missing_proof_fields_fail_closed(pc, admission, field):
    _, path, proof = admission
    del proof[field]
    path.write_text(json.dumps(proof))
    with pytest.raises(pc.PlatformCredentialError):
        check(pc, admission)


@pytest.mark.parametrize("change", ["missing", "world_readable", "symlink", "changed_artifact"])
def test_private_original_proof_is_required(pc, admission, change):
    _, path, proof = admission
    if change == "missing":
        path.unlink()
    elif change == "world_readable":
        path.chmod(0o644)
    elif change == "symlink":
        original = path.with_name("original.json")
        path.rename(original)
        path.symlink_to(original)
    else:
        path.with_name(proof["console_evidence_file"]).write_bytes(b"changed")
    with pytest.raises(pc.PlatformCredentialError):
        check(pc, admission)


@pytest.mark.parametrize(
    "overrides",
    [
        {"purpose": "old-experiment"},
        {"local_held_usd": Decimal("7")},
        {"local_spent_since_proof_usd": Decimal("5")},
        {"required_usd": Decimal("NaN")},
        {"required_usd": Decimal("-0.01")},
        {"local_held_usd": Decimal("-1")},
        {"now": NOW.replace(tzinfo=None)},
        {"required_valid_until": NOW},
        {"required_valid_until": NOW + timedelta(hours=1)},
    ],
)
def test_call_time_and_shared_unknown_protection_are_not_declarations(pc, admission, overrides):
    with pytest.raises(pc.PlatformCredentialError):
        check(pc, admission, **overrides)


def test_already_reserved_dispatch_can_recheck_zero_increment_without_releasing_occupancy(
    pc, admission
):
    assert check(pc, admission, required_usd=Decimal("0")).available_gift_usd == Decimal("6.50")
    with pytest.raises(pc.PlatformCredentialError, match="PLATFORM_BALANCE_INSUFFICIENT"):
        check(pc, admission, required_usd=Decimal("0"), local_held_usd=Decimal("8"))


def test_currency_rounding_never_turns_insufficient_gift_into_eligibility(pc, admission):
    assert check(pc, admission, required_usd=Decimal("6.50"))
    with pytest.raises(pc.PlatformCredentialError):
        check(
            pc,
            admission,
            required_usd=Decimal("6.50"),
            local_spent_since_proof_usd=Decimal("0.00000000000000000000000000001"),
        )


def test_platform_key_resolution_never_borrows_the_old_key_file(pc, tmp_path):
    private_file(tmp_path / openai_local._KEY_PATH, (KEY + "\n").encode())
    with pytest.raises(pc.PlatformCredentialError, match="PLATFORM_CREDENTIAL_MISSING"):
        credential(pc, tmp_path)


def test_duplicate_json_fields_cannot_restate_a_funding_fact(pc, admission):
    path = admission[1]
    path.write_text(
        path.read_text().replace(
            '"gift_claim_active": true', '"gift_claim_active": false, "gift_claim_active": true'
        )
    )
    with pytest.raises(pc.PlatformCredentialError, match="PLATFORM_PROOF_INVALID"):
        check(pc, admission)


def test_empty_console_original_is_not_funding_evidence(pc, admission):
    _, path, record = admission
    path.with_name(record["console_evidence_file"]).write_bytes(b"")
    record["console_evidence_sha256"] = hashlib.sha256(b"").hexdigest()
    path.write_text(json.dumps(record))
    with pytest.raises(pc.PlatformCredentialError, match="PLATFORM_PROOF_INVALID"):
        check(pc, admission)


@pytest.mark.parametrize("change", ["symlink", "world_readable", "hardlink", "traversal"])
def test_console_original_has_the_same_private_file_boundary(pc, admission, change):
    root, path, record = admission
    artifact = path.with_name(record["console_evidence_file"])
    if change == "symlink":
        outside = root / "console-outside"
        artifact.rename(outside)
        artifact.symlink_to(outside)
    elif change == "world_readable":
        artifact.chmod(0o644)
    elif change == "hardlink":
        os.link(artifact, root / "console-link")
    else:
        record["console_evidence_file"] = "../console.txt"
        path.write_text(json.dumps(record))
    with pytest.raises(pc.PlatformCredentialError):
        check(pc, admission)


@pytest.mark.parametrize("source", ["process", "dotenv"])
def test_env_override_must_match_the_actual_wire_credential(pc, admission, monkeypatch, source):
    root = admission[0]
    if source == "process":
        monkeypatch.setenv("OPENAI_API_KEY", "different-synthetic-env-key")
    else:
        (root / "infra").mkdir()
        (root / "infra/.env.local").write_text("OPENAI_API_KEY=different-synthetic-env-key\n")
    with pytest.raises(pc.PlatformCredentialError, match="PLATFORM_CREDENTIAL_BINDING_MISMATCH"):
        credential(pc, root)
    monkeypatch.setenv("OPENAI_API_KEY", KEY)
    assert credential(pc, root).request_headers()["Authorization"] == "Bearer " + KEY


@pytest.mark.parametrize(
    "name,value",
    [
        ("OPENAI_BASE_URL", "https://elsewhere.invalid/v1"),
        ("OPENAI_ORG_ID", "org-other"),
        ("OPENAI_PROJECT_ID", "proj-other"),
    ],
)
def test_destination_environment_cannot_silently_change_identity(
    pc, admission, monkeypatch, name, value
):
    monkeypatch.setenv(name, value)
    with pytest.raises(pc.PlatformCredentialError):
        credential(pc, admission[0])


def cli(pc, monkeypatch, *args):
    monkeypatch.setattr(sys, "argv", ["nexus-platform-local", *args])
    return pc.main()


def test_dedicated_save_uses_private_independent_path_and_has_no_authority(
    pc, tmp_path, monkeypatch, capsys
):
    original = private_file(tmp_path / openai_local._KEY_PATH, b"old-sdk-openai-key\n")
    grant = private_file(tmp_path / ".secrets/synthetic-grant.json", b"old-grant")
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(getpass, "getpass", lambda prompt: KEY)
    assert cli(pc, monkeypatch, "save-key") == 0
    output = capsys.readouterr()
    assert json.loads(output.out)["status"] == "PLATFORM_KEY_SAVED"
    assert json.loads(output.out)["dispatch_authority"] is False
    assert KEY not in output.out + output.err
    saved = tmp_path / pc.PLATFORM_KEY_PATH
    assert saved.read_text() == KEY + "\n" and saved.stat().st_mode & 0o777 == 0o600
    assert original.read_bytes() == b"old-sdk-openai-key\n" and grant.read_bytes() == b"old-grant"
    assert credential(pc, tmp_path).fingerprint == pc.credential_fingerprint(KEY)
    assert cli(pc, monkeypatch, "save-key") == 1
    assert json.loads(capsys.readouterr().out)["code"] == "PLATFORM_KEY_ALREADY_EXISTS"
    assert saved.read_text() == KEY + "\n"


def test_key_only_does_not_even_load_settings(pc, tmp_path, monkeypatch, capsys):
    def forbidden(*args, **kwargs):
        pytest.fail("saving a key must not load configuration")

    monkeypatch.setattr(openai_local, "local_settings", forbidden)
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(getpass, "getpass", lambda prompt: KEY)
    assert cli(pc, monkeypatch, "save-key") == 0
    assert KEY not in capsys.readouterr().out


@pytest.mark.parametrize("kind", ["file", "symlink", "parent_symlink"])
def test_platform_save_never_overwrites_existing_objects(pc, tmp_path, monkeypatch, kind, capsys):
    key_path = tmp_path / pc.PLATFORM_KEY_PATH
    outside = private_file(tmp_path / "outside" / "api-key", b"original")
    key_path.parent.parent.mkdir(exist_ok=True)
    if kind == "parent_symlink":
        key_path.parent.symlink_to(outside.parent, target_is_directory=True)
    else:
        key_path.parent.mkdir()
        if kind == "file":
            private_file(key_path, b"original")
        else:
            key_path.symlink_to(outside)
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr(getpass, "getpass", lambda prompt: KEY)
    assert cli(pc, monkeypatch, "save-key") == 1
    assert key_path.read_bytes() == b"original" and outside.read_bytes() == b"original"
    assert KEY not in capsys.readouterr().out


@pytest.mark.parametrize("mode", ["not_tty", "echo_fallback", "prompt_error", "flush_error"])
def test_save_failure_is_private_and_never_retried(pc, tmp_path, monkeypatch, capsys, mode):
    monkeypatch.setattr(sys.stdin, "isatty", lambda: mode != "not_tty")

    def prompt(_):
        if mode == "echo_fallback":
            warnings.warn("hidden input unavailable", getpass.GetPassWarning, stacklevel=2)
            pytest.fail("must stop before echo fallback")
        if mode == "prompt_error":
            raise ValueError("PLATFORM_" + KEY)
        return KEY

    monkeypatch.setattr(getpass, "getpass", prompt)
    if mode == "flush_error":

        class PartialWrite(io.FileIO):
            def write(self, data):
                if self.tell() == 0:
                    super().write(data[:5])
                raise OSError(KEY)

        monkeypatch.setattr(
            os,
            "fdopen",
            lambda fd, mode: io.TextIOWrapper(io.BufferedWriter(PartialWrite(fd, mode))),
        )
    assert cli(pc, monkeypatch, "save-key") == 1
    output = capsys.readouterr()
    assert KEY not in output.out + output.err
    assert json.loads(output.out)["status"] == "blocked"
    path = tmp_path / pc.PLATFORM_KEY_PATH
    if mode == "flush_error":
        assert path.read_bytes() == b"synth"
    else:
        assert not path.exists()


def test_check_cli_only_reports_bound_eligibility_without_account_details(
    pc, admission, monkeypatch, capsys
):
    monkeypatch.setattr(pc, "_now", lambda: NOW)
    assert (
        cli(
            pc,
            monkeypatch,
            "check",
            "--proof-file",
            str(admission[1]),
            "--required-usd",
            "2",
            "--local-held-usd",
            "1",
            "--local-spent-since-proof-usd",
            "0",
            "--valid-until",
            "2026-10-31T08:05:00Z",
        )
        == 0
    )
    output = capsys.readouterr()
    result = json.loads(output.out)
    assert result["status"] == "PLATFORM_ELIGIBLE_AT_CHECK"
    assert result["dispatch_authority"] is False and result["model_requests"] == 0
    assert "funding_proof_hash" in result and "credential_binding_hash" in result
    assert all(
        value not in output.out + output.err for value in [KEY, "org-synthetic", "proj-synthetic"]
    )


def test_normal_cli_entrypoint_is_registered(pc):
    import tomllib

    pyproject = Path(__file__).resolve().parents[3] / "pyproject.toml"
    assert (
        tomllib.loads(pyproject.read_text())["project"]["scripts"]["nexus-platform-local"]
        == "nexus.ops.platform_credentials:main"
    )
