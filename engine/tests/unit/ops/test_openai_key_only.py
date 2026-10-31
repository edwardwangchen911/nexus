"""The normal key-only entry saves one private file without activating a provider."""

import getpass
import io
import json
import os
import socket
import warnings

import pytest
from nexus.ops import openai_local

KEY = "synthetic-platform-key-canary"


@pytest.fixture
def key_only_cli(tmp_path, monkeypatch):
    (tmp_path / "engine").mkdir()
    (tmp_path / "engine/pyproject.toml").write_text("# synthetic repository marker\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(os, "environ", {})
    monkeypatch.setattr("sys.argv", ["nexus-openai-local", "save-key", "--backend", "responses"])
    monkeypatch.setattr("sys.stdin.isatty", lambda: True)
    monkeypatch.setattr(getpass, "getpass", lambda prompt: KEY)

    def forbidden(*args, **kwargs):
        pytest.fail("Key-only must not load app configuration or activate an external capability")

    loader = openai_local.local_settings
    for name in (
        "local_settings",
        "prepare_database",
        "verify_connection",
        "session_factory",
        "AsyncOpenAI",
        "status",
    ):
        monkeypatch.setattr(openai_local, name, forbidden)
    monkeypatch.setattr(openai_local.httpx, "AsyncClient", forbidden)
    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket.socket, "connect_ex", forbidden)
    return tmp_path / ".secrets/nexus-openai-local/api-key", loader


def test_save_key_only_is_private_and_usable_by_the_existing_loader(key_only_cli, capsys):
    key, loader = key_only_cli
    assert openai_local.main() == 0
    captured = capsys.readouterr()
    result = json.loads(captured.out)
    assert result == {
        "status": "OPENAI_KEY_SAVED",
        "credential_kind": "platform_api_key",
        "model_requests": 0,
        "database_writes": 0,
        "provider_requests": 0,
        "dispatch_authority": False,
        "execution_locked": True,
    }
    assert KEY not in captured.out + captured.err
    assert key.read_text() == KEY + "\n"
    assert key.stat().st_mode & 0o777 == 0o600
    settings = loader(key.parents[2], backend="responses")
    assert settings.openai_api_key.get_secret_value() == KEY


@pytest.mark.parametrize("kind", ["file", "symlink", "parent_symlink"])
def test_save_key_only_never_overwrites_or_follows_existing_credentials(
    key_only_cli, monkeypatch, capsys, kind, tmp_path
):
    key, _ = key_only_cli
    original = b"synthetic-original-credential\n"
    target = tmp_path / "outside-key"
    target.write_bytes(original)
    key.parent.parent.mkdir()
    if kind == "parent_symlink":
        outside = tmp_path / "outside-dir"
        outside.mkdir()
        (outside / "api-key").write_bytes(original)
        key.parent.symlink_to(outside, target_is_directory=True)
    else:
        key.parent.mkdir()
        if kind == "file":
            key.write_bytes(original)
        else:
            key.symlink_to(target)
    assert openai_local.main() == 1
    captured = capsys.readouterr()
    assert json.loads(captured.out)["status"] == "blocked"
    assert KEY not in captured.out + captured.err
    assert key.read_bytes() == original and target.read_bytes() == original


@pytest.mark.parametrize("value", ["", "a b", "x" * 1025])
def test_save_key_only_rejects_invalid_input_without_creating_a_key(
    key_only_cli, monkeypatch, capsys, value
):
    key, _ = key_only_cli
    monkeypatch.setattr(getpass, "getpass", lambda prompt: value)
    assert openai_local.main() == 1
    assert json.loads(capsys.readouterr().out)["code"] == "OPENAI_KEY_INVALID"
    assert not key.exists()


def test_save_key_only_requires_a_local_terminal(key_only_cli, monkeypatch, capsys):
    key, _ = key_only_cli
    monkeypatch.setattr("sys.stdin.isatty", lambda: False)
    assert openai_local.main() == 1
    assert json.loads(capsys.readouterr().out)["code"] == "OPENAI_KEY_REQUIRES_LOCAL_TERMINAL"
    assert not key.exists()


def test_save_key_only_refuses_getpass_echo_fallback(key_only_cli, monkeypatch, capsys):
    key, _ = key_only_cli

    def cannot_hide(prompt):
        warnings.warn("cannot disable echo", getpass.GetPassWarning, stacklevel=2)
        pytest.fail("Input must stop before getpass falls back to echoing")

    monkeypatch.setattr(getpass, "getpass", cannot_hide)
    assert openai_local.main() == 1
    assert json.loads(capsys.readouterr().out)["code"] == "OPENAI_KEY_SAVE_FAILED"
    assert not key.exists()


@pytest.mark.parametrize("error", [OSError(KEY), ValueError("OPENAI_" + KEY), KeyboardInterrupt()])
def test_save_key_only_errors_are_fixed_and_do_not_expose_input(
    key_only_cli, monkeypatch, capsys, error
):
    key, _ = key_only_cli

    def broken_prompt(prompt):
        raise error

    monkeypatch.setattr(getpass, "getpass", broken_prompt)
    assert openai_local.main() == 1
    captured = capsys.readouterr()
    assert KEY not in captured.out + captured.err
    assert json.loads(captured.out)["code"] in {
        "OPENAI_KEY_SAVE_FAILED",
        "OPENAI_KEY_ENTRY_CANCELLED",
    }
    assert not key.exists()


def test_save_key_only_preserves_partial_file_when_flush_fails(key_only_cli, monkeypatch, capsys):
    key, _ = key_only_cli

    class PartialDiskWrite(io.FileIO):
        def write(self, data):
            # The real exclusive-created file receives a prefix before the raw
            # write fails during TextIOWrapper's context-exit flush.
            if self.tell() == 0:
                super().write(data[:12])
            raise OSError("synthetic disk failure: " + KEY)

    def failing_fdopen(descriptor, mode):
        return io.TextIOWrapper(io.BufferedWriter(PartialDiskWrite(descriptor, mode)))

    monkeypatch.setattr(os, "fdopen", failing_fdopen)
    assert openai_local.main() == 1
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {
        "status": "blocked",
        "code": "OPENAI_KEY_SAVE_FAILED",
        "execution_locked": True,
    }
    assert KEY not in captured.out + captured.err
    assert "synthetic disk failure" not in captured.out + captured.err
    assert key.read_bytes() == b"synthetic-pl"
    assert key.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize(
    "arguments", [["--backend", "codex_sdk"], ["--backend", "responses", "--skip-key"]]
)
def test_save_key_only_rejects_ambiguous_modes(key_only_cli, monkeypatch, capsys, arguments):
    key, _ = key_only_cli
    monkeypatch.setattr("sys.argv", ["nexus-openai-local", "save-key", *arguments])
    assert openai_local.main() == 1
    assert json.loads(capsys.readouterr().out)["code"] == "OPENAI_KEY_SAVE_ARGUMENT_INVALID"
    assert not key.exists()
