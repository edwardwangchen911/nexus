"""Host-local Gateway configuration never depends on diagnostic launch scripts."""

import json
import subprocess
from pathlib import Path

import pytest
from nexus_fosun_gateway.local_runtime import load_local_gateway_settings, main


def _private(path: Path, content: str) -> None:
    path.write_text(content)
    path.chmod(0o600)


def _root(tmp_path: Path) -> Path:
    local = tmp_path / ".secrets" / "nexus-fosun-local"
    provider = tmp_path / ".secrets" / "fosun-openapi"
    local.mkdir(parents=True, mode=0o700)
    provider.mkdir(mode=0o700)
    (tmp_path / ".secrets").chmod(0o700)
    manifest = tmp_path / "contracts" / "providers" / "fosun" / "manifest.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text("{}")
    _private(
        local / "hmac.env",
        "FOSUN_GATEWAY_HMAC_CURRENT_KEY_ID=broker-local-v1\n"
        "FOSUN_GATEWAY_HMAC_CURRENT_SECRET=" + "a" * 32 + "\n"
        "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_KEY_ID=market-local-v1\n"
        "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_SECRET=" + "b" * 32 + "\n",
    )
    for name in (
        "server.pem",
        "server.key",
        "broker-ca.pem",
        "market-ca.pem",
        "server-ca.pem",
        "broker-client.pem",
        "broker-client.key",
        "market-client.pem",
        "market-client.key",
    ):
        _private(local / name, "test-pem")
    _private(provider / "api-key", "test-api-key\n")
    _private(provider / "private.pem", "test-private-key\n")
    _private(provider / "server-public.pem", "test-server-key\n")
    _private(local / "replay.sqlite3", "test-existing-replay")
    return tmp_path


def test_local_settings_keep_read_only_scope_and_existing_replay(tmp_path: Path) -> None:
    root = _root(tmp_path)

    settings = load_local_gateway_settings(root)

    assert settings.transport_mode == "local_read_only"
    assert settings.capability == "READ_ONLY"
    assert settings.replay_store_path == root / ".secrets/nexus-fosun-local/replay.sqlite3"
    assert (root / ".secrets/nexus-fosun-local/replay.sqlite3").read_text() == (
        "test-existing-replay"
    )
    assert settings.activation_probe_artifact is None
    assert settings.fsopenapi_api_key.get_secret_value() == "test-api-key"


def test_local_settings_reject_symlinked_provider_material(tmp_path: Path) -> None:
    root = _root(tmp_path)
    key = root / ".secrets/fosun-openapi/api-key"
    key.unlink()
    key.symlink_to(root / ".secrets/fosun-openapi/private.pem")

    with pytest.raises(ValueError, match="LOCAL_GATEWAY_MATERIAL_UNSAFE"):
        load_local_gateway_settings(root)


def test_local_settings_reject_public_hmac_file(tmp_path: Path) -> None:
    root = _root(tmp_path)
    (root / ".secrets/nexus-fosun-local/hmac.env").chmod(0o644)

    with pytest.raises(ValueError, match="LOCAL_GATEWAY_MATERIAL_UNSAFE"):
        load_local_gateway_settings(root)


def test_local_settings_reject_empty_provider_key(tmp_path: Path) -> None:
    root = _root(tmp_path)
    _private(root / ".secrets/fosun-openapi/api-key", " \n")

    with pytest.raises(ValueError, match="LOCAL_GATEWAY_MATERIAL_UNSAFE"):
        load_local_gateway_settings(root)


def test_local_settings_reject_symlinked_health_client_key(tmp_path: Path) -> None:
    root = _root(tmp_path)
    key = root / ".secrets/nexus-fosun-local/broker-client.key"
    key.unlink()
    key.symlink_to(root / ".secrets/nexus-fosun-local/market-client.key")

    with pytest.raises(ValueError, match="LOCAL_GATEWAY_MATERIAL_UNSAFE"):
        load_local_gateway_settings(root)


def test_checkout_release_rejects_modified_gateway_source(tmp_path: Path) -> None:
    import nexus_fosun_gateway.local_runtime as runtime_module

    checkout_release = getattr(runtime_module, "checkout_release", None)
    assert callable(checkout_release)

    root = _root(tmp_path)
    source = root / "integrations/fosun_gateway/src/nexus_fosun_gateway/main.py"
    source.parent.mkdir(parents=True)
    source.write_text("original\n")
    for args in (
        ("init", "-q"),
        (
            "add",
            "integrations/fosun_gateway/src/nexus_fosun_gateway/main.py",
            "contracts/providers/fosun/manifest.json",
        ),
        ("-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "base"),
    ):
        subprocess.run(("git", "-C", str(root), *args), check=True, capture_output=True)
    assert checkout_release(root) is not None

    source.write_text("modified\n")

    assert checkout_release(root) is None


def test_checkout_release_fails_closed_when_git_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import nexus_fosun_gateway.local_runtime as runtime_module

    def missing_git(*_args: object, **_kwargs: object) -> None:
        raise FileNotFoundError

    monkeypatch.setattr(runtime_module.subprocess, "run", missing_git)

    assert runtime_module.checkout_release(tmp_path) is None


def test_local_check_is_value_free_and_does_not_start_service(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = _root(tmp_path)
    monkeypatch.setattr("nexus_fosun_gateway.local_runtime._runtime_matches_repo", lambda _: True)

    assert main(["check", "--repo-root", str(root)]) == 0

    result = json.loads(capsys.readouterr().out)
    assert result == {
        "status": "ready",
        "mode": "local_read_only",
        "capability": "READ_ONLY",
        "network_checked": False,
    }


def test_local_run_passes_fixed_settings_to_formal_gateway(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _root(tmp_path)
    monkeypatch.setattr("nexus_fosun_gateway.local_runtime._runtime_matches_repo", lambda _: True)
    monkeypatch.setattr(
        "nexus_fosun_gateway.local_runtime.checkout_release", lambda _: "a" * 40, raising=False
    )
    monkeypatch.setenv("NEXUS_RELEASE_REVISION", "a" * 40)
    received = []
    monkeypatch.setattr("nexus_fosun_gateway.main.run", received.append)

    assert main(["run", "--repo-root", str(root)]) == 0

    assert len(received) == 1
    assert received[0].transport_mode == "local_read_only"
    assert received[0].replay_store_path == root / ".secrets/nexus-fosun-local/replay.sqlite3"


def test_local_run_rejects_release_drift_before_formal_gateway(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    root = _root(tmp_path)
    monkeypatch.setattr("nexus_fosun_gateway.local_runtime._runtime_matches_repo", lambda _: True)
    monkeypatch.setattr(
        "nexus_fosun_gateway.local_runtime.checkout_release", lambda _: "a" * 40, raising=False
    )
    monkeypatch.setenv("NEXUS_RELEASE_REVISION", "b" * 40)
    monkeypatch.setattr(
        "nexus_fosun_gateway.main.run", lambda *_: pytest.fail("Gateway ran under drift")
    )

    assert main(["run", "--repo-root", str(root)]) == 1
    assert json.loads(capsys.readouterr().out) == {
        "status": "blocked",
        "code": "LOCAL_GATEWAY_RELEASE_UNVERIFIED",
    }


def test_local_check_rejects_runtime_from_another_checkout(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    root = _root(tmp_path)

    assert main(["check", "--repo-root", str(root)]) == 1
    assert json.loads(capsys.readouterr().out) == {
        "status": "blocked",
        "code": "LOCAL_GATEWAY_RELEASE_UNVERIFIED",
    }
