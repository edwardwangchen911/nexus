"""Immutable local serving preserves current facts and keeps unrelated credentials out."""

import importlib.util
import json
import os
import subprocess
from pathlib import Path

import pytest
from dotenv import dotenv_values

SOURCE = Path(__file__).resolve().parents[4] / "scripts/macos/local-web-release.py"
spec = importlib.util.spec_from_file_location("local_web_release", SOURCE)
web = importlib.util.module_from_spec(spec)
spec.loader.exec_module(web)


@pytest.fixture
def release(tmp_path):
    root = tmp_path / "repo"
    root.mkdir()
    subprocess.run(["git", "init", "-q", str(root)], check=True)
    (root / "source.txt").write_text("controlled source")
    subprocess.run(["git", "-C", str(root), "add", "source.txt"], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(root),
            "-c",
            "user.name=fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ],
        check=True,
    )
    built = tmp_path / "release"
    built.mkdir(mode=0o700)
    (built / "dist/server").mkdir(parents=True)
    (built / "dist/client").mkdir()
    (built / "dist/server/index.js").write_text("export default {}")
    (built / "dist/client/app.js").write_text("console.log('compiled')")
    (built / "dist/server/wrangler.json").write_text(
        json.dumps(
            dict(
                compatibility_date="2026-01-01",
                compatibility_flags=["nodejs_compat"],
                assets={"directory": "../client"},
            )
        )
    )
    manifest = dict(
        schema="nexus_local_web_release_v1",
        revision=web.git(root, "rev-parse", "HEAD"),
        tree=web.git(root, "rev-parse", "HEAD^{tree}"),
        artifacts=web.artifact_hashes(built),
    )
    (built / web.MANIFEST).write_text(json.dumps(manifest))
    env = root / ".env.local"
    env.write_text(
        'NEXUS_ENGINE_BASE_URL=http://127.0.0.1:58000\nNEXUS_ENGINE_HMAC_CURRENT_SECRET="fake-bff-secret"\nOPENAI_API_KEY=MODEL_CANARY\nFOSUN_PASSWORD=BROKER_CANARY\n'
    )
    env.chmod(0o600)
    return root, built, env


def test_check_is_readonly_and_does_not_launch_or_write_runtime(release, monkeypatch):
    root, built, env = release
    monkeypatch.setattr(os, "execve", lambda *a: pytest.fail("launched"))
    assert web.serve(root, built, env, 5173, check=True)["external_requests"] == 0
    assert not (built / "runtime").exists()


def test_stale_source_and_tampered_assets_are_rejected(release):
    root, built, env = release
    (built / "dist/client/app.js").write_text("tampered")
    with pytest.raises(ValueError, match="ARTIFACT_MISMATCH"):
        web.serve(root, built, env, 5173, check=True)
    manifest = json.loads((built / web.MANIFEST).read_bytes())
    manifest["artifacts"] = web.artifact_hashes(built)
    manifest["revision"] = "0" * 40
    (built / web.MANIFEST).write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="VERSION_MISMATCH"):
        web.serve(root, built, env, 5173, check=True)


def test_serve_runs_prebuilt_local_worker_with_only_private_bff_bindings(release, monkeypatch):
    root, built, env = release
    calls = []

    class Probe:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def bind(self, address):
            assert address == ("127.0.0.1", 5173)

    monkeypatch.setattr(web.socket, "socket", Probe)

    class Exec(Exception):
        pass

    def capture(*args):
        calls.append(args)
        raise Exec

    monkeypatch.setattr(os, "execve", capture)
    monkeypatch.setenv("OPENAI_API_KEY", "PARENT_MODEL_CANARY")
    monkeypatch.setenv("FOSUN_PASSWORD", "PARENT_BROKER_CANARY")
    with pytest.raises(Exec):
        web.serve(root, built, env, 5173)
    executable, argv, child = calls[0]
    assert "--local" in argv and "--no-bundle" in argv
    assert argv[argv.index("--ip") + 1] == "127.0.0.1"
    assert "--tunnel" not in argv and "--remote" not in argv
    assert not any("CANARY" in value or "fake-bff-secret" in value for value in child.values())
    bindings = dotenv_values(built / "runtime/.dev.vars", interpolate=False)
    assert bindings["NEXUS_ENGINE_HMAC_CURRENT_SECRET"] == "fake-bff-secret"
    assert "OPENAI_API_KEY" not in bindings and "FOSUN_PASSWORD" not in bindings
    assert (built / "runtime/.dev.vars").stat().st_mode & 0o777 == 0o600
    assert json.loads((built / "runtime/wrangler.json").read_bytes())["no_bundle"] is True
    assert web.verify_release(root, built)


def test_non_loopback_backend_and_public_env_are_rejected(release):
    root, built, env = release
    env.write_text(
        "NEXUS_ENGINE_BASE_URL=https://example.invalid\nNEXUS_ENGINE_HMAC_CURRENT_SECRET=fake\n"
    )
    with pytest.raises(ValueError, match="BFF_CONFIG_INVALID"):
        web.serve(root, built, env, 5173, check=True)
    env.chmod(0o644)
    with pytest.raises(ValueError, match="ENV_UNSAFE"):
        web.serve(root, built, env, 5173, check=True)


def test_runtime_binding_symlink_cannot_overwrite_another_file(release, monkeypatch, tmp_path):
    root, built, env = release
    runtime = built / "runtime"
    runtime.mkdir(mode=0o700)
    protected = tmp_path / "protected.txt"
    protected.write_text("preserve this file")
    protected.chmod(0o600)
    (runtime / ".dev.vars").symlink_to(protected)

    class Probe:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def bind(self, address):
            pass

    monkeypatch.setattr(web.socket, "socket", Probe)
    monkeypatch.setattr(os, "execve", lambda *args: None)
    with pytest.raises((OSError, ValueError)):
        web.serve(root, built, env, 5173)
    assert protected.read_text() == "preserve this file"
