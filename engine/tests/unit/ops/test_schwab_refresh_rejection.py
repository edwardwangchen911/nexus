"""Normal refresh remembers a 400 for exactly the rejected local credential version."""

import asyncio
import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from nexus.providers.market import schwab_oauth as oauth

NOW = datetime(2026, 11, 17, tzinfo=UTC)
CODE = "SCHWAB_OAUTH_REFRESH_PREVIOUSLY_REJECTED"
INVALID = "SCHWAB_OAUTH_REFRESH_REJECTION_STORE_INVALID"


def setup_root(root):
    (root / "infra").mkdir()
    (root / "infra/.env.local").write_text(
        "NEXUS_SCHWAB_APP_KEY=fake-key\nNEXUS_SCHWAB_APP_SECRET=fake-secret\nNEXUS_SCHWAB_CALLBACK_URL=https://127.0.0.1\n"
    )
    return oauth.save_schwab_token(
        root,
        oauth.SchwabOAuthToken(
            "fake-access", "fake-refresh", NOW - timedelta(hours=1), NOW - timedelta(minutes=30)
        ),
    )


async def invoke(root, client, force=False):
    return await oauth.load_or_refresh_schwab_token(
        root,
        client,
        process_environment={},
        now=NOW,
        refresh_window=timedelta(minutes=5),
        force=force,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("concurrent", [False, True])
async def test_same_version_rejection_survives_calls_and_force(tmp_path, concurrent):
    path = setup_root(tmp_path)
    original = path.read_bytes()
    calls = []

    async def respond(request):
        calls.append(1)
        await asyncio.sleep(0)
        return httpx.Response(
            400, json={"error_description": "never save fake-refresh fake-secret"}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:

        async def rejected():
            with pytest.raises(oauth.SchwabOAuthError) as caught:
                await invoke(tmp_path, client)
            return str(caught.value)

        outcomes = (
            await asyncio.gather(rejected(), rejected())
            if concurrent
            else [await rejected(), await rejected()]
        )
        assert sorted(outcomes) == sorted(["SCHWAB_OAUTH_REFRESH_HTTP_400", CODE])
        with pytest.raises(oauth.SchwabOAuthError, match=CODE):
            await invoke(tmp_path, client, force=True)
    assert len(calls) == 1 and path.read_bytes() == original
    files = tuple((path.parent / ".refresh-rejections").iterdir())
    assert len(files) == 1
    content = files[0].read_text()
    assert "fake-refresh" not in content and "fake-secret" not in content
    before = {
        p: (p.read_bytes(), p.stat().st_mtime_ns) for p in path.parent.rglob("*") if p.is_file()
    }
    status = oauth.inspect_schwab_token_readiness(tmp_path, now=NOW)
    assert status["remote_authentication"] == "known_rejected"
    assert status["oauth_error_code"] == "unknown"
    assert status["local_access_token"] == "refresh_required"
    assert before == {
        p: (p.read_bytes(), p.stat().st_mtime_ns) for p in path.parent.rglob("*") if p.is_file()
    }
    assert "fake-" not in json.dumps(status)


@pytest.mark.asyncio
@pytest.mark.parametrize("replacement", ["token", "config"])
async def test_new_credentials_do_not_consume_old_failure(tmp_path, replacement):
    path = setup_root(tmp_path)
    count = []

    def respond(request):
        count.append(1)
        return (
            httpx.Response(400)
            if len(count) == 1
            else httpx.Response(
                200,
                json={
                    "access_token": "new-access",
                    "refresh_token": "new-refresh",
                    "expires_in": 1800,
                    "token_type": "Bearer",
                },
            )
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(oauth.SchwabOAuthError):
            await invoke(tmp_path, client)
        old = {p: p.read_bytes() for p in (path.parent / ".refresh-rejections").iterdir()}
        if replacement == "token":
            oauth.save_schwab_token(
                tmp_path,
                oauth.SchwabOAuthToken(
                    "replacement-access",
                    "replacement-refresh",
                    NOW - timedelta(hours=1),
                    NOW - timedelta(minutes=30),
                ),
            )
        else:
            p = tmp_path / "infra/.env.local"
            p.write_text(p.read_text().replace("fake-secret", "replacement-secret"))
        assert (
            oauth.inspect_schwab_token_readiness(tmp_path, now=NOW)["remote_authentication"]
            == "not_checked"
        )
        assert (await invoke(tmp_path, client)).access_token == "new-access"
        assert len(count) == 2 and all(p.read_bytes() == v for p, v in old.items())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", ["invalid_json", "deep_json", "world_readable", "symlink", "directory_mode"]
)
async def test_unsafe_rejection_fails_closed_without_network(tmp_path, damage):
    path = setup_root(tmp_path)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(400))
    ) as client:
        with pytest.raises(oauth.SchwabOAuthError):
            await invoke(tmp_path, client)
    directory = path.parent / ".refresh-rejections"
    record = next(directory.iterdir())
    if damage == "invalid_json":
        record.write_text("{")
    elif damage == "deep_json":
        record.write_text("[" * 1500 + "0" + "]" * 1500)
        original = oauth.json.loads

        def read_nested(*args, **kwargs):
            if args[0].startswith(b"["):
                raise RecursionError("nested JSON")
            return original(*args, **kwargs)

        # Pytest may raise the interpreter recursion limit; force the same
        # decoder boundary as a fresh normal process deterministically.
        oauth.json.loads = read_nested
    elif damage == "world_readable":
        record.chmod(0o644)
    elif damage == "directory_mode":
        directory.chmod(0o755)
    else:
        other = tmp_path / "elsewhere"
        record.rename(other)
        record.symlink_to(other)

    def forbidden(request):
        raise AssertionError("outbound request")

    try:
        async with httpx.AsyncClient(transport=httpx.MockTransport(forbidden)) as client:
            with pytest.raises(oauth.SchwabOAuthError, match=INVALID):
                await invoke(tmp_path, client, force=True)
        status = oauth.inspect_schwab_token_readiness(tmp_path, now=NOW)
        assert status["remote_authentication"] == "unknown" and status["reason_code"] == INVALID
    finally:
        if damage == "deep_json":
            oauth.json.loads = original


@pytest.mark.asyncio
async def test_unversioned_legacy_receipt_is_not_applied(tmp_path):
    setup_root(tmp_path)
    old = tmp_path / ".secrets/nexus-agent-paper/old"
    old.mkdir(parents=True)
    (old / "schwab-failure.json").write_text(
        json.dumps({"provider_error_code": "SCHWAB_OAUTH_REFRESH_HTTP_400"})
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda r: httpx.Response(
                200, json={"access_token": "new-access", "expires_in": 1800, "token_type": "Bearer"}
            )
        )
    ) as client:
        assert (await invoke(tmp_path, client)).access_token == "new-access"


@pytest.mark.asyncio
async def test_new_process_observes_rejection_and_does_not_request(tmp_path):
    import os
    import subprocess
    import sys

    setup_root(tmp_path)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda r: httpx.Response(400, json={"error": "invalid_client"})
        )
    ) as client:
        with pytest.raises(oauth.SchwabOAuthError):
            await invoke(tmp_path, client)
    code = """import asyncio,sys
from datetime import datetime,UTC,timedelta
from pathlib import Path
import httpx
from nexus.providers.market.schwab_oauth import load_or_refresh_schwab_token,SchwabOAuthError
async def main():
 def forbidden(request):raise AssertionError("network attempted after restart")
 async with httpx.AsyncClient(transport=httpx.MockTransport(forbidden)) as client:
  try:
   await load_or_refresh_schwab_token(
    Path(sys.argv[1]),client,process_environment={},
    now=datetime(2026,11,17,tzinfo=UTC),refresh_window=timedelta(minutes=5),force=True)
  except SchwabOAuthError as error:print(str(error),error.oauth_error_code)
asyncio.run(main())"""
    result = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, "-c", code, str(tmp_path)],
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == CODE + " invalid_client"


@pytest.mark.asyncio
async def test_forced_rejection_is_not_bypassed_by_current_access_token(tmp_path):
    setup_root(tmp_path)
    oauth.save_schwab_token(
        tmp_path,
        oauth.SchwabOAuthToken(
            "current-access", "current-refresh", NOW, NOW + timedelta(minutes=30)
        ),
    )
    count = []

    def respond(request):
        count.append(1)
        return httpx.Response(400)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(oauth.SchwabOAuthError, match="SCHWAB_OAUTH_REFRESH_HTTP_400"):
            await invoke(tmp_path, client, force=True)
        with pytest.raises(oauth.SchwabOAuthError, match=CODE):
            await invoke(tmp_path, client)
    assert len(count) == 1
    assert (
        oauth.inspect_schwab_token_readiness(tmp_path, now=NOW)["local_access_token"] == "current"
    )


@pytest.mark.parametrize("code", [CODE, INVALID])
def test_service_preflight_keeps_rejection_optional(monkeypatch, tmp_path, code):
    from nexus.ops import agent_paper_loop as loop
    from unit.ops.test_agent_paper_loop import service_ready_environment

    output = tmp_path / ".secrets/nexus-agent-paper"
    output.mkdir(parents=True, mode=0o700)
    monkeypatch.setattr(loop, "load_schwab_app_config", lambda *a: object())
    monkeypatch.setattr(loop, "load_schwab_token", lambda *a: object())
    monkeypatch.setattr(loop, "require_live_schwab_access_token", lambda *a, **kw: "fake-access")
    monkeypatch.setattr(loop, "_schwab_token_store_writable", lambda *a: True)
    monkeypatch.setattr(loop, "_codex_sdk_preflight_blocker", lambda: None)
    monkeypatch.setattr(
        loop,
        "inspect_schwab_token_readiness",
        lambda *a, **kw: {"reason_code": code, "remote_authentication": "known_rejected"},
    )
    result = loop.agent_paper_service_preflight(
        service_ready_environment() | {"FRED_API_KEY": "fake-fred"}, repo_root=tmp_path, now=NOW
    )
    assert result["status"] == "ready" and result["blocking_codes"] == []
    assert result["optional_codes"] == [code] and result["external_requests"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("code", [CODE, INVALID])
async def test_normal_failed_preparation_preserves_safe_guard_code(tmp_path, code):
    from nexus.ops.agent_paper_once import _raise_schwab_l1_incomplete

    path = tmp_path / "schwab-failure.json"
    with pytest.raises(ValueError, match="^SCHWAB_L1_INCOMPLETE$"):
        _raise_schwab_l1_incomplete(oauth.SchwabOAuthError(code), diagnostic_path=path)
    assert json.loads(path.read_text())["provider_error_code"] == code


@pytest.mark.asyncio
async def test_corrupt_old_record_is_unknown_store_after_credential_replacement(tmp_path):
    path = setup_root(tmp_path)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(400))
    ) as client:
        with pytest.raises(oauth.SchwabOAuthError):
            await invoke(tmp_path, client)
    next((path.parent / ".refresh-rejections").iterdir()).write_text("{")
    oauth.save_schwab_token(
        tmp_path,
        oauth.SchwabOAuthToken(
            "replacement-access",
            "replacement-refresh",
            NOW - timedelta(hours=1),
            NOW - timedelta(minutes=30),
        ),
    )
    status = oauth.inspect_schwab_token_readiness(tmp_path, now=NOW)
    assert status["remote_authentication"] == "unknown" and status["reason_code"] == INVALID
