"""Manual audit is explicit and rejects private input without echoing it."""

import json
import subprocess
import sys
from pathlib import Path
from uuid import UUID

import pytest
from nexus.ops import daily_once


@pytest.mark.parametrize("selected", [False, True])
def test_audit_cli_accepts_only_the_explicit_root_and_optional_run(monkeypatch, capsys, selected):
    root_id, run_id = UUID(int=101), UUID(int=102)
    args = ["daily_once", "audit", "--root-id", str(root_id)]
    if selected:
        args += ["--run-id", str(run_id)]
    monkeypatch.setattr("sys.argv", args)

    async def execute(settings, request):
        assert request.root_id == root_id
        assert request.run_id == (run_id if selected else None)
        assert request.action == "audit"
        return {"status": "recorded", "execution_locked": True}

    monkeypatch.setattr(daily_once, "run_once", execute)
    daily_once.main()
    assert json.loads(capsys.readouterr().out) == {
        "status": "recorded",
        "execution_locked": True,
    }


@pytest.mark.parametrize(
    "args",
    [
        ["--root-id", "private-account-canary"],
        ["--root-id", str(UUID(int=101)), "--run-id", "private-account-canary"],
        ["--root-id", str(UUID(int=101)), "--extra", "private-account-canary"],
    ],
)
def test_audit_cli_bad_arguments_never_reach_database(monkeypatch, capsys, args):
    monkeypatch.setattr("sys.argv", ["daily_once", "audit", *args])

    def forbidden(*args, **kwargs):
        pytest.fail("invalid audit arguments must not initialize settings")

    monkeypatch.setattr(daily_once, "DailyOnceSettings", forbidden)
    with pytest.raises(SystemExit) as error:
        daily_once.main()
    assert error.value.code == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert json.loads(output.err) == {"status": "failed", "code": "DAILY_ONCE_REQUEST_INVALID"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "endpoint",
    [
        None,
        "http://example.invalid:9000",
        "http://secret@localhost:9000",
        "http://localhost:9000/path",
        "http://localhost:9000?private=canary",
    ],
)
async def test_audit_rejects_implicit_or_remote_storage_before_database(monkeypatch, endpoint):
    for name in (
        "ENDPOINT",
        "REGION",
        "BUCKET",
        "ACCESS_KEY_ID",
        "SECRET_ACCESS_KEY",
    ):
        monkeypatch.delenv(f"OBJECT_STORE_{name}", raising=False)
    config = daily_once.DailyOnceSettings(
        database_url="postgresql+asyncpg://synthetic:test@127.0.0.1/test",
        **(
            {}
            if endpoint is None
            else {
                "object_store_endpoint": endpoint,
                "object_store_region": "test",
                "object_store_bucket": "test",
                "object_store_access_key_id": "test",
                "object_store_secret_access_key": "test",
            }
        ),
    )

    def forbidden(*args, **kwargs):
        pytest.fail("invalid audit storage must not create an engine or object client")

    monkeypatch.setattr(daily_once, "create_async_engine", forbidden)
    monkeypatch.setattr(daily_once, "S3AuditManifestObjectStore", forbidden)
    with pytest.raises(daily_once.DailyOnceError, match="^DAILY_ONCE_AUDIT_STORE_UNAVAILABLE$"):
        await daily_once.run_once(
            config,
            daily_once.DailyAuditRequest(action="audit", root_id=UUID(int=101)),
        )


@pytest.mark.parametrize("scheme", ["http", "https"])
@pytest.mark.parametrize("lowercase", [False, True])
def test_real_local_audit_client_ignores_environment_proxy_without_changing_legacy(
    tmp_path,
    scheme,
    lowercase,
):
    # A clean child environment cannot load real credentials or alter the test runner's env.
    environment = {
        "PYTHONPATH": str(Path(__file__).resolve().parents[3] / "src"),
        "AWS_CONFIG_FILE": "/dev/null",
        "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
        "AWS_EC2_METADATA_DISABLED": "true",
    }
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
        environment[name.lower() if lowercase else name] = "http://proxy.example.invalid:8080"
    source = """
import json
import sys
from nexus.audit.manifest import S3AuditManifestObjectStore
from nexus.ops.daily_once import DailyOnceSettings, _audit_store

def deny_network(event, args):
    if event in {"socket.connect", "socket.getaddrinfo"}:
        raise AssertionError("no network allowed in client configuration test")
sys.addaudithook(deny_network)
endpoint = sys.argv[1] + "://127.0.0.1:19000"
settings = DailyOnceSettings(
    object_store_endpoint=endpoint, object_store_region="us-east-1",
    object_store_bucket="synthetic-audit", object_store_access_key_id="synthetic-key",
    object_store_secret_access_key="synthetic-secret",
)
local = _audit_store(settings)._client
legacy = S3AuditManifestObjectStore(settings)._client
try:
    local_proxy = local._endpoint.http_session._proxy_config.proxy_url_for(endpoint)
    legacy_proxy = legacy._endpoint.http_session._proxy_config.proxy_url_for(endpoint)
    print(json.dumps({
        "local_direct": local_proxy is None,
        "legacy_proxy": legacy_proxy == "http://proxy.example.invalid:8080",
    }))
finally:
    local.close()
    legacy.close()
"""
    result = subprocess.run(
        [sys.executable, "-c", source, scheme],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        timeout=20,
        check=True,
    )
    assert json.loads(result.stdout) == {"local_direct": True, "legacy_proxy": True}
