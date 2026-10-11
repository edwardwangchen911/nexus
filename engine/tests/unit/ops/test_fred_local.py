"""Explicit FRED command composition; no credentials, database, or network in tests."""

import json
import sys

import httpx
import pytest
from unit.research.test_fred_observations import captured_payloads


@pytest.mark.parametrize(
    "argv",
    [
        ["canary-invalid-command"],
        ["collect", "--series", "canary-private", "--output-dir", "/canary-path"],
        ["import"],
    ],
)
def test_bad_arguments_are_fixed_and_touch_no_settings(monkeypatch, capsys, argv):
    from nexus.ops import fred_local

    monkeypatch.setattr(sys, "argv", ["fred_local", *argv])
    monkeypatch.setattr(
        fred_local, "load_local_environment", lambda *args: pytest.fail("settings touched")
    )
    with pytest.raises(SystemExit) as exited:
        fred_local.main()
    assert exited.value.code == 2
    output = capsys.readouterr()
    assert json.loads(output.err)["code"] == "FRED_REQUEST_INVALID"
    assert "canary" not in output.err


def test_collect_normal_main_uses_fixed_origin_no_retries_and_archives(
    monkeypatch, tmp_path, capsys
):
    from nexus.ops import fred_local

    originals = captured_payloads()
    calls = []
    actual_client = httpx.AsyncClient
    options = []

    def handle(request):
        calls.append(request)
        return httpx.Response(200, content=json.dumps(originals[len(calls) - 1]).encode())

    def client(**kwargs):
        options.append(kwargs)
        return actual_client(transport=httpx.MockTransport(handle), **kwargs)

    monkeypatch.setattr(fred_local.httpx, "AsyncClient", client)
    monkeypatch.setattr(
        fred_local, "load_local_environment", lambda *args: {"FRED_API_KEY": "test-key"}
    )
    directory = tmp_path / "capture"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "fred_local",
            "collect",
            "--series",
            "DGS10",
            "--start",
            "2026-08-24",
            "--end",
            "2026-08-28",
            "--vintage",
            "2026-08-28",
            "--output-dir",
            str(directory),
        ],
    )
    fred_local.main()
    assert json.loads(capsys.readouterr().out)["status"] == "collected"
    assert len(calls) == 2
    assert all(call.url.host == "api.stlouisfed.org" for call in calls)
    assert options == [{"follow_redirects": False, "timeout": 30}]
    assert (directory / "complete.json").exists()
    assert directory.stat().st_mode & 0o077 == 0
    assert b"test-key" not in (directory / "000.receipt.json").read_bytes()


def test_import_rejects_redirected_database_before_store_construction():
    from nexus.ops.daily_once import DailyOnceSettings
    from nexus.ops.fred_local import validate_local_import

    settings = DailyOnceSettings.model_validate(
        {
            "DATABASE_URL": "postgresql+asyncpg://test:test@127.0.0.1/test?host=elsewhere",
            "OBJECT_STORE_ENDPOINT": "http://127.0.0.1:9000",
            "OBJECT_STORE_REGION": "test",
            "OBJECT_STORE_BUCKET": "test",
            "OBJECT_STORE_ACCESS_KEY_ID": "test",
            "OBJECT_STORE_SECRET_ACCESS_KEY": "test",
        }
    )
    with pytest.raises(ValueError, match="FRED_CONFIG_INVALID"):
        validate_local_import(settings)


def test_main_real_loader_honors_process_key_and_local_database_override(
    monkeypatch, tmp_path, capsys
):
    from nexus.ops import fred_local
    from unit.research.test_fred_observations import parse

    expected_db = "postgresql+asyncpg://synthetic:synthetic@127.0.0.1:55436/isolated_override"
    monkeypatch.setenv("FRED_API_KEY", "synthetic-env-key")
    monkeypatch.setenv("DATABASE_URL", expected_db)
    for name, value in {
        "ENDPOINT": "http://127.0.0.1:9000",
        "REGION": "test",
        "BUCKET": "test",
        "ACCESS_KEY_ID": "test",
        "SECRET_ACCESS_KEY": "test",
    }.items():
        monkeypatch.setenv(f"OBJECT_STORE_{name}", value)
    called = []

    async def collect(request, *, key, directory):
        assert key.get_secret_value() == "synthetic-env-key"
        called.append("collect")
        return {"status": "collected"}

    async def importing(settings, *, collection):
        assert settings.database_url == expected_db
        fred_local.validate_local_import(settings)
        called.append("import")
        return {"status": "imported"}

    monkeypatch.setattr(fred_local, "collect", collect)
    monkeypatch.setattr(fred_local, "import_collection", importing)
    monkeypatch.setattr(fred_local, "load_fred_collection", lambda _: parse())
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "fred_local",
            "--repo-root",
            str(tmp_path),
            "collect",
            "--series",
            "DGS10",
            "--start",
            "2026-08-24",
            "--end",
            "2026-08-28",
            "--vintage",
            "2026-08-28",
            "--output-dir",
            str(tmp_path / "out"),
        ],
    )
    fred_local.main()
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "fred_local",
            "--repo-root",
            str(tmp_path),
            "import",
            "--collection-dir",
            str(tmp_path / "out"),
        ],
    )
    fred_local.main()
    assert called == ["collect", "import"]
    output = capsys.readouterr()
    assert "synthetic-env-key" not in output.out + output.err


@pytest.mark.parametrize("status", [400, 401, 403, 429, 503])
def test_normal_collection_preserves_only_http_status_in_private_failure(
    monkeypatch, tmp_path, capsys, status
):
    from nexus.ops import fred_local

    actual_client = httpx.AsyncClient
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(status, content=b"private response canary-secret")

    monkeypatch.setattr(
        fred_local.httpx,
        "AsyncClient",
        lambda **kwargs: actual_client(transport=httpx.MockTransport(handle), **kwargs),
    )
    monkeypatch.setattr(
        fred_local, "load_local_environment", lambda *args: {"FRED_API_KEY": "canary-key"}
    )
    directory = tmp_path / "capture"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "fred_local",
            "collect",
            "--series",
            "DGS10",
            "--start",
            "2026-08-24",
            "--end",
            "2026-08-28",
            "--vintage",
            "2026-08-28",
            "--output-dir",
            str(directory),
        ],
    )
    with pytest.raises(SystemExit) as exited:
        fred_local.main()
    assert exited.value.code == 1
    failure = json.loads((directory / "failure.json").read_bytes())
    assert failure["http_status"] == status
    assert failure["stage"] == "metadata_fetch"
    assert failure["code"] == (
        "RETRY_BUDGET_EXHAUSTED" if status in {429, 503} else "STATUS_REJECTED"
    )
    assert len(calls) == 1
    output = capsys.readouterr()
    assert "canary" not in output.out + output.err
    assert "canary" not in (directory / "failure.json").read_text()
    assert not (directory / "complete.json").exists()
