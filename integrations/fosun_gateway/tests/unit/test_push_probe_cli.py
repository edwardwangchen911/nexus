import json
from pathlib import Path
from types import SimpleNamespace

import nexus_fosun_gateway.push_probe_cli as push_probe_cli
import pytest
from nexus_fosun_gateway.push_stream import PushStreamError
from pydantic import SecretStr

MANIFEST_PATH = Path(__file__).parents[4] / "contracts" / "providers" / "fosun" / "manifest.json"


class FakeBundle:
    def __init__(self, *, fail_session: bool = False) -> None:
        self.fail_session = fail_session
        self.session_calls = 0
        self.closed = False

    async def create_push_session(self) -> str:
        self.session_calls += 1
        if self.fail_session:
            raise RuntimeError("secret upstream failure")
        return "synthetic-session"

    async def aclose(self) -> None:
        self.closed = True


def _arguments(tmp_path: Path) -> list[str]:
    return [
        "--manifest",
        str(MANIFEST_PATH),
        "--code",
        "usAAPL",
        "--duration",
        "20",
        "--max-frames",
        "20",
        "--output-dir",
        str(tmp_path / "archive"),
    ]


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        provider_manifest=MANIFEST_PATH,
        fsopenapi_api_key=SecretStr("synthetic-api-key"),
        probe_https_proxy_url="http://127.0.0.1:9090",
    )


@pytest.mark.asyncio
async def test_push_probe_runs_one_session_and_one_bounded_collection(tmp_path, monkeypatch):
    bundle = FakeBundle()
    calls = []
    monkeypatch.setenv("FOSUN_PROVIDER_MANIFEST", str(MANIFEST_PATH))
    monkeypatch.setattr(
        push_probe_cli.PushProbeProviderSettings,
        "model_validate",
        lambda _values: _settings(),
    )

    async def collect(**kwargs):
        calls.append(kwargs)
        directory = kwargs["directory"]
        directory.mkdir(mode=0o700)
        (directory / "receipt.json").write_text("{}")
        return {
            "status": "bounded_complete",
            "stop_reason": "DURATION_LIMIT",
            "subscription_confirmed": True,
            "frame_count": 2,
        }

    def build_transport(_manifest, _settings, budget):
        budget.bind_http_controls()
        with budget.operation("Accounts"):
            budget.before_dispatch("auth")
            budget.after_response("auth", 200)
        return bundle

    result = await push_probe_cli._execute_once(
        tuple(_arguments(tmp_path)),
        build_transport=build_transport,
        collect=collect,
        load=lambda _directory: {"status": "bounded_complete"},
    )

    assert result == {
        "status": "bounded_complete",
        "code": "OK",
        "stop_reason": "DURATION_LIMIT",
        "subscription_confirmed": True,
        "frame_count": 2,
        "physical_request_count": 1,
    }
    assert bundle.session_calls == 1
    assert bundle.closed is True
    assert len(calls) == 1
    assert calls[0]["api_key"] == "synthetic-api-key"
    assert calls[0]["session"] == "synthetic-session"
    assert calls[0]["proxy_url"] == "http://127.0.0.1:9090"
    assert calls[0]["duration"] == 20.0
    assert calls[0]["max_frames"] == 20


@pytest.mark.asyncio
async def test_push_probe_failure_is_value_free_closes_once_and_never_collects(
    tmp_path, monkeypatch
):
    bundle = FakeBundle(fail_session=True)
    collected = False
    monkeypatch.setenv("FOSUN_PROVIDER_MANIFEST", str(MANIFEST_PATH))
    monkeypatch.setattr(
        push_probe_cli.PushProbeProviderSettings,
        "model_validate",
        lambda _values: _settings(),
    )

    async def collect(**_kwargs):
        nonlocal collected
        collected = True
        raise AssertionError

    def build_transport(_manifest, _settings, budget):
        budget.bind_http_controls()
        with budget.operation("Accounts"):
            budget.before_dispatch("auth")
            budget.after_response("auth", 200)
        return bundle

    result = await push_probe_cli._execute_once(
        tuple(_arguments(tmp_path)),
        build_transport=build_transport,
        collect=collect,
        load=lambda _directory: (_ for _ in ()).throw(AssertionError),
    )

    assert result == push_probe_cli.failure_result("PUSH_PROBE_FAILED", 1)
    assert "secret upstream failure" not in json.dumps(result)
    assert bundle.session_calls == 1
    assert bundle.closed is True
    assert collected is False


@pytest.mark.asyncio
async def test_archive_protocol_failure_is_not_mislabeled_as_settings(tmp_path, monkeypatch):
    bundle = FakeBundle()
    monkeypatch.setenv("FOSUN_PROVIDER_MANIFEST", str(MANIFEST_PATH))
    monkeypatch.setattr(
        push_probe_cli.PushProbeProviderSettings,
        "model_validate",
        lambda _values: _settings(),
    )

    def build_transport(_manifest, _settings, budget):
        budget.bind_http_controls()
        with budget.operation("Accounts"):
            budget.before_dispatch("auth")
            budget.after_response("auth", 200)
        return bundle

    async def collect(**kwargs):
        kwargs["directory"].mkdir(mode=0o700)
        return {
            "status": "partial",
            "stop_reason": "DISCONNECTED",
            "subscription_confirmed": True,
            "frame_count": 1,
        }

    result = await push_probe_cli._execute_once(
        tuple(_arguments(tmp_path)),
        build_transport=build_transport,
        collect=collect,
        load=lambda _directory: (_ for _ in ()).throw(
            PushStreamError("PUSH_ARCHIVE_INVALID")
        ),
    )

    assert result["code"] == "PUSH_PROBE_FAILED"
    assert result["physical_request_count"] == 1
    assert bundle.closed is True


def test_push_probe_cli_rejects_duplicate_or_secret_arguments_before_settings(
    tmp_path, monkeypatch
):
    settings_called = False

    def settings(_values):
        nonlocal settings_called
        settings_called = True
        raise AssertionError

    monkeypatch.setattr(push_probe_cli.PushProbeProviderSettings, "model_validate", settings)
    arguments = _arguments(tmp_path) + ["--api-key", "secret-canary"]

    exit_code = push_probe_cli.main(arguments)

    assert exit_code == 2
    assert settings_called is False


def test_push_probe_cli_without_settings_emits_one_value_free_frame(tmp_path, capfd):
    exit_code = push_probe_cli.main(_arguments(tmp_path))

    captured = capfd.readouterr()
    frame = json.loads(captured.out)
    assert exit_code == 2
    assert captured.out.count("\n") == 1
    assert captured.err == ""
    assert frame["status"] == "failed"
    assert set(frame) == {
        "status",
        "code",
        "stop_reason",
        "subscription_confirmed",
        "frame_count",
        "physical_request_count",
    }
