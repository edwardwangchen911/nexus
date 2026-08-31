import json
import logging
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests
from nexus_fosun_gateway.adapter.probe_sdk import (
    PhysicalHttpBudget,
    ProbeRequestPlan,
    ProbeSdkError,
    SdkProbeExecutor,
    build_probe_transport,
)
from nexus_fosun_gateway.probe_settings import ProbeProviderSettings

_CANARIES = (
    "synthetic-api-secret-canary",
    "synthetic-private-key-canary",
    "synthetic-server-key-canary",
    "synthetic-provider-body-canary",
    "synthetic-account-canary",
)
_MANIFEST_PATH = (
    Path(__file__).parents[4]
    / "contracts"
    / "providers"
    / "fosun"
    / "manifest.json"
)


def test_factory_and_settings_never_render_secret_canaries(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    class FailingSdkClient:
        def __init__(self, **_kwargs: object) -> None:
            raise RuntimeError("synthetic-provider-body-canary")

    monkeypatch.setattr(
        "nexus_fosun_gateway.adapter.probe_sdk.importlib.import_module",
        lambda name: (
            SimpleNamespace(SDKClient=FailingSdkClient)
            if name == "fsopenapi"
            else SimpleNamespace()
        ),
    )
    settings = _settings(tmp_path)
    with caplog.at_level(logging.DEBUG), pytest.raises(ProbeSdkError) as exc_info:
        build_probe_transport(_admitted_document(), settings, PhysicalHttpBudget())

    rendered = "\n".join(
        (repr(settings), repr(exc_info.value), caplog.text, capsys.readouterr().out)
    )
    for canary in _CANARIES:
        assert canary not in rendered


@pytest.mark.asyncio
async def test_executor_sanitizes_provider_and_close_exceptions(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    budget = PhysicalHttpBudget()
    auth_module = SimpleNamespace(requests=_FailingRequests())

    def module_import(name: str) -> object:
        if name == "fsopenapi":
            return SimpleNamespace(SDKClient=_FailingSdkClient)
        if name == "fsopenapi.auth":
            return auth_module
        raise AssertionError(name)

    monkeypatch.setattr(
        "nexus_fosun_gateway.adapter.probe_sdk.importlib.import_module",
        module_import,
    )
    bundle = build_probe_transport(_admitted_document(), _settings(tmp_path), budget)
    with caplog.at_level(logging.DEBUG), pytest.raises(ProbeSdkError) as exc_info:
        await SdkProbeExecutor(
            bundle,
            budget,
        ).run(
            ProbeRequestPlan(
                symbol="usAAPL",
                session_date=date(2026, 8, 21),
                cash_flow_date_from=date(2026, 8, 20),
                cash_flow_date_to=date(2026, 8, 21),
                limit=1,
            )
        )

    captured = capsys.readouterr()
    rendered = "\n".join(
        (repr(exc_info.value), caplog.text, captured.out, captured.err)
    )
    assert exc_info.value.code == "PROBE_FAILED"
    for canary in _CANARIES:
        assert canary not in rendered


class _FailingSdkClient:
    def __init__(self, **_kwargs: object) -> None:
        self._http_session = _FailingSession()
        self.account = SimpleNamespace(list_accounts=self._list_accounts)

    def _list_accounts(self) -> object:
        raise PermissionError("synthetic-provider-body-canary")


class _FailingSession:
    trust_env = True

    def __init__(self, *, fail_close: bool = False) -> None:
        self._fail_close = fail_close
        self._adapters = {"https://": requests.adapters.HTTPAdapter(max_retries=2)}

    def mount(self, prefix: str, adapter: object) -> None:
        self._adapters[prefix] = adapter

    def get_adapter(self, prefix: str) -> object:
        return self._adapters[prefix]

    def request(self, *_args: object, **_kwargs: object) -> object:
        raise AssertionError("unexpected request")

    def post(self, *_args: object, **_kwargs: object) -> object:
        raise AssertionError("unexpected auth request")

    def close(self) -> None:
        if self._fail_close:
            raise RuntimeError("synthetic-server-key-canary")


class _FailingRequests:
    adapters = requests.adapters

    def Session(self) -> _FailingSession:  # noqa: N802 - mirrors requests API
        return _FailingSession(fail_close=True)


def _settings(tmp_path: Path) -> ProbeProviderSettings:
    return ProbeProviderSettings.model_validate(
        {
            "FOSUN_PROVIDER_MANIFEST": tmp_path / "manifest.json",
            "FOSUN_SDK_ADMITTED": True,
            "FOSUN_PROBE_OUTPUT_DIR": tmp_path,
            "FOSUN_PROBE_PUBLISHER_PRIVATE_KEY_FILE": (
                tmp_path / "synthetic-private-key-canary"
            ),
            "FOSUN_ACTIVATION_PROBE_PUBLISHER_PUBLIC_KEY_B64U": "A" * 43,
            "FSOPENAPI_BASE_URL": "https://provider.invalid",
            "FSOPENAPI_API_KEY": "synthetic-api-secret-canary",
            "FSOPENAPI_CLIENT_PRIVATE_KEY": "synthetic-private-key-canary",
            "FSOPENAPI_SERVER_PUBLIC_KEY": "synthetic-server-key-canary",
        }
    )


def _admitted_document() -> dict[str, object]:
    document = json.loads(_MANIFEST_PATH.read_bytes())
    document["sdk"]["license"] = "WRITTEN-PERMISSION"
    document["admission"]["status"] = "admitted"
    document["admission"]["provider_base_url"] = "https://provider.invalid"
    document["admission"]["evidence"] = {
        "written_permission": "synthetic/permission",
        "provider_test_vector": "synthetic/test-vector",
        "security_review": "synthetic/security-review",
        "tls_verification_remediation": "synthetic/tls-review",
    }
    return document
