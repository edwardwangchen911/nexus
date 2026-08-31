import json
from pathlib import Path

import nexus_fosun_gateway.admission as admission
import pytest
from nexus_fosun_gateway.admission import admitted_sdk_subject
from nexus_fosun_gateway.probe_settings import ProbeProviderSettings
from pydantic import ValidationError

MANIFEST_PATH = Path(__file__).parents[4] / "contracts" / "providers" / "fosun" / "manifest.json"


def test_committed_manifest_is_ready_only_for_the_one_shot_probe() -> None:
    document = json.loads(MANIFEST_PATH.read_bytes())

    assert document["admission"]["status"] == "probe_ready"
    assert admitted_sdk_subject(document) is None
    subject = admission.probe_ready_sdk_subject(document)
    assert subject is not None
    assert subject.provider_base_url == "https://openapi.fosunxcz.com"


def test_admitted_subject_is_parsed_only_after_complete_validation() -> None:
    document = _admitted_document()

    subject = admitted_sdk_subject(document)

    assert subject is not None
    assert subject.provider_base_url == "https://openapi.fosunxcz.com"
    assert subject.sdk_tag == "v1.2.0"
    assert subject.sdk_commit == "891d95fb4967efaa682387c1b35966ceea38fb21"
    assert subject.evidence_references == (
        "synthetic/permission",
        "synthetic/test-vector",
        "synthetic/security-review",
        "synthetic/tls-review",
    )


def test_pull_only_admission_does_not_depend_on_deferred_push_contract() -> None:
    document = _admitted_document()
    document["openapi"]["push"] = {
        "url": "https://deferred-push.invalid/openapi.yaml",
        "sha256": "0" * 64,
    }
    document["admission"]["openapi_sha256"]["push"] = "0" * 64

    subject = admitted_sdk_subject(document)

    assert subject is not None
    assert subject.provider_base_url == "https://openapi.fosunxcz.com"


@pytest.mark.parametrize(
    "provider_base_url",
    [
        "https://provider.invalid",
        "https://openapi.fosunxcz.com:443",
        "https://openapi.fosunxcz.com/path",
        "https://OPENAPI.FOSUNXCZ.COM",
    ],
)
def test_admission_rejects_every_noncanonical_rest_origin(
    provider_base_url: str,
) -> None:
    document = _admitted_document()
    document["admission"]["provider_base_url"] = provider_base_url

    assert admitted_sdk_subject(document) is None


def test_admission_rejects_an_arbitrary_private_use_label() -> None:
    document = _admitted_document()
    document["sdk"]["license"] = "SYNTHETIC-NONEMPTY-LABEL"

    assert admitted_sdk_subject(document) is None
    assert admission.probe_ready_sdk_subject(document) is None


def test_probe_settings_require_explicit_admission_and_hide_secrets(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    values = _settings_values(tmp_path)
    settings = ProbeProviderSettings.model_validate(values)

    rendered = repr(settings)
    assert settings.sdk_admitted is True
    assert settings.probe_https_proxy_url == "http://127.0.0.1:9090"
    assert settings.provider_manifest.is_absolute()
    assert settings.output_dir.is_absolute()
    assert "synthetic-api-key-canary" not in rendered
    assert "synthetic-private-key-canary" not in rendered
    assert "synthetic-server-key-canary" not in rendered
    assert "synthetic-publisher-private-key-canary" not in rendered

    values["FOSUN_SDK_ADMITTED"] = False
    with pytest.raises(ValidationError):
        ProbeProviderSettings.model_validate(values)


@pytest.mark.parametrize(
    "value",
    [
        "",
        "http://127.0.0.1:9091",
        "http://localhost:9090",
        "https://127.0.0.1:9090",
        "http://104.153.109.103:9090",
        "http://user:secret@127.0.0.1:9090",
    ],
)
def test_probe_settings_accept_only_the_whitelisted_local_https_proxy_path(
    tmp_path: Path,
    value: str,
) -> None:
    values = _settings_values(tmp_path)
    values["FOSUN_PROBE_HTTPS_PROXY_URL"] = value

    with pytest.raises(ValidationError):
        ProbeProviderSettings.model_validate(values)


@pytest.mark.parametrize("value", [False, 1, "1", "TRUE", "True"])
def test_probe_settings_reject_nonexact_admission_switch(
    tmp_path: Path,
    value: object,
) -> None:
    values = _settings_values(tmp_path)
    values["FOSUN_SDK_ADMITTED"] = value

    with pytest.raises(ValidationError):
        ProbeProviderSettings.model_validate(values)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("FOSUN_PROVIDER_MANIFEST", "relative/manifest.json"),
        ("FOSUN_PROBE_OUTPUT_DIR", "relative/output"),
        ("FOSUN_PROBE_PUBLISHER_PRIVATE_KEY_FILE", "relative/publisher.key"),
        ("FSOPENAPI_BASE_URL", "http://provider.invalid"),
        ("FSOPENAPI_BASE_URL", "https://user:secret@provider.invalid"),
        ("FSOPENAPI_BASE_URL", "https://provider.invalid/path?query=1"),
        ("FSOPENAPI_BASE_URL", "https://provider.invalid/"),
        ("FSOPENAPI_BASE_URL", "https://provider.invalid"),
        ("FSOPENAPI_BASE_URL", "https://openapi.fosunxcz.com:443"),
        ("FSOPENAPI_BASE_URL", "https://OPENAPI.FOSUNXCZ.COM"),
    ],
)
def test_probe_settings_reject_ambiguous_paths_and_provider_urls(
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    values = _settings_values(tmp_path)
    values[field] = value

    with pytest.raises(ValidationError):
        ProbeProviderSettings.model_validate(values)


@pytest.mark.parametrize(
    "field",
    [
        "FSOPENAPI_API_KEY",
        "FSOPENAPI_CLIENT_PRIVATE_KEY",
        "FSOPENAPI_SERVER_PUBLIC_KEY",
    ],
)
def test_probe_settings_reject_blank_secret_material(tmp_path: Path, field: str) -> None:
    values = _settings_values(tmp_path)
    values[field] = "   "

    with pytest.raises(ValidationError) as exc_info:
        ProbeProviderSettings.model_validate(values)

    assert "synthetic-" not in str(exc_info.value)


@pytest.mark.parametrize(
    "publisher_key",
    ["", "A" * 42, "A" * 44, "A" * 42 + "B", "A" * 42 + "!"],
)
def test_probe_settings_require_canonical_publisher_public_key(
    tmp_path: Path,
    publisher_key: str,
) -> None:
    values = _settings_values(tmp_path)
    values["FOSUN_ACTIVATION_PROBE_PUBLISHER_PUBLIC_KEY_B64U"] = publisher_key

    with pytest.raises(ValidationError):
        ProbeProviderSettings.model_validate(values)


def _settings_values(tmp_path: Path) -> dict[str, object]:
    return {
        "FOSUN_PROVIDER_MANIFEST": tmp_path / "manifest.json",
        "FOSUN_SDK_ADMITTED": True,
        "FOSUN_PROBE_OUTPUT_DIR": tmp_path,
        "FOSUN_PROBE_PUBLISHER_PRIVATE_KEY_FILE": (
            tmp_path / "synthetic-publisher-private-key-canary"
        ),
        "FOSUN_ACTIVATION_PROBE_PUBLISHER_PUBLIC_KEY_B64U": "A" * 43,
        "FOSUN_PROBE_HTTPS_PROXY_URL": "http://127.0.0.1:9090",
        "FSOPENAPI_BASE_URL": "https://openapi.fosunxcz.com",
        "FSOPENAPI_API_KEY": "synthetic-api-key-canary",
        "FSOPENAPI_CLIENT_PRIVATE_KEY": "synthetic-private-key-canary",
        "FSOPENAPI_SERVER_PUBLIC_KEY": "synthetic-server-key-canary",
    }


def _admitted_document() -> dict[str, object]:
    document = json.loads(MANIFEST_PATH.read_bytes())
    document["sdk"]["license"] = "PROVIDER-AUTHORIZED-PRIVATE-USE"
    document["admission"]["status"] = "admitted"
    document["admission"]["provider_base_url"] = "https://openapi.fosunxcz.com"
    document["admission"]["evidence"] = {
        "written_permission": "synthetic/permission",
        "provider_test_vector": "synthetic/test-vector",
        "security_review": "synthetic/security-review",
        "tls_verification_remediation": "synthetic/tls-review",
    }
    return document
