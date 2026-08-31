import json
from pathlib import Path

import pytest
from nexus_fosun_gateway.admission import admitted_sdk_subject
from nexus_fosun_gateway.probe_settings import ProbeProviderSettings
from pydantic import ValidationError

MANIFEST_PATH = Path(__file__).parents[4] / "contracts" / "providers" / "fosun" / "manifest.json"


def test_blocked_manifest_has_no_admitted_subject() -> None:
    assert admitted_sdk_subject(json.loads(MANIFEST_PATH.read_bytes())) is None


def test_admitted_subject_is_parsed_only_after_complete_validation() -> None:
    document = _admitted_document()

    subject = admitted_sdk_subject(document)

    assert subject is not None
    assert subject.provider_base_url == "https://provider.invalid"
    assert subject.sdk_tag == "v1.2.0"
    assert subject.sdk_commit == "891d95fb4967efaa682387c1b35966ceea38fb21"
    assert subject.evidence_references == (
        "synthetic/permission",
        "synthetic/test-vector",
        "synthetic/security-review",
        "synthetic/tls-review",
    )


def test_probe_settings_require_explicit_admission_and_hide_secrets(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    values = _settings_values(tmp_path)
    settings = ProbeProviderSettings.model_validate(values)

    rendered = repr(settings)
    assert settings.sdk_admitted is True
    assert settings.provider_manifest.is_absolute()
    assert settings.output_dir.is_absolute()
    assert "synthetic-api-key-canary" not in rendered
    assert "synthetic-private-key-canary" not in rendered
    assert "synthetic-server-key-canary" not in rendered
    assert "synthetic-publisher-private-key-canary" not in rendered

    values["FOSUN_SDK_ADMITTED"] = False
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
        "FSOPENAPI_BASE_URL": "https://provider.invalid",
        "FSOPENAPI_API_KEY": "synthetic-api-key-canary",
        "FSOPENAPI_CLIENT_PRIVATE_KEY": "synthetic-private-key-canary",
        "FSOPENAPI_SERVER_PUBLIC_KEY": "synthetic-server-key-canary",
    }


def _admitted_document() -> dict[str, object]:
    document = json.loads(MANIFEST_PATH.read_bytes())
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
