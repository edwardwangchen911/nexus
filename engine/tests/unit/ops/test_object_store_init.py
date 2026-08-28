"""Fail-closed object-store bucket initialization contracts."""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest
from botocore.exceptions import ClientError
from nexus.object_store_init import ObjectStoreInitSettings, ensure_bucket, main
from pydantic import ValidationError


def _missing_bucket() -> ClientError:
    return ClientError(
        {
            "Error": {"Code": "NoSuchBucket", "Message": "fixture only"},
            "ResponseMetadata": {"HTTPStatusCode": 404},
        },
        "HeadBucket",
    )


@dataclass
class _S3Client:
    exists: bool
    versioning_enabled: bool = False
    head_calls: list[dict[str, object]] = field(default_factory=list)
    create_calls: list[dict[str, object]] = field(default_factory=list)
    put_versioning_calls: list[dict[str, object]] = field(default_factory=list)

    def head_bucket(self, **kwargs: object) -> object:
        self.head_calls.append(kwargs)
        if not self.exists:
            raise _missing_bucket()
        return {}

    def create_bucket(self, **kwargs: object) -> object:
        self.create_calls.append(kwargs)
        self.exists = True
        return {}

    def get_bucket_versioning(self, **kwargs: object) -> object:
        return {"Status": "Enabled"} if self.versioning_enabled else {}

    def put_bucket_versioning(self, **kwargs: object) -> object:
        self.put_versioning_calls.append(kwargs)
        self.versioning_enabled = True
        return {}


def test_development_creates_exact_missing_bucket_and_verifies_it() -> None:
    client = _S3Client(exists=False)

    result = ensure_bucket(_settings("development"), client=client)

    assert result == "created"
    assert client.create_calls == [{"Bucket": "nexus-fixtures"}]
    assert client.head_calls == [
        {"Bucket": "nexus-fixtures"},
        {"Bucket": "nexus-fixtures"},
    ]
    assert client.put_versioning_calls == [
        {
            "Bucket": "nexus-fixtures",
            "VersioningConfiguration": {"Status": "Enabled"},
        }
    ]


def test_existing_bucket_replay_is_idempotent() -> None:
    client = _S3Client(exists=True, versioning_enabled=True)

    first = ensure_bucket(_settings("development"), client=client)
    replay = ensure_bucket(_settings("development"), client=client)

    assert first == replay == "existing"
    assert client.create_calls == []
    assert client.head_calls == [
        {"Bucket": "nexus-fixtures"},
        {"Bucket": "nexus-fixtures"},
    ]
    assert client.put_versioning_calls == []


def test_development_enables_versioning_on_an_existing_local_bucket() -> None:
    client = _S3Client(exists=True)

    assert ensure_bucket(_settings("development"), client=client) == "existing"

    assert client.versioning_enabled
    assert len(client.put_versioning_calls) == 1


def test_production_requires_preprovisioned_bucket_versioning() -> None:
    client = _S3Client(exists=True)

    with pytest.raises(RuntimeError, match="production object-store versioning is disabled"):
        ensure_bucket(_settings("production"), client=client)

    assert client.put_versioning_calls == []


def test_production_missing_bucket_fails_without_creating() -> None:
    client = _S3Client(exists=False)

    with pytest.raises(RuntimeError, match="production object-store bucket is missing"):
        ensure_bucket(_settings("production"), client=client)

    assert client.create_calls == []
    assert client.head_calls == [{"Bucket": "nexus-fixtures"}]


def test_non_missing_head_error_is_not_misclassified() -> None:
    class _DeniedClient(_S3Client):
        def head_bucket(self, **kwargs: object) -> object:
            raise ClientError(
                {
                    "Error": {"Code": "AccessDenied", "Message": "sensitive endpoint detail"},
                    "ResponseMetadata": {"HTTPStatusCode": 403},
                },
                "HeadBucket",
            )

    with pytest.raises(ClientError):
        ensure_bucket(_settings("development"), client=_DeniedClient(exists=False))


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("OBJECT_STORE_ENDPOINT", "   "),
        ("OBJECT_STORE_ENDPOINT", "minio:9000"),
        ("OBJECT_STORE_REGION", "   "),
        ("OBJECT_STORE_ACCESS_KEY_ID", "   "),
        ("OBJECT_STORE_SECRET_ACCESS_KEY", "   "),
    ),
)
def test_settings_reject_empty_or_ambiguous_required_values(
    field: str,
    value: str,
) -> None:
    values = _setting_values("development")
    values[field] = value

    with pytest.raises(ValidationError):
        ObjectStoreInitSettings(**values)


def test_cli_failure_output_never_includes_provider_error_detail(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    values = _setting_values("development")
    for name, value in values.items():
        monkeypatch.setenv(name, value)

    def _raise_sensitive_error(_: ObjectStoreInitSettings) -> object:
        raise RuntimeError("fixture-secret-sensitive-detail")

    monkeypatch.setattr("nexus.object_store_init.ensure_bucket", _raise_sensitive_error)

    assert main() == 1
    output = capsys.readouterr()
    assert output.out == ""
    assert output.err == "object-store initialization failed\n"
    assert "fixture-secret-sensitive-detail" not in output.err


def _settings(environment: str) -> ObjectStoreInitSettings:
    return ObjectStoreInitSettings(**_setting_values(environment))


def _setting_values(environment: str) -> dict[str, str]:
    return {
        "NEXUS_ENV": environment,
        "OBJECT_STORE_ENDPOINT": "http://127.0.0.1:9000",
        "OBJECT_STORE_REGION": "us-east-1",
        "OBJECT_STORE_BUCKET": "nexus-fixtures",
        "OBJECT_STORE_ACCESS_KEY_ID": "fixture-access",
        "OBJECT_STORE_SECRET_ACCESS_KEY": "fixture-secret",
    }
