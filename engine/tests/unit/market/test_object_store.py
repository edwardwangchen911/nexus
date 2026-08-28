"""Create-only S3/MinIO provider evidence storage contracts."""

from __future__ import annotations

from hashlib import sha256
from io import BytesIO
from typing import cast

import pytest
from botocore.exceptions import ClientError
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.settings import EngineSettings
from pydantic import SecretStr


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.versions: dict[str, str] = {}
        self.put_calls: list[dict[str, object]] = []

    def put_object(self, **kwargs: object) -> dict[str, str]:
        self.put_calls.append(kwargs)
        key = cast(str, kwargs["Key"])
        body = cast(bytes, kwargs["Body"])
        if kwargs.get("IfNoneMatch") == "*" and key in self.objects:
            raise ClientError(
                {
                    "Error": {"Code": "PreconditionFailed"},
                    "ResponseMetadata": {"HTTPStatusCode": 412},
                },
                "PutObject",
            )
        self.objects[key] = body
        version = f"version-{len(self.versions) + 1}"
        self.versions[key] = version
        return {"VersionId": version}

    def get_object(self, **kwargs: object) -> dict[str, object]:
        key = cast(str, kwargs["Key"])
        return {
            "Body": BytesIO(self.objects[key]),
            "VersionId": self.versions[key],
        }

    def delete_object(self, **kwargs: object) -> dict[str, object]:
        key = cast(str, kwargs["Key"])
        self.objects.pop(key, None)
        return {}


def _settings() -> EngineSettings:
    return EngineSettings.for_test().model_copy(
        update={
            "object_store_endpoint": "http://minio.invalid:9000",
            "object_store_region": "us-east-1",
            "object_store_bucket": "nexus-fixtures",
            "object_store_access_key_id": SecretStr("fixture-access"),
            "object_store_secret_access_key": SecretStr("fixture-secret"),
        }
    )


@pytest.mark.asyncio
async def test_put_if_absent_returns_versioned_content_addressed_uri() -> None:
    client = FakeS3Client()
    store = S3ProviderEvidenceStore(_settings(), client=client)
    payload = b'{"bounded":true}'
    digest = sha256(payload).hexdigest()
    key = f"raw/fred/2026/08/28/DFF/{digest}.json"

    first = await store.put_if_absent(key=key, content=payload)
    replay = await store.put_if_absent(key=key, content=payload)

    assert first == replay == f"s3://nexus-fixtures/{key}?versionId=version-1"
    assert client.objects[key] == payload
    assert all(call.get("IfNoneMatch") == "*" for call in client.put_calls)


@pytest.mark.asyncio
async def test_existing_digest_with_different_bytes_is_rejected() -> None:
    client = FakeS3Client()
    store = S3ProviderEvidenceStore(_settings(), client=client)
    payload = b"expected"
    digest = sha256(payload).hexdigest()
    key = f"evidence/sec/accession/{digest}.json"
    client.objects[key] = b"tampered"
    client.versions[key] = "existing-version"

    with pytest.raises(RuntimeError, match="PROVIDER_OBJECT_CONTENT_MISMATCH"):
        await store.put_if_absent(key=key, content=payload)


@pytest.mark.asyncio
async def test_market_promotion_is_create_only_and_deletes_only_temporary_key() -> None:
    client = FakeS3Client()
    store = S3ProviderEvidenceStore(_settings(), client=client)
    payload = b"PAR1-fixture"
    digest = sha256(payload).hexdigest()
    temporary = "temporary/market/0198d500-1000-7000-8000-000000002999.parquet"
    final = f"normalized/market/2026/08/28/instrument/{digest}.parquet"

    await store.put_temporary(temporary, payload)
    uri = await store.promote(temporary, final)
    await store.delete_temporary(temporary)

    assert uri == f"s3://nexus-fixtures/{final}?versionId=version-2"
    assert client.objects[final] == payload
    assert temporary not in client.objects
