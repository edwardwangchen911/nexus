"""Create-only S3/MinIO provider evidence storage contracts."""

from __future__ import annotations

from hashlib import sha256
from io import BytesIO
from typing import cast

import pytest
from botocore.exceptions import ClientError
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.providers.settings import ProviderWorkerSettings


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


def _settings() -> ProviderWorkerSettings:
    return ProviderWorkerSettings.model_validate(
        {
            "PROVIDER_DATABASE_URL": "postgresql+asyncpg://provider.invalid/nexus",
            "PROVIDER_OBJECT_STORE_ENDPOINT": "http://minio.invalid:9000",
            "PROVIDER_OBJECT_STORE_REGION": "us-east-1",
            "PROVIDER_OBJECT_STORE_BUCKET": "nexus-fixtures",
            "PROVIDER_OBJECT_STORE_ACCESS_KEY_ID": "fixture-access",
            "PROVIDER_OBJECT_STORE_SECRET_ACCESS_KEY": "fixture-secret",
            "PROVIDER_OBJECT_STORE_PREFIX": "provider/market",
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

    storage_key = f"provider/market/{key}"
    assert first == replay == f"s3://nexus-fixtures/{storage_key}?versionId=version-1"
    assert client.objects[storage_key] == payload
    assert all(call.get("IfNoneMatch") == "*" for call in client.put_calls)


@pytest.mark.asyncio
async def test_existing_digest_with_different_bytes_is_rejected() -> None:
    client = FakeS3Client()
    store = S3ProviderEvidenceStore(_settings(), client=client)
    payload = b"expected"
    digest = sha256(payload).hexdigest()
    key = f"evidence/sec/accession/{digest}.json"
    storage_key = f"provider/market/{key}"
    client.objects[storage_key] = b"tampered"
    client.versions[storage_key] = "existing-version"

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

    storage_temporary = f"provider/market/{temporary}"
    storage_final = f"provider/market/{final}"
    assert uri == f"s3://nexus-fixtures/{storage_final}?versionId=version-2"
    assert client.objects[storage_final] == payload
    assert storage_temporary not in client.objects
