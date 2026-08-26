"""S3-compatible content-addressed audit manifest storage contracts."""

from __future__ import annotations

import hashlib

import pytest
from botocore.exceptions import ClientError
from nexus.audit.manifest import S3AuditManifestObjectStore
from nexus.settings import EngineSettings


class _Body:
    def __init__(self, content: bytes) -> None:
        self._content = content

    def read(self, amount: int) -> bytes:
        return self._content[:amount]

    def close(self) -> None:
        return None


class _S3Client:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.put_calls: list[dict[str, object]] = []

    def put_object(self, **kwargs: object) -> dict[str, object]:
        self.put_calls.append(kwargs)
        key = str(kwargs["Key"])
        content = bytes(kwargs["Body"])  # type: ignore[arg-type]
        if key in self.objects:
            raise ClientError(
                {
                    "Error": {"Code": "PreconditionFailed", "Message": "exists"},
                    "ResponseMetadata": {"HTTPStatusCode": 412},
                },
                "PutObject",
            )
        self.objects[key] = content
        return {"ETag": "fixture"}

    def get_object(self, **kwargs: object) -> dict[str, object]:
        return {"Body": _Body(self.objects[str(kwargs["Key"])])}


def _key(content: bytes) -> str:
    digest = hashlib.sha256(content).hexdigest()
    return f"audit/manifests/2026-08-27/{digest}.ndjson.gz"


@pytest.mark.asyncio
async def test_s3_manifest_store_is_create_only_and_replay_safe() -> None:
    client = _S3Client()
    store = S3AuditManifestObjectStore(EngineSettings.for_test(), client=client)
    content = b"\x1f\x8bfixture-audit"
    key = _key(content)

    first = await store.put_if_absent(key=key, content=content)
    replay = await store.put_if_absent(key=key, content=content)

    assert first == replay == f"s3://nexus-fixtures/{key}"
    assert client.put_calls[0]["IfNoneMatch"] == "*"
    assert client.put_calls[0]["ContentEncoding"] == "gzip"
    assert client.put_calls[0]["ContentType"] == "application/x-ndjson"
    assert client.put_calls[0]["Metadata"] == {"sha256": hashlib.sha256(content).hexdigest()}


@pytest.mark.asyncio
async def test_s3_manifest_store_rejects_wrong_keys_and_existing_content() -> None:
    client = _S3Client()
    store = S3AuditManifestObjectStore(EngineSettings.for_test(), client=client)
    content = b"\x1f\x8bfixture-audit"
    key = _key(content)
    client.objects[key] = b"different"

    with pytest.raises(RuntimeError, match="content mismatch"):
        await store.put_if_absent(key=key, content=content)
    with pytest.raises(ValueError, match="key"):
        await store.put_if_absent(key="../unsafe", content=content)

