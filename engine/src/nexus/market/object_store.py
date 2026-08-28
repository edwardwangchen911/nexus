"""Create-only S3/MinIO storage for provider evidence and normalized market data."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import re
from collections.abc import Mapping
from typing import Protocol, cast
from urllib.parse import quote

import boto3  # type: ignore[import-untyped]
from botocore.config import Config  # type: ignore[import-untyped]
from botocore.exceptions import ClientError  # type: ignore[import-untyped]

from nexus.settings import EngineSettings

_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_TEMPORARY = re.compile(
    r"^temporary/market/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12}\.parquet$"
)
_MAX_OBJECT_BYTES = 64 * 1024 * 1024


class _S3Client(Protocol):
    def put_object(self, **kwargs: object) -> Mapping[str, object]: ...

    def get_object(self, **kwargs: object) -> Mapping[str, object]: ...

    def delete_object(self, **kwargs: object) -> object: ...


class _ReadableBody(Protocol):
    def read(self, amount: int = -1) -> bytes: ...

    def close(self) -> None: ...


class S3ProviderEvidenceStore:
    """One immutable object namespace implementing both provider store seams."""

    def __init__(
        self,
        settings: EngineSettings,
        *,
        client: _S3Client | None = None,
    ) -> None:
        endpoint = settings.object_store_endpoint
        region = settings.object_store_region
        bucket = settings.object_store_bucket
        access_key = settings.object_store_access_key_id
        secret_key = settings.object_store_secret_access_key
        if any(
            value is None
            for value in (endpoint, region, bucket, access_key, secret_key)
        ):
            raise ValueError("object-store settings are incomplete")
        assert endpoint is not None
        assert region is not None
        assert bucket is not None
        assert access_key is not None
        assert secret_key is not None
        self._bucket = bucket
        self._client = client or cast(
            _S3Client,
            boto3.client(
                "s3",
                endpoint_url=endpoint,
                region_name=region,
                aws_access_key_id=access_key.get_secret_value(),
                aws_secret_access_key=secret_key.get_secret_value(),
                config=Config(
                    connect_timeout=2,
                    read_timeout=5,
                    retries={"max_attempts": 1},
                    s3={"addressing_style": "path"},
                ),
            ),
        )

    async def put_if_absent(self, *, key: str, content: bytes) -> str:
        digest = _validate_content_key(key, content)
        return await self._put_create_only(key, content, digest)

    async def archive_raw(self, key: str, payload: bytes) -> str:
        if not key.startswith("raw/"):
            raise ValueError("provider raw object key is invalid")
        return await self.put_if_absent(key=key, content=payload)

    async def put_temporary(self, key: str, payload: bytes) -> None:
        if _TEMPORARY.fullmatch(key) is None or not payload:
            raise ValueError("temporary market object is invalid")
        await asyncio.to_thread(
            self._client.put_object,
            Bucket=self._bucket,
            Key=key,
            Body=payload,
            ContentType="application/vnd.apache.parquet",
            IfNoneMatch="*",
        )

    async def promote(self, temporary_key: str, final_key: str) -> str:
        if _TEMPORARY.fullmatch(temporary_key) is None or not final_key.startswith(
            "normalized/market/"
        ):
            raise ValueError("market promotion key is invalid")
        payload = await asyncio.to_thread(self._read_bounded, temporary_key)
        digest = _validate_content_key(final_key, payload)
        return await self._put_create_only(final_key, payload, digest)

    async def delete_temporary(self, key: str) -> None:
        if _TEMPORARY.fullmatch(key) is None:
            raise ValueError("temporary market object key is invalid")
        await asyncio.to_thread(
            self._client.delete_object,
            Bucket=self._bucket,
            Key=key,
        )

    async def _put_create_only(
        self,
        key: str,
        content: bytes,
        digest: str,
    ) -> str:
        try:
            response = await asyncio.to_thread(
                self._client.put_object,
                Bucket=self._bucket,
                Key=key,
                Body=content,
                IfNoneMatch="*",
                Metadata={"sha256": digest},
            )
            version = response.get("VersionId")
        except ClientError as error:
            if not _is_create_conflict(error):
                raise
            version = await asyncio.to_thread(
                self._verify_existing,
                key,
                content,
            )
        if not isinstance(version, str) or not version:
            raise RuntimeError("PROVIDER_OBJECT_VERSION_UNAVAILABLE")
        return f"s3://{self._bucket}/{key}?versionId={quote(version, safe='')}"

    def _verify_existing(self, key: str, expected: bytes) -> str:
        response = self._client.get_object(Bucket=self._bucket, Key=key)
        body = cast(_ReadableBody | None, response.get("Body"))
        if body is None or not callable(getattr(body, "read", None)):
            raise RuntimeError("PROVIDER_OBJECT_BODY_UNAVAILABLE")
        try:
            existing = body.read(len(expected) + 1)
        finally:
            if callable(getattr(body, "close", None)):
                body.close()
        if not hmac.compare_digest(existing, expected):
            raise RuntimeError("PROVIDER_OBJECT_CONTENT_MISMATCH")
        version = response.get("VersionId")
        if not isinstance(version, str) or not version:
            raise RuntimeError("PROVIDER_OBJECT_VERSION_UNAVAILABLE")
        return version

    def _read_bounded(self, key: str) -> bytes:
        response = self._client.get_object(Bucket=self._bucket, Key=key)
        body = cast(_ReadableBody | None, response.get("Body"))
        if body is None or not callable(getattr(body, "read", None)):
            raise RuntimeError("PROVIDER_OBJECT_BODY_UNAVAILABLE")
        try:
            payload = body.read(_MAX_OBJECT_BYTES + 1)
        finally:
            if callable(getattr(body, "close", None)):
                body.close()
        if not payload or len(payload) > _MAX_OBJECT_BYTES:
            raise RuntimeError("PROVIDER_OBJECT_SIZE_INVALID")
        return payload


def _validate_content_key(key: str, content: bytes) -> str:
    if (
        not content
        or len(content) > _MAX_OBJECT_BYTES
        or not key.isascii()
        or len(key) > 1_024
        or key.startswith("/")
        or "//" in key
        or any(segment in {"", ".", ".."} for segment in key.split("/"))
        or not key.startswith(("raw/", "evidence/", "normalized/market/"))
    ):
        raise ValueError("provider content-addressed object is invalid")
    digest = hashlib.sha256(content).hexdigest()
    filename = key.rsplit("/", 1)[-1]
    key_digest = filename.split(".", 1)[0]
    if _DIGEST.fullmatch(key_digest) is None or not hmac.compare_digest(
        digest,
        key_digest,
    ):
        raise ValueError("provider object key does not match content")
    return digest


def _is_create_conflict(error: ClientError) -> bool:
    status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    document = error.response.get("Error", {})
    code = document.get("Code") if isinstance(document, dict) else None
    return status in {409, 412} or code in {
        "ConditionalRequestConflict",
        "PreconditionFailed",
    }
