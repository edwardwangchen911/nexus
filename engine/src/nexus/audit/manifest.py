"""Content-addressed, redacted snapshots of contiguous audit-chain segments."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import re
from collections.abc import Mapping
from datetime import date
from typing import Protocol, cast

import boto3  # type: ignore[import-untyped]
from botocore.config import Config  # type: ignore[import-untyped]
from botocore.exceptions import ClientError  # type: ignore[import-untyped]
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.export import render_redacted_audit_gzip
from nexus.domain.ids import new_uuid7
from nexus.persistence.models.runtime import AuditChainManifest, AuditEvent
from nexus.settings import EngineSettings

_MANIFEST_KEY = re.compile(
    r"^audit/manifests/(\d{4}-\d{2}-\d{2})/([0-9a-f]{64})\.ndjson\.gz$"
)


class NoAuditEventsError(RuntimeError):
    """No new chain segment exists for the requested daily manifest."""


class AuditManifestObjectStore(Protocol):
    """Minimal idempotent content-addressed object-store boundary."""

    async def put_if_absent(self, *, key: str, content: bytes) -> str: ...


class _AuditManifestS3Client(Protocol):
    def put_object(self, **kwargs: object) -> object: ...

    def get_object(self, **kwargs: object) -> Mapping[str, object]: ...


class _ReadableBody(Protocol):
    def read(self, amount: int) -> bytes: ...

    def close(self) -> None: ...


class S3AuditManifestObjectStore:
    """Create-only S3/MinIO adapter for content-addressed audit manifests."""

    def __init__(
        self,
        settings: EngineSettings,
        *,
        client: _AuditManifestS3Client | None = None,
        disable_environment_proxies: bool = False,
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
        self._client = client or boto3.client(
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
                # Local-only callers must not send signed requests through an env proxy.
                proxies={} if disable_environment_proxies else None,
            ),
        )

    async def put_if_absent(self, *, key: str, content: bytes) -> str:
        match = _MANIFEST_KEY.fullmatch(key)
        if match is None:
            raise ValueError("audit manifest key is invalid")
        try:
            date.fromisoformat(match.group(1))
        except ValueError:
            raise ValueError("audit manifest key is invalid") from None
        digest = hashlib.sha256(content).hexdigest()
        if (
            not content
            or content[:2] != b"\x1f\x8b"
            or not hmac.compare_digest(digest, match.group(2))
        ):
            raise ValueError("audit manifest key does not match its gzip content")
        try:
            await asyncio.to_thread(
                self._client.put_object,
                Bucket=self._bucket,
                Key=key,
                Body=content,
                ContentEncoding="gzip",
                ContentType="application/x-ndjson",
                IfNoneMatch="*",
                Metadata={"sha256": digest},
            )
        except ClientError as error:
            error_document = error.response.get("Error", {})
            status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            code = error_document.get("Code") if isinstance(error_document, dict) else None
            if status not in {409, 412} and code not in {
                "ConditionalRequestConflict",
                "PreconditionFailed",
            }:
                raise
            await asyncio.to_thread(self._verify_existing, key, content)
        return f"s3://{self._bucket}/{key}"

    def _verify_existing(self, key: str, expected: bytes) -> None:
        response = self._client.get_object(Bucket=self._bucket, Key=key)
        body = cast(_ReadableBody | None, response.get("Body"))
        if body is None or not callable(getattr(body, "read", None)):
            raise RuntimeError("audit manifest object body is unavailable")
        try:
            existing = body.read(len(expected) + 1)
        finally:
            if callable(getattr(body, "close", None)):
                body.close()
        if not hmac.compare_digest(existing, expected):
            raise RuntimeError("audit manifest object content mismatch")


async def publish_audit_chain_manifest(
    session: AsyncSession,
    *,
    session_date: date,
    object_store: AuditManifestObjectStore,
) -> AuditChainManifest:
    """Publish exactly the next contiguous redacted chain segment once."""
    if type(session_date) is not date:
        raise ValueError("manifest session must be a date")
    await session.execute(
        text(
            "SELECT pg_advisory_xact_lock("
            "hashtextextended('nexus:audit-chain', 2020))"
        )
    )
    existing = await session.scalar(
        select(AuditChainManifest).where(
            AuditChainManifest.session == session_date
        )
    )
    if existing is not None:
        return existing
    previous = await session.scalar(
        select(AuditChainManifest)
        .order_by(AuditChainManifest.last_event_seq.desc())
        .limit(1)
    )
    if previous is not None and session_date <= previous.session:
        raise ValueError("manifest sessions must increase monotonically")
    first_event_seq = 1 if previous is None else previous.last_event_seq + 1
    head = await session.scalar(
        select(AuditEvent).order_by(AuditEvent.event_seq.desc()).limit(1)
    )
    if head is None or head.event_seq < first_event_seq:
        raise NoAuditEventsError("no new audit events are available")
    events = tuple(
        (
            await session.scalars(
                select(AuditEvent)
                .where(
                    AuditEvent.event_seq >= first_event_seq,
                    AuditEvent.event_seq <= head.event_seq,
                )
                .order_by(AuditEvent.event_seq)
            )
        ).all()
    )
    expected_sequences = tuple(range(first_event_seq, head.event_seq + 1))
    if tuple(event.event_seq for event in events) != expected_sequences:
        raise RuntimeError("audit manifest chain segment is not contiguous")
    content = render_redacted_audit_gzip(events)
    object_sha256 = hashlib.sha256(content).hexdigest()
    key = (
        f"audit/manifests/{session_date.isoformat()}/"
        f"{object_sha256}.ndjson.gz"
    )
    object_uri = await object_store.put_if_absent(key=key, content=content)
    if (
        not 1 <= len(object_uri) <= 2_048
        or not object_uri.endswith(key)
        or "\x00" in object_uri
    ):
        raise ValueError("audit manifest object URI is not content addressed")
    manifest = AuditChainManifest(
        id=new_uuid7(),
        session=session_date,
        first_event_seq=first_event_seq,
        last_event_seq=head.event_seq,
        head_hash=head.event_hash,
        object_uri=object_uri,
        object_sha256=object_sha256,
        byte_size=len(content),
    )
    session.add(manifest)
    await session.flush()
    return manifest
