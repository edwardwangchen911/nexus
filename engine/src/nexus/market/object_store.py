"""Prefix-scoped S3/MinIO storage for provider evidence and market data."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import re
import threading
from collections.abc import Callable, Mapping
from typing import Protocol, cast
from urllib.parse import quote, unquote, urlsplit

import boto3  # type: ignore[import-untyped]
from botocore.config import Config  # type: ignore[import-untyped]
from botocore.exceptions import ClientError  # type: ignore[import-untyped]
from pydantic import SecretStr


class ObjectStoreSettings(Protocol):
    @property
    def object_store_endpoint(self) -> str | None: ...

    @property
    def object_store_region(self) -> str | None: ...

    @property
    def object_store_bucket(self) -> str | None: ...

    @property
    def object_store_access_key_id(self) -> SecretStr | None: ...

    @property
    def object_store_secret_access_key(self) -> SecretStr | None: ...

    @property
    def object_store_prefix(self) -> str | None: ...


_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_SAFE_OBJECT_KEY = re.compile(r"^[A-Za-z0-9._/-]+$")
_MAX_OBJECT_BYTES = 64 * 1024 * 1024
_OBJECT_IO_WORKERS = 32
_OBJECT_IO_TIMEOUT_SECONDS = 4.0
_OBJECT_IO_GATE = threading.BoundedSemaphore(_OBJECT_IO_WORKERS)


class _DropObjectStoreDependencyRecord(logging.Filter):
    """Block SDK records that may format payloads or signed credentials."""

    def filter(self, record: logging.LogRecord) -> bool:
        del record
        return False


_OBJECT_STORE_DEPENDENCY_NAMESPACES = (
    "boto3",
    "botocore",
    "s3transfer",
    "urllib3",
)
_OBJECT_STORE_DEPENDENCY_FILTER = _DropObjectStoreDependencyRecord()
_OBJECT_STORE_DEPENDENCY_LOG_LOCK = threading.RLock()


class ProviderObjectIOUnavailable(RuntimeError):
    """A bounded object request could not safely start or finish in time."""

    def __init__(self, error_code: str) -> None:
        if error_code not in {
            "OBJECT_IO_SATURATED",
            "OBJECT_IO_DEADLINE_EXCEEDED",
        }:
            raise ValueError("unsupported provider object I/O error code")
        super().__init__(error_code)
        self.error_code = error_code


class _S3Client(Protocol):
    def put_object(self, **kwargs: object) -> Mapping[str, object]: ...

    def get_object(self, **kwargs: object) -> Mapping[str, object]: ...


class _ReadableBody(Protocol):
    def read(self, amount: int = -1) -> bytes: ...

    def close(self) -> None: ...


class S3ProviderEvidenceStore:
    """One immutable object namespace implementing both provider store seams."""

    def __init__(
        self,
        settings: ObjectStoreSettings,
        *,
        client: _S3Client | None = None,
    ) -> None:
        endpoint = settings.object_store_endpoint
        region = settings.object_store_region
        bucket = settings.object_store_bucket
        access_key = settings.object_store_access_key_id
        secret_key = settings.object_store_secret_access_key
        prefix = settings.object_store_prefix
        if any(
            value is None for value in (endpoint, region, bucket, access_key, secret_key, prefix)
        ):
            raise ValueError("object-store settings are incomplete")
        assert endpoint is not None
        assert region is not None
        assert bucket is not None
        assert access_key is not None
        assert secret_key is not None
        assert prefix is not None
        self._bucket = bucket
        self._prefix = prefix
        _enforce_object_store_dependency_log_boundary()
        self._client = client or cast(
            _S3Client,
            boto3.client(
                "s3",
                endpoint_url=endpoint,
                region_name=region,
                aws_access_key_id=access_key.get_secret_value(),
                aws_secret_access_key=secret_key.get_secret_value(),
                config=Config(
                    connect_timeout=1,
                    read_timeout=3,
                    retries={"total_max_attempts": 1},
                    max_pool_connections=_OBJECT_IO_WORKERS,
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

    async def read_raw(self, key: str, *, uri: str) -> bytes:
        if not key.startswith("raw/") or not _object_key_is_safe(key):
            raise ValueError("provider raw object key is invalid")
        version_id = _version_id_from_frozen_uri(
            uri,
            bucket=self._bucket,
            storage_key=self._storage_key(key),
        )
        return await _run_object_io(
            self._read_bounded,
            key,
            version_id=version_id,
        )

    async def put_normalized(self, key: str, payload: bytes) -> str:
        if not key.startswith("normalized/market/"):
            raise ValueError("normalized market object key is invalid")
        return await self.put_if_absent(key=key, content=payload)

    async def read_normalized(self, uri: str) -> bytes:
        """Read a frozen diagnostic artifact only from this store's market prefix."""
        key = urlsplit(uri).path.removeprefix(f"/{self._prefix}/")
        if not key.startswith("normalized/market/") or not _object_key_is_safe(key):
            raise ValueError("normalized market object key is invalid")
        version_id = _version_id_from_frozen_uri(
            uri,
            bucket=self._bucket,
            storage_key=self._storage_key(key),
        )
        return await _run_object_io(self._read_bounded, key, version_id=version_id)

    async def _put_create_only(
        self,
        key: str,
        content: bytes,
        digest: str,
    ) -> str:
        storage_key = self._storage_key(key)
        try:
            response = await _run_object_io(
                self._put_object,
                Bucket=self._bucket,
                Key=storage_key,
                Body=content,
                IfNoneMatch="*",
                Metadata={"sha256": digest},
            )
            version = response.get("VersionId")
        except ClientError as error:
            if not _is_create_conflict(error):
                raise
            version = await _run_object_io(
                self._verify_existing,
                storage_key,
                content,
            )
        if not isinstance(version, str) or not version or version == "null":
            raise RuntimeError("PROVIDER_OBJECT_VERSION_UNAVAILABLE")
        return f"s3://{self._bucket}/{storage_key}?versionId={quote(version, safe='')}"

    def _verify_existing(self, key: str, expected: bytes) -> str:
        response = self._get_object(Bucket=self._bucket, Key=key)
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
        if not isinstance(version, str) or not version or version == "null":
            raise RuntimeError("PROVIDER_OBJECT_VERSION_UNAVAILABLE")
        return version

    def _read_bounded(self, key: str, *, version_id: str | None = None) -> bytes:
        request: dict[str, object] = {
            "Bucket": self._bucket,
            "Key": self._storage_key(key),
        }
        if version_id is not None:
            request["VersionId"] = version_id
        response = self._get_object(
            **request,
        )
        if version_id is not None:
            response_version = response.get("VersionId")
            if not isinstance(response_version, str) or not hmac.compare_digest(
                response_version,
                version_id,
            ):
                raise RuntimeError("PROVIDER_OBJECT_VERSION_MISMATCH")
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

    def _storage_key(self, key: str) -> str:
        return f"{self._prefix}/{key}"

    def _put_object(self, **kwargs: object) -> Mapping[str, object]:
        _enforce_object_store_dependency_log_boundary()
        return self._client.put_object(**kwargs)

    def _get_object(self, **kwargs: object) -> Mapping[str, object]:
        _enforce_object_store_dependency_log_boundary()
        return self._client.get_object(**kwargs)


def _enforce_object_store_dependency_log_boundary() -> None:
    """Route credential-bearing SDK diagnostics to a closed local sink."""
    with _OBJECT_STORE_DEPENDENCY_LOG_LOCK:
        names = set(_OBJECT_STORE_DEPENDENCY_NAMESPACES)
        names.update(
            name
            for name in logging.Logger.manager.loggerDict
            if isinstance(name, str)
            and any(
                name.startswith(f"{namespace}.")
                for namespace in _OBJECT_STORE_DEPENDENCY_NAMESPACES
            )
        )
        for name in names:
            dependency_logger = logging.getLogger(name)
            if _OBJECT_STORE_DEPENDENCY_FILTER not in dependency_logger.filters:
                dependency_logger.addFilter(_OBJECT_STORE_DEPENDENCY_FILTER)
            if name in _OBJECT_STORE_DEPENDENCY_NAMESPACES:
                if not any(
                    isinstance(handler, logging.NullHandler)
                    for handler in dependency_logger.handlers
                ):
                    dependency_logger.addHandler(logging.NullHandler())
                dependency_logger.propagate = False


def _validate_content_key(key: str, content: bytes) -> str:
    if (
        not content
        or len(content) > _MAX_OBJECT_BYTES
        or not _object_key_is_safe(key)
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


def _object_key_is_safe(key: str) -> bool:
    return (
        key.isascii()
        and len(key) <= 1_024
        and not key.startswith("/")
        and "//" not in key
        and _SAFE_OBJECT_KEY.fullmatch(key) is not None
        and all(segment not in {"", ".", ".."} for segment in key.split("/"))
    )


def _is_create_conflict(error: ClientError) -> bool:
    status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    document = error.response.get("Error", {})
    code = document.get("Code") if isinstance(document, dict) else None
    return status in {409, 412} or code in {
        "ConditionalRequestConflict",
        "PreconditionFailed",
    }


def _version_id_from_frozen_uri(
    uri: str,
    *,
    bucket: str,
    storage_key: str,
) -> str:
    """Return the exact version only for this configured bucket and object."""
    if (
        not isinstance(uri, str)
        or not uri
        or not uri.isascii()
        or "#" in uri
        or any(ord(char) < 0x20 or ord(char) == 0x7F for char in uri)
    ):
        raise ValueError("provider raw object URI is invalid")
    try:
        parsed = urlsplit(uri)
        username = parsed.username
        password = parsed.password
        hostname = parsed.hostname
    except ValueError as error:
        raise ValueError("provider raw object URI is invalid") from error
    if (
        parsed.scheme != "s3"
        or parsed.netloc != bucket
        or hostname != bucket
        or username is not None
        or password is not None
        or parsed.path != f"/{storage_key}"
    ):
        raise ValueError("provider raw object URI is invalid")
    prefix = "versionId="
    if (
        not parsed.query.startswith(prefix)
        or parsed.query.count("=") != 1
        or any(separator in parsed.query for separator in ("&", ";"))
    ):
        raise ValueError("provider raw object URI is invalid")
    encoded_version = parsed.query.removeprefix(prefix)
    if not encoded_version or not _has_valid_percent_escapes(encoded_version):
        raise ValueError("provider raw object URI is invalid")
    try:
        version = unquote(encoded_version, errors="strict")
    except UnicodeError as error:
        raise ValueError("provider raw object URI is invalid") from error
    if (
        not version
        or version == "null"
        or any(ord(char) < 0x20 or ord(char) == 0x7F for char in version)
    ):
        raise ValueError("provider raw object URI is invalid")
    return version


def _has_valid_percent_escapes(value: str) -> bool:
    index = 0
    hexdigits = frozenset("0123456789abcdefABCDEF")
    while index < len(value):
        if value[index] != "%":
            index += 1
            continue
        if index + 2 >= len(value) or any(
            char not in hexdigits for char in value[index + 1 : index + 3]
        ):
            return False
        index += 3
    return True


async def _run_object_io[T](
    operation: Callable[..., T],
    *args: object,
    **kwargs: object,
) -> T:
    """Run one bounded blocking request without an interpreter-exit join hazard."""
    if not _OBJECT_IO_GATE.acquire(blocking=False):
        raise ProviderObjectIOUnavailable("OBJECT_IO_SATURATED")
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _OBJECT_IO_TIMEOUT_SECONDS
    future: asyncio.Future[object] = loop.create_future()

    def worker() -> None:
        result: object | None = None
        error: BaseException | None = None
        try:
            result = operation(*args, **kwargs)
        except BaseException as caught:  # pragma: no branch - one delivery path
            error = caught
        finally:
            # Only the blocking worker owns the slot. Caller timeout or
            # cancellation cannot admit a 33rd abandoned request.
            _OBJECT_IO_GATE.release()
        try:
            loop.call_soon_threadsafe(
                _deliver_object_io_result,
                future,
                result,
                error,
            )
        except RuntimeError:
            # The owning loop may already be closed after process shutdown.
            pass

    thread = threading.Thread(
        target=worker,
        name="nexus-object-io",
        daemon=True,
    )
    try:
        thread.start()
    except BaseException:
        _OBJECT_IO_GATE.release()
        raise

    try:
        async with asyncio.timeout_at(deadline):
            return cast(T, await future)
    except TimeoutError as error:
        raise ProviderObjectIOUnavailable("OBJECT_IO_DEADLINE_EXCEEDED") from error
    finally:
        # A daemon may finish after timeout/cancellation. Cancelling its
        # delivery future makes that late callback an intentional no-op.
        if not future.done():
            future.cancel()


def _deliver_object_io_result(
    future: asyncio.Future[object],
    result: object | None,
    error: BaseException | None,
) -> None:
    if future.done():
        return
    if error is None:
        future.set_result(result)
    else:
        future.set_exception(error)
