"""Create-only S3/MinIO provider evidence storage contracts."""

from __future__ import annotations

import asyncio
import logging
import threading
from hashlib import sha256
from io import BytesIO
from typing import Any, cast

import pytest
from botocore.awsrequest import AWSResponse
from botocore.exceptions import ClientError
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.providers.settings import ProviderWorkerSettings


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.versions: dict[str, str] = {}
        self.put_calls: list[dict[str, object]] = []
        self.get_calls: list[dict[str, object]] = []

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
        self.get_calls.append(kwargs)
        key = cast(str, kwargs["Key"])
        return {
            "Body": BytesIO(self.objects[key]),
            "VersionId": self.versions[key],
        }

    def delete_object(self, **kwargs: object) -> dict[str, object]:
        key = cast(str, kwargs["Key"])
        self.objects.pop(key, None)
        return {}


class VersionedS3Client(FakeS3Client):
    def __init__(self) -> None:
        super().__init__()
        self.versioned_objects: dict[tuple[str, str], bytes] = {}
        self.get_calls: list[dict[str, object]] = []

    def get_object(self, **kwargs: object) -> dict[str, object]:
        self.get_calls.append(kwargs)
        key = cast(str, kwargs["Key"])
        version = cast(str, kwargs["VersionId"])
        return {
            "Body": BytesIO(self.versioned_objects[(key, version)]),
            "VersionId": version,
        }


class MisreportingVersionedS3Client(VersionedS3Client):
    def get_object(self, **kwargs: object) -> dict[str, object]:
        response = super().get_object(**kwargs)
        response["VersionId"] = "different-version"
        return response


class NullVersionS3Client(FakeS3Client):
    def put_object(self, **kwargs: object) -> dict[str, str]:
        response = super().put_object(**kwargs)
        key = cast(str, kwargs["Key"])
        self.versions[key] = "null"
        response["VersionId"] = "null"
        return response


class BlockingS3Client(FakeS3Client):
    def __init__(self, expected_concurrency: int) -> None:
        super().__init__()
        self.expected_concurrency = expected_concurrency
        self.active = 0
        self.max_active = 0
        self.lock = threading.Lock()
        self.reached = threading.Event()
        self.release = threading.Event()
        self.worker_daemon_flags: list[bool] = []
        self.worker_names: list[str] = []

    def put_object(self, **kwargs: object) -> dict[str, str]:
        with self.lock:
            self.worker_daemon_flags.append(threading.current_thread().daemon)
            self.worker_names.append(threading.current_thread().name)
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            if self.active == self.expected_concurrency:
                self.reached.set()
        try:
            if not self.release.wait(timeout=3):
                raise TimeoutError("test release was not signalled")
            return super().put_object(**kwargs)
        finally:
            with self.lock:
                self.active -= 1


class _SdkRawResponse:
    def __init__(self, payload: bytes) -> None:
        self._body = BytesIO(payload)

    def stream(self, amt: int = 1_024, decode_content: bool = False) -> object:
        del decode_content
        while chunk := self._body.read(amt):
            yield chunk

    def read(self, amount: int = -1) -> bytes:
        return self._body.read(amount)

    def close(self) -> None:
        self._body.close()


class _SdkHttpSession:
    def __init__(self, get_payload: bytes = b"") -> None:
        self._get_payload = get_payload
        self.requests: list[object] = []

    def send(self, request: object) -> AWSResponse:
        self.requests.append(request)
        method = cast(str, getattr(request, "method"))
        payload = self._get_payload if method == "GET" else b""
        return AWSResponse(
            cast(str, getattr(request, "url")),
            200,
            {
                "content-length": str(len(payload)),
                "x-amz-version-id": "sdk-version-1",
            },
            _SdkRawResponse(payload),
        )

    def close(self) -> None:
        pass


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


def _sdk_log_settings() -> ProviderWorkerSettings:
    return ProviderWorkerSettings.model_validate(
        {
            "PROVIDER_DATABASE_URL": "postgresql+asyncpg://provider.invalid/nexus",
            "PROVIDER_OBJECT_STORE_ENDPOINT": "https://s3-log-endpoint-canary.invalid",
            "PROVIDER_OBJECT_STORE_REGION": "us-east-1",
            "PROVIDER_OBJECT_STORE_BUCKET": "nexus-log-bucket-canary",
            "PROVIDER_OBJECT_STORE_ACCESS_KEY_ID": "s3-access-key-log-canary",
            "PROVIDER_OBJECT_STORE_SECRET_ACCESS_KEY": "s3-secret-key-log-canary",
            "PROVIDER_OBJECT_STORE_PREFIX": "provider/market",
        }
    )


def _reopen_dependency_loggers(monkeypatch: pytest.MonkeyPatch) -> None:
    """Model a root DEBUG reconfiguration after the store was constructed."""
    for name in ("boto3", "botocore", "s3transfer", "urllib3"):
        logger = logging.getLogger(name)
        monkeypatch.setattr(logger, "propagate", True)
        monkeypatch.setattr(logger, "level", logging.DEBUG)


def _assert_sdk_canaries_absent(rendered: str, *, payload: bytes) -> None:
    assert "s3-access-key-log-canary" not in rendered
    assert "s3-secret-key-log-canary" not in rendered
    assert "Authorization" not in rendered
    assert "Credential=" not in rendered
    assert payload.decode() not in rendered
    assert "s3-log-endpoint-canary.invalid" not in rendered


@pytest.mark.asyncio
async def test_real_s3_put_never_reaches_dependency_logs(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"raw-s3-put-payload-log-canary"
    caplog.set_level(logging.DEBUG)
    caplog.set_level(logging.DEBUG, logger="boto3")
    caplog.set_level(logging.DEBUG, logger="botocore")
    store = S3ProviderEvidenceStore(_sdk_log_settings())
    session = _SdkHttpSession()
    cast(Any, store._client)._endpoint.http_session = session
    _reopen_dependency_loggers(monkeypatch)
    digest = sha256(payload).hexdigest()

    uri = await store.archive_raw(
        f"raw/market/fosun/2026/08/30/log/{digest}.json",
        payload,
    )

    assert uri.endswith("versionId=sdk-version-1")
    assert session.requests
    _assert_sdk_canaries_absent(caplog.text, payload=payload)


@pytest.mark.asyncio
async def test_real_s3_get_never_reaches_dependency_logs(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"raw-s3-get-payload-log-canary"
    caplog.set_level(logging.DEBUG)
    caplog.set_level(logging.DEBUG, logger="boto3")
    caplog.set_level(logging.DEBUG, logger="botocore")
    store = S3ProviderEvidenceStore(_sdk_log_settings())
    session = _SdkHttpSession(get_payload=payload)
    cast(Any, store._client)._endpoint.http_session = session
    _reopen_dependency_loggers(monkeypatch)
    digest = sha256(payload).hexdigest()
    key = f"raw/market/fosun/2026/08/30/log/{digest}.json"
    storage_key = f"provider/market/{key}"

    result = await store.read_raw(
        key,
        uri=(
            "s3://nexus-log-bucket-canary/"
            f"{storage_key}?versionId=sdk-version-1"
        ),
    )

    assert result == payload
    assert session.requests
    _assert_sdk_canaries_absent(caplog.text, payload=payload)


def test_object_store_client_budget_has_one_attempt_and_32_connections(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def client_factory(*_: object, **kwargs: object) -> FakeS3Client:
        captured.update(kwargs)
        return FakeS3Client()

    monkeypatch.setattr("nexus.market.object_store.boto3.client", client_factory)

    S3ProviderEvidenceStore(_settings())

    config = captured["config"]
    assert getattr(config, "connect_timeout") == 1
    assert getattr(config, "read_timeout") == 3
    assert getattr(config, "max_pool_connections") == 32
    retries = cast(dict[str, object], getattr(config, "retries"))
    assert retries["total_max_attempts"] == 1
    assert "max_attempts" not in retries


@pytest.mark.asyncio
async def test_object_store_executes_32_independent_writes_concurrently() -> None:
    client = BlockingS3Client(expected_concurrency=32)
    store = S3ProviderEvidenceStore(_settings(), client=client)
    tasks = []
    for index in range(32):
        payload = f"PAR1-{index}".encode()
        digest = sha256(payload).hexdigest()
        tasks.append(
            asyncio.create_task(
                store.put_normalized(
                    f"normalized/market/2026/08/28/{index}/{digest}.parquet",
                    payload,
                )
            )
        )
    reached = False
    try:
        async with asyncio.timeout(1):
            while not client.reached.is_set():
                await asyncio.sleep(0.01)
        reached = True
    except TimeoutError:
        pass
    finally:
        client.release.set()
        await asyncio.gather(*tasks)

    assert reached
    assert client.max_active == 32


@pytest.mark.asyncio
async def test_cancelled_object_calls_keep_slots_until_daemon_workers_exit() -> None:
    client = BlockingS3Client(expected_concurrency=32)
    store = S3ProviderEvidenceStore(_settings(), client=client)
    tasks = []
    for index in range(32):
        payload = f"blocked-{index}".encode()
        digest = sha256(payload).hexdigest()
        tasks.append(
            asyncio.create_task(
                store.put_normalized(
                    f"normalized/market/2026/08/28/{index}/{digest}.parquet",
                    payload,
                )
            )
        )
    try:
        async with asyncio.timeout(1):
            while not client.reached.is_set():
                await asyncio.sleep(0.01)

        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        overflow_payload = b"overflow"
        overflow_digest = sha256(overflow_payload).hexdigest()
        with pytest.raises(RuntimeError, match="OBJECT_IO_SATURATED"):
            async with asyncio.timeout(0.25):
                await store.put_normalized(
                    f"normalized/market/2026/08/28/overflow/{overflow_digest}.parquet",
                    overflow_payload,
                )
    finally:
        client.release.set()
        async with asyncio.timeout(1):
            while client.active:
                await asyncio.sleep(0.01)

    recovered_payload = b"recovered"
    recovered_digest = sha256(recovered_payload).hexdigest()
    await store.put_normalized(
        f"normalized/market/2026/08/28/recovered/{recovered_digest}.parquet",
        recovered_payload,
    )

    assert client.worker_daemon_flags
    assert all(client.worker_daemon_flags)
    assert all(name.startswith("nexus-object-io") for name in client.worker_names)


@pytest.mark.asyncio
async def test_object_call_has_a_fixed_total_wall_clock_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "nexus.market.object_store._OBJECT_IO_TIMEOUT_SECONDS",
        0.05,
        raising=False,
    )
    client = BlockingS3Client(expected_concurrency=1)
    store = S3ProviderEvidenceStore(_settings(), client=client)
    payload = b"deadline"
    digest = sha256(payload).hexdigest()
    try:
        with pytest.raises(RuntimeError, match="OBJECT_IO_DEADLINE_EXCEEDED"):
            async with asyncio.timeout(0.5):
                await store.put_normalized(
                    f"normalized/market/2026/08/28/deadline/{digest}.parquet",
                    payload,
                )
    finally:
        client.release.set()
        async with asyncio.timeout(1):
            while client.active:
                await asyncio.sleep(0.01)


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
@pytest.mark.parametrize(
    "unsafe_segment",
    ["query?x", "fragment#x", "encoded%2Fslash", "back\\slash", "line\nbreak"],
)
async def test_content_addressed_writer_rejects_uri_unsafe_key_segments(
    unsafe_segment: str,
) -> None:
    client = FakeS3Client()
    store = S3ProviderEvidenceStore(_settings(), client=client)
    payload = b'{"unsafe":true}'
    digest = sha256(payload).hexdigest()

    with pytest.raises(ValueError, match="provider content-addressed object is invalid"):
        await store.put_if_absent(
            key=f"evidence/sec/{unsafe_segment}/{digest}.json",
            content=payload,
        )

    assert client.put_calls == []


@pytest.mark.asyncio
async def test_put_rejects_the_s3_null_version_sentinel() -> None:
    client = NullVersionS3Client()
    store = S3ProviderEvidenceStore(_settings(), client=client)
    payload = b'{"versioning":"suspended"}'
    digest = sha256(payload).hexdigest()

    with pytest.raises(RuntimeError, match="PROVIDER_OBJECT_VERSION_UNAVAILABLE"):
        await store.put_if_absent(
            key=f"raw/market/fosun/2026/08/30/{digest}.json",
            content=payload,
        )


@pytest.mark.asyncio
async def test_create_conflict_rejects_an_existing_null_version() -> None:
    client = FakeS3Client()
    store = S3ProviderEvidenceStore(_settings(), client=client)
    payload = b'{"versioning":"suspended-conflict"}'
    digest = sha256(payload).hexdigest()
    key = f"raw/market/fosun/2026/08/30/{digest}.json"
    storage_key = f"provider/market/{key}"
    client.objects[storage_key] = payload
    client.versions[storage_key] = "null"

    with pytest.raises(RuntimeError, match="PROVIDER_OBJECT_VERSION_UNAVAILABLE"):
        await store.put_if_absent(key=key, content=payload)


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
async def test_raw_staging_reader_is_bounded_and_raw_prefix_only() -> None:
    client = FakeS3Client()
    store = S3ProviderEvidenceStore(_settings(), client=client)
    payload = b'{"staged":true}'
    digest = sha256(payload).hexdigest()
    key = f"raw/market/fosun/2026/08/28/request/{digest}.json"
    uri = await store.archive_raw(key, payload)

    assert await store.read_raw(key, uri=uri) == payload
    with pytest.raises(ValueError, match="raw object key"):
        await store.read_raw(
            f"normalized/market/{digest}.parquet",
            uri=uri,
        )


@pytest.mark.asyncio
async def test_raw_staging_reader_fetches_the_frozen_s3_version() -> None:
    client = VersionedS3Client()
    store = S3ProviderEvidenceStore(_settings(), client=client)
    frozen_payload = b'{"version":"frozen"}'
    current_payload = b'{"version":"current"}'
    digest = sha256(frozen_payload).hexdigest()
    key = f"raw/market/fosun/2026/08/28/request/{digest}.json"
    storage_key = f"provider/market/{key}"
    client.versioned_objects[(storage_key, "frozen/version")] = frozen_payload
    client.versioned_objects[(storage_key, "current-version")] = current_payload
    client.objects[storage_key] = current_payload
    client.versions[storage_key] = "current-version"
    frozen_uri = f"s3://nexus-fixtures/{storage_key}?versionId=frozen%2Fversion"

    payload = await store.read_raw(key, uri=frozen_uri)

    assert payload == frozen_payload
    assert client.get_calls == [
        {
            "Bucket": "nexus-fixtures",
            "Key": storage_key,
            "VersionId": "frozen/version",
        }
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "uri",
    [
        "s3://other-bucket/provider/market/raw/market/fosun/item?versionId=v1",
        "s3://nexus-fixtures/provider/other/raw/market/fosun/item?versionId=v1",
        "s3://nexus-fixtures/provider/market/raw/market/fosun/other?versionId=v1",
        "s3://nexus-fixtures/provider/market/raw/market/fosun/item",
        "s3://nexus-fixtures/provider/market/raw/market/fosun/item?versionId=v1&x=1",
        "s3://nexus-fixtures/provider/market/raw/market/fosun/item?versionId=%ZZ",
        "s3://nexus-fixtures/provider/market/raw/market/fosun/item?versionId=null",
        "s3://nexus-fixtures/provider/market/raw/market/fosun/item?versionId=%6Eull",
        "s3://user@nexus-fixtures/provider/market/raw/market/fosun/item?versionId=v1",
        "s3://nexus-fixtures/provider/market/raw/market/fosun/item?versionId=v1#fragment",
    ],
)
async def test_raw_staging_reader_rejects_a_locator_outside_the_frozen_object(
    uri: str,
) -> None:
    client = VersionedS3Client()
    store = S3ProviderEvidenceStore(_settings(), client=client)

    with pytest.raises(ValueError, match="raw object URI"):
        await store.read_raw("raw/market/fosun/item", uri=uri)

    assert client.get_calls == []


@pytest.mark.asyncio
async def test_raw_staging_reader_rejects_a_misreported_s3_version() -> None:
    client = MisreportingVersionedS3Client()
    store = S3ProviderEvidenceStore(_settings(), client=client)
    payload = b'{"version":"frozen"}'
    digest = sha256(payload).hexdigest()
    key = f"raw/market/fosun/2026/08/28/request/{digest}.json"
    storage_key = f"provider/market/{key}"
    client.versioned_objects[(storage_key, "frozen-version")] = payload
    frozen_uri = f"s3://nexus-fixtures/{storage_key}?versionId=frozen-version"

    with pytest.raises(RuntimeError, match="PROVIDER_OBJECT_VERSION_MISMATCH"):
        await store.read_raw(key, uri=frozen_uri)


@pytest.mark.asyncio
async def test_normalized_writer_is_content_addressed_and_create_only() -> None:
    client = FakeS3Client()
    store = S3ProviderEvidenceStore(_settings(), client=client)
    payload = b"PAR1-immutable-normalized"
    digest = sha256(payload).hexdigest()
    key = f"normalized/market/2026/08/28/instrument/{digest}.parquet"

    first = await store.put_normalized(key, payload)
    replay = await store.put_normalized(key, payload)

    assert first.endswith("versionId=version-1")
    assert replay.endswith("versionId=version-1")
    assert client.objects[f"provider/market/{key}"] == payload
    assert client.get_calls == [
        {"Bucket": "nexus-fixtures", "Key": f"provider/market/{key}"}
    ]
    with pytest.raises(ValueError, match="normalized market object key"):
        await store.put_normalized(f"raw/market/{digest}.json", payload)
