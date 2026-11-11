"""Frozen local evidence reads must not leave the local host through a proxy."""

from typing import Any

import pytest
from nexus.research.sec_exhibit_storage import configured_exhibit_store
from nexus.settings import EngineSettings
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.parametrize(
    ("endpoint", "direct"),
    [
        ("http://127.0.0.1:59119", True),
        ("https://127.23.4.5:9000", True),
        ("http://localhost:9000", True),
        ("http://[::1]:9000", True),
        ("https://localhost.external.invalid:9000", False),
        ("https://127.0.0.1.external.invalid:9000", False),
        ("http://minio:9000", False),
        ("https://evidence.example.invalid", False),
    ],
)
def test_configured_evidence_read_routes_only_literal_loopback_direct(
    monkeypatch: pytest.MonkeyPatch, endpoint: str, direct: bool
) -> None:
    monkeypatch.setenv("HTTP_PROXY", "http://proxy.example.invalid:9090")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.example.invalid:9090")
    captured: dict[str, Any] = {}

    def client(*args: object, **kwargs: Any) -> object:
        captured.update(kwargs)
        return object()

    monkeypatch.setattr("nexus.market.object_store.boto3.client", client)
    settings = EngineSettings(
        OBJECT_STORE_ENDPOINT=endpoint,
        OBJECT_STORE_REGION="us-east-1",
        OBJECT_STORE_BUCKET="synthetic-evidence",
        OBJECT_STORE_ACCESS_KEY_ID="synthetic-access",
        OBJECT_STORE_SECRET_ACCESS_KEY="synthetic-secret",
    )
    session = AsyncSession()
    session.info["engine_settings"] = settings
    configured_exhibit_store(session)
    assert captured["endpoint_url"] == endpoint
    assert captured["config"].proxies == ({} if direct else None)
    assert captured["config"].retries == {"total_max_attempts": 1}


def test_explicit_evidence_store_override_keeps_its_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(*args: object, **kwargs: object) -> object:
        pytest.fail("an explicit evidence store must retain its own transport")

    monkeypatch.setattr("nexus.research.sec_exhibit_storage.S3ProviderEvidenceStore", forbidden)
    session = AsyncSession()
    store = object()
    session.info["sec_exhibit_store"] = store
    assert configured_exhibit_store(session) is store


def test_evidence_store_still_requires_engine_configuration() -> None:
    with pytest.raises(ValueError, match="SEC_EXHIBIT_STORE_UNAVAILABLE"):
        configured_exhibit_store(AsyncSession())
