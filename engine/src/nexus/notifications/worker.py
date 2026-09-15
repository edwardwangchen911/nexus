"""Dedicated outbox worker for the local-only Bark notification adapter."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Awaitable, Callable, Mapping
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from datetime import UTC, datetime

import httpx
from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationError, model_validator

from nexus.domain.ids import new_uuid7
from nexus.notifications.bark import (
    BARK_EVENT_SCHEMAS,
    BarkPublisher,
    validate_bark_read_only_url,
    validate_local_bark_server_url,
)
from nexus.outbox.dispatcher import dispatch_outbox_once
from nexus.outbox.models import Publisher, PublisherSpec
from nexus.outbox.registry import PublisherRegistry
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings

_DEVICE_KEY = re.compile(r"^[A-Za-z0-9_-]{1,255}$")


class BarkWorkerConfigurationError(RuntimeError):
    """A value-free local startup rejection."""


class BarkWorkerSettings(BaseModel):
    """Bark worker configuration with secret values hidden from representations."""

    model_config = ConfigDict(extra="ignore", hide_input_in_errors=True)

    enabled: bool = Field(default=False, validation_alias="NEXUS_BARK_ENABLED")
    database_url: SecretStr | None = Field(default=None, validation_alias="DATABASE_URL")
    server_url: str = Field(
        default="http://127.0.0.1:8080",
        validation_alias="NEXUS_BARK_SERVER_URL",
    )
    device_key: SecretStr | None = Field(
        default=None,
        validation_alias="NEXUS_BARK_DEVICE_KEY",
    )
    encryption_key: SecretStr | None = Field(
        default=None,
        validation_alias="NEXUS_BARK_ENCRYPTION_KEY",
    )
    activated_at: datetime | None = Field(
        default=None,
        validation_alias="NEXUS_BARK_ACTIVATED_AT",
    )
    read_only_url: str | None = Field(
        default=None,
        validation_alias="NEXUS_BARK_READ_ONLY_URL",
    )
    request_timeout_seconds: float = Field(
        default=3.0,
        ge=0.1,
        le=10.0,
        validation_alias="NEXUS_BARK_REQUEST_TIMEOUT_SECONDS",
    )
    poll_seconds: float = Field(
        default=5.0,
        ge=0.5,
        le=60.0,
        validation_alias="NEXUS_BARK_POLL_SECONDS",
    )

    @model_validator(mode="after")
    def validate_enabled_configuration(self) -> BarkWorkerSettings:
        if not self.enabled:
            return self
        if (
            self.database_url is None
            or self.device_key is None
            or self.encryption_key is None
            or self.activated_at is None
        ):
            raise ValueError("enabled Bark worker configuration is incomplete")
        validate_local_bark_server_url(self.server_url)
        if self.activated_at.tzinfo is None or self.activated_at.utcoffset() != UTC.utcoffset(
            self.activated_at
        ):
            raise ValueError("Bark activation time must use UTC")
        self.activated_at = self.activated_at.astimezone(UTC)
        if self.read_only_url is not None:
            validate_bark_read_only_url(self.read_only_url)
        return self

    def __repr_args__(self) -> list[tuple[str | None, object]]:
        return []


@dataclass(frozen=True)
class BarkCredentials:
    device_key: str = field(repr=False)
    encryption_key: bytes = field(repr=False)


def load_bark_worker_settings(environment: Mapping[str, str]) -> BarkWorkerSettings:
    """Validate process configuration without exposing secret values in errors."""
    normalized_environment = dict(environment)
    if normalized_environment.get("NEXUS_BARK_READ_ONLY_URL") == "":
        normalized_environment.pop("NEXUS_BARK_READ_ONLY_URL")
    try:
        return BarkWorkerSettings.model_validate(normalized_environment)
    except (ValidationError, ValueError):
        raise BarkWorkerConfigurationError("BARK_CONFIGURATION_INVALID") from None


def load_bark_credentials(settings: BarkWorkerSettings) -> BarkCredentials:
    """Validate the two environment-backed values without exposing them in errors."""
    if settings.device_key is None or settings.encryption_key is None:
        raise BarkWorkerConfigurationError("BARK_CONFIGURATION_INVALID")
    device_key = settings.device_key.get_secret_value()
    encryption_key = settings.encryption_key.get_secret_value()
    if _DEVICE_KEY.fullmatch(device_key) is None:
        raise BarkWorkerConfigurationError("BARK_DEVICE_KEY_INVALID")
    if len(encryption_key) != 16 or not encryption_key.isascii():
        raise BarkWorkerConfigurationError("BARK_ENCRYPTION_KEY_INVALID")
    return BarkCredentials(
        device_key=device_key,
        encryption_key=encryption_key.encode("ascii"),
    )


def build_bark_registry(publisher: Publisher) -> PublisherRegistry:
    """Register only notification-safe Paper event contracts."""
    return PublisherRegistry(
        PublisherSpec(
            event_type=event_type,
            schema_version=schema_version,
            consumer_idempotent=True,
            publisher=publisher,
        )
        for event_type, schema_version in BARK_EVENT_SCHEMAS
    )


async def run_bark_worker(
    settings: BarkWorkerSettings,
    *,
    run_forever: bool,
    client: httpx.AsyncClient | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> int:
    """Deliver queued notifications without participating in business transactions."""
    if not settings.enabled:
        return 0
    if settings.database_url is None or settings.activated_at is None:
        raise BarkWorkerConfigurationError("BARK_CONFIGURATION_INVALID")
    credentials = load_bark_credentials(settings)
    factory = session_factory(EngineSettings(database_url=settings.database_url.get_secret_value()))
    worker_id = f"bark-notifications-{new_uuid7()}"
    delivered = 0
    async with AsyncExitStack() as stack:
        transport = client
        if transport is None:
            transport = await stack.enter_async_context(
                httpx.AsyncClient(timeout=settings.request_timeout_seconds)
            )
        publisher = BarkPublisher(
            client=transport,
            server_url=settings.server_url,
            device_key=credentials.device_key,
            encryption_key=credentials.encryption_key,
            read_only_url=settings.read_only_url,
        )
        registry = build_bark_registry(publisher)
        while True:
            worked = await dispatch_outbox_once(
                factory,
                registry=registry,
                worker_id=worker_id,
                clock=clock,
                claim_registered_only=True,
                created_at_or_after=settings.activated_at,
            )
            if worked:
                delivered += 1
            if not run_forever:
                return delivered
            if not worked:
                await sleep(settings.poll_seconds)
