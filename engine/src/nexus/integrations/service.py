"""Persistence and projection logic for integration status resources."""

import hashlib
import re
from datetime import UTC, datetime
from typing import Literal, TypeVar, cast

from pydantic import BaseModel
from sqlalchemy import desc, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.api.auth import VerifiedServiceActor
from nexus.api.errors import ErrorCode, NexusError
from nexus.api.schemas.integrations import (
    IntegrationMode,
    IntegrationStatusResponse,
    ProviderName,
)
from nexus.persistence.models.data import ProviderCapability
from nexus.persistence.models.runtime import IdempotencyRecord, IntegrationStatus

_PROVIDERS: tuple[ProviderName, ...] = ("fosun", "alpaca", "sec", "fred", "openai")
_MODES: frozenset[str] = frozenset({"disabled", "fixture", "read_only"})
_CONTRACT_HASH = re.compile(r"^[0-9a-f]{64}$")
_FIXTURE_AS_OF = datetime(2026, 8, 22, tzinfo=UTC)
_FOSUN_BROKER_OPERATIONS = ("fosun_list_orders", "fosun_sync_portfolio")
_ResponseModel = TypeVar("_ResponseModel", bound=BaseModel)


class CapabilityProjection:
    """The one authoritative READ_ONLY record and its validation outcome."""

    def __init__(self, authority: ProviderCapability | None, now: datetime) -> None:
        self.authority = authority
        self.valid = (
            authority if authority is not None and _is_valid_capability(authority, now) else None
        )

    @property
    def contract_hash(self) -> str | None:
        if self.authority is None or _CONTRACT_HASH.fullmatch(self.authority.contract_hash) is None:
            return None
        return self.authority.contract_hash

    @property
    def expires_at(self) -> datetime | None:
        authority = self.authority
        if authority is None or self.contract_hash is None:
            return None
        return authority.expires_at


class IntegrationStatusService:
    """Own status persistence, admission authority, CAS, and replay within one session."""

    def __init__(self, session: AsyncSession, now: datetime) -> None:
        self._session = session
        self._now = now

    @property
    def providers(self) -> tuple[ProviderName, ...]:
        return _PROVIDERS

    async def find_status(self, provider: ProviderName) -> IntegrationStatus | None:
        rows = await self._session.scalars(
            select(IntegrationStatus)
            .where(IntegrationStatus.provider == provider)
            .execution_options(populate_existing=True)
        )
        return rows.one_or_none()

    async def all_statuses(self) -> dict[str, IntegrationStatus]:
        rows = await self._session.scalars(select(IntegrationStatus))
        return {row.provider: row for row in rows}

    async def capability(
        self,
        provider: ProviderName,
        *,
        operation: str | None = None,
        market_scope: str | None = None,
    ) -> CapabilityProjection:
        statement = select(ProviderCapability).where(
            ProviderCapability.provider == provider,
            ProviderCapability.capability == "READ_ONLY",
        )
        if operation is not None:
            statement = statement.where(
                ProviderCapability.payload["operation"].as_string() == operation
            )
        if market_scope is not None:
            statement = statement.where(
                ProviderCapability.payload["market_scope"].as_string() == market_scope
            )
        authority = await self._session.scalar(
            statement.order_by(
                desc(ProviderCapability.created_at), desc(ProviderCapability.id)
            ).limit(1)
        )
        return CapabilityProjection(authority, self._now)

    async def capabilities(self) -> dict[ProviderName, CapabilityProjection]:
        return {provider: await self.capability(provider) for provider in self.providers}

    def response(
        self,
        provider: ProviderName,
        persisted: IntegrationStatus | None,
        capability: CapabilityProjection,
    ) -> IntegrationStatusResponse:
        mode = _stored_mode(persisted)
        version = persisted.version if persisted is not None else 0
        if mode == "read_only":
            if (
                persisted is not None
                and capability.valid is not None
                and persisted.admitted_contract_hash == capability.valid.contract_hash
                and _capability_is_after_invalidation(capability.valid, persisted)
            ):
                if persisted.last_success_at is not None:
                    return IntegrationStatusResponse(
                        provider=provider,
                        mode="read_only",
                        quality="PASS",
                        as_of=persisted.last_success_at,
                        capability_expires_at=capability.valid.expires_at,
                        contract_hash=capability.valid.contract_hash,
                        next_action=None,
                        version=version,
                    )
                return IntegrationStatusResponse(
                    provider=provider,
                    mode="read_only",
                    quality="PARTIAL",
                    as_of=None,
                    capability_expires_at=capability.valid.expires_at,
                    contract_hash=capability.valid.contract_hash,
                    next_action="A successful read-only connectivity check is required.",
                    version=version,
                )
            return self._disabled_response(
                provider,
                version=version,
                quality="FAIL",
                capability=capability,
                next_action="A valid capability admission is required.",
            )
        if mode == "fixture" and provider == "fosun":
            return IntegrationStatusResponse(
                provider=provider,
                mode="fixture",
                quality="PASS",
                as_of=(persisted.last_success_at if persisted is not None else None)
                or _FIXTURE_AS_OF,
                capability_expires_at=None,
                contract_hash=None,
                next_action="Read-only admission is required.",
                version=version,
            )
        return self._disabled_response(provider, version=version, capability=capability)

    def require_mode_transition(
        self,
        provider: ProviderName,
        mode: IntegrationMode,
        persisted: IntegrationStatus | None,
        capability: CapabilityProjection,
    ) -> str | None:
        if mode == "fixture" and provider != "fosun":
            raise NexusError(
                ErrorCode.FORBIDDEN, "The operation is not permitted.", status_code=403
            )
        if mode != "read_only":
            return None
        if capability.valid is None or not _capability_is_after_invalidation(
            capability.valid, persisted
        ):
            if provider == "fosun":
                raise NexusError(
                    ErrorCode.CAPABILITY_EXPIRED,
                    "A valid capability admission is required.",
                    status_code=409,
                )
            raise NexusError(
                ErrorCode.PROVIDER_UNAVAILABLE, "The provider is unavailable.", status_code=503
            )
        return capability.valid.contract_hash

    async def write_status(
        self,
        provider: ProviderName,
        *,
        mode: IntegrationMode,
        expected_version: int,
        last_success_at: datetime | None,
        admitted_contract_hash: str | None,
    ) -> IntegrationStatus:
        if provider == "fosun":
            await _lock_fosun_broker_capability_scopes(self._session)
        invalidated_at = self._now if mode == "disabled" else None
        if expected_version == 0:
            insert_statement = (
                insert(IntegrationStatus)
                .values(
                    provider=provider,
                    status=mode,
                    last_success_at=last_success_at,
                    version=1,
                    mode_changed_at=self._now,
                    capability_invalidated_at=invalidated_at,
                    admitted_contract_hash=admitted_contract_hash,
                )
                .on_conflict_do_nothing(index_elements=[IntegrationStatus.provider])
                .returning(IntegrationStatus.id)
            )
            write_id = (await self._session.execute(insert_statement)).scalar_one_or_none()
        else:
            update_statement = (
                update(IntegrationStatus)
                .where(
                    IntegrationStatus.provider == provider,
                    IntegrationStatus.version == expected_version,
                )
                .values(
                    status=mode,
                    last_success_at=last_success_at,
                    version=IntegrationStatus.version + 1,
                    mode_changed_at=self._now,
                    capability_invalidated_at=(
                        self._now
                        if mode == "disabled"
                        else IntegrationStatus.capability_invalidated_at
                    ),
                    admitted_contract_hash=admitted_contract_hash,
                )
                .returning(IntegrationStatus.id)
            )
            write_id = (await self._session.execute(update_statement)).scalar_one_or_none()
        if write_id is None:
            raise _version_conflict()
        await self._session.flush()
        status = await self.find_status(provider)
        if status is None:
            raise NexusError(
                ErrorCode.INTERNAL_ERROR, "The request could not be completed.", status_code=500
            )
        return status

    async def refresh_fixture(
        self, provider: ProviderName, *, expected_version: int, last_success_at: datetime
    ) -> IntegrationStatus:
        """CAS-refresh fixture connectivity without changing mode/admission state."""
        if expected_version == 0:
            insert_statement = (
                insert(IntegrationStatus)
                .values(
                    provider=provider,
                    status="fixture",
                    last_success_at=last_success_at,
                    version=1,
                    mode_changed_at=self._now,
                    capability_invalidated_at=None,
                    admitted_contract_hash=None,
                )
                .on_conflict_do_nothing(index_elements=[IntegrationStatus.provider])
                .returning(IntegrationStatus.id)
            )
            write_id = (await self._session.execute(insert_statement)).scalar_one_or_none()
        else:
            update_statement = (
                update(IntegrationStatus)
                .where(
                    IntegrationStatus.provider == provider,
                    IntegrationStatus.version == expected_version,
                )
                .values(
                    last_success_at=last_success_at,
                    version=IntegrationStatus.version + 1,
                )
                .returning(IntegrationStatus.id)
            )
            write_id = (await self._session.execute(update_statement)).scalar_one_or_none()
        if write_id is None:
            raise _version_conflict()
        await self._session.flush()
        status = await self.find_status(provider)
        if status is None:
            raise NexusError(
                ErrorCode.INTERNAL_ERROR, "The request could not be completed.", status_code=500
            )
        return status

    async def claim_or_replay(
        self,
        *,
        actor: VerifiedServiceActor,
        route: str,
        idempotency_key: str,
        request_hash: str,
        response_model: type[_ResponseModel],
    ) -> _ResponseModel | None:
        statement = (
            insert(IdempotencyRecord)
            .values(
                actor_id=actor.actor_id,
                route=route,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                schema_version="1",
                response_payload={},
            )
            .on_conflict_do_nothing(
                index_elements=[
                    IdempotencyRecord.actor_id,
                    IdempotencyRecord.route,
                    IdempotencyRecord.idempotency_key,
                ]
            )
            .returning(IdempotencyRecord.id)
        )
        if (await self._session.execute(statement)).scalar_one_or_none() is not None:
            return None
        record = await self._session.scalar(
            select(IdempotencyRecord).where(
                IdempotencyRecord.actor_id == actor.actor_id,
                IdempotencyRecord.route == route,
                IdempotencyRecord.idempotency_key == idempotency_key,
            )
        )
        if record is None:
            raise NexusError(
                ErrorCode.INTERNAL_ERROR, "The request could not be completed.", status_code=500
            )
        if record.request_hash != request_hash:
            raise NexusError(
                ErrorCode.IDEMPOTENCY_CONFLICT,
                "The idempotency key was already used for a different request.",
                status_code=409,
            )
        return response_model.model_validate(record.response_payload)

    async def complete_idempotency(
        self,
        *,
        actor: VerifiedServiceActor,
        route: str,
        idempotency_key: str,
        response: BaseModel,
    ) -> None:
        await self._session.execute(
            update(IdempotencyRecord)
            .where(
                IdempotencyRecord.actor_id == actor.actor_id,
                IdempotencyRecord.route == route,
                IdempotencyRecord.idempotency_key == idempotency_key,
            )
            .values(response_payload=response.model_dump(mode="json"))
        )

    def request_hash(self, raw_body: bytes, if_match: str) -> str:
        return hashlib.sha256(raw_body + b"\n" + if_match.encode("utf-8")).hexdigest()

    @staticmethod
    def parse_if_match(value: str) -> int:
        candidate = value[1:-1] if value.startswith('"') and value.endswith('"') else value
        try:
            version = int(candidate)
        except ValueError:
            raise NexusError(
                ErrorCode.VALIDATION_ERROR, "The request could not be completed.", status_code=422
            ) from None
        if version < 0 or str(version) != candidate:
            raise NexusError(
                ErrorCode.VALIDATION_ERROR, "The request could not be completed.", status_code=422
            )
        return version

    def _disabled_response(
        self,
        provider: ProviderName,
        *,
        version: int,
        capability: CapabilityProjection,
        quality: Literal["PASS", "PARTIAL", "FAIL"] = "PARTIAL",
        next_action: str = "Provider is disabled.",
    ) -> IntegrationStatusResponse:
        return IntegrationStatusResponse(
            provider=provider,
            mode="disabled",
            quality=quality,
            as_of=None,
            capability_expires_at=capability.expires_at,
            contract_hash=capability.contract_hash,
            next_action=next_action,
            version=version,
        )


def _is_valid_capability(capability: ProviderCapability, now: datetime) -> bool:
    return (
        _CONTRACT_HASH.fullmatch(capability.contract_hash) is not None
        and capability.created_at <= now
        and capability.expires_at > now
    )


async def _lock_fosun_broker_capability_scopes(session: AsyncSession) -> None:
    for operation in _FOSUN_BROKER_OPERATIONS:
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 0))"),
            {"scope": (f"provider-capability-scope:v1:fosun:READ_ONLY:{operation}:BROKER")},
        )


def _capability_is_after_invalidation(
    capability: ProviderCapability, persisted: IntegrationStatus | None
) -> bool:
    return (
        persisted is None
        or persisted.capability_invalidated_at is None
        or capability.created_at > persisted.capability_invalidated_at
    )


def _stored_mode(persisted: IntegrationStatus | None) -> IntegrationMode:
    if persisted is not None and persisted.status in _MODES:
        return cast(IntegrationMode, persisted.status)
    return "fixture" if persisted is None else "disabled"


def _version_conflict() -> NexusError:
    return NexusError(
        ErrorCode.VERSION_CONFLICT, "The resource version is no longer current.", status_code=409
    )
