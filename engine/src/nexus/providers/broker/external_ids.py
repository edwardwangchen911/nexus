"""Append-only persistence and rotation for encrypted broker account identifiers."""

from __future__ import annotations

import hmac
from typing import cast
from uuid import UUID

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.ids import new_uuid7
from nexus.persistence.models.portfolio import Account, AccountExternalIdEnvelope
from nexus.providers.broker.crypto import ExternalIdCipherRing, ExternalIdEnvelope


class ExternalIdEnvelopeError(RuntimeError):
    """The external-ID envelope history is missing or inconsistent."""


class ExternalIdEnvelopeRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def append_current(
        self,
        *,
        account_id: UUID,
        provider: str,
        envelope: ExternalIdEnvelope,
    ) -> AccountExternalIdEnvelope:
        account = await self._session.scalar(
            select(Account).where(Account.id == account_id).with_for_update()
        )
        if account is None or account.provider != provider:
            raise ExternalIdEnvelopeError("broker account provider is inconsistent")
        current = await self.current(account_id, envelope.field_context)
        _validate_transition(current, envelope)
        version = 1 if current is None else current.envelope_version + 1
        record = AccountExternalIdEnvelope(
            id=new_uuid7(),
            account_id=account_id,
            provider=provider,
            field_context=envelope.field_context,
            envelope_version=version,
            aad_version=envelope.aad_version,
            key_version=envelope.key_version,
            nonce=envelope.nonce,
            ciphertext=envelope.ciphertext,
            fingerprint=envelope.fingerprint,
        )
        self._session.add(record)
        await self._session.flush([record])
        return record

    async def current(
        self, account_id: UUID, field_context: str = "external_account_id"
    ) -> AccountExternalIdEnvelope | None:
        return cast(
            AccountExternalIdEnvelope | None,
            await self._session.scalar(
                select(AccountExternalIdEnvelope)
                .where(
                    AccountExternalIdEnvelope.account_id == account_id,
                    AccountExternalIdEnvelope.field_context == field_context,
                )
                .order_by(desc(AccountExternalIdEnvelope.envelope_version))
                .limit(1)
            ),
        )

    async def rewrap_current(
        self,
        *,
        account_id: UUID,
        provider: str,
        keyring: ExternalIdCipherRing,
    ) -> AccountExternalIdEnvelope:
        account = await self._session.scalar(
            select(Account).where(Account.id == account_id).with_for_update()
        )
        if account is None or account.provider != provider:
            raise ExternalIdEnvelopeError("broker account provider is inconsistent")
        current = await self.current(account_id)
        if current is None or current.aad_version != "external_id_aad_v2":
            raise ExternalIdEnvelopeError("current external ID envelope is unavailable")
        envelope = ExternalIdEnvelope(
            key_version=current.key_version,
            aad_version=current.aad_version,
            field_context=current.field_context,
            nonce=current.nonce,
            ciphertext=current.ciphertext,
            fingerprint=current.fingerprint,
        )
        plaintext = keyring.decrypt(envelope, provider=provider, account_id=account_id)
        rewrapped = keyring.current.encrypt(
            plaintext,
            provider=provider,
            account_id=account_id,
            field_context=current.field_context,
        )
        if rewrapped.key_version == current.key_version:
            raise ExternalIdEnvelopeError("external ID is already on the current key")
        return await self.append_current(
            account_id=account_id,
            provider=provider,
            envelope=rewrapped,
        )


def _validate_transition(
    current: AccountExternalIdEnvelope | None,
    envelope: ExternalIdEnvelope,
) -> None:
    if envelope.aad_version != "external_id_aad_v2":
        raise ExternalIdEnvelopeError("new external ID envelopes must use account-bound AAD")
    if current is None:
        return
    if current.aad_version != "external_id_aad_v2":
        raise ExternalIdEnvelopeError("current external ID envelope AAD is unsupported")
    if not hmac.compare_digest(current.fingerprint, envelope.fingerprint):
        raise ExternalIdEnvelopeError("external ID fingerprint identity changed")
