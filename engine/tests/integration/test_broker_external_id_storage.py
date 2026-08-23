import base64
from datetime import UTC, datetime
from uuid import UUID

import pytest
from helpers.broker_storage import _seed_accounts
from nexus.persistence.models.portfolio import AccountExternalIdEnvelope
from nexus.persistence.session import session_factory
from nexus.providers.broker.crypto import ExternalIdCipher, ExternalIdCipherRing
from nexus.providers.broker.external_ids import (
    ExternalIdEnvelopeError,
    ExternalIdEnvelopeRepository,
)
from nexus.settings import EngineSettings
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError

NOW = datetime(2026, 8, 23, 4, 0, tzinfo=UTC)
ACCOUNT_A = UUID("0198d400-5000-7000-8000-000000000001")
ACCOUNT_B = UUID("0198d400-5000-7000-8000-000000000002")
PORTFOLIO_A = UUID("0198d400-5000-7000-8000-000000000003")
PORTFOLIO_B = UUID("0198d400-5000-7000-8000-000000000004")
OLD_KEY = base64.urlsafe_b64encode(b"o" * 32).decode()
CURRENT_KEY = base64.urlsafe_b64encode(b"c" * 32).decode()
BLIND_KEY = base64.urlsafe_b64encode(b"b" * 32).decode()


@pytest.mark.asyncio
async def test_external_id_rewrap_appends_version_and_rejects_cross_account_fingerprint(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_accounts(factory)
    old = ExternalIdCipher.from_base64_key(
        OLD_KEY, key_version="synthetic-v1", blind_index_key=BLIND_KEY
    )
    current = ExternalIdCipher.from_base64_key(
        CURRENT_KEY, key_version="synthetic-v2", blind_index_key=BLIND_KEY
    )
    ring = ExternalIdCipherRing(current=current, previous=old)
    first = old.encrypt("synthetic-account-shared", provider="fosun", account_id=ACCOUNT_A)
    async with factory() as session:
        async with session.begin():
            stored_first = await ExternalIdEnvelopeRepository(session).append_current(
                account_id=ACCOUNT_A,
                provider="fosun",
                envelope=first,
            )
            stored_second = await ExternalIdEnvelopeRepository(session).rewrap_current(
                account_id=ACCOUNT_A,
                provider="fosun",
                keyring=ring,
            )
            assert stored_first.envelope_version == 1
            assert stored_second.envelope_version == 2
            assert stored_first.fingerprint == stored_second.fingerprint
            assert stored_first.ciphertext != stored_second.ciphertext
    conflicting = current.encrypt(
        "synthetic-account-shared", provider="fosun", account_id=ACCOUNT_B
    )
    async with factory() as session:
        with pytest.raises(DBAPIError, match="another account"):
            async with session.begin():
                await ExternalIdEnvelopeRepository(session).append_current(
                    account_id=ACCOUNT_B,
                    provider="fosun",
                    envelope=conflicting,
                )
    async with factory() as session:
        count = await session.scalar(select(func.count()).select_from(AccountExternalIdEnvelope))
    assert count == 2
    async with factory() as session:
        with pytest.raises(DBAPIError, match="immutable"):
            await session.execute(
                text(
                    "UPDATE account_external_id_envelopes SET ciphertext = :value "
                    "WHERE account_id = :account_id"
                ),
                {"value": b"tampered", "account_id": ACCOUNT_A},
            )
            await session.commit()


@pytest.mark.asyncio
async def test_repository_rejects_same_account_field_identity_change(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_accounts(factory, include_second=False)
    cipher = ExternalIdCipher.from_base64_key(
        CURRENT_KEY, key_version="synthetic-v2", blind_index_key=BLIND_KEY
    )
    first = cipher.encrypt("synthetic-account-a", provider="fosun", account_id=ACCOUNT_A)
    different = cipher.encrypt("synthetic-account-b", provider="fosun", account_id=ACCOUNT_A)
    async with factory() as session:
        async with session.begin():
            await ExternalIdEnvelopeRepository(session).append_current(
                account_id=ACCOUNT_A,
                provider="fosun",
                envelope=first,
            )
    async with factory() as session:
        with pytest.raises(ExternalIdEnvelopeError, match="identity changed"):
            async with session.begin():
                await ExternalIdEnvelopeRepository(session).append_current(
                    account_id=ACCOUNT_A,
                    provider="fosun",
                    envelope=different,
                )
    async with factory() as session:
        count = await session.scalar(select(func.count()).select_from(AccountExternalIdEnvelope))
    assert count == 1


@pytest.mark.asyncio
async def test_database_rejects_identity_change_and_version_gap_without_adding_rows(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_accounts(factory, include_second=False)
    cipher = ExternalIdCipher.from_base64_key(
        CURRENT_KEY, key_version="synthetic-v2", blind_index_key=BLIND_KEY
    )
    first = cipher.encrypt("synthetic-account-a", provider="fosun", account_id=ACCOUNT_A)
    different = cipher.encrypt("synthetic-account-b", provider="fosun", account_id=ACCOUNT_A)
    async with factory() as session:
        async with session.begin():
            await ExternalIdEnvelopeRepository(session).append_current(
                account_id=ACCOUNT_A,
                provider="fosun",
                envelope=first,
            )

    insert_sql = text(
        "INSERT INTO account_external_id_envelopes "
        "(id, account_id, provider, field_context, envelope_version, aad_version, "
        "key_version, nonce, ciphertext, fingerprint) VALUES "
        "(:id, :account_id, 'fosun', :field_context, :version, :aad_version, "
        ":key_version, :nonce, :ciphertext, :fingerprint)"
    )
    attempts = (
        (
            2,
            different,
            "external ID fingerprint identity changed",
        ),
        (
            3,
            first,
            "external ID envelope version is not append-only",
        ),
    )
    for index, (version, envelope, message) in enumerate(attempts, start=1):
        async with factory() as session:
            with pytest.raises(DBAPIError, match=message):
                async with session.begin():
                    await session.execute(
                        insert_sql,
                        {
                            "id": UUID(f"0198d400-5000-7000-8000-00000000010{index}"),
                            "account_id": ACCOUNT_A,
                            "field_context": envelope.field_context,
                            "version": version,
                            "aad_version": envelope.aad_version,
                            "key_version": envelope.key_version,
                            "nonce": envelope.nonce,
                            "ciphertext": envelope.ciphertext,
                            "fingerprint": envelope.fingerprint,
                        },
                    )

    async with factory() as session:
        count = await session.scalar(select(func.count()).select_from(AccountExternalIdEnvelope))
    assert count == 1
