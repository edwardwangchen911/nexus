import base64
from uuid import UUID

import pytest
from cryptography.exceptions import InvalidTag
from nexus.providers.broker.crypto import (
    ExternalIdCipher,
    ExternalIdCipherRing,
    ExternalIdEnvelope,
)

KEY = base64.urlsafe_b64encode(b"s" * 32).decode()
BLIND_KEY = base64.urlsafe_b64encode(b"b" * 32).decode()
ACCOUNT_ID = UUID("0198d400-2000-7000-8000-000000000001")
OTHER_ACCOUNT_ID = UUID("0198d400-2000-7000-8000-000000000002")


def test_external_ids_use_randomized_authenticated_encryption_and_safe_repr() -> None:
    cipher = ExternalIdCipher.from_base64_key(
        KEY, key_version="synthetic-v1", blind_index_key=BLIND_KEY
    )

    first = cipher.encrypt("synthetic-account-001", provider="fosun", account_id=ACCOUNT_ID)
    second = cipher.encrypt("synthetic-account-001", provider="fosun", account_id=ACCOUNT_ID)

    assert first.ciphertext != second.ciphertext
    assert first.nonce != second.nonce
    assert first.fingerprint == second.fingerprint
    assert cipher.decrypt(first, provider="fosun", account_id=ACCOUNT_ID) == "synthetic-account-001"
    assert "synthetic-account-001" not in repr(first)
    assert first.ciphertext.hex() not in repr(first)


def test_external_id_ciphertext_is_bound_to_provider_aad() -> None:
    cipher = ExternalIdCipher.from_base64_key(
        KEY, key_version="synthetic-v1", blind_index_key=BLIND_KEY
    )
    encrypted = cipher.encrypt("synthetic-account-001", provider="fosun", account_id=ACCOUNT_ID)

    with pytest.raises(InvalidTag):
        cipher.decrypt(encrypted, provider="another-provider", account_id=ACCOUNT_ID)
    with pytest.raises(InvalidTag):
        cipher.decrypt(encrypted, provider="fosun", account_id=OTHER_ACCOUNT_ID)


def test_keyring_rejects_non_account_bound_aad() -> None:
    cipher = ExternalIdCipher.from_base64_key(
        KEY, key_version="synthetic-v1", blind_index_key=BLIND_KEY
    )
    encrypted = cipher.encrypt("synthetic-account-001", provider="fosun", account_id=ACCOUNT_ID)
    unsupported = ExternalIdEnvelope(
        key_version=encrypted.key_version,
        aad_version="external_id_aad_v1",
        field_context=encrypted.field_context,
        nonce=encrypted.nonce,
        ciphertext=encrypted.ciphertext,
        fingerprint=encrypted.fingerprint,
    )

    with pytest.raises(ValueError, match="unsupported"):
        ExternalIdCipherRing(current=cipher).decrypt(
            unsupported,
            provider="fosun",
            account_id=ACCOUNT_ID,
        )


@pytest.mark.parametrize("key", [b"short", b"x" * 31, b"x" * 33])
def test_external_id_cipher_rejects_non_aes256_keys(key: bytes) -> None:
    encoded = base64.urlsafe_b64encode(key).decode()

    with pytest.raises(ValueError, match="32 bytes"):
        ExternalIdCipher.from_base64_key(
            encoded, key_version="synthetic-v1", blind_index_key=BLIND_KEY
        )


def test_external_id_cipher_ring_decrypts_current_and_previous_versions() -> None:
    current = ExternalIdCipher.from_base64_key(
        KEY, key_version="synthetic-v2", blind_index_key=BLIND_KEY
    )
    previous = ExternalIdCipher.from_base64_key(
        base64.urlsafe_b64encode(b"p" * 32).decode(),
        key_version="synthetic-v1",
        blind_index_key=BLIND_KEY,
    )
    ring = ExternalIdCipherRing(current=current, previous=previous)

    assert (
        ring.decrypt(
            previous.encrypt("synthetic-account-001", provider="fosun", account_id=ACCOUNT_ID),
            provider="fosun",
            account_id=ACCOUNT_ID,
        )
        == "synthetic-account-001"
    )
    assert ring.current is current

    unknown = ExternalIdCipher.from_base64_key(
        base64.urlsafe_b64encode(b"u" * 32).decode(),
        key_version="synthetic-v0",
        blind_index_key=BLIND_KEY,
    )
    with pytest.raises(ValueError, match="not loaded"):
        ring.decrypt(
            unknown.encrypt("synthetic-account-001", provider="fosun", account_id=ACCOUNT_ID),
            provider="fosun",
            account_id=ACCOUNT_ID,
        )


def test_fingerprint_is_stable_across_aead_rotation_but_context_scoped() -> None:
    old = ExternalIdCipher.from_base64_key(
        base64.urlsafe_b64encode(b"o" * 32).decode(),
        key_version="synthetic-v1",
        blind_index_key=BLIND_KEY,
    )
    current = ExternalIdCipher.from_base64_key(
        KEY, key_version="synthetic-v2", blind_index_key=BLIND_KEY
    )

    old_envelope = old.encrypt("synthetic-account-001", provider="fosun", account_id=ACCOUNT_ID)
    rewrapped = current.encrypt("synthetic-account-001", provider="fosun", account_id=ACCOUNT_ID)
    different_field = current.encrypt(
        "synthetic-account-001",
        provider="fosun",
        account_id=ACCOUNT_ID,
        field_context="secondary_external_id",
    )

    assert old_envelope.fingerprint == rewrapped.fingerprint
    assert different_field.fingerprint != rewrapped.fingerprint
