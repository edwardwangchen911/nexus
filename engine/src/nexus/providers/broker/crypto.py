"""Broker-worker-only authenticated encryption for external account identifiers."""

import base64
import binascii
import hashlib
import hmac
import os
from dataclasses import dataclass
from uuid import UUID

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

_NONCE_BYTES = 12
_KEY_BYTES = 32


@dataclass(frozen=True, repr=False)
class ExternalIdEnvelope:
    key_version: str
    aad_version: str
    field_context: str
    nonce: bytes
    ciphertext: bytes
    fingerprint: str

    def __repr__(self) -> str:
        return (
            "ExternalIdEnvelope(key_version="
            f"{self.key_version!r}, fingerprint={self.fingerprint!r})"
        )


class ExternalIdCipher:
    """Account-bound AES-256-GCM plus a stable, separately keyed blind index."""

    def __init__(self, key: bytes, *, key_version: str, blind_index_key: bytes) -> None:
        if len(key) != _KEY_BYTES or len(blind_index_key) != _KEY_BYTES:
            raise ValueError("external ID keys must be exactly 32 bytes")
        if not key_version or not key_version.isascii():
            raise ValueError("external ID key version must be non-empty ASCII")
        self._aead = AESGCM(key)
        self._key_digest = hashlib.sha256(key).digest()
        self._blind_index_key = blind_index_key
        self._key_version = key_version

    @classmethod
    def from_base64_key(
        cls,
        encoded_key: str,
        *,
        key_version: str,
        blind_index_key: str,
    ) -> "ExternalIdCipher":
        return cls(
            _decode_key(encoded_key, label="AEAD"),
            key_version=key_version,
            blind_index_key=_decode_key(blind_index_key, label="blind-index"),
        )

    def encrypt(
        self,
        external_id: str,
        *,
        provider: str,
        account_id: UUID,
        field_context: str = "external_account_id",
    ) -> ExternalIdEnvelope:
        plaintext = _external_id_bytes(external_id)
        aad = self._aad(provider, account_id, field_context)
        nonce = os.urandom(_NONCE_BYTES)
        ciphertext = self._aead.encrypt(nonce, plaintext, aad)
        fingerprint = hmac.new(
            self._blind_index_key,
            _blind_index_context(provider, field_context) + b"\x00" + plaintext,
            hashlib.sha256,
        ).hexdigest()
        return ExternalIdEnvelope(
            key_version=self._key_version,
            aad_version="external_id_aad_v2",
            field_context=field_context,
            nonce=nonce,
            ciphertext=ciphertext,
            fingerprint=fingerprint,
        )

    def decrypt(
        self,
        envelope: ExternalIdEnvelope,
        *,
        provider: str,
        account_id: UUID,
    ) -> str:
        if envelope.aad_version != "external_id_aad_v2":
            raise ValueError("external ID AAD version is unsupported")
        if not hmac.compare_digest(envelope.key_version, self._key_version):
            raise ValueError("external ID key version is not loaded")
        plaintext = self._aead.decrypt(
            envelope.nonce,
            envelope.ciphertext,
            self._aad(provider, account_id, envelope.field_context),
        )
        return plaintext.decode("utf-8")

    @property
    def key_version(self) -> str:
        return self._key_version

    @property
    def key_digest(self) -> bytes:
        return self._key_digest

    def _aad(self, provider: str, account_id: UUID, field_context: str) -> bytes:
        if not provider or not provider.isascii():
            raise ValueError("provider must be non-empty ASCII")
        if not field_context or not field_context.isascii():
            raise ValueError("external ID field context must be non-empty ASCII")
        return (
            "nexus:external-id:external_id_aad_v2:"
            f"{provider}:{account_id}:{field_context}:{self._key_version}"
        ).encode()

class ExternalIdCipherRing:
    """Select exactly one loaded AEAD key by the authenticated envelope version."""

    def __init__(
        self,
        *,
        current: ExternalIdCipher,
        previous: ExternalIdCipher | None = None,
    ) -> None:
        if previous is not None and (
            previous.key_version == current.key_version
            or hmac.compare_digest(previous.key_digest, current.key_digest)
        ):
            raise ValueError("external ID key versions and key material must differ")
        self.current = current
        self._by_version = {current.key_version: current}
        if previous is not None:
            self._by_version[previous.key_version] = previous

    def decrypt(self, envelope: ExternalIdEnvelope, *, provider: str, account_id: UUID) -> str:
        cipher = self._by_version.get(envelope.key_version)
        if cipher is None:
            raise ValueError("external ID key version is not loaded")
        if envelope.aad_version != "external_id_aad_v2":
            raise ValueError("external ID AAD version is unsupported")
        return cipher.decrypt(envelope, provider=provider, account_id=account_id)


def _external_id_bytes(value: str) -> bytes:
    encoded = value.encode("utf-8")
    if not encoded or len(encoded) > 512:
        raise ValueError("external ID must contain between 1 and 512 UTF-8 bytes")
    return encoded


def _blind_index_context(provider: str, field_context: str) -> bytes:
    if not provider or not provider.isascii() or not field_context or not field_context.isascii():
        raise ValueError("blind-index context must be non-empty ASCII")
    return f"nexus:external-id-blind-index:v1:{provider}:{field_context}".encode()


def _decode_key(value: str, *, label: str) -> bytes:
    try:
        decoded = base64.b64decode(value, altchars=b"-_", validate=True)
    except (ValueError, binascii.Error) as error:
        raise ValueError(f"external ID {label} key must be valid base64") from error
    if len(decoded) != _KEY_BYTES:
        raise ValueError(f"external ID {label} key must be exactly 32 bytes")
    return decoded
