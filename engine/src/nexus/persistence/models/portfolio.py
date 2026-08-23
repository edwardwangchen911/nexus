"""Immutable broker account and external identifier facts."""

from uuid import UUID

from sqlalchemy import CheckConstraint, ForeignKey, Integer, LargeBinary, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from nexus.domain.ids import new_uuid7
from nexus.persistence.base import Base, ImmutableCreatedAt


class Account(ImmutableCreatedAt, Base):
    __tablename__ = "accounts"
    __table_args__ = (
        CheckConstraint("status IN ('ACTIVE', 'DISABLED')", name="status_allowed"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    provider: Mapped[str] = mapped_column(String(50))
    status: Mapped[str] = mapped_column(String(20))

    def __repr__(self) -> str:
        return f"Account(id={self.id!r}, provider={self.provider!r}, status={self.status!r})"


class AccountExternalIdEnvelope(ImmutableCreatedAt, Base):
    __tablename__ = "account_external_id_envelopes"
    __table_args__ = (
        CheckConstraint("envelope_version >= 1", name="envelope_version_positive"),
        CheckConstraint("aad_version = 'external_id_aad_v2'", name="aad_version_allowed"),
        UniqueConstraint(
            "account_id",
            "field_context",
            "envelope_version",
            name="uq_account_external_id_envelopes_version",
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    account_id: Mapped[UUID] = mapped_column(
        ForeignKey("accounts.id", name="fk_account_external_id_envelopes_account_id_accounts")
    )
    provider: Mapped[str] = mapped_column(String(50))
    field_context: Mapped[str] = mapped_column(String(100))
    envelope_version: Mapped[int] = mapped_column(Integer)
    aad_version: Mapped[str] = mapped_column(String(50))
    key_version: Mapped[str] = mapped_column(String(100))
    nonce: Mapped[bytes] = mapped_column(LargeBinary)
    ciphertext: Mapped[bytes] = mapped_column(LargeBinary)
    fingerprint: Mapped[str] = mapped_column(String(64))
