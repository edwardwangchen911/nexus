"""Shared SQLAlchemy metadata and database column conventions."""

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import DateTime, MetaData, Numeric, event, func, inspect
from sqlalchemy.dialects.postgresql import UUID as PostgreSQLUUID
from sqlalchemy.engine import Connection
from sqlalchemy.orm import DeclarativeBase, Mapped, Mapper, mapped_column
from sqlalchemy.types import TypeDecorator

from nexus.domain.time import require_utc

NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

DECIMAL_NUMERIC = Numeric(38, 12)


class UTCDateTime(TypeDecorator[datetime]):
    """PostgreSQL TIMESTAMPTZ that accepts and returns only canonical UTC instants."""

    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value: datetime | None, dialect: object) -> datetime | None:
        """Reject ambiguous values before they can reach the database."""
        del dialect
        if value is None:
            return None
        return require_utc(value)

    def process_result_value(self, value: datetime | None, dialect: object) -> datetime | None:
        """Canonicalize driver-returned timezone values at the persistence boundary."""
        del dialect
        if value is None:
            return None
        return require_utc(value)


class Base(DeclarativeBase):
    """Declarative base with deterministic names for schema objects."""

    metadata = MetaData(naming_convention=NAMING_CONVENTION)
    type_annotation_map = {
        UUID: PostgreSQLUUID(as_uuid=True),
        Decimal: DECIMAL_NUMERIC,
    }


class ImmutableCreatedAt:
    """Rows retain their original creation instant for auditability."""

    created_at: Mapped[datetime] = mapped_column(
        UTCDateTime(),
        nullable=False,
        server_default=func.now(),
    )


@event.listens_for(Base, "before_update", propagate=True)
def reject_created_at_update(
    mapper: Mapper[Base], connection: Connection, target: Base
) -> None:
    """Fail fast before the unit of work can rewrite an audit creation instant."""
    del mapper, connection
    created_at_changed = isinstance(target, ImmutableCreatedAt) and (
        inspect(target).attrs.created_at.history.has_changes()
    )
    if created_at_changed:
        raise ValueError("created_at is immutable")
