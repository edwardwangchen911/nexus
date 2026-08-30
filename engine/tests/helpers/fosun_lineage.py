"""Shared deterministic Fosun daily transport-lineage fixtures."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from hashlib import sha256
from uuid import UUID

from nexus.domain.ids import new_uuid7
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import DataArtifact
from sqlalchemy.ext.asyncio import AsyncSession


@dataclass(frozen=True)
class FosunDailyLineage:
    raw_artifact: DataArtifact
    calendar_authority_id: UUID
    request_start_at: datetime
    request_end_at: datetime
    raw_digest: str


async def seed_fosun_daily_lineage(
    session: AsyncSession,
    *,
    manifest_id: UUID,
    provider_authorized_at: datetime,
    sessions: Sequence[date],
    raw_payload: bytes | None = None,
    received_at: datetime | None = None,
    session_closes: Mapping[date, datetime] | None = None,
    calendar_range_end: date | None = None,
) -> FosunDailyLineage:
    """Create the exact raw/calendar facts required by migration 0029."""
    ordered_sessions = tuple(sorted(set(sessions)))
    if not ordered_sessions:
        raise ValueError("FOSUN_TEST_SESSIONS_REQUIRED")
    if provider_authorized_at.tzinfo is None:
        raise ValueError("FOSUN_TEST_AUTHORITY_MUST_BE_AWARE")
    payload = raw_payload or f'{{"manifest_id":"{manifest_id}"}}'.encode()
    archived_at = received_at or provider_authorized_at - timedelta(seconds=1)
    if archived_at > provider_authorized_at:
        raise ValueError("FOSUN_TEST_RAW_AFTER_AUTHORITY")
    request_end_at = datetime.combine(ordered_sessions[-1], time.min, tzinfo=UTC)
    request_start_at = request_end_at - timedelta(days=400)
    raw_digest = sha256(payload).hexdigest()
    raw_artifact = DataArtifact(
        id=new_uuid7(),
        uri=f"memory://raw/fosun/{raw_digest}.json",
        sha256=raw_digest,
        byte_size=len(payload),
        mime_type="application/json",
        schema_version="fosun_gateway_market_bars_v1",
        source="fosun",
        observed_at=archived_at,
        available_at=archived_at,
        received_at=archived_at,
        created_at=archived_at,
    )
    calendar_authority_id = new_uuid7()
    end = calendar_range_end or (
        provider_authorized_at.date() + timedelta(days=366)
    )
    session.add(raw_artifact)
    session.add(
        ExchangeCalendarAuthorityRecord(
            id=calendar_authority_id,
            exchange="XNYS",
            version=f"fosun-lineage-{manifest_id}",
            source="fixture",
            range_start=request_start_at.date(),
            range_end=max(end, request_end_at.date()),
            query_parameters={},
            completeness_status="complete",
            content_hash=sha256(f"calendar:{manifest_id}".encode()).hexdigest(),
            published_at=archived_at - timedelta(days=1),
            created_at=archived_at - timedelta(days=1),
        )
    )
    closes = session_closes or {}
    for session_date in ordered_sessions:
        close = closes.get(
            session_date,
            datetime.combine(session_date, time(20), tzinfo=UTC),
        )
        if close > provider_authorized_at - timedelta(minutes=30):
            close = provider_authorized_at - timedelta(minutes=31)
        if close.date() != session_date:
            raise ValueError("FOSUN_TEST_SESSION_CLOSE_MISMATCH")
        session.add(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=calendar_authority_id,
                session=session_date,
                decision_at=close + timedelta(minutes=1),
                market_open_at=close - timedelta(hours=6, minutes=30),
                market_close_at=close,
                created_at=archived_at - timedelta(days=1),
            )
        )
    await session.flush()
    return FosunDailyLineage(
        raw_artifact=raw_artifact,
        calendar_authority_id=calendar_authority_id,
        request_start_at=request_start_at,
        request_end_at=request_end_at,
        raw_digest=raw_digest,
    )
