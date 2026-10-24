"""Read only the exact, persisted Schwab Level One double-sided quote authority."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.time import require_utc
from nexus.monitoring.contracts import PriceObservation
from nexus.persistence.models.data import DataArtifact
from nexus.persistence.models.market import Instrument, LatestQuote


async def read_latest_price(
    session: AsyncSession,
    *,
    instrument_id: UUID,
    now: datetime,
) -> PriceObservation | None:
    now = require_utc(now)
    instrument = await session.get(Instrument, instrument_id)
    if instrument is None or instrument.currency != "USD":
        return None
    eligible = (
        LatestQuote.instrument_id == instrument_id,
        LatestQuote.source == "schwab",
        LatestQuote.feed == "l1",
        LatestQuote.event_kind == "quote",
        LatestQuote.created_at <= now,
        LatestQuote.received_at <= now,
        LatestQuote.available_at <= now,
        LatestQuote.observed_at <= now,
    )
    rows = tuple(
        await session.scalars(
            select(LatestQuote)
            .where(*eligible)
            .order_by(LatestQuote.observed_at.desc(), LatestQuote.id.desc())
            .limit(1)
        )
    )
    if not rows:
        return None
    quote = rows[0]
    conflict = await session.scalar(
        select(
            select(LatestQuote.id)
            .where(
                *eligible,
                LatestQuote.observed_at == quote.observed_at,
                or_(
                    LatestQuote.bid.is_distinct_from(quote.bid),
                    LatestQuote.ask.is_distinct_from(quote.ask),
                ),
            )
            .exists()
        )
    )
    if conflict:
        return None
    artifact = await session.get(DataArtifact, quote.data_artifact_id)
    if (
        artifact is None
        or artifact.source != "schwab"
        or artifact.schema_version != "schwab_level_one_stream_v1"
        or artifact.created_at > now
        or artifact.available_at > now
        or artifact.received_at != quote.received_at
        or quote.quality_flags
    ):
        return None
    return PriceObservation(
        instrument_id=instrument_id,
        quote_id=quote.id,
        artifact_id=artifact.id,
        content_hash=quote.content_hash,
        source=quote.source,
        feed=quote.feed,
        currency=instrument.currency,
        price_basis="unadjusted_bid_ask_midpoint",
        delivery_mode=quote.delivery_mode,
        evidence_status=quote.evidence_status,
        bid=quote.bid,
        ask=quote.ask,
        observed_at=quote.observed_at,
        available_at=quote.available_at,
        received_at=quote.received_at,
    )
