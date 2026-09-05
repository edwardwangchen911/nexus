"""Select the established local market scope without accepting unrelated newer pools."""

from datetime import date, datetime
from typing import cast

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.time import require_utc
from nexus.persistence.models.market import UniverseSnapshot


async def select_market_universe(
    session: AsyncSession, *, effective_session: date, published_by: datetime
) -> UniverseSnapshot | None:
    """Keep session applicability separate from the publication cutoff."""
    cutoff = require_utc(published_by)
    visible = (
        UniverseSnapshot.effective_from <= effective_session,
        UniverseSnapshot.published_at.is_not(None),
        UniverseSnapshot.published_at <= cutoff,
    )
    local = await session.scalar(
        select(UniverseSnapshot)
        .where(
            *visible,
            UniverseSnapshot.key == "fosun_local_holdings",
            UniverseSnapshot.source == "fosun_local_read_only",
        )
        .order_by(
            UniverseSnapshot.published_at.desc(),
            UniverseSnapshot.created_at.desc(),
            UniverseSnapshot.id.desc(),
        )
        .limit(1)
    )
    if local is not None:
        return local
    # (key, version) is unique; other internal research/backtest versions are not a fallback.
    return cast(
        UniverseSnapshot | None,
        await session.scalar(
            select(UniverseSnapshot).where(
                *visible,
                UniverseSnapshot.key == "us_liquid",
                UniverseSnapshot.version == "us_liquid_v1",
            )
        ),
    )
