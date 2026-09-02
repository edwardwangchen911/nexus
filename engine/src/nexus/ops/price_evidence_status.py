"""Redacted local evidence inventory, never a research admission decision."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.persistence.models.data import DataArtifact, PriceBarSetAuthority, PriceSeriesManifest
from nexus.persistence.models.portfolio import PositionSnapshot


async def read_price_evidence_status(
    session: AsyncSession,
    *,
    snapshot_id: UUID,
    snapshot_as_of: datetime | None,
    checked_at: datetime,
) -> dict[str, object]:
    """Inventory latest visible Fosun daily series for held instruments only.

    Manifest labels and stored authority counts are not revalidated price authority.
    In particular, raw metadata cannot prove an omitted upstream adjustment argument.
    """
    held = tuple(
        await session.scalars(
            select(PositionSnapshot.instrument_id)
            .where(PositionSnapshot.portfolio_snapshot_id == snapshot_id)
            .distinct()
        )
    )
    latest = (
        select(
            PriceSeriesManifest.instrument,
            PriceSeriesManifest.delivery_mode,
            PriceSeriesManifest.adjustment,
            PriceSeriesManifest.row_count,
            PriceSeriesManifest.start_at,
            PriceSeriesManifest.end_at,
            func.greatest(
                PriceSeriesManifest.created_at,
                PriceSeriesManifest.provider_authorized_at,
                DataArtifact.created_at,
                DataArtifact.available_at,
                DataArtifact.received_at,
            ).label("visible_at"),
        )
        .join(DataArtifact, DataArtifact.id == PriceSeriesManifest.data_artifact_id)
        .where(
            PriceSeriesManifest.instrument.in_(tuple(map(str, held))),
            PriceSeriesManifest.source == "fosun",
            PriceSeriesManifest.interval == "1Day",
            PriceSeriesManifest.created_at <= checked_at,
            PriceSeriesManifest.end_at <= checked_at,
            PriceSeriesManifest.provider_authorized_at <= checked_at,
            DataArtifact.created_at <= checked_at,
            DataArtifact.available_at <= checked_at,
            DataArtifact.received_at <= checked_at,
        )
        .distinct(PriceSeriesManifest.instrument)
        .order_by(
            PriceSeriesManifest.instrument,
            DataArtifact.available_at.desc(),
            PriceSeriesManifest.created_at.desc(),
            PriceSeriesManifest.id.desc(),
        )
    ).subquery()
    rows = (await session.execute(select(latest))).mappings().all()
    authority_count = await session.scalar(
        select(func.count())
        .select_from(PriceBarSetAuthority)
        .join(
            PriceSeriesManifest,
            PriceSeriesManifest.id == PriceBarSetAuthority.price_series_manifest_id,
        )
        .where(
            PriceSeriesManifest.instrument.in_(tuple(map(str, held))),
            PriceSeriesManifest.source == "fosun",
            PriceBarSetAuthority.available_at <= checked_at,
            PriceBarSetAuthority.created_at <= checked_at,
            PriceSeriesManifest.created_at <= checked_at,
        )
    )
    missing = len(held) - len(rows)
    unknown = sum(row["delivery_mode"] == "unknown" for row in rows)
    raw = sum(row["adjustment"] == "raw" for row in rows)
    after_snapshot = sum(
        snapshot_as_of is not None and row["visible_at"] > snapshot_as_of for row in rows
    )
    reasons = []
    if missing:
        reasons.append("HELD_FOSUN_DAILY_SERIES_MISSING")
    if unknown:
        reasons.append("HISTORY_DELIVERY_UNVERIFIED")
    if raw:
        reasons.append("RAW_SERIES_NOT_CORPORATE_ACTION_ADJUSTED")
    if held and not authority_count:
        reasons.append("NO_STORED_FOSUN_PRICE_AUTHORITY")
    if after_snapshot:
        reasons.append("LATEST_SERIES_NOT_VISIBLE_AT_ACCOUNT_SNAPSHOT")
    latest_bar = max((row["end_at"] for row in rows), default=None)
    return {
        "scope": "held_instruments_fosun_daily",
        "checked_at": checked_at.isoformat(),
        "account_snapshot_as_of": snapshot_as_of.isoformat() if snapshot_as_of else None,
        "held_instruments": len(held),
        "instruments_with_series": len(rows),
        "missing_series": missing,
        "delivery_unverified": unknown,
        "declared_raw_series": raw,
        "latest_series_after_account_snapshot": after_snapshot,
        "stored_authority_records": int(authority_count or 0),
        "authority_revalidated": False,
        "adjustment_request_verified": False,
        "minimum_series_rows": min((row["row_count"] for row in rows), default=0),
        "maximum_series_rows": max((row["row_count"] for row in rows), default=0),
        "latest_bar_at": latest_bar.isoformat() if latest_bar else None,
        "reasons": reasons,
    }
