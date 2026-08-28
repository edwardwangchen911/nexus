"""Point-in-time PostgreSQL loaders for authorized daily and intraday series."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal, cast
from uuid import UUID

from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.time import require_utc
from nexus.market.intraday_authority import (
    HASH_VERSION as INTRADAY_HASH_VERSION,
)
from nexus.market.intraday_authority import (
    POLICY_VERSION as INTRADAY_POLICY_VERSION,
)
from nexus.market.intraday_authority import (
    IntradayBarSetPreparation,
    prepare_intraday_bar_set,
)
from nexus.market.intraday_metrics import (
    IntradayAuthorityView,
    IntradayQuoteView,
    IntradayTradeView,
)
from nexus.market.price_bar_authority import (
    HASH_VERSION as DAILY_HASH_VERSION,
)
from nexus.market.price_bar_authority import (
    POLICY_VERSION as DAILY_POLICY_VERSION,
)
from nexus.market.price_bar_authority import (
    SOURCE_POLICY_ID as DAILY_SOURCE_POLICY_ID,
)
from nexus.market.price_bar_authority import (
    PriceBarSetPreparation,
    prepare_price_bar_set,
)
from nexus.persistence.models.data import (
    DataArtifact,
    IntradayBarSetAuthority,
    PriceBarRecord,
    PriceBarSetAuthority,
    PriceSeriesManifest,
)
from nexus.persistence.models.market import LatestQuote
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.providers.base import InstrumentId, PriceBar, PriceSeries
from nexus.quality.source_policy import (
    INTRADAY_SOURCE_POLICY_ID,
    INTRADAY_SOURCE_POLICY_PAYLOAD,
    load_intraday_source_policy_v1,
)

_MAX_DAILY_CANDIDATES = 16
_MAX_INTRADAY_AUTHORITY_ROWS = 720
_INTRADAY_LOOKBACK = timedelta(minutes=90)


@dataclass(frozen=True)
class _IntradayCandidate:
    source: str
    feed: str
    delivery_mode: str
    bars: tuple[PriceBar, ...]
    latest_authority_at: datetime


async def load_daily_series(
    session: AsyncSession,
    instrument_id: UUID,
    decision_time: datetime,
) -> tuple[PriceSeries, ...]:
    """Load whole, revalidated 1Day candidates visible at decision time."""
    decision = require_utc(decision_time)
    candidates = (
        await session.execute(
            select(PriceBarSetAuthority, PriceSeriesManifest, ResearchEvidenceRef)
            .join(
                PriceSeriesManifest,
                PriceSeriesManifest.id == PriceBarSetAuthority.price_series_manifest_id,
            )
            .join(
                ResearchEvidenceRef,
                ResearchEvidenceRef.id == PriceBarSetAuthority.evidence_ref_id,
            )
            .where(
                PriceSeriesManifest.instrument == str(instrument_id),
                PriceSeriesManifest.interval == "1Day",
                PriceBarSetAuthority.available_at <= decision,
                ResearchEvidenceRef.available_at <= decision,
            )
            .order_by(
                PriceBarSetAuthority.available_at.desc(),
                PriceBarSetAuthority.id.desc(),
            )
            .limit(_MAX_DAILY_CANDIDATES)
        )
    ).all()
    loaded: list[PriceSeries] = []
    for authority, manifest, evidence in candidates:
        prepared = await prepare_price_bar_set(session, manifest.id)
        if prepared is None or not _daily_authority_valid(
            authority,
            manifest,
            evidence,
            prepared,
        ):
            continue
        records = tuple(
            await session.scalars(
                select(PriceBarRecord)
                .where(
                    PriceBarRecord.price_series_manifest_id == manifest.id,
                    PriceBarRecord.available_at <= decision,
                )
                .order_by(
                    PriceBarRecord.session,
                    PriceBarRecord.observed_at,
                    PriceBarRecord.revision,
                    PriceBarRecord.id,
                )
            )
        )
        if len(records) != authority.bar_count:
            continue
        loaded.append(
            PriceSeries(
                source=manifest.source,
                feed=manifest.feed,
                adjustment=manifest.adjustment,
                interval="1Day",
                bars=tuple(_price_bar(record) for record in records),
                mode=_provider_mode(manifest.source, manifest.delivery_mode),
                delivery_mode=cast(
                    Literal["real_time", "delayed", "fixture", "unknown"],
                    manifest.delivery_mode,
                ),
                quality="PASS",
            )
        )
    return tuple(loaded)


async def load_intraday_series(
    session: AsyncSession,
    instrument_id: UUID,
    decision_time: datetime,
) -> IntradayAuthorityView:
    """Load one whole-source intraday view without admitting legacy evidence."""
    decision = require_utc(decision_time)
    try:
        await load_intraday_source_policy_v1(session)
    except ValueError:
        return _empty_intraday(instrument_id, "INTRADAY_SOURCE_POLICY_UNAVAILABLE")

    authority_rows = (
        await session.execute(
            select(
                IntradayBarSetAuthority,
                PriceSeriesManifest,
                ResearchEvidenceRef,
            )
            .join(
                PriceSeriesManifest,
                PriceSeriesManifest.id == IntradayBarSetAuthority.price_series_manifest_id,
            )
            .join(
                ResearchEvidenceRef,
                ResearchEvidenceRef.id == IntradayBarSetAuthority.evidence_ref_id,
            )
            .where(
                PriceSeriesManifest.instrument == str(instrument_id),
                PriceSeriesManifest.interval == "1Min",
                IntradayBarSetAuthority.available_at <= decision,
                IntradayBarSetAuthority.last_observed_at >= decision - _INTRADAY_LOOKBACK,
                ResearchEvidenceRef.available_at <= decision,
            )
            .order_by(
                IntradayBarSetAuthority.available_at.desc(),
                IntradayBarSetAuthority.id.desc(),
            )
            .limit(_MAX_INTRADAY_AUTHORITY_ROWS)
        )
    ).all()
    grouped: dict[tuple[str, str, str], list[tuple[datetime, UUID, PriceBar]]] = defaultdict(list)
    for authority, manifest, evidence in authority_rows:
        prepared = await prepare_intraday_bar_set(session, manifest.id)
        if prepared is None or not _intraday_authority_valid(
            authority,
            manifest,
            evidence,
            prepared,
        ):
            continue
        records = tuple(
            await session.scalars(
                select(PriceBarRecord)
                .where(
                    PriceBarRecord.price_series_manifest_id == manifest.id,
                    PriceBarRecord.available_at <= decision,
                    PriceBarRecord.observed_at >= decision - _INTRADAY_LOOKBACK,
                )
                .order_by(
                    PriceBarRecord.observed_at,
                    PriceBarRecord.revision,
                    PriceBarRecord.id,
                )
            )
        )
        if len(records) != authority.bar_count:
            continue
        key = (manifest.source, manifest.feed, manifest.delivery_mode)
        grouped[key].extend(
            (authority.available_at, authority.id, _price_bar(record)) for record in records
        )

    candidates = tuple(
        _intraday_candidate(key, rows)
        for key, rows in grouped.items()
        if _intraday_provenance_allowed(*key)
    )
    observations = await _latest_intraday_observations(
        session,
        instrument_id=instrument_id,
        decision_time=decision,
    )
    selected_key = _select_intraday_key(candidates, observations)
    if selected_key is None:
        return _empty_intraday(instrument_id, "INTRADAY_DATA_UNAVAILABLE")
    source, feed, delivery_mode = selected_key
    selected = next(
        (candidate for candidate in candidates if _candidate_key(candidate) == selected_key),
        None,
    )
    matching = tuple(
        observation
        for observation in observations
        if (observation.source, observation.feed, observation.delivery_mode) == selected_key
    )
    trade_row = next(
        (
            row
            for row in matching
            if row.event_kind in {"trade", "snapshot", "fixture"} and row.last is not None
        ),
        None,
    )
    quote_row = next(
        (
            row
            for row in matching
            if row.event_kind in {"quote", "snapshot", "fixture"}
            and row.bid is not None
            and row.ask is not None
        ),
        None,
    )
    reasons = _cross_source_reasons(observations)
    return IntradayAuthorityView(
        instrument_id=instrument_id,
        source=source,
        feed=feed,
        delivery_mode=cast(
            Literal["real_time", "delayed", "fixture", "unknown"],
            delivery_mode,
        ),
        bars=() if selected is None else selected.bars[-90:],
        trade=(
            None
            if trade_row is None
            else IntradayTradeView(
                last=cast(Decimal, trade_row.last),
                observed_at=trade_row.observed_at,
                available_at=trade_row.available_at,
                quality_flags=tuple(trade_row.quality_flags),
            )
        ),
        quote=(
            None
            if quote_row is None
            else IntradayQuoteView(
                bid=cast(Decimal, quote_row.bid),
                ask=cast(Decimal, quote_row.ask),
                observed_at=quote_row.observed_at,
                available_at=quote_row.available_at,
                quality_flags=tuple(quote_row.quality_flags),
            )
        ),
        quality_reasons=reasons,
    )


def _daily_authority_valid(
    authority: PriceBarSetAuthority,
    manifest: PriceSeriesManifest,
    evidence: ResearchEvidenceRef,
    prepared: PriceBarSetPreparation,
) -> bool:
    return (
        authority.source_policy_id == DAILY_SOURCE_POLICY_ID
        and authority.hash_version == DAILY_HASH_VERSION
        and authority.policy_version == DAILY_POLICY_VERSION
        and authority.source == manifest.source == evidence.source
        and authority.provider_object_id
        == evidence.provider_object_id
        == f"price-bar-set:{manifest.id}"
        and authority.revision == evidence.revision == prepared.content_hash
        and authority.content_hash == evidence.content_hash == prepared.content_hash
        and authority.byte_length == evidence.byte_length == prepared.byte_length
        and authority.bar_count == prepared.bar_count
        and authority.first_session == prepared.first_session
        and authority.last_session == prepared.last_session
        and authority.available_at == evidence.available_at == prepared.available_at
    )


def _intraday_authority_valid(
    authority: IntradayBarSetAuthority,
    manifest: PriceSeriesManifest,
    evidence: ResearchEvidenceRef,
    prepared: IntradayBarSetPreparation,
) -> bool:
    return (
        authority.source_policy_id == INTRADAY_SOURCE_POLICY_ID
        and authority.hash_version == INTRADAY_HASH_VERSION
        and authority.policy_version == INTRADAY_POLICY_VERSION
        and authority.source == manifest.source == evidence.source
        and authority.provider_object_id
        == evidence.provider_object_id
        == f"intraday-bar-set:{manifest.id}"
        and authority.revision == evidence.revision == prepared.content_hash
        and authority.content_hash == evidence.content_hash == prepared.content_hash
        and authority.byte_length == evidence.byte_length == prepared.byte_length
        and authority.bar_count == prepared.bar_count
        and authority.first_observed_at == prepared.first_observed_at
        and authority.last_observed_at == prepared.last_observed_at
        and authority.available_at == evidence.available_at == prepared.available_at
    )


def _price_bar(record: PriceBarRecord) -> PriceBar:
    return PriceBar(
        instrument_id=InstrumentId(record.instrument_id),
        session=record.session,
        open=record.open,
        high=record.high,
        low=record.low,
        close=record.close,
        volume=record.volume,
        observed_at=record.observed_at,
        available_at=record.available_at,
        received_at=record.received_at,
        revision=record.revision,
        quality_flags=tuple(record.quality_flags),
    )


def _provider_mode(
    source: str,
    delivery_mode: str,
) -> Literal["fixture", "read_only", "delayed"]:
    if source == "fixture":
        return "fixture"
    if delivery_mode == "delayed":
        return "delayed"
    return "read_only"


def _intraday_provenance_allowed(source: str, feed: str, delivery_mode: str) -> bool:
    if source == "fixture":
        return delivery_mode == "fixture"
    if source != "alpaca":
        return False
    alpaca = INTRADAY_SOURCE_POLICY_PAYLOAD["alpaca"]
    assert isinstance(alpaca, dict)
    return feed in alpaca["feeds"] and delivery_mode in alpaca["delivery_modes"]


def _intraday_candidate(
    key: tuple[str, str, str],
    rows: list[tuple[datetime, UUID, PriceBar]],
) -> _IntradayCandidate:
    latest_by_time: dict[datetime, tuple[datetime, UUID, PriceBar]] = {}
    for item in rows:
        observed = item[2].observed_at
        if observed is None:
            continue
        previous = latest_by_time.get(observed)
        if previous is None or (item[0], item[1]) > (previous[0], previous[1]):
            latest_by_time[observed] = item
    selected = tuple(
        item[2]
        for item in sorted(
            latest_by_time.values(), key=lambda item: _bar_observed(item[2])
        )
    )
    return _IntradayCandidate(
        source=key[0],
        feed=key[1],
        delivery_mode=key[2],
        bars=selected,
        latest_authority_at=max(item[0] for item in rows),
    )


def _bar_observed(bar: PriceBar) -> datetime:
    if bar.observed_at is None:
        raise AssertionError("intraday candidate bar must have observed_at")
    return bar.observed_at


async def _latest_intraday_observations(
    session: AsyncSession,
    *,
    instrument_id: UUID,
    decision_time: datetime,
) -> tuple[LatestQuote, ...]:
    ranked = (
        select(
            LatestQuote.id.label("id"),
            func.row_number()
            .over(
                partition_by=(
                    LatestQuote.source,
                    LatestQuote.feed,
                    LatestQuote.delivery_mode,
                    LatestQuote.event_kind,
                ),
                order_by=(
                    LatestQuote.observed_at.desc(),
                    LatestQuote.available_at.desc(),
                    LatestQuote.received_at.desc(),
                    LatestQuote.id.desc(),
                ),
            )
            .label("observation_rank"),
        )
        .outerjoin(DataArtifact, DataArtifact.id == LatestQuote.data_artifact_id)
        .where(
            LatestQuote.instrument_id == instrument_id,
            LatestQuote.available_at <= decision_time,
            LatestQuote.observed_at <= decision_time,
            LatestQuote.event_kind.in_(("trade", "quote", "snapshot", "fixture")),
            or_(
                and_(
                    LatestQuote.evidence_status == "fixture",
                    LatestQuote.source == "fixture",
                    LatestQuote.data_artifact_id.is_(None),
                ),
                and_(
                    LatestQuote.evidence_status == "verified",
                    LatestQuote.source != "fixture",
                    LatestQuote.data_artifact_id.is_not(None),
                    DataArtifact.source == LatestQuote.source,
                    DataArtifact.available_at <= decision_time,
                ),
            ),
        )
        .subquery()
    )
    return tuple(
        await session.scalars(
            select(LatestQuote)
            .join(ranked, ranked.c.id == LatestQuote.id)
            .where(ranked.c.observation_rank == 1)
            .order_by(
                LatestQuote.source,
                LatestQuote.feed,
                LatestQuote.event_kind,
            )
        )
    )


def _select_intraday_key(
    candidates: tuple[_IntradayCandidate, ...],
    observations: tuple[LatestQuote, ...],
) -> tuple[str, str, str] | None:
    candidate_keys = {_candidate_key(candidate) for candidate in candidates}
    observation_keys = {
        (row.source, row.feed, row.delivery_mode)
        for row in observations
        if _intraday_provenance_allowed(row.source, row.feed, row.delivery_mode)
    }
    keys = candidate_keys | observation_keys
    priority = cast(list[str], INTRADAY_SOURCE_POLICY_PAYLOAD["priority"])
    feed_priority = cast(dict[str, object], INTRADAY_SOURCE_POLICY_PAYLOAD["alpaca"])["feeds"]
    assert isinstance(feed_priority, list)
    return min(
        keys,
        key=lambda key: (
            priority.index(key[0]),
            feed_priority.index(key[1]) if key[0] == "alpaca" else 0,
            key,
        ),
        default=None,
    )


def _candidate_key(candidate: _IntradayCandidate) -> tuple[str, str, str]:
    return candidate.source, candidate.feed, candidate.delivery_mode


def _cross_source_reasons(observations: tuple[LatestQuote, ...]) -> tuple[str, ...]:
    latest_by_source: dict[str, LatestQuote] = {}
    for row in observations:
        if row.last is None:
            continue
        previous = latest_by_source.get(row.source)
        if previous is None or row.observed_at > previous.observed_at:
            latest_by_source[row.source] = row
    values = tuple(latest_by_source.values())
    for index, first in enumerate(values):
        for second in values[index + 1 :]:
            if abs(first.observed_at - second.observed_at) > timedelta(seconds=5):
                continue
            assert first.last is not None and second.last is not None
            baseline = min(abs(first.last), abs(second.last))
            if baseline == 0 or abs(first.last - second.last) / baseline > Decimal("0.005"):
                return ("INTRADAY_SOURCE_DIVERGENCE",)
    return ()


def _empty_intraday(instrument_id: UUID, reason: str) -> IntradayAuthorityView:
    return IntradayAuthorityView(
        instrument_id=instrument_id,
        source=None,
        feed=None,
        delivery_mode=None,
        bars=(),
        trade=None,
        quote=None,
        quality_reasons=(reason,),
    )
