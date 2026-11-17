"""Append fresh synthetic quotes and one risk fact per new session scope."""

from hashlib import sha256
from uuid import uuid4

from sqlalchemy import select


async def append_session_fresh_inputs(
    factory, instrument_id, at, *, midpoint=None, decision_session=None
):
    from nexus.market.intraday_ingestion import canonical_quote_observation
    from nexus.persistence.models.data import DataArtifact
    from nexus.persistence.models.market import LatestQuote
    from nexus.persistence.models.paper import RiskQueryAuthorityRecord
    from nexus.risk.authority import canonical_query_authority_hash

    async with factory() as session, session.begin():
        old = await session.scalar(
            select(LatestQuote)
            .where(LatestQuote.instrument_id == instrument_id, LatestQuote.event_kind == "quote")
            .order_by(LatestQuote.received_at.desc())
            .limit(1)
        )
        artifact = DataArtifact(
            id=uuid4(),
            uri="memory://synthetic/monitor-fresh",
            sha256=sha256(f"synthetic:{instrument_id}:{at.isoformat()}".encode()).hexdigest(),
            byte_size=100,
            mime_type="application/json",
            schema_version="schwab_level_one_stream_v1",
            source="schwab",
            observed_at=at,
            available_at=at,
            received_at=at,
            created_at=at,
        )
        session.add(artifact)
        await session.flush()
        values = {
            c.name: getattr(old, c.name)
            for c in old.__table__.columns
            if c.name not in {"id", "created_at", "content_hash"}
        }
        values.update(
            observed_at=at,
            available_at=at,
            received_at=at,
            data_artifact_id=artifact.id,
            revision=artifact.sha256,
        )
        if midpoint is not None:
            values.update(bid=midpoint, ask=midpoint)
        raw = canonical_quote_observation(
            instrument_id=instrument_id,
            source="schwab",
            feed="l1",
            delivery_mode="real_time",
            event_kind="quote",
            bid=values["bid"],
            ask=values["ask"],
            last=None,
            observed_at=at,
            available_at=at,
            received_at=at,
            revision=artifact.sha256,
        )
        session.add(LatestQuote(**values, content_hash=sha256(raw).hexdigest(), created_at=at))
        risks = tuple(
            await session.scalars(
                select(RiskQueryAuthorityRecord).where(
                    RiskQueryAuthorityRecord.required_instrument_ids == [instrument_id]
                )
            )
        )
        # Repeated preparation appends one new fact per authority scope; prior
        # snapshots stay immutable and are not duplicated at the new timestamp.
        latest = {}
        for risk in risks:
            key = (
                risk.source_policy_id,
                risk.universe_snapshot_id,
                decision_session or risk.decision_session,
                risk.kind,
                tuple(risk.required_instrument_ids),
            )
            if key not in latest or risk.available_at > latest[key].available_at:
                latest[key] = risk
        for risk in latest.values():
            values = {
                c.name: getattr(risk, c.name)
                for c in risk.__table__.columns
                if c.name not in {"id", "created_at", "content_hash"}
            }
            values["available_at"] = at
            if decision_session is not None:
                values["decision_session"] = decision_session
            digest = canonical_query_authority_hash(
                **{
                    k: values[k]
                    for k in (
                        "source_policy_id",
                        "universe_snapshot_id",
                        "decision_session",
                        "kind",
                        "required_instrument_ids",
                        "blocked_instrument_ids",
                        "complete",
                        "source_watermark_hash",
                        "available_at",
                    )
                }
            )
            session.add(RiskQueryAuthorityRecord(**values, content_hash=digest, created_at=at))


async def candidate_input_boundary(factory, settings, instrument_id, now, calendar_id):
    """Read the normal candidate gate and the prior daily authority it requires."""
    from nexus.monitoring.observations import read_latest_price
    from nexus.paper.agent_cycle_authority import (
        load_quasi_realtime_cycle_authority,
        prepare_candidates,
    )
    from nexus.paper.candidate_batch import load_agent_paper_candidate_batch
    from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
    from nexus.persistence.models.data import (
        PriceBarRecord,
        PriceBarSetAuthority,
        PriceSeriesManifest,
    )
    from nexus.persistence.models.paper import PAPER_DECISION_SOURCE_POLICY_ID
    from sqlalchemy import func

    async with factory() as db:
        batch = await load_agent_paper_candidate_batch(
            db, batch_id=settings.agent_paper_candidate_batch_id
        )
        authority = await load_quasi_realtime_cycle_authority(
            db,
            account_id=settings.agent_paper_account_id,
            now=now,
            expected_calendar_authority_id=calendar_id,
            expected_universe_snapshot_id=batch.universe_snapshot_id,
        )
        candidates = await prepare_candidates(
            db, authority=authority, requested_instrument_ids=(instrument_id,), max_instruments=1
        )
        quote = await read_latest_price(db, instrument_id=instrument_id, now=now)
        prior = await db.scalar(
            select(ExchangeCalendarSessionRecord.session)
            .where(
                ExchangeCalendarSessionRecord.calendar_authority_id == calendar_id,
                ExchangeCalendarSessionRecord.session < authority.decision_session,
            )
            .order_by(ExchangeCalendarSessionRecord.session.desc())
            .limit(1)
        )
        daily = await db.scalar(
            select(func.count())
            .select_from(PriceBarSetAuthority)
            .join(
                PriceSeriesManifest,
                PriceSeriesManifest.id == PriceBarSetAuthority.price_series_manifest_id,
            )
            .join(PriceBarRecord, PriceBarRecord.price_series_manifest_id == PriceSeriesManifest.id)
            .where(
                PriceBarSetAuthority.source_policy_id == PAPER_DECISION_SOURCE_POLICY_ID,
                PriceBarSetAuthority.available_at <= now,
                PriceBarSetAuthority.created_at <= now,
                PriceBarRecord.instrument_id == instrument_id,
                PriceBarRecord.session == prior,
                PriceBarRecord.available_at <= now,
                PriceBarRecord.created_at <= now,
                PriceSeriesManifest.created_at <= now,
                PriceSeriesManifest.source == "alpaca",
                PriceSeriesManifest.feed == "sip",
                PriceSeriesManifest.delivery_mode == "delayed",
                PriceSeriesManifest.adjustment == "all",
                PriceSeriesManifest.interval == "1Day",
            )
        )
        return dict(
            candidate_count=len(candidates),
            prior_session=prior,
            daily_authorities=daily,
            quote_present=quote is not None,
            same_quote=bool(
                candidates
                and quote is not None
                and candidates[0].decision_quote_id == quote.quote_id
            ),
        )


async def append_prior_session_daily(factory, monkeypatch, instrument_id, prior_session):
    """Author new synthetic Alpaca input, then use the unchanged production finalizer."""
    from datetime import UTC, datetime, time, timedelta

    from helpers import price_bar_authority as prices
    from nexus.domain.ids import new_uuid7
    from nexus.market.price_bar_authority import finalize_price_bar_set
    from nexus.persistence.models.data import PriceSeriesManifest
    from nexus.persistence.models.paper import PAPER_DECISION_SOURCE_POLICY_ID

    close = datetime.combine(prior_session, time(21), tzinfo=UTC)
    with monkeypatch.context() as dated:
        dated.setattr(prices, "INSTRUMENT_ID", instrument_id)
        dated.setattr(prices, "OBSERVED_AT", close)
        dated.setattr(prices, "AVAILABLE_AT", close + timedelta(minutes=15))
        dated.setattr(prices, "RECEIVED_AT", close + timedelta(minutes=15, seconds=1))
        raw = await prices.seed_price_candidate(factory, manifest_id=new_uuid7())
    async with factory() as db, db.begin():
        manifest = await db.get(PriceSeriesManifest, raw.manifest_id)
        result = await finalize_price_bar_set(
            db, manifest, source_policy_id=PAPER_DECISION_SOURCE_POLICY_ID
        )
        assert result is not None
        authority, _ = result
        assert authority.first_session == authority.last_session == prior_session
        return authority.id
