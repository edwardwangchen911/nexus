"""First authority assembly uses facts, without a persisted or fabricated decision."""

import asyncio
from datetime import timedelta
from uuid import UUID

import pytest
from nexus.persistence.models.backtest import ExchangeCalendarAuthorityRecord
from nexus.persistence.models.data import PriceBarRecord
from nexus.persistence.models.paper import RiskQueryAuthorityRecord
from nexus.risk.authority import (
    RiskAuthorityInput,
    canonical_query_authority_hash,
    prepare_risk_market_snapshot,
)
from sqlalchemy import event
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from engine.tests.integration.test_risk_authority_pg import (
    CALENDAR_ID,
    DECISION_SESSION,
    DECISION_TIME,
    EXECUTION_SESSION,
    INSTRUMENT_ID,
    QUALITY_ID,
    SOURCE_POLICY_ID,
    UNIVERSE_ID,
    _load,
    _seed_authority,
)


def _request(source_policy_id=SOURCE_POLICY_ID):
    return RiskAuthorityInput(
        decision_session=DECISION_SESSION,
        execution_session=EXECUTION_SESSION,
        decision_time=DECISION_TIME,
        source_policy_id=source_policy_id,
        calendar_authority_id=CALENDAR_ID,
        universe_snapshot_id=UNIVERSE_ID,
        quality_report_id=QUALITY_ID,
    )


async def _prepare(url, request, ids=(INSTRUMENT_ID,)):
    engine = create_async_engine(url)
    try:
        async with async_sessionmaker(engine).begin() as session:
            market = await prepare_risk_market_snapshot(
                session, authority=request, required_instrument_ids=ids
            )
            assert not session.new and not session.dirty and not session.deleted
            return market
    finally:
        await engine.dispose()


@pytest.mark.parametrize("source", ["fixture", "fosun"])
def test_first_prepare_matches_existing_loader_without_decision_input(
    migrated_database_url, source
):
    decision = asyncio.run(_seed_authority(migrated_database_url, price_source=source))
    request = _request(decision.source_policy_id)
    market = asyncio.run(_prepare(migrated_database_url, request))
    assert market == asyncio.run(_load(migrated_database_url, decision))
    assert market.authority_hash == decision.authority_hash
    assert market.authority_hash != "0" * 64
    with pytest.raises(ValueError, match="does not match"):
        asyncio.run(
            _load(migrated_database_url, decision.model_copy(update={"authority_hash": "f" * 64}))
        )


@pytest.mark.parametrize(
    "ids", [(), (INSTRUMENT_ID, INSTRUMENT_ID), (UUID(int=99),), (INSTRUMENT_ID, UUID(int=99))]
)
def test_first_prepare_rejects_missing_extra_or_duplicate_set(migrated_database_url, ids):
    asyncio.run(_seed_authority(migrated_database_url))
    with pytest.raises(ValueError):
        asyncio.run(_prepare(migrated_database_url, _request(), ids))


@pytest.mark.parametrize("kind", ["root", "price", "event"])
def test_first_prepare_rejects_future_root_price_or_event(migrated_database_url, kind):
    # Construct future evidence on insertion. Frozen facts must never be rewritten.
    models = {
        "root": (ExchangeCalendarAuthorityRecord,),
        "price": (PriceBarRecord,),
        "event": (RiskQueryAuthorityRecord,),
    }[kind]

    def future_on_insert(mapper, connection, target):
        field = "published_at" if kind == "root" else "available_at"
        setattr(target, field, DECISION_TIME + timedelta(seconds=1))
        if kind == "price" and isinstance(target, PriceBarRecord):
            target.received_at = target.available_at
        if kind == "event":
            target.content_hash = canonical_query_authority_hash(
                source_policy_id=target.source_policy_id,
                universe_snapshot_id=target.universe_snapshot_id,
                decision_session=target.decision_session,
                kind=target.kind,
                required_instrument_ids=target.required_instrument_ids,
                blocked_instrument_ids=target.blocked_instrument_ids,
                complete=target.complete,
                source_watermark_hash=target.source_watermark_hash,
                available_at=target.available_at,
            )

    for model in models:
        event.listen(model, "before_insert", future_on_insert)
    try:
        asyncio.run(_seed_authority(migrated_database_url))
    finally:
        for model in models:
            event.remove(model, "before_insert", future_on_insert)
    with pytest.raises(ValueError):
        asyncio.run(_prepare(migrated_database_url, _request()))
