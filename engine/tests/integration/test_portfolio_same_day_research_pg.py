"""The normal Schwab same-session producer must admit an entire frozen batch."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from helpers.paper_batch import CALENDAR_ID, DECISION_TIME, INSTRUMENTS, seed_paper_batch
from integration.test_agent_paper_cycle_pg import _ObjectStore, _seed_two_candidate_authority
from nexus.agents.paper_context import (
    prepare_agent_paper_research_snapshot,
    read_agent_paper_research_context,
)
from nexus.paper import agent_cycle
from nexus.paper.agent_cycle_authority import (
    load_quasi_realtime_cycle_authority,
    prepare_candidates,
)
from nexus.paper.agent_cycle_models import candidate_manifest_hash, floor_decision_slot
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.paper.projection import load_paper_projection
from nexus.paper.service import create_paper_order_batch
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.paper import (
    PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
    AgentPaperCycleRecord,
    PaperOrderRecord,
    StrategyDecisionRecord,
)
from nexus.persistence.models.runtime import Job
from nexus.risk.authority import prepare_schwab_realtime_risk_snapshot
from nexus.risk.canonical import canonical_risk_market_authority_hash
from nexus.strategy.forward import load_forward_strategy_decision
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

NOW = datetime(2026, 8, 27, 14, tzinfo=UTC)


async def seed_normal_path(url, monkeypatch, *, occupied=False):
    seed = await seed_paper_batch(url)
    engine = create_async_engine(url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    monkeypatch.setattr(
        "nexus.paper.service.current_paper_order_price_authority_available", lambda: True
    )
    async with sessions.begin() as session:
        if occupied:
            batch = await create_paper_order_batch(
                session,
                account_id=seed.account_id,
                strategy_decision_id=seed.decision_id,
                idempotency_key="existing-plan",
                request_time=seed.request_time,
            )
            assert batch.order_ids
        registered = await load_forward_strategy_decision(
            session,
            account_id=seed.account_id,
            strategy_version="baseline_v1",
            decision_session=DECISION_TIME.date(),
        )
        await _seed_two_candidate_authority(
            session,
            registered,
            decision_time=NOW,
            source_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
        )
        # Persist synthetic authorities before the chosen historical cutoff. This
        # never changes runtime facts; fixtures live in an isolated test database.
        for table in (
            "source_policies",
            "instrument_aliases",
            "price_series_manifests",
            "price_bar_records",
            "price_bar_set_authorities",
            "research_evidence_refs",
            "risk_query_authorities",
            "sector_exposures",
            "agent_data_snapshot_evidence",
        ):
            await session.execute(text(f"ALTER TABLE {table} DISABLE TRIGGER USER"))
            try:
                await session.execute(
                    text(f"UPDATE {table} SET created_at=:at WHERE created_at>:at"), {"at": NOW}
                )
            finally:
                await session.execute(text(f"ALTER TABLE {table} ENABLE TRIGGER USER"))
        for table in ("exchange_calendar_authorities", "universe_snapshots"):
            await session.execute(text(f"ALTER TABLE {table} DISABLE TRIGGER USER"))
            try:
                await session.execute(
                    text(f"UPDATE {table} SET published_at=:at"), {"at": NOW - timedelta(days=2)}
                )
            finally:
                await session.execute(text(f"ALTER TABLE {table} ENABLE TRIGGER USER"))
        authority = await load_quasi_realtime_cycle_authority(
            session,
            account_id=seed.account_id,
            now=NOW,
            expected_calendar_authority_id=CALENDAR_ID,
        )
        assert authority.decision_session == authority.execution_session == NOW.date()
        manifest = await prepare_candidates(
            session,
            authority=authority,
            requested_instrument_ids=(INSTRUMENTS[1][0], INSTRUMENTS[0][0]),
        )
        assert len(manifest) == 2
    return engine, sessions, authority, manifest


def frozen_cycle(authority, manifest):
    return AgentPaperCycleRecord(
        id=uuid4(),
        account_id=authority.account_id,
        portfolio_id=authority.portfolio_id,
        decision_session=authority.decision_session,
        decision_time=authority.decision_time,
        decision_slot_at=floor_decision_slot(authority.decision_time),
        execution_session=authority.execution_session,
        universe_snapshot_id=authority.universe_snapshot_id,
        quality_report_id=None,
        decision_price_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
        execution_price_policy_id=PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
        candidate_manifest=[entry.model_dump(mode="json") for entry in manifest],
        candidate_hash=candidate_manifest_hash(manifest),
        sizing_policy_version="portfolio_allocation_v1",
        sizing_policy_hash="a" * 64,
        execution_locked=True,
    )


@pytest.mark.asyncio
async def test_normal_schwab_snapshot_binds_every_candidate(migrated_database_url, monkeypatch):
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url, monkeypatch
    )
    try:
        async with sessions() as session:
            market = await prepare_schwab_realtime_risk_snapshot(
                session, cycle=frozen_cycle(authority, manifest)
            )
            reverse = await prepare_schwab_realtime_risk_snapshot(
                session, cycle=frozen_cycle(authority, tuple(reversed(manifest)))
            )
            assert market == reverse
            assert market.required_instrument_ids == tuple(
                entry.instrument_id for entry in manifest
            )
            assert market.authority_hash == canonical_risk_market_authority_hash(market)
            assert len(market.query_proofs) == 6
            for entry in manifest:
                assert {
                    proof.authority_id
                    for proof in market.query_proofs
                    if proof.required_instrument_ids == (entry.instrument_id,)
                } == set(entry.risk_query_authority_ids)
                assert (
                    next(
                        price
                        for price in market.prices
                        if price.instrument_id == entry.instrument_id
                    ).authority_id
                    == entry.prior_daily_price_authority_id
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_later_single_candidate_risk_scope_includes_inherited_instruments(
    migrated_database_url,
    monkeypatch,
):
    from nexus.market.ingestion import ingest_price_page
    from nexus.persistence.models.data import PriceBarRecord
    from nexus.persistence.models.market import LatestQuote
    from nexus.persistence.models.paper import (
        PAPER_DECISION_SOURCE_POLICY_ID,
        RiskQueryAuthorityRecord,
    )
    from nexus.providers.base import MarketDataProvenance, PriceBar, ProviderPage
    from nexus.risk.authority import canonical_query_authority_hash
    from nexus.strategy.forward import load_forward_strategy_decision

    seed = await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            registered = await load_forward_strategy_decision(
                session,
                account_id=seed.account_id,
                strategy_version="baseline_v1",
                decision_session=DECISION_TIME.date(),
            )
            authority = await _seed_two_candidate_authority(
                session,
                registered,
                decision_time=NOW,
                source_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
            )
            inherited_aapl, inherited_msft, later_nvda = (item[0] for item in INSTRUMENTS)
            source_quote = await session.scalar(
                select(LatestQuote).where(LatestQuote.instrument_id == inherited_msft)
            )
            quote_values = {
                column.name: getattr(source_quote, column.name)
                for column in LatestQuote.__table__.columns
                if column.name != "id"
            }
            quote_values.update(instrument_id=later_nvda, content_hash="e" * 64)
            session.add(LatestQuote(**quote_values))
            source_bar = await session.scalar(
                select(PriceBarRecord).where(
                    PriceBarRecord.instrument_id == inherited_msft,
                    PriceBarRecord.revision == "second-prior",
                )
            )
            await ingest_price_page(
                session,
                _ObjectStore(),
                ProviderPage(
                    items=(
                        PriceBar(
                            instrument_id=later_nvda,
                            session=source_bar.session,
                            open=source_bar.open,
                            high=source_bar.high,
                            low=source_bar.low,
                            close=source_bar.close,
                            volume=source_bar.volume,
                            observed_at=source_bar.observed_at,
                            available_at=source_bar.available_at,
                            received_at=source_bar.received_at,
                            revision="later-nvda-prior",
                            quality_flags=(),
                        ),
                    ),
                    next_cursor=None,
                    raw_payload=b'{"prior":"later-nvda"}',
                    source_request_id=None,
                    as_of=source_bar.available_at,
                    provenance=MarketDataProvenance(
                        source="alpaca",
                        feed="sip",
                        adjustment="all",
                        interval="1Day",
                        mode="delayed",
                        delivery_mode="delayed",
                    ),
                ),
                commit=False,
                source_policy_id=PAPER_DECISION_SOURCE_POLICY_ID,
            )
            source_proofs = tuple(
                await session.scalars(
                    select(RiskQueryAuthorityRecord).where(
                        RiskQueryAuthorityRecord.required_instrument_ids == [inherited_msft],
                        RiskQueryAuthorityRecord.source_policy_id
                        == PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
                    )
                )
            )
            assert len(source_proofs) == 3
            for source in source_proofs:
                values = {
                    column.name: getattr(source, column.name)
                    for column in RiskQueryAuthorityRecord.__table__.columns
                    if column.name != "id"
                }
                values.update(
                    required_instrument_ids=[later_nvda],
                    blocked_instrument_ids=[],
                    source_watermark_hash="f" * 64,
                )
                values["content_hash"] = canonical_query_authority_hash(
                    **{
                        key: values[key]
                        for key in (
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
                session.add(RiskQueryAuthorityRecord(**values))
            await session.flush()
            execution_manifest = await prepare_candidates(
                session,
                authority=authority,
                requested_instrument_ids=(inherited_aapl, inherited_msft, later_nvda),
                max_instruments=10,
            )
            assert len(execution_manifest) == 3
            research_manifest = tuple(
                item for item in execution_manifest if item.instrument_id == later_nvda
            )
            market = await prepare_schwab_realtime_risk_snapshot(
                session,
                cycle=frozen_cycle(authority, research_manifest),
                required_instrument_ids=(inherited_aapl, inherited_msft, later_nvda),
            )
            assert len(research_manifest) == 1
            assert market.required_instrument_ids == tuple(
                sorted((inherited_aapl, inherited_msft, later_nvda), key=lambda value: value.int)
            )
            assert len(market.query_proofs) == 9
            assert all(proof.required_instrument_ids is not None for proof in market.query_proofs)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", ["cross_price", "cross_proof", "missing_proof", "duplicate"])
async def test_normal_schwab_snapshot_rejects_inexact_evidence(
    migrated_database_url, monkeypatch, bad
):
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url, monkeypatch
    )
    try:
        changed = list(manifest)
        if bad == "cross_price":
            changed[1] = changed[1].model_copy(
                update={"prior_daily_price_authority_id": changed[0].prior_daily_price_authority_id}
            )
        elif bad == "cross_proof":
            changed[1] = changed[1].model_copy(
                update={"risk_query_authority_ids": changed[0].risk_query_authority_ids}
            )
        elif bad == "missing_proof":
            changed[1] = changed[1].model_copy(
                update={
                    "risk_query_authority_ids": (*changed[1].risk_query_authority_ids[:2], uuid4())
                }
            )
        else:
            changed[1] = changed[0]
        async with sessions() as session:
            with pytest.raises(ValueError):
                await prepare_schwab_realtime_risk_snapshot(
                    session, cycle=frozen_cycle(authority, changed)
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_normal_occupied_cycle_prepares_every_research_and_stops_at_guard(
    migrated_database_url, monkeypatch
):
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url, monkeypatch, occupied=True
    )
    try:
        async with sessions.begin() as session:
            projection = await load_paper_projection(
                session, account_id=authority.account_id, as_of=NOW
            )
            assert projection.open_orders and projection.reserved_cash > 0
            counts = [
                await session.scalar(select(func.count()).select_from(model))
                for model in (StrategyDecisionRecord, PaperOrderRecord)
            ]
            batch = await create_agent_paper_candidate_batch(
                session,
                account_id=authority.account_id,
                universe_snapshot_id=authority.universe_snapshot_id,
                instrument_ids=tuple(entry.instrument_id for entry in manifest),
                idempotency_key="occupied-research",
                created_at=NOW,
            )
            result = await agent_cycle._start_cycle(
                session,
                authority=authority,
                decision_slot_at=floor_decision_slot(NOW),
                daily_limit_usd=Decimal("3"),
                candidate_batch_id=batch.batch_id,
                requested_instrument_ids=batch.instrument_ids,
            )
            assert result.status == "researching", result.result_code
            assert len(result.research_run_ids) == 2
            cycle = await session.get(AgentPaperCycleRecord, result.cycle_id)
            for run_id in result.research_run_ids:
                run = await session.get(AgentResearchRunRecord, run_id)
                context = await read_agent_paper_research_context(
                    session,
                    snapshot_id=run.data_snapshot_id,
                    instrument_id=run.instrument_id,
                    as_of=NOW,
                )
                assert context is not None
                assert context[0].paper_portfolio.projection_hash == projection.projection_hash
            jobs = tuple(
                await session.scalars(
                    select(Job).where(
                        Job.payload["run_id"]
                        .as_string()
                        .in_([str(value) for value in result.research_run_ids])
                    )
                )
            )
            assert len(jobs) == 2
            for job in jobs:
                job.status = "running"
                job.started_at = NOW
                job.attempt_count = 1
                job.version += 1
            await session.flush()
            for job in jobs:
                job.status = "succeeded"
                job.finished_at = NOW
                job.version += 1
            await session.flush()
            events = await agent_cycle._events(session, cycle.id)
            for _ in range(2):
                with pytest.raises(
                    ValueError, match="PORTFOLIO_SYNTHESIS_ADMISSION_CONTEXT_REQUIRED"
                ):
                    await agent_cycle._resume_research(
                        session, cycle=cycle, authority=authority, events=events
                    )
            assert len(await agent_cycle._events(session, cycle.id)) == len(events)
            assert [
                await session.scalar(select(func.count()).select_from(model))
                for model in (StrategyDecisionRecord, PaperOrderRecord)
            ] == counts
            assert (
                await load_paper_projection(session, account_id=authority.account_id, as_of=NOW)
                == projection
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_occupied_context_freezes_hash_and_rejects_tampering(
    migrated_database_url, monkeypatch
):
    engine, sessions, authority, manifest = await seed_normal_path(
        migrated_database_url, monkeypatch, occupied=True
    )
    try:
        async with sessions.begin() as session:
            projection = await load_paper_projection(
                session, account_id=authority.account_id, as_of=NOW
            )
            assert projection.open_orders
            cycle = frozen_cycle(authority, manifest)
            snapshot = await prepare_agent_paper_research_snapshot(
                session,
                cycle_id=cycle.id,
                decision_time=NOW,
                candidate=manifest[0],
                projection=projection,
            )
            assert snapshot is not None
            with pytest.raises(ValueError, match="CONTEXT_INVALID"):
                await prepare_agent_paper_research_snapshot(
                    session,
                    cycle_id=cycle.id,
                    decision_time=NOW,
                    candidate=manifest[1],
                    projection=projection.model_copy(update={"projection_hash": "f" * 64}),
                )
    finally:
        await engine.dispose()
