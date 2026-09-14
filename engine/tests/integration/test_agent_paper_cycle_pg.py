from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

import pytest
from helpers.paper_batch import DECISION_TIME, INSTRUMENTS, seed_paper_batch
from nexus.agents.actionable_research import ActionableResearchDecision
from nexus.agents.committee import load_prompt_assets
from nexus.agents.execution_authority import ROLE_ORDER
from nexus.market.ingestion import ingest_price_page
from nexus.market.intraday_ingestion import canonical_quote_observation
from nexus.paper.agent_cycle import (
    advance_agent_paper_cycle,
    resume_schwab_agent_paper_cycle,
    start_schwab_agent_paper_cycle,
)
from nexus.paper.agent_cycle_authority import (
    load_quasi_realtime_cycle_authority,
    prepare_candidates,
)
from nexus.paper.agent_cycle_models import candidate_manifest_hash, floor_decision_slot
from nexus.paper.agent_market import (
    collect_agent_paper_window,
    resolve_agent_paper_market_scope,
)
from nexus.paper.execution_service import advance_internal_paper_order
from nexus.paper.read_service import PaperReadService
from nexus.persistence.models.agents import (
    AgentDataSnapshotEvidenceRecord,
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.market import InstrumentAlias, LatestQuote
from nexus.persistence.models.paper import (
    PAPER_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
    AgentPaperCycleEventRecord,
    AgentPaperCycleRecord,
    PaperExecutionWindowAuthorityRecord,
    PaperExecutionWindowMemberRecord,
    PaperFillRecord,
    PaperLedgerEntryRecord,
    PaperReservationEventRecord,
    RiskQueryAuthorityRecord,
)
from nexus.persistence.models.research import Filing, ResearchEvidenceRef
from nexus.persistence.models.runtime import Job
from nexus.providers.base import MarketDataProvenance, PriceBar, ProviderPage
from nexus.risk.authority import (
    canonical_query_authority_hash,
    prepare_schwab_realtime_risk_snapshot,
)
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


class _ObjectStore:
    async def archive_raw(self, key: str, payload: bytes) -> str:
        return f"memory://{key}"

    async def put_normalized(self, key: str, payload: bytes) -> str:
        return f"memory://{key}"


class _IexProvider:
    calls = 0

    async def fetch_bars(self, request, cursor=None):  # type: ignore[no-untyped-def]
        self.calls += 1
        assert cursor is None
        evaluated_at = request.end + timedelta(seconds=2)
        return ProviderPage(
            items=tuple(
                PriceBar(
                    instrument_id=request.instrument_id,
                    session=request.start.date(),
                    open=Decimal("100"),
                    high=Decimal("101"),
                    low=Decimal("99"),
                    close=Decimal("100.5"),
                    volume=Decimal("20"),
                    observed_at=request.start + timedelta(minutes=index),
                    available_at=evaluated_at - timedelta(seconds=1),
                    received_at=evaluated_at - timedelta(seconds=1),
                )
                for index in range(5)
            ),
            next_cursor=None,
            raw_payload=(f'{{"bars":"{request.start.isoformat()}"}}'.encode()),
            source_request_id="agent-cycle-iex-window",
            as_of=evaluated_at - timedelta(seconds=1),
            provenance=MarketDataProvenance(
                source="alpaca",
                feed="iex",
                adjustment="raw",
                interval="1Min",
                mode="read_only",
                delivery_mode="real_time",
            ),
        )


class _SchwabPriceHistoryProvider(_IexProvider):
    async def fetch_bars(self, request, cursor=None):  # type: ignore[no-untyped-def]
        page = await super().fetch_bars(request, cursor)
        return page.model_copy(
            update={
                "items": tuple(
                    bar.model_copy(update={"volume": Decimal("2000")}) for bar in page.items
                ),
                "raw_payload": b'{"candles":"agent-cycle-schwab-window"}',
                "source_request_id": None,
                "provenance": page.provenance.model_copy(
                    update={
                        "source": "schwab",
                        "feed": "price_history",
                        "delivery_mode": "unknown",
                    }
                ),
            }
        )


async def _seed_adjusted_authority(
    session: AsyncSession,
    decision: object,
    *,
    source_policy_id: UUID = PAPER_DECISION_SOURCE_POLICY_ID,
    available_at: datetime | None = None,
) -> None:
    available_at = available_at or decision.decision_time - timedelta(minutes=30)
    store = _ObjectStore()
    for index, (instrument_id, close, _, _) in enumerate(INSTRUMENTS, start=1):
        for adjustment in ("raw", "all"):
            page = ProviderPage(
                items=(
                    PriceBar(
                        instrument_id=instrument_id,
                        session=decision.decision_session,
                        open=close,
                        high=close,
                        low=close,
                        close=close,
                        volume=Decimal("1000000"),
                        observed_at=available_at,
                        available_at=available_at,
                        received_at=available_at,
                        revision=f"agent-{adjustment}-{index}",
                        quality_flags=(),
                    ),
                ),
                next_cursor=None,
                raw_payload=(f'{{"agent_price":{index},"adjustment":"{adjustment}"}}'.encode()),
                source_request_id=None,
                as_of=available_at,
                provenance=MarketDataProvenance(
                    source="alpaca",
                    feed="sip",
                    adjustment=adjustment,
                    interval="1Day",
                    mode="delayed",
                    delivery_mode="delayed",
                ),
            )
            result = await ingest_price_page(
                session,
                store,
                page,
                commit=False,
                source_policy_id=(PAPER_DECISION_SOURCE_POLICY_ID if adjustment == "all" else None),
            )
            assert result.price_authority is not None
    prior_page = ProviderPage(
        items=(
            PriceBar(
                instrument_id=INSTRUMENTS[0][0],
                session=decision.decision_session - timedelta(days=1),
                open=INSTRUMENTS[0][1],
                high=INSTRUMENTS[0][1],
                low=INSTRUMENTS[0][1],
                close=INSTRUMENTS[0][1],
                volume=Decimal("1000000"),
                observed_at=available_at - timedelta(days=1),
                available_at=available_at,
                received_at=available_at,
                revision="agent-prior-all-aapl",
                quality_flags=(),
            ),
        ),
        next_cursor=None,
        raw_payload=b'{"agent_price":"prior","adjustment":"all"}',
        source_request_id=None,
        as_of=available_at,
        provenance=MarketDataProvenance(
            source="alpaca",
            feed="sip",
            adjustment="all",
            interval="1Day",
            mode="delayed",
            delivery_mode="delayed",
        ),
    )
    prior_result = await ingest_price_page(
        session,
        store,
        prior_page,
        commit=False,
        source_policy_id=PAPER_DECISION_SOURCE_POLICY_ID,
    )
    assert prior_result.price_authority is not None
    required = (INSTRUMENTS[0][0],)
    for index, kind in enumerate(("corporate_action", "suspension", "delisting"), start=1):
        content_hash = canonical_query_authority_hash(
            source_policy_id=source_policy_id,
            universe_snapshot_id=decision.universe_snapshot_id,
            decision_session=decision.decision_session,
            kind=kind,
            required_instrument_ids=required,
            blocked_instrument_ids=(),
            complete=True,
            source_watermark_hash=f"{index + 6}" * 64,
            available_at=available_at,
        )
        session.add(
            RiskQueryAuthorityRecord(
                id=UUID(int=0x63000000000000000000000000000500 + index),
                source_policy_id=source_policy_id,
                universe_snapshot_id=decision.universe_snapshot_id,
                decision_session=decision.decision_session,
                kind=kind,
                required_instrument_ids=list(required),
                blocked_instrument_ids=[],
                complete=True,
                source_watermark_hash=f"{index + 6}" * 64,
                available_at=available_at,
                content_hash=content_hash,
            )
        )
    await session.flush()


async def _seed_cycle_research_authority(
    session: AsyncSession,
    *,
    registered_decision: object,
    source_policy_id: UUID = PAPER_DECISION_SOURCE_POLICY_ID,
    quote_received_at: datetime = DECISION_TIME,
    price_available_at: datetime | None = None,
) -> None:
    authority_time = price_available_at or DECISION_TIME
    await session.execute(text("ALTER TABLE universe_snapshots DISABLE TRIGGER USER"))
    try:
        await session.execute(
            text(
                "UPDATE universe_snapshots SET key='us_liquid',version='us_liquid_v1',"
                "source='fosun_local_read_only'"
            )
        )
    finally:
        await session.execute(text("ALTER TABLE universe_snapshots ENABLE TRIGGER USER"))
    # Agent admission validates its exact roots directly. A legacy QualityReport
    # must not be an otherwise unreachable prerequisite for this path.
    await session.execute(text("ALTER TABLE quality_reports DISABLE TRIGGER USER"))
    try:
        await session.execute(
            text(
                "UPDATE quality_reports SET payload=jsonb_set("
                "payload,'{decision_session}','\"2026-01-01\"'::jsonb)"
            )
        )
    finally:
        await session.execute(text("ALTER TABLE quality_reports ENABLE TRIGGER USER"))
    prompts = load_prompt_assets()
    calendar_authority_id = await session.scalar(
        select(ExchangeCalendarSessionRecord.calendar_authority_id).limit(1)
    )
    assert calendar_authority_id is not None
    session.add(
        ExchangeCalendarSessionRecord(
            calendar_authority_id=calendar_authority_id,
            session=authority_time.date() - timedelta(days=1),
            decision_at=DECISION_TIME - timedelta(days=1),
        )
    )
    session.add_all(
        AgentPromptVersionRecord(
            id=UUID(int=0x63000000000000000000000000000100 + index),
            role=role,
            version=f"agent-paper-{role}-v1",
            content_hash=prompts[role].content_hash,
            content=prompts[role].content,
        )
        for index, role in enumerate(ROLE_ORDER)
    )
    session.add(
        AgentModelConfigRecord(
            id=UUID("63000000-0000-0000-0000-000000000001"),
            model_id="fixture-agent-paper",
            version="codex-sdk-chatgpt-v4-actionable",
            config_hash="6" * 64,
            max_input_tokens=100,
            max_tool_calls=1,
            max_output_tokens=100,
            input_usd_per_million=Decimal("1"),
            output_usd_per_million=Decimal("1"),
            pricing_available_at=authority_time - timedelta(days=1),
            pricing_expires_at=authority_time + timedelta(days=1),
        )
    )
    for index, (instrument_id, _, _, _) in enumerate(INSTRUMENTS, start=1):
        evidence = ResearchEvidenceRef(
            id=UUID(int=0x63000000000000000000000000000200 + index),
            source="sec",
            provider_object_id=f"agent-paper:{instrument_id}",
            revision="v1",
            available_at=authority_time - timedelta(hours=2),
            location=f"memory://sha256/{f'{index + 6}' * 64}",
            content_hash=f"{index + 6}" * 64,
            byte_length=10,
        )
        snapshot = DataSnapshot(
            id=UUID(int=0x63000000000000000000000000000300 + index),
            source="nexus_research",
            provider_object_id=str(instrument_id),
            revision="agent-paper-v1",
            observed_at=authority_time - timedelta(hours=2),
            available_at=authority_time - timedelta(hours=2),
            received_at=authority_time - timedelta(hours=2),
            schema_version="research_filings_v1",
            payload={"scope": "persisted_research_observations"},
        )
        session.add_all(
            (
                InstrumentAlias(
                    id=UUID(int=0x63000000000000000000000000000400 + index),
                    instrument_id=instrument_id,
                    symbol="AAPL" if index == 1 else f"T{index}",
                    alias_type="ticker",
                    valid_from=date(2026, 1, 1),
                    valid_to=None,
                ),
                evidence,
                snapshot,
            )
        )
        await session.flush()
        session.add(
            Filing(
                instrument_id=instrument_id,
                evidence_ref_id=evidence.id,
                source="sec",
                provider_object_id=evidence.provider_object_id,
                revision=evidence.revision,
                accession_number=f"000000000{index}-26-000001",
                form="10-Q",
                filed_at=authority_time - timedelta(days=1),
                accepted_at=authority_time - timedelta(hours=2),
                created_at=authority_time - timedelta(hours=2),
            )
        )
        await session.flush()
        session.add(
            AgentDataSnapshotEvidenceRecord(
                data_snapshot_id=snapshot.id,
                instrument_id=instrument_id,
                evidence_ref_id=evidence.id,
                authority_kind="research",
                selected_at=None,
                source=evidence.source,
                provider_object_id=evidence.provider_object_id,
                revision=evidence.revision,
                available_at=evidence.available_at,
                location=evidence.location,
                content_hash=evidence.content_hash,
            )
        )
    await session.flush()

    await _seed_adjusted_authority(
        session,
        registered_decision,
        source_policy_id=source_policy_id,
        available_at=price_available_at,
    )
    artifact = DataArtifact(
        id=UUID("63000000-0000-0000-0000-000000000090"),
        uri=f"memory://raw/market/schwab/stream/{'e' * 64}.json",
        sha256="e" * 64,
        byte_size=100,
        mime_type="application/json",
        schema_version="schwab_level_one_stream_v1",
        source="schwab",
        observed_at=quote_received_at - timedelta(seconds=10),
        available_at=quote_received_at,
        received_at=quote_received_at,
        created_at=quote_received_at,
    )
    quote_hash = canonical_quote_observation(
        instrument_id=INSTRUMENTS[0][0],
        source="schwab",
        feed="l1",
        delivery_mode="real_time",
        event_kind="quote",
        bid=Decimal("99.90"),
        ask=Decimal("100.10"),
        last=None,
        observed_at=quote_received_at - timedelta(seconds=10),
        available_at=quote_received_at,
        received_at=quote_received_at,
        revision="e" * 64,
    )
    session.add_all(
        (
            artifact,
            LatestQuote(
                id=UUID("63000000-0000-0000-0000-000000000091"),
                instrument_id=INSTRUMENTS[0][0],
                source="schwab",
                feed="l1",
                delivery_mode="real_time",
                bid=Decimal("99.90"),
                ask=Decimal("100.10"),
                last=None,
                observed_at=quote_received_at - timedelta(seconds=10),
                available_at=quote_received_at,
                received_at=quote_received_at,
                revision="e" * 64,
                quality_flags=[],
                event_kind="quote",
                content_hash=sha256(quote_hash).hexdigest(),
                evidence_status="verified",
                data_artifact_id=artifact.id,
                created_at=quote_received_at,
            ),
        )
    )
    await session.flush()


@pytest.mark.asyncio
async def test_quasi_realtime_authority_uses_database_time_window_and_same_session(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            await session.execute(
                text("ALTER TABLE exchange_calendar_authorities DISABLE TRIGGER USER")
            )
            await session.execute(text("ALTER TABLE universe_snapshots DISABLE TRIGGER USER"))
            try:
                published_at = datetime(2026, 8, 25, 20, tzinfo=UTC)
                await session.execute(
                    text("UPDATE exchange_calendar_authorities SET published_at=:at"),
                    {"at": published_at},
                )
                await session.execute(
                    text("UPDATE universe_snapshots SET published_at=:at"),
                    {"at": published_at},
                )
                await session.execute(
                    text(
                        "UPDATE universe_snapshots SET key='us_liquid',"
                        "version='us_liquid_v1',source='fosun_local_read_only'"
                    )
                )
            finally:
                await session.execute(text("ALTER TABLE universe_snapshots ENABLE TRIGGER USER"))
                await session.execute(
                    text("ALTER TABLE exchange_calendar_authorities ENABLE TRIGGER USER")
                )
        async with sessions() as session:
            at_open = await load_quasi_realtime_cycle_authority(
                session,
                account_id=seed.account_id,
                now=datetime(2026, 8, 26, 11, tzinfo=UTC),
            )
            before_close = await load_quasi_realtime_cycle_authority(
                session,
                account_id=seed.account_id,
                now=datetime(2026, 8, 26, 19, 59, 59, tzinfo=UTC),
            )
            with pytest.raises(ValueError, match="AGENT_PAPER_MARKET_WINDOW_CLOSED"):
                await load_quasi_realtime_cycle_authority(
                    session,
                    account_id=seed.account_id,
                    now=datetime(2026, 8, 26, 20, tzinfo=UTC),
                )

        assert at_open.decision_session == date(2026, 8, 26)
        assert at_open.execution_session == at_open.decision_session
        assert at_open.decision_time == datetime(2026, 8, 26, 11, tzinfo=UTC)
        assert before_close.decision_time == datetime(2026, 8, 26, 19, 59, 59, tzinfo=UTC)
        assert at_open.quality_report_id is None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_quasi_realtime_authority_observes_dst_holiday_and_allows_later_slots(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    winter_session = date(2026, 12, 1)
    winter_open = datetime(2026, 12, 1, 14, 30, tzinfo=UTC)
    winter_close = datetime(2026, 12, 1, 21, tzinfo=UTC)
    calendar_id = UUID("63000000-0000-0000-0000-000000000098")
    try:
        async with sessions.begin() as session:
            session.add(
                ExchangeCalendarAuthorityRecord(
                    id=calendar_id,
                    exchange="XNYS",
                    version="agent-paper-winter-test",
                    source="fixture",
                    range_start=winter_session,
                    range_end=winter_session,
                    query_parameters={},
                    completeness_status="complete",
                    content_hash="9" * 64,
                    published_at=datetime(2026, 11, 30, tzinfo=UTC),
                    created_at=datetime(2026, 11, 30, tzinfo=UTC),
                )
            )
            await session.flush()
            session.add(
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=calendar_id,
                    session=winter_session,
                    decision_at=winter_close,
                    market_open_at=winter_open,
                    market_close_at=winter_close,
                    created_at=datetime(2026, 11, 30, tzinfo=UTC),
                )
            )
            await session.execute(text("ALTER TABLE universe_snapshots DISABLE TRIGGER USER"))
            try:
                await session.execute(
                    text(
                        "UPDATE universe_snapshots SET key='us_liquid',"
                        "version='us_liquid_v1',source='fosun_local_read_only',"
                        "published_at='2026-11-30T00:00:00Z'"
                    )
                )
            finally:
                await session.execute(text("ALTER TABLE universe_snapshots ENABLE TRIGGER USER"))

        async with sessions() as session:
            winter = await load_quasi_realtime_cycle_authority(
                session,
                account_id=seed.account_id,
                now=datetime(2026, 12, 1, 12, tzinfo=UTC),
            )
            with pytest.raises(ValueError, match="AGENT_PAPER_CALENDAR_UNAVAILABLE"):
                await load_quasi_realtime_cycle_authority(
                    session,
                    account_id=seed.account_id,
                    now=datetime(2026, 11, 26, 12, tzinfo=UTC),
                )
        assert winter.decision_session == winter_session

        async with sessions.begin() as session:
            session.add(
                AgentPaperCycleRecord(
                    id=UUID("63000000-0000-0000-0000-000000000097"),
                    account_id=winter.account_id,
                    portfolio_id=winter.portfolio_id,
                    decision_session=winter.decision_session,
                    decision_time=winter.decision_time,
                    decision_slot_at=floor_decision_slot(winter.decision_time),
                    execution_session=winter.execution_session,
                    universe_snapshot_id=winter.universe_snapshot_id,
                    quality_report_id=None,
                    decision_price_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
                    execution_price_policy_id=PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
                    candidate_manifest=[],
                    candidate_hash=candidate_manifest_hash(()),
                    sizing_policy_version="agent_paper_sizing_v1",
                    sizing_policy_hash="a" * 64,
                    execution_locked=True,
                )
            )
        async with sessions() as session:
            later = await load_quasi_realtime_cycle_authority(
                session,
                account_id=seed.account_id,
                now=datetime(2026, 12, 1, 12, 5, tzinfo=UTC),
            )
        assert later.decision_session == winter_session
        assert later.decision_time == datetime(2026, 12, 1, 12, 5, tzinfo=UTC)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_schwab_risk_snapshot_uses_cycle_frozen_prior_daily_and_proofs(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    decision_time = datetime(2026, 8, 26, 11, 30, tzinfo=UTC)
    try:
        from nexus.strategy.forward import load_forward_strategy_decision

        async with sessions.begin() as session:
            registered = await load_forward_strategy_decision(
                session,
                account_id=seed.account_id,
                strategy_version="baseline_v1",
                decision_session=DECISION_TIME.date(),
            )
            assert registered is not None
            await _seed_cycle_research_authority(
                session,
                registered_decision=registered.decision,
                source_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
                quote_received_at=decision_time,
                price_available_at=decision_time,
            )
            historical_tables = (
                "source_policies",
                "instrument_aliases",
                "price_series_manifests",
                "price_bar_records",
                "price_bar_set_authorities",
                "research_evidence_refs",
                "risk_query_authorities",
                "sector_exposures",
                "agent_data_snapshot_evidence",
            )
            for table_name in historical_tables:
                await session.execute(text(f"ALTER TABLE {table_name} DISABLE TRIGGER USER"))
                try:
                    await session.execute(
                        text(f"UPDATE {table_name} SET created_at=:at WHERE created_at>:at"),
                        {"at": decision_time},
                    )
                finally:
                    await session.execute(text(f"ALTER TABLE {table_name} ENABLE TRIGGER USER"))
            await session.execute(
                text("ALTER TABLE exchange_calendar_authorities DISABLE TRIGGER USER")
            )
            await session.execute(text("ALTER TABLE universe_snapshots DISABLE TRIGGER USER"))
            try:
                published_at = decision_time - timedelta(days=1)
                await session.execute(
                    text("UPDATE exchange_calendar_authorities SET published_at=:at"),
                    {"at": published_at},
                )
                await session.execute(
                    text("UPDATE universe_snapshots SET published_at=:at"),
                    {"at": published_at},
                )
            finally:
                await session.execute(text("ALTER TABLE universe_snapshots ENABLE TRIGGER USER"))
                await session.execute(
                    text("ALTER TABLE exchange_calendar_authorities ENABLE TRIGGER USER")
                )
            newest_risk_ids = set(
                await session.scalars(
                    select(RiskQueryAuthorityRecord.id).where(
                        RiskQueryAuthorityRecord.source_policy_id
                        == PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
                        RiskQueryAuthorityRecord.available_at == decision_time,
                    )
                )
            )
            for index, kind in enumerate(
                ("corporate_action", "suspension", "delisting"), start=1
            ):
                older_at = decision_time - timedelta(minutes=1)
                watermark = f"{index + 3}" * 64
                session.add(
                    RiskQueryAuthorityRecord(
                        id=UUID(int=0x63000000000000000000000000000900 + index),
                        source_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
                        universe_snapshot_id=registered.decision.universe_snapshot_id,
                        decision_session=registered.decision.decision_session,
                        kind=kind,
                        required_instrument_ids=[INSTRUMENTS[0][0]],
                        blocked_instrument_ids=[],
                        complete=True,
                        source_watermark_hash=watermark,
                        available_at=older_at,
                        content_hash=canonical_query_authority_hash(
                            source_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
                            universe_snapshot_id=registered.decision.universe_snapshot_id,
                            decision_session=registered.decision.decision_session,
                            kind=kind,
                            required_instrument_ids=[INSTRUMENTS[0][0]],
                            blocked_instrument_ids=[],
                            complete=True,
                            source_watermark_hash=watermark,
                            available_at=older_at,
                        ),
                        created_at=decision_time,
                    )
                )
            await session.flush()

        async with sessions() as session:
            await session.execute(text("SET LOCAL enable_indexscan=off"))
            await session.execute(text("SET LOCAL enable_bitmapscan=off"))
            authority = await load_quasi_realtime_cycle_authority(
                session,
                account_id=seed.account_id,
                now=decision_time,
            )
            selected_previous = await session.get(
                ExchangeCalendarSessionRecord,
                (authority.calendar_authority_id, authority.decision_session - timedelta(days=1)),
            )
            if selected_previous is None:
                await session.execute(
                    text("ALTER TABLE exchange_calendar_sessions DISABLE TRIGGER USER")
                )
                try:
                    session.add(
                        ExchangeCalendarSessionRecord(
                            calendar_authority_id=authority.calendar_authority_id,
                            session=authority.decision_session - timedelta(days=1),
                            decision_at=DECISION_TIME - timedelta(days=1),
                            market_open_at=(decision_time - timedelta(days=1)).replace(
                                hour=13, minute=30
                            ),
                            market_close_at=(decision_time - timedelta(days=1)).replace(
                                hour=20, minute=0
                            ),
                            created_at=decision_time - timedelta(days=1),
                        )
                    )
                    await session.flush()
                finally:
                    await session.execute(
                        text("ALTER TABLE exchange_calendar_sessions ENABLE TRIGGER USER")
                    )
            manifest = await prepare_candidates(session, authority=authority)
            assert len(manifest) == 1
            assert set(manifest[0].risk_query_authority_ids) == newest_risk_ids
            cycle = AgentPaperCycleRecord(
                id=UUID("63000000-0000-0000-0000-000000000099"),
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
                candidate_manifest=[row.model_dump(mode="json") for row in manifest],
                candidate_hash=candidate_manifest_hash(manifest),
                sizing_policy_version="agent_paper_sizing_v1",
                sizing_policy_hash="a" * 64,
                execution_locked=True,
            )
            market = await prepare_schwab_realtime_risk_snapshot(session, cycle=cycle)

        assert market.source_policy_id == PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID
        assert market.execution_session == market.decision_session
        assert market.prices[0].authority_id == manifest[0].prior_daily_price_authority_id
        assert {proof.authority_id for proof in market.query_proofs} == set(
            manifest[0].risk_query_authority_ids
        )

        async with sessions.begin() as session:
            first = await start_schwab_agent_paper_cycle(
                session,
                authority=authority,
                decision_slot_at=floor_decision_slot(authority.decision_time),
                idempotency_key="schwab-trigger-1",
            )
        async with sessions.begin() as session:
            replay = await start_schwab_agent_paper_cycle(
                session,
                authority=authority,
                decision_slot_at=floor_decision_slot(authority.decision_time),
                idempotency_key="schwab-trigger-1",
            )
            job_count = await session.scalar(select(func.count()).select_from(Job))
        async with sessions.begin() as session:
            resumed = await resume_schwab_agent_paper_cycle(
                session,
                cycle_id=first.cycle_id,
                idempotency_key="schwab-resume-1",
            )
            resumed_job_count = await session.scalar(select(func.count()).select_from(Job))

        assert first.status == "researching"
        assert replay == first.model_copy(update={"replayed": True})
        assert resumed == first.model_copy(update={"replayed": True})
        assert len(first.research_run_ids) == 1
        assert job_count == resumed_job_count == 1

        output_at = decision_time + timedelta(seconds=10)
        output_id = UUID("63000000-0000-0000-0000-000000000777")
        admitted = ActionableResearchDecision(
            instrument_id=manifest[0].instrument_id,
            output_id=output_id,
            intent_hash="9" * 64,
            stance="buy_candidate",
            sizing_action="increase",
            evidence_ref_ids=(UUID(int=778),),
        )
        async with sessions.begin() as session:
            await session.execute(
                text("ALTER TABLE strategy_decision_targets DISABLE TRIGGER USER")
            )
            await session.execute(text("ALTER TABLE strategy_decisions DISABLE TRIGGER USER"))
            try:
                await session.execute(text("DELETE FROM strategy_decision_targets"))
                await session.execute(text("DELETE FROM strategy_decisions"))
            finally:
                await session.execute(text("ALTER TABLE strategy_decisions ENABLE TRIGGER USER"))
                await session.execute(
                    text("ALTER TABLE strategy_decision_targets ENABLE TRIGGER USER")
                )
            await session.execute(text("ALTER TABLE agent_research_runs DISABLE TRIGGER USER"))
            await session.execute(text("ALTER TABLE jobs DISABLE TRIGGER USER"))
            try:
                await session.execute(
                    text("UPDATE agent_research_runs SET status='completed' WHERE id=:run_id"),
                    {"run_id": first.research_run_ids[0]},
                )
                await session.execute(
                    text(
                        "UPDATE jobs SET status='succeeded',started_at=:at,finished_at=:at,"
                        "attempt_count=1,result_code='AGENT_RESEARCH_COMPLETED',version=version+1"
                    ),
                    {"at": output_at},
                )
            finally:
                await session.execute(text("ALTER TABLE jobs ENABLE TRIGGER USER"))
                await session.execute(text("ALTER TABLE agent_research_runs ENABLE TRIGGER USER"))
            session.add(
                AgentResearchOutputRecord(
                    id=output_id,
                    run_id=first.research_run_ids[0],
                    sequence=1,
                    status="completed",
                    role=None,
                    prompt_hash=None,
                    content_hash="7" * 64,
                    payload={},
                    intent={},
                    created_at=output_at,
                )
            )

        async def fake_load_actionable(
            *_args, decision_time: datetime, **_kwargs
        ) -> ActionableResearchDecision:
            assert decision_time == output_at
            assert _kwargs["allow_safe_no_trade"] is True
            return admitted

        monkeypatch.setattr(
            "nexus.paper.agent_cycle.load_actionable_research_output", fake_load_actionable
        )
        monkeypatch.setattr(
            "nexus.paper.agent_decision_store.load_actionable_research_output",
            fake_load_actionable,
        )
        async with sessions.begin() as session:
            awaiting_market = await resume_schwab_agent_paper_cycle(
                session,
                cycle_id=first.cycle_id,
                idempotency_key="schwab-resume-2",
            )
        assert awaiting_market.status == "awaiting_market", awaiting_market.result_code
        assert len(awaiting_market.order_ids) == 1

        async with sessions() as session:
            scope = await resolve_agent_paper_market_scope(session, cycle_id=first.cycle_id)
        assert scope.window_start == datetime(2026, 8, 26, 11, 31, tzinfo=UTC)
        assert scope.window_end == datetime(2026, 8, 26, 11, 36, tzinfo=UTC)
        evaluated_at = scope.window_end + timedelta(seconds=2)
        async with sessions.begin() as session:
            no_window = await advance_internal_paper_order(
                session,
                order_id=awaiting_market.order_ids[0],
                evaluated_at=evaluated_at,
                idempotency_key="schwab-paper-no-window-1",
            )
        assert no_window.action == "no_data"
        assert no_window.fill_id is None

        provider = _SchwabPriceHistoryProvider()
        window = await collect_agent_paper_window(
            sessions,
            cycle_id=first.cycle_id,
            provider=provider,
            object_store=_ObjectStore(),
            evaluated_at=evaluated_at,
        )
        window_replay = await collect_agent_paper_window(
            sessions,
            cycle_id=first.cycle_id,
            provider=provider,
            object_store=_ObjectStore(),
            evaluated_at=evaluated_at,
        )
        assert window.purpose == "paper_fill"
        assert (window.source, window.feed, window.delivery_mode) == (
            "schwab",
            "price_history",
            "unknown",
        )
        assert window_replay == window.model_copy(update={"replayed": True})
        assert provider.calls == 1
        async with sessions.begin() as session:
            filled = await advance_internal_paper_order(
                session,
                order_id=awaiting_market.order_ids[0],
                evaluated_at=evaluated_at,
                idempotency_key="schwab-paper-fill-1",
            )
        async with sessions.begin() as session:
            fill_replay = await advance_internal_paper_order(
                session,
                order_id=awaiting_market.order_ids[0],
                evaluated_at=evaluated_at,
                idempotency_key="schwab-paper-fill-1",
            )
        assert filled.action == "filled"
        assert fill_replay == filled
        async with sessions() as session:
            debits = await session.scalar(select(func.sum(PaperLedgerEntryRecord.debit_amount)))
            credits = await session.scalar(select(func.sum(PaperLedgerEntryRecord.credit_amount)))
            fill_count = await session.scalar(select(func.count(PaperFillRecord.id)))
            execution_window_count = await session.scalar(
                select(func.count(PaperExecutionWindowAuthorityRecord.id))
            )
            execution_member_count = await session.scalar(
                select(func.count(PaperExecutionWindowMemberRecord.bar_id))
            )
            reservation_events = tuple(
                await session.scalars(
                    select(PaperReservationEventRecord.event_type)
                    .where(PaperReservationEventRecord.account_id == seed.account_id)
                    .order_by(PaperReservationEventRecord.event_seq)
                )
            )
        assert debits == credits
        assert fill_count == execution_window_count == 1
        assert execution_member_count == 5
        assert "consumed" in reservation_events
        assert "released" in reservation_events
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_first_call_enqueues_existing_research_jobs_and_replay_does_not_duplicate(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market_close = datetime(2026, 8, 27, 20, tzinfo=UTC)
    seed = await seed_paper_batch(migrated_database_url, execution_market_close_at=market_close)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        from nexus.strategy.forward import load_forward_strategy_decision

        async with sessions.begin() as session:
            registered = await load_forward_strategy_decision(
                session,
                account_id=seed.account_id,
                strategy_version="baseline_v1",
                decision_session=DECISION_TIME.date(),
            )
            assert registered is not None
            await _seed_cycle_research_authority(session, registered_decision=registered.decision)

        async with sessions.begin() as session:
            first = await advance_agent_paper_cycle(
                session,
                account_id=seed.account_id,
                decision_time=DECISION_TIME,
                idempotency_key="agent-paper-cycle-1",
            )
        assert first.status == "researching", first.result_code
        assert first.replayed is False
        assert len(first.research_run_ids) == 1

        async with sessions.begin() as session:
            second = await advance_agent_paper_cycle(
                session,
                account_id=seed.account_id,
                decision_time=DECISION_TIME,
                idempotency_key="agent-paper-cycle-1",
            )
            assert second == first.model_copy(update={"replayed": True})
            assert await session.scalar(select(func.count()).select_from(Job)) == 1
            cycle_count = await session.scalar(
                select(func.count()).select_from(AgentPaperCycleRecord)
            )
            assert cycle_count == 1
            cycle = await session.scalar(select(AgentPaperCycleRecord))
            assert cycle is not None
            assert len(cycle.candidate_manifest) == 1
            candidate = cycle.candidate_manifest[0]
            assert candidate["symbol"] == "AAPL"
            assert candidate["decision_quote_id"] == str(
                UUID("63000000-0000-0000-0000-000000000091")
            )
            assert candidate["decision_evidence_hash"] == "e" * 64
            assert candidate["decision_trade_id"] is None
            assert len(candidate["risk_query_authority_ids"]) == 3
            assert (
                await session.scalar(select(func.count()).select_from(AgentPaperCycleEventRecord))
                == 2
            )
        async with sessions.begin() as session:
            with pytest.raises(ValueError, match="AGENT_PAPER_IDEMPOTENCY_CONFLICT"):
                await advance_agent_paper_cycle(
                    session,
                    account_id=seed.account_id,
                    decision_time=DECISION_TIME + timedelta(seconds=1),
                    idempotency_key="agent-paper-cycle-1",
                )

        admitted: dict[UUID, ActionableResearchDecision] = {}
        async with sessions.begin() as session:
            await session.execute(
                text("ALTER TABLE strategy_decision_targets DISABLE TRIGGER USER")
            )
            await session.execute(text("ALTER TABLE strategy_decisions DISABLE TRIGGER USER"))
            try:
                await session.execute(text("DELETE FROM strategy_decision_targets"))
                await session.execute(text("DELETE FROM strategy_decisions"))
            finally:
                await session.execute(text("ALTER TABLE strategy_decisions ENABLE TRIGGER USER"))
                await session.execute(
                    text("ALTER TABLE strategy_decision_targets ENABLE TRIGGER USER")
                )
            await session.execute(text("ALTER TABLE agent_research_runs DISABLE TRIGGER USER"))
            try:
                await session.execute(
                    text(
                        "UPDATE agent_research_runs SET status='completed' WHERE id = ANY(:run_ids)"
                    ),
                    {"run_ids": list(first.research_run_ids)},
                )
            finally:
                await session.execute(text("ALTER TABLE agent_research_runs ENABLE TRIGGER USER"))
            await session.execute(text("ALTER TABLE jobs DISABLE TRIGGER USER"))
            try:
                await session.execute(
                    text(
                        "UPDATE jobs SET status='succeeded',started_at=:at,finished_at=:at,"
                        "attempt_count=1,result_code='AGENT_RESEARCH_COMPLETED',version=version+1"
                    ),
                    {"at": DECISION_TIME},
                )
            finally:
                await session.execute(text("ALTER TABLE jobs ENABLE TRIGGER USER"))
            for index, (run_id, instrument) in enumerate(
                zip(first.research_run_ids, INSTRUMENTS[:1], strict=True), start=1
            ):
                output = AgentResearchOutputRecord(
                    id=UUID(int=0x63000000000000000000000000000500 + index),
                    run_id=run_id,
                    sequence=1,
                    status="completed",
                    role=None,
                    prompt_hash=None,
                    content_hash=f"{index}" * 64,
                    payload={},
                    intent={},
                )
                session.add(output)
                admitted[run_id] = ActionableResearchDecision(
                    instrument_id=instrument[0],
                    output_id=output.id,
                    intent_hash=f"{index + 3}" * 64,
                    stance="buy_candidate" if index == 1 else "watch",
                    sizing_action="increase" if index == 1 else "none",
                    evidence_ref_ids=(UUID(int=0x63000000000000000000000000000200 + index),),
                )

        async def fake_load_actionable(
            _session: AsyncSession,
            *,
            run_id: UUID,
            decision_time: datetime,
            allow_safe_no_trade: bool,
        ) -> ActionableResearchDecision:
            assert decision_time == DECISION_TIME
            assert allow_safe_no_trade is True
            return admitted[run_id]

        monkeypatch.setattr(
            "nexus.paper.agent_cycle.load_actionable_research_output", fake_load_actionable
        )
        monkeypatch.setattr(
            "nexus.paper.agent_decision_store.load_actionable_research_output",
            fake_load_actionable,
        )
        async with sessions.begin() as session:
            completed = await advance_agent_paper_cycle(
                session,
                account_id=seed.account_id,
                decision_time=DECISION_TIME,
                idempotency_key="agent-paper-cycle-1",
            )
        assert completed.status == "awaiting_market", completed.result_code
        assert completed.strategy_decision_id is not None
        assert completed.batch_id is not None
        assert len(completed.order_ids) == 1
        assert completed.replayed is False

        async with sessions.begin() as session:
            replayed = await advance_agent_paper_cycle(
                session,
                account_id=seed.account_id,
                decision_time=DECISION_TIME,
                idempotency_key="agent-paper-cycle-1",
            )
            assert replayed == completed.model_copy(update={"replayed": True})

        window_start = datetime(2026, 8, 27, 13, 30, tzinfo=UTC)
        evaluated_at = window_start + timedelta(minutes=25, seconds=2)
        market = await collect_agent_paper_window(
            sessions,
            cycle_id=completed.cycle_id,
            instrument_id=INSTRUMENTS[0][0],
            window_start=window_start,
            provider=_IexProvider(),
            object_store=_ObjectStore(),
            evaluated_at=evaluated_at,
        )
        assert market.purpose == "paper_fill"
        async with sessions.begin() as session:
            advanced = await advance_internal_paper_order(
                session,
                order_id=completed.order_ids[0],
                evaluated_at=evaluated_at,
                idempotency_key="agent-paper-first-fill",
            )
        assert advanced.action == "partially_filled"
        async with sessions() as session:
            historical = await PaperReadService(
                session, evaluated_at - timedelta(seconds=1)
            ).latest_agent_cycle()
            projected = await PaperReadService(session, evaluated_at).latest_agent_cycle()
        assert historical is not None
        assert historical.status == "awaiting_market"
        assert projected is not None
        assert projected.status == "partially_filled"
        assert projected.result_code == "PAPER_ORDERS_PARTIALLY_FILLED"
        second_window_start = window_start + timedelta(minutes=5)
        second_evaluated_at = evaluated_at + timedelta(minutes=5)
        await collect_agent_paper_window(
            sessions,
            cycle_id=completed.cycle_id,
            instrument_id=INSTRUMENTS[0][0],
            window_start=second_window_start,
            provider=_IexProvider(),
            object_store=_ObjectStore(),
            evaluated_at=second_evaluated_at,
        )
        async with sessions.begin() as session:
            final_fill = await advance_internal_paper_order(
                session,
                order_id=completed.order_ids[0],
                evaluated_at=second_evaluated_at,
                idempotency_key="agent-paper-final-fill",
            )
        assert final_fill.action == "filled"
        async with sessions() as session:
            projected = await PaperReadService(session, second_evaluated_at).latest_agent_cycle()
        assert projected is not None
        assert projected.status == "completed"
        assert projected.result_code == "PAPER_CYCLE_COMPLETED"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_mixed_terminal_research_jobs_stop_without_replacement_runs(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        from nexus.strategy.forward import load_forward_strategy_decision

        async with sessions.begin() as session:
            registered = await load_forward_strategy_decision(
                session,
                account_id=seed.account_id,
                strategy_version="baseline_v1",
                decision_session=DECISION_TIME.date(),
            )
            assert registered is not None
            await _seed_cycle_research_authority(session, registered_decision=registered.decision)
        async with sessions.begin() as session:
            first = await advance_agent_paper_cycle(
                session,
                account_id=seed.account_id,
                decision_time=DECISION_TIME,
                idempotency_key="agent-paper-terminal-failure",
            )
        assert first.status == "researching"

        async with sessions.begin() as session:
            jobs = tuple(await session.scalars(select(Job).order_by(Job.id)))
            await session.execute(text("ALTER TABLE jobs DISABLE TRIGGER USER"))
            try:
                for index, job in enumerate(jobs):
                    status = "succeeded" if index == 0 else "dead_letter"
                    await session.execute(
                        text(
                            "UPDATE jobs SET status=:status,started_at=:at,finished_at=:at,"
                            "attempt_count=1,version=version+1 WHERE id=:job_id"
                        ),
                        {"status": status, "at": DECISION_TIME, "job_id": job.id},
                    )
            finally:
                await session.execute(text("ALTER TABLE jobs ENABLE TRIGGER USER"))
        async with sessions.begin() as session:
            stopped = await advance_agent_paper_cycle(
                session,
                account_id=seed.account_id,
                decision_time=DECISION_TIME,
                idempotency_key="agent-paper-terminal-failure",
            )
            run_count = await session.scalar(
                select(func.count()).select_from(AgentResearchRunRecord)
            )
            job_count = await session.scalar(select(func.count()).select_from(Job))
        assert stopped.status == "unavailable"
        assert stopped.result_code == "RESEARCH_OUTPUT_NOT_ACTIONABLE"
        assert stopped.research_run_ids == first.research_run_ids
        assert run_count == 1
        assert job_count == 1
    finally:
        await engine.dispose()
