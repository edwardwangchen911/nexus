from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from helpers.paper_batch import (
    DATA_SNAPSHOT_ID,
    DECISION_SESSION,
    DECISION_TIME,
    EXECUTION_SESSION,
    INSTRUMENTS,
    UNIVERSE_ID,
    seed_paper_batch,
)
from nexus.paper.agent_cycle_models import CandidateManifestEntry, candidate_manifest_hash
from nexus.paper.agent_market import (
    AgentPaperMarketScope,
    PaperMarketError,
    collect_agent_paper_window,
)
from nexus.persistence.models.data import DataArtifact, PriceBarSetAuthority
from nexus.persistence.models.paper import (
    PAPER_DECISION_SOURCE_POLICY_ID,
    PAPER_EXECUTION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
    AgentPaperCycleRecord,
    AgentPaperMarketWindowRecord,
    PaperFillRecord,
)
from nexus.providers.base import InstrumentId, MarketDataProvenance, PriceBar, ProviderPage
from nexus.providers.market.alpaca import MarketProviderPayloadError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

WINDOW_START = datetime(2026, 8, 27, 13, 30, tzinfo=UTC)
EVALUATED_AT = WINDOW_START + timedelta(minutes=5, seconds=2)


class _Store:
    async def archive_raw(self, key: str, payload: bytes) -> str:
        return f"memory://{key}"

    async def put_normalized(self, key: str, payload: bytes) -> str:
        return f"memory://{key}"


class _Provider:
    calls = 0

    async def fetch_bars(self, request, cursor=None):  # type: ignore[no-untyped-def]
        self.calls += 1
        assert cursor is None
        assert request.start == WINDOW_START
        assert request.end == WINDOW_START + timedelta(minutes=5)
        assert request.interval == "1Min"
        assert request.adjustment == "raw"
        return ProviderPage(
            items=tuple(
                PriceBar(
                    instrument_id=InstrumentId(INSTRUMENTS[0][0]),
                    session=EXECUTION_SESSION,
                    open=Decimal("100"),
                    high=Decimal("101"),
                    low=Decimal("99"),
                    close=Decimal("100.5"),
                    volume=Decimal("1000"),
                    observed_at=WINDOW_START + timedelta(minutes=index),
                    available_at=EVALUATED_AT - timedelta(seconds=1),
                    received_at=EVALUATED_AT - timedelta(seconds=1),
                )
                for index in range(5)
            ),
            next_cursor=None,
            raw_payload=b'{"bars":"real-shape-fixture"}',
            source_request_id="iex-window-1",
            as_of=EVALUATED_AT - timedelta(seconds=1),
            provenance=MarketDataProvenance(
                source="alpaca",
                feed="iex",
                adjustment="raw",
                interval="1Min",
                mode="read_only",
                delivery_mode="real_time",
            ),
        )


class _SchwabProvider(_Provider):
    async def fetch_bars(self, request, cursor=None):  # type: ignore[no-untyped-def]
        page = await super().fetch_bars(request, cursor)
        return page.model_copy(
            update={
                "raw_payload": b'{"candles":"real-shape-fixture"}',
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


class _UnavailableSchwabProvider:
    async def fetch_bars(self, request, cursor=None):  # type: ignore[no-untyped-def]
        del request, cursor
        raise MarketProviderPayloadError("SCHWAB_PRICE_HISTORY_WINDOW_INVALID")


@pytest.mark.asyncio
async def test_no_trade_cycle_persists_one_replayable_non_fill_probe(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    candidate = CandidateManifestEntry(
        instrument_id=INSTRUMENTS[0][0],
        symbol="AAPL",
        inclusion_reason="quality_rank",
        research_snapshot_id=DATA_SNAPSHOT_ID,
        prior_daily_price_authority_id=UUID(int=31),
        decision_quote_id=UUID(int=32),
        decision_trade_id=None,
        decision_artifact_id=UUID(int=33),
        decision_evidence_hash="d" * 64,
        risk_query_authority_ids=(UUID(int=34), UUID(int=35), UUID(int=36)),
    )
    provider = _Provider()
    try:
        async with sessions.begin() as session:
            cycle = AgentPaperCycleRecord(
                id=UUID("71000000-0000-0000-0000-000000000010"),
                account_id=seed.account_id,
                portfolio_id=seed.portfolio_id,
                decision_session=DECISION_SESSION,
                decision_time=DECISION_TIME,
                execution_session=EXECUTION_SESSION,
                universe_snapshot_id=UNIVERSE_ID,
                quality_report_id=None,
                decision_price_policy_id=PAPER_DECISION_SOURCE_POLICY_ID,
                execution_price_policy_id=PAPER_EXECUTION_SOURCE_POLICY_ID,
                candidate_manifest=[candidate.model_dump(mode="json")],
                candidate_hash=candidate_manifest_hash((candidate,)),
                sizing_policy_version="agent_paper_sizing_v1",
                sizing_policy_hash="7" * 64,
                execution_locked=True,
            )
            session.add(cycle)

        first = await collect_agent_paper_window(
            sessions,
            cycle_id=cycle.id,
            instrument_id=candidate.instrument_id,
            window_start=WINDOW_START,
            provider=provider,
            object_store=_Store(),
            evaluated_at=EVALUATED_AT,
        )
        replay = await collect_agent_paper_window(
            sessions,
            cycle_id=cycle.id,
            instrument_id=candidate.instrument_id,
            window_start=WINDOW_START,
            provider=provider,
            object_store=_Store(),
            evaluated_at=EVALUATED_AT,
        )

        assert first.purpose == "acceptance_probe"
        assert first.bar_count == 5
        assert first.replayed is False
        assert replay == first.model_copy(update={"replayed": True})
        assert provider.calls == 1
        async with sessions() as session:
            binding = await session.get(AgentPaperMarketWindowRecord, first.authority_id)
            assert binding is not None
            assert binding.order_id is None
            authority = await session.get(PriceBarSetAuthority, binding.source_authority_id)
            raw = await session.get(DataArtifact, binding.raw_artifact_id)
            assert authority is not None
            assert authority.source_policy_id == PAPER_EXECUTION_SOURCE_POLICY_ID
            assert await session.scalar(select(func.count(PaperFillRecord.id))) == 0
            assert raw is not None
            assert raw.sha256 == binding.raw_content_hash
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_schwab_cycle_persists_one_unknown_delivery_probe(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    candidate = CandidateManifestEntry(
        instrument_id=INSTRUMENTS[0][0],
        symbol="AAPL",
        inclusion_reason="quality_rank",
        research_snapshot_id=DATA_SNAPSHOT_ID,
        prior_daily_price_authority_id=UUID(int=41),
        decision_quote_id=UUID(int=42),
        decision_trade_id=None,
        decision_artifact_id=UUID(int=43),
        decision_evidence_hash="e" * 64,
        risk_query_authority_ids=(UUID(int=44), UUID(int=45), UUID(int=46)),
    )
    cycle_id = UUID("71000000-0000-0000-0000-000000000020")
    scope = AgentPaperMarketScope(
        cycle_id=cycle_id,
        instrument_id=candidate.instrument_id,
        execution_source_policy_id=PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
        window_start=WINDOW_START,
        window_end=WINDOW_START + timedelta(minutes=5),
    )

    async def resolve(*_args, **_kwargs):
        return scope

    monkeypatch.setattr("nexus.paper.agent_market.resolve_agent_paper_market_scope", resolve)
    try:
        async with sessions.begin() as session:
            session.add(
                AgentPaperCycleRecord(
                    id=cycle_id,
                    account_id=seed.account_id,
                    portfolio_id=seed.portfolio_id,
                    decision_session=EXECUTION_SESSION,
                    decision_time=WINDOW_START - timedelta(minutes=5),
                    execution_session=EXECUTION_SESSION,
                    universe_snapshot_id=UNIVERSE_ID,
                    quality_report_id=None,
                    decision_price_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
                    execution_price_policy_id=PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
                    candidate_manifest=[candidate.model_dump(mode="json")],
                    candidate_hash=candidate_manifest_hash((candidate,)),
                    sizing_policy_version="agent_paper_sizing_v1",
                    sizing_policy_hash="8" * 64,
                    execution_locked=True,
                )
            )

        with pytest.raises(PaperMarketError, match="PAPER_MARKET_WINDOW_UNAVAILABLE"):
            await collect_agent_paper_window(
                sessions,
                cycle_id=cycle_id,
                provider=_UnavailableSchwabProvider(),
                object_store=_Store(),
                evaluated_at=EVALUATED_AT,
            )

        result = await collect_agent_paper_window(
            sessions,
            cycle_id=cycle_id,
            provider=_SchwabProvider(),
            object_store=_Store(),
            evaluated_at=EVALUATED_AT,
        )

        assert result.purpose == "acceptance_probe"
        assert (result.source, result.feed, result.delivery_mode) == (
            "schwab",
            "price_history",
            "unknown",
        )
        async with sessions() as session:
            binding = await session.get(AgentPaperMarketWindowRecord, result.authority_id)
            raw = (
                None
                if binding is None
                else await session.get(DataArtifact, binding.raw_artifact_id)
            )
            authority = (
                None
                if binding is None
                else await session.get(PriceBarSetAuthority, binding.source_authority_id)
            )
            assert binding is not None
            assert authority is not None
            assert authority.source_policy_id == PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID
            assert raw is not None
            assert raw.schema_version == "schwab_price_history_raw_v1"
    finally:
        await engine.dispose()
