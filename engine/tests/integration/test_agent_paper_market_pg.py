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
from nexus.paper.agent_market import collect_agent_paper_window
from nexus.persistence.models.data import DataArtifact, PriceBarSetAuthority
from nexus.persistence.models.paper import (
    PAPER_DECISION_SOURCE_POLICY_ID,
    PAPER_EXECUTION_SOURCE_POLICY_ID,
    AgentPaperCycleRecord,
    AgentPaperMarketWindowRecord,
    PaperFillRecord,
)
from nexus.providers.base import InstrumentId, MarketDataProvenance, PriceBar, ProviderPage
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


@pytest.mark.asyncio
async def test_no_trade_cycle_persists_one_replayable_non_fill_probe(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    candidate = CandidateManifestEntry(
        instrument_id=INSTRUMENTS[0][0],
        symbol="T1",
        inclusion_reason="quality_rank",
        research_snapshot_id=DATA_SNAPSHOT_ID,
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
