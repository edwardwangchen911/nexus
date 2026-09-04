"""Normal research freezes supplementary price observations without changing price authority."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from helpers.research_snapshots import seed_filing
from nexus.agents.database_tools import database_read_only_tools
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.ops.openai_local import prepare_database
from nexus.persistence.models.data import DataSnapshot, PriceBarRecord, PriceBarSetAuthority
from nexus.research.current_price_trends_storage import persist_current_price_trends
from sqlalchemy import func, select

from .test_current_price_trends import INSTRUMENT, setup_trends


@pytest.mark.asyncio
async def test_normal_prepare_and_exact_price_tool_preserve_supplement_and_old_snapshot(
    migrated_database_url,
    tmp_path,
    monkeypatch,
):
    settings, factory, store, _, archive = await setup_trends(
        migrated_database_url, tmp_path / "archive", monkeypatch
    )
    await prepare_database(settings)
    async with factory.begin() as session:
        await seed_filing(session, instrument_id=INSTRUMENT, seed_instrument=False)
        old = await prepare_research_snapshot(session, INSTRUMENT, as_of=datetime.now(UTC))
        assert old is not None
        old_id, old_revision, old_payload = old.id, old.revision, old.payload
        evidence = await persist_current_price_trends(session, archive, store=store)
        evidence_id = evidence.id
    async with factory.begin() as session:
        new = await prepare_research_snapshot(session, INSTRUMENT, as_of=datetime.now(UTC))
        assert str(evidence_id) in new.payload["evidence_ids"]
        old = await session.get(DataSnapshot, old_id)
        assert (old.revision, old.payload) == (old_revision, old_payload)
        for snapshot, has_trends in ((old, False), (new, True)):
            cutoff = datetime.fromisoformat(snapshot.payload["as_of"])
            request = ResearchRequest(
                instrument_id=INSTRUMENT, data_snapshot_id=snapshot.id, decision_time=cutoff
            )
            admission = await admit_research_run(
                session,
                actor_id="test",
                route="/internal/v1/research-runs",
                idempotency_key=f"trends-{has_trends}",
                request_hash=str(int(has_trends)) * 64,
                request=request,
                prompt_version_id=None,
                model_config_id=None,
                daily_limit_usd=Decimal("3"),
                now=cutoff,
            )
            tools = await database_read_only_tools(
                session, admission.run, allow_fixture_evidence=False
            )
            result = await tools.execute(
                "get_price_features",
                {
                    "instrument_id": str(INSTRUMENT),
                    "data_snapshot_id": str(snapshot.id),
                    "as_of": cutoff.isoformat(),
                    "features": ["close", "sma_20", "sma_200", "rsi_14", "volatility_20"],
                },
            )
            if not has_trends:
                assert result["status"] == "unavailable" and result["items"] == []
                continue
            assert result["status"] == "ok"
            item = result["items"][0]
            assert item["evidence_ref_id"] == str(evidence_id)
            assert item["source"] == "alpaca_current_trends" and item["adjustment"] == "split"
            assert item["features"]["sma_200"] == "100.500000000000"
            assert item["observed_sessions"] == item["contiguous_tail_sessions"] == 200
            assert item["as_of_session"] == item["last_observed_on"]
            assert item["historical_backtest_eligible"] is False
            assert item["market_price_eligible"] is False
            assert item["total_return_eligible"] is False
            assert item["execution_locked"] is True
        assert await session.scalar(select(func.count()).select_from(PriceBarRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarSetAuthority)) == 0


@pytest.mark.asyncio
async def test_trends_alone_do_not_bootstrap_research(migrated_database_url, tmp_path, monkeypatch):
    _, factory, store, _, archive = await setup_trends(
        migrated_database_url, tmp_path / "archive", monkeypatch
    )
    async with factory.begin() as session:
        await persist_current_price_trends(session, archive, store=store)
        assert await prepare_research_snapshot(session, INSTRUMENT, as_of=datetime.now(UTC)) is None
