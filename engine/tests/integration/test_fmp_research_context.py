"""Normally imported profile augments frozen SEC context but cannot bootstrap research."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from helpers.research_snapshots import seed_filing
from nexus.agents.database_tools import database_read_only_tools
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.ops.openai_local import prepare_database
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.data import DataSnapshot, PriceBarSetAuthority
from nexus.persistence.models.research import FinancialFactRecord
from sqlalchemy import func, select


@pytest.mark.asyncio
async def test_normal_import_is_background_in_exact_new_frozen_tool_only(
    migrated_database_url,
    tmp_path,
):
    from nexus.ops.fmp_profile_local import import_profile

    from .test_fmp_profile import INSTRUMENT, setup_profile

    settings, factory, store, _, _ = await setup_profile(
        migrated_database_url, tmp_path / "profile"
    )
    await prepare_database(settings)
    async with factory() as session, session.begin():
        await seed_filing(session, instrument_id=INSTRUMENT, seed_instrument=False)
        old = await prepare_research_snapshot(session, INSTRUMENT, as_of=datetime.now(UTC))
        assert old is not None
        old_id, old_hash = old.id, old.revision
        old_ids = list(old.payload["evidence_ids"])
    imported = await import_profile(
        settings,
        instrument_id=INSTRUMENT,
        symbol="AAPL",
        collection_dir=tmp_path / "profile",
        store=store,
    )
    profile_id = UUID(imported["evidence_id"])
    decision = datetime.now(UTC)
    async with factory() as session, session.begin():
        frozen = await prepare_research_snapshot(session, INSTRUMENT, as_of=decision)
        assert frozen is not None
        assert str(profile_id) in frozen.payload["evidence_ids"]
        old = await session.get(DataSnapshot, old_id)
        assert old.revision == old_hash and old.payload["evidence_ids"] == old_ids
        assert str(profile_id) not in old_ids
        for snapshot, expected_profiles in ((old, 0), (frozen, 1)):
            cutoff = datetime.fromisoformat(snapshot.payload["as_of"])
            request = ResearchRequest(
                instrument_id=INSTRUMENT, data_snapshot_id=snapshot.id, decision_time=cutoff
            )
            admission = await admit_research_run(
                session,
                actor_id="test",
                route="/internal/v1/research-runs",
                idempotency_key=f"fmp-context-{expected_profiles}",
                request_hash=str(expected_profiles) * 64,
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
                "get_security_snapshot",
                {
                    "instrument_id": str(INSTRUMENT),
                    "data_snapshot_id": str(snapshot.id),
                    "as_of": cutoff.isoformat(),
                },
            )
            profiles = [
                row["company_profile"] for row in result["items"] if "company_profile" in row
            ]
            assert len(profiles) == expected_profiles
            if profiles:
                profile = profiles[0]
                assert profile["industry"] == "Vendor Industry"
                assert profile["source_published_at"] is None
                assert profile["sector_taxonomy"] is None
                assert profile["financial_fact_eligible"] is False
                assert profile["sector_risk_eligible"] is False
                assert profile["price_authority_eligible"] is False
                assert profile["historical_backtest_eligible"] is False
                assert profile["execution_locked"] is True
                assert "price" not in profile
        assert await session.scalar(select(func.count()).select_from(FinancialFactRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarSetAuthority)) == 0


@pytest.mark.asyncio
async def test_profile_only_import_does_not_create_research_or_financial_authority(
    migrated_database_url,
    tmp_path,
):
    from nexus.ops.fmp_profile_local import import_profile

    from .test_fmp_profile import INSTRUMENT, setup_profile

    settings, factory, store, _, _ = await setup_profile(
        migrated_database_url, tmp_path / "profile"
    )
    result = await import_profile(
        settings,
        instrument_id=INSTRUMENT,
        symbol="AAPL",
        collection_dir=tmp_path / "profile",
        store=store,
    )
    assert result["status"] == "stored_context"
    async with factory() as session, session.begin():
        assert await prepare_research_snapshot(session, INSTRUMENT, as_of=datetime.now(UTC)) is None
        assert await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(FinancialFactRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarSetAuthority)) == 0
