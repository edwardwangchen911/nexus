"""Normal three-stock v10 Job persistence freezes verified SEC T0 or an explicit gap."""

from datetime import timedelta
from uuid import UUID

import pytest
from helpers.current_identity import MemoryStore
from helpers.disclosure_baseline import import_baseline_capture, import_baseline_identity
from integration.test_candidate_multistock_path import AT, _set_test_insert_clock
from integration.test_candidate_multistock_path import (
    test_fresh_multistock_candidate_cycle_freezes_native_jobs as normal_cycle,
)
from nexus.agents.paper_context import read_agent_paper_research_context
from nexus.persistence.models.agents import AgentResearchOutputRecord, AgentResearchRunRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from nexus.strategy.intents import ResearchIntent
from sqlalchemy import select


@pytest.mark.asyncio
async def test_normal_v10_three_stock_worker_freezes_verified_baseline_and_missing_gap(
    migrated_database_url, monkeypatch, tmp_path
):
    store = MemoryStore()
    captured = {}
    monkeypatch.setattr(
        "nexus.monitoring.disclosure_context.configured_exhibit_store", lambda session: store
    )

    async def prepare(settings, sessions, authority, manifest, clock_at):
        historical = AT - timedelta(seconds=2)
        clock_at[0] = historical
        await _set_test_insert_clock(sessions, historical)
        local_settings = settings.model_copy(
            update={"object_store_endpoint": "http://127.0.0.1:9000"}
        )
        identity = await import_baseline_identity(
            local_settings,
            store,
            tmp_path / "identity",
            exchange="NASDAQ",
            received=historical - timedelta(seconds=1),
        )
        assert identity.instrument_id == manifest[0].instrument_id
        captured["baseline"] = await import_baseline_capture(
            local_settings,
            store,
            tmp_path / "index",
            identity=identity,
            exchange="NASDAQ",
            received=historical - timedelta(seconds=1),
        )
        clock_at[0] = AT + timedelta(seconds=1)
        await _set_test_insert_clock(sessions, AT - timedelta(seconds=1))

    await normal_cycle(
        migrated_database_url,
        monkeypatch,
        3,
        False,
        "formal_v11",
        monitoring=True,
        disclosure=True,
        before_research=prepare,
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        runs = tuple(
            await session.scalars(
                select(AgentResearchRunRecord).where(AgentResearchRunRecord.status == "completed")
            )
        )
        assert len(runs) == 3
        ready, missing = 0, 0
        for run in runs:
            assert (
                run.prompt_manifest["portfolio"]["version"]
                == "local-portfolio-synthesis-v10-sec-monitor"
            )
            bound = await read_agent_paper_research_context(
                session,
                snapshot_id=run.data_snapshot_id,
                instrument_id=run.instrument_id,
                as_of=run.decision_time,
            )
            assert bound is not None
            context = bound[0].disclosure_monitoring
            assert context is not None and context.research_as_of == run.decision_time
            output = await session.scalar(
                select(AgentResearchOutputRecord).where(
                    AgentResearchOutputRecord.run_id == run.id,
                    AgentResearchOutputRecord.role.is_(None),
                    AgentResearchOutputRecord.status == "completed",
                )
            )
            intent = ResearchIntent.model_validate(output.intent)
            if context.ready:
                ready += 1
                assert context.baseline.snapshot_id == UUID(captured["baseline"]["snapshot_id"])
                assert (
                    intent.disclosure_proposal.baseline_snapshot_id == context.baseline.snapshot_id
                )
                assert intent.disclosure_proposal.baseline_hash == context.baseline.snapshot_hash
            else:
                missing += 1
                assert context.gap_reason == "SEC_BASELINE_MISSING"
                assert intent.disclosure_proposal is None
            assert (run.input_tokens, run.output_tokens) == (300, 300)
            assert run.settled_cost_usd is not None
        assert (ready, missing) == (1, 2)
        from datetime import UTC, datetime

        import httpx
        from helpers.portfolio_api import app, headers

        engine_app = app(migrated_database_url)
        engine_app.state.utc_clock = lambda: datetime.now(UTC)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=engine_app), base_url="http://test"
        ) as client:
            for run in runs:
                path = f"/internal/v1/research-runs/{run.id}"
                response = await client.get(path, headers=headers("GET", path))
                assert response.status_code == 200, response.text
                assert response.json()["data"]["stage_trace"]["status"] == "verified"
                saved = await session.get(DataSnapshot, run.data_snapshot_id)
                actual = response.json()["data"]["intent"].get("disclosure_proposal")
                expected = saved.payload["agent_paper_context"]["disclosure_monitoring"]["baseline"]
                assert (actual is not None) == (expected is not None)
                if actual is not None:
                    assert actual["baseline_hash"] == expected["snapshot_hash"]

    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_pinned_v9_remains_legacy_when_active_prompt_is_v10(
    migrated_database_url, monkeypatch
):
    from nexus.agents.codex_versions import BOUNDED_STAGE_BUDGET_CONFIG_VERSION
    from nexus.agents.persistence import load_complete_prompt_authority
    from nexus.ops.openai_local import prepare_database
    from nexus.persistence.models.agents import AgentModelConfigRecord
    from unit.ops.test_full_input_capacity_registration import V9_OPTIONS

    async def pin_v9_then_register_v10(settings, sessions, authority, manifest, clock_at):
        async with sessions() as session:
            pinned = await load_complete_prompt_authority(session)
            model = await session.scalar(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version == BOUNDED_STAGE_BUDGET_CONFIG_VERSION
                )
            )
        assert pinned["portfolio"].version == "local-portfolio-synthesis-v9-daily-pending"

        async def protected(session, *, actor_id, idempotency_key):
            return (pinned, model) if actor_id == "local-agent-paper-cycle" else None

        monkeypatch.setattr(
            "nexus.paper.formal_budget_protection.pinned_research_authorities", protected
        )
        await _set_test_insert_clock(sessions, AT - timedelta(milliseconds=500))
        await prepare_database(
            settings,
            **V9_OPTIONS,
            formal_bounded_stage_budget=True,
            price_monitoring=True,
            disclosure_monitoring=True,
        )
        async with sessions() as session:
            current = await load_complete_prompt_authority(session)
            assert current["portfolio"].version == "local-portfolio-synthesis-v10-sec-monitor"

        async def forbidden(*args, **kwargs):
            raise AssertionError("pinned v9 must not acquire a SEC baseline dependency")

        monkeypatch.setattr("nexus.agents.paper_context.freeze_disclosure_baseline", forbidden)

    await normal_cycle(
        migrated_database_url,
        monkeypatch,
        3,
        False,
        "formal_v11",
        monitoring=True,
        before_research=pin_v9_then_register_v10,
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        runs = tuple(await session.scalars(select(AgentResearchRunRecord)))
        assert len(runs) == 3
        for run in runs:
            snapshot = await session.get(DataSnapshot, run.data_snapshot_id)
            assert "disclosure_monitoring" not in snapshot.payload["agent_paper_context"]
            assert (
                run.prompt_manifest["portfolio"]["version"]
                == "local-portfolio-synthesis-v9-daily-pending"
            )
    await factory.kw["bind"].dispose()
