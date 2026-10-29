"""Every fixed reader uses a real claim and releases its transaction before return."""

from datetime import timedelta

import pytest
from helpers.staged_worker import ShortTools
from nexus.agents.database_tools import database_read_only_tools
from nexus.jobs.models import QueueName
from nexus.jobs.repository import claim_next_job
from nexus.persistence.models.runtime import Job
from sqlalchemy import select, text


@pytest.mark.asyncio
@pytest.mark.parametrize("scenario", ["research", "comparison"])
async def test_all_fixed_tool_positive_results_match_native_frozen_readers(
    migrated_database_url, monkeypatch, scenario
):
    captured = {"calls": []}
    if scenario == "research":
        from integration import test_agent_execution_pg as source
        from nexus.persistence.session import session_factory
        from nexus.settings import EngineSettings

        factory = session_factory(EngineSettings(database_url=migrated_database_url))
        prompts = await source._seed_authority(factory, include_base_evidence=False)
        await source._seed_database_tool_authority(factory, link_price=False, price_source="fosun")
        async with factory.begin() as session:
            admission = await source._admit_price_run(
                session, prompts, allow_fixture_price_authority=False
            )
        run = admission.run
        common = {
            "instrument_id": str(run.instrument_id),
            "data_snapshot_id": str(run.data_snapshot_id),
            "as_of": run.decision_time.isoformat(),
        }
        calls = [
            ("get_security_snapshot", common),
            ("get_financial_facts", {**common, "fields": ["revenue"]}),
            (
                "search_filing_evidence",
                {
                    **common,
                    "forms": ["10-Q"],
                    "filed_from": "2026-01-01",
                    "filed_to": run.decision_time.date().isoformat(),
                },
            ),
            ("get_price_features", {**common, "features": ["close"]}),
            (
                "get_macro_snapshot",
                {
                    "data_snapshot_id": common["data_snapshot_id"],
                    "as_of": common["as_of"],
                    "series": ["DGS10"],
                },
            ),
        ]
        async with factory() as session:
            native = await database_read_only_tools(session, run, allow_fixture_evidence=False)
            for name, payload in calls:
                value = await native.execute(name, payload)
                assert value["status"] == "ok" and value["items"], (name, value)
                captured["calls"].append((name, payload, value))
    else:
        from integration import test_agent_portfolio_context as source
        from nexus.persistence.session import session_factory
        from nexus.settings import EngineSettings

        loader = source.database_read_only_tools

        class Recorded:
            def __init__(self, original):
                self.original = original

            def __getattr__(self, name):
                return getattr(self.original, name)

            async def execute(self, name, payload):
                value = await self.original.execute(name, payload)
                if value["status"] == "ok":
                    captured["calls"].append((name, payload, value))
                return value

        async def record(session, run, **kwargs):
            original = await loader(session, run, **kwargs)
            captured["run"], captured["native"] = run, original
            return Recorded(original)

        monkeypatch.setattr(source, "database_read_only_tools", record)
        await source.test_existing_comparison_is_frozen_for_research_without_creating_proposals(
            migrated_database_url
        )
        run, native = captured["run"], captured["native"]
        factory = session_factory(EngineSettings(database_url=migrated_database_url))
        async with factory() as session:
            native = await database_read_only_tools(session, run, allow_fixture_evidence=False)
            exposure = next(
                value for name, _, value in captured["calls"] if name == "get_portfolio_exposure"
            )
            item = exposure["items"][0]
            payload = {
                "portfolio_snapshot_id": item["portfolio_snapshot_id"],
                "diagnostic_run_id": item["diagnostic_run_id"],
                "as_of": run.decision_time.isoformat(),
            }
            value = await native.execute("get_portfolio_diagnostics", payload)
            assert value["status"] == "ok" and value["items"]
            captured["calls"].append(("get_portfolio_diagnostics", payload, value))
    expected = {
        "research": {
            "get_security_snapshot",
            "get_financial_facts",
            "search_filing_evidence",
            "get_price_features",
            "get_macro_snapshot",
        },
        "comparison": {
            "get_proposal_comparison",
            "get_portfolio_exposure",
            "get_portfolio_diagnostics",
        },
    }[scenario]
    assert {name for name, _, _ in captured["calls"]} == expected
    now = run.decision_time + timedelta(seconds=1)
    try:
        async with factory.begin() as session:
            job = await session.scalar(
                select(Job).where(Job.payload["run_id"].astext == str(run.id))
            )
            assert job is not None
            claim = await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id="short-tool-equivalence",
                now=now,
                job_ids=(job.id,),
            )
            assert claim is not None
        tools = ShortTools(factory, run.id, claim, lambda: now, native)
        for name, payload, original in captured["calls"]:
            assert await tools.execute(name, payload) == original
            async with factory.begin() as observer:
                assert (
                    await observer.scalar(
                        text(
                            "SELECT count(*) FROM pg_stat_activity "
                            "WHERE datname=current_database() "
                            "AND pid<>pg_backend_pid() AND state='idle in transaction'"
                        )
                    )
                    == 0
                )
                # A normal subsequent writer can obtain the same claim row immediately.
                await observer.execute(
                    text("SELECT id FROM jobs WHERE id=:job FOR UPDATE NOWAIT"), {"job": job.id}
                )
        assert set(tools.reads) == expected
    finally:
        await factory.kw["bind"].dispose()
