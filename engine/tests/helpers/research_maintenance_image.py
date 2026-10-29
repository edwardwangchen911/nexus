"""Opt-in installed-image probe; stdin supplies only isolated synthetic fixture inputs.

Mount this file alone at /compat/probe.py, never mount engine/src. Run F(inspect),
R(refuse or finish), F(inspect) against the same disposable database. This does
not create fixtures, run migrations, send models or accept unknown effects.
"""

import asyncio
import hashlib
import json
import re
import sys
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from uuid import UUID, uuid4

import nexus
from nexus.agents import research_maintenance
from nexus.agents.research_stages.proof import read_terminal_proof
from nexus.agents.research_stages.protocol import ProtocolRejected
from nexus.paper.manual_request import admit_manual_request
from nexus.paper.research_request import finish_research_request
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from nexus.worker import run_agent_worker_once
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession

TABLES = (
    "research_stage_scope",
    "research_stage_route",
    "research_stage_member",
    "research_stage_authorization",
    "research_stage_observation",
    "research_stage_verification",
    "research_stage_cleanup",
    "research_stage_terminal",
    "research_stage_prepared_job",
    "agent_research_runs",
    "agent_daily_budget_reservations",
    "jobs",
    "job_attempts",
    "job_leases",
    "manual_paper_requests",
    "manual_paper_request_events",
    "research_request_manifests",
    "research_request_members",
    "research_monitor_increments",
    "research_monitor_triggers",
)


async def snapshot(factory, now):
    async with factory() as session:
        facts = {}
        for table in TABLES:
            facts[table] = sorted(
                await session.scalars(text(f"SELECT to_jsonb(t)::text FROM {table} t"))
            )
        proofs = {}
        for run_id in await session.scalars(text("SELECT run_id FROM research_stage_route")):
            try:
                proof = await read_terminal_proof(session, run_id=run_id, now=now)
                proofs[str(run_id)] = None if proof is None else asdict(proof)
            except ProtocolRejected as error:
                proofs[str(run_id)] = {"blocked": str(error)}
        return {
            "schema": await session.scalar(text("SELECT version_num FROM alembic_version")),
            "facts_hash": hashlib.sha256(json.dumps(facts, sort_keys=True).encode()).hexdigest(),
            "proofs": proofs,
        }


async def main():
    args = json.load(sys.stdin)
    url = make_url(args["settings"]["database_url"])
    assert re.fullmatch(r"nexus_test_[0-9a-f]{32}", url.database or "")
    assert url.host in {"127.0.0.1", "localhost"}
    assert str(Path(nexus.__file__).resolve()).startswith("/app/engine/src/nexus/")
    assert research_maintenance.IMAGE_MAINTENANCE is args["expected_maintenance"]
    action = args.get("action", "inspect")
    assert action in {"inspect", "refuse", "finish", "close"}
    settings = EngineSettings(**args["settings"])
    factory = session_factory(settings)
    now = datetime.fromisoformat(args["now"])
    original_scalar = AsyncSession.scalar

    async def fixture_clock(self, statement, *values, **options):
        if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
            return now
        return await original_scalar(self, statement, *values, **options)

    # Same explicit synthetic clock as the existing installed-image consumer.
    AsyncSession.scalar = fixture_clock
    try:
        before = await snapshot(factory, now)
        assert before["schema"] == "0099_research_stages"
        if action == "refuse":
            assert research_maintenance.IMAGE_MAINTENANCE
            async with factory() as session:
                queued = tuple(
                    await session.scalars(
                        text(
                            "SELECT id FROM jobs WHERE status='queued' "
                            "AND job_type='agent.paper.research'"
                        )
                    )
                )
            assert queued, "fixture must include a queued research identity"
            for job_id in queued:
                try:
                    await run_agent_worker_once(settings, job_ids=(job_id,))
                except ValueError as error:
                    assert str(error) == "RESEARCH_MAINTENANCE"
                else:
                    raise AssertionError("maintenance worker did not refuse")
            async with factory.begin() as session:
                try:
                    await admit_manual_request(
                        session,
                        account_id=settings.agent_paper_account_id,
                        batch_id=settings.agent_paper_candidate_batch_id,
                        model_config_id=settings.agent_paper_formal_model_config_id,
                        mode="research_and_monitor",
                        contract_version=3,
                        idempotency_key=f"maintenance-probe:{uuid4()}",
                    )
                except ValueError as error:
                    assert str(error) == "RESEARCH_MAINTENANCE"
                else:
                    raise AssertionError("maintenance preparation did not refuse")
        elif action == "close":
            from nexus.agents.research_stages.protocol import StagedProtocol
            from nexus.jobs.repository import ClaimedJob
            from pydantic import TypeAdapter

            assert research_maintenance.IMAGE_MAINTENANCE
            claim = TypeAdapter(ClaimedJob).validate_python(args["claim"])
            known = await StagedProtocol(factory).close(
                claim, now=now, orphan=args.get("orphan", False)
            )
            assert known is args["expected_known"]
        elif action == "finish":
            assert research_maintenance.IMAGE_MAINTENANCE
            async with factory.begin() as session:
                row = await session.get(ManualPaperRequest, UUID(args["request_id"]))
                assert row is not None
                await finish_research_request(session, row=row, cancel_unused=True)
        after = await snapshot(factory, now)
        if action not in {"finish", "close"}:
            assert after == before
        print(json.dumps({"action": action, "before": before, "after": after}, default=str))
    finally:
        AsyncSession.scalar = original_scalar
        await factory.kw["bind"].dispose()


asyncio.run(main())
