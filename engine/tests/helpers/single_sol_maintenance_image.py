"""Check nonempty v4 receipts through installed formal/maintenance images on fixture PG."""

import asyncio
import hashlib
import json
import re
import sys
from pathlib import Path

import nexus
from nexus.agents import research_maintenance
from nexus.ops import manual_paper
from nexus.ops.manual_paper_host import dispatch_pure_research_request
from nexus.paper.single_sol_owner import binding_for
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from nexus.worker import run_agent_worker_once
from sqlalchemy import select, text
from sqlalchemy.engine import make_url

TABLES = (
    "manual_paper_requests",
    "manual_paper_request_events",
    "research_request_manifests",
    "research_request_members",
    "single_sol_budget_scopes",
    "single_sol_budget_credits",
    "agent_research_runs",
    "agent_research_events",
    "agent_research_outputs",
    "agent_daily_budget_reservations",
    "research_stage_route",
    "research_stage_scope",
    "research_stage_member",
    "research_stage_authorization",
    "research_stage_observation",
    "research_stage_verification",
    "research_stage_cleanup",
    "research_stage_terminal",
    "jobs",
    "job_attempts",
    "job_leases",
    "audit_events",
)


async def frozen_facts(factory):
    all_rows = {}
    async with factory.begin() as db:
        await db.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        for table in TABLES:
            rows = await db.scalars(text(f"SELECT to_jsonb(r)::text FROM {table} r"))
            all_rows[table] = sorted(rows)
    return hashlib.sha256(json.dumps(all_rows, sort_keys=True).encode()).hexdigest()


async def refused(call):
    try:
        await call
    except ValueError as error:
        assert str(error) == "RESEARCH_MAINTENANCE"
    else:
        raise AssertionError("maintenance granted new research")


async def run(args):
    url = make_url(args["url"])
    if (
        url.host != "127.0.0.1"
        or url.port != 5432
        or not re.fullmatch(r"nexus_test_[0-9a-f]{32}", url.database or "")
    ):
        raise ValueError("isolated PostgreSQL required")
    assert Path(nexus.__file__).resolve().is_relative_to("/app/engine/src/nexus")
    assert research_maintenance.IMAGE_MAINTENANCE is args["maintenance"]
    expected = args["status"]
    assert expected in {"queued", "unknown", "completed"}

    def guard(event, values):
        if event == "socket.connect" and values[1][:2] != ("127.0.0.1", 5432):
            raise PermissionError("only synthetic PostgreSQL is permitted")
        if event in {"subprocess.Popen", "os.system", "os.posix_spawn", "os.fork"}:
            raise PermissionError("no child processes")

    sys.addaudithook(guard)
    settings = EngineSettings.for_test().model_copy(update={"database_url": args["url"]})
    factory = session_factory(settings)
    try:
        async with factory() as db:
            head = await db.scalar(text("SELECT version_num FROM alembic_version"))
            assert head == CURRENT_MIGRATION_HEAD == "0116_single_sol_request"
            row = (await db.scalars(select(ManualPaperRequest))).one()
            binding = binding_for(row)
            settings = settings.model_copy(
                update=dict(
                    agent_paper_account_id=row.account_id,
                    agent_paper_candidate_batch_id=row.candidate_batch_id,
                    agent_paper_formal_model_config_id=row.model_config_id,
                    agent_daily_budget_usd=13,
                )
            )

        async def cli(*values):
            return await manual_paper.run(settings, manual_paper.parser().parse_args(values))

        before = await frozen_facts(factory)
        receipt = await cli("read", "--request-id", str(row.id))
        assert receipt.status == expected and receipt.execution_locked and receipt.cycle_id is None
        assert receipt.research_contract == "single_sol_research_v1"
        if expected == "queued":
            assert receipt.research is None
        else:
            (member,) = receipt.research.members
            assert member.instrument_id == binding.instrument_id
            assert member.usage_known == (expected == "completed")
            async with factory() as db:
                state, cost = (
                    await db.execute(
                        text(
                            "SELECT state,settled_usd "
                            "FROM agent_daily_budget_reservations WHERE run_id=:id"
                        ),
                        {"id": member.run_id},
                    )
                ).one()
                assert (
                    state == "reserved" and cost is None
                    if expected == "unknown"
                    else state == "settled" and cost > 0
                )
        if args["maintenance"]:
            await refused(
                cli(
                    "start-single-sol",
                    "--instrument-id",
                    str(binding.instrument_id),
                    "--idempotency-key",
                    "fixture-refused",
                    "--expected-intent-hash",
                    binding.intent_hash,
                )
            )
            await refused(
                dispatch_pure_research_request(
                    settings, request_id=row.id, calendar_authority_id=binding.calendar_id
                )
            )
            if receipt.research is not None:
                await refused(
                    run_agent_worker_once(
                        settings, job_ids=(member.job_id,), formal_stage_capacity=True
                    )
                )
                assert (await cli("finish-research", "--request-id", str(row.id))) == receipt
        assert (await cli("read", "--request-id", str(row.id))) == receipt
        assert await frozen_facts(factory) == before
        print(
            json.dumps(
                dict(
                    status=receipt.status,
                    facts=before,
                    schema=head,
                    receipt=hashlib.sha256(receipt.model_dump_json().encode()).hexdigest(),
                )
            )
        )
    finally:
        await factory.kw["bind"].dispose()


if __name__ == "__main__":
    asyncio.run(run(json.load(sys.stdin)))
