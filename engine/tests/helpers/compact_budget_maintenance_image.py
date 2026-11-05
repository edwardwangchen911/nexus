"""Installed images read mixed natural credits without changing old unknown funds."""

import asyncio
import hashlib
import json
import os
import re
import sys
from datetime import timedelta
from pathlib import Path

import nexus
from helpers.single_sol_maintenance_image import refused
from nexus.agents import research_maintenance
from nexus.paper.single_sol_budget import _original_obligation
from nexus.paper.single_sol_compat import inspect_service_budget_scope
from nexus.paper.single_sol_obligations import funded_obligations
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from nexus.worker import run_agent_worker_once, run_portfolio_synthesis_worker_once
from sqlalchemy import text
from sqlalchemy.engine import make_url


async def facts(factory):
    result = {}
    async with factory.begin() as db:
        await db.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        names = await db.scalars(text("SELECT tablename FROM pg_tables WHERE schemaname='public'"))
        for name in names:
            assert re.fullmatch(r"[a-z_][a-z_0-9]*", name)
            result[name] = sorted(
                await db.scalars(text(f'SELECT to_jsonb(r)::text FROM "{name}" r'))
            )
    return hashlib.sha256(json.dumps(result, sort_keys=True).encode()).hexdigest()


async def read_mixed(factory, url, *, maintenance):
    async with factory.begin() as db:
        await db.execute(text("SET TRANSACTION READ ONLY"))
        assert (
            await db.scalar(text("SELECT version_num FROM alembic_version"))
            == (CURRENT_MIGRATION_HEAD)
            == "0119_compact_budget_obligations"
        )
        guard = (
            (
                await db.execute(
                    text("""SELECT g.*,b.candidate_instrument_ids AS members
            FROM single_sol_budget_scopes g JOIN agent_paper_candidate_batches b
            ON b.id=g.batch_id""")
                )
            )
            .mappings()
            .one()
        )
        rows = (
            await db.execute(
                text("""SELECT c.task,c.run_id,
            COALESCE(r.model_config_id,s.model_config_id) AS model_id,
            COALESCE(rb.state,sb.state) AS state
            FROM single_sol_budget_credits c
            LEFT JOIN agent_research_runs r ON r.id=c.run_id
            LEFT JOIN agent_portfolio_synthesis_runs s ON s.id=c.run_id
            LEFT JOIN agent_daily_budget_reservations rb ON rb.run_id=c.run_id
            LEFT JOIN agent_portfolio_synthesis_budget_reservations sb ON sb.run_id=c.run_id
            ORDER BY c.task""")
            )
        ).all()
        assert len(rows) == 4 and all(r.state == "settled" for r in rows)
        selected = {r.model_id for r in rows}
        assert len(selected) == 1 and guard["model_id"] not in selected
        # The synthetic old owner is deliberately unresolved; never release it.
        source = (
            await db.execute(
                text("""SELECT state,settled_usd FROM
            agent_daily_budget_reservations WHERE run_id=:run"""),
                {"run": guard["source_run_id"]},
            )
        ).one()
        assert source.state == "reserved" and source.settled_usd is None
        now = guard["slot"] + timedelta(minutes=5)
        settings = EngineSettings.for_test().model_copy(
            update=dict(
                database_url=url,
                agent_paper_account_id=guard["account_id"],
                agent_paper_candidate_batch_id=guard["batch_id"],
                agent_paper_formal_model_config_id=next(iter(selected)),
                agent_daily_budget_usd=guard["daily_limit"],
            )
        )
        scope = await inspect_service_budget_scope(db, settings, now=now)
        assert scope == {"status": "compact", "remaining_obligations": 1}
        credited = await funded_obligations(
            db,
            now=now,
            account=guard["account_id"],
            batch=guard["batch_id"],
            model=guard["model_id"],
            calendar=guard["calendar_id"],
            members=tuple(guard["members"]),
            maximum=guard["maximum"],
        )
        assert credited == {r.task: r.run_id for r in rows}
        for row in rows:
            kind = "synthesis" if row.task == "synthesis" else "research"
            assert await _original_obligation(db, guard, kind, row.run_id, now) == row.task
    if maintenance:
        await refused(run_agent_worker_once(settings, formal_stage_capacity=True))
        await refused(run_portfolio_synthesis_worker_once(settings, formal_stage_capacity=True))
    return dict(
        scope=scope,
        credits={k: str(v) for k, v in credited.items()},
        old_source_state=source.state,
        old_source_settled=source.settled_usd,
    )


async def run(args):
    url = make_url(args["url"])
    assert url.host == "127.0.0.1" and url.port == 5432
    assert re.fullmatch(r"nexus_test_[0-9a-f]{32}", url.database or "")
    assert Path(nexus.__file__).resolve().is_relative_to("/app/engine/src/nexus")
    assert research_maintenance.IMAGE_MAINTENANCE is args["maintenance"]

    def guard(event, values):
        if event == "socket.connect" and values[1][:2] != ("127.0.0.1", 5432):
            raise PermissionError("only synthetic PostgreSQL is permitted")
        if event in {"subprocess.Popen", "os.system", "os.posix_spawn", "os.fork"}:
            raise PermissionError("no child processes")

    sys.addaudithook(guard)
    factory = session_factory(
        EngineSettings.for_test().model_copy(update={"database_url": args["url"]})
    )
    try:
        assert await facts(factory) == args["facts"]
        result = await read_mixed(factory, args["url"], maintenance=args["maintenance"])
        assert await facts(factory) == args["facts"]
        print(json.dumps(result, sort_keys=True))
    finally:
        await factory.kw["bind"].dispose()


async def verify_images(factory, database_url):
    formal = os.environ.get("NEXUS_TEST_V5_FORMAL_IMAGE")
    maintenance = os.environ.get("NEXUS_TEST_V5_MAINTENANCE_IMAGE")
    if not formal and not maintenance:
        return
    assert formal and maintenance
    assert all(re.fullmatch(r"sha256:[0-9a-f]{64}", x) for x in (formal, maintenance))
    url = make_url(database_url)
    assert url.host == "127.0.0.1" and url.port == 56019
    assert re.fullmatch(r"nexus_test_[0-9a-f]{32}", url.database or "")
    before = await facts(factory)
    expected = await read_mixed(factory, database_url, maintenance=False)
    args = dict(url=url.set(port=5432).render_as_string(hide_password=False), facts=before)
    results = []
    for mode, image in ((False, formal), (True, maintenance), (False, formal)):
        args["maintenance"] = mode
        proc = await asyncio.create_subprocess_exec(
            "docker",
            "run",
            "--rm",
            "--read-only",
            "--tmpfs",
            "/tmp",
            "-i",
            "--network",
            "container:nexus-v4-disposition-offline-20261105",
            "--mount",
            f"type=bind,src={Path(__file__).parent},dst=/compat/helpers,readonly",
            image,
            "python",
            "-B",
            "-c",
            "import sys;sys.path.insert(0,'/compat');import asyncio,json;"
            "from helpers.compact_budget_maintenance_image import run;"
            "asyncio.run(run(json.load(sys.stdin)))",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(json.dumps(args).encode()), 60)
        except TimeoutError:
            proc.terminate()
            await proc.wait()
            raise
        assert proc.returncode == 0, stderr.decode()
        results.append(json.loads(stdout))
    assert results == [expected] * 3
    assert await facts(factory) == before
    output = Path(os.environ["NEXUS_TEST_V5_IMAGE_EVIDENCE"])
    output.mkdir(parents=True, exist_ok=True)
    (output / "mixed-natural.json").write_text(
        json.dumps(
            dict(
                formal=formal,
                maintenance=maintenance,
                transitions=results,
                facts_unchanged=True,
                mounted_source=False,
                external_network=False,
            ),
            indent=2,
        )
    )
