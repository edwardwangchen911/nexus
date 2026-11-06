"""Nonempty installed-image compatibility using synthetic local archive bytes only."""

import asyncio
import base64
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from uuid import UUID

import nexus
from helpers.compact_budget_maintenance_image import facts
from helpers.single_sol_maintenance_image import refused
from nexus.agents import research_maintenance
from nexus.api.research_disclosure_monitoring import read_disclosure_monitoring_view
from nexus.monitoring.disclosure_forward import ForwardObservation
from nexus.monitoring.disclosure_observations import read_disclosure_observation
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.monitoring import MonitorPlan
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from nexus.worker import run_agent_worker_once
from sqlalchemy import text
from sqlalchemy.engine import make_url


async def read_image(args):
    url = make_url(args["url"])
    assert url.host == "127.0.0.1" and url.port == 5432
    assert re.fullmatch(r"nexus_test_[0-9a-f]{32}", url.database or "")
    assert Path(nexus.__file__).resolve().is_relative_to("/app/engine/src/nexus")
    assert research_maintenance.IMAGE_MAINTENANCE is args["maintenance"]

    def guard(event, values):
        if event == "socket.connect" and values[1][:2] != ("127.0.0.1", 5432):
            raise PermissionError("only isolated synthetic PostgreSQL")
        if event in {"subprocess.Popen", "os.system", "os.posix_spawn", "os.fork"}:
            raise PermissionError("no child processes")

    sys.addaudithook(guard)

    class Archive:
        async def read_raw(self, key, *, uri):
            assert uri == "s3://synthetic/" + key
            return base64.b64decode(args["objects"][key], validate=True)

        read_evidence = read_raw

    settings = EngineSettings.for_test().model_copy(update={"database_url": args["url"]})
    factory = session_factory(settings)
    factory.kw["info"] = {"sec_exhibit_store": Archive()}
    try:
        assert await facts(factory) == args["facts"]
        async with factory.begin() as db:
            await db.execute(text("SET TRANSACTION READ ONLY"))
            plan = await db.get(MonitorPlan, UUID(args["plan"]))
            run = await db.get(AgentResearchRunRecord, plan.source_run_id)
            now = datetime.fromisoformat(args["now"])
            view = await read_disclosure_observation(db, plan=plan, now=now)
            assert isinstance(view, ForwardObservation) and view.result == "true"
            assert view.comparison_from.isoformat() == args["comparison_from"]
            assert view.old_history_gap == "unknown"
            page = await read_disclosure_monitoring_view(db, run, now=now)
            assert page.latest_check.result == "true"
            assert page.latest_check.prior_history_from < plan.baseline_as_of
            assert page.latest_check.prior_history_unknown is True
            receipt = dict(
                observation=view.model_dump(mode="json"), page=page.model_dump(mode="json")
            )
        if args["maintenance"]:
            await refused(run_agent_worker_once(settings, formal_stage_capacity=True))
        assert await facts(factory) == args["facts"]
        print(json.dumps(receipt))
    finally:
        await factory.kw["bind"].dispose()


async def verify_images(factory, store, captured, plan):
    names = [
        os.environ[k] for k in ("NEXUS_TEST_SEC_FORMAL_IMAGE", "NEXUS_TEST_SEC_MAINTENANCE_IMAGE")
    ]
    assert all(re.fullmatch(r"sha256:[0-9a-f]{64}", n) for n in names)
    url = factory.kw["bind"].url
    assert url.host == "127.0.0.1" and url.port == 56019
    before = await facts(factory)
    args = dict(
        url=url.set(port=5432).render_as_string(hide_password=False),
        facts=before,
        plan=str(plan.id),
        now=captured["clock"][0].isoformat(),
        comparison_from=captured["forward_comparison_from"].isoformat(),
        objects={k: base64.b64encode(v).decode() for k, v in store.objects.items()},
    )
    receipts = []
    for maintenance, image in [(False, names[0]), (True, names[1]), (False, names[0])]:
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
            "from helpers.sec_forward_images import read_image;"
            "asyncio.run(read_image(json.load(sys.stdin)))",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(json.dumps(args | dict(maintenance=maintenance)).encode()), 60
        )
        assert proc.returncode == 0, stderr.decode()
        receipts.append(json.loads(stdout))
    assert receipts[0] == receipts[1] == receipts[2]
    assert await facts(factory) == before
    Path(os.environ["NEXUS_TEST_SEC_IMAGE_EVIDENCE"]).write_text(
        json.dumps(
            dict(
                formal=names[0],
                maintenance=names[1],
                transitions=3,
                facts_unchanged=True,
                forward_successor_read=True,
                old_history_unknown=True,
                maintenance_refused=True,
                mounted_implementation=False,
                external_calls=0,
            )
        )
    )
