"""Nonempty v4/v5 formal-maintenance-formal checks using installed image code."""

import asyncio
import hashlib
import json
import os
import re
import sys
from pathlib import Path

import nexus
from helpers.single_sol_maintenance_image import frozen_facts, refused
from nexus.agents import research_maintenance
from nexus.agents.platform_profile import read_sol_business_profile, sol_business_units_version
from nexus.ops import manual_paper
from nexus.ops.manual_paper_host import dispatch_pure_research_request
from nexus.paper.manual_entry import read_configured_manual_request
from nexus.paper.manual_request import read_manual_request
from nexus.paper.single_sol_owner import binding_for
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from nexus.worker import run_agent_worker_once
from sqlalchemy import select, text
from sqlalchemy.engine import make_url


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
    settings = EngineSettings.for_test().model_copy(update={"database_url": args["url"]})
    factory = session_factory(settings)
    try:
        async with factory() as db:
            head = await db.scalar(text("SELECT version_num FROM alembic_version"))
            assert head == CURRENT_MIGRATION_HEAD == "0118_compact_sol_profile"
            rows = list(await db.scalars(select(ManualPaperRequest)))
            assert {row.contract_version for row in rows} == {4, 5}
            assert {str(row.id) for row in rows} == set(args["receipts"])
        before = await frozen_facts(factory)
        async with factory() as db:
            model_facts = list(
                await db.scalars(
                    text("SELECT to_jsonb(m)::text FROM agent_model_configs m ORDER BY id")
                )
            )
            assert model_facts == args["model_facts"]
            for row in rows:
                model = await read_sol_business_profile(db, model_config_id=row.model_config_id)
                assert model is not None
                assert sol_business_units_version(model) == args["model_versions"][str(model.id)]
        results = {}
        for row in rows:
            binding = binding_for(row)
            scoped = settings.model_copy(
                update=dict(
                    agent_paper_account_id=row.account_id,
                    agent_paper_candidate_batch_id=row.candidate_batch_id,
                    agent_paper_formal_model_config_id=row.model_config_id,
                    agent_daily_budget_usd=13,
                )
            )

            async def cli(*values):
                return await manual_paper.run(scoped, manual_paper.parser().parse_args(values))

            receipt = await cli("read", "--request-id", str(row.id))
            assert receipt.model_dump(mode="json") == args["receipts"][str(row.id)], {
                k: (v, args["receipts"][str(row.id)].get(k))
                for k, v in receipt.model_dump(mode="json").items()
                if v != args["receipts"][str(row.id)].get(k)
            }
            assert receipt.execution_locked and receipt.cycle_id is None
            assert receipt.research_contract == "single_sol_research_v1"
            (member,) = receipt.research.members
            assert member.instrument_id == binding.instrument_id
            async with factory() as db:
                state, cost = (
                    await db.execute(
                        text(
                            "SELECT state,settled_usd FROM agent_daily_budget_reservations "
                            "WHERE run_id=:run"
                        ),
                        {"run": member.run_id},
                    )
                ).one()
            if receipt.status == "unknown":
                assert not member.usage_known and state == "reserved" and cost is None
            else:
                assert member.usage_known and state == "settled" and cost > 0
            if args["maintenance"]:
                await refused(
                    cli(
                        "start-single-sol",
                        "--admission-policy",
                        "shared-budget",
                        "--instrument-id",
                        str(binding.instrument_id),
                        "--idempotency-key",
                        "image-refused-new-owner",
                        "--expected-intent-hash",
                        binding.intent_hash,
                    )
                )
                await refused(
                    dispatch_pure_research_request(
                        scoped,
                        request_id=row.id,
                        calendar_authority_id=binding.calendar_id,
                    )
                )
                await refused(
                    run_agent_worker_once(
                        scoped,
                        job_ids=(member.job_id,),
                        formal_stage_capacity=True,
                    )
                )
                closed = await cli("finish-research", "--request-id", str(row.id))
                assert closed.model_dump(mode="json") == args["raw_receipts"][str(row.id)]
            assert await cli("read", "--request-id", str(row.id)) == receipt
            results[str(row.id)] = dict(
                version=row.contract_version,
                status=receipt.status,
                receipt=hashlib.sha256(receipt.model_dump_json().encode()).hexdigest(),
            )
        assert await frozen_facts(factory) == before == args["facts"]
        async with factory() as db:
            assert (
                list(
                    await db.scalars(
                        text("SELECT to_jsonb(m)::text FROM agent_model_configs m ORDER BY id")
                    )
                )
                == model_facts
            )
        print(json.dumps(dict(schema=head, facts=before, requests=results)))
    finally:
        await factory.kw["bind"].dispose()


async def verify_images(factory, settings, *, label):
    formal = os.environ.get("NEXUS_TEST_V5_FORMAL_IMAGE")
    maintenance = os.environ.get("NEXUS_TEST_V5_MAINTENANCE_IMAGE")
    if not formal and not maintenance:
        return
    assert formal and maintenance
    assert all(re.fullmatch(r"sha256:[0-9a-f]{64}", x) for x in (formal, maintenance))
    url = make_url(settings.database_url)
    assert url.host == "127.0.0.1" and url.port == 56019
    assert re.fullmatch(r"nexus_test_[0-9a-f]{32}", url.database or "")
    receipts = {}
    raw_receipts = {}
    model_versions = {}
    async with factory() as db:
        model_facts = list(
            await db.scalars(
                text("SELECT to_jsonb(m)::text FROM agent_model_configs m ORDER BY id")
            )
        )
        for row in await db.scalars(select(ManualPaperRequest)):
            model = await read_sol_business_profile(db, model_config_id=row.model_config_id)
            assert model is not None
            model_versions[str(model.id)] = sol_business_units_version(model)
            raw = await read_manual_request(db, row)
            raw_receipts[str(row.id)] = raw.model_dump(mode="json")
            view = await read_configured_manual_request(db, settings, request_id=row.id)
            receipts[str(row.id)] = view.model_dump(mode="json")
    before = await frozen_facts(factory)
    args = dict(
        url=url.set(port=5432).render_as_string(hide_password=False),
        receipts=receipts,
        raw_receipts=raw_receipts,
        facts=before,
        model_facts=model_facts,
        model_versions=model_versions,
    )
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
            "--tmpfs",
            "/app/Library:rw,mode=1777",
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
            "from helpers.budgeted_single_maintenance_image import run;"
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
        result = json.loads(stdout)
        results.append(result)
    assert results[0] == results[1] == results[2]
    assert await frozen_facts(factory) == before
    output = Path(os.environ["NEXUS_TEST_V5_IMAGE_EVIDENCE"])
    output.mkdir(parents=True, exist_ok=True)
    (output / f"{label}.json").write_text(
        json.dumps(
            dict(
                formal=formal,
                maintenance=maintenance,
                transitions=results,
                mounted_source=False,
                external_network=False,
                facts_unchanged=True,
            ),
            indent=2,
        )
    )


if __name__ == "__main__":
    asyncio.run(run(json.load(sys.stdin)))
