"""Installed images read nonempty new v3 and frozen unknown facts without writes."""

import asyncio
import json
import os
import re
import sys
from pathlib import Path
from uuid import UUID

import nexus
from helpers.compact_budget_maintenance_image import facts
from helpers.single_sol_maintenance_image import refused
from nexus.agents import research_maintenance
from nexus.ops import manual_paper
from nexus.paper.independent_history_scope import binding
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from nexus.worker import run_agent_worker_once
from sqlalchemy.engine import make_url


async def run(args):
    url = make_url(args["url"])
    assert url.host == "127.0.0.1" and url.port == 5432
    assert re.fullmatch(r"nexus_test_[0-9a-f]{32}", url.database or "")
    assert Path(nexus.__file__).resolve().is_relative_to("/app/engine/src/nexus")
    assert research_maintenance.IMAGE_MAINTENANCE is args["maintenance"]

    def guard(event, values):
        if event == "socket.connect" and values[1][:2] != ("127.0.0.1", 5432):
            raise PermissionError("only synthetic PostgreSQL")
        if event in {"subprocess.Popen", "os.system", "os.posix_spawn", "os.fork"}:
            raise PermissionError("no child processes")

    sys.addaudithook(guard)
    settings = EngineSettings.for_test().model_copy(update={"database_url": args["url"]})
    factory = session_factory(settings)
    try:
        assert await facts(factory) == args["facts"]
        async with factory() as db:
            row = await db.get(ManualPaperRequest, UUID(args["new"]))
            assert binding(row).version == "independent_accepted_history_v1"
            settings = settings.model_copy(
                update=dict(
                    agent_paper_account_id=row.account_id,
                    agent_paper_candidate_batch_id=row.candidate_batch_id,
                    agent_paper_formal_model_config_id=row.model_config_id,
                )
            )
        if (row.research_profile or {}).get("budget_priority") is not None:
            from nexus.paper.independent_priority import read_priority

            async with factory.begin() as db:
                from sqlalchemy import text

                await db.execute(text("SET TRANSACTION READ ONLY"))
                grant = await read_priority(db, settings, idempotency_key="full-new-root")
                assert grant.consumed_request_id == row.id and not grant.grant_unconsumed
            if args["maintenance"]:
                await refused(
                    manual_paper.run(
                        settings,
                        manual_paper.parser().parse_args(
                            [
                                "authorize-independent-priority",
                                "--idempotency-key",
                                "never-send",
                                "--accept-priority",
                            ]
                        ),
                    )
                )
        results = []
        for identity, expected in [
            (args["new"], args.get("expected_new", "completed")),
            (args["old"], "unknown"),
        ]:
            receipt = await manual_paper.run(
                settings, manual_paper.parser().parse_args(["read", "--request-id", identity])
            )
            assert receipt.status == expected and receipt.execution_locked
            results.append(receipt.model_dump(mode="json"))
        if args.get("expected_new") == "unknown":
            from nexus.agents.research_stages.proof import read_terminal_proof
            from nexus.agents.research_stages.protocol import ProtocolRejected
            from sqlalchemy import text

            async with factory() as db:
                run_id = await db.scalar(
                    text("SELECT run_id FROM research_stage_terminal WHERE known=false")
                )
                now = await db.scalar(text("SELECT clock_timestamp()"))
                proof = await read_terminal_proof(
                    db, run_id=run_id, now=now, close_empty_unknown=True
                )
                assert proof.empty_authorizations and not proof.cleanup_confirmed
                assert not proof.known and not proof.budget_settled
                try:
                    await read_terminal_proof(db, run_id=run_id, now=now)
                except ProtocolRejected:
                    pass
                else:
                    raise AssertionError("strict reader admitted zero-stage unknown")
        if args["maintenance"]:
            await refused(run_agent_worker_once(settings, formal_stage_capacity=True))
            await refused(
                manual_paper.run(
                    settings, manual_paper.parser().parse_args(["check-independent-admission"])
                )
            )
            from nexus.paper.manual_request import admit_manual_request

            async with factory.begin() as db:
                await refused(
                    admit_manual_request(
                        db,
                        account_id=row.account_id,
                        batch_id=row.candidate_batch_id,
                        model_config_id=row.model_config_id,
                        mode="research_and_monitor",
                        contract_version=3,
                        runtime_mode="responses",
                        idempotency_key="never-send",
                    )
                )
        assert await facts(factory) == args["facts"]
        print(json.dumps(results))
    finally:
        await factory.kw["bind"].dispose()


async def verify_images(factory, database_url, new, old, *, expected_new="completed"):
    names = [
        os.environ[k]
        for k in ("NEXUS_TEST_HISTORY_FORMAL_IMAGE", "NEXUS_TEST_HISTORY_MAINTENANCE_IMAGE")
    ]
    assert all(re.fullmatch(r"sha256:[0-9a-f]{64}", name) for name in names)
    url = make_url(database_url)
    assert url.host == "127.0.0.1" and url.port == 56019
    before = await facts(factory)
    args = dict(
        url=url.set(port=5432).render_as_string(hide_password=False),
        facts=before,
        new=str(new),
        old=str(old),
        expected_new=expected_new,
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
            "from helpers.independent_history_images import run;"
            "asyncio.run(run(json.load(sys.stdin)))",
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
    Path(os.environ["NEXUS_TEST_HISTORY_IMAGE_EVIDENCE"]).write_text(
        json.dumps(
            dict(
                formal=names[0],
                maintenance=names[1],
                facts_unchanged=True,
                old_unknown_preserved=True,
                new_v3_read=True,
                maintenance_refused=True,
                transitions=3,
                mounted_implementation=False,
                external_calls=0,
            )
        )
    )
