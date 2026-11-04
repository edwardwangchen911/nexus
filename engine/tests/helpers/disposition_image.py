"""Exercise installed images against nonempty synthetic disposition histories."""

import asyncio
import base64
import hashlib
import json
import re
import sys
from pathlib import Path

import nexus
from helpers.single_sol_maintenance_image import frozen_facts, refused
from nexus.agents import research_maintenance
from nexus.agents.research_stages.protocol import StagedProtocol
from nexus.ops import manual_paper
from nexus.ops.agent_paper_loop import run_local_agent_paper_loop
from nexus.ops.manual_paper_host import peek_manual_request
from nexus.paper.independent_unknown_acceptance import (
    inspect_or_accept_independent_unknown,
    require_independent_execution_open,
)
from nexus.paper.manual_request import require_manual_request_history
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from nexus.worker import run_agent_worker_once
from sqlalchemy import event, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session


async def probe(args):
    url = make_url(args["url"])
    assert url.host == "127.0.0.1" and url.port == 5432
    assert re.fullmatch(r"nexus_test_[0-9a-f]{32}", url.database or "")
    assert Path(nexus.__file__).resolve().is_relative_to("/app/engine/src/nexus")
    assert research_maintenance.IMAGE_MAINTENANCE is args["maintenance"]
    case = args["case"]
    assert case in {"acknowledged", "late", "v3", "unaccepted"}

    def restrict(event, values):
        if event == "socket.connect" and values[1][:2] != ("127.0.0.1", 5432):
            raise PermissionError("only disposable PostgreSQL allowed")
        if event in {"subprocess.Popen", "os.system", "os.posix_spawn", "os.fork"}:
            raise PermissionError("no child processes allowed")

    sys.addaudithook(restrict)
    reads = set()
    if case == "v3":
        # Restore the synthetic SEC originals with the SQL dump. The image's
        # normal readers verify their exact receipts, hashes and DB bindings.
        originals = args["originals"]

        class RestoredStore:
            async def read_raw(self, key, *, uri):
                content = base64.b64decode(originals[key], validate=True)
                digest = hashlib.sha256(content).hexdigest()
                assert uri == f"s3://synthetic/{key}?versionId={digest}"
                reads.add(key)
                return content

            read_evidence = read_raw

        @event.listens_for(Session, "after_begin")
        def bind_originals(session, _transaction, _connection):
            session.info["sec_exhibit_store"] = RestoredStore()

    settings = EngineSettings.for_test().model_copy(update={"database_url": args["url"]})
    factory = session_factory(settings)
    try:
        before = await frozen_facts(factory)
        async with factory.begin() as db:
            row = (await db.scalars(select(ManualPaperRequest))).one()
            assert row.contract_version == (3 if case == "v3" else 4)
            settings = settings.model_copy(
                update={
                    "agent_paper_account_id": row.account_id,
                    "agent_paper_candidate_batch_id": row.candidate_batch_id,
                    "agent_paper_formal_model_config_id": row.model_config_id,
                }
            )
            now = await db.scalar(text("SELECT clock_timestamp()"))
            count = await db.scalar(
                text(
                    "SELECT count(*) FROM audit_events "
                    "WHERE action='paper.independent_unknown.accepted'"
                )
            )
            assert count == (0 if case == "unaccepted" else 1)

        async def cli(*argv):
            return await manual_paper.run(settings, manual_paper.parser().parse_args(argv))

        receipt = await cli("read", "--request-id", str(row.id))
        assert receipt.status == "unknown" and receipt.execution_locked and receipt.research
        assert any(not m.usage_known for m in receipt.research.members)
        supported = not args["old"] or case == "v3"
        disposition = (
            ("recheck_required" if case == "late" else "unknown_acknowledged")
            if supported and count
            else None
        )
        assert receipt.independent_disposition == disposition
        async with factory.begin() as db:
            try:
                await require_manual_request_history(db, account_id=row.account_id, now=now)
                assert disposition == "unknown_acknowledged"
                assert await peek_manual_request(db, account_id=row.account_id) is None
            except ValueError:
                assert disposition != "unknown_acknowledged"
            if count:
                try:
                    await require_independent_execution_open(db, request_id=row.id)
                except ValueError:
                    pass
                else:
                    raise AssertionError("spent identity opened")
        if args["maintenance"]:
            await refused(
                run_local_agent_paper_loop(
                    settings, account_id=row.account_id, repo_root=Path("/app")
                )
            )
            await refused(
                run_agent_worker_once(
                    settings,
                    job_ids=(receipt.research.members[0].job_id,),
                    formal_stage_capacity=True,
                )
            )
            await refused(StagedProtocol(factory).authorize(None, stage=1, now=now, wire=b""))
            if not args["old"] and case == "unaccepted":
                async with factory.begin() as db:
                    await refused(
                        inspect_or_accept_independent_unknown(
                            db,
                            settings=settings,
                            request_id=row.id,
                            repo_root=Path("/app"),
                            env_file=Path("/app/fixture.env"),
                            accept=True,
                            accept_unknown_effects=True,
                            expected_proof_hash="0" * 64,
                        )
                    )
            close = await cli("finish-research", "--request-id", str(row.id))
            assert close.status == receipt.status and close.research == receipt.research
        assert bool(reads) == (case == "v3")
        assert await frozen_facts(factory) == before
        print(
            json.dumps(
                dict(
                    case=case,
                    facts=before,
                    disposition=disposition,
                    audit_count=count,
                    unknown=True,
                )
            )
        )
    finally:
        await factory.kw["bind"].dispose()


if __name__ == "__main__":
    asyncio.run(probe(json.load(sys.stdin)))
