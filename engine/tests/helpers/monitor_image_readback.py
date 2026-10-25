"""Standalone image consumer: mount this file plus a synthetic manifest, never repo code."""

import asyncio
import hashlib
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from uuid import UUID

import nexus
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text
from sqlalchemy.engine import make_url


async def checked(name, operation, results):
    started = time.monotonic()
    try:
        async with asyncio.timeout(10):
            value = await operation
    except TimeoutError:
        results["timeout_reader"] = name
        raise
    finally:
        results.setdefault("reader_seconds", {})[name] = round(time.monotonic() - started, 6)
    if results["reader_seconds"][name] > 10:
        results["timeout_reader"] = name
        raise TimeoutError("read exceeded ten seconds")
    return value


async def main():
    manifest = json.loads(Path("/compat/manifest.json").read_text())
    secret = json.load(sys.stdin)
    url = make_url(secret["database_url"])
    assert url.host == "127.0.0.1" and url.port == 5432
    assert re.fullmatch(r"nexus_test_[0-9a-f]{32}", url.database or "")
    module_path = str(Path(nexus.__file__).resolve())
    assert module_path.startswith("/app/engine/src/nexus/")
    results = {"module_path": module_path, "image_schema": CURRENT_MIGRATION_HEAD}
    factory = session_factory(EngineSettings(database_url=secret["database_url"]))
    try:
        async with factory() as session, session.begin():
            # This is deliberately the first transaction statement.
            await session.execute(text("SET TRANSACTION READ ONLY"))
            assert await session.scalar(text("SHOW transaction_read_only")) == "on"
            results["transaction_read_only"] = "on"
            mode = manifest["mode"]
            if mode == "schema":
                from nexus.jobs.daily_pipeline import assert_daily_pipeline_schema_ready

                expected = manifest["expected_image_schema"]
                assert CURRENT_MIGRATION_HEAD == expected
                try:
                    await checked(
                        "schema_guard", assert_daily_pipeline_schema_ready(session), results
                    )
                except RuntimeError as error:
                    assert str(error) == "DAILY_PIPELINE_DATABASE_HEAD_MISMATCH"
                    assert manifest["expect_mismatch"]
                    results["schema_guard"] = str(error)
                else:
                    assert not manifest["expect_mismatch"]
                    results["schema_guard"] = "ready"
            elif mode == "adoption":
                from nexus.persistence.models.data import DataSnapshot
                from nexus.research.accounting_adoption_storage import payload_bytes, read_adoptions

                before = await checked(
                    "adoption_before",
                    read_adoptions(
                        session,
                        instrument_id=UUID(manifest["instrument_id"]),
                        as_of=datetime.fromisoformat(manifest["before"]),
                    ),
                    results,
                )
                assert before == {}
                found = await checked(
                    "adoption_after",
                    read_adoptions(
                        session,
                        instrument_id=UUID(manifest["instrument_id"]),
                        as_of=datetime.fromisoformat(manifest["after"]),
                        evidence_ids=(UUID(manifest["evidence_id"]),),
                    ),
                    results,
                )
                assert set(map(str, found)) == {manifest["evidence_id"]}
                adopted = found[UUID(manifest["evidence_id"])]
                assert (
                    hashlib.sha256(payload_bytes(adopted)).hexdigest() == manifest["adoption_hash"]
                )
                assert adopted["designation_sha256"] == manifest["designation_hash"]
                for label in ("old", "new"):
                    snapshot = await session.get(DataSnapshot, UUID(manifest[label + "_snapshot"]))
                    digest = hashlib.sha256(
                        json.dumps(snapshot.payload, sort_keys=True).encode()
                    ).hexdigest()
                    assert digest == manifest[label + "_payload_hash"]
                    assert (manifest["evidence_id"] in snapshot.payload["evidence_ids"]) == (
                        label == "new"
                    )
                results["adoption_hash"] = manifest["adoption_hash"]
                results["evidence_ids"] = sorted(map(str, found))
                results["snapshots_unchanged"] = True
            elif mode == "monitor":
                from nexus.api.research_disclosure_monitoring import read_disclosure_monitoring_view
                from nexus.api.research_monitoring import read_monitoring_view
                from nexus.monitoring.daily_v2_consumption import validate_daily_event_receipt
                from nexus.monitoring.mixed_readback import mixed_batch_readback
                from nexus.persistence.models.agents import AgentResearchRunRecord

                now = datetime.fromisoformat(manifest["now"])
                valid = await checked(
                    "daily_event_receipt",
                    validate_daily_event_receipt(session, UUID(manifest["proof_id"])),
                    results,
                )
                assert valid is True
                parent = await session.get(AgentResearchRunRecord, UUID(manifest["parent_run_id"]))
                daily = await session.get(AgentResearchRunRecord, UUID(manifest["daily_run_id"]))
                price = await checked(
                    "price_api", read_monitoring_view(session, parent, now=now), results
                )
                results["price"] = price.model_dump(mode="json")
                assert price.execution_locked
                assert len(price.triggers) == 1
                trigger = price.triggers[0]
                assert trigger.status == "covered" and trigger.usage_status == "known"
                assert str(trigger.run_id) == manifest["daily_run_id"]
                disclosure = await checked(
                    "disclosure_api",
                    read_disclosure_monitoring_view(session, daily, now=now),
                    results,
                )
                results["disclosure"] = disclosure.model_dump(mode="json")
                assert disclosure.state == "withdrawn"
                assert disclosure.coverage_reason == "events_unresolved"
                mixed = await checked(
                    "mixed_batch",
                    mixed_batch_readback(session, UUID(manifest["batch_id"])),
                    results,
                )
                results["mixed"] = mixed
                assert mixed == {
                    "impact_review": "not_verified",
                    "coverage": "gap",
                    "unresolved_event_count": 2,
                }
                results.update(
                    price=price.model_dump(mode="json"),
                    disclosure=disclosure.model_dump(mode="json"),
                    mixed=mixed,
                )
            else:
                raise AssertionError("unknown compatibility scenario")
    except Exception as error:
        results.update(status="failed", error_type=type(error).__name__)
        print(json.dumps(results, sort_keys=True), flush=True)
        return 1
    finally:
        await factory.kw["bind"].dispose()
    results["status"] = "passed"
    print(json.dumps(results, sort_keys=True), flush=True)
    return 0


raise SystemExit(asyncio.run(main()))
