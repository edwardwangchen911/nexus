"""Actual installed image writer with explicit synthetic clock and byte-store fixtures.

This proves schema/write compatibility, not natural operation or an uninjected CLI.
The 8ac rollback retains its older ordinary-daily SEC reconciliation behavior.
"""

import asyncio
import base64
import hashlib
import hmac
import json
import re
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from uuid import UUID

import httpx
import nexus
from nexus.jobs.daily_pipeline import assert_daily_pipeline_schema_ready
from nexus.main import create_engine_app
from nexus.monitoring.daily_v2_consumption import validate_daily_event_receipt
from nexus.ops import research_monitor
from nexus.persistence.models.monitoring import (
    MonitorCoverageReceipt,
    MonitorDailyEventReceipt,
    MonitorEpisode,
    MonitorPlan,
)
from nexus.persistence.models.runtime import AuditEvent
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession
from uuid6 import uuid7

ROOT = Path("/compat")
KEY = "rollback-synthetic-hmac-32-bytes!!"


class OriginalStore:
    def __init__(self, objects):
        self.objects = objects

    async def read_raw(self, key, *, uri):
        assert uri == "s3://synthetic/" + key
        digest = self.objects[key]
        raw = (ROOT / "objects" / digest).read_bytes()
        assert hashlib.sha256(raw).hexdigest() == digest
        return raw

    read_evidence = read_raw

    async def put_if_absent(self, **kwargs):
        raise AssertionError("rollback reconciliation must not write source objects")


async def facts(factory):
    result = {}
    async with factory() as session:
        tables = await session.scalars(
            text("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename")
        )
        for name in tables:
            assert re.fullmatch(r"[a-z_][a-z_0-9]*", name)
            rows = await session.scalars(text(f'SELECT to_jsonb(t)::text FROM "{name}" t'))
            result[name] = Counter(rows)
    return result


async def check_episode_baselines(factory, before, after, now):
    from nexus.agents.paper_context import AgentPaperResearchContext
    from nexus.monitoring.contracts import PriceReviewCondition
    from nexus.monitoring.evaluation import EpisodeState
    from nexus.monitoring.installation import consume_episode_baseline, consumed_baseline
    from nexus.persistence.models.data import DataSnapshot

    def keyed(rows):
        values = [json.loads(row) for row in rows]
        return {(r["account_id"], r["instrument_id"], r["condition_hash"]): r for r in values}

    old = keyed(before[MonitorEpisode.__tablename__])
    new = keyed(after[MonitorEpisode.__tablename__])
    expected = dict(old)
    evidence = []
    added_plans = after[MonitorPlan.__tablename__] - before[MonitorPlan.__tablename__]
    async with factory() as session:
        for raw in added_plans:
            row = json.loads(raw)
            if row["kind"] != "price" or row["predecessor_id"] is None:
                continue
            key = (row["account_id"], row["instrument_id"], row["condition_hash"])
            prior = old.get(key)
            snapshot = await session.get(DataSnapshot, UUID(row["data_snapshot_id"]))
            context = AgentPaperResearchContext.model_validate(
                snapshot.payload["agent_paper_context"]
            )
            baseline = consumed_baseline(
                PriceReviewCondition.model_validate(row["condition"]), context
            )
            state = (
                EpisodeState()
                if prior is None
                else EpisodeState(
                    **{name: prior[name] for name in ("sequence", "latched", "observed_false")}
                )
            )
            updated, applied = consume_episode_baseline(
                state,
                baseline,
                last_checked_at=None
                if prior is None
                else datetime.fromisoformat(prior["checked_at"]),
                observed_at=context.decision_quote.observed_at,
            )
            if not applied:
                continue
            actual = new[key]
            fixed = {"account_id", "instrument_id", "condition_hash", "created_at"}
            if prior is not None:
                assert {k: actual[k] for k in fixed} == {k: prior[k] for k in fixed}
            assert datetime.fromisoformat(actual["checked_at"]) == now
            assert {
                k: actual[k] for k in ("sequence", "latched", "observed_false")
            } == updated.model_dump()
            if state.latched and baseline == "true":
                assert actual["latched"] and actual["sequence"] == state.sequence
            expected[key] = actual
            evidence.append({"key": key, "baseline": baseline, "before": prior, "after": actual})
    assert new == expected
    assert evidence
    return evidence


def headers(path):
    stamp, nonce = str(int(time.time())), str(uuid7())
    actor, request = "rollback-fixture", str(uuid7())
    digest = hashlib.sha256(b"").hexdigest()
    canonical = "\n".join(("GET", path, stamp, nonce, actor, request, digest))
    signature = (
        base64.urlsafe_b64encode(
            hmac.new(KEY.encode(), canonical.encode(), hashlib.sha256).digest()
        )
        .rstrip(b"=")
        .decode()
    )
    return {
        "X-Nexus-Key-Id": "rollback-test",
        "X-Nexus-Timestamp": stamp,
        "X-Nexus-Nonce": nonce,
        "X-Nexus-Body-SHA256": digest,
        "X-Nexus-Signature": signature,
        "X-Nexus-Actor-Id": actor,
        "X-Request-Id": request,
    }


async def main():
    manifest = json.loads((ROOT / "manifest.json").read_text())
    secret = json.load(sys.stdin)
    url = make_url(secret["database_url"])
    assert url.host == "127.0.0.1" and url.port == 5432
    assert re.fullmatch(r"nexus_test_[0-9a-f]{32}", url.database or "")
    assert str(Path(nexus.__file__).resolve()).startswith("/app/engine/src/nexus/")
    now = datetime.fromisoformat(manifest["now"])
    store = OriginalStore(manifest["objects"])
    settings = EngineSettings(
        database_url=secret["database_url"],
        agent_paper_account_id=UUID(manifest["account_id"]),
        agent_paper_candidate_batch_id=UUID(manifest["candidate_batch_id"]),
        NEXUS_ENV="test",
        NEXUS_ENGINE_HMAC_CURRENT_KEY_ID="rollback-test",
        NEXUS_ENGINE_HMAC_CURRENT_SECRET=KEY,
    )
    original_scalar = AsyncSession.scalar

    async def clock(self, statement, *args, **kwargs):
        if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
            return now
        return await original_scalar(self, statement, *args, **kwargs)

    AsyncSession.scalar = clock

    def with_originals(value):
        factory = session_factory(value)
        factory.configure(info={"engine_settings": value, "sec_exhibit_store": store})
        return factory

    research_monitor.session_factory = with_originals
    factory = with_originals(settings)
    application = None
    try:
        async with factory() as session:
            await assert_daily_pipeline_schema_ready(session)
        before = await facts(factory)
        first = await research_monitor.run(
            settings, research_monitor.parser().parse_args(["reconcile"])
        )
        assert first["unverified"] == first["unknown"] == 0, first
        assert first["model_requests"] == first["provider_requests"] == 0
        after = await facts(factory)
        allowed = {
            model.__tablename__
            for model in (MonitorPlan, MonitorCoverageReceipt, MonitorDailyEventReceipt, AuditEvent)
        }
        episode_evidence = await check_episode_baselines(factory, before, after, now)
        changed = {name for name in before if before[name] != after[name]}
        assert changed == allowed | {MonitorEpisode.__tablename__}, changed
        for name in before:
            if name != MonitorEpisode.__tablename__:
                assert not before[name] - after[name], name
        daily_id = UUID(manifest["daily_run_id"])
        async with factory() as session:
            proofs = tuple(
                await session.scalars(
                    select(MonitorDailyEventReceipt).where(
                        MonitorDailyEventReceipt.run_id == daily_id
                    )
                )
            )
            assert len(proofs) == 1 and proofs[0].kind == "price"
            assert str(proofs[0].original_run_id) == manifest["original_run_id"]
            assert await validate_daily_event_receipt(session, proofs[0].id)
            plans = tuple(
                await session.scalars(
                    select(MonitorPlan).where(MonitorPlan.source_run_id == daily_id)
                )
            )
            assert len(plans) == 1 and plans[0].kind == "price"
            assert plans[0].generation == manifest["price_generation"] + 1
            receipts = tuple(
                await session.scalars(
                    select(MonitorCoverageReceipt).where(MonitorCoverageReceipt.run_id == daily_id)
                )
            )
            assert {r.kind: r.status for r in receipts} == {
                "price": "installed",
                "disclosure": "gap",
            }
            assert (
                next(r for r in receipts if r.kind == "disclosure").reason
                == "MONITOR_KIND_EVENTS_UNRESOLVED"
            )
        second = await research_monitor.run(
            settings, research_monitor.parser().parse_args(["reconcile"])
        )
        assert second["unverified"] == 0, second
        assert await facts(factory) == after
        application = create_engine_app(settings)
        await application.state.session_factory.kw["bind"].dispose()
        application.state.session_factory = with_originals(settings)
        application.state.utc_clock = lambda: now
        views = {}
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application), base_url="http://test"
        ) as client:
            for label in ("parent", "daily"):
                path = "/internal/v1/research-runs/" + manifest[label + "_run_id"]
                response = await client.get(path, headers=headers(path))
                assert response.status_code == 200, (label, response.status_code, response.text)
                views[label] = response.json()["data"]
        price = views["parent"]["monitoring"]
        assert price["execution_locked"] and len(price["triggers"]) == 1
        assert price["triggers"][0]["status"] == "covered"
        assert price["triggers"][0]["usage_status"] == "known"
        disclosure = views["daily"]["disclosure_monitoring"]
        assert disclosure["execution_locked"] and disclosure["state"] == "withdrawn"
        assert disclosure["coverage_reason"] == "events_unresolved"
        final = await facts(factory)
        from nexus.persistence.models.runtime import UsedNonce

        nonce_table = UsedNonce.__tablename__
        assert {name for name in after if after[name] != final[name]} == {nonce_table}
        assert sum((final[nonce_table] - after[nonce_table]).values()) == 2
        print(
            json.dumps(
                {
                    "status": "passed",
                    "first": first,
                    "second": second,
                    "changed_tables": sorted(changed),
                    "nonce_rows_added": 2,
                    "episode_baselines": episode_evidence,
                    "source_objects": len(manifest["objects"]),
                    "fixture_clock": manifest["now"],
                    "proof_id": str(proofs[0].id),
                    "plan_id": str(plans[0].id),
                }
            ),
            flush=True,
        )
    finally:
        AsyncSession.scalar = original_scalar
        await factory.kw["bind"].dispose()
        if application is not None:
            await application.state.session_factory.kw["bind"].dispose()


asyncio.run(main())
