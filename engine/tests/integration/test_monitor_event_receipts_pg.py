"""Mixed event receipts require original reviews and atomic typed coverage."""

import asyncio
import json
from hashlib import sha256
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

HEAD = "0096_monitor_event_receipts"


async def receipt_table(url):
    engine = create_async_engine(url)
    try:
        async with engine.connect() as conn:
            return await conn.scalar(
                text("SELECT to_regclass('research_monitor_mixed_event_receipts')::text")
            )
    finally:
        await engine.dispose()


def test_empty_receipt_migration_roundtrip(migrated_database_url, monkeypatch):
    assert asyncio.run(receipt_table(migrated_database_url)) is not None
    monkeypatch.setenv("DATABASE_URL", migrated_database_url)
    config = Config(str(Path(__file__).parents[2] / "alembic.ini"))
    command.downgrade(config, "0095_monitor_mixed_increments")
    assert asyncio.run(receipt_table(migrated_database_url)) is None
    command.upgrade(config, "head")
    assert asyncio.run(receipt_table(migrated_database_url)) is not None


@pytest.mark.asyncio
async def test_receipt_canonical_hash_matches_python(migrated_database_url):
    engine = create_async_engine(migrated_database_url)
    try:
        async with engine.connect() as conn:
            for value in (
                {"中文": ["文本😀", 'line\nquote"slash\\', None], "a": True},
                {"z": "\x7f", "a": {"b": 2}},
            ):
                expected = json.dumps(value, sort_keys=True, separators=(",", ":"))
                actual = await conn.scalar(
                    text("SELECT nexus_mixed_receipt_canonical(CAST(:v AS jsonb))"),
                    {"v": json.dumps(value)},
                )
                assert actual == expected
                assert (
                    await conn.scalar(
                        text(
                            "SELECT encode(sha256(convert_to("
                            "nexus_mixed_receipt_canonical(CAST(:v AS jsonb)),'UTF8')),'hex')"
                        ),
                        {"v": json.dumps(value)},
                    )
                    == sha256(expected.encode()).hexdigest()
                )
    finally:
        await engine.dispose()


async def native_result(url, monkeypatch, root, mode):
    import argparse
    from datetime import timedelta
    from uuid import UUID

    from helpers.monitor_materials import prepared_materials
    from helpers.paper_batch import CALENDAR_ID
    from integration.test_candidate_multistock_path import _set_test_insert_clock
    from integration.test_monitor_increment_pg import append_fresh_inputs
    from nexus.agents import execution
    from nexus.ops.research_monitor import run
    from nexus.persistence.models.agents import AgentResearchRunRecord
    from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
    from nexus.persistence.models.data import DataSnapshot
    from nexus.persistence.models.monitoring import MonitorIncrement
    from nexus.persistence.models.runtime import JobLease
    from nexus.settings import AgentRuntimeMode
    from nexus.worker import run_agent_worker_once
    from sqlalchemy import func, select

    for path in ("nexus.persistence.migrations", "nexus.ops.research_monitor"):
        monkeypatch.setattr(path + ".CURRENT_MIGRATION_HEAD", HEAD)
    factory, settings, store, captured, pointer, batch, materials = await prepared_materials(
        url, monkeypatch, root
    )
    async with factory() as session:
        parent = await session.get(AgentResearchRunRecord, pointer.source_run_id)
        later = await session.scalar(select(func.max(JobLease.expires_at))) + timedelta(seconds=1)
    captured["clock"][0] = later
    await _set_test_insert_clock(factory, later)
    await append_fresh_inputs(factory, pointer.instrument_id, later)
    async with factory() as session, session.begin():
        monday = later + timedelta(days=3)
        session.add(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=CALENDAR_ID,
                session=monday.date(),
                decision_at=monday.replace(hour=21),
                market_open_at=monday.replace(hour=13, minute=30),
                market_close_at=monday.replace(hour=20),
            )
        )
    settings = settings.model_copy(
        update={
            "agent_paper_formal_model_config_id": parent.model_config_id,
            "agent_runtime_mode": AgentRuntimeMode.CODEX_SDK,
        }
    )
    admitted = await run(
        settings,
        argparse.Namespace(
            action="admit-batch",
            batch_id=batch,
            materials_snapshot_id=materials,
            calendar_authority_id=CALENDAR_ID,
        ),
    )
    assert admitted["status"] == "admitted"
    async with factory() as session:
        member = await session.scalar(
            select(MonitorIncrement).where(MonitorIncrement.run_id == UUID(admitted["run_id"]))
        )
        frozen = await session.get(DataSnapshot, member.snapshot_id)
    context = frozen.payload["monitor_context"]
    proposal = {"context_hash": member.context_hash, "items": []}
    for event in context["trigger_checks"]:
        content = next(
            (
                x["snapshot_id"]
                for x in context["materials"]["contents"]
                if event["event_key"].endswith(x["archive"]["entry"]["accession_number"])
            ),
            None,
        )
        proposal["items"].append(
            dict(
                trigger_id=event["trigger_id"],
                kind=event["kind"],
                event_key=event["event_key"],
                status="unresolved" if mode == "all_unresolved" else "addressed",
                rationale="原件已审阅😀",
                evidence_refs=[],
                content_snapshot_id=content,
            )
        )
    if mode == "wrong_member":
        proposal["items"][0]["kind"] = "disclosure"

    def digest(v):
        return sha256(json.dumps(v, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    ph = digest(proposal)
    reviews = {
        role: dict(
            proposal_hash=ph, decision="accepted", rationale="独立审阅通过", evidence_refs=[]
        )
        for role in ("bear", "evidence_audit")
    }
    original = execution._append_output

    async def with_original_aux(session, research, **kwargs):
        # This persistence seam supplies future aux fields before their Python schema
        # is integrated. Worker/job/usage are native; normal aux production is tested
        # by the main writer, not claimed by this relational regression.
        role = kwargs["role"]
        payload = dict(kwargs["payload"])
        if role == "market_state":
            payload["mixed_event_proposal"] = proposal
        elif role in reviews:
            reviews[role]["evidence_refs"] = payload["claims"][0]["evidence_refs"][:1]
            payload["mixed_event_review"] = reviews[role]
            if role == "evidence_audit" and mode == "veto":
                payload["vote"] = "oppose"
        elif role == "portfolio":
            payload["mixed_event_adoption_hash"] = ph
        return await original(session, research, **{**kwargs, "payload": payload})

    monkeypatch.setattr(execution, "_append_output", with_original_aux)
    if mode == "unknown":
        from nexus.agents import codex_staged
        from unit.agents.test_codex_sdk import _restore_view

        native = codex_staged.invoke_codex

        async def timeout(payload, **kwargs):
            if _restore_view(payload)["stage"] == "critique":
                raise TimeoutError("synthetic unknown")
            return await native(payload, **kwargs)

        monkeypatch.setattr(codex_staged, "invoke_codex", timeout)
    assert await run_agent_worker_once(
        settings, job_ids=(member.job_id,), formal_stage_capacity=True
    )
    return factory, member, context, ph, digest(reviews), captured["clock"][0]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode",
    [
        "valid",
        "missing_kind",
        "wrong_member",
        "unknown",
        "veto",
        "zero_events_partial",
        "all_unresolved",
    ],
)
async def test_original_mixed_receipts_and_atomic_coverage(
    migrated_database_url, monkeypatch, tmp_path, mode
):
    from nexus.persistence.models.agents import AgentResearchOutputRecord
    from nexus.persistence.models.monitoring import (
        MonitorCoverageReceipt,
        MonitorMixedEventReceipt,
        MonitorTrigger,
    )
    from sqlalchemy import select, update
    from sqlalchemy.exc import DBAPIError

    factory, member, context, ph, rh, now = await native_result(
        migrated_database_url, monkeypatch, tmp_path, mode
    )
    async with factory() as session:
        output = await session.scalar(
            select(AgentResearchOutputRecord)
            .where(
                AgentResearchOutputRecord.run_id == member.run_id,
                AgentResearchOutputRecord.role.is_(None),
            )
            .order_by(AgentResearchOutputRecord.sequence.desc())
        )

    async def consume():
        async with factory() as session, session.begin():
            await session.execute(
                update(MonitorTrigger)
                .where(MonitorTrigger.batch_id == member.batch_id)
                .values(status="completed")
            )
            for kind in (
                ("price",)
                if mode in {"missing_kind", "zero_events_partial"}
                else ("price", "disclosure")
            ):
                session.add(
                    MonitorCoverageReceipt(
                        account_id=member.account_id,
                        instrument_id=member.instrument_id,
                        run_id=member.run_id,
                        output_id=output.id,
                        kind=kind,
                        prior_plan_id=None,
                        active_plan_id=None,
                        research_as_of=member.research_as_of,
                        observed_at=now,
                        status="gap",
                        reason="MONITOR_NO_NEW_PLAN",
                        quality_status=None,
                    )
                )
            for event in (
                []
                if mode in {"zero_events_partial", "all_unresolved"}
                else context["trigger_checks"]
            ):
                session.add(
                    MonitorMixedEventReceipt(
                        account_id=member.account_id,
                        instrument_id=member.instrument_id,
                        batch_id=member.batch_id,
                        trigger_id=event["trigger_id"],
                        run_id=member.run_id,
                        output_id=output.id,
                        materials_snapshot_id=context["materials_snapshot_id"],
                        context_hash=member.context_hash,
                        proposal_hash=ph,
                        review_hash=rh,
                        observed_at=now,
                    )
                )
            await session.flush()
            await session.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))

    try:
        if mode in {"valid", "all_unresolved"}:
            await consume()
            async with factory() as session:
                assert len(tuple(await session.scalars(select(MonitorMixedEventReceipt)))) == (
                    0 if mode == "all_unresolved" else 3
                )
                receipts = tuple(
                    await session.scalars(
                        select(MonitorCoverageReceipt).where(
                            MonitorCoverageReceipt.run_id == member.run_id
                        )
                    )
                )
                assert {r.kind for r in receipts} == {"price", "disclosure"}
            monkeypatch.setenv("DATABASE_URL", migrated_database_url)
            with pytest.raises(DBAPIError, match="MONITOR_EVENT_HISTORY_DOWNGRADE_FORBIDDEN"):
                await asyncio.to_thread(
                    command.downgrade,
                    Config(str(Path(__file__).parents[2] / "alembic.ini")),
                    "0095_monitor_mixed_increments",
                )
        else:
            code = {
                "missing_kind": "COVERAGE",
                "zero_events_partial": "COVERAGE",
                "wrong_member": "ITEMS",
                "unknown": "NATIVE",
                "veto": "REVIEW",
            }[mode]
            with pytest.raises(DBAPIError, match="MONITOR_EVENT_RECEIPT_" + code + "_INVALID"):
                await consume()
            async with factory() as session:
                assert not tuple(await session.scalars(select(MonitorMixedEventReceipt)))
                assert not tuple(
                    await session.scalars(
                        select(MonitorCoverageReceipt).where(
                            MonitorCoverageReceipt.run_id == member.run_id
                        )
                    )
                )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_legacy_price_admission_ignores_mixed_receipt_guard(
    migrated_database_url, monkeypatch
):
    from integration import test_monitor_mixed_db_pg as legacy

    for path in ("nexus.persistence.migrations", "nexus.ops.research_monitor"):
        monkeypatch.setattr(path + ".CURRENT_MIGRATION_HEAD", HEAD)
    monkeypatch.setattr(legacy, "runtime_head", lambda patch: None)
    await legacy.test_legacy_single_price_admission_stays_valid(migrated_database_url, monkeypatch)
