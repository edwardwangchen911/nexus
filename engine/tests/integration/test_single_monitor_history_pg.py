"""Copy into integration/: real old acceptances, v6 producer, no model transport.

The unknown case is an explicitly synthetic preparing->unknown event; it checks
the normal proof gate and does not claim provider-timeout or settled-run coverage.
"""

from datetime import timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from helpers.single_sol_clock import preopen_fixture_clock, preopen_target
from integration import test_unknown_preparation_ack_pg as history_fixture
from nexus.ops import manual_paper
from nexus.paper import preparation_unknown_ack, scheduled_unknown_acceptance
from nexus.paper.candidate_batch import load_agent_paper_candidate_batch
from nexus.paper.independent_unknown_acceptance import independent_unknown_accepted
from nexus.paper.independent_unknown_proof import independent_unknown_proof
from nexus.paper.manual_entry import manual_host_scope
from nexus.paper.manual_request import (
    claim_manual_request,
    read_manual_request,
    record_manual_request_event,
)
from nexus.paper.single_sol_owner import binding_for
from nexus.paper.single_sol_research import start_single_sol
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.models.runtime import IdempotencyRecord, WorkerHeartbeat
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from pydantic import SecretStr
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test_v6_retains_both_original_acceptances_and_refuses_substitution(
    migrated_database_url, tmp_path, monkeypatch
):
    probe = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async with probe() as db:
            target = preopen_target(
                await db.scalar(text("SELECT pg_catalog.clock_timestamp()"))
            ).replace(hour=9)
    finally:
        await probe.kw["bind"].dispose()
    monkeypatch.setattr(history_fixture, "AT", (target - timedelta(days=3)).replace(hour=14))

    async def check(factory, settings, old, clock, original_scalar):
        # Preserve the real historical proof builders; only align this isolated
        # database and local helper clocks, as in the existing history test.
        monkeypatch.setattr(AsyncSession, "scalar", original_scalar)
        await preopen_fixture_clock(migrated_database_url, monkeypatch, target=clock[0])
        await factory.kw["bind"].dispose()
        live = settings.model_copy(
            update={
                "agent_daily_budget_usd": Decimal("13"),
                "openai_api_key": SecretStr("sk-proj-" + "synthetic-only" * 5),
            }
        )
        async with factory.begin() as db:
            now = await db.scalar(text("SELECT clock_timestamp()"))
            batch = await load_agent_paper_candidate_batch(
                db, batch_id=live.agent_paper_candidate_batch_id
            )
            assert len(batch.instrument_ids) == 3
            ids = batch.instrument_ids
            preparation = await preparation_unknown_ack._audits(db, live.agent_paper_account_id)
            scheduled = await scheduled_unknown_acceptance._audits(db, live.agent_paper_account_id)
            assert len(preparation) == len(scheduled) == 1
            await preparation_unknown_ack._validate(db, preparation[0], now=now)
            await scheduled_unknown_acceptance._validate(db, scheduled[0], now=now)
            expected_proofs = (preparation[0].event_hash, scheduled[0].event_hash)
            # Daily limit is outside manual_host_scope; retain the native fixture's
            # exact scope and refresh its persisted heartbeat to the fixture clock.
            for heartbeat in await db.scalars(select(WorkerHeartbeat)):
                if heartbeat.worker_id.endswith(":manual:" + manual_host_scope(live)):
                    heartbeat.seen_at = now
            assert await db.scalar(
                select(WorkerHeartbeat.worker_id).where(
                    WorkerHeartbeat.worker_id.endswith(":manual:" + manual_host_scope(live)),
                    WorkerHeartbeat.migration_head == CURRENT_MIGRATION_HEAD,
                )
            )

        async def call(*args):
            return await manual_paper.run(live, manual_paper.parser().parse_args(list(args)))

        async def snapshot(db):
            return {
                table: tuple(
                    await db.scalars(text(f"SELECT to_jsonb(t)::text FROM {table} t ORDER BY 1"))
                )
                for table in (
                    "manual_paper_requests",
                    "manual_paper_request_events",
                    "idempotency_records",
                    "audit_events",
                    "agent_daily_budget_reservations",
                    "agent_research_runs",
                    "research_stage_authorization",
                    "single_sol_budget_scopes",
                    "single_sol_budget_credits",
                )
            }

        from nexus.ops.openai_local import prepare_database
        from unit.ops.test_formal_stage_registration import OPTIONS

        await prepare_database(
            live.model_copy(
                update={
                    "agent_runtime_mode": __import__(
                        "nexus.settings", fromlist=["AgentRuntimeMode"]
                    ).AgentRuntimeMode.CODEX_SDK
                }
            ),
            **(
                OPTIONS
                | dict(
                    max_wire_bytes=100479,
                    full_input_capacity=True,
                    formal_bounded_stage_budget=True,
                    price_monitoring=True,
                    disclosure_monitoring=True,
                )
            ),
        )
        offer = await call("inspect-single-monitor-sol-intent", "--instrument-id", str(ids[0]))
        assert (offer.binding.preparation_proof, offer.binding.scheduled_proof) == expected_proofs
        assert all(expected_proofs)
        async with factory() as db:
            original = await snapshot(db)

        # Same offered hash cannot authorize another selected member.
        with pytest.raises(ValueError, match="^MANUAL_IDEMPOTENCY_CONFLICT$"):
            await call(
                "start-single-monitor-sol",
                "--instrument-id",
                str(ids[1]),
                "--idempotency-key",
                "must-not-create-other-member",
                "--expected-intent-hash",
                offer.intent_hash,
            )
        async with factory() as db:
            assert await snapshot(db) == original

        # Substitute each actual original claim under a rollback-only savepoint.
        # The normal producer must revalidate the original audit before insertion,
        # even though the presented intent hash is unchanged. No proof is mocked.
        for module, field in (
            (preparation_unknown_ack, "claim_ids"),
            (scheduled_unknown_acceptance, "claim_id"),
        ):
            async with factory.begin() as db:
                point = await db.begin_nested()
                audit = (await module._audits(db, live.agent_paper_account_id))[0]
                raw = audit.payload[field]
                claim_id = UUID(raw[0] if isinstance(raw, list) else raw)
                await db.execute(
                    update(IdempotencyRecord)
                    .where(IdempotencyRecord.id == claim_id)
                    .values(response_payload={"isolated_changed_original": True})
                )
                with pytest.raises(ValueError):
                    await start_single_sol(
                        db,
                        live,
                        instrument_id=ids[0],
                        idempotency_key="must-not-create-changed-proof",
                        expected_intent_hash=offer.intent_hash,
                        shared_budget=True,
                        monitoring=True,
                    )
                assert (
                    await db.scalar(
                        text("SELECT count(*) FROM manual_paper_requests WHERE contract_version=6")
                    )
                    == 0
                )
                await point.rollback()
            async with factory() as db:
                assert await snapshot(db) == original

        receipt = await call(
            "start-single-monitor-sol",
            "--instrument-id",
            str(ids[0]),
            "--idempotency-key",
            "v6-both-original-proofs",
            "--expected-intent-hash",
            offer.intent_hash,
        )
        async with factory.begin() as db:
            row = await db.get(ManualPaperRequest, receipt.request_id)
            frozen = binding_for(row)
            assert row.contract_version == 6 and row.mode == "research_and_monitor"
            assert (frozen.preparation_proof, frozen.scheduled_proof) == expected_proofs
            assert row.candidate_batch_id == batch.batch_id and row.batch_hash == batch.batch_hash
            assert (
                await read_manual_request(db, await db.get(ManualPaperRequest, old.request_id))
            ).status == "unknown"
            # Real normal events, synthetic preparation outcome: no model send,
            # no new reservation and no claim of a completed research lifecycle.
            _, claimed = await claim_manual_request(db, request_id=row.id)
            assert claimed
            unknown = await record_manual_request_event(
                db, row, status="unknown", code="MANUAL_RESULT_UNKNOWN"
            )
            assert unknown.status == "unknown"
            now = await db.scalar(text("SELECT clock_timestamp()"))
            assert not await independent_unknown_accepted(db, row, now=now)
            with pytest.raises(ValueError, match="^INDEPENDENT_UNKNOWN_ACCEPTANCE_BLOCKED$"):
                await independent_unknown_proof(db, row=row, now=now)
            # Both real old proofs still validate after the new unknown fact.
            for module in (preparation_unknown_ack, scheduled_unknown_acceptance):
                audit = (await module._audits(db, live.agent_paper_account_id))[0]
                await module._validate(db, audit, now=now)
            assert await db.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 0

    await history_fixture.test_explicit_unknown_batch_is_atomic_and_does_not_replay(
        migrated_database_url,
        monkeypatch,
        "sol-successor",
        after_successor_setup=check,
        successor_at=target,
        history_symbols=("HST1", "HST2", "HST3"),
        include_scheduled_history=True,
    )
