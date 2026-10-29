"""Tests of a DB-backed isolated protocol, not release acceptance."""

from datetime import timedelta
from decimal import Decimal

import pytest
from helpers.commit_ack_fault import CommitAcknowledgementLost, lose_commit_ack
from helpers.paper_batch import INSTRUMENTS
from helpers.staged_protocol import ProtocolRejected, StagedProtocol
from integration.test_portfolio_synthesis_persistence_pg import seed_synthesis
from nexus.jobs.models import QueueName
from nexus.jobs.repository import LeaseLostError, claim_next_job
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import Job, JobLease
from sqlalchemy import select, text


async def controlled_observe(protocol, ticket, *, now, payload, tokens, valid=True):
    """Test transport supplies its original bytes and usage for explicit verification.

    This is only the primitive test driver; the SDK adapter independently passes
    its original request hash, response and measured usage, never readback data.
    Late observations remain diagnostic, with no verification after lease expiry.
    """
    await protocol.observe(ticket, now=now, payload=payload, tokens=tokens, valid=valid)
    async with protocol.factory() as session:
        row = (
            (
                await session.execute(
                    text(
                        "SELECT a.wire_hash,o.eligible FROM stage_proto_authorization a "
                        "JOIN stage_proto_observation o USING(run_id,stage) "
                        "WHERE a.run_id=:run AND a.stage=:stage"
                    ),
                    {"run": ticket.run_id, "stage": ticket.stage},
                )
            )
            .mappings()
            .one()
        )
    if row["eligible"]:
        await protocol.verify(
            ticket, now=now, wire_hash=row["wire_hash"], payload=payload, tokens=tokens
        )


async def prepared(url):
    engine, factory, _ = await seed_synthesis(
        url, complete_research=False, candidate_instruments=INSTRUMENTS
    )
    async with factory() as session:
        cycle = await session.scalar(select(AgentPaperCycleRecord))
        runs = tuple(
            await session.scalars(
                select(AgentResearchRunRecord).order_by(AgentResearchRunRecord.id)
            )
        )
        jobs = tuple(await session.scalars(select(Job).order_by(Job.id)))
        now = cycle.decision_time + timedelta(seconds=1)
    claims = []
    for job in jobs:
        if "run_id" not in job.payload:
            continue
        async with factory.begin() as session:
            claim = await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id="isolated-stage-owner",
                now=now,
                lease_seconds=120,
                job_ids=(job.id,),
            )
            assert claim is not None
            claims.append(claim)
    protocol = StagedProtocol(factory)
    await protocol.install(
        scope_id=cycle.id,
        claims=claims,
        now=now,
        work_deadline=now + timedelta(seconds=100),
        cleanup_deadline=now + timedelta(seconds=110),
    )
    return engine, factory, protocol, cycle.id, runs, claims, now


@pytest.mark.asyncio
@pytest.mark.parametrize("orphan", [False, True])
@pytest.mark.parametrize("verification", ["missing", "rejected", "trusted"])
async def test_normal_and_orphan_require_same_durable_verification(
    migrated_database_url, orphan, verification
):
    import hashlib

    from helpers.staged_protocol import evidence_digest, stage_evidence

    engine, factory, protocol, scope, runs, claims, now = await prepared(migrated_database_url)
    try:
        ticket = await protocol.authorize(claims[0], stage=1, now=now, wire=b"original-wire")
        await protocol.observe(ticket, now=now, payload=b"original-response", tokens=(10, 10))
        if verification != "missing":
            assert await protocol.verify(
                ticket,
                now=now,
                wire_hash=hashlib.sha256(b"original-wire").hexdigest(),
                payload=b"original-response",
                tokens=(10, 10) if verification == "trusted" else (1, 1),
            ) is (verification == "trusted")
        await protocol.cleanup(ticket, now=now, confirmed=True)
        closed_at = claims[0].expires_at + timedelta(seconds=1) if orphan else now
        assert await protocol.close(claims[0], now=closed_at, orphan=orphan) is (
            verification == "trusted"
        )
        async with factory() as session:
            terminal = (
                (await session.execute(text("SELECT * FROM stage_proto_terminal"))).mappings().one()
            )
            rows = await stage_evidence(session, ticket.run_id, terminal["evidence"])
            assert rows == terminal["evidence"] and evidence_digest(rows) == terminal["chain_hash"]
            budget = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == ticket.run_id
                )
            )
            assert budget.state == ("settled" if verification == "trusted" else "reserved")
            if verification != "trusted":
                assert budget.settled_usd is None
        with pytest.raises((ProtocolRejected, LeaseLostError)):
            await protocol.verify(
                ticket,
                now=closed_at,
                wire_hash=hashlib.sha256(b"original-wire").hexdigest(),
                payload=b"original-response",
                tokens=(10, 10),
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("server_commits", [False, True])
async def test_verification_commit_uncertainty_stops_sends_and_closes_from_durable_facts(
    migrated_database_url, server_commits
):
    import hashlib

    engine, factory, protocol, scope, runs, claims, now = await prepared(migrated_database_url)
    try:
        ticket = await protocol.authorize(claims[0], stage=1, now=now, wire=b"original")
        await protocol.observe(ticket, now=now, payload=b"response", tokens=(10, 10))
        with pytest.raises(CommitAcknowledgementLost):
            with lose_commit_ack(engine, server_commits=server_commits):
                await protocol.verify(
                    ticket,
                    now=now,
                    wire_hash=hashlib.sha256(b"original").hexdigest(),
                    payload=b"response",
                    tokens=(10, 10),
                )
        with pytest.raises(ProtocolRejected, match="ORIGIN_STOPPED"):
            await protocol.authorize(claims[0], stage=2, now=now, wire=b"never-sent")
        assert (
            await protocol.close(
                claims[0], now=claims[0].expires_at + timedelta(seconds=1), orphan=True
            )
            is server_commits
        )
        async with factory() as session:
            budget = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == ticket.run_id
                )
            )
            assert budget.state == ("settled" if server_commits else "reserved")
            assert (
                await session.scalar(text("SELECT stage_proto_inflight(:scope)"), {"scope": scope})
                == 1
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_stage_authority_is_unique_and_only_initial_confirmed_commit_grants_send(
    migrated_database_url,
):
    engine, factory, protocol, scope, runs, claims, now = await prepared(migrated_database_url)
    try:
        ticket = await protocol.authorize(claims[0], stage=1, now=now, wire=b"frozen-stage-one")
        assert ticket is not None
        with pytest.raises(ProtocolRejected, match="ALREADY_AUTHORIZED"):
            await protocol.authorize(claims[0], stage=1, now=now, wire=b"frozen-stage-one")
        async with factory() as session:
            assert await session.scalar(text("SELECT count(*) FROM stage_proto_authorization")) == 1
            assert (await session.get(Job, claims[0].job_id)).attempt_count == 1
            lease = await session.scalar(
                select(JobLease).where(JobLease.job_id == claims[0].job_id)
            )
            assert lease.expires_at == claims[0].expires_at
            hold = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == ticket.run_id
                )
            )
            assert hold.state == "reserved" and hold.reserved_usd == Decimal("0.0002")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["authorization", "observation"])
@pytest.mark.parametrize("server_commits", [False, True])
async def test_commit_ambiguity_preserves_budget_and_stops_later_dispatch(
    migrated_database_url, boundary, server_commits
):
    engine, factory, protocol, scope, runs, claims, now = await prepared(migrated_database_url)
    try:
        ticket = None
        if boundary == "observation":
            ticket = await protocol.authorize(claims[0], stage=1, now=now, wire=b"stage1")
        async with factory() as observer:
            before = tuple(
                (row.id, row.state, row.reserved_usd, row.settled_usd)
                for row in await observer.scalars(
                    select(AgentDailyBudgetReservationRecord).order_by(
                        AgentDailyBudgetReservationRecord.id
                    )
                )
            )
        with pytest.raises(CommitAcknowledgementLost):
            with lose_commit_ack(engine, server_commits=server_commits):
                if boundary == "authorization":
                    await protocol.authorize(claims[0], stage=1, now=now, wire=b"stage1")
                else:
                    await controlled_observe(
                        protocol, ticket, now=now, payload=b"known", tokens=(10, 10)
                    )
        for claim, stage in ((claims[0], 1), (claims[0], 2), (claims[1], 1), (claims[2], 1)):
            with pytest.raises(ProtocolRejected, match="ORIGIN_STOPPED"):
                await protocol.authorize(claim, stage=stage, now=now, wire=b"never-send")
        async with factory() as observer:
            auth = await observer.scalar(text("SELECT count(*) FROM stage_proto_authorization"))
            observation = await observer.scalar(
                text("SELECT count(*) FROM stage_proto_observation")
            )
            assert auth == (1 if boundary == "observation" or server_commits else 0)
            assert observation == (1 if boundary == "observation" and server_commits else 0)
            assert (
                tuple(
                    (row.id, row.state, row.reserved_usd, row.settled_usd)
                    for row in await observer.scalars(
                        select(AgentDailyBudgetReservationRecord).order_by(
                            AgentDailyBudgetReservationRecord.id
                        )
                    )
                )
                == before
            )
            assert all(j.attempt_count == 1 for j in await observer.scalars(select(Job)))
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stages,unknown,cleanup_confirmed",
    [(3, False, True), (1, False, True), (2, True, True), (1, False, False)],
)
async def test_exact_chain_closure_settles_known_calls_and_keeps_unknown_whole_reservation(
    migrated_database_url, stages, unknown, cleanup_confirmed
):
    from nexus.agents.persistence import ActionableResearchError, load_actionable_research_output

    engine, factory, protocol, scope, runs, claims, now = await prepared(migrated_database_url)
    try:
        tickets = []
        for stage in range(1, stages + 1):
            ticket = await protocol.authorize(
                claims[0], stage=stage, now=now, wire=f"stage{stage}".encode()
            )
            tickets.append(ticket)
            if not (unknown and stage == stages):
                await controlled_observe(
                    protocol,
                    ticket,
                    now=now,
                    payload=f"response{stage}".encode(),
                    tokens=(10, 10),
                    valid=stages > 1,
                )
                await protocol.cleanup(ticket, now=now, confirmed=cleanup_confirmed)
                # A response replay is a read, never a second charge.
                await controlled_observe(
                    protocol,
                    ticket,
                    now=now,
                    payload=f"response{stage}".encode(),
                    tokens=(10, 10),
                    valid=stages > 1,
                )
        async with factory() as observer:
            hold = await observer.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == ticket.run_id
                )
            )
            assert hold.state == "reserved" and hold.reserved_usd == Decimal("0.0002")
            with pytest.raises(ActionableResearchError):
                await load_actionable_research_output(
                    observer, run_id=ticket.run_id, decision_time=now
                )
        assert await protocol.close(claims[0], now=now + timedelta(seconds=1)) is (not unknown)
        async with factory() as observer:
            hold = await observer.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == ticket.run_id
                )
            )
            run = await observer.get(AgentResearchRunRecord, ticket.run_id)
            assert (await observer.get(Job, claims[0].job_id)).status == "failed"
            assert run.status == "failed"  # Prototype never fabricates an actionable result.
            if unknown:
                assert hold.state == "reserved" and hold.settled_usd is None
                assert run.settled_cost_usd is None
            else:
                assert hold.state == "settled"
                assert (run.input_tokens, run.output_tokens) == (stages * 10, stages * 10)
                assert hold.settled_usd == Decimal("0.00002") * stages
            with pytest.raises(ActionableResearchError):
                await load_actionable_research_output(
                    observer, run_id=ticket.run_id, decision_time=now
                )
        with pytest.raises(ProtocolRejected, match="STOPPED_OR_DEADLINE"):
            await protocol.authorize(
                claims[0], stage=stages + 1, now=now + timedelta(seconds=2), wire=b"late"
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_batch_stop_blocks_sibling_next_stage_and_third_stock_fill(migrated_database_url):
    engine, factory, protocol, scope, runs, claims, now = await prepared(migrated_database_url)
    try:
        for claim in claims[:2]:
            ticket = await protocol.authorize(claim, stage=1, now=now, wire=b"first-stage")
            await controlled_observe(protocol, ticket, now=now, payload=b"known", tokens=(10, 10))
            await protocol.cleanup(ticket, now=now, confirmed=True)
        await protocol.stop(scope)
        # Independent live owner demonstrates DB stop, not just the local flag.
        peer = StagedProtocol(factory)
        peer.claims = dict(protocol.claims)
        for claim, stage in ((claims[0], 2), (claims[1], 2), (claims[2], 1)):
            with pytest.raises(ProtocolRejected, match="STOPPED_OR_DEADLINE"):
                await peer.authorize(claim, stage=stage, now=now, wire=b"not-sent")
        async with factory() as observer:
            assert (
                await observer.scalar(text("SELECT count(*) FROM stage_proto_authorization")) == 2
            )
            assert all(
                row.state == "reserved"
                for row in await observer.scalars(select(AgentDailyBudgetReservationRecord))
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_orphan_closure_keeps_late_known_observation_separate_from_unknown_budget(
    migrated_database_url,
):
    engine, factory, protocol, scope, runs, claims, now = await prepared(migrated_database_url)
    try:
        ticket = await protocol.authorize(claims[0], stage=1, now=now, wire=b"authorized")
        with pytest.raises(ProtocolRejected, match="ORPHAN_NOT_QUALIFIED"):
            await protocol.close(claims[0], now=now + timedelta(seconds=1), orphan=True)
        expired = claims[0].expires_at + timedelta(seconds=1)
        assert await protocol.close(claims[0], now=expired, orphan=True) is False
        await controlled_observe(
            protocol, ticket, now=expired, payload=b"late-known", tokens=(10, 10)
        )
        await protocol.cleanup(ticket, now=expired, confirmed=True)
        async with factory() as observer:
            run = await observer.get(AgentResearchRunRecord, ticket.run_id)
            hold = await observer.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == ticket.run_id
                )
            )
            assert run.status == "failed" and run.settled_cost_usd is None
            assert hold.state == "reserved" and hold.reserved_usd == Decimal("0.0002")
            assert (await observer.get(Job, claims[0].job_id)).status == "dead_letter"
            assert await observer.scalar(text("SELECT known FROM stage_proto_terminal")) is False
            assert (
                await observer.scalar(text("SELECT input_tokens FROM stage_proto_observation"))
                == 10
            )
        with pytest.raises(ProtocolRejected, match="ALREADY_TERMINAL"):
            await protocol.close(claims[0], now=expired, orphan=True)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_cancel_and_authorization_serialization_preserves_authorized_effects(
    migrated_database_url,
):
    import asyncio

    engine, factory, protocol, scope, runs, claims, now = await prepared(migrated_database_url)
    try:
        # Stop holds the exact scope row before a competing authorization.
        async with factory() as controller:
            await controller.begin()
            await controller.execute(
                text("UPDATE stage_proto_scope SET stopped=true WHERE id=:id"), {"id": scope}
            )
            started = asyncio.Event()
            native = protocol._member

            async def entered(session, claim):
                started.set()
                return await native(session, claim)

            protocol._member = entered
            task = asyncio.create_task(
                protocol.authorize(claims[0], stage=1, now=now, wire=b"blocked")
            )
            await asyncio.wait_for(started.wait(), 5)
            await controller.commit()
            with pytest.raises(ProtocolRejected, match="STOPPED_OR_DEADLINE"):
                await asyncio.wait_for(task, 5)
        async with factory() as observer:
            assert (
                await observer.scalar(text("SELECT count(*) FROM stage_proto_authorization")) == 0
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_intermediate_response_and_cleanup_are_separate_and_cannot_publish(
    migrated_database_url,
):
    from sqlalchemy.exc import DBAPIError

    engine, factory, protocol, scope, runs, claims, now = await prepared(migrated_database_url)
    try:
        ticket = await protocol.authorize(claims[0], stage=1, now=now, wire=b"first")
        await controlled_observe(protocol, ticket, now=now, payload=b"good", tokens=(10, 10))
        # Response has committed while cleanup is still unresolved.
        async with factory() as observer:
            assert (
                await observer.scalar(text("SELECT input_tokens FROM stage_proto_observation"))
                == 10
            )
            assert await observer.scalar(text("SELECT count(*) FROM stage_proto_cleanup")) == 0
        with pytest.raises(DBAPIError) as rejected:
            await protocol.authorize(claims[0], stage=2, now=now, wire=b"second")
        assert rejected.value.orig.sqlstate == "23514"
        await protocol.cleanup(ticket, now=now, confirmed=False)
        with pytest.raises(DBAPIError) as rejected:
            await protocol.authorize(claims[0], stage=2, now=now, wire=b"second")
        assert rejected.value.orig.sqlstate == "23514"
        with pytest.raises(ProtocolRejected, match="OBSERVATION_CONFLICT"):
            await controlled_observe(
                protocol, ticket, now=now, payload=b"contradiction", tokens=(20, 20)
            )
        with pytest.raises(DBAPIError) as rejected:
            async with factory.begin() as session:
                await session.execute(text("UPDATE stage_proto_observation SET input_tokens=0"))
        assert rejected.value.orig.sqlstate == "23514"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_new_owner_cannot_reconstruct_send_permission_from_authorization(
    migrated_database_url,
):
    from dataclasses import replace

    from sqlalchemy.exc import DBAPIError

    engine, factory, protocol, scope, runs, claims, now = await prepared(migrated_database_url)
    try:
        await protocol.authorize(claims[0], stage=1, now=now, wire=b"first")
        reader = StagedProtocol(factory)
        with pytest.raises(ProtocolRejected, match="ORIGIN_STOPPED_OR_UNAVAILABLE"):
            await reader.authorize(claims[0], stage=1, now=now, wire=b"first")
        replaced = replace(claims[0], attempt_no=2)
        with pytest.raises(ProtocolRejected, match="ORIGIN_STOPPED_OR_UNAVAILABLE"):
            await protocol.authorize(replaced, stage=1, now=now, wire=b"first")
        # Even direct SQL cannot create another stage authorization with forged
        # Attempt binding. Immutable identity is independent of attempt number.
        with pytest.raises(DBAPIError) as rejected:
            async with factory.begin() as session:
                await session.execute(
                    text("""INSERT INTO stage_proto_authorization
                    SELECT run_id,stage,job_id,2,worker_id,lease_token,fence_token,
                    reserved_id,wire_hash,schema_hash,prior_hash,authorized_at
                    FROM stage_proto_authorization""")
                )
        assert rejected.value.orig.sqlstate in {"23514", "23505"}
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_two_flight_limit_is_persistent_and_not_just_local_task_count(migrated_database_url):
    from sqlalchemy.exc import DBAPIError

    engine, factory, protocol, scope, runs, claims, now = await prepared(migrated_database_url)
    try:
        for claim in claims[:2]:
            await protocol.authorize(claim, stage=1, now=now, wire=b"flight")
        with pytest.raises(DBAPIError) as rejected:
            await protocol.authorize(claims[2], stage=1, now=now, wire=b"third-flight")
        assert rejected.value.orig.sqlstate == "23514"
        async with factory() as observer:
            assert (
                await observer.scalar(text("SELECT count(*) FROM stage_proto_authorization")) == 2
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_batch_stop_cannot_be_erased_by_later_writer(migrated_database_url):
    from sqlalchemy.exc import DBAPIError

    engine, factory, protocol, scope, runs, claims, now = await prepared(migrated_database_url)
    try:
        await protocol.stop(scope)
        with pytest.raises(DBAPIError) as rejected:
            async with factory.begin() as session:
                await session.execute(text("UPDATE stage_proto_scope SET stopped=false"))
        assert rejected.value.orig.sqlstate == "23514"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["pricing", "deadline", "claim"])
async def test_stage_two_rechecks_pricing_original_deadline_and_native_fence(
    migrated_database_url, changed
):
    from helpers.paper_batch import DECISION_TIME
    from nexus.jobs.repository import fail_job
    from nexus.persistence.models.agents import AgentModelConfigRecord
    from sqlalchemy import event

    def expiring(mapper, connection, target):
        target.pricing_expires_at = DECISION_TIME + timedelta(seconds=50)

    if changed == "pricing":
        event.listen(AgentModelConfigRecord, "before_insert", expiring)
    try:
        engine, factory, protocol, scope, runs, claims, now = await prepared(migrated_database_url)
    finally:
        if changed == "pricing":
            event.remove(AgentModelConfigRecord, "before_insert", expiring)
    try:
        ticket = await protocol.authorize(claims[0], stage=1, now=now, wire=b"first")
        await controlled_observe(protocol, ticket, now=now, payload=b"known", tokens=(10, 10))
        await protocol.cleanup(ticket, now=now, confirmed=True)
        if changed == "claim":
            async with factory.begin() as session:
                await fail_job(
                    session,
                    claim=claims[0],
                    now=now + timedelta(seconds=1),
                    error_code="OWNER_ENDED",
                )
            with pytest.raises(LeaseLostError):
                await protocol.authorize(
                    claims[0], stage=2, now=now + timedelta(seconds=2), wire=b"next"
                )
        else:
            later = now + timedelta(seconds=50 if changed == "pricing" else 100)
            with pytest.raises(
                ProtocolRejected, match="PRICING_EXPIRED" if changed == "pricing" else "DEADLINE"
            ):
                await protocol.authorize(claims[0], stage=2, now=later, wire=b"next")
        async with factory() as observer:
            assert (
                await observer.scalar(text("SELECT count(*) FROM stage_proto_authorization")) == 1
            )
            hold = await observer.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == ticket.run_id
                )
            )
            assert hold.state == "reserved" and hold.settled_usd is None
            lease = await observer.scalar(
                select(JobLease).where(JobLease.job_id == claims[0].job_id)
            )
            assert lease.expires_at == claims[0].expires_at
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_commit_unknown_persistently_stops_other_existing_owner(migrated_database_url):
    engine, factory, protocol, scope, runs, claims, now = await prepared(migrated_database_url)
    peer = StagedProtocol(factory)
    peer.claims = dict(protocol.claims)
    try:
        with pytest.raises(CommitAcknowledgementLost):
            with lose_commit_ack(engine, server_commits=True):
                await protocol.authorize(claims[0], stage=1, now=now, wire=b"uncertain")
        with pytest.raises(ProtocolRejected, match="STOPPED_OR_DEADLINE"):
            await peer.authorize(claims[1], stage=1, now=now, wire=b"must-not-dispatch")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_failed_close_stops_batch_even_when_cleanup_unconfirmed(migrated_database_url):
    engine, factory, protocol, scope, runs, claims, now = await prepared(migrated_database_url)
    try:
        ticket = await protocol.authorize(claims[0], stage=1, now=now, wire=b"first")
        await controlled_observe(
            protocol, ticket, now=now, payload=b"known", tokens=(10, 10), valid=False
        )
        await protocol.cleanup(ticket, now=now, confirmed=False)
        assert await protocol.close(claims[0], now=now + timedelta(seconds=1)) is True
        with pytest.raises(ProtocolRejected, match="STOPPED_OR_DEADLINE"):
            await protocol.authorize(
                claims[1], stage=1, now=now + timedelta(seconds=2), wire=b"second"
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_late_observation_before_orphan_cannot_settle_unknown_budget(migrated_database_url):
    engine, factory, protocol, scope, runs, claims, now = await prepared(migrated_database_url)
    try:
        ticket = await protocol.authorize(claims[0], stage=1, now=now, wire=b"first")
        expired = claims[0].expires_at + timedelta(seconds=1)
        await controlled_observe(protocol, ticket, now=expired, payload=b"late", tokens=(10, 10))
        assert await protocol.close(claims[0], now=expired, orphan=True) is False
        async with factory() as observer:
            hold = await observer.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == ticket.run_id
                )
            )
            assert hold.state == "reserved" and hold.settled_usd is None
            assert (
                await observer.scalar(text("SELECT input_tokens FROM stage_proto_observation"))
                == 10
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("attempt_no", 2),
        ("fence_token", 999),
        ("worker_id", "another-owner"),
        ("job_id", __import__("uuid").UUID(int=99)),
    ],
)
async def test_cleanup_rejects_wrong_origin_binding(migrated_database_url, field, value):
    from dataclasses import replace

    engine, factory, protocol, scope, runs, claims, now = await prepared(migrated_database_url)
    try:
        ticket = await protocol.authorize(claims[0], stage=1, now=now, wire=b"first")
        wrong = replace(ticket, claim=replace(ticket.claim, **{field: value}))
        with pytest.raises(ProtocolRejected, match="CLEANUP_BINDING_INVALID"):
            await protocol.cleanup(wrong, now=now, confirmed=True)
        async with factory() as observer:
            assert await observer.scalar(text("SELECT count(*) FROM stage_proto_cleanup")) == 0
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_known_settlement_does_not_release_unconfirmed_process_slot(migrated_database_url):
    engine, factory, protocol, scope, runs, claims, now = await prepared(migrated_database_url)
    try:
        tickets = []
        for claim in claims[:2]:
            tickets.append(await protocol.authorize(claim, stage=1, now=now, wire=b"flight"))
        await controlled_observe(protocol, tickets[0], now=now, payload=b"known", tokens=(10, 10))
        await protocol.cleanup(tickets[0], now=now, confirmed=False)
        assert await protocol.close(claims[0], now=now + timedelta(seconds=1)) is True
        async with factory() as observer:
            assert (
                await observer.scalar(text("SELECT stage_proto_inflight(:scope)"), {"scope": scope})
                == 2
            )
            hold = await observer.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == tickets[0].run_id
                )
            )
            assert hold.state == "settled" and hold.settled_usd == Decimal("0.00002")
        with pytest.raises(ProtocolRejected):
            await protocol.authorize(claims[2], stage=1, now=now, wire=b"no-third")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("server_commits", [False, True])
async def test_terminal_commit_unknown_does_not_allow_sibling_dispatch(
    migrated_database_url, server_commits
):
    engine, factory, protocol, scope, runs, claims, now = await prepared(migrated_database_url)
    peer = StagedProtocol(factory)
    peer.claims = dict(protocol.claims)
    try:
        ticket = await protocol.authorize(claims[0], stage=1, now=now, wire=b"first")
        await controlled_observe(protocol, ticket, now=now, payload=b"known", tokens=(10, 10))
        await protocol.cleanup(ticket, now=now, confirmed=True)
        with pytest.raises(CommitAcknowledgementLost):
            with lose_commit_ack(engine, server_commits=server_commits):
                await protocol.close(claims[0], now=now + timedelta(seconds=1))
        with pytest.raises(ProtocolRejected, match="STOPPED_OR_DEADLINE"):
            await peer.authorize(
                claims[1], stage=1, now=now + timedelta(seconds=2), wire=b"must-not-send"
            )
        async with factory() as observer:
            assert await observer.scalar(text("SELECT count(*) FROM stage_proto_terminal")) == int(
                server_commits
            )
            hold = await observer.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == ticket.run_id
                )
            )
            assert hold.state == ("settled" if server_commits else "reserved")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("late_offset", [0, 1])
async def test_staged_orphan_qualification_is_readonly_and_late_usage_never_unblocks(
    migrated_database_url, late_offset
):
    from helpers.staged_protocol import require_staged_orphan_proof

    engine, factory, protocol, scope, runs, claims, now = await prepared(migrated_database_url)
    try:
        ticket = await protocol.authorize(claims[0], stage=1, now=now, wire=b"authorized-unknown")
        late = now + timedelta(seconds=150)
        async with factory() as session:
            with pytest.raises(ValueError, match="STAGED_TERMINAL_PROOF_INVALID"):
                await require_staged_orphan_proof(session, run_id=ticket.run_id, now=late)
        await protocol.close(claims[0], now=late, orphan=True)
        async with factory() as session:
            proof = await require_staged_orphan_proof(session, run_id=ticket.run_id, now=late)
            assert proof["unknown"] and proof["requires_explicit_acceptance"]
            assert proof["authorized_stages"] == 1
        await controlled_observe(
            protocol,
            ticket,
            now=late + timedelta(seconds=late_offset),
            payload=b"late-known",
            tokens=(10, 10),
        )
        async with factory() as session:
            after = await require_staged_orphan_proof(
                session, run_id=ticket.run_id, now=late + timedelta(seconds=2)
            )
            assert after == proof
            hold = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == ticket.run_id
                )
            )
            assert hold.state == "reserved" and hold.settled_usd is None
            assert (
                await session.get(AgentResearchRunRecord, ticket.run_id)
            ).settled_cost_usd is None
    finally:
        await engine.dispose()
