"""Normal v5 lifecycle with isolated HTTP and durable native fault boundaries."""

import json
from datetime import timedelta
from decimal import Decimal

import httpx
import pytest
from integration import test_single_sol_request_pg as legacy
from nexus import worker
from nexus.ops import manual_paper
from nexus.ops.manual_paper_host import claim_configured_manual_request
from nexus.paper.manual_request import read_manual_request, require_manual_account_available
from nexus.paper.research_request import finish_research_request, prepare_research_request
from nexus.persistence.models.manual_paper import ManualPaperRequest
from sqlalchemy import text


async def prepared_owner(url, tmp_path, monkeypatch):
    factory, settings, store, ids, calendar, _ = await legacy.single_scope(
        url, tmp_path, monkeypatch
    )

    async def call(*args):
        return await manual_paper.run(settings, manual_paper.parser().parse_args(list(args)))

    policy = ("--admission-policy", "shared-budget")
    offer = await call("inspect-single-sol-intent", "--instrument-id", str(ids[0]), *policy)
    receipt = await call(
        "start-single-sol",
        "--instrument-id",
        str(ids[0]),
        "--idempotency-key",
        "lifecycle-v5",
        "--expected-intent-hash",
        offer.intent_hash,
        *policy,
    )
    async with factory.begin() as db:
        _, claimed = await claim_configured_manual_request(
            db, settings, request_id=receipt.request_id, calendar_authority_id=calendar
        )
        assert claimed
    async with factory.begin() as db:
        db.info["sec_exhibit_store"] = store
        view = await prepare_research_request(db, settings, request_id=receipt.request_id)
        assert view.status == "active" and len(view.research.members) == 1
    return factory, settings, store, receipt.request_id, view.research.members[0], call, offer


async def hold(db, run_id):
    return (
        await db.execute(
            text(
                "SELECT state,reserved_usd,settled_usd FROM agent_daily_budget_reservations "
                "WHERE run_id=:run"
            ),
            {"run": run_id},
        )
    ).one()


@pytest.mark.asyncio
async def test_unused_cancellation_releases_only_own_reserve_and_allows_new_intent(
    migrated_database_url, tmp_path, monkeypatch
):
    factory, settings, store, request_id, member, call, offer = await prepared_owner(
        migrated_database_url, tmp_path, monkeypatch
    )
    sent, _ = legacy.install_sol_http(monkeypatch, store)
    try:
        async with factory.begin() as db:
            row = await db.get(ManualPaperRequest, request_id)
            view = await finish_research_request(db, row=row, cancel_unused=True)
            assert view.status == "unavailable" and view.research.members[0].usage_known
            assert await hold(db, member.run_id) == ("released", Decimal("2.12192"), None)
            assert await db.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 0
            assert await db.scalar(text("SELECT count(*) FROM single_sol_budget_credits")) == 0
            await require_manual_account_available(
                db,
                account_id=settings.agent_paper_account_id,
                now=await db.scalar(text("SELECT clock_timestamp()")),
            )
        next_offer = await call(
            "inspect-single-sol-intent",
            "--instrument-id",
            str(member.instrument_id),
            "--admission-policy",
            "shared-budget",
        )
        assert next_offer.intent_hash == offer.intent_hash
        assert not await worker.run_agent_worker_once(
            settings, job_ids=(member.job_id,), formal_stage_capacity=True
        )
        assert sent == []
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("committed", [False, True])
async def test_authorization_ack_unknown_never_sends_or_releases_on_lease_expiry(
    migrated_database_url, tmp_path, monkeypatch, committed
):
    from nexus.agents.research_stages.executor import StagedCommitUnconfirmed
    from nexus.agents.research_stages.protocol import StagedProtocol
    from nexus.paper import research_request

    factory, settings, store, request_id, member, _, _ = await prepared_owner(
        migrated_database_url, tmp_path, monkeypatch
    )
    sent, _ = legacy.install_sol_http(monkeypatch, store)
    commit = StagedProtocol._commit
    injected = False
    original_error = RuntimeError("controlled authorization acknowledgement unavailable")

    async def lost(self, session, scope):
        nonlocal injected
        target = (
            not injected
            and await session.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 1
        )
        if target:
            injected = True
            if committed:
                await commit(self, session, scope)
            raise original_error
        await commit(self, session, scope)

    try:
        with monkeypatch.context() as fault:
            fault.setattr(StagedProtocol, "_commit", lost)
            with pytest.raises(
                StagedCommitUnconfirmed, match="AUTHORIZATION_COMMIT_UNCONFIRMED"
            ) as raised:
                await worker.run_agent_worker_once(
                    settings, job_ids=(member.job_id,), formal_stage_capacity=True
                )
        assert raised.value.__cause__ is original_error
        assert sent == [] and injected
        async with factory() as db:
            assert await hold(db, member.run_id) == ("reserved", Decimal("2.12192"), None)
            assert await db.scalar(
                text("SELECT count(*) FROM research_stage_authorization")
            ) == int(committed)
            assert await db.scalar(text("SELECT count(*) FROM research_stage_terminal")) == 0
            end = await db.scalar(text("SELECT expires_at FROM job_leases")) + timedelta(seconds=1)

        # Expiry alone cannot manufacture a zero-send or cleanup proof.
        async def expired(_db):
            return end

        monkeypatch.setattr(research_request, "database_now", expired)
        with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN|NO_ZERO_CALL_PROOF"):
            async with factory.begin() as db:
                row = await db.get(ManualPaperRequest, request_id)
                await finish_research_request(db, row=row, cancel_unused=True)
        async with factory() as db:
            row = await db.get(ManualPaperRequest, request_id)
            assert (await read_manual_request(db, row)).status == "active"
            assert await hold(db, member.run_id) == ("reserved", Decimal("2.12192"), None)
            assert await db.scalar(text("SELECT count(*) FROM research_stage_terminal")) == 0
            with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                await require_manual_account_available(db, account_id=row.account_id, now=end)
        assert sent == []
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("known", [True, False])
async def test_orphan_closes_original_stage_only_after_lease_and_keeps_unknown(
    migrated_database_url, tmp_path, monkeypatch, known
):
    from nexus.agents import platform_native
    from nexus.agents.research_stages import executor
    from nexus.agents.research_stages.proof import read_terminal_proof
    from nexus.paper import research_request
    from unit.agents.test_http_stage_lifecycle import Body, terminal
    from unit.agents.test_platform_usage import usage

    factory, settings, store, request_id, member, _, _ = await prepared_owner(
        migrated_database_url, tmp_path, monkeypatch
    )
    legacy.install_sol_http(monkeypatch, store)
    sends, clients = [], []

    def handle(request):
        sends.append(request)
        if not known:
            raise httpx.ReadTimeout("controlled timeout before terminal")
        value = terminal("incomplete")
        value["response"].update(model="gpt-6.1-sol", service_tier="default", usage=usage())
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=Body(("data: " + json.dumps(value) + "\n\n").encode()),
        )

    def client():
        value = httpx.AsyncClient(transport=httpx.MockTransport(handle))
        clients.append(value)
        return value

    class VanishedWorker(BaseException):
        pass

    finalize = executor.execute_and_finalize_job

    async def vanished(session, **kwargs):
        if await session.scalar(text("SELECT count(*) FROM research_stage_cleanup")):
            raise VanishedWorker
        return await finalize(session, **kwargs)

    monkeypatch.setattr(platform_native, "native_http_client", client)
    try:
        with monkeypatch.context() as fault:
            fault.setattr(executor, "execute_and_finalize_job", vanished)
            with pytest.raises(VanishedWorker):
                await worker.run_agent_worker_once(
                    settings, job_ids=(member.job_id,), formal_stage_capacity=True
                )
        assert len(sends) == 1 and all(c.is_closed for c in clients)
        with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
            async with factory.begin() as db:
                row = await db.get(ManualPaperRequest, request_id)
                await finish_research_request(db, row=row, cancel_unused=True)
        async with factory() as db:
            assert await hold(db, member.run_id) == ("reserved", Decimal("2.12192"), None)
            assert await db.scalar(text("SELECT count(*) FROM research_stage_terminal")) == 0
            end = await db.scalar(text("SELECT expires_at FROM job_leases")) + timedelta(seconds=1)

        async def expired(_db):
            return end

        monkeypatch.setattr(research_request, "database_now", expired)
        async with factory.begin() as db:
            row = await db.get(ManualPaperRequest, request_id)
            view = await finish_research_request(db, row=row, cancel_unused=True)
            assert view.status == ("unavailable" if known else "unknown")
            proof = await read_terminal_proof(db, run_id=member.run_id, now=end)
            assert proof.known is known and proof.orphan and proof.cleanup_confirmed
            assert await hold(db, member.run_id) == (
                ("settled", Decimal("2.12192"), Decimal("0.0299"))
                if known
                else ("reserved", Decimal("2.12192"), None)
            )
            assert (await finish_research_request(db, row=row, cancel_unused=True)) == view
            assert await db.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 1
            assert await db.scalar(text("SELECT count(*) FROM research_stage_terminal")) == 1
            if known:
                await require_manual_account_available(db, account_id=row.account_id, now=end)
            else:
                with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                    await require_manual_account_available(db, account_id=row.account_id, now=end)
        assert len(sends) == 1
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_late_history_refuses_next_stage_but_retains_observation_and_cleanup(
    migrated_database_url, tmp_path, monkeypatch
):
    from nexus.audit.service import append_audit_event
    from nexus.paper import preparation_unknown_ack

    factory, settings, store, request_id, member, call, _ = await prepared_owner(
        migrated_database_url, tmp_path, monkeypatch
    )

    async def late_history(payload):
        assert payload["stage"] == "research"
        async with factory.begin() as db:
            await append_audit_event(
                db,
                actor_id=preparation_unknown_ack.ACTOR,
                action=preparation_unknown_ack.ACTION,
                schema_version=preparation_unknown_ack.SCHEMA,
                payload={"account_id": str(settings.agent_paper_account_id)},
                occurred_at=await db.scalar(text("SELECT clock_timestamp()")),
            )

    sent, clients = legacy.install_sol_http(monkeypatch, store, on_request=late_history)
    try:
        assert await worker.run_agent_worker_once(
            settings, job_ids=(member.job_id,), formal_stage_capacity=True
        )
        assert sent == ["research"] and all(c.is_closed for c in clients)
        async with factory.begin() as db:
            for table in ("authorization", "observation", "verification", "cleanup"):
                assert await db.scalar(text(f"SELECT count(*) FROM research_stage_{table}")) == 1
            assert await hold(db, member.run_id) == (
                "settled",
                Decimal("2.12192"),
                Decimal("0.0299"),
            )
            row = await db.get(ManualPaperRequest, request_id)
            assert (await finish_research_request(db, row=row)).status == "unavailable"
        with pytest.raises(ValueError, match="MANUAL_PREPARATION_ACK_BLOCKED"):
            await call(
                "inspect-single-sol-intent",
                "--instrument-id",
                str(member.instrument_id),
                "--admission-policy",
                "shared-budget",
            )
        assert sent == ["research"]
    finally:
        await factory.kw["bind"].dispose()
