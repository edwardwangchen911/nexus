"""Native Sol authorization and shared terminal accounting, using isolated PG only."""

import hashlib
import json
from datetime import timedelta
from decimal import Decimal

import pytest
from integration.test_sol_native_preparation_pg import admit, scope
from nexus.agents.codex_formal_stage import formal_stage_job_qualified
from nexus.agents.codex_versions import BOUNDED_STAGE_BUDGET_CONFIG_VERSION
from nexus.agents.platform_native import native_prepared_material, read_native_allocation
from nexus.agents.platform_wire import compile_native_platform_stage
from nexus.agents.research_stages.proof import read_terminal_proof
from nexus.agents.research_stages.protocol import StagedProtocol, stage_evidence, usage_known
from nexus.agents.research_stages.routes import VERSION
from nexus.agents.research_usage import research_reserved_occupancy
from nexus.jobs.models import QueueName
from nexus.jobs.repository import claim_next_job
from nexus.paper.research_request import prepare_research_request
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentModelConfigRecord,
    AgentResearchRunRecord,
)
from sqlalchemy import select, text
from unit.agents.test_platform_usage import usage


async def prepared_stage(url, tmp_path):
    factory, settings, store, _ = await scope(url, tmp_path)
    request_id = await admit(factory, settings, store)
    async with factory.begin() as session:
        session.info["sec_exhibit_store"] = store
        result = await prepare_research_request(session, settings, request_id=request_id)
        member = result.research.members[0]
        run = await session.get(AgentResearchRunRecord, member.run_id)
        model = await session.get(AgentModelConfigRecord, run.model_config_id)
        binding, allocation = await read_native_allocation(session, run=run)
        compiler, request, refs, inputs = await native_prepared_material(
            session, run=run, model=model
        )
        compiled = compile_native_platform_stage(
            input_hash=allocation.input_hash,
            allocation=allocation,
            expected_allocation_hash=allocation.allocation_hash,
            stage="research",
            prior_usage=[],
            compiled_request=compiler.build_stage_request(
                stage="research",
                request=request,
                evidence_refs=refs,
                frozen_inputs=inputs,
                previous=[],
                financial_table_candidate=True,
            ),
            price_record=model.platform_profile["price_record"],
        )
    async with factory.begin() as session:
        now = await session.scalar(text("SELECT clock_timestamp()"))
        await session.execute(
            text("SELECT set_config('nexus.stage_protocol',:v,true)"), {"v": VERSION}
        )
        claim = await claim_next_job(
            session,
            queue=QueueName.GENERAL,
            worker_id="sol-offline",
            now=now,
            job_ids=(member.job_id,),
            research_budget_profile=BOUNDED_STAGE_BUDGET_CONFIG_VERSION,
            locked_job_guard=formal_stage_job_qualified,
            lease_seconds=330,
        )
        assert claim is not None
    protocol = StagedProtocol(factory)
    await protocol.install(
        scope_id=request_id,
        claims=(claim,),
        now=now,
        independent=True,
        work_deadline=now + timedelta(seconds=300),
        cleanup_deadline=now + timedelta(seconds=307),
    )
    return factory, member, model, claim, protocol, now, compiled


@pytest.mark.asyncio
@pytest.mark.parametrize("orphan", [False, True])
@pytest.mark.parametrize("known", [True, False])
async def test_sol_normal_and_orphan_consume_identical_original_price(
    migrated_database_url, tmp_path, orphan, known
):
    factory, member, model, claim, protocol, now, compiled = await prepared_stage(
        migrated_database_url, tmp_path
    )
    try:
        ticket = await protocol.authorize(
            claim, stage=1, now=now, wire=compiled.wire, platform_wire=compiled
        )
        raw = dict(model="gpt-6.1-sol", service_tier="default", status="completed", usage=usage())
        if not known:
            del raw["usage"]["input_tokens_details"]["cache_write_tokens"]
        payload = json.dumps(raw).encode()
        await protocol.observe(ticket, now=now, payload=payload, tokens=(12000, 1000))
        assert await protocol.verify(
            ticket,
            now=now,
            wire_hash=hashlib.sha256(compiled.wire).hexdigest(),
            payload=payload,
            tokens=(12000, 1000),
        )
        await protocol.cleanup(ticket, now=now, confirmed=True)
        end = claim.expires_at + timedelta(seconds=1) if orphan else now + timedelta(seconds=1)
        assert await protocol.close(claim, now=end, orphan=orphan) is known
        async with factory() as session:
            rows = await stage_evidence(session, member.run_id)
            assert rows[0]["platform_binding"]["version"] == 2
            assert usage_known(rows, model=model) is known
            proof = await read_terminal_proof(session, run_id=member.run_id, now=end)
            assert proof is not None
            hold = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == member.run_id
                )
            )
            if known:
                assert hold.state == "settled" and hold.settled_usd == Decimal("0.0299")
            else:
                assert hold.state == "reserved" and hold.settled_usd is None
                assert await research_reserved_occupancy(session, hold) == hold.reserved_usd
            assert await session.scalar(text("SELECT count(*) FROM research_comparison_chain")) == 0
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("audit_veto", [False, True])
async def test_normal_worker_sends_native_sol_three_stages_and_settles_original_price(
    migrated_database_url, tmp_path, monkeypatch, audit_veto
):
    import httpx
    from nexus import worker
    from nexus.agents import platform_native
    from unit.agents.test_codex_sdk import _assessment_payload, _restore_view, _staged_batch
    from unit.agents.test_http_stage_lifecycle import Body, terminal

    factory, settings, store, _ = await scope(migrated_database_url, tmp_path)
    request_id = await admit(factory, settings, store)
    async with factory.begin() as session:
        session.info["sec_exhibit_store"] = store
        result = await prepare_research_request(session, settings, request_id=request_id)
    requests, clients = [], []

    def handle(request):
        body = json.loads(request.content)
        assert body["model"] == "gpt-6.1-sol" and body["reasoning"] == {"effort": "medium"}
        assert body["store"] is False and body["tools"] == []
        payload = _restore_view(json.loads(body["input"][1]["content"]))
        requests.append(payload["stage"])
        if payload["stage"] == "synthesis":
            response = _assessment_payload(payload["previous_claims"])
            response["synthesis_assessment"]["stance"] = "watch"
            response.update(
                monitoring_adoption_hash=None,
                disclosure_adoption_hash=None,
                daily_pending_adoption_hash=None,
            )
        else:
            response = _staged_batch(payload["stage"], payload["requested_roles"])
            suffix = "proposal" if payload["stage"] == "research" else "review"
            for role in response["roles"]:
                if audit_veto and role["role"] == "evidence_audit":
                    role["vote"] = "abstain"
                role.update(
                    {
                        f"monitoring_{suffix}": None,
                        f"disclosure_{suffix}": None,
                        f"daily_pending_{suffix}": None,
                    }
                )
        value = terminal()
        value["response"].update(model="gpt-6.1-sol", service_tier="default", usage=usage())
        value["response"]["output"][0]["content"][0]["text"] = json.dumps(response)
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=Body(("data: " + json.dumps(value) + "\n\n").encode()),
        )

    def client():
        value = httpx.AsyncClient(transport=httpx.MockTransport(handle))
        clients.append(value)
        return value

    monkeypatch.setattr(platform_native, "native_http_client", client)
    monkeypatch.setattr(
        "nexus.research.sec_exhibit_storage.S3ProviderEvidenceStore", lambda *_: store
    )
    monkeypatch.setattr(
        "nexus.monitoring.disclosure_context.configured_exhibit_store", lambda s: store
    )
    try:
        for member in result.research.members:
            assert await worker.run_agent_worker_once(
                settings, job_ids=(member.job_id,), formal_stage_capacity=True
            )
            async with factory() as session:
                run = await session.get(AgentResearchRunRecord, member.run_id)
                rows = await stage_evidence(session, run.id)
                facts = (
                    await session.execute(
                        text(
                            "SELECT state,details FROM agent_research_events "
                            "WHERE run_id=:run ORDER BY sequence"
                        ),
                        {"run": run.id},
                    )
                ).all()
                assert len(rows) == 3, (run.status, facts, rows)
                assert run.status == "completed", (run.status, facts)
                assert run.settled_cost_usd == Decimal("0.0897")
                intent = await session.scalar(
                    text(
                        "SELECT intent FROM agent_research_outputs "
                        "WHERE run_id=:run AND status='completed'"
                    ),
                    {"run": run.id},
                )
                assert intent["synthesis_assessment"]["stance"] == "watch"
                assert intent["stance"] == "watch" and intent["exposure_action"] == "none"
                if audit_veto:
                    assert intent["evidence_audit"] == "partial"
                proof = await read_terminal_proof(
                    session,
                    run_id=run.id,
                    now=await session.scalar(text("SELECT clock_timestamp()")),
                )
                assert proof.known and proof.cleanup_confirmed and not proof.orphan
        assert requests == ["research", "critique", "synthesis"] * 3
        assert len(clients) == 9 and all(c.is_closed for c in clients)
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field",
    [
        "version",
        "price_record_hash",
        "model_hash",
        "credential_binding_hash",
        "allocation_hash",
        "input_hash",
        "input_upper_bound",
    ],
)
async def test_sql_rejects_mismatched_native_stage_binding(
    migrated_database_url, tmp_path, monkeypatch, field
):
    from nexus.agents import platform_stage_accounting as accounting
    from sqlalchemy.exc import DBAPIError

    factory, member, model, claim, protocol, now, compiled = await prepared_stage(
        migrated_database_url, tmp_path
    )
    original = accounting.platform_authorization_binding

    async def tamper(*args, **kwargs):
        binding = await original(*args, **kwargs)
        binding[field] = (
            1
            if field == "version"
            else binding[field] + 1
            if field == "input_upper_bound"
            else "0" * 64
        )
        return binding

    monkeypatch.setattr(accounting, "platform_authorization_binding", tamper)
    try:
        with pytest.raises(DBAPIError, match="PLATFORM_NATIVE_AUTHORIZATION_INVALID"):
            await protocol.authorize(
                claim, stage=1, now=now, wire=compiled.wire, platform_wire=compiled
            )
        async with factory() as session:
            assert (
                await session.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 0
            )
            hold = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == member.run_id
                )
            )
            assert hold.state == "reserved" and hold.settled_usd is None
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_late_sol_observation_cannot_reprice_or_release_a_frozen_unknown(
    migrated_database_url, tmp_path
):
    factory, member, model, claim, protocol, now, compiled = await prepared_stage(
        migrated_database_url, tmp_path
    )
    try:
        ticket = await protocol.authorize(
            claim, stage=1, now=now, wire=compiled.wire, platform_wire=compiled
        )
        await protocol.cleanup(ticket, now=now, confirmed=True)
        assert await protocol.close(claim, now=now + timedelta(seconds=1)) is False
        async with factory() as session:
            proof = await read_terminal_proof(
                session, run_id=member.run_id, now=now + timedelta(seconds=1)
            )
        raw = json.dumps(
            dict(model="gpt-6.1-sol", service_tier="default", status="completed", usage=usage())
        ).encode()
        await protocol.observe(
            ticket, now=now + timedelta(seconds=2), payload=raw, tokens=(12000, 1000)
        )
        async with factory() as session:
            assert (
                await read_terminal_proof(
                    session, run_id=member.run_id, now=now + timedelta(days=8)
                )
                == proof
            )
            hold = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == member.run_id
                )
            )
            assert hold.state == "reserved" and hold.settled_usd is None
            assert await research_reserved_occupancy(session, hold) == hold.reserved_usd
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("price_tier_known", [True, False])
async def test_verified_sol_overrun_fences_scope_and_preserves_original_hold(
    migrated_database_url, tmp_path, price_tier_known
):
    factory, member, model, claim, protocol, now, compiled = await prepared_stage(
        migrated_database_url, tmp_path
    )
    try:
        ticket = await protocol.authorize(
            claim, stage=1, now=now, wire=compiled.wire, platform_wire=compiled
        )
        measured = usage()
        # Output overrun has a known rate; >272000 input leaves this price tier.
        tokens = (12000, 400000) if price_tier_known else (3000000, 1000)
        measured.update(input_tokens=tokens[0], output_tokens=tokens[1], total_tokens=sum(tokens))
        raw = json.dumps(
            dict(model="gpt-6.1-sol", service_tier="default", status="completed", usage=measured)
        ).encode()
        await protocol.observe(ticket, now=now, payload=raw, tokens=tokens)
        assert await protocol.verify(
            ticket,
            now=now,
            wire_hash=hashlib.sha256(compiled.wire).hexdigest(),
            payload=raw,
            tokens=tokens,
        )
        assert protocol.local_stopped
        await protocol.cleanup(ticket, now=now, confirmed=True)
        assert await protocol.close(claim, now=now + timedelta(seconds=1)) is price_tier_known
        async with factory() as session:
            hold = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == member.run_id
                )
            )
            assert hold.state == "reserved" and hold.settled_usd is None
            occupancy = await research_reserved_occupancy(session, hold)
            if price_tier_known:
                assert occupancy > hold.reserved_usd
            else:
                assert occupancy == hold.reserved_usd
            if price_tier_known:
                proof = await read_terminal_proof(
                    session, run_id=member.run_id, now=now + timedelta(seconds=1)
                )
                assert proof.known and not proof.budget_settled
            else:
                from nexus.agents.research_stages.protocol import ProtocolRejected

                # Unknown acceptance must not waive the separately verified capacity overrun.
                with pytest.raises(ProtocolRejected, match="STAGED_TERMINAL_PROOF_INVALID"):
                    await read_terminal_proof(
                        session, run_id=member.run_id, now=now + timedelta(seconds=1)
                    )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "timeout",
        "cache_unknown",
        "incomplete_known",
        "verification_ack_lost",
        "authorization_ack_lost",
    ],
)
async def test_native_worker_failure_never_retries_or_falls_back_and_preserves_accounting(
    migrated_database_url, tmp_path, monkeypatch, fault
):
    import httpx
    from nexus import worker
    from nexus.agents import platform_native
    from nexus.agents.research_stages.executor import StagedCommitUnconfirmed
    from unit.agents.test_http_stage_lifecycle import Body, terminal

    factory, settings, store, _ = await scope(migrated_database_url, tmp_path)
    request_id = await admit(factory, settings, store)
    async with factory.begin() as session:
        session.info["sec_exhibit_store"] = store
        result = await prepare_research_request(session, settings, request_id=request_id)
    calls, clients = [], []

    def handle(request):
        calls.append(request)
        if fault == "timeout":
            raise httpx.ReadTimeout("controlled offline timeout")
        value = terminal("incomplete" if fault == "incomplete_known" else "completed")
        value["response"].update(model="gpt-6.1-sol", service_tier="default", usage=usage())
        if fault == "cache_unknown":
            del value["response"]["usage"]["input_tokens_details"]["cache_write_tokens"]
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=Body(("data: " + json.dumps(value) + "\n\n").encode()),
        )

    def client():
        item = httpx.AsyncClient(transport=httpx.MockTransport(handle))
        clients.append(item)
        return item

    monkeypatch.setattr(platform_native, "native_http_client", client)
    monkeypatch.setattr(
        "nexus.research.sec_exhibit_storage.S3ProviderEvidenceStore", lambda *_: store
    )
    if fault == "verification_ack_lost":
        original = StagedProtocol.verify

        async def lost(*args, **kwargs):
            await original(*args, **kwargs)
            raise RuntimeError("controlled lost verification acknowledgement")

        monkeypatch.setattr(StagedProtocol, "verify", lost)
    if fault == "authorization_ack_lost":
        commit = StagedProtocol._commit
        injected = [False]

        async def lost_commit(self, session, scope):
            target = (
                not injected[0]
                and await session.scalar(text("SELECT count(*) FROM research_stage_authorization"))
                == 1
            )
            await commit(self, session, scope)
            if target:
                injected[0] = True
                raise RuntimeError("controlled lost authorization acknowledgement")

        monkeypatch.setattr(StagedProtocol, "_commit", lost_commit)
    try:
        member = result.research.members[0]
        if fault == "authorization_ack_lost":
            with pytest.raises(StagedCommitUnconfirmed, match="AUTHORIZATION_COMMIT_UNCONFIRMED"):
                await worker.run_agent_worker_once(
                    settings, job_ids=(member.job_id,), formal_stage_capacity=True
                )
        else:
            assert await worker.run_agent_worker_once(
                settings, job_ids=(member.job_id,), formal_stage_capacity=True
            )
        assert len(calls) == (0 if fault == "authorization_ack_lost" else 1)
        assert all(c.is_closed for c in clients)
        async with factory() as session:
            holds = (await session.scalars(select(AgentDailyBudgetReservationRecord))).all()
            hold = next(h for h in holds if h.run_id == member.run_id)
            known = fault in {"incomplete_known", "verification_ack_lost"}
            assert hold.state == ("settled" if known else "reserved")
            assert hold.settled_usd == (Decimal("0.0299") if known else None)
            assert all(
                h.state == "reserved" and h.settled_usd is None
                for h in holds
                if h.run_id != member.run_id
            )
            assert await session.scalar(text("SELECT count(*) FROM job_attempts")) == 1
            assert (
                await session.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 1
            )
            if fault == "authorization_ack_lost":
                from nexus.agents.research_stages.protocol import ProtocolRejected

                assert (
                    await session.scalar(text("SELECT count(*) FROM research_stage_terminal")) == 0
                )
                with pytest.raises(ProtocolRejected, match="STAGED_TERMINAL_PROOF_INVALID"):
                    await read_terminal_proof(
                        session,
                        run_id=member.run_id,
                        now=await session.scalar(text("SELECT clock_timestamp()")),
                    )
            else:
                proof = await read_terminal_proof(
                    session,
                    run_id=member.run_id,
                    now=await session.scalar(text("SELECT clock_timestamp()")),
                )
                assert proof is not None and proof.known is known
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["credential", "material"])
async def test_native_preclaim_rejects_changed_authority_without_claim_or_send(
    migrated_database_url, tmp_path, monkeypatch, changed
):
    from copy import deepcopy

    from nexus import worker
    from nexus.agents import platform_native
    from pydantic import SecretStr

    factory, settings, store, _ = await scope(migrated_database_url, tmp_path)
    request_id = await admit(factory, settings, store)
    async with factory.begin() as session:
        session.info["sec_exhibit_store"] = store
        result = await prepare_research_request(session, settings, request_id=request_id)
    monkeypatch.setattr(
        "nexus.research.sec_exhibit_storage.S3ProviderEvidenceStore", lambda *_: store
    )

    def forbidden():
        pytest.fail("preclaim refusal must not create an HTTP client")

    monkeypatch.setattr(platform_native, "native_http_client", forbidden)
    if changed == "credential":
        settings = settings.model_copy(
            update={"openai_api_key": SecretStr("sk-proj-" + "different-synthetic" * 4)}
        )
        reason = "PLATFORM_HTTP_CREDENTIAL_CHANGED"
    else:
        original = platform_native.native_prepared_material

        async def drift(*args, **kwargs):
            compiler, request, refs, inputs = await original(*args, **kwargs)
            inputs = deepcopy(inputs)
            inputs["new_unfrozen_material"] = "not in the prepared input"
            return compiler, request, refs, inputs

        monkeypatch.setattr(platform_native, "native_prepared_material", drift)
        reason = "CODEX_TOOL_CITATION_BINDING_INVALID"
    try:
        with pytest.raises(ValueError, match=reason):
            await worker.run_agent_worker_once(
                settings, job_ids=(result.research.members[0].job_id,), formal_stage_capacity=True
            )
        async with factory() as session:
            assert await session.scalar(text("SELECT count(*) FROM job_attempts")) == 0
            assert (
                await session.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 0
            )
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM agent_daily_budget_reservations "
                        "WHERE state='reserved' AND settled_usd IS NULL"
                    )
                )
                == 3
            )
    finally:
        await factory.kw["bind"].dispose()
