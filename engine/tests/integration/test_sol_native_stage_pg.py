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
    return await prepare_successor_stage(factory, settings, store)


async def prepare_successor_stage(factory, settings, store):
    request_id = await admit(factory, settings, store, two_flight=False)
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


def install_sol_http(
    monkeypatch,
    store,
    *,
    audit_veto=False,
    on_request=None,
    model_response=None,
    critique_instruction_bytes=0,
):
    import httpx
    from nexus.agents import platform_native
    from unit.agents.test_codex_sdk import _assessment_payload, _restore_view, _staged_batch
    from unit.agents.test_http_stage_lifecycle import Body, terminal

    requests, clients = [], []

    async def handle(request):
        body = json.loads(request.content)
        assert body["model"] == "gpt-6.1-sol" and body["reasoning"] == {"effort": "medium"}
        assert body["store"] is False and body["tools"] == []
        payload = _restore_view(json.loads(body["input"][1]["content"]))
        requests.append(payload["stage"])
        if on_request is not None:
            await on_request(payload)
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
        if model_response is not None:
            model_response(response, payload)
        # The normal formal prompt has no monitor fields. Omit only absent
        # optional-null fixture fields; keep every substantive value for validation.
        properties = body["text"]["format"]["schema"]["properties"]
        response = {k: v for k, v in response.items() if v is not None or k in properties}
        if "roles" in response:
            role_schema = properties["roles"]["items"]
            if "$ref" in role_schema:
                role_schema = body["text"]["format"]["schema"]["$defs"][
                    role_schema["$ref"].split("/")[-1]
                ]
            role_properties = role_schema["properties"]
            response["roles"] = [
                {k: v for k, v in role.items() if v is not None or k in role_properties}
                for role in response["roles"]
            ]
        value = terminal()
        value["response"].update(model="gpt-6.1-sol", service_tier="default", usage=usage())
        if payload["stage"] == "critique" and critique_instruction_bytes:
            value["response"]["instructions"] = "x" * critique_instruction_bytes
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
    return requests, clients


@pytest.mark.asyncio
@pytest.mark.parametrize("audit_veto", [False, True])
async def test_normal_worker_sends_native_sol_three_stages_and_settles_original_price(
    migrated_database_url, tmp_path, monkeypatch, audit_veto
):
    from nexus import worker

    factory, settings, store, _ = await scope(migrated_database_url, tmp_path)
    request_id = await admit(factory, settings, store, two_flight=False)
    async with factory.begin() as session:
        session.info["sec_exhibit_store"] = store
        result = await prepare_research_request(session, settings, request_id=request_id)
    requests, clients = install_sol_http(monkeypatch, store, audit_veto=audit_veto)
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
                from nexus.api.research_stages import read_stage_trace
                from nexus.monitoring.source_qualification import load_qualified_source
                from nexus.persistence.models.agents import AgentResearchOutputRecord

                trace = await read_stage_trace(session, run)
                assert trace is not None and trace.status == "verified"
                output = await session.scalar(
                    select(AgentResearchOutputRecord).where(
                        AgentResearchOutputRecord.run_id == run.id,
                        AgentResearchOutputRecord.status == "completed",
                    )
                )
                source = await load_qualified_source(
                    session,
                    account_id=settings.agent_paper_account_id,
                    output_id=output.id,
                    now=await session.scalar(text("SELECT clock_timestamp()")),
                )
                assert source.intent.model_dump(mode="json") == intent
                from nexus.agents.codex_formal_stage import formal_stage_output_qualified

                assert not await formal_stage_output_qualified(
                    session,
                    run.id,
                    decision_time=await session.scalar(text("SELECT clock_timestamp()")),
                )
        async with factory.begin() as session:
            from nexus.paper.manual_request import require_manual_account_available
            from nexus.paper.research_request import finish_research_request
            from nexus.persistence.models.manual_paper import ManualPaperRequest

            row = await session.get(ManualPaperRequest, request_id)
            done = await finish_research_request(session, row=row)
            assert done.status == "completed"
            await require_manual_account_available(
                session,
                account_id=settings.agent_paper_account_id,
                now=await session.scalar(text("SELECT clock_timestamp()")),
            )
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
        "output_limit",
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
    request_id = await admit(factory, settings, store, two_flight=False)
    async with factory.begin() as session:
        session.info["sec_exhibit_store"] = store
        result = await prepare_research_request(session, settings, request_id=request_id)
    calls, clients = [], []

    def handle(request):
        calls.append(request)
        if fault == "timeout":
            raise httpx.ReadTimeout("controlled offline timeout")
        value = terminal(
            "incomplete" if fault in {"incomplete_known", "output_limit"} else "completed"
        )
        value["response"].update(model="gpt-6.1-sol", service_tier="default", usage=usage())
        if fault == "output_limit":
            value["response"]["incomplete_details"] = {"reason": "max_output_tokens"}
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
            known = fault in {"incomplete_known", "output_limit", "verification_ack_lost"}
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
                if fault in {"incomplete_known", "output_limit"}:
                    from nexus.api.routes.research import _hydrate_response
                    from nexus.persistence.models.agents import AgentResearchOutputRecord

                    run = await session.get(AgentResearchRunRecord, member.run_id)
                    output = await session.scalar(
                        select(AgentResearchOutputRecord)
                        .where(AgentResearchOutputRecord.run_id == member.run_id)
                        .order_by(AgentResearchOutputRecord.sequence.desc())
                        .limit(1)
                    )
                    original_payload = dict(output.payload)
                    view = await _hydrate_response(session, run, output)
                    assert view.status == "failed" and view.intent is None
                    assert view.abstention_reason == (
                        "model_output_limit_reached"
                        if fault == "output_limit"
                        else "model_research_stopped"
                    )
                    assert view.settled_cost_usd == str(hold.settled_usd)
                    assert output.payload == original_payload
        if fault in {"cache_unknown", "timeout"}:
            from nexus.paper.independent_unknown_proof import independent_unknown_proof
            from nexus.paper.manual_request import require_manual_account_available
            from nexus.paper.research_request import finish_research_request
            from nexus.persistence.models.manual_paper import ManualPaperRequest

            async with factory.begin() as session:
                row = await session.get(ManualPaperRequest, request_id)
                ended = await finish_research_request(session, row=row, cancel_unused=True)
                assert ended.status == "unknown"
                now = await session.scalar(text("SELECT clock_timestamp()"))
                native_proof = await independent_unknown_proof(session, row=row, now=now)
                assert native_proof["unknown_run_ids"] == [str(member.run_id)]
                with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                    await require_manual_account_available(
                        session, account_id=settings.agent_paper_account_id, now=now
                    )
                held = await session.scalar(
                    select(AgentDailyBudgetReservationRecord).where(
                        AgentDailyBudgetReservationRecord.run_id == member.run_id
                    )
                )
                assert held.state == "reserved" and held.settled_usd is None
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
    request_id = await admit(factory, settings, store, two_flight=False)
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


@pytest.mark.asyncio
async def test_native_host_preclaim_refusal_closes_only_proved_unused_members(
    migrated_database_url, tmp_path, monkeypatch
):
    from nexus.agents import platform_native
    from nexus.ops.manual_paper_host import dispatch_pure_research_request
    from nexus.paper.manual_request import read_manual_request, require_manual_account_available
    from nexus.persistence.models.manual_paper import ManualPaperRequest
    from pydantic import SecretStr

    factory, settings, store, _ = await scope(migrated_database_url, tmp_path)
    request_id = await admit(factory, settings, store, claim=False)
    from nexus.paper import research_request

    prepare = research_request.prepare_research_request

    async def changed_after_prepare(session, *args, **kwargs):
        session.info["sec_exhibit_store"] = store
        result = await prepare(session, *args, **kwargs)
        settings.openai_api_key = SecretStr("different-synthetic")
        return result

    monkeypatch.setattr(research_request, "prepare_research_request", changed_after_prepare)
    monkeypatch.setattr(
        "nexus.research.sec_exhibit_storage.S3ProviderEvidenceStore", lambda *_: store
    )
    monkeypatch.setattr(
        "nexus.monitoring.disclosure_context.configured_exhibit_store", lambda *_: store
    )

    def forbidden():
        pytest.fail("pure preparation refusal must not send")

    monkeypatch.setattr(platform_native, "native_http_client", forbidden)
    try:
        assert await dispatch_pure_research_request(settings, request_id=request_id)
        async with factory() as session:
            row = await session.get(ManualPaperRequest, request_id)
            result = await read_manual_request(session, row)
            assert result.status == "unavailable"
            assert (
                await session.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 0
            )
            holds = (await session.scalars(select(AgentDailyBudgetReservationRecord))).all()
            assert len(holds) == 3 and all(
                h.state == "released" and h.settled_usd is None for h in holds
            )
            assert (
                await session.scalar(
                    text(
                        "SELECT count(*) FROM audit_events WHERE action='research.request_failure' "
                        "AND payload->>'failure_code'='PLATFORM_HTTP_CREDENTIAL_CHANGED'"
                    )
                )
                == 1
            )
            await require_manual_account_available(
                session,
                account_id=settings.agent_paper_account_id,
                now=await session.scalar(text("SELECT clock_timestamp()")),
            )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_native_host_two_flight_uses_existing_dispatch_and_public_closeout(
    migrated_database_url, tmp_path, monkeypatch
):
    import asyncio

    from nexus.ops.manual_paper_host import dispatch_pure_research_request
    from nexus.paper.manual_request import read_manual_request, require_manual_account_available
    from nexus.persistence.models.manual_paper import ManualPaperRequest

    factory, settings, store, _ = await scope(migrated_database_url, tmp_path)
    request_id = await admit(factory, settings, store, claim=False)
    first = asyncio.Event()
    research = []
    running = []

    async def concurrent(payload):
        async with factory() as session:
            running.append(
                await session.scalar(text("SELECT count(*) FROM jobs WHERE status='running'"))
            )
        if payload["stage"] == "research":
            research.append(payload)
            if len(research) == 2:
                first.set()
            await first.wait()

    requests, clients = install_sol_http(monkeypatch, store, on_request=concurrent)
    try:
        async with asyncio.timeout(30):
            assert await dispatch_pure_research_request(settings, request_id=request_id)
        assert max(running) == 2 and all(n <= 2 for n in running)
        assert len(requests) == 9 and all(
            requests.count(stage) == 3 for stage in ["research", "critique", "synthesis"]
        )
        assert len(clients) == 9 and all(c.is_closed for c in clients)
        async with factory() as session:
            row = await session.get(ManualPaperRequest, request_id)
            result = await read_manual_request(session, row)
            assert result.status == "completed"
            assert all(m.usage_known for m in result.research.members)
            await require_manual_account_available(
                session,
                account_id=settings.agent_paper_account_id,
                now=await session.scalar(text("SELECT clock_timestamp()")),
            )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_native_parallel_manifest_requires_original_dispatch_authority(
    migrated_database_url, tmp_path, monkeypatch
):
    from nexus import worker
    from nexus.agents import platform_native

    factory, settings, store, _ = await scope(migrated_database_url, tmp_path)
    request_id = await admit(factory, settings, store)
    async with factory.begin() as session:
        session.info["sec_exhibit_store"] = store
        prepared = await prepare_research_request(session, settings, request_id=request_id)

    def forbidden():
        pytest.fail("missing dispatch proof must reject before HTTP")

    monkeypatch.setattr(platform_native, "native_http_client", forbidden)
    monkeypatch.setattr(
        "nexus.research.sec_exhibit_storage.S3ProviderEvidenceStore", lambda *_: store
    )
    try:
        with pytest.raises(ValueError, match="RESEARCH_ONLY_POOL_BINDING_INVALID"):
            await worker.run_agent_worker_once(
                settings, job_ids=(prepared.research.members[0].job_id,), formal_stage_capacity=True
            )
        async with factory() as session:
            assert await session.scalar(text("SELECT count(*) FROM job_attempts")) == 0
            assert (
                await session.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 0
            )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_native_formal_worker_retains_original_cycle_and_output_gate(
    migrated_database_url, monkeypatch
):
    from integration.test_sol_native_preparation_pg import (
        test_formal_root_preserves_cycle_owner_and_freezes_native_sol_routes as prepare,
    )
    from nexus import worker
    from nexus.agents.capacity_cycle import capacity_output_mode_qualified
    from nexus.agents.codex_formal_stage import formal_stage_output_qualified
    from nexus.api.research_stages import read_stage_trace
    from sqlalchemy import func

    requests, clients = install_sol_http(monkeypatch, None)

    async def validate(factory, settings, cycle_id):
        async with factory() as session:
            routes = (
                await session.execute(
                    text("SELECT run_id,job_id FROM research_stage_route ORDER BY run_id")
                )
            ).all()
        from nexus.ops.agent_paper_formal import (
            require_formal_host_trigger_profile,
            resolve_formal_host_cycle_profile,
        )
        from nexus.persistence.models.paper import AgentPaperCycleRecord

        async with factory() as session:
            at = await session.scalar(select(func.clock_timestamp()))
            cycle = await session.get(AgentPaperCycleRecord, cycle_id)
        await require_formal_host_trigger_profile(settings, now=at)
        from nexus.settings import AgentRuntimeMode

        sdk_with_native_identity = settings.model_copy(
            update={"agent_runtime_mode": AgentRuntimeMode.CODEX_SDK}
        )
        with pytest.raises(ValueError, match="CODEX_FORMAL_HOST_PROFILE_INVALID"):
            await require_formal_host_trigger_profile(sdk_with_native_identity, now=at)
        with pytest.raises(ValueError, match="CODEX_FORMAL_HOST_PROFILE_INVALID"):
            await resolve_formal_host_cycle_profile(
                sdk_with_native_identity,
                account_id=cycle.account_id,
                cycle_id=cycle_id,
                research_job_id=routes[0].job_id,
            )
        assert await resolve_formal_host_cycle_profile(
            settings,
            account_id=cycle.account_id,
            cycle_id=cycle_id,
            research_job_id=routes[0].job_id,
        )
        for rid, jid in routes:
            assert await worker.run_agent_worker_once(
                settings, job_ids=(jid,), formal_stage_capacity=True
            )
            async with factory() as session:
                run = await session.get(AgentResearchRunRecord, rid)
                if run.status != "completed":
                    print(
                        "synthetic formal diagnostic",
                        await session.scalar(
                            text(
                                "SELECT payload->'diagnostic' FROM agent_research_outputs "
                                "WHERE run_id=:id AND status='failed'"
                            ),
                            {"id": run.id},
                        ),
                    )
                assert run.status == "completed"
                now = await session.scalar(select(func.clock_timestamp()))
                assert await formal_stage_output_qualified(session, rid, decision_time=now)
                assert await capacity_output_mode_qualified(session, run, formal=True)
                assert not await capacity_output_mode_qualified(session, run, formal=False)
                trace = await read_stage_trace(session, run)
                assert trace.status == "verified"
        assert len(requests) == 9 and all(c.is_closed for c in clients)

    await prepare(migrated_database_url, monkeypatch, after_prepared=validate)


@pytest.mark.asyncio
async def test_native_increment_worker_settles_only_child_and_retains_parent(
    migrated_database_url, monkeypatch
):
    from integration.test_sol_native_preparation_pg import (
        test_normal_increment_selects_sol_and_retains_exact_sdk_predecessor as prepare,
    )
    from nexus import worker
    from nexus.monitoring.admission import finish_increment, require_monitor_account_available
    from nexus.monitoring.source_qualification import load_qualified_source
    from nexus.persistence.models.agents import AgentResearchOutputRecord
    from sqlalchemy import func

    async def validate(factory, settings, member, trigger, parent):
        requests, clients = install_sol_http(monkeypatch, None)
        async with factory() as session:
            before = tuple(
                await session.scalars(
                    text(
                        "SELECT to_jsonb(o)::text FROM agent_research_outputs o "
                        "WHERE run_id=:id ORDER BY sequence"
                    ),
                    {"id": parent.id},
                )
            )
        assert await worker.run_agent_worker_once(
            settings, job_ids=(member.job_id,), formal_stage_capacity=True
        )
        async with factory.begin() as session:
            child = await session.get(AgentResearchRunRecord, member.run_id)
            assert child.status == "completed"
            assert child.settled_cost_usd == Decimal("0.0897")
            now = await session.scalar(select(func.clock_timestamp()))
            result = await finish_increment(
                session, account_id=settings.agent_paper_account_id, trigger_id=trigger.id, now=now
            )
            assert result.status == "completed"
            output = await session.scalar(
                select(AgentResearchOutputRecord).where(
                    AgentResearchOutputRecord.run_id == child.id,
                    AgentResearchOutputRecord.status == "completed",
                )
            )
            source = await load_qualified_source(
                session, account_id=settings.agent_paper_account_id, output_id=output.id, now=now
            )
            assert source.run.id == child.id and source.context.parent_run_id == parent.id
            await require_monitor_account_available(
                session, account_id=settings.agent_paper_account_id, now=now
            )
            after = tuple(
                await session.scalars(
                    text(
                        "SELECT to_jsonb(o)::text FROM agent_research_outputs o "
                        "WHERE run_id=:id ORDER BY sequence"
                    ),
                    {"id": parent.id},
                )
            )
            assert before == after
        assert requests == ["research", "critique", "synthesis"] and len(clients) == 3
        assert all(c.is_closed for c in clients)

    await prepare(migrated_database_url, monkeypatch, after_admitted=validate)


@pytest.mark.asyncio
async def test_normal_sol_formal_research_reconciles_monitor_plans_once(
    migrated_database_url,
    monkeypatch,
    tmp_path,
    portfolio_case="orphan",
    after_admission=None,
    all_approved=False,
):
    from integration.test_sol_native_preparation_pg import (
        test_formal_root_preserves_cycle_owner_and_freezes_native_sol_routes as prepare,
    )
    from nexus.ops import agent_paper_loop as loop
    from nexus.ops import research_monitor
    from nexus.ops.platform_credentials import PLATFORM_KEY_PATH
    from nexus.persistence.models.monitoring import MonitorCoverageReceipt, MonitorPlan
    from nexus.persistence.models.paper import AgentPaperCycleRecord
    from sqlalchemy import func
    from unit.ops.test_sol_host_runtime import environment

    async def validate(factory, settings, cycle_id):
        async with factory() as session:
            cycle = await session.get(AgentPaperCycleRecord, cycle_id)
            routes = (
                await session.execute(
                    text(
                        "SELECT run_id,job_id FROM research_stage_route "
                        "WHERE scope_id=:cycle ORDER BY run_id"
                    ),
                    {"cycle": cycle_id},
                )
            ).all()
        config = environment()
        config.pop("OPENAI_API_KEY")
        config.update(
            DATABASE_URL=migrated_database_url,
            NEXUS_AGENT_PAPER_ACCOUNT_ID=str(cycle.account_id),
            NEXUS_AGENT_PAPER_CANDIDATE_BATCH_ID=str(cycle.candidate_batch_id),
            NEXUS_AGENT_PAPER_FORMAL_MODEL_CONFIG_ID=str(
                settings.agent_paper_formal_model_config_id
            ),
            AGENT_DAILY_BUDGET_USD=str(settings.agent_daily_budget_usd),
        )
        secret_path = tmp_path / PLATFORM_KEY_PATH
        secret_path.parent.mkdir(parents=True)
        secret_path.write_text(settings.openai_api_key.get_secret_value())
        secret_path.chmod(0o600)
        env_file = tmp_path / "host.env"
        env_file.write_text("\n".join(f"{k}={v}" for k, v in config.items()))
        host = loop._FileEngineSettings.model_validate(
            loop.load_agent_paper_service_environment(tmp_path, {}, env_file=env_file)
        )
        async with factory.begin() as session:
            at = await session.scalar(select(func.clock_timestamp()))
            assert not await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id="generic",
                now=at,
                lease_seconds=120,
                job_ids=tuple(route.job_id for route in routes),
            )
        from unit.agents.test_monitoring_producer import proposal_wire

        def reviewed_response(body, payload):
            proposal = payload.get("monitoring_proposal")
            if payload["stage"] == "synthesis":
                assessment = body["synthesis_assessment"]
                for review in assessment["reviews"]:
                    review.update(
                        status="accepted",
                        resolved=True,
                        missing_evidence=[],
                        prevented_conclusion=None,
                        rationale="已核对冻结证据。",
                    )
                assessment.update(
                    review_conclusion="sufficient",
                    uncertainties=[],
                    claims=[{"text": "证据支持继续观察并复核价格。", "citations": [0]}],
                )
                body["monitoring_adoption_hash"] = proposal["content_hash"] if approved else None
            else:
                for row in body["roles"]:
                    row["vote"] = "support"
                    if payload["stage"] == "research" and row["role"] == "market_state":
                        row["monitoring_proposal"] = proposal_wire(cycle.decision_time)
                    if payload["stage"] == "critique":
                        veto = not approved and row["role"] == "evidence_audit"
                        row["vote"] = "abstain" if veto else "support"
                        row["monitoring_review"] = {
                            "proposal_hash": proposal["content_hash"],
                            "decision": "rejected" if veto else "accepted",
                            "rationale": "核对冻结价格条件及其证据。",
                            "citations": [0],
                        }

        requests, clients = install_sol_http(monkeypatch, None, model_response=reviewed_response)
        for run_id, job_id in routes:
            approved = all_approved or run_id != routes[-1].run_id
            snapshot = loop.AgentPaperLoopSnapshot(
                now=cycle.decision_time,
                cycle_id=cycle_id,
                cycle_status="researching",
                research_job_id=job_id,
                research_job_status="queued",
            )
            assert loop.choose_loop_action(snapshot) == loop.AgentPaperLoopAction.RUN_RESEARCH_JOB
            assert (
                await loop.execute_agent_paper_loop_action(
                    host,
                    account_id=cycle.account_id,
                    repo_root=tmp_path,
                    env_file=env_file,
                    action=loop.choose_loop_action(snapshot),
                    snapshot=snapshot,
                )
                is None
            )
            async with factory() as session:
                run = await session.get(AgentResearchRunRecord, run_id)
                assert run.status == "completed" and run.settled_cost_usd == Decimal("0.0897")
        assert requests == ["research", "critique", "synthesis"] * 3
        args = research_monitor.parser().parse_args(["reconcile"])
        result = await research_monitor.run(host, args)
        async with factory() as session:
            coverage = (
                await session.execute(
                    select(MonitorCoverageReceipt.reason, MonitorCoverageReceipt.quality_status)
                )
            ).all()
        if all_approved:
            assert result["installed"] == 3 and result["gap"] == 0, coverage
            assert ("MONITOR_AUDIT_REJECTED", "partial") not in coverage
        else:
            assert result["installed"] == 2 and result["gap"] == 1, coverage
            assert ("MONITOR_AUDIT_REJECTED", "partial") in coverage
        assert (
            result["execution_locked"]
            and result["model_requests"] == result["provider_requests"] == 0
        )
        again = await research_monitor.run(host, args)
        assert again["installed"] == 0 and again["recorded"] == 0
        async with factory() as session:
            plans = tuple(await session.scalars(select(MonitorPlan)))
            expected = routes if all_approved else routes[:-1]
            assert len(plans) == len(expected) and {p.source_run_id for p in plans} == {
                r.run_id for r in expected
            }
            assert (
                await session.scalar(select(func.count()).select_from(MonitorCoverageReceipt)) == 3
            )
        assert len(requests) == 9 and all(c.is_closed for c in clients)
        await _verify_native_portfolio_boundary(
            factory,
            host,
            cycle_id,
            monkeypatch,
            portfolio_case,
            tmp_path,
            env_file,
            after_admission,
        )
        assert len(requests) == 9

    await prepare(migrated_database_url, monkeypatch, after_prepared=validate, monitoring=True)


async def _verify_native_portfolio_boundary(
    factory, host, cycle_id, monkeypatch, portfolio_case, root, env_file, after_admission
):
    from nexus.paper.agent_cycle import resume_schwab_agent_paper_cycle
    from nexus.persistence.models.agents import AgentPortfolioSynthesisRunRecord
    from nexus.persistence.models.paper import AgentPaperCycleEventRecord
    from sqlalchemy import func

    async with factory.begin() as session:
        at = await session.scalar(select(func.clock_timestamp()))
        result = await resume_schwab_agent_paper_cycle(
            session,
            cycle_id=cycle_id,
            idempotency_key="sol-formal-portfolio-admit",
            now=at,
            daily_limit_usd=host.agent_daily_budget_usd,
            formal_stage_capacity=True,
        )
        last = await session.scalar(
            select(AgentPaperCycleEventRecord)
            .where(AgentPaperCycleEventRecord.cycle_id == cycle_id)
            .order_by(AgentPaperCycleEventRecord.sequence.desc())
        )
        assert result.status == "researching", (last.result_code, last.fact_ids)
        portfolio = await session.scalar(
            select(AgentPortfolioSynthesisRunRecord).where(
                AgentPortfolioSynthesisRunRecord.cycle_id == cycle_id
            )
        )
        assert (
            portfolio is not None
            and portfolio.model_config_id == host.agent_paper_formal_model_config_id
        )
        from nexus.agents.portfolio_synthesis_worker import read_synthesis_job

        portfolio_job = await read_synthesis_job(session, cycle_id=cycle_id)
        assert portfolio_job is not None
        at = await session.scalar(select(func.clock_timestamp()))
        assert portfolio_job.available_at <= at
        ordinary = await claim_next_job(
            session,
            queue=QueueName.GENERAL,
            worker_id="generic",
            now=at,
            lease_seconds=120,
            job_ids=(portfolio_job.id,),
        )
        assert ordinary is None
    from nexus.agents.portfolio_synthesis_worker import (
        CodexSynthesisTransport,
        run_synthesis_job_once,
    )

    async def forbidden(*args, **kwargs):
        raise AssertionError("SDK attempted native portfolio I/O")

    monkeypatch.setattr(CodexSynthesisTransport, "invoke", forbidden)
    assert not await run_synthesis_job_once(
        factory,
        transport=CodexSynthesisTransport(formal_stage_capacity=True),
        worker_id="sdk",
        clock=lambda: at,
        job_ids=(portfolio_job.id,),
        formal_stage_capacity=True,
    )
    async with factory.begin() as session:
        from nexus.persistence.models.agents import (
            AgentPortfolioSynthesisBudgetReservationRecord,
        )
        from nexus.persistence.models.runtime import Job

        repeated = await resume_schwab_agent_paper_cycle(
            session,
            cycle_id=cycle_id,
            idempotency_key="sol-formal-portfolio-reread",
            now=await session.scalar(select(func.clock_timestamp())),
            daily_limit_usd=host.agent_daily_budget_usd,
            formal_stage_capacity=True,
        )
        assert repeated.result_code == "PORTFOLIO_SYNTHESIS_PENDING"
        assert (await read_synthesis_job(session, cycle_id=cycle_id)).id == portfolio_job.id
        job = await session.get(Job, portfolio_job.id)
        hold = await session.scalar(
            select(AgentPortfolioSynthesisBudgetReservationRecord).where(
                AgentPortfolioSynthesisBudgetReservationRecord.run_id == portfolio.id
            )
        )
        from nexus.agents.portfolio_synthesis_persistence import (
            persist_portfolio_synthesis_terminal,
        )

        for unproven in (False, None):
            with pytest.raises(
                ValueError, match="PORTFOLIO_SYNTHESIS_PLATFORM_USAGE_PROOF_REQUIRED"
            ):
                await persist_portfolio_synthesis_terminal(
                    session,
                    run_id=portfolio.id,
                    input_hash=portfolio.input_hash,
                    status="failed",
                    intent=None,
                    input_tokens=None,
                    output_tokens=None,
                    now=at,
                    provider_call_started=unproven,
                )
        assert job.status == "queued" and job.attempt_count == 0
        assert hold.state == "reserved" and hold.reserved_usd > 0 and hold.settled_usd is None
    if after_admission is not None:
        await after_admission(
            factory, host, portfolio, portfolio_job, at, monkeypatch, portfolio_case, root, env_file
        )
        return

    # A committed claim can outlive its process. Exercise normal cycle
    # recovery with no provider I/O or made-up zero-call settlement.
    from nexus.agents.portfolio_synthesis_worker import formal_synthesis_job_qualified

    async with factory.begin() as session:
        claim_at = await session.scalar(select(func.clock_timestamp()))

        async def original_owner(locked_session, job):
            return await formal_synthesis_job_qualified(locked_session, job, now=claim_at)

        claim = await claim_next_job(
            session,
            queue=QueueName.GENERAL,
            worker_id="isolated-exit",
            now=claim_at,
            lease_seconds=120,
            job_ids=(portfolio_job.id,),
            job_types=("agent.portfolio_synthesis",),
            formal_synthesis_profile=True,
            locked_job_guard=original_owner,
        )
        assert claim is not None
        member = await session.get(AgentPortfolioSynthesisRunRecord, portfolio.id)
        if portfolio_case != "prepared_orphan":
            member.status = "running"
    from datetime import timedelta

    from nexus.persistence.models.agents import AgentModelConfigRecord
    from sqlalchemy.ext.asyncio import AsyncSession

    async with factory() as session:
        model = await session.get(AgentModelConfigRecord, portfolio.model_config_id)
        after_price = model.pricing_expires_at + timedelta(seconds=1)
    original_scalar = AsyncSession.scalar

    async def historical_clock(self, statement, *args, **kwargs):
        if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
            return after_price
        return await original_scalar(self, statement, *args, **kwargs)

    for instant, key, expected in (
        (claim_at, "live", "researching"),
        (after_price, "expired", "unavailable"),
    ):
        if key == "expired":
            monkeypatch.setattr(AsyncSession, "scalar", historical_clock)
            from nexus.ops.agent_paper_formal import require_formal_host_trigger_profile

            with pytest.raises(ValueError, match="CODEX_FORMAL_AUTHORITY_INVALID"):
                await require_formal_host_trigger_profile(host, now=instant)
        async with factory.begin() as session:
            resumed = await resume_schwab_agent_paper_cycle(
                session,
                cycle_id=cycle_id,
                idempotency_key="sol-synthesis-" + key,
                now=instant,
                daily_limit_usd=host.agent_daily_budget_usd,
                formal_stage_capacity=True,
            )
            assert resumed.status == expected, resumed
            held = await session.get(AgentPortfolioSynthesisBudgetReservationRecord, hold.id)
            original = await session.get(AgentPortfolioSynthesisRunRecord, portfolio.id)
            assert (
                held.state == "reserved"
                and held.reserved_usd == hold.reserved_usd
                and held.settled_usd is None
            )
            assert original.status == (
                ("prepared" if portfolio_case == "prepared_orphan" else "running")
                if key == "live"
                else "failed"
            )
            assert original.input_tokens is None and original.settled_cost_usd is None
            assert (await session.get(Job, portfolio_job.id)).attempt_count == 1
    async with factory.begin() as session:
        reread = await resume_schwab_agent_paper_cycle(
            session,
            cycle_id=cycle_id,
            idempotency_key="sol-synthesis-expired",
            now=after_price,
            daily_limit_usd=host.agent_daily_budget_usd,
            formal_stage_capacity=True,
        )
        assert reread.replayed and reread.status == "unavailable"
        assert reread.result_code == "PORTFOLIO_SYNTHESIS_UNAVAILABLE"
        from nexus.paper.manual_request import require_manual_account_available

        with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
            await require_manual_account_available(
                session,
                account_id=host.agent_paper_account_id,
                now=after_price,
                read_only=True,
            )
        held = await session.get(AgentPortfolioSynthesisBudgetReservationRecord, hold.id)
        assert held.state == "reserved" and held.settled_usd is None


@pytest.mark.asyncio
async def test_late_invalid_history_fences_only_the_unsent_formal_sol_stages(
    migrated_database_url,
    monkeypatch,
):
    from integration.test_sol_native_preparation_pg import (
        test_formal_root_preserves_cycle_owner_and_freezes_native_sol_routes as prepare,
    )
    from nexus.audit.service import append_audit_event
    from nexus.paper import preparation_unknown_ack as legacy
    from nexus.worker import run_agent_worker_once

    async def after_prepared(factory, settings, cycle_id):
        async with factory() as session:
            account_id = await session.scalar(
                text("SELECT account_id FROM agent_paper_cycles WHERE id=:cycle"),
                {"cycle": cycle_id},
            )
            decision_time = await session.scalar(
                text("SELECT decision_time FROM agent_paper_cycles WHERE id=:cycle"),
                {"cycle": cycle_id},
            )
            monkeypatch.setattr(
                legacy, "_FORMAL_SUCCESSOR_POLICY_AT", decision_time - timedelta(minutes=1)
            )
            assert account_id is not None
            run_id, job_id = (
                await session.execute(
                    text(
                        "SELECT run_id,job_id FROM research_stage_route WHERE scope_id=:cycle "
                        "ORDER BY run_id LIMIT 1"
                    ),
                    {"cycle": cycle_id},
                )
            ).one()

        async def late_history(payload):
            if payload["stage"] == "research":
                async with factory.begin() as session:
                    await append_audit_event(
                        session,
                        actor_id=legacy.ACTOR,
                        action=legacy.ACTION,
                        schema_version=legacy.SCHEMA,
                        payload={"account_id": str(account_id)},
                        occurred_at=await session.scalar(text("SELECT clock_timestamp()")),
                    )

        import asyncio

        from nexus.agents.persistence import try_lock_budget_day
        from nexus.agents.research_stages.protocol import StagedProtocol
        from nexus.jobs.repository import _lock_active_claim

        authorize = StagedProtocol.authorize
        lock_order_checked = []

        async def verify_then_authorize(protocol, claim, **kwargs):
            if kwargs["stage"] != 2:
                return await authorize(protocol, claim, **kwargs)
            async with factory.begin() as closing:
                await protocol._member(closing, claim)
                await _lock_active_claim(closing, claim=claim, now=kwargs["now"])
                pid = await closing.scalar(text("SELECT pg_backend_pid()"))
                pending = asyncio.create_task(authorize(protocol, claim, **kwargs))
                try:
                    async with asyncio.timeout(10):
                        while not await closing.scalar(
                            text(
                                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity "
                                "WHERE :pid=ANY(pg_blocking_pids(pid)))"
                            ),
                            {"pid": pid},
                        ):
                            await asyncio.sleep(0.02)
                    assert await try_lock_budget_day(closing, kwargs["now"].date().isoformat())
                    lock_order_checked.append(True)
                except BaseException:
                    pending.cancel()
                    await asyncio.gather(pending, return_exceptions=True)
                    raise
            return await pending

        monkeypatch.setattr(StagedProtocol, "authorize", verify_then_authorize)
        requests, clients = install_sol_http(monkeypatch, object(), on_request=late_history)
        assert await run_agent_worker_once(settings, job_ids=(job_id,), formal_stage_capacity=True)
        assert lock_order_checked == [True]
        assert requests == ["research"]
        assert all(client.is_closed for client in clients)
        async with factory() as session:
            rows = await stage_evidence(session, run_id)
            assert len(rows) == 1 and rows[0]["response_hash"] is not None
            assert (
                await session.scalar(
                    text("SELECT count(*) FROM research_stage_authorization WHERE run_id=:r"),
                    {"r": run_id},
                )
                == 1
            )

    await prepare(migrated_database_url, monkeypatch, after_prepared=after_prepared)
