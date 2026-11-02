"""Normal Sol portfolio dispatch and settlement against an isolated HTTP peer."""

import json
from decimal import Decimal
from uuid import uuid4

import pytest
from integration.test_sol_native_stage_pg import (
    test_normal_sol_formal_research_reconciles_monitor_plans_once as normal_research,
)
from sqlalchemy import select, text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "portfolio_case",
    [
        "complete",
        "timeout",
        "unknown_cache",
        "wrong_key",
        "close_failed",
        "invalid_output",
        "known_orphan",
        "overrun",
        "overrun_orphan",
        "token_overrun",
        "auth_lost",
        "terminal_ack_lost",
        "cleanup_ack_lost",
        "terminal_not_saved",
        "cleanup_not_saved",
        "wrong_scope",
        "source_expired",
        "prepared_orphan",
        "illegal_action",
        "cancel_after_terminal",
    ],
)
async def test_formal_responses_portfolio(
    migrated_database_url, monkeypatch, tmp_path, portfolio_case
):
    await normal_research(
        migrated_database_url,
        monkeypatch,
        tmp_path,
        portfolio_case,
        None if portfolio_case == "prepared_orphan" else _run_native_portfolio_http,
    )


async def _run_native_portfolio_http(
    factory, host, portfolio, job, at, monkeypatch, case, root, env_file
):
    import asyncio

    import httpx
    from integration.test_portfolio_synthesis_persistence_pg import synthesis_intent
    from nexus.agents import platform_native
    from nexus.agents.persistence import _shared_daily_budget_used
    from nexus.ops import agent_paper_loop as loop
    from nexus.paper.formal_budget_protection import _native_usage
    from nexus.persistence.models.agents import (
        AgentPortfolioSynthesisBudgetReservationRecord,
        AgentPortfolioSynthesisRunRecord,
    )
    from nexus.persistence.models.runtime import Job
    from unit.agents.test_http_stage_lifecycle import Body, terminal
    from unit.agents.test_platform_usage import usage

    output_overrun = {"overrun": 1000000, "overrun_orphan": 1000000, "token_overrun": 8193}.get(
        case
    )
    baseline = None
    inventory_baseline = None
    actual_cost = None
    if output_overrun is not None:
        actual_cost = Decimal("0.0299") + Decimal(output_overrun - 1000) * Decimal("10") / 1000000
        async with factory() as session:
            from nexus.paper.formal_budget_inventory import inspect_inventory

            inventory_input = dict(
                account_id=host.agent_paper_account_id,
                batch_id=portfolio.candidate_batch_id,
                enabled=True,
                now=at,
                configuration_hash="0" * 64,
            )
            inventory_baseline = (
                await inspect_inventory(session, **inventory_input)
            ).conservative_native_occupied_usd
            baseline = await _shared_daily_budget_used(session, business_date=at.date())
    terminal_saved = asyncio.Event()
    claims = {}
    actual, clients = [], []

    async def respond(request):
        data = json.loads(request.content)
        assert data["model"] == "gpt-6.1-sol" and data["reasoning"] == {"effort": "medium"}
        assert data["store"] is False and data["tools"] == []
        actual.append(request.url.path)
        if case == "timeout":
            raise httpx.ReadTimeout("synthetic observation timeout", request=request)
        event = terminal()
        counts = usage()
        if output_overrun is not None:
            counts.update(output_tokens=output_overrun, total_tokens=12000 + output_overrun)
        if case == "unknown_cache":
            counts["input_tokens_details"].pop("cache_write_tokens")
        event["response"].update(model="gpt-6.1-sol", service_tier="default", usage=counts)
        event["response"]["output"][0]["content"][0]["text"] = synthesis_intent(
            portfolio
        ).model_dump_json()
        if case == "invalid_output":
            event["response"]["output"][0]["content"][0]["text"] = "{}"

        if case == "illegal_action":
            invalid = synthesis_intent(portfolio).model_dump(mode="json")
            invalid["choices"][0].update(action="include", role="core", rank=1)
            event["response"]["output"][0]["content"][0]["text"] = json.dumps(invalid)

        class WaitAfterTerminal(Body):
            async def __aiter__(self):
                yield self.data
                await asyncio.Event().wait()

        class CloseFailed(Body):
            async def aclose(self):
                raise OSError("synthetic close failure")

        stream_type = (
            CloseFailed
            if case == "close_failed"
            else WaitAfterTerminal
            if case == "cancel_after_terminal"
            else Body
        )
        from nexus import worker

        if case == "complete":
            from nexus.agents.portfolio_responses import (
                NativePortfolioTransport,
                PortfolioAlreadyDispatched,
            )
            from nexus.agents.synthesis_lifecycle import SynthesisDeadline

            duplicate = NativePortfolioTransport(factory, host)
            duplicate.claim = claims["claim"]
            with pytest.raises(PortfolioAlreadyDispatched):
                await duplicate.invoke(
                    {}, lifecycle=SynthesisDeadline(asyncio.get_running_loop().time())
                )
            assert not await worker.run_portfolio_synthesis_worker_once(
                host, job_ids=(job.id,), formal_stage_capacity=True
            )
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=stream_type(("data: " + json.dumps(event) + "\n\n").encode()),
        )

    def client():
        c = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        if case == "wrong_scope":
            c.headers["openai-project"] = "other-synthetic-project"
        clients.append(c)
        return c

    monkeypatch.setattr(platform_native, "native_http_client", client)
    if case == "wrong_key":
        from nexus.ops.platform_credentials import PLATFORM_KEY_PATH

        (root / PLATFORM_KEY_PATH).write_text("offline-rotated-project-key")
    from nexus.agents import portfolio_responses, portfolio_synthesis_worker

    if case == "complete":
        original_write = portfolio_responses.NativePortfolioTransport._write_evidence

        async def capture_claim(self, claim, identity, phase, value):
            await original_write(self, claim, identity, phase, value)
            if phase == "authorized":
                claims["claim"] = claim

        monkeypatch.setattr(
            portfolio_responses.NativePortfolioTransport, "_write_evidence", capture_claim
        )
    if case == "cancel_after_terminal" or output_overrun is not None:
        original_append = portfolio_responses.append_receipt

        async def observed(session, job_id, phase, value, at):
            await original_append(session, job_id, phase, value, at)
            if phase == "terminal":
                if actual_cost is not None:
                    async with factory.begin() as competitor:
                        assert not await competitor.scalar(
                            text(
                                "SELECT pg_try_advisory_xact_lock("
                                "hashtext('agent-budget:accounting'))"
                            )
                        )
                    hold = await session.scalar(
                        select(AgentPortfolioSynthesisBudgetReservationRecord).where(
                            AgentPortfolioSynthesisBudgetReservationRecord.run_id == portfolio.id
                        )
                    )
                    assert await _native_usage(session, hold) == max(hold.reserved_usd, actual_cost)
                terminal_saved.set()

        monkeypatch.setattr(portfolio_responses, "append_receipt", observed)
    verified_readback = case in {"terminal_ack_lost", "cleanup_ack_lost"}
    if verified_readback or case in {"terminal_not_saved", "cleanup_not_saved"}:
        phase_name = case.split("_")[0]
        original_write = portfolio_responses.NativePortfolioTransport._write_evidence

        async def broken_receipt(self, claim, identity, phase, value):
            if phase != phase_name:
                return await original_write(self, claim, identity, phase, value)
            if verified_readback:
                await original_write(self, claim, identity, phase, value)
            raise OSError("isolated receipt acknowledgement failure")

        monkeypatch.setattr(
            portfolio_responses.NativePortfolioTransport, "_write_evidence", broken_receipt
        )
    if case == "source_expired":
        original_check = portfolio_responses.NativePortfolioTransport.require_job

        async def expires_before_send(self, session, selected, now):
            await original_check(self, session, selected, now)
            if selected.status == "running":
                raise ValueError("PLATFORM_PRICE_WINDOW_INVALID")

        monkeypatch.setattr(
            portfolio_responses.NativePortfolioTransport, "require_job", expires_before_send
        )
    if case == "auth_lost":
        send = portfolio_responses.send_response_stage

        async def lose_ack(*args, pre_send, **kwargs):
            async def ambiguous():
                await pre_send()
                raise RuntimeError("isolated commit acknowledgement lost")

            return await send(*args, pre_send=ambiguous, **kwargs)

        monkeypatch.setattr(portfolio_responses, "send_response_stage", lose_ack)

    class ProcessExit(BaseException):
        pass

    if case in {"known_orphan", "overrun_orphan"}:

        async def exit_before_settlement(*args, **kwargs):
            raise ProcessExit

        monkeypatch.setattr(portfolio_synthesis_worker, "_finish", exit_before_settlement)
    snapshot = loop.AgentPaperLoopSnapshot(
        now=at,
        cycle_id=portfolio.cycle_id,
        cycle_status="researching",
        research_job_id=job.id,
        research_job_status="queued",
    )
    if case == "wrong_key":
        with pytest.raises(ValueError):
            await loop.execute_agent_paper_loop_action(
                host,
                account_id=host.agent_paper_account_id,
                repo_root=root,
                env_file=env_file,
                action=loop.choose_loop_action(snapshot),
                snapshot=snapshot,
            )
    elif case == "cancel_after_terminal":
        action = asyncio.create_task(
            loop.execute_agent_paper_loop_action(
                host,
                account_id=host.agent_paper_account_id,
                repo_root=root,
                env_file=env_file,
                action=loop.choose_loop_action(snapshot),
                snapshot=snapshot,
            )
        )
        try:
            async with asyncio.timeout(15):
                await terminal_saved.wait()
            action.cancel()
            with pytest.raises(asyncio.CancelledError):
                await action
        finally:
            if not action.done():
                action.cancel()
                await asyncio.gather(action, return_exceptions=True)
    elif case in {"known_orphan", "overrun_orphan"}:
        with pytest.raises(ProcessExit):
            await loop.execute_agent_paper_loop_action(
                host,
                account_id=host.agent_paper_account_id,
                repo_root=root,
                env_file=env_file,
                action=loop.choose_loop_action(snapshot),
                snapshot=snapshot,
            )
        from datetime import timedelta

        from nexus.paper.agent_cycle import resume_schwab_agent_paper_cycle
        from nexus.persistence.models.agents import AgentModelConfigRecord
        from nexus.persistence.models.runtime import JobLease
        from sqlalchemy.ext.asyncio import AsyncSession

        async with factory() as session:
            model = await session.get(AgentModelConfigRecord, portfolio.model_config_id)
            after_price = model.pricing_expires_at + timedelta(seconds=1)
        original_scalar = AsyncSession.scalar

        async def elapsed(self, statement, *args, **kwargs):
            if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
                return after_price
            return await original_scalar(self, statement, *args, **kwargs)

        monkeypatch.setattr(AsyncSession, "scalar", elapsed)
        async with factory.begin() as session:
            lease = await session.scalar(select(JobLease).where(JobLease.job_id == job.id))
            assert lease.expires_at < after_price
            ended = await resume_schwab_agent_paper_cycle(
                session,
                cycle_id=portfolio.cycle_id,
                idempotency_key="known-orphan-close",
                now=after_price,
                daily_limit_usd=host.agent_daily_budget_usd,
                formal_stage_capacity=True,
            )
            assert ended.status == "unavailable"
    else:
        assert (
            await loop.execute_agent_paper_loop_action(
                host,
                account_id=host.agent_paper_account_id,
                repo_root=root,
                env_file=env_file,
                action=loop.choose_loop_action(snapshot),
                snapshot=snapshot,
            )
            is None
        )
    async with factory() as session:
        result = await session.get(AgentPortfolioSynthesisRunRecord, portfolio.id)
        reservation = await session.scalar(
            select(AgentPortfolioSynthesisBudgetReservationRecord).where(
                AgentPortfolioSynthesisBudgetReservationRecord.run_id == portfolio.id
            )
        )
        current = await session.get(Job, job.id)
        assert current.attempt_count == (0 if case == "wrong_key" else 1)
        assert result.status == (
            "completed"
            if verified_readback
            else {
                "wrong_key": "prepared",
                "complete": "completed",
                "illegal_action": "abstained",
            }.get(case, "failed")
        ), (actual, current.last_error_code, current.result_code)
        if verified_readback or case in {
            "complete",
            "invalid_output",
            "known_orphan",
            "illegal_action",
            "cancel_after_terminal",
        }:
            assert reservation.state == "settled" and result.settled_cost_usd == Decimal("0.0299")
        elif case in {"wrong_scope", "source_expired"}:
            assert reservation.state == "released" and result.settled_cost_usd == 0
        else:
            assert reservation.state == "reserved" and result.settled_cost_usd is None
        if output_overrun is not None:
            from datetime import timedelta

            from nexus.agents.budget import BudgetExhausted
            from nexus.agents.persistence import reserve_daily_budget
            from nexus.agents.research_usage import require_research_accounting_clear

            assert actual_cost is not None and baseline is not None and terminal_saved.is_set()
            assert (actual_cost > reservation.reserved_usd) == (case != "token_overrun")
            occupied = max(actual_cost, reservation.reserved_usd)
            assert await _native_usage(session, reservation) == occupied
            assert (
                await _shared_daily_budget_used(session, business_date=reservation.business_date)
                == baseline + occupied - reservation.reserved_usd
            )
            assert inventory_baseline is not None
            inventory = await inspect_inventory(session, **inventory_input)
            assert "RESEARCH_ACCOUNTING_UNSETTLED" in inventory.blocking_codes
            assert inventory.conservative_native_occupied_usd == (
                inventory_baseline + occupied - reservation.reserved_usd
            )
            with pytest.raises(BudgetExhausted, match="RESEARCH_ACCOUNTING_UNSETTLED"):
                await require_research_accounting_clear(session)
            with pytest.raises(BudgetExhausted, match="RESEARCH_ACCOUNTING_UNSETTLED"):
                await reserve_daily_budget(
                    session,
                    run_id=uuid4(),
                    now=at + timedelta(days=1),
                    daily_limit_usd=Decimal("100"),
                    worst_case_usd=Decimal("1"),
                    pricing_version="legacy-sdk",
                    pricing_fresh=True,
                )
    assert actual == (
        []
        if case in {"wrong_key", "auth_lost", "wrong_scope", "source_expired"}
        else ["/v1/responses"]
    )
    assert all(c.is_closed for c in clients)
    if case == "wrong_key":
        return
    from nexus.paper.agent_cycle import resume_schwab_agent_paper_cycle
    from nexus.persistence.models.runtime import AuditEvent
    from sqlalchemy import func

    async with factory.begin() as session:
        rows = (
            await session.scalars(
                select(AuditEvent)
                .where(AuditEvent.actor_id == f"sol-portfolio:{job.id}")
                .order_by(AuditEvent.event_seq)
            )
        ).all()
        names = [r.action for r in rows]
        if case == "source_expired":
            assert names == ["portfolio.responses.prepared", "portfolio.responses.cleanup"]
        else:
            assert names[:2] == ["portfolio.responses.prepared", "portfolio.responses.authorized"]
        assert names.count("portfolio.responses.terminal") == int(
            case
            not in {"timeout", "wrong_scope", "auth_lost", "terminal_not_saved", "source_expired"}
        )
        if case == "cleanup_not_saved":
            assert names[-1] == "portfolio.responses.terminal"
        else:
            assert names[-1] == "portfolio.responses.cleanup" and rows[-1].payload["confirmed"] == (
                case != "close_failed"
            )
            assert rows[0].payload["body_hash"] == rows[-1].payload["identity"]["body_hash"]
        from nexus.agents.platform_portfolio import native_unknown_receipts
        from nexus.persistence.models.runtime import JobAttempt

        attempt = await session.scalar(select(JobAttempt).where(JobAttempt.job_id == job.id))
        read_at = await session.scalar(select(func.clock_timestamp()))
        if case in {"close_failed", "cleanup_not_saved", "source_expired"}:
            with pytest.raises(
                ValueError, match="PORTFOLIO_SYNTHESIS_PLATFORM_USAGE_PROOF_REQUIRED"
            ):
                await native_unknown_receipts(session, job, attempt, read_at)
        elif case not in {"timeout", "unknown_cache", "auth_lost", "terminal_not_saved"}:
            with pytest.raises(ValueError, match="PORTFOLIO_SYNTHESIS_USAGE_KNOWN"):
                await native_unknown_receipts(session, job, attempt, read_at)
        else:
            assert [
                event.id for event in await native_unknown_receipts(session, job, attempt, read_at)
            ] == [r.id for r in rows]
        ended = await resume_schwab_agent_paper_cycle(
            session,
            cycle_id=portfolio.cycle_id,
            idempotency_key="sol-api-final",
            now=await session.scalar(select(func.clock_timestamp())),
            daily_limit_usd=host.agent_daily_budget_usd,
            formal_stage_capacity=True,
        )
        assert ended.status == (
            "completed" if case == "complete" or verified_readback else "unavailable"
        ), ended
        from datetime import timedelta

        from nexus.paper.manual_request import require_manual_account_available
        from nexus.persistence.models.runtime import JobLease

        checked = await session.scalar(select(func.clock_timestamp()))
        expiry = await session.scalar(select(func.max(JobLease.expires_at)))
        if case in {"known_orphan", "overrun_orphan"}:
            assert checked > expiry
        else:
            assert expiry > checked
            with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                await require_manual_account_available(
                    session, account_id=host.agent_paper_account_id, now=checked, read_only=True
                )
        after_leases = max(checked, expiry + timedelta(microseconds=1))
        if case in {
            "timeout",
            "close_failed",
            "unknown_cache",
            "auth_lost",
            "terminal_not_saved",
            "cleanup_not_saved",
            "overrun",
            "overrun_orphan",
            "token_overrun",
        }:
            with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                await require_manual_account_available(
                    session,
                    account_id=host.agent_paper_account_id,
                    now=after_leases,
                    read_only=True,
                )
        else:
            await require_manual_account_available(
                session, account_id=host.agent_paper_account_id, now=after_leases, read_only=True
            )
        if case == "complete":
            assert ended.result_code == "PORTFOLIO_ASSESSMENT_DEFERRED"
