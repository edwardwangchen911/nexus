from datetime import timedelta

import pytest
from integration.test_portfolio_synthesis_persistence_pg import seed_synthesis, synthesis_intent
from nexus.agents.portfolio_synthesis_persistence import admit_portfolio_synthesis
from nexus.agents.portfolio_synthesis_worker import read_synthesis_job, run_synthesis_job_once
from nexus.agents.responses_budget import UsageSnapshot
from nexus.paper.agent_decision_store import load_portfolio_proposal, persist_portfolio_proposal
from nexus.paper.target_service import approve_initial_target_projection
from nexus.persistence.models.agents import AgentPortfolioSynthesisOutputRecord
from nexus.persistence.models.paper import (
    AgentPaperCycleRecord,
    PaperFillRecord,
    PaperLedgerTransactionRecord,
    PaperOrderBatchRecord,
    PaperOrderRecord,
    PaperReservationRecord,
    PaperTargetVersionRecord,
    StrategyDecisionRecord,
)
from sqlalchemy import func, select


async def proposal_source(url, *, approve=True):
    engine, sessions, kwargs = await seed_synthesis(url)
    async with sessions.begin() as session:
        cycle = await session.get(AgentPaperCycleRecord, kwargs["cycle_id"])
        if approve:
            await approve_initial_target_projection(
                session,
                account_id=cycle.account_id,
                projection_as_of=cycle.decision_time,
                expected_projection_hash=kwargs["projection_hash"],
                approved_by="local-user",
                approved_at=kwargs["now"],
                idempotency_key="first-baseline",
            )
        admission = await admit_portfolio_synthesis(session, **kwargs)
        raw = synthesis_intent(admission.run).model_dump_json()
        job = await read_synthesis_job(session, cycle_id=kwargs["cycle_id"])
        assert job is not None

    class Transport:
        def supports(self, model):
            return True

        async def invoke(self, payload):
            return raw, UsageSnapshot(input_tokens=10, output_tokens=20, provider_calls=1)

    assert await run_synthesis_job_once(
        sessions,
        transport=Transport(),
        worker_id="proposal-test",
        job_ids=(job.id,),
        clock=lambda: kwargs["now"] + timedelta(seconds=1),
    )
    async with sessions() as session:
        output_id = await session.scalar(
            select(AgentPortfolioSynthesisOutputRecord.id).where(
                AgentPortfolioSynthesisOutputRecord.run_id == admission.run.id
            )
        )
        assert output_id is not None
    return (
        engine,
        sessions,
        dict(
            cycle_id=kwargs["cycle_id"],
            synthesis_output_id=output_id,
            now=kwargs["now"] + timedelta(seconds=2),
        ),
    )


@pytest.mark.asyncio
async def test_proposal_persists_exact_full_target_replay_and_no_execution(migrated_database_url):
    engine, sessions, args = await proposal_source(migrated_database_url)
    try:
        async with sessions.begin() as session:
            ledger_count = await session.scalar(
                select(func.count()).select_from(PaperLedgerTransactionRecord)
            )
            first = await persist_portfolio_proposal(session, **args)
        async with sessions.begin() as session:
            replay = await persist_portfolio_proposal(session, **args)
            read = await load_portfolio_proposal(
                session, account_id=first.account_id, decision_id=first.decision_id
            )
            assert first == replay == read
            assert first.proposal.cash_weight == 1
            assert all(weight == 0 for weight in first.proposal.weights.values())
            assert first.proposal.baseline.initial_approval_id is not None
            from nexus.persistence.models.paper import StrategyDecisionAgentOutputRecord

            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(StrategyDecisionAgentOutputRecord)
                    .where(
                        StrategyDecisionAgentOutputRecord.strategy_decision_id == first.decision_id
                    )
                )
                == 2
            )
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(StrategyDecisionRecord)
                    .where(StrategyDecisionRecord.agent_paper_cycle_id == args["cycle_id"])
                )
                == 1
            )
            for model in (
                PaperTargetVersionRecord,
                PaperOrderBatchRecord,
                PaperOrderRecord,
                PaperFillRecord,
                PaperReservationRecord,
            ):
                assert await session.scalar(select(func.count()).select_from(model)) == 0
            assert (
                await session.scalar(select(func.count()).select_from(PaperLedgerTransactionRecord))
                == ledger_count
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["missing_approval", "expired", "changed_content"])
async def test_proposal_fail_closed_authority(migrated_database_url, kind):
    engine, sessions, args = await proposal_source(
        migrated_database_url, approve=kind != "missing_approval"
    )
    try:
        if kind == "expired":
            args["now"] += timedelta(days=1)
        async with sessions.begin() as session:
            if kind == "changed_content":
                first = await persist_portfolio_proposal(session, **args)
                args["proposal"] = first.proposal.model_copy(update={"cash_weight": "0.9"})
            with pytest.raises(ValueError, match="POLICY_UNCONFIRMED|EXPIRED|CONFLICT"):
                await persist_portfolio_proposal(session, **args)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("approved", [True, False])
async def test_cycle_handoff_is_recorded_once_and_never_executes(migrated_database_url, approved):
    from nexus.paper.agent_cycle import _events, _resume_batch, _resume_portfolio_synthesis

    engine, sessions, args = await proposal_source(migrated_database_url, approve=approved)
    try:
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            result = await _resume_portfolio_synthesis(
                session,
                cycle=cycle,
                events=await _events(session, cycle.id),
                run_ids=(),
                now=args["now"],
                daily_limit_usd=None,
            )
            assert result.result_code == (
                "TARGET_ACTIVATION_PENDING" if approved else "POLICY_UNCONFIRMED"
            )
            assert result.status == ("decision_recorded" if approved else "unavailable")
            if approved:
                count = len(await _events(session, cycle.id))
                replay = await _resume_batch(
                    session, cycle=cycle, events=await _events(session, cycle.id)
                )
                assert replay.result_code == "TARGET_ACTIVATION_PENDING"
                assert replay.strategy_decision_id == result.strategy_decision_id
                assert len(await _events(session, cycle.id)) == count
                from helpers.paper_batch import CALENDAR_ID
                from nexus.ops.agent_paper_loop import (
                    AgentPaperLoopAction,
                    _inspect_portfolio_batch,
                    choose_loop_action,
                )
                from nexus.paper.agent_cycle_authority import ServiceCalendarAuthority

                calendar = ServiceCalendarAuthority(
                    calendar_authority_id=CALENDAR_ID,
                    as_of=args["now"],
                    local_date=cycle.decision_session,
                    session=cycle.decision_session,
                    decision_window_start_at=cycle.decision_time,
                    market_open_at=cycle.decision_time,
                    market_close_at=args["now"] + timedelta(hours=1),
                    next_session=(args["now"] + timedelta(days=1)).date(),
                    next_decision_window_start_at=args["now"] + timedelta(days=1),
                    next_market_close_at=args["now"] + timedelta(days=1, hours=1),
                )
                for _ in range(2):
                    snapshot = await _inspect_portfolio_batch(
                        session,
                        account_id=cycle.account_id,
                        now=args["now"],
                        calendar=calendar,
                        performance_due_at=None,
                    )
                    assert snapshot is not None
                    assert snapshot.cycle_result_code == "TARGET_ACTIVATION_PENDING"
                    assert (
                        choose_loop_action(snapshot) == AgentPaperLoopAction.STOP_CYCLE_UNAVAILABLE
                    )
                assert len(await _events(session, cycle.id)) == count
            for model in (PaperTargetVersionRecord, PaperOrderBatchRecord, PaperOrderRecord):
                assert await session.scalar(select(func.count()).select_from(model)) == 0
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["maintain", "reduce", "exit", "increase", "include"])
@pytest.mark.parametrize("has_active_target", [False, True])
async def test_first_synthesis_knows_actual_exposure_without_granting_target_authority(
    migrated_database_url, action, has_active_target
):
    from decimal import Decimal
    from uuid import uuid4

    from nexus.agents.portfolio_synthesis_contracts import (
        PortfolioSynthesisInputV1,
        PortfolioSynthesisIntentV1,
        canonical_portfolio_synthesis_hash,
        validate_portfolio_synthesis_intent,
    )
    from nexus.risk.canonical import canonical_paper_projection_hash
    from nexus.risk.models import PaperPortfolioProjection, PaperPositionBalance

    engine, sessions, kwargs = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            run = (await admit_portfolio_synthesis(session, **kwargs)).run
            frozen = PortfolioSynthesisInputV1.model_validate(run.frozen_input)
            key = frozen.candidate_instrument_ids[0]
            data = frozen.projection.model_dump()
            data["positions"] = (
                PaperPositionBalance(
                    instrument_id=key,
                    quantity=Decimal(1),
                    reserved_quantity=Decimal(0),
                    cost_basis=Decimal(100),
                    reference_close=Decimal(100),
                ),
            )
            data["available_cash"] -= Decimal(100)
            projection = PaperPortfolioProjection.model_validate(data)
            data["projection_hash"] = canonical_paper_projection_hash(projection)
            projection = PaperPortfolioProjection.model_validate(data)
            doc = frozen.model_dump()
            doc["projection"] = projection
            doc["projection_hash"] = projection.projection_hash
            if has_active_target:
                # Actual drift must not rewrite the active all-cash target's semantics.
                doc.update(
                    active_target_version_id=uuid4(),
                    active_target_content_hash="a" * 64,
                    active_target_cash_weight=Decimal(1),
                )
            doc["security_outputs"][0]["intent"].update(
                quality_status="complete",
                stance="buy_candidate" if action in {"include", "increase"} else "hold",
            )
            frozen = PortfolioSynthesisInputV1.model_validate(doc)
            choice_doc = synthesis_intent(run).model_dump()
            choice_doc["input_hash"] = canonical_portfolio_synthesis_hash(frozen)
            choice_doc["choices"][0].update(
                action=action, role="core", rank=1 if action in {"include", "increase"} else None
            )
            intent = PortfolioSynthesisIntentV1.model_validate(choice_doc)
            if (action == "include") != has_active_target:
                with pytest.raises(ValueError, match="exceeds"):
                    validate_portfolio_synthesis_intent(intent, frozen)
            else:
                validate_portfolio_synthesis_intent(intent, frozen)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_later_proposal_uses_active_target_and_preserves_historical_replay(
    migrated_database_url,
):
    """Seed a second immutable research checkpoint, then use real synthesis/proposal services."""
    import json
    from copy import deepcopy
    from decimal import Decimal
    from hashlib import sha256
    from uuid import uuid4

    from nexus.paper.agent_cycle import _append_event
    from nexus.paper.portfolio_proposal import canonical_portfolio_target_content_hash
    from nexus.paper.projection import load_paper_projection
    from nexus.persistence.models.agents import (
        AgentPortfolioSynthesisRunRecord,
        AgentResearchOutputEvidenceRecord,
        AgentResearchOutputRecord,
        AgentResearchRunRecord,
    )
    from nexus.persistence.models.runtime import Job

    engine, sessions, args = await proposal_source(migrated_database_url)

    def values(record):
        return {
            column.name: deepcopy(getattr(record, column.name))
            for column in record.__table__.columns
            if column.name != "id"
        }

    try:
        async with sessions.begin() as session:
            first = await persist_portfolio_proposal(session, **args)
            cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            target = PaperTargetVersionRecord(
                account_id=first.account_id,
                portfolio_id=cycle.portfolio_id,
                version_seq=1,
                strategy_decision_id=first.decision_id,
                previous_target_version_id=None,
                target_content_hash=canonical_portfolio_target_content_hash(first.proposal),
                activation_hash="a" * 64,
                activated_at=args["now"],
                created_at=args["now"],
            )
            session.add(target)
            await session.flush()
            shift = timedelta(minutes=5)
            new_time = cycle.decision_time + shift
            second = AgentPaperCycleRecord(
                id=uuid4(),
                **(
                    values(cycle)
                    | {
                        "decision_time": new_time,
                        "decision_slot_at": cycle.decision_slot_at + shift,
                        "created_at": new_time,
                    }
                ),
            )
            session.add(second)
            await session.flush()
            synthesis_run = await session.scalar(
                select(AgentPortfolioSynthesisRunRecord).where(
                    AgentPortfolioSynthesisRunRecord.cycle_id == cycle.id
                )
            )
            run_ids, output_ids = [], []
            for source_id in synthesis_run.research_output_ids:
                old_output = await session.get(AgentResearchOutputRecord, source_id)
                old_run = await session.get(AgentResearchRunRecord, old_output.run_id)
                new_run = AgentResearchRunRecord(
                    id=uuid4(),
                    **(
                        values(old_run)
                        | {
                            "decision_time": new_time,
                            "idempotency_key": f"agent-paper:{second.id}:{old_run.instrument_id}",
                            "created_at": new_time,
                        }
                    ),
                )
                session.add(new_run)
                await session.flush()
                intent_doc = deepcopy(old_output.intent)
                intent_doc["decision_time"] = new_time.isoformat()
                intent_doc["decision_id"] = str(new_run.id)
                from nexus.strategy.intents import ResearchIntent

                intent_doc = ResearchIntent.model_validate(intent_doc).model_dump(mode="json")
                content = {
                    "status": "completed",
                    "role": None,
                    "payload": old_output.payload,
                    "intent": intent_doc,
                }
                new_output = AgentResearchOutputRecord(
                    id=uuid4(),
                    **(
                        values(old_output)
                        | {
                            "run_id": new_run.id,
                            "intent": intent_doc,
                            "content_hash": sha256(
                                json.dumps(
                                    content, sort_keys=True, separators=(",", ":"), default=str
                                ).encode()
                            ).hexdigest(),
                            "created_at": new_time + timedelta(minutes=1),
                        }
                    ),
                )
                session.add(new_output)
                await session.flush()
                for evidence in await session.scalars(
                    select(AgentResearchOutputEvidenceRecord).where(
                        AgentResearchOutputEvidenceRecord.output_id == old_output.id
                    )
                ):
                    session.add(
                        AgentResearchOutputEvidenceRecord(
                            **(values(evidence) | {"output_id": new_output.id})
                        )
                    )
                old_job = await session.scalar(
                    select(Job).where(Job.payload["run_id"].as_string() == str(old_run.id))
                )
                session.add(
                    Job(
                        id=uuid4(),
                        **(
                            values(old_job)
                            | {
                                "job_key": f"second-research:{new_run.id}",
                                "payload": {"run_id": str(new_run.id)},
                                "effective_at": new_time,
                                "started_at": new_time,
                                "finished_at": new_time + timedelta(minutes=1),
                                "created_at": new_time,
                            }
                        ),
                    )
                )
                run_ids.append(new_run.id)
                output_ids.append(new_output.id)
            await session.flush()
            await _append_event(session, second, None, "prepared", "TEST_PREPARED", {})
            await _append_event(
                session,
                second,
                "prepared",
                "researching",
                "RESEARCH_JOBS_PREPARED",
                {"research_run_ids": list(map(str, run_ids))},
            )
            projection = await load_paper_projection(
                session, account_id=first.account_id, as_of=new_time
            )
            admitted = await admit_portfolio_synthesis(
                session,
                cycle_id=second.id,
                research_output_ids=tuple(output_ids),
                active_target_version_id=target.id,
                projection_hash=projection.projection_hash,
                daily_limit_usd=Decimal(1),
                now=new_time + timedelta(minutes=2),
            )
            raw = synthesis_intent(admitted.run).model_dump_json()
            job = await read_synthesis_job(session, cycle_id=second.id)

        class Transport:
            def supports(self, model):
                return True

            async def invoke(self, payload):
                return raw, UsageSnapshot(input_tokens=10, output_tokens=20, provider_calls=1)

        at = new_time + timedelta(minutes=2, seconds=1)
        assert await run_synthesis_job_once(
            sessions, transport=Transport(), worker_id="second", job_ids=(job.id,), clock=lambda: at
        )
        async with sessions.begin() as session:
            output = await session.scalar(
                select(AgentPortfolioSynthesisOutputRecord).where(
                    AgentPortfolioSynthesisOutputRecord.run_id == admitted.run.id
                )
            )
            latest = await persist_portfolio_proposal(
                session,
                cycle_id=second.id,
                synthesis_output_id=output.id,
                now=at + timedelta(seconds=1),
            )
            assert latest.proposal.baseline.target_version_id == target.id
            assert latest.proposal.baseline.initial_approval_id is None
            assert latest.proposal.weights == first.proposal.weights
            assert latest.proposal.cash_weight == first.proposal.cash_weight
            assert await persist_portfolio_proposal(session, **args) == first
    finally:
        await engine.dispose()
