"""Real DB admission, replay, evidence binding and shared-budget regressions."""

import asyncio
import json
from datetime import timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import UUID, uuid4

import pytest
from helpers.paper_batch import CALENDAR_ID, DECISION_TIME, INSTRUMENTS, seed_paper_batch
from integration.test_agent_paper_cycle_pg import (
    _safe_no_trade_intent,
    _seed_two_candidate_authority,
)
from nexus.agents.budget import BudgetExhausted
from nexus.agents.persistence import (
    _shared_daily_budget_used,
    load_complete_prompt_authority,
    reserve_daily_budget,
)
from nexus.agents.portfolio_synthesis_contracts import (
    PortfolioSynthesisInputV1,
    PortfolioSynthesisIntentV1,
    canonical_portfolio_synthesis_hash,
    validate_portfolio_synthesis_intent,
)
from nexus.agents.portfolio_synthesis_persistence import (
    admit_portfolio_synthesis,
    persist_portfolio_synthesis_terminal,
)
from nexus.agents.responses_budget import UsageSnapshot
from nexus.audit.service import append_audit_event
from nexus.domain.evidence import EvidenceRef
from nexus.ops.portfolio_challenger_compare import CompareRequest, execute_compare, inspect_compare
from nexus.paper.agent_challenger import (
    PORTFOLIO_CHALLENGER_PUBLICATION_ACTION,
    AgentChallengerError,
    PortfolioChallengerPublicationV1,
)
from nexus.paper.agent_cycle import _start_cycle
from nexus.paper.agent_cycle_models import floor_decision_slot
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.paper.portfolio_challenger_case import (
    compare_portfolio_challenger_decisions,
    freeze_prospective_portfolio_challenger_case,
)
from nexus.paper.projection import load_paper_projection
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPortfolioSynthesisBudgetReservationRecord,
    AgentPortfolioSynthesisOutputRecord,
    AgentPortfolioSynthesisRunRecord,
    AgentPromptVersionRecord,
    AgentResearchOutputEvidenceRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.persistence.models.runtime import AuditEvent, Job
from nexus.settings import EngineSettings
from nexus.strategy.forward import load_forward_strategy_decision
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


async def seed_synthesis(
    url,
    *,
    normal_path=None,
    complete_research=True,
    candidate_instruments=INSTRUMENTS[:2],
):
    additional = tuple(item for item in candidate_instruments if item not in INSTRUMENTS)
    if normal_path is None:
        seed = await seed_paper_batch(url, additional_instruments=additional)
        engine = create_async_engine(url)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
    else:
        engine, sessions, authority, _ = normal_path
    async with sessions.begin() as session:
        if normal_path is None:
            registered = await load_forward_strategy_decision(
                session,
                account_id=seed.account_id,
                strategy_version="baseline_v1",
                decision_session=DECISION_TIME.date(),
            )
            authority = await _seed_two_candidate_authority(
                session,
                registered,
                authority_instruments=INSTRUMENTS + additional,
                candidate_instruments=candidate_instruments,
            )
        decision_time = authority.decision_time
        # A synthetic one-hour remaining session makes expiry deterministic, without wall time.
        await session.execute(text("ALTER TABLE exchange_calendar_sessions DISABLE TRIGGER USER"))
        await session.execute(
            text(
                "UPDATE exchange_calendar_sessions SET market_close_at=:close,decision_at=:close "
                "WHERE calendar_authority_id=:calendar AND session=:day"
            ),
            {
                "close": decision_time + timedelta(hours=1),
                "calendar": CALENDAR_ID,
                "day": decision_time.date(),
            },
        )
        await session.execute(text("ALTER TABLE exchange_calendar_sessions ENABLE TRIGGER USER"))
        batch = await create_agent_paper_candidate_batch(
            session,
            account_id=authority.account_id,
            universe_snapshot_id=authority.universe_snapshot_id,
            instrument_ids=tuple(item[0] for item in candidate_instruments),
            idempotency_key="synthesis-candidates",
            created_at=decision_time,
        )
        started = await _start_cycle(
            session,
            authority=authority,
            decision_slot_at=floor_decision_slot(decision_time),
            daily_limit_usd=Decimal("1"),
            candidate_batch_id=batch.batch_id,
            requested_instrument_ids=batch.instrument_ids,
        )
        assert len(started.research_run_ids) == len(candidate_instruments)
        outputs = []
        symbol_by_instrument = {
            instrument_id: "AAPL" if index == 1 else f"T{index}"
            for index, (instrument_id, *_rest) in enumerate(INSTRUMENTS + additional, start=1)
        }
        for run_id in started.research_run_ids:
            if not complete_research:
                continue
            run = await session.get(AgentResearchRunRecord, run_id)
            evidence = await session.get(ResearchEvidenceRef, UUID(run.evidence_manifest[0]["id"]))
            intent = _safe_no_trade_intent(
                run=run, evidence=evidence, valid_until=decision_time + timedelta(days=1)
            )
            intent = intent.model_copy(
                update={"display_symbol": symbol_by_instrument[run.instrument_id]}
            )
            bound_records = [
                await session.get(ResearchEvidenceRef, UUID(frozen["id"]))
                for frozen in run.evidence_manifest
            ]
            intent = intent.model_copy(
                update={
                    "evidence_refs": tuple(
                        EvidenceRef(
                            source=bound.source,
                            object_id=bound.provider_object_id,
                            available_at=bound.available_at,
                            location=bound.location,
                            content_hash=bound.content_hash,
                        )
                        for bound in bound_records
                    )
                }
            )
            document = intent.model_dump(mode="json")
            payload = {"quality_status": intent.quality_status}
            content = {"status": "completed", "role": None, "payload": payload, "intent": document}
            output = AgentResearchOutputRecord(
                id=uuid4(),
                run_id=run_id,
                sequence=1,
                status="completed",
                role=None,
                prompt_hash=None,
                payload=payload,
                intent=document,
                content_hash=sha256(
                    json.dumps(content, sort_keys=True, separators=(",", ":"), default=str).encode()
                ).hexdigest(),
                created_at=decision_time + timedelta(minutes=1),
            )
            session.add(output)
            await session.flush()
            for bound in bound_records:
                session.add(
                    AgentResearchOutputEvidenceRecord(
                        output_id=output.id,
                        evidence_ref_id=bound.id,
                        source=bound.source,
                        provider_object_id=bound.provider_object_id,
                        revision=bound.revision,
                        available_at=bound.available_at,
                        location=bound.location,
                        content_hash=bound.content_hash,
                    )
                )
            await session.execute(text("ALTER TABLE agent_research_runs DISABLE TRIGGER USER"))
            await session.execute(
                text("UPDATE agent_research_runs SET status='completed' WHERE id=:id"),
                {"id": run_id},
            )
            await session.execute(text("ALTER TABLE agent_research_runs ENABLE TRIGGER USER"))
            job = await session.scalar(
                select(Job).where(Job.payload["run_id"].as_string() == str(run_id))
            )
            job.status = "running"
            job.started_at = decision_time
            job.attempt_count = 1
            job.version += 1
            await session.flush()
            job.status = "succeeded"
            job.finished_at = decision_time + timedelta(minutes=1)
            job.version += 1
            outputs.append(output.id)
        content = "Synthetic frozen portfolio synthesis prompt."
        session.add(
            AgentPromptVersionRecord(
                role="portfolio_decision",
                version="portfolio-management-synthesis-v1",
                content=content,
                content_hash=sha256(content.encode()).hexdigest(),
                created_at=decision_time,
            )
        )
        projection = await load_paper_projection(
            session, account_id=authority.account_id, as_of=decision_time
        )
    return (
        engine,
        sessions,
        {
            "cycle_id": started.cycle_id,
            "research_output_ids": tuple(outputs),
            "active_target_version_id": None,
            "projection_hash": projection.projection_hash,
            "daily_limit_usd": Decimal("1"),
            "now": decision_time + timedelta(minutes=2),
        },
    )


def synthesis_intent(run):
    frozen = PortfolioSynthesisInputV1.model_validate(run.frozen_input)
    choices = []
    for source in frozen.security_outputs:
        statement = {
            "text": "Evidence remains incomplete; defer the comparison.",
            "references": [
                {
                    "research_output_id": source.research_output_id,
                    "evidence_ref_id": source.evidence_ref_ids[0],
                }
            ],
        }
        choices.append(
            {
                "instrument_id": source.instrument_id,
                "action": "defer",
                "role": "none",
                "rank": None,
                "replaces_instrument_id": None,
                "depends_on_instrument_ids": [],
                "thesis": statement,
                "counter_thesis": statement,
                "invalidation": statement,
            }
        )
    return PortfolioSynthesisIntentV1.model_validate(
        {
            "schema_version": "portfolio_synthesis_intent_v1",
            "input_hash": run.input_hash,
            "compared_instrument_ids": frozen.candidate_instrument_ids,
            "choices": choices,
            "cash_rationale": choices[0]["thesis"],
        }
    )


@pytest.mark.asyncio
async def test_portfolio_challenger_case_requires_publication_before_new_frozen_run(
    migrated_database_url,
):
    engine, sessions, kwargs = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            run = (await admit_portfolio_synthesis(session, **kwargs)).run
            frozen = PortfolioSynthesisInputV1.model_validate(run.frozen_input)
            incumbent = await session.get(AgentPromptVersionRecord, run.prompt_version_id)
            assert incumbent is not None
            challenger_content = incumbent.content + "\nCompare unresolved evidence separately."
            challenger = AgentPromptVersionRecord(
                id=uuid4(),
                role="portfolio_decision",
                version="portfolio-challenger-test-v1",
                release_channel="challenger",
                content=challenger_content,
                content_hash=sha256(challenger_content.encode()).hexdigest(),
                created_at=frozen.admitted_at - timedelta(seconds=1),
            )
            session.add(challenger)
            await session.flush()
            publication = PortfolioChallengerPublicationV1(
                schema_version="portfolio_challenger_pub_v1",
                proposal_hash="a" * 64,
                incumbent_prompt_version_id=incumbent.id,
                challenger_prompt_version_id=challenger.id,
                challenger_prompt_hash=challenger.content_hash,
                challenger_model_config_id=run.model_config_id,
                replayed=False,
            )
            case = freeze_prospective_portfolio_challenger_case(
                run=run,
                incumbent_prompt=incumbent,
                challenger_prompt=challenger,
                publication=publication,
                published_at=frozen.admitted_at - timedelta(seconds=1),
                now=frozen.admitted_at + timedelta(seconds=1),
            )
            assert len(case.frozen_input.candidate_instrument_ids) == 2
            assert case.source_input_hash == run.input_hash
            assert case.challenger_input_hash != run.input_hash
            assert case.frozen_input.prompt_version_id == challenger.id
            assert run.frozen_input["prompt_version_id"] == str(incumbent.id)
            old_intent = synthesis_intent(run)
            with pytest.raises(ValueError, match="portfolio synthesis input binding mismatch"):
                validate_portfolio_synthesis_intent(old_intent, case.frozen_input)
            challenger_intent = old_intent.model_copy(
                update={"input_hash": case.challenger_input_hash}
            )
            validate_portfolio_synthesis_intent(challenger_intent, case.frozen_input)
            same = compare_portfolio_challenger_decisions(
                incumbent_input=frozen,
                incumbent_intent=old_intent,
                challenger_input=case.frozen_input,
                challenger_intent=challenger_intent,
            )
            assert not same.selection_changed and not same.reasoning_changed
            changed_choice = challenger_intent.choices[0].model_copy(
                update={
                    "thesis": challenger_intent.choices[0].thesis.model_copy(
                        update={"text": "The same evidence remains unresolved for this candidate."}
                    )
                }
            )
            changed_intent = PortfolioSynthesisIntentV1.model_validate(
                challenger_intent.model_copy(
                    update={"choices": (changed_choice, *challenger_intent.choices[1:])}
                ).model_dump(mode="json")
            )
            reasoning = compare_portfolio_challenger_decisions(
                incumbent_input=frozen,
                incumbent_intent=old_intent,
                challenger_input=case.frozen_input,
                challenger_intent=changed_intent,
            )
            assert not reasoning.selection_changed and reasoning.reasoning_changed
            assert reasoning.economic_result == "not_evaluated"
            with pytest.raises(
                AgentChallengerError, match="PORTFOLIO_CHALLENGER_CASE_NOT_PROSPECTIVE"
            ):
                freeze_prospective_portfolio_challenger_case(
                    run=run,
                    incumbent_prompt=incumbent,
                    challenger_prompt=challenger,
                    publication=publication,
                    published_at=frozen.admitted_at,
                    now=frozen.admitted_at + timedelta(seconds=1),
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("model_failure", "incumbent_status"),
    [(False, "completed"), (False, "abstained"), (True, None)],
)
async def test_portfolio_challenger_compare_is_once_only_and_cannot_create_orders(
    migrated_database_url,
    model_failure,
    incumbent_status,
):
    from nexus.persistence.models.paper import PaperOrderRecord

    engine, sessions, kwargs = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            incumbent = await session.scalar(
                select(AgentPromptVersionRecord).where(
                    AgentPromptVersionRecord.role == "portfolio_decision",
                    AgentPromptVersionRecord.release_channel == "active",
                )
            )
            model = await session.scalar(select(AgentModelConfigRecord))
            assert incumbent is not None and model is not None
            content = incumbent.content + "\nKeep candidate evidence separate."
            challenger = AgentPromptVersionRecord(
                id=uuid4(),
                role="portfolio_decision",
                version="portfolio-challenger-comparison-test-v1",
                release_channel="challenger",
                content=content,
                content_hash=sha256(content.encode()).hexdigest(),
                created_at=kwargs["now"] - timedelta(seconds=1),
            )
            session.add(challenger)
            await session.flush()
            publication = PortfolioChallengerPublicationV1(
                schema_version="portfolio_challenger_pub_v1",
                proposal_hash="b" * 64,
                incumbent_prompt_version_id=incumbent.id,
                challenger_prompt_version_id=challenger.id,
                challenger_prompt_hash=challenger.content_hash,
                challenger_model_config_id=model.id,
                replayed=False,
            )
            await append_audit_event(
                session,
                actor_id="human-local",
                action=PORTFOLIO_CHALLENGER_PUBLICATION_ACTION,
                schema_version=publication.schema_version,
                payload=publication.model_dump(mode="json"),
                occurred_at=kwargs["now"] - timedelta(seconds=1),
            )
        async with sessions.begin() as session:
            run = (await admit_portfolio_synthesis(session, **kwargs)).run
            assert run.model_config_id == model.id

        class FakeTransport:
            calls = 0

            def supports(self, _model):
                return True

            async def invoke(self, payload):
                self.calls += 1
                if model_failure:
                    raise RuntimeError("private-model-detail")
                intent = synthesis_intent(run).model_copy(
                    update={"input_hash": payload["input_hash"]}
                )
                return intent.model_dump_json(), UsageSnapshot(
                    input_tokens=100,
                    output_tokens=50,
                    provider_calls=1,
                    tool_calls=0,
                )

        fake = FakeTransport()
        request = CompareRequest(cycle_id=run.cycle_id, proposal_hash=publication.proposal_hash)
        settings = EngineSettings(
            database_url=migrated_database_url, agent_runtime_mode="codex_sdk"
        )
        assert (await inspect_compare(settings, request)).status == "not_run"
        observed_at = kwargs["now"] + timedelta(seconds=1)
        if model_failure:
            with pytest.raises(AgentChallengerError, match="PORTFOLIO_CHALLENGER_RESULT_UNKNOWN"):
                await execute_compare(settings, request, transport=fake, clock=lambda: observed_at)
            with pytest.raises(AgentChallengerError, match="PORTFOLIO_CHALLENGER_RESULT_UNKNOWN"):
                await execute_compare(settings, request, transport=fake, clock=lambda: observed_at)
        else:
            first = await execute_compare(
                settings, request, transport=fake, clock=lambda: observed_at
            )
            replay = await execute_compare(
                settings,
                request,
                transport=fake,
                clock=lambda: observed_at + timedelta(days=1),
            )
            assert first == replay
            assert first.intent.input_hash == first.challenger_input_hash
            assert first.challenger_input_hash != run.input_hash
        assert fake.calls == 1
        observed = await inspect_compare(settings, request)
        assert observed.status == ("result_unknown" if model_failure else "incumbent_pending")
        if not model_failure:
            async with sessions.begin() as session:
                current_run = await session.get(AgentPortfolioSynthesisRunRecord, run.id)
                assert current_run is not None
                current_run.status = "running"
            async with sessions.begin() as session:
                await persist_portfolio_synthesis_terminal(
                    session,
                    run_id=run.id,
                    input_hash=run.input_hash,
                    status=incumbent_status,
                    intent=synthesis_intent(run) if incumbent_status == "completed" else None,
                    input_tokens=10,
                    output_tokens=20,
                    now=kwargs["now"] + timedelta(minutes=1),
                )
            compared = await inspect_compare(settings, request)
            if incumbent_status == "completed":
                assert compared.status == "compared"
                assert compared.selection_changed is False
                assert compared.reasoning_changed is False
                assert compared.changed_candidate_count == 0
                assert compared.economic_result == "not_evaluated"
            else:
                assert compared.status == "incumbent_unavailable"
                assert compared.selection_changed is None
        async with sessions() as session:
            assert await session.scalar(select(func.count(PaperOrderRecord.id))) == 0
            assert await session.scalar(
                select(func.count(AuditEvent.id)).where(
                    AuditEvent.action == "portfolio.challenger.compared"
                )
            ) == (0 if model_failure else 1)
            current_run = await session.get(AgentPortfolioSynthesisRunRecord, run.id)
            assert current_run is not None
            assert current_run.status == ("prepared" if model_failure else incumbent_status)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_five_candidate_cycle_reaches_no_trade_and_replays_exactly_once(
    migrated_database_url,
):
    from integration.test_portfolio_proposal_pg import proposal_source
    from nexus.paper.agent_cycle import _events, _resume_research
    from nexus.persistence.models.paper import (
        AgentPaperCycleRecord,
        PaperOrderRecord,
        StrategyDecisionRecord,
    )

    extra = (
        (
            UUID("51000000-0000-0000-0000-000000000014"),
            Decimal("80"),
            Decimal("0"),
            "consumer_staples",
        ),
        (
            UUID("51000000-0000-0000-0000-000000000015"),
            Decimal("120"),
            Decimal("0"),
            "communication_services",
        ),
    )
    candidates = INSTRUMENTS + extra
    seeded = await seed_synthesis(
        migrated_database_url,
        candidate_instruments=candidates,
    )
    engine, sessions, args = await proposal_source(
        migrated_database_url,
        seeded=seeded,
    )
    try:
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            assert cycle is not None
            assert len(cycle.candidate_manifest) == 5
            assert all(
                len(row["risk_query_authority_ids"]) == 3 for row in cycle.candidate_manifest
            )
            assert (
                await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 5
            )
            assert (
                await session.scalar(select(func.count()).select_from(AgentResearchOutputRecord))
                == 5
            )
            synthesis = await session.scalar(
                select(AgentPortfolioSynthesisRunRecord).where(
                    AgentPortfolioSynthesisRunRecord.cycle_id == cycle.id
                )
            )
            frozen = PortfolioSynthesisInputV1.model_validate(synthesis.frozen_input)
            assert frozen.candidate_instrument_ids == tuple(sorted(item[0] for item in candidates))
            first = await _resume_research(
                session,
                cycle=cycle,
                authority=None,
                events=await _events(session, cycle.id),
                now=args["now"],
            )
            assert first.status == "completed"
            assert first.result_code == "NO_ORDER_REQUIRED"
            counts = (
                await session.scalar(
                    select(func.count())
                    .select_from(StrategyDecisionRecord)
                    .where(StrategyDecisionRecord.agent_paper_cycle_id == cycle.id)
                ),
                await session.scalar(select(func.count()).select_from(PaperOrderRecord)),
            )
            replay = await _resume_research(
                session,
                cycle=cycle,
                authority=None,
                events=await _events(session, cycle.id),
                now=args["now"],
            )
            assert replay == first.model_copy(update={"replayed": True})
            assert counts == (1, 0)
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(StrategyDecisionRecord)
                    .where(StrategyDecisionRecord.agent_paper_cycle_id == cycle.id)
                ),
                await session.scalar(select(func.count()).select_from(PaperOrderRecord)),
            ) == counts
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_one_run_job_reservation_replay_and_conflict(migrated_database_url):
    engine, sessions, kwargs = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            first = await admit_portfolio_synthesis(session, **kwargs)
            frozen = PortfolioSynthesisInputV1.model_validate(first.run.frozen_input)
            assert first.run.input_hash == canonical_portfolio_synthesis_hash(frozen)
            assert len(frozen.security_outputs) == 2
            assert frozen.expires_at == DECISION_TIME + timedelta(hours=1)
        async with sessions.begin() as session:
            replay = await admit_portfolio_synthesis(
                session,
                **(
                    kwargs
                    | {
                        "research_output_ids": tuple(reversed(kwargs["research_output_ids"])),
                        "now": DECISION_TIME + timedelta(days=1),
                    }
                ),
            )
            assert replay.replayed and replay.run.id == first.run.id
            assert (
                await session.scalar(
                    select(func.count()).select_from(AgentPortfolioSynthesisRunRecord)
                )
                == 1
            )
            assert (
                await session.scalar(
                    select(func.count()).select_from(AgentPortfolioSynthesisBudgetReservationRecord)
                )
                == 1
            )
            job = await session.scalar(
                select(Job).where(Job.job_type == "agent.portfolio_synthesis")
            )
            assert job.priority == 180 and job.max_attempts == 1 and job.retry_policy == "never"
        for changed in (
            {"projection_hash": "a" * 64},
            {"research_output_ids": kwargs["research_output_ids"][:1]},
            {"active_target_version_id": uuid4()},
        ):
            async with sessions.begin() as session:
                with pytest.raises(ValueError, match="CONFLICT"):
                    await admit_portfolio_synthesis(session, **(kwargs | changed))
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["incomplete", "projection", "expired", "target"])
async def test_invalid_authority_creates_no_run_or_job(migrated_database_url, changed):
    engine, sessions, kwargs = await seed_synthesis(migrated_database_url)
    change = {
        "incomplete": {"research_output_ids": kwargs["research_output_ids"][:1]},
        "projection": {"projection_hash": "a" * 64},
        "expired": {"now": DECISION_TIME + timedelta(hours=1)},
        "target": {"active_target_version_id": uuid4()},
    }[changed]
    try:
        async with sessions.begin() as session:
            with pytest.raises(ValueError):
                await admit_portfolio_synthesis(session, **(kwargs | change))
            assert (
                await session.scalar(
                    select(func.count()).select_from(AgentPortfolioSynthesisRunRecord)
                )
                == 0
            )
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(Job)
                    .where(Job.job_type == "agent.portfolio_synthesis")
                )
                == 0
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_terminal_is_bound_settled_and_exactly_once(migrated_database_url):
    engine, sessions, kwargs = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            admission = await admit_portfolio_synthesis(session, **kwargs)
            run = admission.run
            intent = synthesis_intent(run)
            run.status = "running"
        terminal = dict(
            run_id=run.id,
            input_hash=run.input_hash,
            status="completed",
            intent=intent,
            input_tokens=10,
            output_tokens=20,
            now=kwargs["now"] + timedelta(minutes=1),
        )
        async with sessions.begin() as session:
            output = await persist_portfolio_synthesis_terminal(session, **terminal)
            assert output.status == "completed"
            reservation = await session.scalar(
                select(AgentPortfolioSynthesisBudgetReservationRecord)
            )
            assert reservation.state == "settled" and reservation.settled_usd == Decimal("0.00003")
            assert await _shared_daily_budget_used(
                session, business_date=kwargs["now"].date()
            ) == Decimal("0.00043")
        async with sessions.begin() as session:
            replay = await persist_portfolio_synthesis_terminal(session, **terminal)
            assert replay.id == output.id
            admitted = await admit_portfolio_synthesis(session, **kwargs)
            assert admitted.replayed and admitted.run.status == "completed"
            assert (
                await session.scalar(
                    select(func.count()).select_from(AgentPortfolioSynthesisOutputRecord)
                )
                == 1
            )
            with pytest.raises(ValueError, match="CONFLICT"):
                await persist_portfolio_synthesis_terminal(
                    session, **(terminal | {"output_tokens": 21})
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("pre_call", [True, False])
async def test_failure_releases_only_before_call_and_preserves_unknown_cost(
    migrated_database_url, pre_call
):
    engine, sessions, kwargs = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            run = (await admit_portfolio_synthesis(session, **kwargs)).run
            if not pre_call:
                run.status = "running"
                await session.flush()
            await persist_portfolio_synthesis_terminal(
                session,
                run_id=run.id,
                input_hash=run.input_hash,
                status="failed",
                intent=None,
                input_tokens=None,
                output_tokens=None,
                now=kwargs["now"],
            )
            reservation = await session.scalar(
                select(AgentPortfolioSynthesisBudgetReservationRecord)
            )
            assert reservation.state == ("released" if pre_call else "reserved")
            assert reservation.settled_usd == (Decimal("0") if pre_call else None)
            assert run.settled_cost_usd == (Decimal("0") if pre_call else None)
            assert await _shared_daily_budget_used(
                session, business_date=kwargs["now"].date()
            ) == Decimal("0.0004" if pre_call else "0.0006")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_shared_budget_includes_research_and_synthesis_in_both_directions(
    migrated_database_url,
):
    engine, sessions, kwargs = await seed_synthesis(migrated_database_url)
    try:
        # Two security reservations each 0.0002; synthesis worst case is another 0.0002.
        async with sessions.begin() as session:
            with pytest.raises(BudgetExhausted):
                await admit_portfolio_synthesis(
                    session, **(kwargs | {"daily_limit_usd": Decimal("0.0005")})
                )
            assert (
                await session.scalar(
                    select(func.count()).select_from(AgentPortfolioSynthesisRunRecord)
                )
                == 0
            )
        async with sessions.begin() as session:
            await admit_portfolio_synthesis(
                session, **(kwargs | {"daily_limit_usd": Decimal("0.0006")})
            )
            with pytest.raises(BudgetExhausted):
                await reserve_daily_budget(
                    session,
                    run_id=uuid4(),
                    now=kwargs["now"],
                    daily_limit_usd=Decimal("0.0006"),
                    worst_case_usd=Decimal("0.00001"),
                    pricing_version="test",
                    pricing_fresh=True,
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_concurrent_admission_returns_one_identity(migrated_database_url):
    engine, sessions, kwargs = await seed_synthesis(migrated_database_url)
    try:

        async def admit():
            async with sessions.begin() as session:
                result = await admit_portfolio_synthesis(session, **kwargs)
                return result.run.id, result.replayed

        results = await asyncio.gather(admit(), admit())
        assert results[0][0] == results[1][0]
        assert sorted(result[1] for result in results) == [False, True]
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_terminal_rejects_unbound_evidence_missing_usage_and_exposure_escalation(
    migrated_database_url,
):
    engine, sessions, kwargs = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            run = (await admit_portfolio_synthesis(session, **kwargs)).run
            run.status = "running"
        intent = synthesis_intent(run)
        terminal = dict(
            run_id=run.id,
            input_hash=run.input_hash,
            status="completed",
            intent=intent,
            input_tokens=10,
            output_tokens=20,
            now=kwargs["now"],
        )
        document = intent.model_dump(mode="json")
        document["choices"][0]["thesis"]["references"][0]["evidence_ref_id"] = str(uuid4())
        unbound = PortfolioSynthesisIntentV1.model_validate(document)
        escalated = intent.model_dump(mode="json")
        escalated["choices"][0].update(action="increase", rank=1, role="core")
        for change in (
            {"intent": unbound},
            {"input_tokens": None, "output_tokens": None},
            {"intent": PortfolioSynthesisIntentV1.model_validate(escalated)},
            {"input_tokens": 101},
            {"input_hash": "b" * 64},
        ):
            async with sessions.begin() as session:
                with pytest.raises(ValueError):
                    await persist_portfolio_synthesis_terminal(session, **(terminal | change))
                assert (
                    await session.scalar(
                        select(func.count()).select_from(AgentPortfolioSynthesisOutputRecord)
                    )
                    == 0
                )
                persisted = await session.get(AgentPortfolioSynthesisRunRecord, run.id)
                assert persisted.status == "running" and persisted.settled_cost_usd is None
        frozen = PortfolioSynthesisInputV1.model_validate(run.frozen_input)
        validate_portfolio_synthesis_intent(intent, frozen)
        changed = frozen.model_dump(mode="json")
        changed["projection"]["available_cash"] = "1"
        with pytest.raises(ValueError):
            PortfolioSynthesisInputV1.model_validate(changed)
        for field in (
            "candidate_batch_hash",
            "policy_hash",
            "prompt_hash",
            "model_hash",
            "tool_hash",
        ):
            altered = frozen.model_dump(mode="json")
            altered[field] = "b" * 64 if altered[field] != "b" * 64 else "c" * 64
            assert (
                canonical_portfolio_synthesis_hash(
                    PortfolioSynthesisInputV1.model_validate(altered)
                )
                != run.input_hash
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_active_target_input_preserves_frozen_full_allocation(migrated_database_url):
    from nexus.persistence.models.paper import PaperTargetVersionRecord, StrategyDecisionRecord

    engine, sessions, kwargs = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            decision = await session.scalar(select(StrategyDecisionRecord))
            target = PaperTargetVersionRecord(
                account_id=decision.account_id,
                portfolio_id=decision.portfolio_id,
                version_seq=1,
                strategy_decision_id=decision.id,
                previous_target_version_id=None,
                target_content_hash="a" * 64,
                activation_hash="b" * 64,
                activated_at=DECISION_TIME,
            )
            session.add(target)
            await session.flush()
            run = (
                await admit_portfolio_synthesis(
                    session, **(kwargs | {"active_target_version_id": target.id})
                )
            ).run
            frozen = PortfolioSynthesisInputV1.model_validate(run.frozen_input)
            assert frozen.active_target_cash_weight == decision.cash_weight
            assert sum(
                (item.target_weight for item in frozen.active_target_weights), Decimal("0")
            ) + frozen.active_target_cash_weight == Decimal("1")
            changed = frozen.model_dump(mode="json")
            changed["active_target_cash_weight"] = "1"
            with pytest.raises(ValueError):
                PortfolioSynthesisInputV1.model_validate(changed)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_rejects_security_run_not_admitted_for_exact_cycle(migrated_database_url):
    engine, sessions, kwargs = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            output = await session.get(AgentResearchOutputRecord, kwargs["research_output_ids"][0])
            await session.execute(text("ALTER TABLE agent_research_runs DISABLE TRIGGER USER"))
            await session.execute(
                text("UPDATE agent_research_runs SET idempotency_key=:key WHERE id=:id"),
                {"key": "another-cycle", "id": output.run_id},
            )
            await session.execute(text("ALTER TABLE agent_research_runs ENABLE TRIGGER USER"))
        async with sessions.begin() as session:
            with pytest.raises(ValueError, match="RESEARCH_INVALID"):
                await admit_portfolio_synthesis(session, **kwargs)
            assert (
                await session.scalar(
                    select(func.count()).select_from(AgentPortfolioSynthesisRunRecord)
                )
                == 0
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_concurrent_security_and_synthesis_share_one_remaining_budget(migrated_database_url):
    engine, sessions, kwargs = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            source = await session.scalar(select(AgentResearchRunRecord).limit(1))
            values = {
                column.name: getattr(source, column.name)
                for column in AgentResearchRunRecord.__table__.columns
                if column.name not in {"id", "created_at"}
            }
            extra_id = uuid4()
            values.update(id=extra_id, idempotency_key="extra-research", status="prepared")
            session.add(AgentResearchRunRecord(**values))

        async def synthesis():
            async with sessions.begin() as session:
                await admit_portfolio_synthesis(
                    session, **(kwargs | {"daily_limit_usd": Decimal("0.0006")})
                )

        async def research():
            async with sessions.begin() as session:
                await reserve_daily_budget(
                    session,
                    run_id=extra_id,
                    now=kwargs["now"],
                    daily_limit_usd=Decimal("0.0006"),
                    worst_case_usd=Decimal("0.0002"),
                    pricing_version=source.pricing_version,
                    pricing_fresh=True,
                )

        outcomes = await asyncio.gather(synthesis(), research(), return_exceptions=True)
        assert sum(isinstance(item, BudgetExhausted) for item in outcomes) == 1
        assert sum(item is None for item in outcomes) == 1
        async with sessions.begin() as session:
            assert await _shared_daily_budget_used(
                session, business_date=kwargs["now"].date()
            ) == Decimal("0.0006")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_terminal_rejects_run_columns_disagreeing_with_frozen_input(migrated_database_url):
    engine, sessions, kwargs = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            run = (await admit_portfolio_synthesis(session, **kwargs)).run
            run.status = "running"
        async with sessions.begin() as session:
            await session.execute(
                text("ALTER TABLE agent_portfolio_synthesis_runs DISABLE TRIGGER USER")
            )
            await session.execute(
                text("UPDATE agent_portfolio_synthesis_runs SET prompt_hash=:hash WHERE id=:id"),
                {"hash": "d" * 64, "id": run.id},
            )
            await session.execute(
                text("ALTER TABLE agent_portfolio_synthesis_runs ENABLE TRIGGER USER")
            )
        async with sessions.begin() as session:
            with pytest.raises(ValueError, match="INPUT_CONFLICT"):
                await persist_portfolio_synthesis_terminal(
                    session,
                    run_id=run.id,
                    input_hash=run.input_hash,
                    status="abstained",
                    intent=None,
                    input_tokens=10,
                    output_tokens=20,
                    now=kwargs["now"],
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_abstention_is_terminal_and_settles_known_usage(migrated_database_url):
    engine, sessions, kwargs = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            run = (await admit_portfolio_synthesis(session, **kwargs)).run
            run.status = "running"
        terminal = dict(
            run_id=run.id,
            input_hash=run.input_hash,
            status="abstained",
            intent=None,
            input_tokens=10,
            output_tokens=20,
            now=kwargs["now"],
        )
        async with sessions.begin() as session:
            output = await persist_portfolio_synthesis_terminal(session, **terminal)
            assert output.intent is None and output.status == "abstained"
        async with sessions.begin() as session:
            assert (await persist_portfolio_synthesis_terminal(session, **terminal)).id == output.id
            reservation = await session.scalar(
                select(AgentPortfolioSynthesisBudgetReservationRecord)
            )
            assert reservation.state == "settled" and reservation.settled_usd == Decimal("0.00003")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_comparison_reductions_are_bounded_by_research_and_frozen_target(
    migrated_database_url,
):
    engine, sessions, kwargs = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            run = (await admit_portfolio_synthesis(session, **kwargs)).run
        for stance, action, existing, complete, allowed in (
            ("hold", "exit", True, True, True),
            ("hold", "exit", False, True, False),
            ("buy_candidate", "exit", True, True, False),
            ("watch", "maintain", True, True, False),
            ("hold", "maintain", True, False, False),
            ("hold", "maintain", False, True, False),
            ("hold", "maintain", True, True, True),
            ("buy_candidate", "include", False, True, True),
            ("buy_candidate", "increase", True, True, True),
        ):
            document = PortfolioSynthesisInputV1.model_validate(run.frozen_input).model_dump(
                mode="json"
            )
            research = document["security_outputs"][0]["intent"]
            research["stance"] = stance
            research["exposure_action"] = {
                "hold": "maintain",
                "buy_candidate": "eligible_for_increase",
                "watch": "none",
            }[stance]
            research["synthesis_assessment"]["stance"] = stance
            if complete:
                research["quality_status"] = "complete"
                research["evidence_audit"] = "complete"
                research["synthesis_assessment"]["review_conclusion"] = "sufficient"
                for review in research["synthesis_assessment"]["reviews"]:
                    review.update(
                        status="accepted",
                        resolved=True,
                        missing_evidence=[],
                        prevented_conclusion=None,
                    )
                for vote in research["role_votes"]:
                    vote["vote"] = "support"
            if existing:
                document.update(
                    active_target_version_id=str(uuid4()),
                    active_target_content_hash="a" * 64,
                    active_target_instrument_ids=[document["candidate_instrument_ids"][0]],
                    active_target_weights=[
                        {
                            "instrument_id": document["candidate_instrument_ids"][0],
                            "target_weight": "0.1",
                        }
                    ],
                    active_target_cash_weight="0.9",
                )
            frozen = PortfolioSynthesisInputV1.model_validate(document)
            intent_document = synthesis_intent(run).model_dump(mode="json")
            intent_document["input_hash"] = canonical_portfolio_synthesis_hash(frozen)
            intent_document["choices"][0].update(
                action=action,
                role="core" if action in {"include", "increase"} else "none",
                rank=1 if action in {"include", "increase"} else None,
            )
            intent = PortfolioSynthesisIntentV1.model_validate(intent_document)
            if allowed:
                validate_portfolio_synthesis_intent(intent, frozen)
            else:
                with pytest.raises(ValueError):
                    validate_portfolio_synthesis_intent(intent, frozen)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_synthesis_prompt_does_not_replace_security_committee_portfolio_role(
    migrated_database_url,
):
    engine, sessions, kwargs = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            prompts = await load_complete_prompt_authority(session)
            original = await session.scalar(
                select(AgentPromptVersionRecord).where(
                    AgentPromptVersionRecord.role == "portfolio",
                    AgentPromptVersionRecord.version == "agent-paper-portfolio-v1",
                )
            )
            assert prompts is not None and prompts["portfolio"].id == original.id
            content = "Existing read-only portfolio decision prompt."
            old = AgentPromptVersionRecord(
                role="portfolio_decision",
                version="portfolio-decision-v1",
                content=content,
                content_hash=sha256(content.encode()).hexdigest(),
                created_at=DECISION_TIME,
            )
            session.add(old)
            await session.flush()
            from nexus.agents.portfolio_decision_persistence import _load_prompt

            assert (
                await _load_prompt(session, prompt_version_id=None, now=kwargs["now"])
            ).id == old.id
            run = (await admit_portfolio_synthesis(session, **kwargs)).run
            assert run.prompt_version_id != old.id
            selected = await session.get(AgentPromptVersionRecord, run.prompt_version_id)
            assert (
                selected.role == "portfolio_decision"
                and selected.version == "portfolio-management-synthesis-v1"
            )
    finally:
        await engine.dispose()
