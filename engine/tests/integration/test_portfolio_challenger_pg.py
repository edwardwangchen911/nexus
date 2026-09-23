"""Frozen multi-candidate cycle samples for Agent challenger retrospectives."""

import json
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest
from helpers.paper_batch import INSTRUMENTS
from integration.test_portfolio_proposal_pg import proposal_source
from nexus.agents.portfolio_synthesis_contracts import SYNTHESIS_PROMPT_VERSION
from nexus.agents.responses_budget import UsageSnapshot
from nexus.ops.portfolio_challenger import (
    ProposeRequest,
    PublishRequest,
    execute_propose,
    execute_publish,
)
from nexus.paper import agent_challenger
from nexus.paper import agent_challenger_runtime as challenger_runtime
from nexus.paper.agent_challenger import (
    AgentChallengerError,
    PortfolioChallengerCycleSampleV1,
    PortfolioChallengerRetrospectiveV1,
)
from nexus.paper.agent_cycle import _events, _resume_portfolio_synthesis
from nexus.persistence.models.agents import AgentPromptVersionRecord
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import AuditEvent
from nexus.settings import EngineSettings
from sqlalchemy import func, select


@pytest.mark.asyncio
async def test_completed_portfolio_cycle_yields_all_candidate_evidence(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Dropping a candidate or synthesis output would corrupt the improvement sample."""
    engine, sessions, args = await proposal_source(migrated_database_url)
    try:
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            assert cycle is not None
            result = await _resume_portfolio_synthesis(
                session,
                cycle=cycle,
                events=await _events(session, cycle.id),
                run_ids=(),
                now=args["now"] + timedelta(seconds=1),
                daily_limit_usd=None,
            )
            assert result.status == "completed"
            assert result.result_code == "NO_ORDER_REQUIRED"

        collector = getattr(agent_challenger, "collect_portfolio_challenger_cycle_sample", None)
        assert callable(collector), "multi-candidate completed cycle collector is required"
        async with sessions() as session:
            sample = await collector(
                session, cycle_id=cycle.id, as_of=args["now"] + timedelta(minutes=2)
            )
        assert sample.cycle_id == cycle.id
        assert sample.candidate_batch_id == cycle.candidate_batch_id
        assert sample.candidate_instrument_ids == tuple(sorted(item[0] for item in INSTRUMENTS[:2]))
        assert len(sample.research_run_ids) == 2
        assert sample.synthesis_output_id == args["synthesis_output_id"]
        assert sample.result_code == "NO_ORDER_REQUIRED"
        assert {item.instrument_id for item in sample.candidate_facts} == set(
            sample.candidate_instrument_ids
        )
        assert {item.choice.instrument_id for item in sample.candidate_facts} == set(
            sample.candidate_instrument_ids
        )
        assert sample.synthesis_intent.compared_instrument_ids == sample.candidate_instrument_ids
        assert len(sample.source_hash) == 64
        async with sessions() as session:
            retrospective = await agent_challenger.collect_portfolio_challenger_retrospective(
                session,
                account_id=cycle.account_id,
                as_of=args["now"] + timedelta(minutes=2),
            )
        assert tuple(item.cycle_id for item in retrospective.cycles) == (cycle.id,)
        assert retrospective.incumbent_prompt_version == SYNTHESIS_PROMPT_VERSION
        assert retrospective.incumbent_prompt_hash == sample.synthesis_prompt_hash
        assert retrospective.incumbent_prompt_version_id == sample.synthesis_prompt_version_id
        assert len(retrospective.source_hash) == 64
        suggestion = agent_challenger.PortfolioChallengerSuggestionV1(
            source_hash=retrospective.source_hash,
            observed_pattern="The frozen portfolio cycles deferred both candidates.",
            hypothesis="Make each deferral condition explicit before ranking candidates.",
            target_role="portfolio_decision",
            prompt_addendum=(
                "For each deferred candidate, state the exact evidence gap and its effect."
            ),
            expected_behavior_changes=("More specific portfolio deferral reasons",),
            invalidation_conditions=("No change in deferral diagnosis",),
            execution_locked=True,
            deterministic_risk_unchanged=True,
            broker_writes_forbidden=True,
            output_schema_unchanged=True,
            human_review_required=True,
        )
        proposal = agent_challenger.freeze_portfolio_challenger_proposal_v1(
            retrospective, suggestion
        )
        assert proposal.target_role == "portfolio_decision"
        assert proposal.source_cycle_ids == (cycle.id,)
        assert proposal.incumbent_prompt_hash == retrospective.incumbent_prompt_hash
        assert proposal.performance_attribution == "not_established"
        assert proposal.human_review_required is True
        projected = challenger_runtime.compact_portfolio_challenger_retrospective(retrospective)
        assert len(projected["cycles"]) == 1
        assert len(projected["cycles"][0]["candidates"]) == 2
        model = SimpleNamespace(
            id=retrospective.incumbent_model_config_id,
            config_hash=retrospective.incumbent_model_hash,
            model_id=challenger_runtime.MODEL_ID,
            max_input_tokens=96000,
            max_output_tokens=8192,
            input_usd_per_million=Decimal(0),
            output_usd_per_million=Decimal(0),
        )
        calls = []

        async def fake_invoke(payload, *, output_model):
            calls.append(payload)
            assert output_model is challenger_runtime.PortfolioChallengerSuggestionWireV1
            return (
                json.dumps(
                    suggestion.model_dump(
                        mode="json",
                        exclude={
                            "execution_locked",
                            "deterministic_risk_unchanged",
                            "broker_writes_forbidden",
                            "output_schema_unchanged",
                            "human_review_required",
                        },
                    )
                ),
                UsageSnapshot(input_tokens=100, output_tokens=50, provider_calls=1),
            )

        monkeypatch.setattr(challenger_runtime, "invoke_codex", fake_invoke)
        runtime = challenger_runtime.CodexPortfolioChallengerRuntime(model)  # type: ignore[arg-type]
        generated, usage = await runtime.generate(retrospective)
        assert generated == suggestion
        assert usage.provider_calls == 1
        assert len(calls) == 1
        with pytest.raises(ValueError, match="PORTFOLIO_CHALLENGER_REPLAY_FORBIDDEN"):
            await runtime.generate(retrospective)
        altered_history = retrospective.model_dump(mode="json")
        altered_history["as_of"] = (
            (args["now"] + timedelta(minutes=3)).isoformat().replace("+00:00", "Z")
        )
        with pytest.raises(ValueError, match="PORTFOLIO_CHALLENGER_RETROSPECTIVE_INVALID"):
            PortfolioChallengerRetrospectiveV1.model_validate(altered_history)
        tampered = sample.model_dump(mode="json")
        tampered["result_code"] = "PAPER_ORDER_FILLED"
        with pytest.raises(ValueError, match="PORTFOLIO_CHALLENGER_SAMPLE_INCOMPLETE"):
            PortfolioChallengerCycleSampleV1.model_validate(tampered)
        assert sample.order_ids == ()
        assert sample.fill_ids == ()
        assert sample.fill_ledger_transaction_ids == ()
        assert sample.fill_facts == ()
        assert sample.performance_state == "missing"
        assert sample.account_performance is None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_unfinished_portfolio_cycle_is_not_an_improvement_sample(
    migrated_database_url: str,
) -> None:
    engine, sessions, args = await proposal_source(migrated_database_url)
    try:
        with pytest.raises(AgentChallengerError, match="PORTFOLIO_CHALLENGER_SAMPLE_INCOMPLETE"):
            async with sessions() as session:
                await agent_challenger.collect_portfolio_challenger_cycle_sample(
                    session, cycle_id=args["cycle_id"], as_of=args["now"] + timedelta(minutes=2)
                )
        with pytest.raises(AgentChallengerError, match="PORTFOLIO_CHALLENGER_HISTORY_INSUFFICIENT"):
            async with sessions() as session:
                cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
                assert cycle is not None
                await agent_challenger.collect_portfolio_challenger_retrospective(
                    session, account_id=cycle.account_id, as_of=args["now"] + timedelta(minutes=2)
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_portfolio_proposal_job_is_persisted_and_replayed_once(
    migrated_database_url: str,
) -> None:
    engine, sessions, args = await proposal_source(migrated_database_url)
    try:
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            assert cycle is not None
            result = await _resume_portfolio_synthesis(
                session,
                cycle=cycle,
                events=await _events(session, cycle.id),
                run_ids=(),
                now=args["now"] + timedelta(seconds=1),
                daily_limit_usd=None,
            )
            assert result.status == "completed"

        class FakeRuntime:
            calls = 0

            async def generate(self, source):
                self.calls += 1
                suggestion = agent_challenger.PortfolioChallengerSuggestionV1(
                    source_hash=source.source_hash,
                    observed_pattern="Two candidates were deferred.",
                    hypothesis="Explain evidence gaps separately for each candidate.",
                    target_role="portfolio_decision",
                    prompt_addendum=(
                        "Name each candidate's unresolved evidence before deferring it."
                    ),
                    expected_behavior_changes=("More precise deferral reasons",),
                    invalidation_conditions=("No change in deferral reasons",),
                    execution_locked=True,
                    deterministic_risk_unchanged=True,
                    broker_writes_forbidden=True,
                    output_schema_unchanged=True,
                    human_review_required=True,
                )
                return suggestion, UsageSnapshot(
                    input_tokens=100, output_tokens=50, provider_calls=1
                )

        runtime = FakeRuntime()
        settings = EngineSettings(
            database_url=migrated_database_url, agent_runtime_mode="codex_sdk"
        )
        request = ProposeRequest(
            action="propose",
            account_id=cycle.account_id,
            history_limit=1,
            idempotency_key="portfolio-challenger-first-cycle",
        )
        first = await execute_propose(settings, request, runtime=runtime)
        replay = await execute_propose(settings, request, runtime=runtime)
        assert first == replay
        assert first.proposal.source_cycle_ids == (cycle.id,)
        assert first.proposal.target_role == "portfolio_decision"
        assert runtime.calls == 1
        async with sessions() as session:
            count = await session.scalar(
                select(func.count(AuditEvent.id)).where(
                    AuditEvent.action == "portfolio.challenger.proposed"
                )
            )
        assert count == 1

        publication_request = PublishRequest(
            action="publish", proposal_hash=first.proposal.proposal_hash
        )
        publication = await execute_publish(settings, publication_request)
        publication_replay = await execute_publish(settings, publication_request)
        assert publication_replay.model_copy(update={"replayed": False}) == publication
        assert publication_replay.replayed is True
        assert publication.challenger_model_config_id == first.proposal.incumbent_model_config_id
        async with sessions() as session:
            prompt = await session.get(
                AgentPromptVersionRecord, publication.challenger_prompt_version_id
            )
            assert prompt is not None
            assert prompt.role == "portfolio_decision"
            assert prompt.release_channel == "challenger"
            assert first.proposal.prompt_addendum in prompt.content
            assert prompt.content_hash == publication.challenger_prompt_hash
            incumbent = await session.get(
                AgentPromptVersionRecord, first.proposal.incumbent_prompt_version_id
            )
            assert incumbent is not None
            assert incumbent.release_channel == "active"
            publication_count = await session.scalar(
                select(func.count(AuditEvent.id)).where(
                    AuditEvent.action == "portfolio.challenger.published"
                )
            )
            assert publication_count == 1
        with pytest.raises(AgentChallengerError, match="PORTFOLIO_CHALLENGER_PROPOSAL_UNKNOWN"):
            await execute_publish(
                settings, PublishRequest(action="publish", proposal_hash="0" * 64)
            )

        class FailingRuntime:
            calls = 0

            async def generate(self, source):
                self.calls += 1
                raise ValueError("CODEX_TIMEOUT")

        failed_runtime = FailingRuntime()
        failed_request = request.model_copy(
            update={"idempotency_key": "portfolio-challenger-timeout"}
        )
        for _ in range(2):
            with pytest.raises(AgentChallengerError, match="PORTFOLIO_CHALLENGER_RESULT_UNKNOWN"):
                await execute_propose(settings, failed_request, runtime=failed_runtime)
        assert failed_runtime.calls == 1
        async with sessions() as session:
            count_after_failure = await session.scalar(
                select(func.count(AuditEvent.id)).where(
                    AuditEvent.action == "portfolio.challenger.proposed"
                )
            )
        assert count_after_failure == 1
    finally:
        await engine.dispose()
