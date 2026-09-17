"""Real-PostgreSQL admission, budget, and evidence-authority tests for Agents."""

import asyncio
import hashlib
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.agents.budget import BudgetExhausted
from nexus.agents.committee import ROLE_ORDER, load_prompt_assets
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import (
    admit_research_run,
    load_complete_prompt_authority,
    release_daily_budget,
    reserve_daily_budget,
    settle_daily_budget,
)
from nexus.agents.responses_budget import UsageSnapshot
from nexus.audit.service import append_audit_event
from nexus.domain.evidence import EvidenceRef
from nexus.jobs.models import JobKeyV1, JobType, QueueName, RetryPolicy, job_input_hash
from nexus.jobs.repository import claim_next_job, enqueue_job, finalize_job
from nexus.ops.agent_challenger import ProposeRequest, execute_propose
from nexus.paper.agent_challenger import (
    AGENT_CHALLENGER_PROPOSAL_ACTION,
    PROPOSER_PROMPT_HASH,
    PROPOSER_PROMPT_VERSION,
    PROVIDER_PROJECTION_VERSION,
    AgentChallengerCycleFactV4,
    AgentChallengerError,
    AgentChallengerJobOutputV4,
    AgentChallengerReviewFactV4,
    AgentChallengerSuggestionV4,
    freeze_agent_challenger_proposal_v4,
    freeze_agent_challenger_retrospective_v4,
    publish_agent_challenger,
)
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentDataSnapshotEvidenceRecord,
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchOutputEvidenceRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.persistence.models.runtime import IdempotencyRecord, Job
from nexus.persistence.session import session_factory
from nexus.reconciliation.persistence_errors import IdempotencyConflict
from nexus.settings import EngineSettings
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError

_NOW = datetime(2026, 8, 24, 12, tzinfo=UTC)
_INSTRUMENT_ID = UUID("0198d500-1000-7000-8000-000000001501")
_SNAPSHOT_ID = UUID("0198d500-1000-7000-8000-000000001502")
_PROMPT_ID = UUID("0198d500-1000-7000-8000-000000001503")
_MODEL_ID = UUID("0198d500-1000-7000-8000-000000001504")
_EVIDENCE_ID = UUID("0198d500-1000-7000-8000-000000001505")
_PORTFOLIO_MODEL_ID = UUID("0198d500-1000-7000-8000-000000001599")
_RUN_IDS = tuple(UUID(f"0198d500-1000-7000-8000-0000000015{index:02d}") for index in range(10, 20))


def _challenger_reviews() -> tuple[AgentChallengerReviewFactV4, ...]:
    evidence = EvidenceRef(
        source="test",
        object_id="challenger-evidence",
        available_at=_NOW,
        location="fixture",
        content_hash="e" * 64,
    )
    return (
        AgentChallengerReviewFactV4(
            claim_id="bear:0",
            status="unresolved",
            rationale="The claim lacks current catalyst evidence.",
            evidence_refs=(evidence,),
            resolved=False,
            missing_evidence=("Current catalyst evidence",),
            prevented_conclusion="Cannot distinguish watch from a buy candidate.",
        ),
        AgentChallengerReviewFactV4(
            claim_id="evidence_audit:0",
            status="unresolved",
            rationale="The audit found the same gap for its own claim.",
            evidence_refs=(evidence,),
            resolved=False,
            missing_evidence=("Current catalyst evidence",),
            prevented_conclusion="Cannot mark the evidence audit complete.",
        ),
    )


def _challenger_retrospective_fixture():  # type: ignore[no-untyped-def]
    prompt_assets = load_prompt_assets()
    facts = tuple(
        AgentChallengerCycleFactV4(
            cycle_id=UUID(f"0198d500-1000-7000-8000-{9600 + index:012d}"),
            decision_slot_at=_NOW + timedelta(minutes=index),
            result_code="NO_ORDER_REQUIRED",
            strategy_decision_id=UUID(f"0198d500-1000-7000-8000-{9700 + index:012d}"),
            decision_hash=f"{index + 1:064x}",
            sizing_actions=("none",),
            research_run_ids=(UUID(f"0198d500-1000-7000-8000-{9800 + index:012d}"),),
            settled_cost_usd="0",
            stance="watch",
            exposure_action="none",
            quality_status="partial",
            evidence_audit="partial",
            uncertainties=("Catalyst evidence is missing.",),
            disagreement="The evidence remains unresolved.",
            role_votes=("portfolio:abstain",),
            reviews=_challenger_reviews(),
            invalidation_signals=(),
            entry_signals=(),
            exit_signals=(),
        )
        for index in range(4)
    )
    manifest = {
        role: {
            "id": str(
                _PROMPT_ID
                if role == "orchestrator"
                else UUID(f"0198d500-1000-7000-8000-0000000016{20 + index:02d}")
            ),
            "version": prompt_assets[role].version,
            "content_hash": prompt_assets[role].content_hash,
        }
        for index, role in enumerate(ROLE_ORDER)
    }
    prompt_hash = hashlib.sha256(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return freeze_agent_challenger_retrospective_v4(
        instrument_id=_INSTRUMENT_ID,
        as_of=_NOW + timedelta(hours=1),
        incumbent_prompt_manifest=manifest,
        incumbent_prompt_hash=prompt_hash,
        incumbent_portfolio_prompt=prompt_assets["portfolio"].content,
        output_contract={"type": "object", "properties": {"uncertainties": {"type": "array"}}},
        incumbent_model_config_id=_MODEL_ID,
        incumbent_model_hash="2" * 64,
        cycles=facts,
    )


def _challenger_suggestion(retrospective):  # type: ignore[no-untyped-def]
    return AgentChallengerSuggestionV4(
        source_hash=retrospective.source_hash,
        observed_pattern="Four completed cycles repeated watch/none.",
        hypothesis="Make the missing evidence and reversal condition explicit.",
        target_role="portfolio",
        prompt_addendum=(
            "For repeated watch decisions, name the missing evidence and the exact "
            "observation that would change the sizing action using existing fields."
        ),
        expected_behavior_changes=("Specific reversal conditions",),
        invalidation_conditions=("No output difference",),
        execution_locked=True,
        fixed_aapl_scope=True,
        deterministic_risk_unchanged=True,
        broker_writes_forbidden=True,
        output_schema_unchanged=True,
    )


@pytest.mark.asyncio
async def test_challenger_prompt_is_explicit_only_authority(
    migrated_database_url: str,
) -> None:
    """A published challenger must not silently replace the normal active prompt."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    challenger_id = UUID("0198d500-1000-7000-8000-000000001999")
    incomplete_id = UUID("0198d500-1000-7000-8000-000000001998")
    corrupt_id = UUID("0198d500-1000-7000-8000-000000001997")
    prompts = load_prompt_assets()
    async with factory() as session, session.begin():
        session.add_all(
            AgentPromptVersionRecord(
                id=(
                    challenger_id
                    if role == "orchestrator"
                    else UUID(f"0198d500-1000-7000-8000-0000000018{index:02d}")
                ),
                role=role,
                version="challenger-bundle-v1",
                release_channel="challenger",
                content_hash=hashlib.sha256(
                    f"{prompts[role].content}\nbundle".encode()
                ).hexdigest(),
                content=f"{prompts[role].content}\nbundle",
            )
            for index, role in enumerate(ROLE_ORDER)
        )
        auxiliary_content = "Auxiliary strategy research prompt."
        session.add(
            AgentPromptVersionRecord(
                id=UUID("0198d500-1000-7000-8000-000000002099"),
                role="strategy_rnd",
                version=prompts["orchestrator"].version,
                release_channel="active",
                content_hash=hashlib.sha256(auxiliary_content.encode()).hexdigest(),
                content=auxiliary_content,
            )
        )
        session.add(
            AgentPromptVersionRecord(
                id=incomplete_id,
                role="orchestrator",
                version="challenger-incomplete-v1",
                release_channel="challenger",
                content_hash=prompts["orchestrator"].content_hash,
                content=prompts["orchestrator"].content,
            )
        )
        session.add_all(
            AgentPromptVersionRecord(
                id=(
                    corrupt_id
                    if role == "orchestrator"
                    else UUID(f"0198d500-1000-7000-8000-0000000019{80 + index:02d}")
                ),
                role=role,
                version="challenger-corrupt-v1",
                release_channel="challenger",
                content_hash=("f" * 64 if role == "portfolio" else prompts[role].content_hash),
                content=prompts[role].content,
            )
            for index, role in enumerate(ROLE_ORDER)
        )

    async with factory() as session:
        active = await load_complete_prompt_authority(session)
        challenger = await load_complete_prompt_authority(session, orchestrator_id=challenger_id)
        incomplete = await load_complete_prompt_authority(session, orchestrator_id=incomplete_id)
        corrupt = await load_complete_prompt_authority(session, orchestrator_id=corrupt_id)

    assert active is not None
    assert active["orchestrator"].id == _PROMPT_ID
    assert challenger is not None
    assert challenger["orchestrator"].id == challenger_id
    assert all(challenger[role].release_channel == "challenger" for role in ROLE_ORDER)
    assert len({challenger[role].version for role in ROLE_ORDER}) == 1
    assert incomplete is None
    assert corrupt is None


@pytest.mark.asyncio
async def test_explicit_active_prompt_loads_its_complete_version_without_mixing(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    prompts = load_prompt_assets()
    async with factory() as session, session.begin():
        session.add_all(
            AgentPromptVersionRecord(
                id=UUID(f"0198d500-1000-7000-8000-0000000020{index:02d}"),
                role=role,
                version="new-active-v2",
                release_channel="active",
                content_hash=hashlib.sha256(
                    f"{prompts[role].content}\nnew active".encode()
                ).hexdigest(),
                content=f"{prompts[role].content}\nnew active",
            )
            for index, role in enumerate(ROLE_ORDER)
        )

    async with factory() as session:
        explicit_old = await load_complete_prompt_authority(session, orchestrator_id=_PROMPT_ID)

    assert explicit_old is not None
    assert explicit_old["orchestrator"].id == _PROMPT_ID
    assert {item.version for item in explicit_old.values()} == {prompts["orchestrator"].version}


@pytest.mark.asyncio
async def test_publish_challenger_requires_exact_proposal_and_does_not_activate_it(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    prompt_assets = load_prompt_assets()
    facts = tuple(
        AgentChallengerCycleFactV4(
            cycle_id=UUID(f"0198d500-1000-7000-8000-{9600 + index:012d}"),
            decision_slot_at=_NOW + timedelta(minutes=index),
            result_code="NO_ORDER_REQUIRED",
            strategy_decision_id=UUID(f"0198d500-1000-7000-8000-{9700 + index:012d}"),
            decision_hash=f"{index + 1:064x}",
            sizing_actions=("none",),
            research_run_ids=(UUID(f"0198d500-1000-7000-8000-{9800 + index:012d}"),),
            settled_cost_usd="0",
            stance="watch",
            exposure_action="none",
            quality_status="partial",
            evidence_audit="partial",
            uncertainties=("Catalyst evidence is missing.",),
            disagreement="The evidence remains unresolved.",
            role_votes=("portfolio:abstain",),
            reviews=_challenger_reviews(),
            invalidation_signals=(),
            entry_signals=(),
            exit_signals=(),
        )
        for index in range(4)
    )
    incumbent_manifest = {
        role: {
            "id": str(
                _PROMPT_ID
                if role == "orchestrator"
                else UUID(f"0198d500-1000-7000-8000-0000000016{20 + index:02d}")
            ),
            "version": prompt_assets[role].version,
            "content_hash": prompt_assets[role].content_hash,
        }
        for index, role in enumerate(ROLE_ORDER)
    }
    incumbent_prompt_hash = hashlib.sha256(
        json.dumps(incumbent_manifest, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    retrospective = freeze_agent_challenger_retrospective_v4(
        instrument_id=_INSTRUMENT_ID,
        as_of=_NOW + timedelta(hours=1),
        incumbent_prompt_manifest=incumbent_manifest,
        incumbent_prompt_hash=incumbent_prompt_hash,
        incumbent_portfolio_prompt=prompt_assets["portfolio"].content,
        output_contract={"type": "object", "properties": {"uncertainties": {"type": "array"}}},
        incumbent_model_config_id=_MODEL_ID,
        incumbent_model_hash="2" * 64,
        cycles=facts,
    )
    proposal = freeze_agent_challenger_proposal_v4(
        retrospective,
        AgentChallengerSuggestionV4(
            source_hash=retrospective.source_hash,
            observed_pattern="Four completed cycles repeated watch/none.",
            hypothesis="Make the missing evidence and reversal condition explicit.",
            target_role="portfolio",
            prompt_addendum=(
                "For repeated watch decisions, name the missing evidence and the exact "
                "observation that would change the sizing action using existing fields."
            ),
            expected_behavior_changes=("Specific reversal conditions",),
            invalidation_conditions=("No output difference",),
            execution_locked=True,
            fixed_aapl_scope=True,
            deterministic_risk_unchanged=True,
            broker_writes_forbidden=True,
            output_schema_unchanged=True,
        ),
    )
    job_payload = {
        "schema_version": "agent_challenger_job_v4",
        "idempotency_key": "challenger-publish-test",
        "request_hash": job_input_hash(
            {
                "schema_version": "agent_challenger_request_v4",
                "instrument_id": str(_INSTRUMENT_ID),
                "history_limit": 4,
            }
        ),
        "history_limit": 4,
        "retrospective": retrospective.model_dump(mode="json"),
        "proposer_prompt_version": PROPOSER_PROMPT_VERSION,
        "proposer_prompt_hash": PROPOSER_PROMPT_HASH,
        "provider_projection_version": PROVIDER_PROJECTION_VERSION,
    }
    async with factory() as session, session.begin():
        job = await enqueue_job(
            session,
            key=JobKeyV1(
                job_type=JobType.AGENT_CHALLENGER_PROPOSE,
                scope_kind="instrument",
                scope_id=str(_INSTRUMENT_ID),
                effective_at=_NOW + timedelta(minutes=3),
                input_hash=job_input_hash(job_payload),
            ),
            schema_version="agent_challenger_job_v4",
            payload=job_payload,
            queue=QueueName.GENERAL,
            retry_policy=RetryPolicy.NEVER,
            max_attempts=1,
        )
    async with factory() as session, session.begin():
        claim = await claim_next_job(
            session,
            queue=QueueName.GENERAL,
            worker_id="challenger-test-worker",
            now=_NOW + timedelta(hours=1),
            job_ids=(job.id,),
            job_types=(JobType.AGENT_CHALLENGER_PROPOSE.value,),
        )
        assert claim is not None
    output = AgentChallengerJobOutputV4(
        schema_version="agent_challenger_job_output_v4",
        job_id=job.id,
        proposer_prompt_version=PROPOSER_PROMPT_VERSION,
        proposer_prompt_hash=PROPOSER_PROMPT_HASH,
        proposal=proposal,
        input_tokens=10,
        output_tokens=10,
        provider_calls=1,
        tool_calls=0,
    )

    async def persist_output(session) -> None:  # type: ignore[no-untyped-def]
        await append_audit_event(
            session,
            actor_id="agent-challenger-test",
            action=AGENT_CHALLENGER_PROPOSAL_ACTION,
            schema_version=output.schema_version,
            payload=output.model_dump(mode="json"),
            occurred_at=_NOW + timedelta(hours=1, seconds=1),
        )

    async with factory() as session, session.begin():
        await finalize_job(
            session,
            claim=claim,
            now=_NOW + timedelta(hours=1, seconds=1),
            outcome="succeeded",
            result_code="AGENT_CHALLENGER_PROPOSED",
            apply=persist_output,
        )

    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="AGENT_CHALLENGER_PROPOSAL_UNKNOWN"):
            await publish_agent_challenger(
                session,
                proposal_hash="f" * 64,
                actor_id="human-local",
                now=_NOW + timedelta(hours=2),
            )

    protected_tables = (
        "agent_version_evaluations",
        "paper_orders",
        "broker_commands",
        "jobs",
    )
    async with factory() as session:
        before_counts = {
            table: await session.scalar(text(f"SELECT count(*) FROM {table}"))  # noqa: S608
            for table in protected_tables
        }

    async def concurrent_publish(actor_id: str):  # type: ignore[no-untyped-def]
        async with factory() as session, session.begin():
            return await publish_agent_challenger(
                session,
                proposal_hash=proposal.proposal_hash,
                actor_id=actor_id,
                now=_NOW + timedelta(hours=2),
            )

    first, second = await asyncio.gather(
        concurrent_publish("human-local-1"), concurrent_publish("human-local-2")
    )
    published = first if not first.replayed else second
    async with factory() as session:
        active = await load_complete_prompt_authority(session)
        explicit = await load_complete_prompt_authority(
            session, orchestrator_id=published.challenger_prompt_version_id
        )
        after_counts = {
            table: await session.scalar(text(f"SELECT count(*) FROM {table}"))  # noqa: S608
            for table in protected_tables
        }

    assert published.replayed is False
    assert sorted((first.replayed, second.replayed)) == [False, True]
    assert published.challenger_model_config_id == _MODEL_ID
    assert active is not None and active["orchestrator"].id == _PROMPT_ID
    assert after_counts == before_counts
    assert explicit is not None
    assert explicit["orchestrator"].id == published.challenger_prompt_version_id
    assert all(explicit[role].release_channel == "challenger" for role in ROLE_ORDER)
    assert "using existing fields" in explicit["portfolio"].content
    assert "using existing fields" not in explicit["orchestrator"].content
    for role in ROLE_ORDER:
        if role != "portfolio":
            assert explicit[role].content == prompt_assets[role].content
            assert explicit[role].content_hash == prompt_assets[role].content_hash

    async with factory() as session, session.begin():
        replay = await publish_agent_challenger(
            session,
            proposal_hash=proposal.proposal_hash,
            actor_id="human-local",
            now=_NOW + timedelta(hours=3),
        )
    assert replay.replayed is True
    assert replay.challenger_prompt_version_id == published.challenger_prompt_version_id


@pytest.mark.asyncio
async def test_propose_job_calls_runtime_once_and_replays_only_exact_request(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    retrospective = _challenger_retrospective_fixture()

    async def collect(*args, **kwargs):  # type: ignore[no-untyped-def]
        del args, kwargs
        return retrospective

    monkeypatch.setattr(
        "nexus.ops.agent_challenger.collect_agent_challenger_retrospective", collect
    )

    class Runtime:
        calls = 0

        async def generate(self, source):  # type: ignore[no-untyped-def]
            assert source == retrospective
            self.calls += 1
            return _challenger_suggestion(retrospective), UsageSnapshot(
                input_tokens=10,
                output_tokens=10,
                provider_calls=1,
                tool_calls=0,
            )

    runtime = Runtime()
    request = ProposeRequest(
        action="propose",
        instrument_id=_INSTRUMENT_ID,
        history_limit=4,
        idempotency_key="exact-request",
    )
    first = await execute_propose(
        EngineSettings(database_url=migrated_database_url), request, runtime=runtime
    )
    replay = await execute_propose(
        EngineSettings(database_url=migrated_database_url), request, runtime=runtime
    )

    assert runtime.calls == 1
    assert replay == first
    with pytest.raises(AgentChallengerError, match="AGENT_CHALLENGER_IDEMPOTENCY_CONFLICT"):
        await execute_propose(
            EngineSettings(database_url=migrated_database_url),
            request.model_copy(update={"history_limit": 5}),
            runtime=runtime,
        )


@pytest.mark.asyncio
async def test_failed_propose_job_is_not_replayed(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    retrospective = _challenger_retrospective_fixture()

    async def collect(*args, **kwargs):  # type: ignore[no-untyped-def]
        del args, kwargs
        return retrospective

    monkeypatch.setattr(
        "nexus.ops.agent_challenger.collect_agent_challenger_retrospective", collect
    )

    class FailingRuntime:
        calls = 0

        async def generate(self, source):  # type: ignore[no-untyped-def]
            assert source == retrospective
            self.calls += 1
            raise RuntimeError("bounded fixture failure")

    runtime = FailingRuntime()
    request = ProposeRequest(
        action="propose",
        instrument_id=_INSTRUMENT_ID,
        history_limit=4,
        idempotency_key="failed-request",
    )
    with pytest.raises(AgentChallengerError, match="AGENT_CHALLENGER_RESULT_UNKNOWN"):
        await execute_propose(
            EngineSettings(database_url=migrated_database_url), request, runtime=runtime
        )
    with pytest.raises(AgentChallengerError, match="AGENT_CHALLENGER_RESULT_UNKNOWN"):
        await execute_propose(
            EngineSettings(database_url=migrated_database_url), request, runtime=runtime
        )
    assert runtime.calls == 1


@pytest.mark.asyncio
async def test_admission_pins_exact_version_source_run(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    request = ResearchRequest(
        instrument_id=_INSTRUMENT_ID,
        data_snapshot_id=_SNAPSHOT_ID,
        decision_time=_NOW,
    )
    async with factory() as session, session.begin():
        source = await admit_research_run(
            session,
            actor_id="version-source",
            route="/internal/v1/research-runs",
            idempotency_key="version-source",
            request_hash="6" * 64,
            request=request,
            prompt_version_id=_PROMPT_ID,
            model_config_id=_MODEL_ID,
            daily_limit_usd=Decimal("100"),
            now=_NOW,
        )
        source_run_id = source.run_id
        source_prompt_hash = source.run.prompt_hash
        source_model_hash = source.run.model_hash

    newer_model_id = UUID("0198d500-1000-7000-8000-000000001580")
    newer_prompt_ids = {
        role: UUID(f"0198d500-1000-7000-8000-0000000017{index:02d}")
        for index, role in enumerate(ROLE_ORDER)
    }
    prompt_assets = load_prompt_assets()
    async with factory() as session, session.begin():
        session.add(
            AgentModelConfigRecord(
                id=newer_model_id,
                model_id="unrelated-newer-model",
                version="fixture-pricing-v2",
                config_hash="3" * 64,
                max_input_tokens=1_000_000,
                max_tool_calls=8,
                max_output_tokens=10_000,
                input_usd_per_million=Decimal("1"),
                output_usd_per_million=Decimal("2"),
                pricing_available_at=_NOW - timedelta(days=1),
                pricing_expires_at=_NOW + timedelta(days=1),
            )
        )
        session.add_all(
            AgentPromptVersionRecord(
                id=newer_prompt_ids[role],
                role=role,
                version="unrelated-newer-v2",
                content_hash=hashlib.sha256(
                    f"{prompt_assets[role].content}\nunrelated newer".encode()
                ).hexdigest(),
                content=f"{prompt_assets[role].content}\nunrelated newer",
            )
            for role in ROLE_ORDER
        )
        await session.flush()
        newer = await admit_research_run(
            session,
            actor_id="unrelated-newer-actor",
            route="/internal/v1/research-runs",
            idempotency_key="unrelated-newer",
            request_hash="9" * 64,
            request=request,
            prompt_version_id=newer_prompt_ids["orchestrator"],
            model_config_id=newer_model_id,
            daily_limit_usd=Decimal("100"),
            now=_NOW,
        )

    async with factory() as session, session.begin():
        pinned = await admit_research_run(
            session,
            actor_id="local-agent-version-incumbent",
            route="agent-version-evaluation-v1",
            idempotency_key="pinned-incumbent",
            request_hash="7" * 64,
            request=request,
            prompt_version_id=None,
            model_config_id=None,
            daily_limit_usd=Decimal("100"),
            now=_NOW,
            version_source_run_id=source_run_id,
        )

    assert pinned.replayed is False
    assert pinned.run_id != source_run_id
    assert pinned.run.prompt_hash == source_prompt_hash
    assert pinned.run.model_hash == source_model_hash
    assert pinned.run.prompt_hash != newer.run.prompt_hash
    assert pinned.run.model_hash != newer.run.model_hash
    assert pinned.run.prompt_manifest == source.run.prompt_manifest
    assert pinned.run.model_config_id == source.run.model_config_id


@pytest.mark.asyncio
async def test_admission_rejects_ambiguous_version_authority(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    request = ResearchRequest(
        instrument_id=_INSTRUMENT_ID,
        data_snapshot_id=_SNAPSHOT_ID,
        decision_time=_NOW,
    )
    with pytest.raises(ValueError, match="Agent version authority is ambiguous"):
        async with factory() as session, session.begin():
            await admit_research_run(
                session,
                actor_id="local-agent-version-incumbent",
                route="agent-version-evaluation-v1",
                idempotency_key="ambiguous-incumbent",
                request_hash="8" * 64,
                request=request,
                prompt_version_id=_PROMPT_ID,
                model_config_id=_MODEL_ID,
                daily_limit_usd=Decimal("100"),
                now=_NOW,
                reuse_latest_actor_agent_version=True,
                version_source_run_id=_RUN_IDS[0],
            )


@pytest.mark.asyncio
async def test_single_security_admission_rejects_explicit_portfolio_model(
    migrated_database_url: str,
) -> None:
    from nexus.agents.codex_versions import PORTFOLIO_DECISION_CONFIG_VERSION

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    async with factory() as session, session.begin():
        session.add(
            AgentModelConfigRecord(
                id=_PORTFOLIO_MODEL_ID,
                model_id="gpt-5.6-terra",
                version=PORTFOLIO_DECISION_CONFIG_VERSION,
                config_hash="9" * 64,
                max_input_tokens=128_000,
                max_tool_calls=1,
                max_output_tokens=16_384,
                input_usd_per_million=Decimal("0"),
                output_usd_per_million=Decimal("0"),
                pricing_available_at=_NOW - timedelta(days=1),
                pricing_expires_at=_NOW + timedelta(days=1),
            )
        )
    request = ResearchRequest(
        instrument_id=_INSTRUMENT_ID,
        data_snapshot_id=_SNAPSHOT_ID,
        decision_time=_NOW,
    )

    with pytest.raises(ValueError, match="point-in-time data authority is invalid"):
        async with factory() as session, session.begin():
            await admit_research_run(
                session,
                actor_id="fixture-actor",
                route="/internal/v1/research-runs",
                idempotency_key="portfolio-model-not-single-security",
                request_hash="8" * 64,
                request=request,
                prompt_version_id=_PROMPT_ID,
                model_config_id=_PORTFOLIO_MODEL_ID,
                daily_limit_usd=Decimal("3.00"),
                now=_NOW,
            )
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(Job)) == 0
        assert (
            await session.scalar(
                select(func.count()).select_from(AgentDailyBudgetReservationRecord)
            )
            == 0
        )


@pytest.mark.asyncio
async def test_concurrent_budget_counts_settled_usage_and_only_release_restores_capacity(
    migrated_database_url: str,
) -> None:
    """Two sessions cannot oversubscribe; settled actual cost remains in the daily total."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    await _seed_runs(factory, _RUN_IDS[:4])
    barrier = asyncio.Barrier(2)

    async def reserve(run_id: UUID, amount: str):
        async with factory() as session, session.begin():
            await barrier.wait()
            return await reserve_daily_budget(
                session,
                run_id=run_id,
                now=_NOW,
                daily_limit_usd=Decimal("3.00"),
                worst_case_usd=Decimal(amount),
                pricing_version="fixture-pricing-v1",
                pricing_fresh=True,
            )

    results = await asyncio.wait_for(
        asyncio.gather(
            reserve(_RUN_IDS[0], "2.00"),
            reserve(_RUN_IDS[1], "2.00"),
            return_exceptions=True,
        ),
        timeout=5,
    )
    winner = next(result for result in results if not isinstance(result, BaseException))
    assert sum(isinstance(result, BudgetExhausted) for result in results) == 1
    async with factory() as session, session.begin():
        await settle_daily_budget(
            session,
            run_id=winner.run_id,
            actual_usd=Decimal("1.50"),
            input_tokens=120,
            output_tokens=80,
        )
    async with factory() as session, session.begin():
        with pytest.raises(BudgetExhausted):
            await reserve_daily_budget(
                session,
                run_id=_RUN_IDS[2],
                now=_NOW,
                daily_limit_usd=Decimal("3.00"),
                worst_case_usd=Decimal("1.51"),
                pricing_version="fixture-pricing-v1",
                pricing_fresh=True,
            )
    async with factory() as session, session.begin():
        reservation = await reserve_daily_budget(
            session,
            run_id=_RUN_IDS[2],
            now=_NOW,
            daily_limit_usd=Decimal("3.00"),
            worst_case_usd=Decimal("1.50"),
            pricing_version="fixture-pricing-v1",
            pricing_fresh=True,
        )
        await release_daily_budget(session, run_id=reservation.run_id)
    async with factory() as session:
        rows = list(
            await session.scalars(
                select(AgentResearchRunRecord).where(
                    AgentResearchRunRecord.id.in_((_RUN_IDS[0], _RUN_IDS[1], _RUN_IDS[2]))
                )
            )
        )
        settled = next(row for row in rows if row.id == winner.run_id)
        released = await session.scalar(
            select(func.count())
            .select_from(text("agent_daily_budget_reservations"))
            .where(text("state = 'released'"))
        )
    assert (settled.settled_cost_usd, settled.input_tokens, settled.output_tokens) == (
        Decimal("1.500000000000"),
        120,
        80,
    )
    assert released == 1


@pytest.mark.asyncio
async def test_atomic_admission_replays_same_body_and_conflicts_changed_body(
    migrated_database_url: str,
) -> None:
    """The idempotency claim, prepared run, and reservation are one transaction."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    request = ResearchRequest(
        instrument_id=_INSTRUMENT_ID,
        data_snapshot_id=_SNAPSHOT_ID,
        decision_time=_NOW,
    )
    barrier = asyncio.Barrier(2)

    async def admit(request_hash: str):
        async with factory() as session, session.begin():
            return await admit_research_run(
                session,
                actor_id="fixture-actor",
                route="/internal/v1/research-runs",
                idempotency_key="same-key",
                request_hash=request_hash,
                request=request,
                prompt_version_id=_PROMPT_ID,
                model_config_id=_MODEL_ID,
                daily_limit_usd=Decimal("3.00"),
                now=_NOW,
                after_authority=barrier.wait,
            )

    same = await asyncio.wait_for(asyncio.gather(admit("a" * 64), admit("a" * 64)), timeout=5)
    assert same[0].run_id == same[1].run_id
    assert {same[0].replayed, same[1].replayed} == {False, True}
    async with factory() as session, session.begin():
        alternate_key = await admit_research_run(
            session,
            actor_id="fixture-actor",
            route="/internal/v1/research-runs",
            idempotency_key="same-authority-another-key",
            request_hash="a" * 64,
            request=request,
            prompt_version_id=_PROMPT_ID,
            model_config_id=_MODEL_ID,
            daily_limit_usd=Decimal("3.00"),
            now=_NOW,
        )
    assert (alternate_key.run_id, alternate_key.replayed) == (same[0].run_id, True)
    with pytest.raises(IdempotencyConflict):
        async with factory() as session, session.begin():
            await admit_research_run(
                session,
                actor_id="fixture-actor",
                route="/internal/v1/research-runs",
                idempotency_key="same-key",
                request_hash="b" * 64,
                request=request,
                prompt_version_id=_PROMPT_ID,
                model_config_id=_MODEL_ID,
                daily_limit_usd=Decimal("3.00"),
                now=_NOW,
            )
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 1
        assert await session.scalar(select(func.count()).select_from(IdempotencyRecord)) == 2
        job = await session.scalar(select(Job))
        reservations = await session.scalar(
            select(func.count()).select_from(text("agent_daily_budget_reservations"))
        )
    assert job is not None
    assert (job.status, job.schema_version, job.payload) == (
        "queued",
        "agent_research_job_v1",
        {"run_id": str(same[0].run_id)},
    )
    assert reservations == 1


@pytest.mark.asyncio
async def test_admission_accepts_100_evidence_rows_and_rejects_101_before_enqueue(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    await _add_fixture_evidence_links(factory, start=0, count=99)
    async with factory() as session, session.begin():
        accepted = await admit_research_run(
            session,
            actor_id="fixture-actor",
            route="/internal/v1/research-runs",
            idempotency_key="evidence-bound-100",
            request_hash="3" * 64,
            request=ResearchRequest(
                instrument_id=_INSTRUMENT_ID,
                data_snapshot_id=_SNAPSHOT_ID,
                decision_time=_NOW,
            ),
            prompt_version_id=_PROMPT_ID,
            model_config_id=_MODEL_ID,
            daily_limit_usd=Decimal("100.00"),
            now=_NOW,
        )
    assert len(accepted.run.evidence_manifest) == 100

    await _add_fixture_evidence_links(factory, start=99, count=1)
    with pytest.raises(ValueError, match="evidence authority exceeds 100"):
        async with factory() as session, session.begin():
            await admit_research_run(
                session,
                actor_id="fixture-actor",
                route="/internal/v1/research-runs",
                idempotency_key="evidence-bound-101",
                request_hash="4" * 64,
                request=ResearchRequest(
                    instrument_id=_INSTRUMENT_ID,
                    data_snapshot_id=_SNAPSHOT_ID,
                    decision_time=_NOW,
                ),
                prompt_version_id=_PROMPT_ID,
                model_config_id=_MODEL_ID,
                daily_limit_usd=Decimal("100.00"),
                now=_NOW,
            )
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(Job)) == 1
        reservations = await session.scalar(
            select(func.count()).select_from(AgentDailyBudgetReservationRecord)
        )
    assert reservations == 1


@pytest.mark.asyncio
async def test_admission_rejects_future_snapshot_and_rolls_back_claim_and_budget(
    migrated_database_url: str,
) -> None:
    """Point-in-time authority failure leaves no idempotency, run, or reservation artifact."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory, snapshot_available_at=_NOW + timedelta(seconds=1))
    with pytest.raises(ValueError, match="point-in-time data authority"):
        async with factory() as session, session.begin():
            await admit_research_run(
                session,
                actor_id="fixture-actor",
                route="/internal/v1/research-runs",
                idempotency_key="future-data",
                request_hash="c" * 64,
                request=ResearchRequest(
                    instrument_id=_INSTRUMENT_ID,
                    data_snapshot_id=_SNAPSHOT_ID,
                    decision_time=_NOW,
                ),
                prompt_version_id=_PROMPT_ID,
                model_config_id=_MODEL_ID,
                daily_limit_usd=Decimal("3.00"),
                now=_NOW,
            )
    async with factory() as session:
        counts = (
            await session.scalar(select(func.count()).select_from(IdempotencyRecord)),
            await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)),
            await session.scalar(
                select(func.count()).select_from(text("agent_daily_budget_reservations"))
            ),
        )
    assert counts == (0, 0, 0)


@pytest.mark.asyncio
async def test_run_authority_cannot_change_during_a_legal_state_transition(
    migrated_database_url: str,
) -> None:
    """A legal prepared-to-running edge must not authorize actor/hash/authority mutation."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    async with factory() as session, session.begin():
        admission = await admit_research_run(
            session,
            actor_id="fixture-actor",
            route="/internal/v1/research-runs",
            idempotency_key="frozen-authority",
            request_hash="d" * 64,
            request=ResearchRequest(
                instrument_id=_INSTRUMENT_ID,
                data_snapshot_id=_SNAPSHOT_ID,
                decision_time=_NOW,
            ),
            prompt_version_id=_PROMPT_ID,
            model_config_id=_MODEL_ID,
            daily_limit_usd=Decimal("3.00"),
            now=_NOW,
        )
    with pytest.raises(IntegrityError, match="authority is immutable"):
        async with factory() as session, session.begin():
            await session.execute(
                text(
                    "UPDATE agent_research_runs SET status='running', "
                    "actor_id='mutated-actor', input_hash=:hash WHERE id=:run_id"
                ),
                {"hash": "e" * 64, "run_id": admission.run_id},
            )
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, admission.run_id)
    assert run is not None
    assert (run.status, run.actor_id, run.input_hash) == (
        "prepared",
        "fixture-actor",
        "d" * 64,
    )


@pytest.mark.asyncio
async def test_prompt_manifest_and_snapshot_evidence_are_database_immutable(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    async with factory() as session, session.begin():
        admission = await admit_research_run(
            session,
            actor_id="fixture-actor",
            route="/internal/v1/research-runs",
            idempotency_key="frozen-prompt-manifest",
            request_hash="7" * 64,
            request=ResearchRequest(
                instrument_id=_INSTRUMENT_ID,
                data_snapshot_id=_SNAPSHOT_ID,
                decision_time=_NOW,
            ),
            prompt_version_id=_PROMPT_ID,
            model_config_id=_MODEL_ID,
            daily_limit_usd=Decimal("3.00"),
            now=_NOW,
        )
    with pytest.raises(IntegrityError, match="run authority is immutable"):
        async with factory() as session, session.begin():
            await session.execute(
                text(
                    "UPDATE agent_research_runs SET status='running', "
                    "prompt_manifest='{}'::jsonb WHERE id=:run_id"
                ),
                {"run_id": admission.run_id},
            )
    with pytest.raises(IntegrityError, match="run authority is immutable"):
        async with factory() as session, session.begin():
            await session.execute(
                text(
                    "UPDATE agent_research_runs SET status='running', "
                    "evidence_hash=:hash, evidence_manifest='[]'::jsonb WHERE id=:run_id"
                ),
                {"hash": "0" * 64, "run_id": admission.run_id},
            )


@pytest.mark.asyncio
async def test_snapshot_evidence_rejects_nonfixture_task14_authority_without_instrument_owner(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    unowned_id = UUID("0198d500-1000-7000-8000-000000001599")
    async with factory() as session, session.begin():
        session.add(
            ResearchEvidenceRef(
                id=unowned_id,
                source="sec",
                provider_object_id="unowned-evidence",
                revision="v1",
                available_at=_NOW - timedelta(minutes=1),
                location=f"memory://sha256/{'9' * 64}",
                content_hash="9" * 64,
                byte_length=42,
            )
        )
    with pytest.raises(IntegrityError, match="not owned by the instrument"):
        async with factory() as session, session.begin():
            session.add(
                AgentDataSnapshotEvidenceRecord(
                    data_snapshot_id=_SNAPSHOT_ID,
                    instrument_id=_INSTRUMENT_ID,
                    evidence_ref_id=unowned_id,
                    source="sec",
                    provider_object_id="unowned-evidence",
                    revision="v1",
                    available_at=_NOW - timedelta(minutes=1),
                    location=f"memory://sha256/{'9' * 64}",
                    content_hash="9" * 64,
                )
            )
    with pytest.raises(IntegrityError, match="append-only"):
        async with factory() as session, session.begin():
            await session.execute(
                text(
                    "DELETE FROM agent_data_snapshot_evidence WHERE data_snapshot_id=:snapshot_id"
                ),
                {"snapshot_id": _SNAPSHOT_ID},
            )


@pytest.mark.asyncio
async def test_budget_rows_reject_delete_and_preserve_settled_spend(
    migrated_database_url: str,
) -> None:
    """Deleting a reserved or settled row must never reopen the daily USD cap."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    async with factory() as session, session.begin():
        admission = await admit_research_run(
            session,
            actor_id="fixture-actor",
            route="/internal/v1/research-runs",
            idempotency_key="durable-spend",
            request_hash="f" * 64,
            request=ResearchRequest(
                instrument_id=_INSTRUMENT_ID,
                data_snapshot_id=_SNAPSHOT_ID,
                decision_time=_NOW,
            ),
            prompt_version_id=_PROMPT_ID,
            model_config_id=_MODEL_ID,
            daily_limit_usd=Decimal("3.00"),
            now=_NOW,
        )
    with pytest.raises(IntegrityError, match="budget reservation is durable"):
        async with factory() as session, session.begin():
            await session.execute(
                text("DELETE FROM agent_daily_budget_reservations WHERE run_id=:run_id"),
                {"run_id": admission.run_id},
            )
    async with factory() as session, session.begin():
        await settle_daily_budget(
            session,
            run_id=admission.run_id,
            actual_usd=Decimal("0.50"),
            input_tokens=100,
            output_tokens=50,
        )
    with pytest.raises(IntegrityError, match="budget reservation is durable"):
        async with factory() as session, session.begin():
            await session.execute(
                text("DELETE FROM agent_daily_budget_reservations WHERE run_id=:run_id"),
                {"run_id": admission.run_id},
            )
    async with factory() as session:
        reservation = await session.scalar(
            select(AgentDailyBudgetReservationRecord).where(
                AgentDailyBudgetReservationRecord.run_id == admission.run_id
            )
        )
    assert reservation is not None
    assert (reservation.state, reservation.settled_usd) == (
        "settled",
        Decimal("0.500000000000"),
    )


@pytest.mark.asyncio
async def test_agent_job_payload_is_frozen_and_job_cannot_be_deleted(
    migrated_database_url: str,
) -> None:
    """Queue mutation must not redirect an admitted run or erase its durable dispatch."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    async with factory() as session, session.begin():
        admission = await admit_research_run(
            session,
            actor_id="fixture-actor",
            route="/internal/v1/research-runs",
            idempotency_key="frozen-job",
            request_hash="9" * 64,
            request=ResearchRequest(
                instrument_id=_INSTRUMENT_ID,
                data_snapshot_id=_SNAPSHOT_ID,
                decision_time=_NOW,
            ),
            prompt_version_id=_PROMPT_ID,
            model_config_id=_MODEL_ID,
            daily_limit_usd=Decimal("3.00"),
            now=_NOW,
        )
    async with factory() as session:
        job_key = await session.scalar(
            select(Job.job_key).where(Job.payload["run_id"].as_string() == str(admission.run_id))
        )
    assert isinstance(job_key, str)
    with pytest.raises(IntegrityError, match="job authority is immutable"):
        async with factory() as session, session.begin():
            await session.execute(
                text(
                    "UPDATE jobs SET status='running', started_at=now(), version=version+1, "
                    "payload=:payload WHERE job_key=:job_key"
                ),
                {
                    "payload": '{"run_id":"00000000-0000-0000-0000-000000000000"}',
                    "job_key": job_key,
                },
            )
    with pytest.raises(IntegrityError, match="job state is not deletable"):
        async with factory() as session, session.begin():
            await session.execute(
                text("DELETE FROM jobs WHERE job_key=:job_key"),
                {"job_key": job_key},
            )


@pytest.mark.asyncio
async def test_output_evidence_requires_exact_task14_owner_and_point_in_time(
    migrated_database_url: str,
) -> None:
    """Raw SQL cannot attach future or hash/location-mismatched evidence to Agent output."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_authority(factory)
    await _seed_runs(factory, (_RUN_IDS[0],))
    output_id = UUID("0198d500-1000-7000-8000-000000001550")
    evidence_id = UUID("0198d500-1000-7000-8000-000000001551")
    async with factory() as session, session.begin():
        session.add_all(
            (
                AgentResearchOutputRecord(
                    id=output_id,
                    run_id=_RUN_IDS[0],
                    sequence=1,
                    status="validated",
                    content_hash="d" * 64,
                    intent=None,
                ),
                AgentResearchOutputRecord(
                    id=UUID("0198d500-1000-7000-8000-000000001552"),
                    run_id=_RUN_IDS[0],
                    sequence=2,
                    status="audited",
                    content_hash="c" * 64,
                    intent=None,
                ),
                ResearchEvidenceRef(
                    id=evidence_id,
                    source="fixture",
                    provider_object_id="evidence-1",
                    revision="v1",
                    available_at=_NOW - timedelta(minutes=1),
                    location=f"memory://sha256/{'e' * 64}",
                    content_hash="e" * 64,
                    byte_length=42,
                ),
            )
        )
    async with factory() as session, session.begin():
        session.add(
            AgentResearchOutputEvidenceRecord(
                output_id=output_id,
                evidence_ref_id=evidence_id,
                source="fixture",
                provider_object_id="evidence-1",
                revision="v1",
                available_at=_NOW - timedelta(minutes=1),
                location=f"memory://sha256/{'e' * 64}",
                content_hash="e" * 64,
            )
        )
    with pytest.raises(IntegrityError):
        async with factory() as session, session.begin():
            await session.execute(
                text(
                    "INSERT INTO agent_research_output_evidence "
                    "(output_id,evidence_ref_id,source,provider_object_id,revision,"
                    "available_at,location,content_hash) VALUES "
                    "(:output,:evidence,'fixture','evidence-1','v1',"
                    ":available,:location,:hash)"
                ),
                {
                    "output": UUID("0198d500-1000-7000-8000-000000001552"),
                    "evidence": evidence_id,
                    "available": _NOW - timedelta(minutes=1),
                    "location": f"memory://sha256/{'f' * 64}",
                    "hash": "f" * 64,
                },
            )
    future_id = UUID("0198d500-1000-7000-8000-000000001553")
    async with factory() as session, session.begin():
        session.add(
            ResearchEvidenceRef(
                id=future_id,
                source="fixture",
                provider_object_id="future-evidence",
                revision="v1",
                available_at=_NOW + timedelta(seconds=1),
                location=f"memory://sha256/{'f' * 64}",
                content_hash="f" * 64,
                byte_length=42,
            )
        )
    with pytest.raises(IntegrityError, match="future evidence"):
        async with factory() as session, session.begin():
            session.add(
                AgentResearchOutputEvidenceRecord(
                    output_id=output_id,
                    evidence_ref_id=future_id,
                    source="fixture",
                    provider_object_id="future-evidence",
                    revision="v1",
                    available_at=_NOW + timedelta(seconds=1),
                    location=f"memory://sha256/{'f' * 64}",
                    content_hash="f" * 64,
                )
            )


async def _seed_authority(factory, *, snapshot_available_at: datetime = _NOW) -> None:
    prompts = load_prompt_assets()
    prompt_ids = {
        role: _PROMPT_ID
        if role == "orchestrator"
        else UUID(f"0198d500-1000-7000-8000-0000000016{20 + index:02d}")
        for index, role in enumerate(ROLE_ORDER)
    }
    async with factory() as session, session.begin():
        session.add_all(
            (
                Instrument(
                    id=_INSTRUMENT_ID,
                    asset_type="equity",
                    exchange="NASDAQ",
                    currency="USD",
                    tick_size=Decimal("0.01"),
                    risk_bucket="equity",
                ),
                InstrumentAlias(
                    instrument_id=_INSTRUMENT_ID,
                    symbol="AAPL",
                    alias_type="ticker",
                    valid_from=_NOW.date() - timedelta(days=1),
                    valid_to=None,
                ),
                DataSnapshot(
                    id=_SNAPSHOT_ID,
                    source="fixture",
                    provider_object_id="agent-snapshot",
                    revision="v1",
                    observed_at=_NOW,
                    available_at=snapshot_available_at,
                    received_at=snapshot_available_at,
                    schema_version="1",
                    payload={},
                ),
                AgentModelConfigRecord(
                    id=_MODEL_ID,
                    model_id="fixture-model",
                    version="fixture-pricing-v1",
                    config_hash="2" * 64,
                    max_input_tokens=1_000_000,
                    max_tool_calls=8,
                    max_output_tokens=10_000,
                    input_usd_per_million=Decimal("1.00"),
                    output_usd_per_million=Decimal("2.00"),
                    pricing_available_at=_NOW - timedelta(days=1),
                    pricing_expires_at=_NOW + timedelta(days=1),
                ),
                ResearchEvidenceRef(
                    id=_EVIDENCE_ID,
                    source="fixture",
                    provider_object_id="admission-evidence",
                    revision="v1",
                    available_at=_NOW - timedelta(minutes=1),
                    location=f"memory://sha256/{'a' * 64}",
                    content_hash="a" * 64,
                    byte_length=42,
                ),
            )
        )
        session.add_all(
            AgentPromptVersionRecord(
                id=prompt_ids[role],
                role=role,
                version=prompts[role].version,
                content_hash=prompts[role].content_hash,
                content=prompts[role].content,
            )
            for role in ROLE_ORDER
        )
        await session.flush()
        session.add(
            AgentDataSnapshotEvidenceRecord(
                data_snapshot_id=_SNAPSHOT_ID,
                instrument_id=_INSTRUMENT_ID,
                evidence_ref_id=_EVIDENCE_ID,
                source="fixture",
                provider_object_id="admission-evidence",
                revision="v1",
                available_at=_NOW - timedelta(minutes=1),
                location=f"memory://sha256/{'a' * 64}",
                content_hash="a" * 64,
            )
        )


async def _add_fixture_evidence_links(factory, *, start: int, count: int) -> None:
    async with factory() as session, session.begin():
        records = [
            ResearchEvidenceRef(
                id=UUID(f"0198d500-1000-7000-8000-{2000 + index:012d}"),
                source="fixture",
                provider_object_id=f"bounded-evidence-{index}",
                revision="v1",
                available_at=_NOW - timedelta(minutes=1),
                location=f"memory://sha256/{index:064x}",
                content_hash=f"{index:064x}",
                byte_length=1,
            )
            for index in range(start, start + count)
        ]
        session.add_all(records)
        await session.flush()
        session.add_all(
            AgentDataSnapshotEvidenceRecord(
                data_snapshot_id=_SNAPSHOT_ID,
                instrument_id=_INSTRUMENT_ID,
                evidence_ref_id=record.id,
                source=record.source,
                provider_object_id=record.provider_object_id,
                revision=record.revision,
                available_at=record.available_at,
                location=record.location,
                content_hash=record.content_hash,
            )
            for record in records
        )


async def _seed_runs(factory, run_ids: tuple[UUID, ...]) -> None:
    prompt_manifest = {
        role: {"id": str(_PROMPT_ID), "version": "fixture-v1", "content_hash": "1" * 64}
        for role in ROLE_ORDER
    }
    prompt_hash = hashlib.sha256(
        json.dumps(prompt_manifest, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    evidence_manifest = [
        {
            "id": str(_EVIDENCE_ID),
            "source": "fixture",
            "provider_object_id": "admission-evidence",
            "revision": "v1",
            "available_at": (_NOW - timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
            "location": f"memory://sha256/{'a' * 64}",
            "content_hash": "a" * 64,
        }
    ]
    evidence_hash = hashlib.sha256(
        json.dumps(evidence_manifest, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    async with factory() as session, session.begin():
        session.add_all(
            AgentResearchRunRecord(
                id=run_id,
                instrument_id=_INSTRUMENT_ID,
                data_snapshot_id=_SNAPSHOT_ID,
                prompt_version_id=_PROMPT_ID,
                model_config_id=_MODEL_ID,
                decision_time=_NOW,
                actor_id="fixture-actor",
                idempotency_key=f"seed-{run_id}",
                status="prepared",
                retry_count=0,
                schema_hash=hashlib.sha256(b"schema").hexdigest(),
                model_hash="2" * 64,
                prompt_hash=prompt_hash,
                prompt_manifest=prompt_manifest,
                evidence_hash=evidence_hash,
                evidence_manifest=evidence_manifest,
                tool_hash=hashlib.sha256(b"tools").hexdigest(),
                input_hash=hashlib.sha256(str(run_id).encode()).hexdigest(),
                reserved_cost_usd=Decimal("2.00"),
                settled_cost_usd=None,
                pricing_version="fixture-pricing-v1",
                input_tokens=None,
                output_tokens=None,
            )
            for run_id in run_ids
        )
