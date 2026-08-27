"""PostgreSQL-authoritative admission of bounded Strategy R&D experiment families."""

from __future__ import annotations

import asyncio
import hashlib
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.agents.responses import ResponsesModelConfig
from nexus.agents.responses_budget import UsageSnapshot
from nexus.audit.canonical import canonical_json
from nexus.evolution.agent import StrategyRNDRequestV1
from nexus.evolution.agent_tools import StrategyRNDToolContext, StrategyRNDTools
from nexus.evolution.models import StrategyExperimentProposalV1
from nexus.evolution.service import (
    EvolutionAdmissionAuthorityV1,
    EvolutionAdmissionError,
    admit_experiment_family,
    execute_strategy_rnd,
)
from nexus.persistence.models.agents import (
    AgentDataSnapshotEvidenceRecord,
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
)
from nexus.persistence.models.backtest import StrategyDefinitionRecord
from nexus.persistence.models.data import DataSnapshot, QualityReport
from nexus.persistence.models.evolution import (
    StrategyEvolutionPolicyRecord,
    StrategyExperimentFamilyRecord,
    StrategyExperimentRecord,
    StrategyExperimentStateProjectionRecord,
    StrategyResearchLineageRecord,
    StrategyRNDOutputRecord,
    StrategyRNDRunRecord,
)
from nexus.persistence.models.market import Instrument
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import select, text

NOW = datetime(2026, 8, 27, 12, tzinfo=UTC)
INSTRUMENT_ID = UUID("019d0000-0000-7000-8000-000000004301")
SNAPSHOT_ID = UUID("019d0000-0000-7000-8000-000000004302")
QUALITY_ID = UUID("019d0000-0000-7000-8000-000000004303")
EVIDENCE_ID = UUID("019d0000-0000-7000-8000-000000004304")
LINEAGE_ID = UUID("019d0000-0000-7000-8000-000000004305")
FORGED_EVIDENCE_ID = UUID("019d0000-0000-7000-8000-000000004399")
PROMPT_ID = UUID("019d0000-0000-7000-8000-000000004401")
MODEL_ID = UUID("019d0000-0000-7000-8000-000000004402")
SUMMARY_ID = UUID("019d0000-0000-7000-8000-000000004403")
RND_RUN_ID = UUID("019d0000-0000-7000-8000-000000004404")
RND_OUTPUT_ID = UUID("019d0000-0000-7000-8000-000000004405")
PROMPT_V2_ID = UUID("019d0000-0000-7000-8000-000000004411")
MODEL_V2_ID = UUID("019d0000-0000-7000-8000-000000004412")
RND_RUN_V2_ID = UUID("019d0000-0000-7000-8000-000000004414")
RND_OUTPUT_V2_ID = UUID("019d0000-0000-7000-8000-000000004415")


@pytest.mark.asyncio
async def test_admission_rejects_a_proposal_without_agent_output_authority(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    proposal, authority = await _seed_authority(factory, bind_agent_output=False)
    unbound = authority.model_copy(update={"rnd_output_id": None})
    async with factory() as session, session.begin():
        with pytest.raises(
            EvolutionAdmissionError, match="RND_OUTPUT_AUTHORITY_INVALID"
        ):
            await admit_experiment_family(
                session,
                proposal=proposal,
                authority=unbound,
                now=NOW,
                idempotency_key="phase2-unbound-agent-output",
            )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("keys", "expected_error"),
    [
        (("phase2-concurrent-same", "phase2-concurrent-same"), None),
        (
            ("phase2-concurrent-one", "phase2-concurrent-two"),
            "ACTIVE_FAMILY_EXISTS",
        ),
    ],
)
async def test_concurrent_admission_serializes_same_and_competing_keys(
    migrated_database_url: str,
    keys: tuple[str, str],
    expected_error: str | None,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    proposal, authority = await _seed_authority(factory)

    async def invoke(key: str):
        async with factory() as session, session.begin():
            return await admit_experiment_family(
                session,
                proposal=proposal,
                authority=authority,
                now=NOW,
                idempotency_key=key,
            )

    results = await asyncio.gather(
        *(invoke(key) for key in keys), return_exceptions=True
    )
    admitted = [item for item in results if not isinstance(item, BaseException)]
    errors = [item for item in results if isinstance(item, EvolutionAdmissionError)]
    if expected_error is None:
        assert len(admitted) == 2 and not errors
        assert len({item.family_id for item in admitted}) == 1
        assert {item.replayed for item in admitted} == {False, True}
    else:
        assert len(admitted) == 1 and len(errors) == 1
        assert errors[0].reason_code == expected_error


@pytest.mark.asyncio
async def test_agent_run_persists_bounded_usage_and_binds_admitted_family(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    proposal, authority = await _seed_authority(factory, bind_agent_output=False)
    request, tools, runtime = await _seed_agent_execution_authority(
        factory, proposal=proposal
    )
    async with factory() as session, session.begin():
        execution = await execute_strategy_rnd(
            session,
            request=request,
            tools=tools,
            runtime=runtime,
            prompt_version_id=PROMPT_ID,
            model_config_id=MODEL_ID,
            now=NOW,
        )
    assert execution.status == "completed"
    assert execution.output_id is not None
    bound_authority = authority.model_copy(
        update={"rnd_output_id": execution.output_id}
    )
    async with factory() as session, session.begin():
        admitted = await admit_experiment_family(
            session,
            proposal=execution.proposal,
            authority=bound_authority,
            now=NOW,
            idempotency_key="phase2-agent-bound-family",
        )
    async with factory() as session:
        run = await session.get(StrategyRNDRunRecord, execution.run_id)
        output = await session.get(StrategyRNDOutputRecord, execution.output_id)
        family = await session.get(StrategyExperimentFamilyRecord, admitted.family_id)
    assert run is not None
    assert (run.status, run.input_tokens, run.output_tokens, run.tool_call_count) == (
        "completed",
        25,
        10,
        2,
    )
    assert run.settled_cost_usd == Decimal("0.000045")
    assert output is not None and output.validation_status == "VALID"
    assert family is not None and family.rnd_output_id == execution.output_id


@pytest.mark.asyncio
async def test_agent_failure_is_recorded_without_output_or_family(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    proposal, _ = await _seed_authority(factory, bind_agent_output=False)
    request, tools, _ = await _seed_agent_execution_authority(factory, proposal=proposal)
    runtime = _FixtureRuntime(proposal=proposal, fail=True)
    async with factory() as session, session.begin():
        execution = await execute_strategy_rnd(
            session,
            request=request,
            tools=tools,
            runtime=runtime,
            prompt_version_id=PROMPT_ID,
            model_config_id=MODEL_ID,
            now=NOW,
        )
    async with factory() as session:
        run = await session.get(StrategyRNDRunRecord, execution.run_id)
        output_count = await session.scalar(text("SELECT count(*) FROM strategy_rnd_outputs"))
        family_count = await session.scalar(
            text("SELECT count(*) FROM strategy_experiment_families")
        )
    assert execution.status == "failed"
    assert execution.output_id is None and execution.proposal is None
    assert run is not None and run.status == "failed"
    assert (output_count, family_count) == (0, 0)


@pytest.mark.asyncio
async def test_admission_publishes_bounded_candidates_and_replays_same_key(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    proposal, authority = await _seed_authority(factory)
    async with factory() as session, session.begin():
        admitted = await admit_experiment_family(
            session,
            proposal=proposal,
            authority=authority,
            now=NOW,
            idempotency_key="phase2-family-one",
        )
    async with factory() as session, session.begin():
        replay = await admit_experiment_family(
            session,
            proposal=proposal,
            authority=authority,
            now=NOW,
            idempotency_key="phase2-family-one",
        )
    drifted = proposal.model_copy(
        update={"hypothesis": "A different proposal must not replay the first family."}
    )
    async with factory() as session, session.begin():
        with pytest.raises(EvolutionAdmissionError, match="IDEMPOTENCY_CONFLICT"):
            await admit_experiment_family(
                session,
                proposal=drifted,
                authority=authority,
                now=NOW + timedelta(minutes=1),
                idempotency_key="phase2-family-one",
            )

    assert admitted.replayed is False and replay.replayed is True
    assert replay.family_id == admitted.family_id
    assert admitted.candidate_count == 2
    async with factory() as session:
        family = await session.get(StrategyExperimentFamilyRecord, admitted.family_id)
        experiments = list(
            await session.scalars(
                select(StrategyExperimentRecord)
                .where(StrategyExperimentRecord.family_id == admitted.family_id)
                .order_by(StrategyExperimentRecord.candidate_ordinal)
            )
        )
        definitions = list(
            await session.scalars(
                select(StrategyDefinitionRecord).where(
                    StrategyDefinitionRecord.id.in_(
                        [item.candidate_strategy_definition_id for item in experiments]
                    )
                )
            )
        )
        paper_orders = await session.scalar(text("SELECT count(*) FROM paper_orders"))
        broker_orders = await session.scalar(
            text("SELECT count(*) FROM broker_order_snapshots")
        )
        provider_runs = await session.scalar(text("SELECT count(*) FROM provider_runs"))
    assert family is not None and family.candidate_count == 2
    assert [item.candidate_ordinal for item in experiments] == [1, 2]
    assert len({item.expression_hash for item in experiments}) == 2
    assert {definition.lifecycle for definition in definitions} == {"challenger"}
    assert (paper_orders, broker_orders, provider_runs) == (0, 0, 0)


@pytest.mark.asyncio
async def test_active_family_and_forged_evidence_fail_closed(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    proposal, authority = await _seed_authority(factory)
    forged = proposal.model_copy(update={"evidence_refs": (FORGED_EVIDENCE_ID,)})
    async with factory() as session, session.begin():
        with pytest.raises(EvolutionAdmissionError, match="EVIDENCE_INCOMPLETE"):
            await admit_experiment_family(
                session,
                proposal=forged,
                authority=authority,
                now=NOW,
                idempotency_key="phase2-forged-evidence",
            )
    async with factory() as session, session.begin():
        await admit_experiment_family(
            session,
            proposal=proposal,
            authority=authority,
            now=NOW,
            idempotency_key="phase2-active-family",
        )
    async with factory() as session, session.begin():
        with pytest.raises(EvolutionAdmissionError, match="ACTIVE_FAMILY_EXISTS"):
            await admit_experiment_family(
                session,
                proposal=proposal,
                authority=authority,
                now=NOW,
                idempotency_key="phase2-competing-family",
            )


@pytest.mark.asyncio
async def test_next_family_preserves_lineage_trial_count_across_time(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    proposal, authority = await _seed_authority(factory)
    async with factory() as session, session.begin():
        first = await admit_experiment_family(
            session,
            proposal=proposal,
            authority=authority,
            now=NOW,
            idempotency_key="phase2-lineage-first",
        )
    async with factory() as session, session.begin():
        await session.execute(
            text(
                "UPDATE strategy_experiment_state_projections "
                "SET current_state='SHADOW_FAILED',cumulative_oos_trial_count=3,"
                "event_sequence=1,version=2,updated_at=:updated_at WHERE family_id=:family_id"
            ),
            {"family_id": first.family_id, "updated_at": NOW + timedelta(days=1)},
        )
    async with factory() as session, session.begin():
        with pytest.raises(
            EvolutionAdmissionError, match="RND_OUTPUT_AUTHORITY_INVALID"
        ):
            await admit_experiment_family(
                session,
                proposal=proposal,
                authority=authority,
                now=NOW + timedelta(days=92),
                idempotency_key="phase2-stale-agent-output",
            )
    await _seed_bound_agent_output(
        factory,
        parent_id=proposal.parent_strategy_definition_id,
        proposal=proposal,
        prompt_id=PROMPT_V2_ID,
        model_id=MODEL_V2_ID,
        run_id=RND_RUN_V2_ID,
        output_id=RND_OUTPUT_V2_ID,
        as_of=NOW + timedelta(days=92),
        prompt_version="strategy_rnd_v2",
        model_version="fixture-v2",
    )
    followup_authority = authority.model_copy(
        update={"rnd_output_id": RND_OUTPUT_V2_ID}
    )
    async with factory() as session, session.begin():
        second = await admit_experiment_family(
            session,
            proposal=proposal,
            authority=followup_authority,
            now=NOW + timedelta(days=92),
            idempotency_key="phase2-lineage-second",
        )
    async with factory() as session:
        projection = await session.get(
            StrategyExperimentStateProjectionRecord, second.family_id
        )

    assert projection is not None
    assert projection.cumulative_oos_trial_count == 3


async def _seed_authority(factory, *, bind_agent_output: bool = True) -> tuple[
    StrategyExperimentProposalV1, EvolutionAdmissionAuthorityV1
]:
    async with factory() as session:
        parent = await session.scalar(
            select(StrategyDefinitionRecord).where(
                StrategyDefinitionRecord.strategy_version == "baseline_v1"
            )
        )
        search = await session.scalar(
            select(StrategyEvolutionPolicyRecord).where(
                StrategyEvolutionPolicyRecord.policy_kind == "evolution_search_space"
            )
        )
        budget = await session.scalar(
            select(StrategyEvolutionPolicyRecord).where(
                StrategyEvolutionPolicyRecord.policy_kind == "candidate_budget"
            )
        )
    assert parent is not None and search is not None and budget is not None
    async with factory() as session, session.begin():
        session.add_all(
            [
                Instrument(
                    id=INSTRUMENT_ID,
                    asset_type="equity",
                    exchange="NASDAQ",
                    currency="USD",
                    tick_size=Decimal("0.01"),
                    risk_bucket="equity",
                ),
                DataSnapshot(
                    id=SNAPSHOT_ID,
                    source="fixture",
                    provider_object_id="phase2-admission",
                    revision="v1",
                    observed_at=NOW - timedelta(minutes=2),
                    available_at=NOW - timedelta(minutes=1),
                    received_at=NOW - timedelta(minutes=1),
                    schema_version="v1",
                    payload={},
                ),
                ResearchEvidenceRef(
                    id=EVIDENCE_ID,
                    source="fixture",
                    provider_object_id="phase2-admission-evidence",
                    revision="v1",
                    available_at=NOW - timedelta(minutes=1),
                    location=f"memory://sha256/{'e' * 64}",
                    content_hash="e" * 64,
                    byte_length=42,
                ),
                StrategyResearchLineageRecord(
                    id=LINEAGE_ID,
                    lineage_key="bounded-us-equity-v1",
                    evolution_policy_id=search.id,
                    cumulative_oos_trial_count=0,
                    status="active",
                ),
            ]
        )
        await session.flush()
        session.add_all(
            [
                QualityReport(
                    id=QUALITY_ID,
                    data_snapshot_id=SNAPSHOT_ID,
                    status="PASS",
                    schema_version="v1",
                    payload={},
                ),
                AgentDataSnapshotEvidenceRecord(
                    data_snapshot_id=SNAPSHOT_ID,
                    instrument_id=INSTRUMENT_ID,
                    evidence_ref_id=EVIDENCE_ID,
                    authority_kind="research",
                    selected_at=None,
                    source="fixture",
                    provider_object_id="phase2-admission-evidence",
                    revision="v1",
                    available_at=NOW - timedelta(minutes=1),
                    location=f"memory://sha256/{'e' * 64}",
                    content_hash="e" * 64,
                ),
            ]
        )
    proposal = StrategyExperimentProposalV1(
        schema_version="strategy_experiment_proposal_v1",
        parent_strategy_definition_id=parent.id,
        champion_generation=1,
        research_lineage_id=LINEAGE_ID,
        hypothesis="A registered close threshold may improve robustness.",
        expected_mechanism="The frozen paired protocol should reduce unstable names.",
        invalidation_conditions=("Paired validation does not improve robustness.",),
        evidence_refs=(EVIDENCE_ID,),
        mutation_axes=({"axis": "min_close", "values": ("3", "10")},),
    )
    if bind_agent_output:
        await _seed_bound_agent_output(factory, parent_id=parent.id, proposal=proposal)
    authority = EvolutionAdmissionAuthorityV1(
        schema_version="strategy_evolution_admission_authority_v1",
        data_snapshot_id=SNAPSHOT_ID,
        quality_report_id=QUALITY_ID,
        candidate_budget_policy_id=budget.id,
        evolution_policy_id=search.id,
        authority_hash="a" * 64,
        protocol_hash="b" * 64,
        code_hash="c" * 64,
        rnd_output_id=RND_OUTPUT_ID if bind_agent_output else None,
    )
    return proposal, authority


async def _seed_bound_agent_output(
    factory,
    *,
    parent_id: UUID,
    proposal: StrategyExperimentProposalV1,
    prompt_id: UUID = PROMPT_ID,
    model_id: UUID = MODEL_ID,
    run_id: UUID = RND_RUN_ID,
    output_id: UUID = RND_OUTPUT_ID,
    as_of: datetime = NOW,
    prompt_version: str = "strategy_rnd_v1",
    model_version: str = "fixture-v1",
) -> None:
    from nexus.evolution.agent import load_strategy_rnd_prompt

    prompt = load_strategy_rnd_prompt()
    config = _runtime_config().model_copy(update={"pricing_version": model_version})
    proposal_document = proposal.model_dump(mode="json")
    async with factory() as session, session.begin():
        session.add_all(
            [
                AgentPromptVersionRecord(
                    id=prompt_id,
                    role=prompt.role,
                    version=prompt_version,
                    content_hash=prompt.content_hash,
                    content=prompt.content,
                ),
                AgentModelConfigRecord(
                    id=model_id,
                    model_id=config.model_id,
                    version=config.pricing_version,
                    config_hash="d" * 64,
                    max_input_tokens=config.max_input_tokens,
                    max_tool_calls=config.max_tool_calls,
                    max_output_tokens=config.max_output_tokens,
                    input_usd_per_million=config.input_usd_per_million,
                    output_usd_per_million=config.output_usd_per_million,
                    pricing_available_at=as_of - timedelta(days=1),
                    pricing_expires_at=as_of + timedelta(days=1),
                ),
            ]
        )
        await session.flush()
        session.add(
            StrategyRNDRunRecord(
                id=run_id,
                prompt_version_id=prompt_id,
                model_config_id=model_id,
                data_snapshot_id=SNAPSHOT_ID,
                champion_definition_id=parent_id,
                champion_generation=1,
                as_of=as_of,
                status="completed",
                reserved_cost_usd=config.worst_case_cost_usd,
                settled_cost_usd=Decimal("0"),
                tool_call_count=0,
                input_tokens=0,
                output_tokens=0,
                request_hash="f" * 64,
            )
        )
        await session.flush()
        session.add(
            StrategyRNDOutputRecord(
                id=output_id,
                run_id=run_id,
                schema_version="strategy_experiment_proposal_v1",
                proposal=proposal_document,
                proposal_hash=hashlib.sha256(
                    canonical_json(proposal_document).encode()
                ).hexdigest(),
                validation_status="VALID",
                reason_code=None,
            )
        )


async def _seed_agent_execution_authority(
    factory, *, proposal: StrategyExperimentProposalV1
) -> tuple[StrategyRNDRequestV1, StrategyRNDTools, _FixtureRuntime]:
    from nexus.evolution.agent import load_strategy_rnd_prompt

    prompt = load_strategy_rnd_prompt()
    config = _runtime_config()
    async with factory() as session, session.begin():
        session.add_all(
            [
                AgentPromptVersionRecord(
                    id=PROMPT_ID,
                    role=prompt.role,
                    version=prompt.version,
                    content_hash=prompt.content_hash,
                    content=prompt.content,
                ),
                AgentModelConfigRecord(
                    id=MODEL_ID,
                    model_id=config.model_id,
                    version=config.pricing_version,
                    config_hash="d" * 64,
                    max_input_tokens=config.max_input_tokens,
                    max_tool_calls=config.max_tool_calls,
                    max_output_tokens=config.max_output_tokens,
                    input_usd_per_million=config.input_usd_per_million,
                    output_usd_per_million=config.output_usd_per_million,
                    pricing_available_at=NOW - timedelta(days=1),
                    pricing_expires_at=NOW + timedelta(days=1),
                ),
            ]
        )
    request = StrategyRNDRequestV1(
        schema_version="strategy_rnd_request_v1",
        parent_strategy_definition_id=proposal.parent_strategy_definition_id,
        champion_generation=proposal.champion_generation,
        research_lineage_id=proposal.research_lineage_id,
        evolution_policy_id=(
            await _policy_id(factory, policy_kind="evolution_search_space")
        ),
        training_validation_summary_id=SUMMARY_ID,
        data_snapshot_id=SNAPSHOT_ID,
        evidence_refs=proposal.evidence_refs,
        as_of=NOW,
    )
    tools = StrategyRNDTools(
        context=StrategyRNDToolContext(
            parent_strategy_definition_id=request.parent_strategy_definition_id,
            champion_generation=request.champion_generation,
            research_lineage_id=request.research_lineage_id,
            evolution_policy_id=request.evolution_policy_id,
            training_validation_summary_id=request.training_validation_summary_id,
            as_of=request.as_of,
            sealed_experiment_ids=(),
            current_family_experiment_ids=(),
        )
    )
    return request, tools, _FixtureRuntime(proposal=proposal)


async def _policy_id(factory, *, policy_kind: str) -> UUID:
    async with factory() as session:
        value = await session.scalar(
            select(StrategyEvolutionPolicyRecord.id).where(
                StrategyEvolutionPolicyRecord.policy_kind == policy_kind
            )
        )
    assert value is not None
    return value


def _runtime_config() -> ResponsesModelConfig:
    return ResponsesModelConfig(
        model_id="configured-strategy-model",
        pricing_version="fixture-v1",
        max_input_tokens=10_000,
        max_output_tokens=1_000,
        max_tool_calls=4,
        absolute_timeout_seconds=Decimal("5"),
        input_usd_per_million=Decimal("1"),
        output_usd_per_million=Decimal("2"),
    )


class _FixtureRuntime:
    def __init__(
        self, *, proposal: StrategyExperimentProposalV1, fail: bool = False
    ) -> None:
        self._proposal = proposal
        self._fail = fail

    @property
    def configuration(self) -> ResponsesModelConfig:
        return _runtime_config()

    @property
    def usage(self) -> UsageSnapshot:
        return UsageSnapshot(
            input_tokens=25,
            output_tokens=10,
            tool_calls=2,
            provider_calls=1,
        )

    async def generate(
        self, request: StrategyRNDRequestV1, tools: StrategyRNDTools
    ) -> StrategyExperimentProposalV1:
        del request, tools
        if self._fail:
            raise TimeoutError("provider details must not be persisted")
        return self._proposal
