"""Restricted composition root for the four Strategy R&D worker operations."""

from __future__ import annotations

import asyncio
from datetime import date
from decimal import Decimal
from typing import cast
from zoneinfo import ZoneInfo

from openai import AsyncOpenAI
from sqlalchemy import func, select

from nexus.agents.codex_sdk import MODEL_ID as CODEX_MODEL_ID
from nexus.agents.responses import ResponsesClient, ResponsesModelConfig
from nexus.agents.responses_budget import UsageSnapshot
from nexus.backtest.engine import run_backtest, run_challenger_backtest_v1
from nexus.backtest.persistence import BacktestPersistenceAuthority, persist_backtest_result
from nexus.evolution.agent import ResponsesStrategyRNDRuntime, StrategyRNDRequestV1
from nexus.evolution.authority import (
    DatabaseStrategyRNDAuthorityResolver,
    LoadedCandidateAuthority,
    LoadedGenerationAuthority,
    StrategyRNDAuthorityError,
    build_strategy_handover_evaluation,
)
from nexus.evolution.codex import CODEX_RND_CONFIG_VERSION, CodexStrategyRNDRuntime
from nexus.evolution.evaluation import (
    EvolutionEvaluationError,
    load_lineage_oos_trial_count,
    persist_candidate_validation_result,
    persist_champion_comparison,
    persist_historical_promotion_decision,
    reserve_oos_exposure,
)
from nexus.evolution.jobs import StrategyRNDJobPayloadV1
from nexus.evolution.models import RegisteredMutationAxisV1, StrategyExperimentProposalV1
from nexus.evolution.registry import (
    RegistryServiceError,
    activate_pending_transition,
    persist_handover_promotion_decision,
    promote_internal_paper_champion,
)
from nexus.evolution.service import (
    EvolutionAdmissionAuthorityV1,
    EvolutionAdmissionError,
    admit_experiment_family,
    execute_strategy_rnd,
)
from nexus.evolution.service import (
    StrategyRNDAuthorityError as StrategyRNDAgentAuthorityError,
)
from nexus.evolution.shadow.service import (
    StrategyShadowServiceError,
    advance_strategy_shadow,
    finalize_strategy_shadow,
    start_strategy_shadow,
)
from nexus.evolution.validation import run_candidate_validation_v1
from nexus.jobs.registry import JobHandlerContext, JobHandlerResult
from nexus.persistence.models.backtest import (
    BacktestAuthorityBundleRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.evolution import (
    StrategyChampionComparisonRecord,
    StrategyExperimentCandidateProjectionRecord,
    StrategyPromotionDecisionRecord,
    StrategyPromotionPolicyRecord,
    StrategyRegistryHeadRecord,
    StrategyShadowFinalizationRecord,
    StrategyShadowGroupRecord,
    StrategyShadowSessionAuthorityRecord,
)
from nexus.quality.source_policy import current_strategy_rnd_adjusted_authority_available
from nexus.settings import AgentRuntimeMode, EngineSettings, Environment

_NEW_YORK = ZoneInfo("America/New_York")
_ADJUSTED_AUTHORITY_UNAVAILABLE = "BACKTEST_ADJUSTED_AUTHORITY_UNAVAILABLE"


def _authority_error_result(error: StrategyRNDAuthorityError) -> JobHandlerResult:
    if error.reason_code == _ADJUSTED_AUTHORITY_UNAVAILABLE:
        return JobHandlerResult(
            outcome="skipped",
            result_code=_ADJUSTED_AUTHORITY_UNAVAILABLE,
        )
    return JobHandlerResult(
        outcome="failed",
        result_code=None,
        error_code=error.reason_code,
    )


class StrategyRNDExecutor:
    """Execute only research, historical simulation, and internal Shadow operations."""

    def __init__(
        self,
        *,
        authority_resolver: DatabaseStrategyRNDAuthorityResolver,
        runtime_mode: AgentRuntimeMode,
        reasoning_model: str | None,
        responses_client: ResponsesClient | None,
    ) -> None:
        self._authority_resolver = authority_resolver
        self._runtime_mode = runtime_mode
        self._reasoning_model = reasoning_model
        self._responses_client = responses_client

    async def __call__(
        self,
        context: JobHandlerContext,
        payload: StrategyRNDJobPayloadV1,
    ) -> JobHandlerResult:
        if (
            self._runtime_mode is AgentRuntimeMode.CODEX_SDK
            and payload.operation == "generate_family"
            and context.claim.attempt_no != 1
        ):
            return JobHandlerResult(
                outcome="failed",
                result_code=None,
                error_code="GENERATION_REPLAY_FORBIDDEN",
            )
        if not current_strategy_rnd_adjusted_authority_available():
            return JobHandlerResult(
                outcome="skipped",
                result_code=_ADJUSTED_AUTHORITY_UNAVAILABLE,
            )
        if payload.operation == "generate_family":
            return await self._generate_family(context, payload)
        if payload.operation == "evaluate_candidate":
            return await self._evaluate_candidate(context, payload)
        if payload.operation == "advance_shadow":
            return await self._advance_shadow(context, payload)
        if payload.operation == "finalize_promotion":
            return await self._finalize_promotion(context, payload)
        raise ValueError("unregistered Strategy R&D operation")

    async def _generate_family(
        self,
        context: JobHandlerContext,
        payload: StrategyRNDJobPayloadV1,
    ) -> JobHandlerResult:
        now = context.finished_at()
        try:
            authority = await self._authority_resolver.load_generation(
                context.session,
                expected_generation=payload.expected_champion_generation,
                as_of=now,
                model_id=(
                    self._reasoning_model
                    if self._runtime_mode
                    in (AgentRuntimeMode.RESPONSES, AgentRuntimeMode.CODEX_SDK)
                    else None
                ),
                model_version=(
                    CODEX_RND_CONFIG_VERSION
                    if self._runtime_mode is AgentRuntimeMode.CODEX_SDK
                    else None
                ),
            )
            worker_deadline = getattr(context, "monotonic_deadline", lambda: None)()
            runtime = self._generation_runtime(authority, deadline=worker_deadline)
            execution = await execute_strategy_rnd(
                context.session,
                request=authority.request,
                tools=authority.tools,
                runtime=runtime,
                prompt_version_id=authority.prompt.id,
                model_config_id=authority.model.id,
                now=now,
            )
            if execution.status != "completed" or execution.proposal is None:
                return JobHandlerResult(
                    outcome="succeeded",
                    result_code=(
                        "GENERATION_ABSTAINED"
                        if execution.status == "abstained"
                        else "GENERATION_MODEL_FAILED"
                    ),
                )
            admitted = await admit_experiment_family(
                context.session,
                proposal=execution.proposal,
                authority=EvolutionAdmissionAuthorityV1(
                    schema_version="strategy_evolution_admission_authority_v1",
                    data_snapshot_id=authority.request.data_snapshot_id,
                    quality_report_id=authority.quality_report_id,
                    candidate_budget_policy_id=authority.candidate_budget_policy_id,
                    evolution_policy_id=authority.request.evolution_policy_id,
                    authority_hash=authority.baseline.stored_result.authority_hash,
                    protocol_hash=authority.baseline.stored_result.protocol_hash,
                    code_hash=authority.baseline.stored_result.code_hash,
                    baseline_backtest_run_id=authority.baseline.baseline_run_id,
                    rnd_output_id=execution.output_id,
                ),
                now=now,
                idempotency_key=(
                    f"rnd-g{authority.request.champion_generation}-"
                    f"{authority.baseline.baseline_run_id.hex[:24]}"
                ),
            )
            return JobHandlerResult(
                outcome="succeeded",
                result_code=(
                    "FAMILY_GENERATION_REPLAYED" if admitted.replayed else "FAMILY_GENERATED"
                ),
            )
        except StrategyRNDAuthorityError as error:
            return _authority_error_result(error)
        except EvolutionAdmissionError as error:
            return JobHandlerResult(
                outcome="failed", result_code=None, error_code=error.reason_code
            )
        except StrategyRNDAgentAuthorityError:
            return JobHandlerResult(
                outcome="failed",
                result_code=None,
                error_code="GENERATION_AUTHORITY_INCOMPLETE",
            )

    def _generation_runtime(
        self, authority: LoadedGenerationAuthority, *, deadline: float | None = None
    ) -> ResponsesStrategyRNDRuntime | CodexStrategyRNDRuntime | _FixedStrategyRNDRuntime:
        if (
            self._runtime_mode is not AgentRuntimeMode.CODEX_SDK
            and authority.model.version == CODEX_RND_CONFIG_VERSION
        ):
            raise StrategyRNDAuthorityError("GENERATION_RUNTIME_INCOMPLETE")
        if self._runtime_mode is AgentRuntimeMode.FIXTURE:
            return _FixedStrategyRNDRuntime(config=_model_configuration(authority))
        if self._runtime_mode is AgentRuntimeMode.RESPONSES:
            if self._responses_client is None or not self._reasoning_model:
                raise StrategyRNDAuthorityError("GENERATION_RUNTIME_INCOMPLETE")
            return ResponsesStrategyRNDRuntime(
                client=self._responses_client,
                config=_model_configuration(authority),
            )
        if self._runtime_mode is AgentRuntimeMode.CODEX_SDK:
            config = _model_configuration(authority)
            if deadline is not None:
                remaining = deadline - asyncio.get_running_loop().time() - 5
                if remaining <= 0:
                    raise StrategyRNDAuthorityError("GENERATION_RUNTIME_INCOMPLETE")
                config = config.model_copy(
                    update={
                        "absolute_timeout_seconds": min(
                            config.absolute_timeout_seconds, Decimal(str(remaining))
                        )
                    }
                )
            try:
                return CodexStrategyRNDRuntime(
                    config=config,
                    monotonic_deadline=None if deadline is None else deadline - 5,
                )
            except ValueError as error:
                raise StrategyRNDAuthorityError("GENERATION_RUNTIME_INCOMPLETE") from error
        raise StrategyRNDAuthorityError("GENERATION_RUNTIME_UNSUPPORTED")

    async def _evaluate_candidate(
        self,
        context: JobHandlerContext,
        payload: StrategyRNDJobPayloadV1,
    ) -> JobHandlerResult:
        assert payload.family_id is not None
        assert payload.experiment_id is not None
        try:
            authority = await self._authority_resolver.load_candidate(
                context.session,
                family_id=payload.family_id,
                experiment_id=payload.experiment_id,
                expected_generation=payload.expected_champion_generation,
                expected_state=payload.candidate_state,
                expected_version=payload.candidate_version,
                as_of=context.finished_at(),
            )
            if authority.projection.current_state == "ADMITTED":
                return await self._validate_candidate(context, authority)
            if authority.projection.current_state == "OOS_SELECTED":
                reservation = await reserve_oos_exposure(
                    context.session,
                    experiment_id=authority.experiment.id,
                    now=context.finished_at(),
                )
                return JobHandlerResult(
                    outcome="succeeded",
                    result_code=(
                        "OOS_EXPOSURE_REPLAYED" if reservation.replayed else "OOS_EXPOSURE_RESERVED"
                    ),
                )
            if authority.projection.current_state == "OOS_RESERVED":
                return await self._run_oos(context, authority)
            return JobHandlerResult(outcome="skipped", result_code="CANDIDATE_ALREADY_EVALUATED")
        except StrategyRNDAuthorityError as error:
            return _authority_error_result(error)
        except EvolutionEvaluationError as error:
            return JobHandlerResult(
                outcome="failed",
                result_code=None,
                error_code=error.reason_code,
            )

    async def _validate_candidate(
        self,
        context: JobHandlerContext,
        authority: LoadedCandidateAuthority,
    ) -> JobHandlerResult:
        report = run_candidate_validation_v1(
            authority.strategy,
            authority.baseline.data,
            authority.baseline.protocol,
        )
        await persist_candidate_validation_result(
            context.session,
            experiment_id=authority.experiment.id,
            validation_sharpe=report.validation_sharpe,
            validation_max_drawdown=report.validation_max_drawdown,
            validation_cost=report.validation_cost,
            valid_window_count=report.valid_window_count,
            authority_complete=report.authority_complete,
            authority_hash=authority.family.authority_hash,
            protocol_hash=authority.family.protocol_hash,
            code_hash=authority.family.code_hash,
            now=context.finished_at(),
        )
        return JobHandlerResult(
            outcome="succeeded",
            result_code=(
                "CANDIDATE_VALIDATED"
                if report.authority_complete
                else "CANDIDATE_VALIDATION_INCOMPLETE"
            ),
        )

    async def _run_oos(
        self,
        context: JobHandlerContext,
        authority: LoadedCandidateAuthority,
    ) -> JobHandlerResult:
        remaining_reservations = int(
            await context.session.scalar(
                select(func.count())
                .select_from(StrategyExperimentCandidateProjectionRecord)
                .where(
                    StrategyExperimentCandidateProjectionRecord.family_id == authority.family.id,
                    StrategyExperimentCandidateProjectionRecord.current_state == "OOS_SELECTED",
                )
            )
            or 0
        )
        if remaining_reservations:
            return JobHandlerResult(outcome="skipped", result_code="OOS_RESERVATIONS_PENDING")
        trial_count = await load_lineage_oos_trial_count(
            context.session, authority.family.research_lineage_id
        )
        paired_protocol = authority.baseline.protocol.model_copy(
            update={"experiment_attempt_count": max(trial_count, 1)}
        )
        champion_result = run_backtest(
            authority.baseline.strategy,
            authority.baseline.data,
            paired_protocol,
        )
        challenger_result = run_challenger_backtest_v1(
            authority.strategy,
            authority.baseline.data,
            paired_protocol,
            experiment_attempt_count=max(trial_count, 1),
        )
        bundle = await context.session.get(
            BacktestAuthorityBundleRecord,
            authority.baseline.data.authority.authority_bundle_id,
        )
        if bundle is None:
            raise StrategyRNDAuthorityError("BACKTEST_AUTHORITY_INCOMPLETE")
        persistence_authority = BacktestPersistenceAuthority(
            authority_bundle_id=bundle.id,
            data_snapshot_id=bundle.data_snapshot_id,
            source_policy_id=bundle.source_policy_id,
            cost_policy_id=bundle.cost_policy_id,
            completed_at=context.finished_at(),
            data_cutoff_at=bundle.completed_at,
        )
        champion_run = await persist_backtest_result(
            context.session,
            authority.baseline.strategy,
            authority.baseline.data,
            paired_protocol,
            champion_result,
            persistence_authority,
        )
        challenger_run = await persist_backtest_result(
            context.session,
            authority.strategy,
            authority.baseline.data,
            paired_protocol,
            challenger_result,
            persistence_authority,
        )
        comparison = await persist_champion_comparison(
            context.session,
            experiment_id=authority.experiment.id,
            champion_backtest_run_id=champion_run.id,
            challenger_backtest_run_id=challenger_run.id,
            now=context.finished_at(),
        )
        policy_id = await context.session.scalar(
            select(StrategyPromotionPolicyRecord.id)
            .where(StrategyPromotionPolicyRecord.version == "promotion_policy_v1")
            .limit(1)
        )
        if policy_id is None:
            raise StrategyRNDAuthorityError("PROMOTION_POLICY_INCOMPLETE")
        decision = await persist_historical_promotion_decision(
            context.session,
            comparison_id=comparison.id,
            promotion_policy_id=policy_id,
            now=context.finished_at(),
        )
        if decision.status == "PASS":
            await start_strategy_shadow(
                context.session,
                experiment_id=authority.experiment.id,
                now=context.finished_at(),
            )
            return JobHandlerResult(outcome="succeeded", result_code="STRATEGY_SHADOW_STARTED")
        return JobHandlerResult(
            outcome="succeeded",
            result_code=f"HISTORICAL_GATE_{decision.status}",
        )

    async def _advance_shadow(
        self,
        context: JobHandlerContext,
        payload: StrategyRNDJobPayloadV1,
    ) -> JobHandlerResult:
        assert payload.experiment_id is not None
        try:
            group = await self._shadow_group(context, payload)
            completed = int(
                await context.session.scalar(
                    select(func.count())
                    .select_from(StrategyShadowSessionAuthorityRecord)
                    .where(StrategyShadowSessionAuthorityRecord.group_id == group.id)
                )
                or 0
            )
            expected_session = await context.session.scalar(
                select(ExchangeCalendarSessionRecord.session)
                .where(
                    ExchangeCalendarSessionRecord.calendar_authority_id
                    == group.calendar_authority_id,
                    ExchangeCalendarSessionRecord.session
                    > group.created_at.astimezone(_NEW_YORK).date(),
                )
                .order_by(ExchangeCalendarSessionRecord.session)
                .offset(completed)
                .limit(1)
            )
            if expected_session is None:
                return JobHandlerResult(
                    outcome="failed",
                    result_code=None,
                    error_code="STRATEGY_SHADOW_CALENDAR_INCOMPLETE",
                )
            result = await advance_strategy_shadow(
                context.session,
                group_id=group.id,
                requested_session=expected_session,
                observed_at=context.finished_at(),
            )
            return JobHandlerResult(outcome="succeeded", result_code=result.reason_code)
        except StrategyShadowServiceError as error:
            return JobHandlerResult(
                outcome="failed", result_code=None, error_code=error.reason_code
            )

    async def _finalize_promotion(
        self,
        context: JobHandlerContext,
        payload: StrategyRNDJobPayloadV1,
    ) -> JobHandlerResult:
        try:
            group = await self._shadow_group(context, payload)
            now = context.finished_at()
            result = await finalize_strategy_shadow(context.session, group_id=group.id, now=now)
            if result.status != "PASS":
                return JobHandlerResult(
                    outcome="succeeded",
                    result_code=f"STRATEGY_SHADOW_{result.status}",
                )
            comparison = await context.session.scalar(
                select(StrategyChampionComparisonRecord).where(
                    StrategyChampionComparisonRecord.experiment_id == group.experiment_id
                )
            )
            finalization = await context.session.get(
                StrategyShadowFinalizationRecord, result.finalization_id
            )
            if comparison is None or finalization is None:
                raise StrategyRNDAuthorityError("HANDOVER_AUTHORITY_INCOMPLETE")
            handover = await context.session.scalar(
                select(StrategyPromotionDecisionRecord).where(
                    StrategyPromotionDecisionRecord.comparison_id == comparison.id,
                    StrategyPromotionDecisionRecord.stage == "handover",
                )
            )
            if handover is None:
                evaluation = await build_strategy_handover_evaluation(
                    context.session,
                    group=group,
                    now=now,
                )
                handover = await persist_handover_promotion_decision(
                    context.session,
                    comparison_id=comparison.id,
                    promotion_policy_id=finalization.promotion_policy_id,
                    evaluation=evaluation,
                    now=now,
                )
            if handover.status != "PASS":
                return JobHandlerResult(
                    outcome="succeeded",
                    result_code=f"HANDOVER_GATE_{handover.status}",
                )
            head = await context.session.get(StrategyRegistryHeadRecord, "internal_paper")
            last_shadow_session = await self._last_shadow_session(context, group)
            effective_session = await context.session.scalar(
                select(ExchangeCalendarSessionRecord.session)
                .where(
                    ExchangeCalendarSessionRecord.calendar_authority_id
                    == group.calendar_authority_id,
                    ExchangeCalendarSessionRecord.session > last_shadow_session,
                )
                .order_by(ExchangeCalendarSessionRecord.session)
                .limit(1)
            )
            if head is None or effective_session is None:
                raise StrategyRNDAuthorityError("REGISTRY_AUTHORITY_INCOMPLETE")
            transition = await promote_internal_paper_champion(
                context.session,
                experiment_id=group.experiment_id,
                expected_generation=head.active_generation,
                expected_version=head.version,
                effective_session=effective_session,
                idempotency_key=f"strategy-rnd-promote-{group.experiment_id}",
                now=now,
            )
            if (
                transition.status == "PENDING_BOUNDARY"
                and now.astimezone(_NEW_YORK).date() >= effective_session
            ):
                transition = await activate_pending_transition(
                    context.session,
                    expected_generation=transition.active_generation,
                    expected_version=transition.version,
                    decision_session=now.astimezone(_NEW_YORK).date(),
                    now=now,
                )
            return JobHandlerResult(outcome="succeeded", result_code=transition.reason_code)
        except (
            RegistryServiceError,
            StrategyRNDAuthorityError,
            StrategyShadowServiceError,
        ) as error:
            return JobHandlerResult(
                outcome="failed", result_code=None, error_code=error.reason_code
            )

    async def _last_shadow_session(
        self,
        context: JobHandlerContext,
        group: StrategyShadowGroupRecord,
    ) -> date:
        value = await context.session.scalar(
            select(StrategyShadowSessionAuthorityRecord.session).where(
                StrategyShadowSessionAuthorityRecord.group_id == group.id,
                StrategyShadowSessionAuthorityRecord.session_ordinal == group.evaluation_sessions,
            )
        )
        if value is None:
            raise StrategyRNDAuthorityError("HANDOVER_AUTHORITY_INCOMPLETE")
        return value

    async def _shadow_group(
        self,
        context: JobHandlerContext,
        payload: StrategyRNDJobPayloadV1,
    ) -> StrategyShadowGroupRecord:
        assert payload.experiment_id is not None
        group = await context.session.scalar(
            select(StrategyShadowGroupRecord).where(
                StrategyShadowGroupRecord.experiment_id == payload.experiment_id
            )
        )
        if group is None or group.champion_generation != payload.expected_champion_generation:
            raise StrategyShadowServiceError("STRATEGY_SHADOW_AUTHORITY_INCOMPLETE")
        finalization = await context.session.scalar(
            select(StrategyShadowFinalizationRecord).where(
                StrategyShadowFinalizationRecord.group_id == group.id
            )
        )
        if payload.operation == "advance_shadow" and finalization is not None:
            raise StrategyShadowServiceError("STRATEGY_SHADOW_ALREADY_FINALIZED")
        return group


def build_strategy_rnd_executor(
    settings: EngineSettings,
    responses_client: object | None = None,
) -> StrategyRNDExecutor:
    """Build only supported runtimes; a local model selection must never become a fixture."""
    if settings.environment is Environment.PRODUCTION and (
        settings.agent_runtime_mode is not AgentRuntimeMode.RESPONSES
        or settings.openai_api_key is None
        or not settings.openai_reasoning_model
    ):
        raise ValueError("production Strategy R&D requires explicit Responses authority")
    runtime_mode = settings.agent_runtime_mode
    if runtime_mode not in (
        AgentRuntimeMode.FIXTURE,
        AgentRuntimeMode.RESPONSES,
        AgentRuntimeMode.CODEX_SDK,
    ):
        raise ValueError("Strategy R&D runtime requires an explicit supported mode")
    client = responses_client
    if runtime_mode is AgentRuntimeMode.CODEX_SDK:
        client = None
    if runtime_mode is AgentRuntimeMode.RESPONSES and not settings.openai_reasoning_model:
        raise ValueError("Responses Strategy R&D requires a reasoning model")
    if runtime_mode is AgentRuntimeMode.RESPONSES and client is None:
        if settings.openai_api_key is None:
            raise ValueError("Responses Strategy R&D requires an API key")
        client = AsyncOpenAI(api_key=settings.openai_api_key.get_secret_value())
    reasoning_model = settings.openai_reasoning_model
    if runtime_mode is AgentRuntimeMode.CODEX_SDK:
        reasoning_model = CODEX_MODEL_ID
    return StrategyRNDExecutor(
        authority_resolver=DatabaseStrategyRNDAuthorityResolver(),
        runtime_mode=runtime_mode,
        reasoning_model=reasoning_model,
        responses_client=(None if client is None else cast(ResponsesClient, client)),
    )


def _model_configuration(authority: LoadedGenerationAuthority) -> ResponsesModelConfig:
    model = authority.model
    return ResponsesModelConfig(
        model_id=model.model_id,
        pricing_version=model.version,
        max_input_tokens=model.max_input_tokens,
        max_output_tokens=model.max_output_tokens,
        max_tool_calls=model.max_tool_calls,
        # The Job worker owns a hard 45-second deadline. Leave deterministic
        # headroom for authority loading, persistence, and terminalization.
        absolute_timeout_seconds=Decimal("30"),
        input_usd_per_million=model.input_usd_per_million,
        output_usd_per_million=model.output_usd_per_million,
    )


class _FixedStrategyRNDRuntime:
    """Deterministic non-network runtime for the explicit fixture mode only."""

    def __init__(self, *, config: ResponsesModelConfig) -> None:
        self._configuration = config

    @property
    def configuration(self) -> ResponsesModelConfig:
        return self._configuration

    @property
    def usage(self) -> UsageSnapshot:
        return UsageSnapshot()

    async def generate(
        self, request: StrategyRNDRequestV1, tools: object
    ) -> StrategyExperimentProposalV1:
        del tools
        return StrategyExperimentProposalV1(
            schema_version="strategy_experiment_proposal_v1",
            parent_strategy_definition_id=request.parent_strategy_definition_id,
            champion_generation=request.champion_generation,
            research_lineage_id=request.research_lineage_id,
            hypothesis=(
                "Registered liquidity, price, and weight thresholds may improve robustness."
            ),
            expected_mechanism=(
                "A bounded grid tests whether tradability filters and sizing remain stable."
            ),
            invalidation_conditions=(
                "Frozen validation and paired comparisons do not improve robustness.",
            ),
            evidence_refs=request.evidence_refs,
            mutation_axes=(
                RegisteredMutationAxisV1(
                    axis="min_adv20_usd",
                    values=(
                        "25000000",
                        "50000000",
                        "75000000",
                        "100000000",
                    ),
                ),
                RegisteredMutationAxisV1(axis="min_close", values=("3", "5", "10")),
                RegisteredMutationAxisV1(axis="target_weight", values=("0.075", "0.10", "0.125")),
            ),
        )
