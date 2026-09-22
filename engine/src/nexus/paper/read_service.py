"""Redacted PostgreSQL read models for the private internal-paper console."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Literal, cast
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import and_, desc, func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.api.errors import ErrorCode, NexusError
from nexus.api.schemas.paper import (
    AgentPaperActivationResultResponse,
    AgentPaperAlpacaExecutionMarketResponse,
    AgentPaperCandidateBatchResponse,
    AgentPaperCandidateResponse,
    AgentPaperCycleBlockerResponse,
    AgentPaperCycleDetailResponse,
    AgentPaperCyclePageResponse,
    AgentPaperCycleResponse,
    AgentPaperDecisionMarketResponse,
    AgentPaperDecisionResponse,
    AgentPaperManagementCycleResponse,
    AgentPaperPortfolioProposalResponse,
    AgentPaperProposalTargetResponse,
    AgentPaperSchwabExecutionMarketResponse,
    AgentPaperSecurityResearchResponse,
    AgentPaperSessionSummaryResponse,
    AgentPaperSynthesisChoiceResponse,
    AgentPaperSynthesisResponse,
    AgentPaperVersionObservationResponse,
    AgentVersionComparisonResponse,
    AgentVersionEvaluationResponse,
    AgentVersionGateResponse,
    AgentVersionLaneMetricsResponse,
    AgentVersionSlotLaneResponse,
    AgentVersionSlotResponse,
    PaperAttributionResponse,
    PaperCancelOrderFactResponse,
    PaperCancelReservationFactResponse,
    PaperCancelResultResponse,
    PaperComparisonResponse,
    PaperComparisonRowResponse,
    PaperFillResponse,
    PaperManagementSummaryResponse,
    PaperOrderEventResponse,
    PaperOrderPageResponse,
    PaperOrderResponse,
    PaperPerformanceResponse,
    PaperPlanActualPositionResponse,
    PaperPlanActualResponse,
    PaperPlanConfirmedFillResponse,
    PaperPlanFactsResponse,
    PaperPlanItemResponse,
    PaperPlanProposalResponse,
    PaperPlanProposalTargetResponse,
    PaperPlanResponse,
    PaperPlanRiskResponse,
    PaperPlanSummaryResponse,
    PaperPlanTargetResponse,
    PaperPortfolioResponse,
    PaperPositionResponse,
    PaperTargetAllocationResponse,
    PaperTargetSummaryResponse,
)
from nexus.domain.decimal import decimal_to_string
from nexus.domain.time import require_utc
from nexus.market.universe import stable_instrument_id
from nexus.paper.agent_cycle_models import (
    CandidateManifestEntry,
    parse_candidate_manifest,
)
from nexus.paper.cursor import PaperOrderCursorCodec, PaperOrderCursorState
from nexus.paper.execution_canonical import (
    canonical_cancel_request_hash,
    canonical_cancel_result_hash,
    canonical_paper_fact_hash,
)
from nexus.paper.management_read_authority import (
    SynthesisReadAuthority,
    load_candidate_batch_authority,
    load_proposal_read_authority,
    load_synthesis_read_authority,
    load_target_read_authority,
)
from nexus.paper.models import (
    InternalOrderStatus,
    OrderEventInput,
    PaperOrder,
    Reservation,
    ReservationEvent,
)
from nexus.paper.orders import current_order_status, fold_reservation_events
from nexus.paper.portfolio_policy import PORTFOLIO_ALLOCATION_V1
from nexus.paper.portfolio_proposal import canonical_portfolio_target_content_hash
from nexus.paper.projection import load_paper_projection
from nexus.persistence.models.agent_versions import (
    AgentVersionCyclePairRecord,
    AgentVersionEvaluationDecisionRecord,
    AgentVersionEvaluationRecord,
)
from nexus.persistence.models.agents import (
    AgentPortfolioSynthesisOutputRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
    StrategyDefinitionRecord,
)
from nexus.persistence.models.data import (
    DataArtifact,
    PriceBarRecord,
    PriceBarSetAuthority,
    PriceSeriesManifest,
)
from nexus.persistence.models.evolution import StrategyRegistryEventRecord
from nexus.persistence.models.market import InstrumentAlias, LatestQuote
from nexus.persistence.models.paper import (
    PAPER_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
    AgentPaperCycleEventRecord,
    AgentPaperCycleRecord,
    AgentPaperMarketWindowRecord,
    PaperAccountRecord,
    PaperExecutionWindowAuthorityRecord,
    PaperFillRecord,
    PaperLedgerTransactionRecord,
    PaperOrderBatchRecord,
    PaperOrderEventRecord,
    PaperOrderRecord,
    PaperPerformanceAttributionRecord,
    PaperPerformanceSnapshotRecord,
    PaperPositionSnapshotRecord,
    PaperRequestIdempotencyRecord,
    PaperReservationEventRecord,
    PaperReservationRecord,
    PaperTargetVersionRecord,
    RiskEvaluationRecord,
    RiskNoChangeEvaluationRecord,
    RiskPolicyRecord,
    StrategyDecisionAgentOutputRecord,
    StrategyDecisionRecord,
    StrategyDecisionTargetRecord,
)
from nexus.persistence.models.portfolio import Account, Portfolio

_MAX_POSITIONS = 500
_MAX_ATTRIBUTIONS = 500
_MAX_EVENTS = 128
_MAX_FILLS = 128
_MAX_AGENT_CYCLES_PER_SESSION = 108
_MAX_AGENT_ORDERS_PER_CYCLE = 50
_STALE_AFTER = timedelta(hours=36)


def portfolio_target_matches_batch(
    *,
    account_id: UUID,
    portfolio_id: UUID,
    batch_decision_id: UUID,
    batch_decision_hash: str,
    proposal_decision_id: UUID,
    proposal_decision_hash: str,
    proposal_target_content_hash: str,
    target_account_id: UUID,
    target_portfolio_id: UUID,
    target_id: UUID,
    expected_target_id: UUID,
    target_decision_id: UUID,
    target_proposal_decision_id: UUID,
    target_content_hash: str,
) -> bool:
    """Validate a batch against target content without conflating decision identities."""
    return (
        account_id == target_account_id
        and portfolio_id == target_portfolio_id
        and batch_decision_id == proposal_decision_id
        and batch_decision_hash == proposal_decision_hash
        and target_id == expected_target_id
        and target_content_hash == proposal_target_content_hash
        and target_decision_id == target_proposal_decision_id
    )


def market_benchmark_row_from_closes(
    *, initial_capital: Decimal, closes: Sequence[Decimal]
) -> PaperComparisonRowResponse:
    """Build the passive SPY row from a complete persisted close path."""
    if len(closes) < 2 or initial_capital <= 0 or any(close <= 0 for close in closes):
        return PaperComparisonRowResponse(
            key="market_benchmark",
            data_state="UNAVAILABLE",
            nav=None,
            total_return_ratio=None,
            max_drawdown_ratio=None,
            turnover_ratio=None,
            total_cost=None,
            reason_code="SAME_WINDOW_FORWARD_FACTS_UNAVAILABLE",
        )
    peak = closes[0]
    max_drawdown = Decimal(0)
    for close in closes:
        peak = max(peak, close)
        max_drawdown = max(max_drawdown, Decimal(1) - close / peak)
    total_return = closes[-1] / closes[0] - Decimal(1)
    return PaperComparisonRowResponse(
        key="market_benchmark",
        data_state="READY",
        nav=decimal_to_string(initial_capital * (Decimal(1) + total_return)),
        total_return_ratio=decimal_to_string(total_return),
        max_drawdown_ratio=decimal_to_string(max_drawdown),
        turnover_ratio=decimal_to_string(Decimal(0)),
        total_cost=decimal_to_string(Decimal(0)),
        reason_code="PERSISTED_MARKET_BENCHMARK",
    )


class PaperReadService:
    def __init__(
        self,
        session: AsyncSession,
        request_time: datetime,
        *,
        cursor_signing_secrets: Sequence[str] = (),
    ) -> None:
        self._session = session
        self._request_time = require_utc(request_time)
        self._cursor_secrets = tuple(cursor_signing_secrets)

    async def account(self) -> tuple[PaperAccountRecord, Portfolio]:
        rows = (
            await self._session.execute(
                select(PaperAccountRecord, Portfolio)
                .join(Portfolio, Portfolio.id == PaperAccountRecord.portfolio_id)
                .join(Account, Account.id == Portfolio.account_id)
                .where(
                    PaperAccountRecord.capability == "simulation_write_internal",
                    Account.provider != "internal_paper_evaluation",
                )
                .order_by(PaperAccountRecord.id)
                .limit(2)
            )
        ).all()
        if len(rows) != 1:
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The internal paper account is unavailable.",
                status_code=409,
            )
        return rows[0][0], rows[0][1]

    async def latest_agent_cycle(self) -> AgentPaperCycleResponse | None:
        account, _ = await self.account()
        cycle = await self._session.scalar(
            select(AgentPaperCycleRecord)
            .where(
                AgentPaperCycleRecord.account_id == account.id,
                AgentPaperCycleRecord.decision_time <= self._request_time,
                AgentPaperCycleRecord.created_at <= self._request_time,
            )
            .order_by(
                AgentPaperCycleRecord.decision_slot_at.desc(),
                AgentPaperCycleRecord.id.desc(),
            )
            .limit(1)
        )
        if cycle is None:
            return None
        return await self._agent_cycle(cycle)

    async def agent_cycle_detail(self, cycle_id: UUID) -> AgentPaperCycleDetailResponse | None:
        account, _ = await self.account()
        cycle = await self._session.scalar(
            select(AgentPaperCycleRecord).where(
                AgentPaperCycleRecord.id == cycle_id,
                AgentPaperCycleRecord.account_id == account.id,
                AgentPaperCycleRecord.decision_time <= self._request_time,
                AgentPaperCycleRecord.created_at <= self._request_time,
            )
        )
        if cycle is None:
            return None
        return AgentPaperCycleDetailResponse(
            cycle=await self._agent_cycle(cycle),
            orders=await self._orders_for_cycle(cycle_id),
        )

    async def management_cycle_detail(
        self, cycle_id: UUID
    ) -> AgentPaperManagementCycleResponse | None:
        account, _ = await self.account()
        cycle = await self._session.scalar(
            select(AgentPaperCycleRecord).where(
                AgentPaperCycleRecord.id == cycle_id,
                AgentPaperCycleRecord.account_id == account.id,
                AgentPaperCycleRecord.decision_time <= self._request_time,
                AgentPaperCycleRecord.created_at <= self._request_time,
            )
        )
        if cycle is None:
            return None
        cycle_response = await self._agent_cycle(cycle)
        try:
            batch_authority = await load_candidate_batch_authority(
                self._session,
                cycle=cycle,
                read_as_of=self._request_time,
            )
            synthesis_authority = await load_synthesis_read_authority(
                self._session,
                cycle=cycle,
                read_as_of=self._request_time,
            )
        except ValueError as error:
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The Agent paper management authority is incomplete.",
                status_code=409,
            ) from error
        candidate_batch = None
        if batch_authority is not None:
            candidate_batch = AgentPaperCandidateBatchResponse(
                candidate_batch_id=batch_authority.batch_id,
                universe_snapshot_id=batch_authority.universe_snapshot_id,
                candidate_instrument_ids=batch_authority.instrument_ids,
                source="explicit_research_scope",
                batch_hash=batch_authority.batch_hash,
                as_of=batch_authority.created_at,
            )
        security_research = await self._portfolio_security_research(synthesis_authority)
        synthesis = self._portfolio_synthesis(synthesis_authority)
        decision = await self._session.scalar(
            select(StrategyDecisionRecord).where(
                StrategyDecisionRecord.agent_paper_cycle_id == cycle.id,
                StrategyDecisionRecord.decision_origin == "agent_research",
                StrategyDecisionRecord.created_at <= self._request_time,
            )
        )
        proposal = await self._portfolio_proposal(decision)
        active_target = await self._current_target_summary(cycle.account_id)
        latest_event = await self._latest_cycle_event(cycle.id)
        activation_event = await self._cycle_activation_event(cycle.id)
        activation_facts = (
            {}
            if activation_event is None or not isinstance(activation_event.fact_ids, dict)
            else activation_event.fact_ids
        )
        activated_target_id = _optional_uuid(activation_facts.get("target_version_id"))
        cycle_target_row = None
        if (
            activation_event is not None
            and activation_event.result_code != "TARGET_ACTIVATION_BLOCKED"
        ):
            identity_filter = None
            if activated_target_id is not None:
                identity_filter = PaperTargetVersionRecord.id == activated_target_id
            elif decision is not None:
                identity_filter = PaperTargetVersionRecord.strategy_decision_id == decision.id
            if identity_filter is not None:
                cycle_target_row = await self._session.scalar(
                    select(PaperTargetVersionRecord).where(
                        PaperTargetVersionRecord.account_id == cycle.account_id,
                        identity_filter,
                        PaperTargetVersionRecord.activated_at <= self._request_time,
                        PaperTargetVersionRecord.created_at <= self._request_time,
                    )
                )
        cycle_target = (
            None
            if cycle_target_row is None
            else await self._target_summary(cycle_target_row, account_id=cycle.account_id)
        )
        batch = (
            None
            if decision is None
            else await self._session.scalar(
                select(PaperOrderBatchRecord).where(
                    PaperOrderBatchRecord.account_id == cycle.account_id,
                    PaperOrderBatchRecord.strategy_decision_id == decision.id,
                    PaperOrderBatchRecord.created_at <= self._request_time,
                )
            )
        )
        plan_summary = None if batch is None else await self._plan_summary(batch.id)
        blocking_plan = await self._blocking_plan_summary(activation_event)
        return AgentPaperManagementCycleResponse(
            cycle=cycle_response,
            orders=await self._orders_for_cycle(cycle_id),
            candidate_batch=candidate_batch,
            security_research=security_research,
            synthesis=synthesis,
            proposal=proposal,
            active_target=active_target,
            activation_result=self._activation_result(
                event=activation_event,
                decision=decision,
                target=cycle_target,
                batch=batch,
                strict_identity=cycle.candidate_batch_id is not None,
            ),
            blocker=self._cycle_blocker(
                event=latest_event,
                synthesis_output=(
                    None if synthesis_authority is None else synthesis_authority.output
                ),
                decision=decision,
            ),
            blocking_plan=blocking_plan,
            plan_summary=plan_summary,
        )

    async def _latest_cycle_event(self, cycle_id: UUID) -> AgentPaperCycleEventRecord:
        event = await self._session.scalar(
            select(AgentPaperCycleEventRecord)
            .where(
                AgentPaperCycleEventRecord.cycle_id == cycle_id,
                AgentPaperCycleEventRecord.occurred_at <= self._request_time,
                AgentPaperCycleEventRecord.created_at <= self._request_time,
            )
            .order_by(AgentPaperCycleEventRecord.sequence.desc())
            .limit(1)
        )
        if event is None:
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The Agent paper cycle is incomplete.",
                status_code=409,
            )
        return event

    async def _cycle_activation_event(
        self, cycle_id: UUID
    ) -> AgentPaperCycleEventRecord | None:
        return cast(
            AgentPaperCycleEventRecord | None,
            await self._session.scalar(
                select(AgentPaperCycleEventRecord)
                .where(
                    AgentPaperCycleEventRecord.cycle_id == cycle_id,
                    AgentPaperCycleEventRecord.result_code.in_(
                        (
                            "TARGET_ACTIVATION_BLOCKED",
                            "PAPER_BATCH_EVALUATED",
                            "NO_ORDER_REQUIRED",
                        )
                    ),
                    AgentPaperCycleEventRecord.occurred_at <= self._request_time,
                    AgentPaperCycleEventRecord.created_at <= self._request_time,
                )
                .order_by(AgentPaperCycleEventRecord.sequence.desc())
                .limit(1)
            )
        )

    async def _portfolio_security_research(
        self, authority: SynthesisReadAuthority | None
    ) -> tuple[AgentPaperSecurityResearchResponse, ...]:
        if authority is None:
            return ()
        results = []
        for source in authority.frozen.security_outputs:
            output = await self._session.get(AgentResearchOutputRecord, source.research_output_id)
            research = (
                None
                if output is None
                else await self._session.get(AgentResearchRunRecord, output.run_id)
            )
            if (
                output is None
                or research is None
                or research.instrument_id != source.instrument_id
                or output.status != "completed"
                or output.content_hash != source.content_hash
                or output.created_at > self._request_time
            ):
                raise NexusError(
                    ErrorCode.DATA_PARTIAL,
                    "Portfolio research is incomplete.",
                    status_code=409,
                )
            results.append(
                AgentPaperSecurityResearchResponse(
                    instrument_id=source.instrument_id,
                    research_run_id=research.id,
                    research_output_id=output.id,
                    status="completed",
                    content_hash=output.content_hash,
                    intent_hash=source.intent_hash,
                    stance=source.intent.stance,
                    quality_status=source.intent.quality_status,
                    as_of=output.created_at,
                )
            )
        return tuple(results)

    def _portfolio_synthesis(
        self,
        authority: SynthesisReadAuthority | None,
    ) -> AgentPaperSynthesisResponse | None:
        if authority is None:
            return None
        run = authority.run
        output = authority.output
        intent = authority.intent
        choices = (
            ()
            if intent is None
            else tuple(
                AgentPaperSynthesisChoiceResponse(
                    instrument_id=choice.instrument_id,
                    action=choice.action,
                    role=choice.role,
                    rank=choice.rank,
                    replaces_instrument_id=choice.replaces_instrument_id,
                    depends_on_instrument_ids=choice.depends_on_instrument_ids,
                    thesis=choice.thesis.text,
                    counter_thesis=choice.counter_thesis.text,
                    invalidation=choice.invalidation.text,
                )
                for choice in intent.choices
            )
        )
        return AgentPaperSynthesisResponse(
            synthesis_run_id=run.id,
            synthesis_output_id=output.id,
            candidate_batch_id=run.candidate_batch_id,
            input_hash=run.input_hash,
            content_hash=output.content_hash,
            binding_hash=output.binding_hash,
            status=cast(Any, output.status),
            choices=choices,
            cash_rationale=None if intent is None else intent.cash_rationale.text,
            as_of=output.created_at,
        )

    async def _portfolio_proposal(
        self,
        decision: StrategyDecisionRecord | None,
    ) -> AgentPaperPortfolioProposalResponse | None:
        if decision is None or decision.strategy_version != "agent_portfolio_proposal_v1":
            return None
        try:
            authority = await load_proposal_read_authority(
                self._session,
                account_id=decision.account_id,
                decision_id=decision.id,
                read_as_of=self._request_time,
            )
        except ValueError as error:
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "Portfolio proposal authority is incomplete.",
                status_code=409,
            ) from error
        if authority is None:
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "Portfolio proposal authority is incomplete.",
                status_code=409,
            )
        proposal = authority.persisted.proposal
        choices = {choice.instrument_id: choice for choice in proposal.synthesis.choices}
        return AgentPaperPortfolioProposalResponse(
            strategy_decision_id=decision.id,
            synthesis_output_id=proposal.synthesis_output_id,
            decision_hash=proposal.decision_hash,
            policy_hash=proposal.policy_hash,
            baseline_target_version_id=proposal.baseline.target_version_id,
            initial_approval_id=proposal.baseline.initial_approval_id,
            cash_weight=decimal_to_string(proposal.cash_weight),
            targets=tuple(
                AgentPaperProposalTargetResponse(
                    instrument_id=instrument_id,
                    target_weight=decimal_to_string(proposal.weights[instrument_id]),
                    exit=proposal.weights[instrument_id] == 0,
                    role=(
                        choices[instrument_id].role
                        if instrument_id in choices
                        else proposal.roles.get(instrument_id, "none")
                    ),
                    reason_code=proposal.reasons[instrument_id],
                    rank=(
                        choices[instrument_id].rank if instrument_id in choices else None
                    ),
                    replaces_instrument_id=(
                        choices[instrument_id].replaces_instrument_id
                        if instrument_id in choices
                        else None
                    ),
                    depends_on_instrument_ids=(
                        choices[instrument_id].depends_on_instrument_ids
                        if instrument_id in choices
                        else ()
                    ),
                )
                for instrument_id in sorted(proposal.weights, key=lambda value: value.int)
            ),
            expires_at=proposal.expires_at,
            as_of=decision.created_at,
        )

    async def _current_target_summary(self, account_id: UUID) -> PaperTargetSummaryResponse | None:
        row = await self._session.scalar(
            select(PaperTargetVersionRecord)
            .where(
                PaperTargetVersionRecord.account_id == account_id,
                PaperTargetVersionRecord.activated_at <= self._request_time,
                PaperTargetVersionRecord.created_at <= self._request_time,
            )
            .order_by(PaperTargetVersionRecord.version_seq.desc())
            .limit(1)
        )
        if row is None:
            return None
        return await self._target_summary(row, account_id=account_id)

    async def _target_summary(
        self,
        row: PaperTargetVersionRecord,
        *,
        account_id: UUID,
    ) -> PaperTargetSummaryResponse:
        if row.account_id != account_id:
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The active target is incomplete.",
                status_code=409,
            )
        try:
            authority = await load_target_read_authority(
                self._session,
                account_id=account_id,
                target_id=row.id,
                read_as_of=self._request_time,
            )
        except ValueError as error:
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The active target is incomplete.",
                status_code=409,
            ) from error
        if authority is None:
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The active target is incomplete.",
                status_code=409,
            )
        proposal = authority.proposal.persisted.proposal
        return PaperTargetSummaryResponse(
            target_version_id=authority.target.id,
            version_seq=authority.target.version_seq,
            strategy_decision_id=authority.target.strategy_decision_id,
            previous_target_version_id=authority.target.previous_target_version_id,
            target_content_hash=authority.target.target_content_hash,
            activation_hash=authority.target.activation_hash,
            cash_weight=decimal_to_string(proposal.cash_weight),
            targets=tuple(
                PaperTargetAllocationResponse(
                    instrument_id=instrument_id,
                    target_weight=decimal_to_string(proposal.weights[instrument_id]),
                    exit=proposal.weights[instrument_id] == 0,
                    role=proposal.roles.get(instrument_id),
                    reason_code=proposal.reasons[instrument_id],
                )
                for instrument_id in sorted(proposal.weights, key=lambda value: value.int)
            ),
            as_of=authority.target.activated_at,
        )

    async def _plan_summary(self, batch_id: UUID) -> PaperPlanSummaryResponse | None:
        projection = await self.plan(batch_id)
        if projection is None:
            return None
        return PaperPlanSummaryResponse(
            batch_id=projection.batch_id,
            target_version_id=(
                None if projection.target is None else projection.target.target_version_id
            ),
            strategy_decision_id=projection.proposal.strategy_decision_id,
            decision_hash=projection.proposal.decision_hash,
            plan_created_at=projection.plan.plan_created_at,
            reconciliation_as_of=projection.plan.reconciliation_as_of,
            occupation_state=projection.plan.occupation_state,
        )

    async def _occupying_plan_summary(self, account_id: UUID) -> PaperPlanSummaryResponse | None:
        try:
            projection = await load_paper_projection(
                self._session,
                account_id=account_id,
                as_of=self._request_time,
            )
        except ValueError as error:
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The occupying paper plan projection is incomplete.",
                status_code=409,
            ) from error
        occupied = bool(
            projection.open_orders
            or projection.reserved_cash != 0
            or any(position.reserved_quantity != 0 for position in projection.positions)
        )
        if not occupied:
            return None
        order_ids = tuple(order.order_id for order in projection.open_orders)
        batch_ids = (
            ()
            if not order_ids
            else tuple(
                await self._session.scalars(
                    select(PaperOrderRecord.batch_id)
                    .where(
                        PaperOrderRecord.account_id == account_id,
                        PaperOrderRecord.id.in_(order_ids),
                    )
                    .distinct()
                )
            )
        )
        if len(batch_ids) != 1:
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The occupying paper plan identity is incomplete.",
                status_code=409,
            )
        summary = await self._plan_summary(batch_ids[0])
        if summary is None or summary.occupation_state not in {"occupied", "unknown"}:
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The occupying paper plan authority is incomplete.",
                status_code=409,
            )
        return summary

    async def _blocking_plan_summary(
        self,
        event: AgentPaperCycleEventRecord | None,
    ) -> PaperPlanSummaryResponse | None:
        if event is None or event.result_code != "TARGET_ACTIVATION_BLOCKED":
            return None
        facts = event.fact_ids if isinstance(event.fact_ids, dict) else {}
        raw_plan_id = facts.get("blocking_plan_id")
        if raw_plan_id is None:
            return None
        plan_id = _optional_uuid(raw_plan_id)
        if plan_id is None:
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The blocking paper plan identity is incomplete.",
                status_code=409,
            )
        summary = await self._plan_summary(plan_id)
        if summary is None:
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The blocking paper plan authority is incomplete.",
                status_code=409,
            )
        return summary

    def _activation_result(
        self,
        *,
        event: AgentPaperCycleEventRecord | None,
        decision: StrategyDecisionRecord | None,
        target: PaperTargetSummaryResponse | None,
        batch: PaperOrderBatchRecord | None,
        strict_identity: bool = False,
    ) -> AgentPaperActivationResultResponse | None:
        if event is None:
            return None
        facts = event.fact_ids if isinstance(event.fact_ids, dict) else {}
        decision_id = _optional_uuid(facts.get("strategy_decision_id"))
        target_id = _optional_uuid(facts.get("target_version_id"))
        batch_id = _optional_uuid(facts.get("batch_id"))
        if event.result_code == "TARGET_ACTIVATION_BLOCKED":
            if (
                batch is not None
                or target is not None
                or batch_id is not None
                or target_id is not None
                or strict_identity
                and (decision is None or decision_id != decision.id)
            ):
                raise NexusError(
                    ErrorCode.DATA_PARTIAL,
                    "The Agent paper activation identity is incomplete.",
                    status_code=409,
                )
            return AgentPaperActivationResultResponse(
                result_code="TARGET_ACTIVATION_BLOCKED",
                strategy_decision_id=decision_id,
                target_version_id=None,
                batch_id=None,
                as_of=event.created_at,
            )
        if strict_identity and (decision is None or target is None):
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The Agent paper activation identity is incomplete.",
                status_code=409,
            )
        if decision is None or target is None:
            return None
        if (strict_identity and decision_id != decision.id) or (
            decision_id is not None and decision_id != decision.id
        ):
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The Agent paper activation identity is incomplete.",
                status_code=409,
            )
        if target_id is None or target_id != target.target_version_id:
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The Agent paper activation identity is incomplete.",
                status_code=409,
            )
        plan_created = event.result_code == "PAPER_BATCH_EVALUATED"
        no_order_required = event.result_code == "NO_ORDER_REQUIRED"
        if (
            plan_created != (batch is not None)
            or no_order_required != (batch is None)
            or batch is not None
            and batch_id != batch.id
            or batch is None
            and batch_id is not None
        ):
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The Agent paper activation identity is incomplete.",
                status_code=409,
            )
        return AgentPaperActivationResultResponse(
            result_code="PLAN_CREATED" if plan_created else "NO_ORDER_REQUIRED",
            strategy_decision_id=decision.id,
            target_version_id=target.target_version_id,
            batch_id=None if batch is None else batch.id,
            as_of=event.created_at,
        )

    def _cycle_blocker(
        self,
        *,
        event: AgentPaperCycleEventRecord,
        synthesis_output: AgentPortfolioSynthesisOutputRecord | None,
        decision: StrategyDecisionRecord | None,
    ) -> AgentPaperCycleBlockerResponse | None:
        code = event.result_code
        facts = event.fact_ids if isinstance(event.fact_ids, dict) else {}
        rejection = facts.get("proposal_rejection_code")
        if code == "PORTFOLIO_PROPOSAL_UNAVAILABLE":
            if not isinstance(rejection, str) or rejection not in {
                "EXIT_TURNOVER_CONFLICT",
                "REDUCTION_TURNOVER_CONFLICT",
                "PORTFOLIO_DEPENDENCY_CONFLICT",
            }:
                raise NexusError(
                    ErrorCode.DATA_PARTIAL,
                    "The portfolio proposal rejection authority is incomplete.",
                    status_code=409,
                )
            return AgentPaperCycleBlockerResponse(
                code="PORTFOLIO_PROPOSAL_UNAVAILABLE",
                source_stage="proposal",
                category="proposal_rejected",
                proposal_rejection_code=cast(Any, rejection),
                as_of=event.occurred_at,
            )
        if code == "AGENT_PAPER_PROVIDER_CAPACITY_EXCEEDED":
            return AgentPaperCycleBlockerResponse(
                code="AGENT_PAPER_PROVIDER_CAPACITY_EXCEEDED",
                source_stage="provider",
                category="provider_capacity",
                proposal_rejection_code=None,
                as_of=event.occurred_at,
            )
        if code == "TARGET_ACTIVATION_BLOCKED":
            return AgentPaperCycleBlockerResponse(
                code="TARGET_ACTIVATION_BLOCKED",
                source_stage="activation",
                category="resource_occupied",
                proposal_rejection_code=None,
                as_of=event.occurred_at,
            )
        if (
            synthesis_output is not None
            and synthesis_output.status == "completed"
            and decision is None
        ):
            return AgentPaperCycleBlockerResponse(
                code="POLICY_UNCONFIRMED",
                source_stage="policy",
                category="policy_unconfirmed",
                proposal_rejection_code=None,
                as_of=synthesis_output.created_at,
            )
        return None

    async def agent_cycles(
        self,
        *,
        decision_session: date | None,
        limit: int,
    ) -> AgentPaperCyclePageResponse:
        if not 1 <= limit <= 20:
            raise ValueError("limit must be between 1 and 20")
        account, _ = await self.account()
        version_evaluation = await self._agent_version_evaluation()
        version_history = await self._agent_version_history(
            account_id=account.id,
            challenger_available=version_evaluation is not None,
        )
        resolved_session = decision_session
        if resolved_session is None:
            resolved_session = await self._current_xnys_session()
        if resolved_session is None:
            return AgentPaperCyclePageResponse(
                items=(),
                summary=await self._agent_session_summary(
                    account_id=account.id,
                    decision_session=None,
                    cycles=(),
                    historical=False,
                ),
                version_observation=version_history[0] if version_history else None,
                version_history=version_history,
                version_evaluation=version_evaluation,
            )
        cycles = tuple(
            await self._session.scalars(
                select(AgentPaperCycleRecord)
                .where(
                    AgentPaperCycleRecord.account_id == account.id,
                    AgentPaperCycleRecord.decision_session == resolved_session,
                    AgentPaperCycleRecord.decision_time <= self._request_time,
                    AgentPaperCycleRecord.created_at <= self._request_time,
                )
                .order_by(
                    AgentPaperCycleRecord.decision_slot_at.desc(),
                    AgentPaperCycleRecord.id.desc(),
                )
                .limit(limit)
            )
        )
        items = tuple([await self._agent_cycle(cycle) for cycle in cycles])
        historical = decision_session is not None
        if historical:
            summary_items = items
        else:
            summary_cycle = await self._session.scalar(
                select(AgentPaperCycleRecord)
                .where(
                    AgentPaperCycleRecord.account_id == account.id,
                    AgentPaperCycleRecord.decision_session == resolved_session,
                    AgentPaperCycleRecord.candidate_batch_id.is_not(None),
                    AgentPaperCycleRecord.decision_time <= self._request_time,
                    AgentPaperCycleRecord.created_at <= self._request_time,
                )
                .order_by(
                    AgentPaperCycleRecord.decision_slot_at.desc(),
                    AgentPaperCycleRecord.id.desc(),
                )
                .limit(1)
            )
            summary_items = (
                () if summary_cycle is None else (await self._agent_cycle(summary_cycle),)
            )
        cycle_count, completed_count = await self._agent_session_counts(
            account_id=account.id,
            decision_session=resolved_session,
            portfolio_only=not historical,
        )
        return AgentPaperCyclePageResponse(
            items=items,
            summary=await self._agent_session_summary(
                account_id=account.id,
                decision_session=resolved_session,
                cycles=summary_items,
                cycle_count=cycle_count,
                completed_count=completed_count,
                historical=historical,
            ),
            version_observation=version_history[0] if version_history else None,
            version_history=version_history,
            version_evaluation=version_evaluation,
        )

    async def _agent_version_evaluation(
        self,
    ) -> AgentVersionEvaluationResponse | None:
        row = (
            await self._session.execute(
                select(
                    AgentVersionEvaluationRecord,
                    AgentVersionEvaluationDecisionRecord,
                )
                .outerjoin(
                    AgentVersionEvaluationDecisionRecord,
                    and_(
                        AgentVersionEvaluationDecisionRecord.evaluation_id
                        == AgentVersionEvaluationRecord.id,
                        AgentVersionEvaluationDecisionRecord.created_at <= self._request_time,
                    ),
                )
                .where(AgentVersionEvaluationRecord.created_at <= self._request_time)
                .order_by(
                    AgentVersionEvaluationRecord.created_at.desc(),
                    AgentVersionEvaluationRecord.id.desc(),
                )
                .limit(1)
            )
        ).one_or_none()
        if row is None:
            return None
        evaluation, decision = row
        pairs = tuple(
            await self._session.scalars(
                select(AgentVersionCyclePairRecord).where(
                    AgentVersionCyclePairRecord.evaluation_id == evaluation.id,
                    AgentVersionCyclePairRecord.created_at <= self._request_time,
                )
            )
        )
        train_completed = sum(pair.phase == "train" for pair in pairs)
        oos_completed = sum(pair.phase == "oos" for pair in pairs)
        registered_slots = tuple(evaluation.train_slots) + tuple(evaluation.oos_slots)
        cycles = tuple(
            await self._session.scalars(
                select(AgentPaperCycleRecord).where(
                    AgentPaperCycleRecord.account_id.in_(
                        (evaluation.incumbent_account_id, evaluation.challenger_account_id)
                    ),
                    AgentPaperCycleRecord.decision_slot_at.in_(registered_slots),
                    AgentPaperCycleRecord.created_at <= self._request_time,
                )
            )
        )
        cycle_by_lane_slot = {(cycle.account_id, cycle.decision_slot_at): cycle for cycle in cycles}
        cycle_ids = tuple(cycle.id for cycle in cycles)
        cycle_events = (
            tuple(
                await self._session.scalars(
                    select(AgentPaperCycleEventRecord)
                    .where(
                        AgentPaperCycleEventRecord.cycle_id.in_(cycle_ids),
                        AgentPaperCycleEventRecord.occurred_at <= self._request_time,
                        AgentPaperCycleEventRecord.created_at <= self._request_time,
                    )
                    .order_by(
                        AgentPaperCycleEventRecord.cycle_id,
                        AgentPaperCycleEventRecord.sequence,
                    )
                )
            )
            if cycle_ids
            else ()
        )
        latest_event_by_cycle = {event.cycle_id: event for event in cycle_events}
        pair_by_slot = {pair.decision_slot_at: pair for pair in pairs}

        def lane(account_id: UUID, slot: datetime) -> AgentVersionSlotLaneResponse:
            cycle = cycle_by_lane_slot.get((account_id, slot))
            event = None if cycle is None else latest_event_by_cycle.get(cycle.id)
            if cycle is None or event is None:
                return AgentVersionSlotLaneResponse(status="pending", result_code=None)
            return AgentVersionSlotLaneResponse(
                status=cast(Any, event.to_status),
                result_code=event.result_code,
            )

        slots: list[AgentVersionSlotResponse] = []
        for index, slot in enumerate(registered_slots):
            incumbent = lane(evaluation.incumbent_account_id, slot)
            challenger = lane(evaluation.challenger_account_id, slot)
            pair = pair_by_slot.get(slot)
            if pair is not None:
                slot_status = "paired"
                reason_code = "PAIR_RECORDED"
            elif incumbent.status in ("unavailable", "failed") or challenger.status in (
                "unavailable",
                "failed",
            ):
                slot_status = "attention"
                reason_code = "PAIR_LANE_TERMINAL_FAILURE"
            elif incumbent.status != "pending" and challenger.status != "pending":
                slot_status = "running"
                reason_code = "PAIR_PENDING"
            elif incumbent.status != "pending" or challenger.status != "pending":
                slot_status = "attention"
                reason_code = "PAIR_LANE_MISSING"
            elif slot > self._request_time:
                slot_status = "scheduled"
                reason_code = "REGISTERED_SLOT_SCHEDULED"
            elif self._request_time <= slot + timedelta(seconds=60):
                slot_status = "due"
                reason_code = "REGISTERED_SLOT_DUE"
            else:
                slot_status = "attention"
                reason_code = "REGISTERED_SLOT_CYCLES_MISSING"
            slots.append(
                AgentVersionSlotResponse(
                    phase="train" if index < len(evaluation.train_slots) else "oos",
                    decision_slot_at=slot,
                    status=cast(Any, slot_status),
                    incumbent=incumbent,
                    challenger=challenger,
                    reason_code=reason_code,
                )
            )
        incumbent_metrics = await self._agent_version_lane_metrics(
            account_id=evaluation.incumbent_account_id,
            window_start=evaluation.train_slots[0],
        )
        challenger_metrics = await self._agent_version_lane_metrics(
            account_id=evaluation.challenger_account_id,
            window_start=evaluation.train_slots[0],
        )
        comparisons = (
            AgentVersionComparisonResponse(
                key="hold_current",
                data_state="READY",
                reason_code="CASH_HOLD_CURRENT",
            ),
            AgentVersionComparisonResponse(
                key="baseline_v1",
                data_state="UNKNOWN",
                reason_code="SAME_WINDOW_FORWARD_FACTS_UNAVAILABLE",
            ),
            AgentVersionComparisonResponse(
                key="market_benchmark",
                data_state="UNKNOWN",
                reason_code="SAME_WINDOW_FORWARD_FACTS_UNAVAILABLE",
            ),
        )
        gates: tuple[AgentVersionGateResponse, ...] = ()
        if decision is not None:
            raw_gates = decision.metric_report.get("gates")
            if not isinstance(raw_gates, list):
                raise ValueError("AGENT_VERSION_DECISION_AUTHORITY_INVALID")
            gates = tuple(AgentVersionGateResponse.model_validate(item) for item in raw_gates)
        return AgentVersionEvaluationResponse(
            evaluation_id=evaluation.id,
            status="observing" if decision is None else decision.status,
            incumbent_prompt_hash=evaluation.incumbent_prompt_hash,
            incumbent_model_hash=evaluation.incumbent_model_hash,
            challenger_prompt_hash=evaluation.challenger_prompt_hash,
            challenger_model_hash=evaluation.challenger_model_hash,
            train_completed=train_completed,
            oos_completed=oos_completed,
            oos_missing=20 - oos_completed,
            oos_incomparable=0,
            slots=tuple(slots),
            incumbent_metrics=incumbent_metrics,
            challenger_metrics=challenger_metrics,
            comparisons=comparisons,
            gates=gates,
            reason_code=("EVALUATION_OBSERVING" if decision is None else decision.reason_code),
            execution_locked=True,
            broker_write=False,
            live_write=False,
        )

    async def _latest_performance_snapshot(
        self, *, account_id: UUID
    ) -> PaperPerformanceSnapshotRecord | None:
        return cast(
            PaperPerformanceSnapshotRecord | None,
            await self._session.scalar(
                select(PaperPerformanceSnapshotRecord)
                .where(
                    PaperPerformanceSnapshotRecord.account_id == account_id,
                    PaperPerformanceSnapshotRecord.as_of <= self._request_time,
                    PaperPerformanceSnapshotRecord.created_at <= self._request_time,
                )
                .order_by(
                    PaperPerformanceSnapshotRecord.as_of.desc(),
                    PaperPerformanceSnapshotRecord.id.desc(),
                )
                .limit(1)
            ),
        )

    async def _agent_version_lane_metrics(
        self,
        *,
        account_id: UUID,
        window_start: datetime,
    ) -> AgentVersionLaneMetricsResponse | None:
        account = await self._session.get(PaperAccountRecord, account_id)
        snapshot = await self._latest_performance_snapshot(account_id=account_id)
        if account is None or snapshot is None:
            return None
        max_drawdown = await self._session.scalar(
            select(func.max(PaperPerformanceSnapshotRecord.drawdown_ratio)).where(
                PaperPerformanceSnapshotRecord.account_id == account_id,
                PaperPerformanceSnapshotRecord.as_of >= window_start,
                PaperPerformanceSnapshotRecord.as_of <= snapshot.as_of,
            )
        )
        orders = tuple(
            await self._session.scalars(
                select(PaperOrderRecord).where(
                    PaperOrderRecord.account_id == account_id,
                    PaperOrderRecord.created_at >= window_start,
                    PaperOrderRecord.created_at <= self._request_time,
                )
            )
        )
        order_ids = tuple(order.id for order in orders)
        fills = (
            tuple(
                await self._session.scalars(
                    select(PaperFillRecord).where(
                        PaperFillRecord.order_id.in_(order_ids),
                        PaperFillRecord.occurred_at <= self._request_time,
                    )
                )
            )
            if order_ids
            else ()
        )
        risk_rows = (
            tuple(
                await self._session.scalars(
                    select(RiskEvaluationRecord).where(RiskEvaluationRecord.order_id.in_(order_ids))
                )
            )
            if order_ids
            else ()
        )
        risk_by_order = {risk.order_id: risk for risk in risk_rows}
        hard_risk_count = sum(
            order.id not in risk_by_order or risk_by_order[order.id].outcome != order.risk_outcome
            for order in orders
        )
        turnover = sum((fill.notional for fill in fills), Decimal(0))
        slippage = sum((fill.slippage_amount for fill in fills), Decimal(0))
        return AgentVersionLaneMetricsResponse(
            nav=decimal_to_string(snapshot.nav),
            total_return_ratio=decimal_to_string(
                snapshot.nav / account.initial_capital - Decimal(1)
            ),
            max_drawdown_ratio=decimal_to_string(
                snapshot.drawdown_ratio if max_drawdown is None else max_drawdown
            ),
            turnover_ratio=decimal_to_string(turnover / account.initial_capital),
            total_cost=decimal_to_string(snapshot.fees + slippage),
            order_count=len(orders),
            fill_count=len(fills),
            hard_risk_count=hard_risk_count,
        )

    async def _agent_version_history(
        self, *, account_id: UUID, challenger_available: bool
    ) -> tuple[AgentPaperVersionObservationResponse, ...]:
        rows = (
            (
                await self._session.execute(
                    text(
                        """
                    WITH scoped_cycles AS (
                        SELECT id, decision_session, decision_time
                        FROM agent_paper_cycles
                        WHERE account_id = :account_id
                          AND candidate_batch_id IS NULL
                          AND decision_time <= :cutoff
                          AND created_at <= :cutoff
                    ),
                    research_runs AS (
                        SELECT DISTINCT
                            cycle.id AS cycle_id,
                            cycle.decision_session,
                            cycle.decision_time,
                            run_id.value::uuid AS run_id
                        FROM scoped_cycles AS cycle
                        JOIN agent_paper_cycle_events AS event
                          ON event.cycle_id = cycle.id
                        CROSS JOIN LATERAL jsonb_array_elements_text(
                            event.fact_ids -> 'research_run_ids'
                        ) AS run_id(value)
                        WHERE event.result_code = 'RESEARCH_JOBS_PREPARED'
                          AND event.occurred_at <= :cutoff
                          AND event.created_at <= :cutoff
                    ),
                    cycle_version_counts AS (
                        SELECT
                            research.cycle_id,
                            count(DISTINCT (run.prompt_hash, run.model_hash)) AS version_count
                        FROM research_runs AS research
                        JOIN agent_research_runs AS run ON run.id = research.run_id
                        WHERE run.decision_time <= :cutoff
                          AND run.created_at <= :cutoff
                        GROUP BY research.cycle_id
                    ),
                    latest_events AS (
                        SELECT DISTINCT ON (event.cycle_id)
                            event.cycle_id,
                            event.to_status
                        FROM agent_paper_cycle_events AS event
                        JOIN scoped_cycles AS cycle ON cycle.id = event.cycle_id
                        WHERE event.occurred_at <= :cutoff
                          AND event.created_at <= :cutoff
                        ORDER BY event.cycle_id, event.sequence DESC
                    ),
                    cycle_orders AS (
                        SELECT
                            decision.agent_paper_cycle_id AS cycle_id,
                            count(paper_order.id) AS order_count
                        FROM strategy_decisions AS decision
                        JOIN scoped_cycles AS cycle
                          ON cycle.id = decision.agent_paper_cycle_id
                        JOIN paper_orders AS paper_order
                          ON paper_order.strategy_decision_id = decision.id
                         AND paper_order.created_at <= :cutoff
                        WHERE decision.decision_origin = 'agent_research'
                          AND decision.created_at <= :cutoff
                        GROUP BY decision.agent_paper_cycle_id
                    ),
                    latest_order_events AS (
                        SELECT DISTINCT ON (order_event.order_id)
                            order_event.order_id,
                            order_event.to_status
                        FROM paper_order_events AS order_event
                        JOIN paper_orders AS paper_order ON paper_order.id = order_event.order_id
                        JOIN strategy_decisions AS decision
                          ON decision.id = paper_order.strategy_decision_id
                        JOIN scoped_cycles AS cycle
                          ON cycle.id = decision.agent_paper_cycle_id
                        WHERE order_event.occurred_at <= :cutoff
                          AND order_event.created_at <= :cutoff
                          AND paper_order.created_at <= :cutoff
                          AND decision.created_at <= :cutoff
                        ORDER BY order_event.order_id, order_event.event_seq DESC
                    ),
                    cycle_terminal_orders AS (
                        SELECT
                            decision.agent_paper_cycle_id AS cycle_id,
                            count(latest_order.order_id) FILTER (
                                WHERE latest_order.to_status IN (
                                    'filled', 'cancelled', 'rejected', 'risk_rejected'
                                )
                            ) AS terminal_order_count
                        FROM strategy_decisions AS decision
                        JOIN scoped_cycles AS cycle
                          ON cycle.id = decision.agent_paper_cycle_id
                        JOIN paper_orders AS paper_order
                          ON paper_order.strategy_decision_id = decision.id
                         AND paper_order.created_at <= :cutoff
                        LEFT JOIN latest_order_events AS latest_order
                          ON latest_order.order_id = paper_order.id
                        WHERE decision.decision_origin = 'agent_research'
                          AND decision.created_at <= :cutoff
                        GROUP BY decision.agent_paper_cycle_id
                    ),
                    version_rows AS (
                        SELECT
                            research.cycle_id,
                            research.run_id,
                            research.decision_session,
                            research.decision_time,
                            prompt.version AS prompt_version,
                            model.model_id,
                            model.version AS model_version,
                            run.prompt_hash,
                            run.model_hash,
                            latest.to_status,
                            coalesce(cycle_orders.order_count, 0) AS cycle_order_count,
                            coalesce(cycle_terminal_orders.terminal_order_count, 0)
                                AS terminal_order_count,
                            decision.id AS decision_id,
                            paper_order.id AS order_id,
                            fill.id AS fill_id
                        FROM research_runs AS research
                        JOIN agent_research_runs AS run ON run.id = research.run_id
                        JOIN cycle_version_counts AS cycle_version
                          ON cycle_version.cycle_id = research.cycle_id
                         AND cycle_version.version_count = 1
                        JOIN agent_prompt_versions AS prompt ON prompt.id = run.prompt_version_id
                        JOIN agent_model_configs AS model ON model.id = run.model_config_id
                        LEFT JOIN latest_events AS latest ON latest.cycle_id = research.cycle_id
                        LEFT JOIN cycle_orders ON cycle_orders.cycle_id = research.cycle_id
                        LEFT JOIN cycle_terminal_orders
                          ON cycle_terminal_orders.cycle_id = research.cycle_id
                        LEFT JOIN strategy_decisions AS decision
                          ON decision.agent_paper_cycle_id = research.cycle_id
                         AND decision.created_at <= :cutoff
                        LEFT JOIN paper_orders AS paper_order
                          ON paper_order.strategy_decision_id = decision.id
                         AND paper_order.created_at <= :cutoff
                        LEFT JOIN paper_fills AS fill
                          ON fill.order_id = paper_order.id
                         AND fill.created_at <= :cutoff
                         AND fill.occurred_at <= :cutoff
                         AND fill.evaluated_at <= :cutoff
                        WHERE run.decision_time <= :cutoff
                          AND run.created_at <= :cutoff
                          AND prompt.created_at <= :cutoff
                          AND model.created_at <= :cutoff
                    ),
                    version_stats AS (
                        SELECT
                            (array_agg(
                                prompt_version
                                ORDER BY decision_time DESC, cycle_id DESC, run_id DESC
                            ))[1] AS prompt_version,
                            (array_agg(
                                model_id
                                ORDER BY decision_time DESC, cycle_id DESC, run_id DESC
                            ))[1] AS model_id,
                            (array_agg(
                                model_version
                                ORDER BY decision_time DESC, cycle_id DESC, run_id DESC
                            ))[1] AS model_version,
                            prompt_hash,
                            model_hash,
                            min(decision_time) AS first_decision_at,
                            max(decision_time) AS last_decision_at,
                            count(DISTINCT decision_session) AS observed_sessions,
                            count(DISTINCT cycle_id) AS cycle_count,
                            count(DISTINCT cycle_id) FILTER (
                                WHERE to_status = 'completed'
                                   OR (
                                       to_status = 'awaiting_market'
                                       AND cycle_order_count > 0
                                       AND cycle_order_count = terminal_order_count
                                   )
                            )
                                AS completed_count,
                            count(DISTINCT cycle_id) FILTER (WHERE to_status = 'unavailable')
                                AS unavailable_count,
                            count(DISTINCT order_id) AS order_count,
                            count(DISTINCT fill_id) AS fill_count
                        FROM version_rows
                        GROUP BY prompt_hash, model_hash
                    ),
                    incumbent_version AS (
                        SELECT run.prompt_hash, run.model_hash
                        FROM agent_research_runs AS run
                        JOIN research_runs AS research ON research.run_id = run.id
                        WHERE run.actor_id = 'local-agent-paper-cycle'
                          AND run.decision_time <= :cutoff
                          AND run.created_at <= :cutoff
                        ORDER BY run.decision_time DESC, run.created_at DESC, run.id DESC
                        LIMIT 1
                    )
                    SELECT
                        version_stats.*,
                        (
                            version_stats.prompt_hash = incumbent.prompt_hash
                            AND version_stats.model_hash = incumbent.model_hash
                        ) AS is_incumbent,
                        (
                            SELECT count(*)
                            FROM cycle_version_counts
                            WHERE version_count > 1
                        ) AS mixed_cycle_count
                    FROM version_stats
                    CROSS JOIN incumbent_version AS incumbent
                    ORDER BY
                        last_decision_at DESC,
                        prompt_hash,
                        model_hash,
                        prompt_version,
                        model_id,
                        model_version
                    LIMIT 20
                    """
                    ),
                    {"account_id": account_id, "cutoff": self._request_time},
                )
            )
            .mappings()
            .all()
        )
        if (
            not rows
            or any(int(row["mixed_cycle_count"]) > 0 for row in rows)
            or not bool(rows[0]["is_incumbent"])
            or sum(bool(row["is_incumbent"]) for row in rows) != 1
        ):
            return ()
        observations: list[AgentPaperVersionObservationResponse] = []
        for row in rows:
            is_incumbent = bool(row["is_incumbent"])
            reason_codes = (
                ["VERSION_PERFORMANCE_ATTRIBUTION_UNAVAILABLE"]
                if is_incumbent
                else [
                    "VERSION_FROZEN_AUDIT_HISTORY",
                    "VERSION_PERFORMANCE_ATTRIBUTION_UNAVAILABLE",
                ]
            )
            if int(row["order_count"]) == 0 or int(row["fill_count"]) == 0:
                reason_codes.append("VERSION_ORDER_PATH_UNOBSERVED")
            if not is_incumbent:
                reason_codes.append("VERSION_SEQUENTIAL_HISTORY_NOT_COMPARABLE")
            elif not challenger_available:
                reason_codes.append("VERSION_CHALLENGER_UNAVAILABLE")
            observations.append(
                AgentPaperVersionObservationResponse(
                    prompt_version=cast(str, row["prompt_version"]),
                    prompt_hash=cast(str, row["prompt_hash"]),
                    model_id=cast(str, row["model_id"]),
                    model_version=cast(str, row["model_version"]),
                    model_hash=cast(str, row["model_hash"]),
                    first_decision_at=cast(datetime, row["first_decision_at"]),
                    last_decision_at=cast(datetime, row["last_decision_at"]),
                    observed_sessions=int(row["observed_sessions"]),
                    cycle_count=int(row["cycle_count"]),
                    completed_count=int(row["completed_count"]),
                    unavailable_count=int(row["unavailable_count"]),
                    order_count=int(row["order_count"]),
                    fill_count=int(row["fill_count"]),
                    challenger_available=challenger_available,
                    lifecycle_state=("incumbent_frozen" if is_incumbent else "historical_frozen"),
                    verdict=(
                        "continue_observing" if is_incumbent else "retained_as_frozen_history"
                    ),
                    reason_codes=cast(Any, tuple(reason_codes)),
                )
            )
        return tuple(observations)

    async def _agent_session_counts(
        self,
        *,
        account_id: UUID,
        decision_session: date,
        portfolio_only: bool = False,
    ) -> tuple[int, int]:
        session_cycle_ids = select(AgentPaperCycleRecord.id).where(
            AgentPaperCycleRecord.account_id == account_id,
            AgentPaperCycleRecord.decision_session == decision_session,
            AgentPaperCycleRecord.decision_time <= self._request_time,
            AgentPaperCycleRecord.created_at <= self._request_time,
        )
        if portfolio_only:
            session_cycle_ids = session_cycle_ids.where(
                AgentPaperCycleRecord.candidate_batch_id.is_not(None)
            )
        latest_cycle_events = (
            select(
                AgentPaperCycleEventRecord.cycle_id,
                func.max(AgentPaperCycleEventRecord.sequence).label("sequence"),
            )
            .where(
                AgentPaperCycleEventRecord.cycle_id.in_(session_cycle_ids),
                AgentPaperCycleEventRecord.occurred_at <= self._request_time,
                AgentPaperCycleEventRecord.created_at <= self._request_time,
            )
            .group_by(AgentPaperCycleEventRecord.cycle_id)
            .subquery()
        )
        session_order_ids = (
            select(PaperOrderRecord.id)
            .join(
                StrategyDecisionRecord,
                StrategyDecisionRecord.id == PaperOrderRecord.strategy_decision_id,
            )
            .where(
                StrategyDecisionRecord.agent_paper_cycle_id.in_(session_cycle_ids),
                StrategyDecisionRecord.decision_origin == "agent_research",
                StrategyDecisionRecord.created_at <= self._request_time,
                PaperOrderRecord.created_at <= self._request_time,
            )
        )
        latest_order_events = (
            select(
                PaperOrderEventRecord.order_id,
                func.max(PaperOrderEventRecord.event_seq).label("event_seq"),
            )
            .where(
                PaperOrderEventRecord.order_id.in_(session_order_ids),
                PaperOrderEventRecord.occurred_at <= self._request_time,
                PaperOrderEventRecord.created_at <= self._request_time,
            )
            .group_by(PaperOrderEventRecord.order_id)
            .subquery()
        )
        terminal_order_statuses = {"filled", "cancelled", "rejected", "risk_rejected"}
        rows = (
            await self._session.execute(
                select(
                    AgentPaperCycleEventRecord.to_status,
                    func.count(PaperOrderRecord.id).label("order_count"),
                    func.count(PaperOrderEventRecord.order_id).label("terminal_order_count"),
                )
                .select_from(AgentPaperCycleRecord)
                .outerjoin(
                    latest_cycle_events,
                    latest_cycle_events.c.cycle_id == AgentPaperCycleRecord.id,
                )
                .outerjoin(
                    AgentPaperCycleEventRecord,
                    and_(
                        AgentPaperCycleEventRecord.cycle_id == AgentPaperCycleRecord.id,
                        AgentPaperCycleEventRecord.sequence == latest_cycle_events.c.sequence,
                    ),
                )
                .outerjoin(
                    StrategyDecisionRecord,
                    and_(
                        StrategyDecisionRecord.agent_paper_cycle_id == AgentPaperCycleRecord.id,
                        StrategyDecisionRecord.decision_origin == "agent_research",
                        StrategyDecisionRecord.created_at <= self._request_time,
                    ),
                )
                .outerjoin(
                    PaperOrderRecord,
                    and_(
                        PaperOrderRecord.strategy_decision_id == StrategyDecisionRecord.id,
                        PaperOrderRecord.created_at <= self._request_time,
                    ),
                )
                .outerjoin(
                    latest_order_events,
                    latest_order_events.c.order_id == PaperOrderRecord.id,
                )
                .outerjoin(
                    PaperOrderEventRecord,
                    and_(
                        PaperOrderEventRecord.order_id == PaperOrderRecord.id,
                        PaperOrderEventRecord.event_seq == latest_order_events.c.event_seq,
                        PaperOrderEventRecord.to_status.in_(terminal_order_statuses),
                        PaperOrderEventRecord.created_at <= self._request_time,
                    ),
                )
                .where(
                    AgentPaperCycleRecord.account_id == account_id,
                    AgentPaperCycleRecord.decision_session == decision_session,
                    AgentPaperCycleRecord.decision_time <= self._request_time,
                    AgentPaperCycleRecord.created_at <= self._request_time,
                    *(
                        (AgentPaperCycleRecord.candidate_batch_id.is_not(None),)
                        if portfolio_only
                        else ()
                    ),
                )
                .group_by(
                    AgentPaperCycleRecord.id,
                    AgentPaperCycleEventRecord.to_status,
                )
                .order_by(AgentPaperCycleRecord.id)
                .limit(_MAX_AGENT_CYCLES_PER_SESSION + 1)
            )
        ).all()
        if len(rows) > _MAX_AGENT_CYCLES_PER_SESSION or any(
            order_count > _MAX_AGENT_ORDERS_PER_CYCLE for _, order_count, _ in rows
        ):
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The Agent paper session is unbounded.",
                status_code=409,
            )
        if any(status is None for status, _, _ in rows):
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The Agent paper session projection is incomplete.",
                status_code=409,
            )
        completed_count = sum(
            status == "completed"
            or (status == "awaiting_market" and order_count > 0 and order_count == terminal_count)
            for status, order_count, terminal_count in rows
        )
        return len(rows), completed_count

    async def _agent_session_summary(
        self,
        *,
        account_id: UUID,
        decision_session: date | None,
        cycles: tuple[AgentPaperCycleResponse, ...],
        cycle_count: int = 0,
        completed_count: int = 0,
        historical: bool,
    ) -> AgentPaperSessionSummaryResponse:
        last_completed = await self._session.scalar(
            select(AgentPaperCycleRecord.decision_time)
            .join(
                AgentPaperCycleEventRecord,
                AgentPaperCycleEventRecord.cycle_id == AgentPaperCycleRecord.id,
            )
            .where(
                AgentPaperCycleEventRecord.to_status == "completed",
                AgentPaperCycleEventRecord.occurred_at <= self._request_time,
                AgentPaperCycleEventRecord.created_at <= self._request_time,
                AgentPaperCycleRecord.account_id == account_id,
                AgentPaperCycleRecord.decision_time <= self._request_time,
                AgentPaperCycleRecord.created_at <= self._request_time,
                *(
                    (AgentPaperCycleRecord.candidate_batch_id.is_not(None),)
                    if not historical
                    else ()
                ),
            )
            .order_by(
                AgentPaperCycleEventRecord.occurred_at.desc(),
                AgentPaperCycleRecord.id.desc(),
            )
            .limit(1)
        )
        latest = cycles[0] if cycles else None
        if historical:
            return AgentPaperSessionSummaryResponse(
                decision_session=decision_session,
                status="historical",
                cycle_count=cycle_count,
                completed_count=completed_count,
                latest_result_code=None if latest is None else latest.result_code,
                last_completed_decision_at=last_completed,
                next_eligible_at=None,
                next_eligible_reason=None,
                execution_locked=True,
            )

        return await self._portfolio_session_summary(
            decision_session=decision_session,
            latest=latest,
            cycle_count=cycle_count,
            completed_count=completed_count,
            last_completed=last_completed,
        )

    async def _portfolio_session_summary(
        self,
        *,
        decision_session: date | None,
        latest: AgentPaperCycleResponse | None,
        cycle_count: int,
        completed_count: int,
        last_completed: datetime | None,
    ) -> AgentPaperSessionSummaryResponse:
        local_now = self._request_time.astimezone(
            ZoneInfo(PORTFOLIO_ALLOCATION_V1.decision_timezone)
        )
        if decision_session is None:
            next_session = await self._next_xnys_session(local_now.date())
            return AgentPaperSessionSummaryResponse(
                decision_session=None,
                status="closed",
                cycle_count=cycle_count,
                completed_count=completed_count,
                latest_result_code=None if latest is None else latest.result_code,
                last_completed_decision_at=last_completed,
                next_eligible_at=(
                    None if next_session is None else self._portfolio_decision_at(next_session)
                ),
                next_eligible_reason=(
                    "CALENDAR_UNAVAILABLE" if next_session is None else "NEXT_SESSION"
                ),
                execution_locked=True,
            )
        scheduled_at = self._portfolio_decision_at(decision_session)
        window_end = scheduled_at + timedelta(minutes=5)
        status: Literal[
            "not_started", "ready", "in_progress", "completed", "stopped", "closed"
        ]
        next_at: datetime | None
        next_reason: Literal[
            "CURRENT_SLOT",
            "SESSION_OPEN",
            "NEXT_SESSION",
            "CURRENT_CYCLE_IN_PROGRESS",
            "CALENDAR_UNAVAILABLE",
        ] | None
        active = latest is not None and latest.status not in {
            "completed",
            "unavailable",
            "failed",
        }
        if active:
            status = "in_progress"
            next_at = None
            next_reason = "CURRENT_CYCLE_IN_PROGRESS"
        elif self._request_time < scheduled_at:
            status = "not_started" if latest is None else "completed"
            next_at = scheduled_at if latest is None else None
            next_reason = "SESSION_OPEN" if latest is None else None
        elif self._request_time < window_end and latest is None:
            status = "ready"
            next_at = scheduled_at
            next_reason = "CURRENT_SLOT"
        else:
            if latest is not None and latest.status == "completed":
                status = "completed"
            elif local_now.time().replace(tzinfo=None) >= time(16):
                status = "closed"
            else:
                status = "stopped"
            next_session = await self._next_xnys_session(decision_session)
            next_at = (
                None if next_session is None else self._portfolio_decision_at(next_session)
            )
            next_reason = "CALENDAR_UNAVAILABLE" if next_session is None else "NEXT_SESSION"
        return AgentPaperSessionSummaryResponse(
            decision_session=decision_session,
            status=status,
            cycle_count=cycle_count,
            completed_count=completed_count,
            latest_result_code=None if latest is None else latest.result_code,
            last_completed_decision_at=last_completed,
            next_eligible_at=next_at,
            next_eligible_reason=next_reason,
            execution_locked=True,
        )

    async def _next_xnys_session(self, after: date) -> date | None:
        value = await self._session.scalar(
            select(ExchangeCalendarSessionRecord.session)
            .join(
                ExchangeCalendarAuthorityRecord,
                ExchangeCalendarAuthorityRecord.id
                == ExchangeCalendarSessionRecord.calendar_authority_id,
            )
            .where(
                ExchangeCalendarAuthorityRecord.exchange == "XNYS",
                ExchangeCalendarAuthorityRecord.completeness_status == "complete",
                ExchangeCalendarAuthorityRecord.published_at <= self._request_time,
                ExchangeCalendarAuthorityRecord.created_at <= self._request_time,
                ExchangeCalendarSessionRecord.created_at <= self._request_time,
                ExchangeCalendarSessionRecord.session > after,
            )
            .order_by(
                ExchangeCalendarSessionRecord.session.asc(),
                ExchangeCalendarAuthorityRecord.published_at.desc(),
                ExchangeCalendarAuthorityRecord.id.desc(),
            )
            .limit(1)
        )
        return cast(date | None, value)

    @staticmethod
    def _portfolio_decision_at(session_date: date) -> datetime:
        return datetime.combine(
            session_date,
            PORTFOLIO_ALLOCATION_V1.decision_local_time,
            tzinfo=ZoneInfo(PORTFOLIO_ALLOCATION_V1.decision_timezone),
        ).astimezone(UTC)

    async def _current_xnys_session(self) -> date | None:
        local_date = self._request_time.astimezone(ZoneInfo("America/New_York")).date()
        session_date = await self._session.scalar(
            select(ExchangeCalendarSessionRecord.session)
            .join(
                ExchangeCalendarAuthorityRecord,
                ExchangeCalendarAuthorityRecord.id
                == ExchangeCalendarSessionRecord.calendar_authority_id,
            )
            .where(
                ExchangeCalendarAuthorityRecord.exchange == "XNYS",
                ExchangeCalendarAuthorityRecord.completeness_status == "complete",
                ExchangeCalendarAuthorityRecord.published_at <= self._request_time,
                ExchangeCalendarAuthorityRecord.created_at <= self._request_time,
                ExchangeCalendarSessionRecord.created_at <= self._request_time,
                ExchangeCalendarSessionRecord.session == local_date,
            )
            .order_by(
                ExchangeCalendarAuthorityRecord.published_at.desc(),
                ExchangeCalendarAuthorityRecord.version.desc(),
                ExchangeCalendarAuthorityRecord.id.desc(),
            )
            .limit(1)
        )
        return cast(date | None, session_date)

    async def _agent_cycle(
        self,
        cycle: AgentPaperCycleRecord,
    ) -> AgentPaperCycleResponse:
        event = await self._session.scalar(
            select(AgentPaperCycleEventRecord)
            .where(
                AgentPaperCycleEventRecord.cycle_id == cycle.id,
                AgentPaperCycleEventRecord.occurred_at <= self._request_time,
                AgentPaperCycleEventRecord.created_at <= self._request_time,
            )
            .order_by(AgentPaperCycleEventRecord.sequence.desc())
            .limit(1)
        )
        if event is None:
            raise NexusError(
                ErrorCode.DATA_PARTIAL, "The Agent paper cycle is incomplete.", status_code=409
            )
        try:
            manifest = parse_candidate_manifest(
                cycle.candidate_manifest,
                allow_legacy=cycle.decision_price_policy_id == PAPER_DECISION_SOURCE_POLICY_ID,
            )
        except (TypeError, ValueError):
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The Agent paper cycle manifest is incomplete.",
                status_code=409,
            ) from None
        decision = await self._session.scalar(
            select(StrategyDecisionRecord).where(
                StrategyDecisionRecord.agent_paper_cycle_id == cycle.id,
                StrategyDecisionRecord.decision_origin == "agent_research",
                StrategyDecisionRecord.created_at <= self._request_time,
            )
        )
        outputs: dict[
            UUID, tuple[AgentResearchOutputRecord, StrategyDecisionAgentOutputRecord]
        ] = {}
        if decision is not None:
            rows = (
                await self._session.execute(
                    select(
                        AgentResearchRunRecord.instrument_id,
                        AgentResearchOutputRecord,
                        StrategyDecisionAgentOutputRecord,
                    )
                    .join(
                        AgentResearchOutputRecord,
                        AgentResearchOutputRecord.run_id == AgentResearchRunRecord.id,
                    )
                    .join(
                        StrategyDecisionAgentOutputRecord,
                        StrategyDecisionAgentOutputRecord.agent_research_output_id
                        == AgentResearchOutputRecord.id,
                    )
                    .where(
                        StrategyDecisionAgentOutputRecord.strategy_decision_id == decision.id,
                        StrategyDecisionAgentOutputRecord.created_at <= self._request_time,
                        AgentResearchOutputRecord.created_at <= self._request_time,
                        AgentResearchRunRecord.created_at <= self._request_time,
                    )
                    .limit(6)
                )
            ).all()
            if len(rows) > 5:
                raise NexusError(
                    ErrorCode.DATA_PARTIAL, "The Agent paper cycle is unbounded.", status_code=409
                )
            outputs = {instrument_id: (output, binding) for instrument_id, output, binding in rows}
        candidates = []
        for candidate in manifest:
            instrument_id = candidate.instrument_id
            bound = outputs.get(instrument_id)
            output, binding = bound if bound is not None else (None, None)
            intent = output.intent if output is not None and isinstance(output.intent, dict) else {}
            stance = intent.get("stance")
            candidates.append(
                AgentPaperCandidateResponse(
                    instrument_id=instrument_id,
                    symbol=candidate.symbol,
                    selection_reasons=(candidate.inclusion_reason,),
                    research_status="completed" if bound is not None else "pending",
                    stance=cast(
                        Literal["buy_candidate", "reduce_candidate", "hold", "watch"] | None,
                        stance
                        if stance in {"buy_candidate", "reduce_candidate", "hold", "watch"}
                        else None,
                    ),
                    sizing_action=cast(
                        Literal["increase", "reduce", "maintain", "none"] | None,
                        None if binding is None else binding.sizing_action,
                    ),
                    intent_hash=None if binding is None else binding.intent_hash,
                    output_id=None if output is None else output.id,
                    rejection_code=None,
                )
            )
        order_ids: tuple[UUID, ...] = ()
        risk_outcomes: tuple[str, ...] = ()
        decision_response = None
        if decision is not None:
            if cycle.sizing_policy_version not in {
                "agent_paper_sizing_v1",
                "portfolio_allocation_v1",
            }:
                raise NexusError(
                    ErrorCode.DATA_PARTIAL,
                    "The Agent paper sizing policy is unknown.",
                    status_code=409,
                )
            order_ids = tuple(
                await self._session.scalars(
                    select(PaperOrderRecord.id)
                    .where(
                        PaperOrderRecord.strategy_decision_id == decision.id,
                        PaperOrderRecord.created_at <= self._request_time,
                    )
                    .order_by(PaperOrderRecord.id)
                    .limit(51)
                )
            )
            if len(order_ids) > 50:
                raise NexusError(
                    ErrorCode.DATA_PARTIAL, "The Agent paper cycle is unbounded.", status_code=409
                )
            risk_outcomes = tuple(
                await self._session.scalars(
                    select(RiskEvaluationRecord.outcome)
                    .join(PaperOrderRecord, PaperOrderRecord.id == RiskEvaluationRecord.order_id)
                    .where(PaperOrderRecord.strategy_decision_id == decision.id)
                    .where(
                        RiskEvaluationRecord.created_at <= self._request_time,
                        PaperOrderRecord.created_at <= self._request_time,
                    )
                    .order_by(PaperOrderRecord.id)
                    .limit(51)
                )
            )
            remaining_risk_outcomes = 50 - len(risk_outcomes)
            no_change_ids = tuple(
                await self._session.scalars(
                    select(RiskNoChangeEvaluationRecord.id)
                    .join(
                        PaperOrderBatchRecord,
                        PaperOrderBatchRecord.id == RiskNoChangeEvaluationRecord.batch_id,
                    )
                    .where(
                        PaperOrderBatchRecord.account_id == decision.account_id,
                        PaperOrderBatchRecord.strategy_decision_id == decision.id,
                        RiskNoChangeEvaluationRecord.created_at <= self._request_time,
                    )
                    .order_by(RiskNoChangeEvaluationRecord.instrument_id)
                    .limit(remaining_risk_outcomes + 1)
                )
            )
            if len(no_change_ids) > remaining_risk_outcomes:
                raise NexusError(
                    ErrorCode.DATA_PARTIAL,
                    "The Agent paper risk projection is unbounded.",
                    status_code=409,
                )
            risk_outcomes += ("no_change",) * len(no_change_ids)
            decision_response = AgentPaperDecisionResponse(
                decision_id=decision.id,
                sizing_policy_version=cast(
                    Literal["agent_paper_sizing_v1", "portfolio_allocation_v1"],
                    cycle.sizing_policy_version,
                ),
                target_cash_weight=decimal_to_string(decision.cash_weight),
                risk_outcomes=cast(
                    tuple[Literal["approved", "reduced", "rejected", "no_change"], ...],
                    risk_outcomes,
                ),
            )
        projected_status = event.to_status
        projected_result_code = event.result_code
        if event.to_status == "awaiting_market" and order_ids:
            latest_order_events = (
                select(
                    PaperOrderEventRecord.order_id,
                    func.max(PaperOrderEventRecord.event_seq).label("event_seq"),
                )
                .where(
                    PaperOrderEventRecord.order_id.in_(order_ids),
                    PaperOrderEventRecord.occurred_at <= self._request_time,
                    PaperOrderEventRecord.created_at <= self._request_time,
                )
                .group_by(PaperOrderEventRecord.order_id)
                .subquery()
            )
            order_statuses = tuple(
                await self._session.scalars(
                    select(PaperOrderEventRecord.to_status)
                    .join(
                        latest_order_events,
                        and_(
                            latest_order_events.c.order_id == PaperOrderEventRecord.order_id,
                            latest_order_events.c.event_seq == PaperOrderEventRecord.event_seq,
                        ),
                    )
                    .order_by(PaperOrderEventRecord.order_id)
                )
            )
            if len(order_statuses) != len(order_ids):
                raise NexusError(
                    ErrorCode.DATA_PARTIAL,
                    "The Agent paper order projection is incomplete.",
                    status_code=409,
                )
            if "partially_filled" in order_statuses:
                projected_status = "partially_filled"
                projected_result_code = "PAPER_ORDERS_PARTIALLY_FILLED"
            elif all(
                status in {"filled", "cancelled", "rejected", "risk_rejected"}
                for status in order_statuses
            ):
                projected_status = "completed"
                projected_result_code = "PAPER_CYCLE_COMPLETED"
        windows = tuple(
            await self._session.scalars(
                select(AgentPaperMarketWindowRecord)
                .where(
                    AgentPaperMarketWindowRecord.cycle_id == cycle.id,
                    AgentPaperMarketWindowRecord.evaluated_at <= self._request_time,
                    AgentPaperMarketWindowRecord.created_at <= self._request_time,
                )
                .order_by(AgentPaperMarketWindowRecord.created_at.desc())
                .limit(51)
            )
        )
        if len(windows) > 50:
            raise NexusError(
                ErrorCode.DATA_PARTIAL, "The Agent paper cycle is unbounded.", status_code=409
            )
        market_status: Literal["pending", "qualified", "probe", "unavailable"] = "pending"
        if any(row.purpose == "paper_fill" for row in windows):
            market_status = "qualified"
        elif windows:
            market_status = "probe"
        elif projected_result_code == "PAPER_MARKET_WINDOW_UNAVAILABLE":
            market_status = "unavailable"
        schwab_execution = (
            cycle.execution_price_policy_id == PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID
        )
        expected_execution = (
            ("schwab", "price_history", "unknown")
            if schwab_execution
            else ("alpaca", "iex", "real_time")
        )
        if any((row.source, row.feed, row.delivery_mode) != expected_execution for row in windows):
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The Agent paper market binding is incomplete.",
                status_code=409,
            )
        decision_market = None
        if (
            cycle.decision_price_policy_id == PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID
            and cycle.candidate_batch_id is None
        ):
            terminal_without_candidate = (
                not manifest
                and projected_status == "unavailable"
                and projected_result_code
                in {
                    "RISK_INPUT_REQUIRED",
                    "OPEN_ORDER_EXISTS",
                    "AGENT_PAPER_PROVIDER_CAPACITY_EXCEEDED",
                }
            )
            if len(manifest) != 1 and not terminal_without_candidate:
                raise NexusError(
                    ErrorCode.DATA_PARTIAL,
                    "The Agent paper decision market is incomplete.",
                    status_code=409,
                )
            if manifest:
                candidate = cast(CandidateManifestEntry, manifest[0])
                quote = await self._session.get(LatestQuote, candidate.decision_quote_id)
                artifact = await self._session.get(DataArtifact, candidate.decision_artifact_id)
                if (
                    quote is None
                    or artifact is None
                    or quote.instrument_id != candidate.instrument_id
                    or quote.data_artifact_id != artifact.id
                    or quote.source != "schwab"
                    or quote.feed != "l1"
                    or quote.delivery_mode != "real_time"
                    or quote.event_kind != "quote"
                    or quote.evidence_status != "verified"
                    or quote.bid is None
                    or quote.ask is None
                    or quote.bid <= 0
                    or quote.ask <= 0
                    or quote.bid > quote.ask
                    or quote.available_at > cycle.decision_time
                    or quote.received_at > cycle.decision_time
                    or artifact.source != "schwab"
                    or artifact.schema_version != "schwab_level_one_stream_v1"
                    or artifact.sha256 != candidate.decision_evidence_hash
                ):
                    raise NexusError(
                        ErrorCode.DATA_PARTIAL,
                        "The Agent paper decision market is incomplete.",
                        status_code=409,
                    )
                decision_market = AgentPaperDecisionMarketResponse(
                    source="schwab",
                    feed="l1",
                    delivery_mode="real_time",
                    delayed=False,
                    quote_at=quote.observed_at,
                )
        return AgentPaperCycleResponse(
            cycle_id=cycle.id,
            status=cast(Any, projected_status),
            result_code=projected_result_code,
            decision_session=cycle.decision_session,
            decision_time=cycle.decision_time,
            decision_slot_at=cycle.decision_slot_at,
            execution_session=cycle.execution_session,
            execution_locked=True,
            candidate_count=len(candidates),
            candidates=tuple(candidates),
            decision=decision_response,
            decision_market=decision_market,
            execution_market=(
                AgentPaperSchwabExecutionMarketResponse(
                    source="schwab",
                    feed="price_history",
                    delivery_mode="unknown",
                    window_minutes=5,
                    status=market_status,
                )
                if schwab_execution
                else AgentPaperAlpacaExecutionMarketResponse(
                    source="alpaca",
                    feed="iex",
                    delivery_mode="real_time",
                    window_minutes=5,
                    status=market_status,
                )
            ),
            order_ids=order_ids,
        )

    async def portfolio(self) -> PaperPortfolioResponse:
        account, portfolio = await self.account()
        snapshot = await self._session.scalar(
            select(PaperPerformanceSnapshotRecord)
            .where(
                PaperPerformanceSnapshotRecord.account_id == account.id,
                PaperPerformanceSnapshotRecord.as_of <= self._request_time,
            )
            .order_by(PaperPerformanceSnapshotRecord.as_of.desc())
            .limit(1)
        )
        if snapshot is None:
            return PaperPortfolioResponse(
                portfolio_id=portfolio.id,
                name=portfolio.name,
                mode="internal_paper",
                currency="USD",
                initial_capital=decimal_to_string(account.initial_capital),
                data_state="EMPTY",
                performance=None,
                comparison=None,
                positions=(),
                attributions=(),
            )

        projection = await load_paper_projection(
            self._session,
            account_id=account.id,
            as_of=snapshot.as_of,
        )
        if projection.portfolio_id != portfolio.id:
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The internal paper projection is unavailable.",
                status_code=409,
            )

        position_rows = (
            await self._session.execute(
                select(
                    PaperPositionSnapshotRecord,
                    PriceBarSetAuthority,
                    PriceSeriesManifest,
                )
                .join(
                    PriceBarSetAuthority,
                    PriceBarSetAuthority.id == PaperPositionSnapshotRecord.price_authority_id,
                )
                .join(
                    PriceSeriesManifest,
                    PriceSeriesManifest.id == PriceBarSetAuthority.price_series_manifest_id,
                )
                .where(
                    PaperPositionSnapshotRecord.account_id == account.id,
                    PaperPositionSnapshotRecord.as_of == snapshot.as_of,
                )
                .order_by(PaperPositionSnapshotRecord.instrument_id)
                .limit(_MAX_POSITIONS + 1)
            )
        ).all()
        _require_bounded("positions", position_rows, _MAX_POSITIONS)
        attribution_rows = tuple(
            (
                await self._session.scalars(
                    select(PaperPerformanceAttributionRecord)
                    .where(PaperPerformanceAttributionRecord.performance_snapshot_id == snapshot.id)
                    .order_by(
                        PaperPerformanceAttributionRecord.dimension,
                        PaperPerformanceAttributionRecord.dimension_key,
                    )
                    .limit(_MAX_ATTRIBUTIONS + 1)
                )
            ).all()
        )
        _require_bounded("attributions", attribution_rows, _MAX_ATTRIBUTIONS)
        symbols = await self._symbols(
            tuple(row.instrument_id for row, _, _ in position_rows),
            snapshot.as_of,
        )
        positions = tuple(
            PaperPositionResponse(
                instrument_id=row.instrument_id,
                symbol=symbols.get(row.instrument_id),
                quantity=decimal_to_string(row.quantity),
                reserved_quantity=decimal_to_string(row.reserved_quantity),
                available_quantity=decimal_to_string(row.quantity - row.reserved_quantity),
                cost_basis=decimal_to_string(row.cost_basis),
                average_cost=decimal_to_string(
                    Decimal(0) if row.quantity == 0 else row.cost_basis / row.quantity
                ),
                reference_close=decimal_to_string(row.reference_close),
                market_value=decimal_to_string(row.quantity * row.reference_close),
                unrealized_pnl=decimal_to_string(
                    row.quantity * row.reference_close - row.cost_basis
                ),
                price_source=manifest.source,
                price_feed=manifest.feed,
                delivery_mode=cast(
                    Literal["real_time", "delayed", "fixture", "unknown"],
                    manifest.delivery_mode,
                ),
                price_as_of=snapshot.as_of,
            )
            for row, _, manifest in position_rows
        )
        attributions = tuple(
            PaperAttributionResponse(
                dimension=cast(Literal["cash", "instrument", "sector"], row.dimension),
                dimension_key=row.dimension_key,
                market_value=decimal_to_string(row.market_value),
                realized_pnl=decimal_to_string(row.realized_pnl),
                unrealized_pnl=decimal_to_string(row.unrealized_pnl),
                fees=decimal_to_string(row.fees),
                slippage=decimal_to_string(row.slippage),
                completeness=cast(Literal["FULL", "PARTIAL"], row.completeness),
            )
            for row in attribution_rows
        )
        market_value = sum(
            (row.quantity * row.reference_close for row, _, _ in position_rows),
            start=Decimal(0),
        )
        state: Literal["READY", "PARTIAL", "STALE"]
        if self._request_time - snapshot.as_of > _STALE_AFTER:
            state = "STALE"
        elif any(row.completeness == "PARTIAL" for row in attribution_rows):
            state = "PARTIAL"
        else:
            state = "READY"
        return PaperPortfolioResponse(
            portfolio_id=portfolio.id,
            name=portfolio.name,
            mode="internal_paper",
            currency="USD",
            initial_capital=decimal_to_string(account.initial_capital),
            data_state=state,
            performance=PaperPerformanceResponse(
                as_of=snapshot.as_of,
                cash=decimal_to_string(snapshot.cash),
                available_cash=decimal_to_string(projection.available_cash),
                reserved_cash=decimal_to_string(projection.reserved_cash),
                market_value=decimal_to_string(market_value),
                nav=decimal_to_string(snapshot.nav),
                peak_nav=decimal_to_string(snapshot.peak_nav),
                realized_pnl=decimal_to_string(snapshot.realized_pnl),
                unrealized_pnl=decimal_to_string(snapshot.unrealized_pnl),
                fees=decimal_to_string(snapshot.fees),
                drawdown_ratio=decimal_to_string(snapshot.drawdown_ratio),
            ),
            comparison=await self._paper_comparison(
                account=account,
                snapshot=snapshot,
                data_state=state,
            ),
            positions=positions,
            attributions=attributions,
        )

    async def management_summary(self) -> PaperManagementSummaryResponse:
        account, _ = await self.account()
        account_id = account.id
        current_target = await self._current_target_summary(account_id)
        occupying_plan = await self._occupying_plan_summary(account_id)
        decision = await self._session.scalar(
            select(StrategyDecisionRecord)
            .where(
                StrategyDecisionRecord.account_id == account_id,
                StrategyDecisionRecord.strategy_version == "agent_portfolio_proposal_v1",
                StrategyDecisionRecord.created_at <= self._request_time,
            )
            .order_by(StrategyDecisionRecord.decision_time.desc(), StrategyDecisionRecord.id.desc())
            .limit(1)
        )
        proposal = None
        if decision is not None:
            proposal = await self._portfolio_proposal(decision)
        return PaperManagementSummaryResponse(
            current_target=current_target,
            occupying_plan=occupying_plan,
            latest_proposal=proposal,
            read_as_of=self._request_time,
        )

    async def _paper_comparison(
        self,
        *,
        account: PaperAccountRecord,
        snapshot: PaperPerformanceSnapshotRecord,
        data_state: Literal["READY", "PARTIAL", "STALE"],
    ) -> PaperComparisonResponse:
        max_drawdown = await self._session.scalar(
            select(func.max(PaperPerformanceSnapshotRecord.drawdown_ratio)).where(
                PaperPerformanceSnapshotRecord.account_id == account.id,
                PaperPerformanceSnapshotRecord.as_of <= snapshot.as_of,
            )
        )
        fill_rows = (
            await self._session.execute(
                select(
                    StrategyDecisionRecord.decision_origin,
                    func.sum(PaperFillRecord.notional),
                    func.sum(PaperFillRecord.slippage_amount),
                )
                .select_from(PaperFillRecord)
                .join(PaperOrderRecord, PaperOrderRecord.id == PaperFillRecord.order_id)
                .join(
                    StrategyDecisionRecord,
                    StrategyDecisionRecord.id == PaperOrderRecord.strategy_decision_id,
                )
                .where(
                    PaperFillRecord.account_id == account.id,
                    PaperFillRecord.occurred_at <= snapshot.as_of,
                )
                .group_by(StrategyDecisionRecord.decision_origin)
            )
        ).all()
        mixed_strategy_facts = any(origin != "agent_research" for origin, _, _ in fill_rows)
        agent_turnover = sum(
            (notional for origin, notional, _ in fill_rows if origin == "agent_research"),
            start=Decimal(0),
        )
        agent_slippage = sum(
            (slippage for origin, _, slippage in fill_rows if origin == "agent_research"),
            start=Decimal(0),
        )
        agent_row = (
            PaperComparisonRowResponse(
                key="agent",
                data_state="UNAVAILABLE",
                nav=None,
                total_return_ratio=None,
                max_drawdown_ratio=None,
                turnover_ratio=None,
                total_cost=None,
                reason_code="MIXED_STRATEGY_FACTS_UNAVAILABLE",
            )
            if mixed_strategy_facts
            else PaperComparisonRowResponse(
                key="agent",
                data_state=data_state,
                nav=decimal_to_string(snapshot.nav),
                total_return_ratio=decimal_to_string(
                    snapshot.nav / account.initial_capital - Decimal(1)
                ),
                max_drawdown_ratio=decimal_to_string(
                    snapshot.drawdown_ratio if max_drawdown is None else max_drawdown
                ),
                turnover_ratio=decimal_to_string(agent_turnover / account.initial_capital),
                total_cost=decimal_to_string(snapshot.fees + agent_slippage),
                reason_code="PERSISTED_AGENT_PERFORMANCE",
            )
        )
        benchmark_row = await self._market_benchmark_row(
            account=account,
            snapshot=snapshot,
        )
        return PaperComparisonResponse(
            window_start=account.created_at,
            as_of=snapshot.as_of,
            rows=(
                agent_row,
                PaperComparisonRowResponse(
                    key="hold_current",
                    data_state="READY",
                    nav=decimal_to_string(account.initial_capital),
                    total_return_ratio=decimal_to_string(Decimal(0)),
                    max_drawdown_ratio=decimal_to_string(Decimal(0)),
                    turnover_ratio=decimal_to_string(Decimal(0)),
                    total_cost=decimal_to_string(Decimal(0)),
                    reason_code="CASH_HOLD_CURRENT",
                ),
                PaperComparisonRowResponse(
                    key="baseline_v1",
                    data_state="UNAVAILABLE",
                    nav=None,
                    total_return_ratio=None,
                    max_drawdown_ratio=None,
                    turnover_ratio=None,
                    total_cost=None,
                    reason_code="SAME_WINDOW_FORWARD_FACTS_UNAVAILABLE",
                ),
                benchmark_row,
            ),
        )

    async def _market_benchmark_row(
        self,
        *,
        account: PaperAccountRecord,
        snapshot: PaperPerformanceSnapshotRecord,
    ) -> PaperComparisonRowResponse:
        unavailable = market_benchmark_row_from_closes(
            initial_capital=account.initial_capital,
            closes=(),
        )
        start_session = account.created_at.astimezone(ZoneInfo("America/New_York")).date()
        end_session = snapshot.as_of.astimezone(ZoneInfo("America/New_York")).date()
        expected_sessions = tuple(
            await self._session.scalars(
                select(ExchangeCalendarSessionRecord.session)
                .join(
                    ExchangeCalendarAuthorityRecord,
                    ExchangeCalendarAuthorityRecord.id
                    == ExchangeCalendarSessionRecord.calendar_authority_id,
                )
                .distinct()
                .where(
                    ExchangeCalendarAuthorityRecord.exchange == "XNYS",
                    ExchangeCalendarAuthorityRecord.completeness_status == "complete",
                    ExchangeCalendarAuthorityRecord.published_at <= self._request_time,
                    ExchangeCalendarAuthorityRecord.created_at <= self._request_time,
                    ExchangeCalendarSessionRecord.session >= start_session,
                    ExchangeCalendarSessionRecord.session <= end_session,
                )
                .order_by(ExchangeCalendarSessionRecord.session)
            )
        )
        if len(expected_sessions) < 2:
            return unavailable
        candidate = (
            await self._session.execute(
                select(PriceBarSetAuthority, PriceSeriesManifest)
                .join(
                    PriceSeriesManifest,
                    PriceSeriesManifest.id == PriceBarSetAuthority.price_series_manifest_id,
                )
                .where(
                    PriceBarSetAuthority.source_policy_id == PAPER_DECISION_SOURCE_POLICY_ID,
                    PriceBarSetAuthority.first_session <= expected_sessions[0],
                    PriceBarSetAuthority.last_session >= expected_sessions[-1],
                    PriceBarSetAuthority.available_at <= self._request_time,
                    PriceBarSetAuthority.created_at <= self._request_time,
                    PriceSeriesManifest.instrument == str(stable_instrument_id("SPY")),
                    PriceSeriesManifest.source == "alpaca",
                    PriceSeriesManifest.feed == "sip",
                    PriceSeriesManifest.delivery_mode == "delayed",
                    PriceSeriesManifest.adjustment == "all",
                    PriceSeriesManifest.interval == "1Day",
                    PriceSeriesManifest.created_at <= self._request_time,
                )
                .order_by(
                    PriceBarSetAuthority.available_at.desc(),
                    PriceBarSetAuthority.id,
                )
                .limit(1)
            )
        ).one_or_none()
        if candidate is None:
            return unavailable
        authority, manifest = candidate
        bars = tuple(
            await self._session.scalars(
                select(PriceBarRecord)
                .where(
                    PriceBarRecord.price_series_manifest_id == manifest.id,
                    PriceBarRecord.instrument_id == stable_instrument_id("SPY"),
                    PriceBarRecord.session >= expected_sessions[0],
                    PriceBarRecord.session <= expected_sessions[-1],
                    PriceBarRecord.available_at <= self._request_time,
                    PriceBarRecord.received_at <= self._request_time,
                    PriceBarRecord.created_at <= self._request_time,
                )
                .order_by(PriceBarRecord.session, PriceBarRecord.observed_at)
            )
        )
        if (
            authority.price_series_manifest_id != manifest.id
            or tuple(bar.session for bar in bars) != expected_sessions
        ):
            return unavailable
        return market_benchmark_row_from_closes(
            initial_capital=account.initial_capital,
            closes=tuple(bar.close for bar in bars),
        )

    async def orders(
        self,
        *,
        status: InternalOrderStatus | None,
        cursor: str | None,
        limit: int,
    ) -> tuple[PaperOrderPageResponse, datetime]:
        account, _ = await self.account()
        codec = PaperOrderCursorCodec(self._cursor_secrets, self._request_time)
        state = codec.decode(cursor, requested_status=status) if cursor else None
        decision_time = state.decision_time if state is not None else self._request_time
        latest_events = (
            select(
                PaperOrderEventRecord.order_id,
                func.max(PaperOrderEventRecord.event_seq).label("event_seq"),
            )
            .where(
                PaperOrderEventRecord.account_id == account.id,
                PaperOrderEventRecord.occurred_at <= decision_time,
            )
            .group_by(PaperOrderEventRecord.order_id)
            .subquery()
        )
        statement = (
            select(PaperOrderRecord, PaperOrderEventRecord)
            .join(latest_events, latest_events.c.order_id == PaperOrderRecord.id)
            .join(
                PaperOrderEventRecord,
                and_(
                    PaperOrderEventRecord.order_id == latest_events.c.order_id,
                    PaperOrderEventRecord.event_seq == latest_events.c.event_seq,
                ),
            )
            .where(
                PaperOrderRecord.account_id == account.id,
                PaperOrderRecord.created_at <= decision_time,
            )
            .order_by(desc(PaperOrderRecord.created_at), desc(PaperOrderRecord.id))
            .limit(limit + 1)
        )
        if status is not None:
            statement = statement.where(PaperOrderEventRecord.to_status == status)
        if state is not None:
            statement = statement.where(
                or_(
                    PaperOrderRecord.created_at < state.after_created_at,
                    and_(
                        PaperOrderRecord.created_at == state.after_created_at,
                        PaperOrderRecord.id < state.after_order_id,
                    ),
                )
            )
        rows = (await self._session.execute(statement)).all()
        has_more = len(rows) > limit
        page_rows = rows[:limit]
        items = tuple(
            [
                await self._order(
                    order,
                    latest_event=latest_event,
                    decision_time=decision_time,
                )
                for order, latest_event in page_rows
            ]
        )
        next_cursor = None
        if has_more and page_rows:
            last_order = page_rows[-1][0]
            next_cursor = codec.encode(
                PaperOrderCursorState(
                    decision_time=decision_time,
                    after_created_at=last_order.created_at,
                    after_order_id=last_order.id,
                    status=status,
                )
            )
        return PaperOrderPageResponse(items=items, next_cursor=next_cursor), decision_time

    async def active_orders(self, *, limit: int) -> tuple[PaperOrderResponse, ...]:
        if not 1 <= limit <= 20:
            raise ValueError("limit must be between 1 and 20")
        account, _ = await self.account()
        latest_events = (
            select(
                PaperOrderEventRecord.order_id,
                func.max(PaperOrderEventRecord.event_seq).label("event_seq"),
            )
            .where(
                PaperOrderEventRecord.account_id == account.id,
                PaperOrderEventRecord.occurred_at <= self._request_time,
                PaperOrderEventRecord.created_at <= self._request_time,
            )
            .group_by(PaperOrderEventRecord.order_id)
            .subquery()
        )
        terminal = ("risk_rejected", "filled", "cancelled", "rejected")
        rows = (
            await self._session.execute(
                select(PaperOrderRecord, PaperOrderEventRecord)
                .join(latest_events, latest_events.c.order_id == PaperOrderRecord.id)
                .join(
                    PaperOrderEventRecord,
                    and_(
                        PaperOrderEventRecord.order_id == latest_events.c.order_id,
                        PaperOrderEventRecord.event_seq == latest_events.c.event_seq,
                    ),
                )
                .where(
                    PaperOrderRecord.account_id == account.id,
                    PaperOrderRecord.created_at <= self._request_time,
                    PaperOrderEventRecord.to_status.not_in(terminal),
                )
                .order_by(desc(PaperOrderRecord.created_at), desc(PaperOrderRecord.id))
                .limit(limit)
            )
        ).all()
        return tuple(
            [
                await self._order(
                    order,
                    latest_event=latest_event,
                    decision_time=self._request_time,
                )
                for order, latest_event in rows
            ]
        )

    async def _orders_for_cycle(self, cycle_id: UUID) -> tuple[PaperOrderResponse, ...]:
        account, _ = await self.account()
        latest_events = (
            select(
                PaperOrderEventRecord.order_id,
                func.max(PaperOrderEventRecord.event_seq).label("event_seq"),
            )
            .where(
                PaperOrderEventRecord.account_id == account.id,
                PaperOrderEventRecord.occurred_at <= self._request_time,
                PaperOrderEventRecord.created_at <= self._request_time,
            )
            .group_by(PaperOrderEventRecord.order_id)
            .subquery()
        )
        rows = (
            await self._session.execute(
                select(PaperOrderRecord, PaperOrderEventRecord)
                .join(
                    StrategyDecisionRecord,
                    StrategyDecisionRecord.id == PaperOrderRecord.strategy_decision_id,
                )
                .outerjoin(latest_events, latest_events.c.order_id == PaperOrderRecord.id)
                .outerjoin(
                    PaperOrderEventRecord,
                    and_(
                        PaperOrderEventRecord.order_id == latest_events.c.order_id,
                        PaperOrderEventRecord.event_seq == latest_events.c.event_seq,
                    ),
                )
                .where(
                    StrategyDecisionRecord.agent_paper_cycle_id == cycle_id,
                    StrategyDecisionRecord.decision_origin == "agent_research",
                    StrategyDecisionRecord.created_at <= self._request_time,
                    PaperOrderRecord.account_id == account.id,
                    PaperOrderRecord.created_at <= self._request_time,
                )
                .order_by(desc(PaperOrderRecord.created_at), desc(PaperOrderRecord.id))
                .limit(_MAX_AGENT_ORDERS_PER_CYCLE + 1)
            )
        ).all()
        if len(rows) > _MAX_AGENT_ORDERS_PER_CYCLE or any(event is None for _, event in rows):
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The Agent paper order projection is incomplete.",
                status_code=409,
            )
        return tuple(
            [
                await self._order(
                    order,
                    latest_event=event,
                    decision_time=self._request_time,
                )
                for order, event in rows
                if event is not None
            ]
        )

    async def owns_order(self, order_id: UUID) -> bool:
        account, _ = await self.account()
        return (
            await self._session.scalar(
                select(PaperOrderRecord.id).where(
                    PaperOrderRecord.id == order_id,
                    PaperOrderRecord.account_id == account.id,
                )
            )
            is not None
        )

    async def cancel_result(
        self,
        order_id: UUID,
        idempotency_key: str,
    ) -> PaperCancelResultResponse | None:
        """Read one prior cancellation attempt without changing persisted state."""
        account, _ = await self.account()
        order = await self._session.scalar(
            select(PaperOrderRecord).where(
                PaperOrderRecord.id == order_id,
                PaperOrderRecord.account_id == account.id,
                PaperOrderRecord.created_at <= self._request_time,
            )
        )
        if order is None:
            return None
        events = tuple(
            (
                await self._session.scalars(
                    select(PaperOrderEventRecord)
                    .where(
                        PaperOrderEventRecord.order_id == order.id,
                        PaperOrderEventRecord.occurred_at <= self._request_time,
                        PaperOrderEventRecord.created_at <= self._request_time,
                    )
                    .order_by(PaperOrderEventRecord.event_seq)
                    .limit(_MAX_EVENTS + 1)
                )
            ).all()
        )
        _require_bounded("order_events", events, _MAX_EVENTS)
        fills = tuple(
            (
                await self._session.scalars(
                    select(PaperFillRecord)
                    .where(
                        PaperFillRecord.order_id == order.id,
                        PaperFillRecord.occurred_at <= self._request_time,
                        PaperFillRecord.created_at <= self._request_time,
                    )
                    .order_by(PaperFillRecord.fill_seq)
                    .limit(_MAX_FILLS + 1)
                )
            ).all()
        )
        _require_bounded("fills", fills, _MAX_FILLS)
        windows = {
            row.id: row
            for row in (
                await self._session.scalars(
                    select(PaperExecutionWindowAuthorityRecord).where(
                        PaperExecutionWindowAuthorityRecord.id.in_(
                            tuple(fill.execution_window_authority_id for fill in fills)
                        )
                    )
                )
            ).all()
        }
        filled_quantity = sum((row.quantity for row in fills), start=Decimal(0))
        latest_event = events[-1] if events else None
        order_valid = _order_chain_is_valid(order, events) and _fill_chain_is_valid(
            order, fills, windows, events
        )
        order_fact = PaperCancelOrderFactResponse(
            order_id=order.id,
            status=(
                cast(InternalOrderStatus, latest_event.to_status)
                if latest_event is not None
                else None
            ),
            current_event_seq=(latest_event.event_seq if latest_event is not None else None),
            filled_quantity=decimal_to_string(filled_quantity),
            remaining_quantity=decimal_to_string(order.quantity - filled_quantity),
            as_of=(latest_event.occurred_at if latest_event is not None else None),
        )
        reservation_fact, reservation_valid = await self._cancel_reservation_fact(
            order,
            order_events=events,
        )
        claim = await self._session.scalar(
            select(PaperRequestIdempotencyRecord).where(
                PaperRequestIdempotencyRecord.account_id == account.id,
                PaperRequestIdempotencyRecord.operation == "cancel",
                PaperRequestIdempotencyRecord.idempotency_key == idempotency_key,
                PaperRequestIdempotencyRecord.created_at <= self._request_time,
            )
        )
        status: Literal["NOT_RECORDED", "COMMITTED", "CONFLICT", "UNKNOWN"]
        committed_at: datetime | None = None
        result_event_seq: int | None = None
        if claim is None:
            status = "NOT_RECORDED"
        elif claim.order_id != order.id:
            status = "CONFLICT"
        elif claim.result_event_seq is None or claim.result_event_seq < 2:
            status = "UNKNOWN"
        else:
            expected_request_hash = canonical_cancel_request_hash(
                order_id=order.id,
                expected_event_seq=claim.result_event_seq - 1,
            )
            if claim.request_hash != expected_request_hash:
                status = "CONFLICT"
            else:
                result_event = next(
                    (event for event in events if event.event_seq == claim.result_event_seq),
                    None,
                )
                if result_event is None or result_event.to_status != "cancelled":
                    status = "UNKNOWN"
                else:
                    committed_fills = sum(
                        (
                            row.quantity
                            for row in fills
                            if row.occurred_at <= result_event.occurred_at
                        ),
                        start=Decimal(0),
                    )
                    expected_result_hash = canonical_cancel_result_hash(
                        order_id=order.id,
                        account_id=order.account_id,
                        total_quantity=order.quantity,
                        filled_quantity=committed_fills,
                        result_event_seq=result_event.event_seq,
                    )
                    if (
                        result_event.actor_kind != "user"
                        or result_event.reason_code != "paper_user_cancelled"
                        or result_event.from_status not in {"submitted", "partially_filled"}
                        or result_event.source_fill_id is not None
                        or claim.result_hash != expected_result_hash
                    ):
                        status = "UNKNOWN"
                    else:
                        status = "COMMITTED"
                        committed_at = result_event.occurred_at
                        result_event_seq = result_event.event_seq
        if status != "CONFLICT" and (
            not order_valid
            or not reservation_valid
            or (status == "COMMITTED" and reservation_fact is None)
        ):
            status = "UNKNOWN"
            committed_at = None
            result_event_seq = None
        return PaperCancelResultResponse(
            status=status,
            order=order_fact,
            reservation=reservation_fact,
            reservation_released=(None if reservation_fact is None else reservation_fact.released),
            result_event_seq=result_event_seq,
            committed_at=committed_at,
        )

    async def plan(self, batch_id: UUID) -> PaperPlanResponse | None:
        """Project one persisted paper plan without advancing its lifecycle."""
        account, _ = await self.account()
        batch = await self._session.scalar(
            select(PaperOrderBatchRecord).where(
                PaperOrderBatchRecord.id == batch_id,
                PaperOrderBatchRecord.account_id == account.id,
                PaperOrderBatchRecord.created_at <= self._request_time,
            )
        )
        if batch is None:
            return None
        decision = await self._session.scalar(
            select(StrategyDecisionRecord).where(
                StrategyDecisionRecord.id == batch.strategy_decision_id,
                StrategyDecisionRecord.account_id == account.id,
                StrategyDecisionRecord.created_at <= self._request_time,
            )
        )
        if decision is None:
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The paper plan proposal is unavailable.",
                status_code=409,
            )
        from nexus.paper.agent_decision_store import (
            load_paper_decision,
        )
        from nexus.paper.errors import PaperBatchError
        from nexus.paper.service import load_paper_order_batch_result

        try:
            canonical_batch = await load_paper_order_batch_result(
                self._session,
                account_id=account.id,
                strategy_decision_id=decision.id,
            )
        except (PaperBatchError, ValueError):
            canonical_batch = None
        if canonical_batch is None or canonical_batch.batch_id != batch.id:
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The paper plan authority chain is incomplete.",
                status_code=409,
            )
        target = None
        if batch.target_version_id is not None:
            try:
                batch_proposal = await load_proposal_read_authority(
                    self._session,
                    account_id=account.id,
                    decision_id=decision.id,
                    read_as_of=self._request_time,
                )
                target_authority = await load_target_read_authority(
                    self._session,
                    account_id=account.id,
                    target_id=batch.target_version_id,
                    read_as_of=self._request_time,
                )
            except ValueError as error:
                raise NexusError(
                    ErrorCode.DATA_PARTIAL,
                    "The paper plan proposal authority is incomplete.",
                    status_code=409,
                ) from error
            if (
                batch_proposal is None
                or target_authority is None
                or not portfolio_target_matches_batch(
                    account_id=account.id,
                    portfolio_id=account.portfolio_id,
                    batch_decision_id=decision.id,
                    batch_decision_hash=canonical_batch.decision_hash,
                    proposal_decision_id=batch_proposal.decision.id,
                    proposal_decision_hash=batch_proposal.persisted.proposal.decision_hash,
                    proposal_target_content_hash=canonical_portfolio_target_content_hash(
                        batch_proposal.persisted.proposal
                    ),
                    target_account_id=target_authority.target.account_id,
                    target_portfolio_id=target_authority.target.portfolio_id,
                    target_id=target_authority.target.id,
                    expected_target_id=batch.target_version_id,
                    target_decision_id=target_authority.target.strategy_decision_id,
                    target_proposal_decision_id=target_authority.proposal.decision.id,
                    target_content_hash=target_authority.target.target_content_hash,
                )
            ):
                raise NexusError(
                    ErrorCode.DATA_PARTIAL,
                    "The paper plan target is unavailable.",
                    status_code=409,
                )
            target = PaperPlanTargetResponse(
                target_version_id=target_authority.target.id,
                version_seq=target_authority.target.version_seq,
                target_content_hash=target_authority.target.target_content_hash,
                as_of=target_authority.target.activated_at,
            )
        else:
            try:
                legacy_authority = await load_paper_decision(
                    self._session,
                    account_id=account.id,
                    decision_id=decision.id,
                )
            except ValueError:
                legacy_authority = None
            if legacy_authority is None or legacy_authority.decision_id != decision.id:
                raise NexusError(
                    ErrorCode.DATA_PARTIAL,
                    "The legacy paper plan decision is incomplete.",
                    status_code=409,
                )
        proposal_targets = tuple(
            (
                await self._session.scalars(
                    select(StrategyDecisionTargetRecord)
                    .where(
                        StrategyDecisionTargetRecord.strategy_decision_id == decision.id,
                        StrategyDecisionTargetRecord.created_at <= self._request_time,
                    )
                    .order_by(StrategyDecisionTargetRecord.instrument_id)
                    .limit(_MAX_AGENT_ORDERS_PER_CYCLE + 1)
                )
            ).all()
        )
        _require_bounded(
            "strategy_decision_targets",
            proposal_targets,
            _MAX_AGENT_ORDERS_PER_CYCLE,
        )
        proposal = PaperPlanProposalResponse(
            strategy_decision_id=decision.id,
            decision_hash=decision.decision_hash,
            cash_weight=decimal_to_string(decision.cash_weight),
            targets=tuple(
                PaperPlanProposalTargetResponse(
                    instrument_id=row.instrument_id,
                    target_weight=decimal_to_string(row.target_weight),
                    exit=row.exit,
                )
                for row in proposal_targets
            ),
            as_of=decision.decision_time,
        )
        order_rows = tuple(
            (
                await self._session.scalars(
                    select(PaperOrderRecord)
                    .where(
                        PaperOrderRecord.batch_id == batch.id,
                        PaperOrderRecord.account_id == account.id,
                        PaperOrderRecord.created_at <= self._request_time,
                    )
                    .order_by(PaperOrderRecord.id)
                    .limit(_MAX_AGENT_ORDERS_PER_CYCLE + 1)
                )
            ).all()
        )
        _require_bounded("paper_plan_orders", order_rows, _MAX_AGENT_ORDERS_PER_CYCLE)
        risk_rows = tuple(
            (
                await self._session.scalars(
                    select(RiskEvaluationRecord)
                    .where(
                        RiskEvaluationRecord.batch_id == batch.id,
                        RiskEvaluationRecord.account_id == account.id,
                        RiskEvaluationRecord.created_at <= self._request_time,
                    )
                    .order_by(RiskEvaluationRecord.order_id)
                    .limit(_MAX_AGENT_ORDERS_PER_CYCLE + 1)
                )
            ).all()
        )
        _require_bounded("paper_plan_risk", risk_rows, _MAX_AGENT_ORDERS_PER_CYCLE)
        order_by_id = {order.id: order for order in order_rows}
        if (
            len(order_by_id) != len(order_rows)
            or len(risk_rows) != len(order_rows)
            or {row.order_id for row in risk_rows} != set(order_by_id)
        ):
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The paper plan risk facts are incomplete.",
                status_code=409,
            )
        risk_by_order = {row.order_id: row for row in risk_rows}
        ledger_times = tuple(
            (
                await self._session.scalars(
                    select(PaperLedgerTransactionRecord.occurred_at).where(
                        PaperLedgerTransactionRecord.order_id.in_(
                            tuple(order.id for order in order_rows)
                        ),
                        PaperLedgerTransactionRecord.occurred_at <= self._request_time,
                    )
                )
            ).all()
        )
        risk = tuple(
            PaperPlanRiskResponse(
                order_id=row.order_id,
                instrument_id=order_by_id[row.order_id].instrument_id,
                outcome=cast(Literal["approved", "reduced", "rejected"], row.outcome),
                desired_quantity=decimal_to_string(row.desired_quantity),
                approved_quantity=decimal_to_string(row.approved_quantity),
                reduced_quantity=decimal_to_string(
                    row.desired_quantity - row.approved_quantity
                    if row.outcome == "reduced"
                    else Decimal(0)
                ),
                rejected_quantity=decimal_to_string(
                    row.desired_quantity if row.outcome == "rejected" else Decimal(0)
                ),
                desired_amount_usd=decimal_to_string(row.desired_quantity * row.worst_price),
                approved_amount_usd=decimal_to_string(row.approved_quantity * row.worst_price),
                reduced_amount_usd=decimal_to_string(
                    (row.desired_quantity - row.approved_quantity) * row.worst_price
                    if row.outcome == "reduced"
                    else Decimal(0)
                ),
                rejected_amount_usd=decimal_to_string(
                    row.desired_quantity * row.worst_price
                    if row.outcome == "rejected"
                    else Decimal(0)
                ),
            )
            for row in risk_rows
        )
        items: list[PaperPlanItemResponse] = []
        reconciliation_times = [batch.evaluated_at, *ledger_times]
        occupation_unknown = False
        occupied = False
        for order in order_rows:
            events = tuple(
                (
                    await self._session.scalars(
                        select(PaperOrderEventRecord)
                        .where(
                            PaperOrderEventRecord.order_id == order.id,
                            PaperOrderEventRecord.occurred_at <= self._request_time,
                            PaperOrderEventRecord.created_at <= self._request_time,
                        )
                        .order_by(PaperOrderEventRecord.event_seq)
                        .limit(_MAX_EVENTS + 1)
                    )
                ).all()
            )
            _require_bounded("paper_plan_order_events", events, _MAX_EVENTS)
            fills = tuple(
                (
                    await self._session.scalars(
                        select(PaperFillRecord)
                        .where(
                            PaperFillRecord.order_id == order.id,
                            PaperFillRecord.occurred_at <= self._request_time,
                            PaperFillRecord.created_at <= self._request_time,
                        )
                        .order_by(PaperFillRecord.fill_seq)
                        .limit(_MAX_FILLS + 1)
                    )
                ).all()
            )
            _require_bounded("paper_plan_fills", fills, _MAX_FILLS)
            windows = {
                row.id: row
                for row in (
                    await self._session.scalars(
                        select(PaperExecutionWindowAuthorityRecord).where(
                            PaperExecutionWindowAuthorityRecord.id.in_(
                                tuple(fill.execution_window_authority_id for fill in fills)
                            )
                        )
                    )
                ).all()
            }
            reconciliation_times.extend(event.occurred_at for event in events)
            reconciliation_times.extend(fill.occurred_at for fill in fills)
            filled = sum((row.quantity for row in fills), start=Decimal(0))
            latest = events[-1] if events else None
            latest_status = (
                cast(InternalOrderStatus, latest.to_status) if latest is not None else None
            )
            reservation_fact, reservation_valid = await self._cancel_reservation_fact(
                order,
                order_events=events,
            )
            if reservation_fact is not None and reservation_fact.as_of is not None:
                reconciliation_times.append(reservation_fact.as_of)
            if (
                not reservation_valid
                or not _order_chain_is_valid(order, events)
                or not _fill_chain_is_valid(order, fills, windows, events)
            ):
                raise NexusError(
                    ErrorCode.DATA_PARTIAL,
                    "The paper plan execution fact chain is incomplete.",
                    status_code=409,
                )
            if latest_status == "reconciling":
                occupation_unknown = True
            is_open = latest_status in {
                "risk_approved",
                "submitted",
                "partially_filled",
                "reconciling",
            }
            open_coverage = max(order.quantity - filled, Decimal(0)) if is_open else Decimal(0)
            risk_row = risk_by_order[order.id]
            desired = risk_row.desired_quantity
            remaining_gap = max(desired - filled - open_coverage, Decimal(0))
            outstanding = None if reservation_fact is None else reservation_fact.outstanding_amount
            outstanding_decimal = Decimal(outstanding) if outstanding is not None else Decimal(0)
            occupied = occupied or is_open or outstanding_decimal > 0
            items.append(
                PaperPlanItemResponse(
                    order_id=order.id,
                    instrument_id=order.instrument_id,
                    side=cast(Literal["buy", "sell"], order.side),
                    status=latest_status,
                    confirmed_fill_quantity=decimal_to_string(filled),
                    confirmed_fills=tuple(
                        PaperPlanConfirmedFillResponse(
                            fill_id=fill.id,
                            quantity=decimal_to_string(fill.quantity),
                            notional_usd=decimal_to_string(fill.notional),
                            as_of=fill.occurred_at,
                        )
                        for fill in fills
                    ),
                    open_order_coverage_quantity=decimal_to_string(open_coverage),
                    open_order_coverage_amount_usd=decimal_to_string(
                        open_coverage * order.worst_price
                    ),
                    cash_reservation_outstanding=(
                        reservation_fact.outstanding_amount
                        if reservation_fact is not None
                        and reservation_fact.reservation_type == "cash"
                        else decimal_to_string(Decimal(0))
                    ),
                    position_reservation_outstanding=(
                        reservation_fact.outstanding_amount
                        if reservation_fact is not None
                        and reservation_fact.reservation_type == "position"
                        else decimal_to_string(Decimal(0))
                    ),
                    remaining_target_gap_quantity=(
                        None if not reservation_valid else decimal_to_string(remaining_gap)
                    ),
                    remaining_target_gap_amount_usd=(
                        None
                        if not reservation_valid
                        else decimal_to_string(remaining_gap * order.worst_price)
                    ),
                    order_as_of=(latest.occurred_at if latest is not None else None),
                    reservation_as_of=(
                        None if reservation_fact is None else reservation_fact.as_of
                    ),
                )
            )
        try:
            actual_projection = await load_paper_projection(
                self._session,
                account_id=account.id,
                as_of=self._request_time,
            )
        except ValueError as error:
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The paper plan actual projection is incomplete.",
                status_code=409,
            ) from error
        else:
            actual = PaperPlanActualResponse(
                data_state="READY",
                as_of=actual_projection.as_of,
                available_cash=decimal_to_string(actual_projection.available_cash),
                reserved_cash=decimal_to_string(actual_projection.reserved_cash),
                nav=decimal_to_string(actual_projection.nav),
                positions=tuple(
                    PaperPlanActualPositionResponse(
                        instrument_id=position.instrument_id,
                        quantity=decimal_to_string(position.quantity),
                        reserved_quantity=decimal_to_string(position.reserved_quantity),
                    )
                    for position in actual_projection.positions
                ),
            )
        return PaperPlanResponse(
            batch_id=batch.id,
            currency="USD",
            association_state=(
                "target_associated"
                if batch.target_version_id is not None
                else "legacy_unassociated"
            ),
            target=target,
            proposal=proposal,
            plan=PaperPlanFactsResponse(
                plan_created_at=batch.evaluated_at,
                reconciliation_as_of=max(reconciliation_times),
                occupation_state=(
                    "unknown" if occupation_unknown else "occupied" if occupied else "released"
                ),
                risk=risk,
                items=tuple(items),
            ),
            actual=actual,
        )

    async def _cancel_reservation_fact(
        self,
        order: PaperOrderRecord,
        *,
        order_events: tuple[PaperOrderEventRecord, ...],
    ) -> tuple[PaperCancelReservationFactResponse | None, bool]:
        reservation = await self._session.scalar(
            select(PaperReservationRecord).where(
                PaperReservationRecord.order_id == order.id,
                PaperReservationRecord.created_at <= self._request_time,
            )
        )
        if reservation is None:
            return None, True
        events = tuple(
            (
                await self._session.scalars(
                    select(PaperReservationEventRecord)
                    .where(
                        PaperReservationEventRecord.reservation_id == reservation.id,
                        PaperReservationEventRecord.occurred_at <= self._request_time,
                        PaperReservationEventRecord.created_at <= self._request_time,
                    )
                    .order_by(PaperReservationEventRecord.event_seq)
                    .limit(_MAX_EVENTS + 1)
                )
            ).all()
        )
        _require_bounded("reservation_events", events, _MAX_EVENTS)
        valid = _reservation_chain_is_valid(
            order=order,
            reservation=reservation,
            events=events,
            order_events=order_events,
        )
        try:
            outstanding = fold_reservation_events(
                Reservation(
                    reservation_id=reservation.id,
                    order_id=reservation.order_id,
                    account_id=reservation.account_id,
                    reservation_type=cast(
                        Literal["cash", "position"], reservation.reservation_type
                    ),
                    currency=cast(Literal["USD"] | None, reservation.currency),
                    instrument_id=reservation.instrument_id,
                    amount=reservation.amount,
                    events=tuple(
                        ReservationEvent(
                            event_id=event.id,
                            event_seq=event.event_seq,
                            event_type=cast(
                                Literal["reserved", "consumed", "released"],
                                event.event_type,
                            ),
                            amount=event.amount,
                            occurred_at=event.occurred_at,
                            payload_hash=event.payload_hash,
                        )
                        for event in events
                    ),
                )
            )
        except ValueError:
            outstanding = None
            valid = False
        return (
            PaperCancelReservationFactResponse(
                reservation_id=reservation.id,
                reservation_type=cast(Literal["cash", "position"], reservation.reservation_type),
                amount=decimal_to_string(reservation.amount),
                outstanding_amount=(
                    None if outstanding is None else decimal_to_string(outstanding)
                ),
                released=(None if outstanding is None else outstanding == 0),
                as_of=(events[-1].occurred_at if events else None),
            ),
            valid,
        )

    async def _order(
        self,
        order: PaperOrderRecord,
        *,
        latest_event: PaperOrderEventRecord,
        decision_time: datetime,
    ) -> PaperOrderResponse:
        events = tuple(
            (
                await self._session.scalars(
                    select(PaperOrderEventRecord)
                    .where(
                        PaperOrderEventRecord.order_id == order.id,
                        PaperOrderEventRecord.occurred_at <= decision_time,
                        PaperOrderEventRecord.created_at <= decision_time,
                    )
                    .order_by(PaperOrderEventRecord.event_seq)
                    .limit(_MAX_EVENTS + 1)
                )
            ).all()
        )
        _require_bounded("order_events", events, _MAX_EVENTS)
        fill_rows = (
            await self._session.execute(
                select(PaperFillRecord, PaperExecutionWindowAuthorityRecord)
                .join(
                    PaperExecutionWindowAuthorityRecord,
                    PaperExecutionWindowAuthorityRecord.id
                    == PaperFillRecord.execution_window_authority_id,
                )
                .where(
                    PaperFillRecord.order_id == order.id,
                    PaperFillRecord.occurred_at <= decision_time,
                )
                .order_by(PaperFillRecord.fill_seq)
                .limit(_MAX_FILLS + 1)
            )
        ).all()
        _require_bounded("fills", fill_rows, _MAX_FILLS)
        authority = (
            await self._session.execute(
                select(
                    StrategyDecisionRecord.strategy_version,
                    RiskPolicyRecord.version,
                    StrategyDecisionRecord.strategy_definition_id,
                    StrategyDecisionRecord.registry_event_id,
                    StrategyDecisionRecord.champion_generation,
                    StrategyDefinitionRecord.strategy_version,
                    StrategyRegistryEventRecord.to_definition_id,
                    StrategyRegistryEventRecord.champion_generation,
                    StrategyRegistryEventRecord.event_type,
                    StrategyDecisionRecord.decision_origin,
                    StrategyDecisionRecord.agent_paper_cycle_id,
                    PaperOrderBatchRecord.target_version_id,
                    StrategyDecisionRecord.id,
                    StrategyDecisionRecord.decision_hash,
                    PaperOrderBatchRecord.id,
                    PaperOrderBatchRecord.account_id,
                    PaperOrderBatchRecord.evaluated_at,
                )
                .join(
                    PaperOrderBatchRecord,
                    PaperOrderBatchRecord.strategy_decision_id == StrategyDecisionRecord.id,
                )
                .join(
                    RiskPolicyRecord,
                    RiskPolicyRecord.id == PaperOrderBatchRecord.risk_policy_id,
                )
                .outerjoin(
                    StrategyDefinitionRecord,
                    StrategyDefinitionRecord.id == StrategyDecisionRecord.strategy_definition_id,
                )
                .outerjoin(
                    StrategyRegistryEventRecord,
                    StrategyRegistryEventRecord.id == StrategyDecisionRecord.registry_event_id,
                )
                .where(PaperOrderBatchRecord.id == order.batch_id)
            )
        ).one_or_none()
        registered_authority = authority is not None and (
            authority[9] == "registered_strategy"
            and authority[0] == authority[5]
            and authority[2] == authority[6]
            and authority[4] == authority[7]
            and authority[8] in {"GENESIS", "PROMOTE", "ROLLBACK"}
        )
        legacy_agent_authority = authority is not None and (
            authority[9] == "agent_research"
            and authority[10] is not None
            and authority[0] == "agent_paper_decision_v1"
            and authority[11] is None
        )
        portfolio_agent_authority = False
        if (
            authority is not None
            and authority[9] == "agent_research"
            and authority[10] is not None
            and authority[0] == "agent_portfolio_proposal_v1"
            and authority[11] is not None
        ):
            from nexus.paper.errors import PaperBatchError
            from nexus.paper.service import load_paper_order_batch_result

            try:
                canonical_batch = await load_paper_order_batch_result(
                    self._session,
                    account_id=order.account_id,
                    strategy_decision_id=authority[12],
                )
                target_authority = await load_target_read_authority(
                    self._session,
                    account_id=order.account_id,
                    target_id=authority[11],
                    read_as_of=decision_time,
                )
                batch_proposal = await load_proposal_read_authority(
                    self._session,
                    account_id=order.account_id,
                    decision_id=authority[12],
                    read_as_of=decision_time,
                )
            except (PaperBatchError, ValueError):
                canonical_batch = None
                target_authority = None
                batch_proposal = None
            portfolio_agent_authority = (
                canonical_batch is not None
                and target_authority is not None
                and batch_proposal is not None
                and canonical_batch.batch_id == authority[14] == order.batch_id
                and canonical_batch.account_id == authority[15] == order.account_id
                and canonical_batch.strategy_decision_id == authority[12]
                and canonical_batch.decision_hash == authority[13]
                and canonical_batch.evaluated_at == authority[16]
                and canonical_batch.evaluated_at <= decision_time
                and portfolio_target_matches_batch(
                    account_id=order.account_id,
                    portfolio_id=batch_proposal.decision.portfolio_id,
                    batch_decision_id=authority[12],
                    batch_decision_hash=canonical_batch.decision_hash,
                    proposal_decision_id=batch_proposal.decision.id,
                    proposal_decision_hash=batch_proposal.persisted.proposal.decision_hash,
                    proposal_target_content_hash=canonical_portfolio_target_content_hash(
                        batch_proposal.persisted.proposal
                    ),
                    target_account_id=target_authority.target.account_id,
                    target_portfolio_id=target_authority.target.portfolio_id,
                    target_id=target_authority.target.id,
                    expected_target_id=authority[11],
                    target_decision_id=target_authority.target.strategy_decision_id,
                    target_proposal_decision_id=target_authority.proposal.decision.id,
                    target_content_hash=target_authority.target.target_content_hash,
                )
            )
        if authority is None or not events or not (
            registered_authority or legacy_agent_authority or portfolio_agent_authority
        ):
            raise NexusError(
                ErrorCode.DATA_PARTIAL,
                "The internal paper order projection is incomplete.",
                status_code=409,
            )
        reservation = await self._session.scalar(
            select(PaperReservationRecord).where(PaperReservationRecord.order_id == order.id)
        )
        outstanding = Decimal(0)
        if reservation is not None:
            reservation_events = tuple(
                (
                    await self._session.scalars(
                        select(PaperReservationEventRecord)
                        .where(
                            PaperReservationEventRecord.reservation_id == reservation.id,
                            PaperReservationEventRecord.occurred_at <= decision_time,
                        )
                        .order_by(PaperReservationEventRecord.event_seq)
                    )
                ).all()
            )
            for event in reservation_events:
                outstanding += event.amount if event.event_type == "reserved" else -event.amount
            if outstanding < 0:
                raise NexusError(
                    ErrorCode.DATA_PARTIAL,
                    "The internal paper reservation projection is invalid.",
                    status_code=409,
                )
        filled = sum((fill.quantity for fill, _ in fill_rows), start=Decimal(0))
        symbol = await self._symbol(order.instrument_id, latest_event.occurred_at)
        submitted_at = next(
            (event.occurred_at for event in events if event.to_status == "submitted"),
            events[0].occurred_at,
        )
        status = cast(InternalOrderStatus, latest_event.to_status)
        return PaperOrderResponse(
            order_id=order.id,
            instrument_id=order.instrument_id,
            symbol=symbol,
            side=cast(Literal["buy", "sell"], order.side),
            quantity=decimal_to_string(order.quantity),
            filled_quantity=decimal_to_string(filled),
            remaining_quantity=decimal_to_string(order.quantity - filled),
            worst_price=decimal_to_string(order.worst_price),
            status=status,
            cancellable=(status in {"submitted", "partially_filled"} and outstanding > 0),
            current_event_seq=latest_event.event_seq,
            submitted_at=submitted_at,
            strategy_version=authority[0],
            risk_policy_version=authority[1],
            events=tuple(
                PaperOrderEventResponse(
                    event_seq=event.event_seq,
                    from_status=cast(InternalOrderStatus | None, event.from_status),
                    to_status=cast(InternalOrderStatus, event.to_status),
                    actor_kind=cast(
                        Literal["task18", "task19", "user", "reconciliation"],
                        event.actor_kind,
                    ),
                    reason_code=event.reason_code,
                    occurred_at=event.occurred_at,
                )
                for event in events
            ),
            fills=tuple(
                PaperFillResponse(
                    fill_id=fill.id,
                    fill_seq=fill.fill_seq,
                    quantity=decimal_to_string(fill.quantity),
                    price=decimal_to_string(fill.price),
                    notional=decimal_to_string(fill.notional),
                    fee=decimal_to_string(fill.fee),
                    occurred_at=fill.occurred_at,
                    source=window.source,
                    feed=window.feed,
                    delivery_mode=cast(
                        Literal["real_time", "delayed", "fixture", "unknown"],
                        window.delivery_mode,
                    ),
                    source_as_of=window.data_available_at,
                    execution_model_hash=fill.execution_model_hash,
                    slippage_bps=cast(Literal[10, 25], fill.slippage_bps),
                    slippage=decimal_to_string(fill.slippage_amount),
                )
                for fill, window in fill_rows
            ),
        )

    async def _symbols(
        self,
        instrument_ids: tuple[UUID, ...],
        as_of: datetime,
    ) -> dict[UUID, str]:
        if not instrument_ids:
            return {}
        rows = await self._session.execute(
            select(InstrumentAlias.instrument_id, InstrumentAlias.symbol)
            .where(
                InstrumentAlias.instrument_id.in_(instrument_ids),
                InstrumentAlias.alias_type == "ticker",
                InstrumentAlias.valid_from <= as_of.date(),
                or_(
                    InstrumentAlias.valid_to.is_(None),
                    InstrumentAlias.valid_to >= as_of.date(),
                ),
            )
            .order_by(
                InstrumentAlias.instrument_id,
                desc(InstrumentAlias.valid_from),
                desc(InstrumentAlias.id),
            )
        )
        result: dict[UUID, str] = {}
        for instrument_id, symbol in rows:
            result.setdefault(instrument_id, symbol)
        return result

    async def _symbol(self, instrument_id: UUID, as_of: datetime) -> str | None:
        return (await self._symbols((instrument_id,), as_of)).get(instrument_id)


def _require_bounded(name: str, values: Sequence[object], maximum: int) -> None:
    if len(values) <= maximum:
        return
    raise NexusError(
        ErrorCode.DATA_PARTIAL,
        "The internal paper response exceeds a safe bound.",
        status_code=409,
        details={"collection": name, "maximum": maximum},
    )


def _paper_time(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _paper_decimal(value: Decimal) -> str:
    return format(value, ".12f")


def _order_chain_is_valid(
    order: PaperOrderRecord,
    events: tuple[PaperOrderEventRecord, ...],
) -> bool:
    try:
        current_order_status(
            PaperOrder(
                order_id=order.id,
                account_id=order.account_id,
                strategy_decision_id=order.strategy_decision_id,
                client_order_id=order.client_order_id,
                instrument_id=order.instrument_id,
                side=cast(Literal["buy", "sell"], order.side),
                quantity=order.quantity,
                worst_price=order.worst_price,
                events=tuple(
                    OrderEventInput(
                        event_id=event.id,
                        event_seq=event.event_seq,
                        occurred_at=event.occurred_at,
                        from_status=cast(InternalOrderStatus | None, event.from_status),
                        to_status=cast(InternalOrderStatus, event.to_status),
                        actor_kind=cast(
                            Literal["task18", "task19", "user", "reconciliation"],
                            event.actor_kind,
                        ),
                        reason_code=event.reason_code,
                        payload_hash=event.payload_hash,
                    )
                    for event in events
                ),
            )
        )
        for event in events:
            if event.account_id != order.account_id:
                raise ValueError("order event account mismatch")
            if event.actor_kind == "task18":
                payload: dict[str, object] = {
                    "order_id": str(order.id),
                    "event_seq": event.event_seq,
                    "from_status": event.from_status,
                    "to_status": event.to_status,
                    "occurred_at": _paper_time(event.occurred_at),
                    "reason_code": event.reason_code,
                }
            else:
                payload = {
                    "order_id": str(order.id),
                    "event_seq": event.event_seq,
                    "from_status": event.from_status,
                    "to_status": event.to_status,
                    "actor_kind": event.actor_kind,
                    "reason_code": event.reason_code,
                    "occurred_at": _paper_time(event.occurred_at),
                    "source_fill_id": (
                        str(event.source_fill_id) if event.source_fill_id is not None else None
                    ),
                }
            if event.payload_hash != canonical_paper_fact_hash("paper_order_event", payload):
                raise ValueError("order event payload hash mismatch")
    except ValueError:
        return False
    return True


def _fill_chain_is_valid(
    order: PaperOrderRecord,
    fills: tuple[PaperFillRecord, ...],
    windows: dict[UUID, PaperExecutionWindowAuthorityRecord],
    events: tuple[PaperOrderEventRecord, ...],
) -> bool:
    for expected_seq, fill in enumerate(fills, start=1):
        window = windows.get(fill.execution_window_authority_id)
        matching_events = [event for event in events if event.source_fill_id == fill.id]
        if (
            fill.fill_seq != expected_seq
            or fill.order_id != order.id
            or fill.account_id != order.account_id
            or fill.instrument_id != order.instrument_id
            or fill.occurred_at != fill.evaluated_at
            or fill.notional != fill.quantity * fill.price
            or window is None
            or window.order_id != order.id
            or window.account_id != order.account_id
            or window.instrument_id != order.instrument_id
            or window.content_hash is None
            or len(matching_events) != 1
            or matching_events[0].occurred_at != fill.occurred_at
            or matching_events[0].actor_kind != "task19"
            or matching_events[0].to_status not in {"partially_filled", "filled"}
        ):
            return False
        payload = {
            "order_id": str(order.id),
            "fill_seq": fill.fill_seq,
            "instrument_id": str(order.instrument_id),
            "side": order.side,
            "quantity": _paper_decimal(fill.quantity),
            "price": _paper_decimal(fill.price),
            "notional": _paper_decimal(fill.notional),
            "fee": _paper_decimal(fill.fee),
            "execution_model_hash": fill.execution_model_hash,
            "window_content_hash": window.content_hash,
            "evaluated_at": _paper_time(fill.evaluated_at),
        }
        if fill.payload_hash != canonical_paper_fact_hash("paper_fill", payload):
            return False
    return True


def _reservation_chain_is_valid(
    *,
    order: PaperOrderRecord,
    reservation: PaperReservationRecord,
    events: tuple[PaperReservationEventRecord, ...],
    order_events: tuple[PaperOrderEventRecord, ...],
) -> bool:
    if reservation.order_id != order.id or reservation.account_id != order.account_id:
        return False
    for event in events:
        if event.account_id != order.account_id:
            return False
        if event.event_seq == 1:
            payload: dict[str, object] = {
                "client_order_id": order.client_order_id,
                "event_seq": 1,
                "event_type": "reserved",
                "amount": _paper_decimal(event.amount),
                "occurred_at": _paper_time(event.occurred_at),
            }
        elif event.event_type == "consumed":
            payload = {
                "reservation_id": str(reservation.id),
                "event_seq": event.event_seq,
                "event_type": "consumed",
                "amount": _paper_decimal(event.amount),
                "source_fill_id": str(event.source_fill_id),
                "occurred_at": _paper_time(event.occurred_at),
            }
        else:
            matching = [
                item
                for item in order_events
                if item.occurred_at == event.occurred_at
                and item.to_status in {"filled", "cancelled"}
            ]
            if len(matching) != 1:
                return False
            reason_code = (
                "paper_order_filled_release"
                if matching[0].to_status == "filled"
                else matching[0].reason_code
            )
            payload = {
                "reservation_id": str(reservation.id),
                "event_seq": event.event_seq,
                "event_type": "released",
                "amount": _paper_decimal(event.amount),
                "occurred_at": _paper_time(event.occurred_at),
                "reason_code": reason_code,
            }
        if event.payload_hash != canonical_paper_fact_hash("paper_reservation_event", payload):
            return False
    return True


def _optional_uuid(value: object) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError) as error:
        raise NexusError(
            ErrorCode.DATA_PARTIAL,
            "The Agent paper fact identity is incomplete.",
            status_code=409,
        ) from error
