"""Single deterministic tool boundary for portfolio decision research."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Final, Protocol
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.portfolio_decision_contracts import (
    AgentScenarioMetricSetV1,
    AgentScenarioProjectionV1,
    EvaluateAdjustmentScenarioToolInputV1,
    PortfolioDecisionInputV1,
    PortfolioDecisionInputV2,
    canonical_portfolio_decision_hash,
    parse_portfolio_decision_input,
)
from nexus.agents.tools import ToolInputError
from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_VERSION, load_real_advisory_policy_v1
from nexus.diagnostics.repository import (
    load_frozen_snapshot,
    load_instrument_identities,
    read_report,
)
from nexus.domain.evidence import EvidenceRef
from nexus.market.ingestion import MarketObjectStore
from nexus.optimizer.scenario_contracts import (
    AdjustmentScenarioConstraintsV1,
    PortfolioScenarioEvaluationV2,
    ScenarioMetricSetV1,
    canonical_scenario_hash,
)
from nexus.optimizer.scenario_engine import evaluate_adjustment_scenario
from nexus.optimizer.scenario_spread_cost import (
    ScenarioSpreadEvidenceManifestV1,
    apply_scenario_spread_cost,
    canonical_scenario_spread_manifest_hash,
    replay_scenario_spread_evidence,
)
from nexus.persistence.models.agents import PortfolioDecisionRunRecord
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.portfolio import PortfolioSnapshotRecord
from nexus.persistence.models.runtime import IdempotencyRecord
from nexus.providers.broker.base import PortfolioSnapshot

TOOL_NAME: Final = "evaluate_adjustment_scenario"
_MAX_EVIDENCE = 100
_PROJECTION_KEYS = frozenset(
    {
        "schema_version",
        "objective",
        "objective_status",
        "analysis_status",
        "baseline",
        "candidate",
        "unknown_metrics",
        "cost_gap_codes",
        "hard_constraint_reasons",
        "changed_position_count",
        "result_hash",
        "citation_ids",
        "execution_locked",
    }
)
_METRIC_SET_KEYS = frozenset(
    {
        "security_top1",
        "security_top3",
        "security_hhi",
        "cash_weight",
        "one_way_turnover",
        "estimated_cost",
    }
)
_TYPED_METRIC_KEYS = frozenset({"status", "value", "reason"})
_FORBIDDEN_PROJECTION_KEYS = frozenset(
    {
        "positions",
        "instrument_id",
        "symbol",
        "current_weight",
        "target_weight",
        "quantity",
        "amount",
        "order",
        "execution_eligible",
    }
)

type ScenarioExecutor = Callable[[], PortfolioScenarioEvaluationV2]


class PortfolioDecisionToolRegistry(Protocol):
    names: tuple[str, ...]

    async def execute(self, name: str, payload: Mapping[str, object]) -> Mapping[str, object]: ...


@dataclass(frozen=True)
class PortfolioCitationAuthority:
    """Opaque per-run citation IDs with a unique server-side reverse mapping."""

    _entries: tuple[tuple[str, EvidenceRef], ...]

    @classmethod
    def from_manifest(
        cls,
        *,
        run_id: UUID,
        manifest: Sequence[Mapping[str, object]],
        evidence_hash: str,
        decision_time: datetime,
    ) -> PortfolioCitationAuthority:
        if not 1 <= len(manifest) <= _MAX_EVIDENCE:
            raise ToolInputError("portfolio evidence manifest is not bounded")
        refs: list[EvidenceRef] = []
        canonical_items: list[str] = []
        try:
            for item in manifest:
                ref = EvidenceRef.model_validate(item)
                normalized = ref.model_dump(mode="json")
                if dict(item) != normalized:
                    raise ToolInputError("portfolio evidence manifest is not canonical")
                refs.append(ref)
                canonical_items.append(_canonical_evidence_json(ref))
        except (TypeError, ValueError, ValidationError) as error:
            if isinstance(error, ToolInputError):
                raise
            raise ToolInputError("portfolio evidence manifest is invalid") from error
        if canonical_items != sorted(canonical_items) or len(set(canonical_items)) != len(
            canonical_items
        ):
            raise ToolInputError("portfolio evidence manifest is not uniquely sorted")
        if canonical_portfolio_evidence_manifest_hash(manifest) != evidence_hash:
            raise ToolInputError("portfolio evidence manifest hash does not match")
        if any(ref.available_at > decision_time for ref in refs):
            raise ToolInputError("future evidence cannot enter portfolio decision authority")
        entries = tuple(
            (
                hashlib.sha256(
                    f"{str(run_id).lower()}:{index}:{canonical_items[index]}".encode()
                ).hexdigest(),
                ref,
            )
            for index, ref in enumerate(refs)
        )
        if len({citation_id for citation_id, _ in entries}) != len(entries):
            raise ToolInputError("portfolio citation authority is not unique")
        return cls(_entries=entries)

    @property
    def citation_ids(self) -> tuple[str, ...]:
        return tuple(citation_id for citation_id, _ in self._entries)

    def resolve(self, citation_id: str) -> EvidenceRef:
        for candidate, evidence in self._entries:
            if candidate == citation_id:
                return evidence
        raise ToolInputError("citation is not in portfolio decision authority")


class PortfolioDecisionTools:
    """Validate the only model-callable request before deterministic execution."""

    names: tuple[str, ...] = (TOOL_NAME,)

    def __init__(
        self,
        *,
        objective: str,
        expected_result_hash: str,
        citation_ids: tuple[str, ...],
        executor: ScenarioExecutor,
    ) -> None:
        self._objective = objective
        self._expected_result_hash = expected_result_hash
        self._citation_ids = citation_ids
        self._executor = executor

    async def execute(self, name: str, payload: Mapping[str, object]) -> Mapping[str, object]:
        if name != TOOL_NAME:
            raise ToolInputError("tool is not in the portfolio decision catalogue")
        try:
            request = EvaluateAdjustmentScenarioToolInputV1.model_validate(payload)
        except (TypeError, ValidationError) as error:
            raise ToolInputError("portfolio decision tool payload is not allowlisted") from error
        if request.objective != self._objective:
            raise ToolInputError("tool objective does not match frozen authority")
        evaluation = self._executor()
        if not isinstance(evaluation, PortfolioScenarioEvaluationV2):
            raise ToolInputError("portfolio scenario executor returned an invalid result")
        if evaluation.objective != self._objective:
            raise ToolInputError("scenario objective does not match frozen authority")
        if evaluation.result_hash != self._expected_result_hash:
            raise ToolInputError("scenario result authority changed")
        projection = project_scenario_for_agent(evaluation, citation_ids=self._citation_ids)
        payload = projection.model_dump(mode="json")
        _validate_projection_shape(payload)
        return payload


async def build_portfolio_decision_tools(
    session: AsyncSession,
    run: PortfolioDecisionRunRecord,
    *,
    scenario_evidence_store: MarketObjectStore,
) -> PortfolioDecisionToolRegistry:
    """Load all authority from one durable run; caller payload contributes no identity."""
    try:
        frozen = parse_portfolio_decision_input(run.frozen_input)
    except (TypeError, ValidationError) as error:
        raise ToolInputError("frozen portfolio decision input is invalid") from error
    if run.tool_hash != canonical_portfolio_tool_hash():
        raise ToolInputError("portfolio decision tool authority does not match run")
    if (
        frozen.portfolio_id != run.portfolio_id
        or frozen.portfolio_snapshot_id != run.portfolio_snapshot_id
        or frozen.diagnostic_report_id != run.diagnostic_report_id
        or frozen.decision_time != run.decision_time
        or frozen.evidence_hash != run.evidence_hash
        or canonical_portfolio_decision_hash(frozen) != run.input_hash
    ):
        raise ToolInputError("portfolio decision run authority does not match frozen input")
    citations = PortfolioCitationAuthority.from_manifest(
        run_id=run.id,
        manifest=run.evidence_manifest,
        evidence_hash=run.evidence_hash,
        decision_time=run.decision_time,
    )
    evaluation = await load_exact_scenario_evaluation(
        session,
        scenario_evidence_store,
        run=run,
        frozen=frozen,
    )
    return PortfolioDecisionTools(
        objective=frozen.objective,
        expected_result_hash=frozen.expected_scenario_result_hash,
        citation_ids=citations.citation_ids,
        executor=lambda: evaluation,
    )


async def load_exact_scenario_evaluation(
    session: AsyncSession,
    store: MarketObjectStore,
    *,
    run: PortfolioDecisionRunRecord,
    frozen: PortfolioDecisionInputV1 | PortfolioDecisionInputV2,
) -> PortfolioScenarioEvaluationV2:
    """Rebuild V1 or replay the exact claim frozen into a V2 Agent run."""
    snapshot, base_result = await _load_base_scenario_evaluation(
        session,
        run=run,
        frozen=frozen,
    )
    if isinstance(frozen, PortfolioDecisionInputV1):
        if base_result.result_hash != frozen.expected_scenario_result_hash:
            raise ToolInputError("portfolio scenario result authority changed")
        return base_result
    claim = await session.get(IdempotencyRecord, frozen.scenario_claim_id)
    if claim is None:
        raise ToolInputError("frozen portfolio scenario claim is missing")
    _, result = await replay_valid_scenario_claim(
        session,
        store,
        actor_id=run.actor_id,
        claim=claim,
        expected_result_hash=frozen.expected_scenario_result_hash,
        expected_manifest_hash=frozen.scenario_evidence_manifest_hash,
        snapshot=snapshot,
        base_result=base_result,
    )
    return result


async def replay_valid_scenario_claim(
    session: AsyncSession,
    store: MarketObjectStore,
    *,
    actor_id: str,
    claim: IdempotencyRecord,
    expected_result_hash: str,
    expected_manifest_hash: str | None,
    snapshot: PortfolioSnapshot,
    base_result: PortfolioScenarioEvaluationV2,
) -> tuple[ScenarioSpreadEvidenceManifestV1, PortfolioScenarioEvaluationV2]:
    """Validate and replay one exact v3 scenario claim without selecting latest quotes."""
    try:
        if (
            claim.actor_id != actor_id
            or claim.route
            != "/internal/v1/portfolio-scenarios/evaluate:portfolio_scenario_evaluation_v3"
            or claim.schema_version != "portfolio_scenario_v3"
            or len(claim.request_hash) != 64
            or any(character not in "0123456789abcdef" for character in claim.request_hash)
        ):
            raise ValueError
        manifest = ScenarioSpreadEvidenceManifestV1.model_validate(claim.response_payload)
        manifest_hash = canonical_scenario_spread_manifest_hash(manifest)
        if (
            manifest.expected_result_hash != expected_result_hash
            or expected_manifest_hash is not None
            and manifest_hash != expected_manifest_hash
        ):
            raise ValueError
        evidence = await replay_scenario_spread_evidence(
            session,
            store,
            snapshot=snapshot,
            base_result=base_result,
            manifest=manifest,
        )
        result = apply_scenario_spread_cost(
            snapshot=snapshot,
            base_result=base_result,
            evidence=evidence,
        )
        if result.result_hash != expected_result_hash:
            raise ValueError
        return manifest, result
    except (TypeError, ValueError, ValidationError) as error:
        raise ToolInputError("frozen portfolio scenario claim is invalid") from error


async def _load_base_scenario_evaluation(
    session: AsyncSession,
    *,
    run: PortfolioDecisionRunRecord,
    frozen: PortfolioDecisionInputV1 | PortfolioDecisionInputV2,
) -> tuple[PortfolioSnapshot, PortfolioScenarioEvaluationV2]:
    snapshot_record = await session.get(PortfolioSnapshotRecord, run.portfolio_snapshot_id)
    diagnostic_record = await session.get(PortfolioDiagnosticReportRecord, run.diagnostic_report_id)
    if (
        snapshot_record is None
        or diagnostic_record is None
        or snapshot_record.portfolio_id != run.portfolio_id
        or snapshot_record.as_of is None
        or snapshot_record.as_of > run.decision_time
        or snapshot_record.created_at > run.decision_time
        or diagnostic_record.portfolio_id != run.portfolio_id
        or diagnostic_record.portfolio_snapshot_id != run.portfolio_snapshot_id
        or diagnostic_record.snapshot_as_of != snapshot_record.as_of
        or diagnostic_record.created_at > run.decision_time
        or diagnostic_record.diagnostic_input_hash != frozen.diagnostic_input_hash
        or diagnostic_record.policy_version != frozen.policy_version
    ):
        raise ToolInputError("portfolio scenario database authority does not match run")
    try:
        snapshot = await load_frozen_snapshot(
            session,
            portfolio_id=run.portfolio_id,
            snapshot_id=run.portfolio_snapshot_id,
        )
        report = await read_report(session, diagnostic_record)
        identities = await load_instrument_identities(session, snapshot=snapshot)
        policy = await load_real_advisory_policy_v1(session)
    except ValueError as error:
        raise ToolInputError("portfolio scenario database authority is incomplete") from error
    if policy.version != frozen.policy_version or any(
        ref.available_at > run.decision_time for ref in policy.authority_refs
    ):
        raise ToolInputError("portfolio scenario policy authority does not match run")
    constraints = AdjustmentScenarioConstraintsV1(
        schema_version="adjustment_scenario_constraints_v1",
        objective=frozen.objective,
        max_turnover=Decimal(frozen.max_turnover),
        minimum_cash=Decimal(frozen.minimum_cash),
        universe_policy=frozen.universe_policy,
        allow_new_positions=frozen.allow_new_positions,
    )

    return snapshot, evaluate_adjustment_scenario(
        snapshot=snapshot,
        report=report,
        identities=identities,
        constraints=constraints,
        policy=policy,
        diagnostic_report_id=run.diagnostic_report_id,
        diagnostic_input_hash=frozen.diagnostic_input_hash,
        request_hash=frozen.scenario_request_hash,
    )


def project_scenario_for_agent(
    evaluation: PortfolioScenarioEvaluationV2,
    *,
    citation_ids: tuple[str, ...],
) -> AgentScenarioProjectionV1:
    """Remove all per-position authority from a validated deterministic result."""
    if canonical_scenario_hash(evaluation) != evaluation.result_hash:
        raise ToolInputError("portfolio scenario result hash is invalid")
    try:
        return AgentScenarioProjectionV1.model_validate(
            {
                "schema_version": "agent_scenario_projection_v1",
                "objective": evaluation.objective,
                "objective_status": evaluation.objective_status,
                "analysis_status": evaluation.analysis_status,
                "baseline": _project_metric_set(evaluation.baseline),
                "candidate": _project_metric_set(evaluation.candidate),
                "unknown_metrics": evaluation.unknown_metrics,
                "cost_gap_codes": _cost_gap_codes(evaluation),
                "hard_constraint_reasons": evaluation.hard_constraint_reasons,
                "changed_position_count": sum(
                    position.current_weight != position.target_weight
                    for position in evaluation.positions
                ),
                "result_hash": evaluation.result_hash,
                "citation_ids": citation_ids,
                "execution_locked": True,
            }
        )
    except (TypeError, ValidationError) as error:
        raise ToolInputError("portfolio scenario projection is invalid") from error


def _project_metric_set(metrics: ScenarioMetricSetV1) -> AgentScenarioMetricSetV1:
    payload = metrics.model_dump(mode="python")
    cost = payload.get("estimated_cost")
    if isinstance(cost, dict) and cost.get("status") == "unknown":
        payload["estimated_cost"] = {
            "status": "unknown",
            "value": None,
            "reason": "COST_EVIDENCE_INCOMPLETE",
        }
    return AgentScenarioMetricSetV1.model_validate(payload)


def _cost_gap_codes(evaluation: PortfolioScenarioEvaluationV2) -> tuple[str, ...]:
    costs = evaluation.cost_breakdown
    components = (
        costs.commission,
        costs.spread,
        costs.slippage,
        costs.regulatory_fees,
        costs.tax_impact,
    )
    return tuple(
        metric.reason
        for metric in components
        if metric.status == "unknown" and metric.reason is not None
    )


def canonical_portfolio_evidence_manifest_hash(
    manifest: Sequence[Mapping[str, object]],
) -> str:
    """Hash normalized full evidence identities in their authoritative order."""
    try:
        normalized = [EvidenceRef.model_validate(item).model_dump(mode="json") for item in manifest]
    except (TypeError, ValidationError) as error:
        raise ToolInputError("portfolio evidence manifest is invalid") from error
    encoded = json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def canonical_portfolio_tool_hash() -> str:
    """Bind the model-visible schemas, deterministic engine, and policy version."""
    module_root = Path(__file__).resolve().parents[1]
    implementation_paths = (
        Path(__file__).resolve(),
        module_root / "optimizer/scenario_contracts.py",
        module_root / "optimizer/scenario_cost_evidence.py",
        module_root / "optimizer/scenario_engine.py",
        module_root / "optimizer/scenario_spread_cost.py",
    )
    authority = {
        "tool_name": TOOL_NAME,
        "input_schema": EvaluateAdjustmentScenarioToolInputV1.model_json_schema(
            mode="serialization"
        ),
        "projection_schema": AgentScenarioProjectionV1.model_json_schema(mode="serialization"),
        "implementation_hashes": {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in implementation_paths
        },
        "policy_version": REAL_ADVISORY_POLICY_VERSION,
    }
    encoded = json.dumps(
        authority,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _canonical_evidence_json(ref: EvidenceRef) -> str:
    return json.dumps(
        ref.model_dump(mode="json"),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _validate_projection_shape(payload: Mapping[str, object]) -> None:
    if set(payload) != _PROJECTION_KEYS or set(payload) & _FORBIDDEN_PROJECTION_KEYS:
        raise ToolInputError("portfolio scenario projection contains forbidden fields")
    for name in ("baseline", "candidate"):
        metrics = payload.get(name)
        if not isinstance(metrics, Mapping) or set(metrics) != _METRIC_SET_KEYS:
            raise ToolInputError("portfolio scenario projection metric set is invalid")
        for metric in metrics.values():
            if not isinstance(metric, Mapping) or set(metric) != _TYPED_METRIC_KEYS:
                raise ToolInputError("portfolio scenario projection metric is invalid")
