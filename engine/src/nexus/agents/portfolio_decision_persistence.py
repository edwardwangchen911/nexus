"""Atomic admission for one bounded, read-only portfolio decision run."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.persistence import reserve_portfolio_decision_budget
from nexus.agents.portfolio_decision_contracts import (
    PortfolioDecisionInputV2,
    PortfolioResearchIntentV1,
    canonical_portfolio_decision_hash,
)
from nexus.agents.portfolio_decision_tools import (
    canonical_portfolio_evidence_manifest_hash,
    canonical_portfolio_tool_hash,
    replay_valid_scenario_claim,
)
from nexus.agents.tools import ToolInputError
from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID, load_real_advisory_policy_v1
from nexus.diagnostics.repository import (
    load_frozen_snapshot,
    load_instrument_identities,
    read_report,
)
from nexus.domain.decimal import decimal_to_string
from nexus.domain.evidence import EvidenceRef
from nexus.jobs.models import (
    JobKeyV1,
    JobType,
    QueueName,
    RetryPolicy,
    canonical_job_key,
    job_input_hash,
)
from nexus.jobs.repository import enqueue_job
from nexus.market.ingestion import MarketObjectStore
from nexus.optimizer.scenario_contracts import (
    PortfolioScenarioEvaluationV2,
    PortfolioScenarioRequestV1,
    canonical_scenario_hash,
)
from nexus.optimizer.scenario_engine import evaluate_adjustment_scenario
from nexus.optimizer.scenario_spread_cost import (
    ScenarioSpreadEvidenceManifestV1,
    canonical_scenario_spread_manifest_hash,
)
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    PortfolioDecisionRunRecord,
)
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.portfolio import (
    Account,
    BrokerSyncRun,
    Portfolio,
    PortfolioSnapshotRecord,
)
from nexus.persistence.models.runtime import IdempotencyRecord, Job
from nexus.providers.broker.base import PortfolioSnapshot
from nexus.reconciliation.admission_claim import claim_or_lock
from nexus.reconciliation.persistence_errors import IdempotencyConflict

_ADMISSION_SCHEMA_VERSION = "portfolio_decision_run_v1"
_JOB_SCHEMA_VERSION = "portfolio_decision_job_v1"
_PROMPT_ROLE = "portfolio_decision"
_PROMPT_VERSION = "portfolio-decision-v1"


@dataclass(frozen=True)
class PortfolioDecisionAdmission:
    run: PortfolioDecisionRunRecord
    replayed: bool

    @property
    def run_id(self) -> UUID:
        return self.run.id


def canonical_portfolio_intent_schema_hash() -> str:
    encoded = json.dumps(
        PortfolioResearchIntentV1.model_json_schema(mode="serialization"),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def build_portfolio_evidence_manifest(
    refs: Sequence[EvidenceRef], *, decision_time: datetime
) -> tuple[list[dict[str, object]], str]:
    if decision_time.tzinfo != UTC:
        raise ValueError("portfolio decision time must use UTC")
    canonical: dict[str, dict[str, object]] = {}
    for ref in refs:
        if ref.available_at > decision_time:
            raise ValueError("future evidence cannot enter portfolio decision authority")
        payload = ref.model_dump(mode="json")
        key = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        canonical[key] = payload
    if not 1 <= len(canonical) <= 100:
        raise ValueError("portfolio evidence authority must contain between 1 and 100 rows")
    manifest = [canonical[key] for key in sorted(canonical)]
    return manifest, canonical_portfolio_evidence_manifest_hash(manifest)


async def select_valid_scenario_claim(
    session: AsyncSession,
    store: MarketObjectStore,
    *,
    actor_id: str,
    expected_result_hash: str,
    snapshot: PortfolioSnapshot,
    base_result: PortfolioScenarioEvaluationV2,
    now: datetime,
) -> tuple[IdempotencyRecord, ScenarioSpreadEvidenceManifestV1]:
    claims = tuple(
        await session.scalars(
            select(IdempotencyRecord)
            .where(
                IdempotencyRecord.actor_id == actor_id,
                IdempotencyRecord.route
                == "/internal/v1/portfolio-scenarios/evaluate:portfolio_scenario_evaluation_v3",
                IdempotencyRecord.schema_version == "portfolio_scenario_v3",
                IdempotencyRecord.created_at <= now,
                IdempotencyRecord.response_payload["expected_result_hash"].as_string()
                == expected_result_hash,
            )
            .order_by(IdempotencyRecord.created_at, IdempotencyRecord.id)
        )
    )
    for claim in claims:
        try:
            manifest, _ = await replay_valid_scenario_claim(
                session,
                store,
                actor_id=actor_id,
                claim=claim,
                expected_result_hash=expected_result_hash,
                expected_manifest_hash=None,
                snapshot=snapshot,
                base_result=base_result,
            )
        except ToolInputError:
            continue
        return claim, manifest
    raise ValueError("portfolio scenario result authority changed")


async def admit_portfolio_decision_run(
    session: AsyncSession,
    *,
    actor_id: str,
    route: str,
    idempotency_key: str,
    request_hash: str,
    request: PortfolioScenarioRequestV1,
    expected_scenario_result_hash: str,
    prompt_version_id: UUID | None,
    model_config_id: UUID | None,
    daily_limit_usd: Decimal,
    now: datetime,
    scenario_evidence_store: MarketObjectStore,
    required_model_id: str | None = None,
    required_model_version: str | None = None,
) -> PortfolioDecisionAdmission:
    """Claim first, then freeze authority, reserve budget, and enqueue no-retry work."""
    if now.tzinfo != UTC or request.as_of > now:
        raise ValueError("point-in-time portfolio authority is invalid")
    claim, owner = await claim_or_lock(
        session,
        actor_id,
        route,
        idempotency_key,
        request_hash,
        _ADMISSION_SCHEMA_VERSION,
    )
    if not owner:
        if claim.request_hash != request_hash:
            raise IdempotencyConflict("idempotency key body hash conflicts with prior request")
        raw_run_id = claim.response_payload.get("run_id")
        if claim.schema_version != _ADMISSION_SCHEMA_VERSION or not isinstance(raw_run_id, str):
            raise RuntimeError("portfolio decision idempotency response is incomplete")
        run = await session.get(PortfolioDecisionRunRecord, UUID(raw_run_id))
        if run is None:
            raise RuntimeError("portfolio decision idempotency run is missing")
        return PortfolioDecisionAdmission(run=run, replayed=True)

    snapshot_record = await session.get(PortfolioSnapshotRecord, request.portfolio_snapshot_id)
    diagnostic_record = await session.get(
        PortfolioDiagnosticReportRecord, request.diagnostic_report_id
    )
    qualified_snapshot_id = await session.scalar(
        select(PortfolioSnapshotRecord.id)
        .join(Portfolio, Portfolio.id == PortfolioSnapshotRecord.portfolio_id)
        .join(Account, Account.id == Portfolio.account_id)
        .join(BrokerSyncRun, BrokerSyncRun.id == PortfolioSnapshotRecord.sync_run_id)
        .where(
            PortfolioSnapshotRecord.id == request.portfolio_snapshot_id,
            PortfolioSnapshotRecord.status == "READY",
            PortfolioSnapshotRecord.created_at <= now,
            PortfolioSnapshotRecord.as_of.is_not(None),
            PortfolioSnapshotRecord.as_of <= now,
            Account.provider == "fosun",
            Account.status == "ACTIVE",
            BrokerSyncRun.status == "READY",
            BrokerSyncRun.portfolio_id == PortfolioSnapshotRecord.portfolio_id,
            BrokerSyncRun.account_id == Portfolio.account_id,
            BrokerSyncRun.accepted_at <= now,
            BrokerSyncRun.completed_at.is_not(None),
            BrokerSyncRun.completed_at <= now,
            BrokerSyncRun.cash_flow_quality == "PASS",
        )
    )
    if (
        snapshot_record is None
        or diagnostic_record is None
        or qualified_snapshot_id is None
        or snapshot_record.as_of != request.as_of
        or diagnostic_record.portfolio_id != snapshot_record.portfolio_id
        or diagnostic_record.portfolio_snapshot_id != snapshot_record.id
        or diagnostic_record.snapshot_as_of != request.as_of
        or diagnostic_record.policy_id != REAL_ADVISORY_POLICY_ID
        or diagnostic_record.created_at > now
    ):
        raise ValueError("point-in-time portfolio authority is invalid")

    prompt = await _load_prompt(session, prompt_version_id=prompt_version_id, now=now)
    model = await _load_model(
        session,
        model_config_id=model_config_id,
        required_model_id=required_model_id,
        required_model_version=required_model_version,
        now=now,
    )
    snapshot = await load_frozen_snapshot(
        session, portfolio_id=snapshot_record.portfolio_id, snapshot_id=snapshot_record.id
    )
    report = await read_report(session, diagnostic_record)
    identities = await load_instrument_identities(session, snapshot=snapshot)
    policy = await load_real_advisory_policy_v1(session)
    if diagnostic_record.policy_version != policy.version or any(
        ref.available_at > now for ref in policy.authority_refs
    ):
        raise ValueError("point-in-time portfolio authority is invalid")
    scenario_request_hash = canonical_scenario_hash(request)
    evaluation = evaluate_adjustment_scenario(
        snapshot=snapshot,
        report=report,
        identities=identities,
        constraints=request.constraints,
        policy=policy,
        diagnostic_report_id=diagnostic_record.id,
        diagnostic_input_hash=diagnostic_record.diagnostic_input_hash,
        request_hash=scenario_request_hash,
    )
    scenario_claim, scenario_manifest = await select_valid_scenario_claim(
        session,
        scenario_evidence_store,
        actor_id=actor_id,
        expected_result_hash=expected_scenario_result_hash,
        snapshot=snapshot,
        base_result=evaluation,
        now=now,
    )
    evidence_manifest, evidence_hash = build_portfolio_evidence_manifest(
        (
            EvidenceRef(
                source="nexus.portfolio_snapshot",
                object_id=str(snapshot_record.id),
                available_at=snapshot_record.created_at,
                location=f"postgresql:portfolio_snapshots/{snapshot_record.id}",
                content_hash=_model_content_hash(snapshot),
            ),
            EvidenceRef(
                source="nexus.portfolio_diagnostic",
                object_id=str(diagnostic_record.id),
                available_at=diagnostic_record.created_at,
                location=f"postgresql:portfolio_diagnostic_reports/{diagnostic_record.id}",
                content_hash=_model_content_hash(report),
            ),
            EvidenceRef(
                source="nexus.portfolio_scenario_request",
                object_id=scenario_request_hash,
                available_at=now,
                location="memory:portfolio_scenario_request_v1",
                content_hash=scenario_request_hash,
            ),
            *policy.authority_refs,
        ),
        decision_time=now,
    )
    frozen = PortfolioDecisionInputV2(
        schema_version="portfolio_decision_input_v2",
        portfolio_id=snapshot_record.portfolio_id,
        portfolio_snapshot_id=snapshot_record.id,
        diagnostic_report_id=diagnostic_record.id,
        decision_time=now,
        objective=request.constraints.objective,
        max_turnover=decimal_to_string(request.constraints.max_turnover),
        minimum_cash=decimal_to_string(request.constraints.minimum_cash),
        universe_policy=request.constraints.universe_policy,
        allow_new_positions=request.constraints.allow_new_positions,
        scenario_request_hash=scenario_request_hash,
        expected_scenario_result_hash=expected_scenario_result_hash,
        diagnostic_input_hash=diagnostic_record.diagnostic_input_hash,
        policy_version=policy.version,
        evidence_hash=evidence_hash,
        scenario_claim_id=scenario_claim.id,
        scenario_evidence_manifest_hash=canonical_scenario_spread_manifest_hash(
            scenario_manifest
        ),
    )
    input_hash = canonical_portfolio_decision_hash(frozen)
    schema_hash = canonical_portfolio_intent_schema_hash()
    tool_hash = canonical_portfolio_tool_hash()
    durable_input_hash = job_input_hash(
        {
            "input_hash": input_hash,
            "schema_hash": schema_hash,
            "model_hash": model.config_hash,
            "prompt_hash": prompt.content_hash,
            "tool_hash": tool_hash,
            "evidence_hash": evidence_hash,
            "pricing_version": model.version,
            "actor_id": actor_id,
            "idempotency_key": idempotency_key,
            "request_hash": request_hash,
        }
    )
    key = JobKeyV1(
        job_type=JobType.AGENT_PORTFOLIO_DECISION,
        scope_kind="portfolio",
        scope_id=str(snapshot_record.portfolio_id),
        effective_at=now,
        input_hash=durable_input_hash,
    )
    job_key = canonical_job_key(key)
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:job_key, 1414))"),
        {"job_key": job_key},
    )
    existing_job = await session.scalar(select(Job).where(Job.job_key == job_key))
    if existing_job is not None:
        raw_run_id = existing_job.payload.get("run_id")
        if not isinstance(raw_run_id, str):
            raise RuntimeError("portfolio decision durable Job payload is incomplete")
        existing_run = await session.get(PortfolioDecisionRunRecord, UUID(raw_run_id))
        if (
            existing_run is None
            or existing_run.actor_id != actor_id
            or existing_run.idempotency_key != idempotency_key
            or existing_run.request_hash != request_hash
        ):
            raise RuntimeError("portfolio decision durable Job run is missing")
        claim.response_payload = {"run_id": str(existing_run.id), "status": existing_run.status}
        await session.flush()
        return PortfolioDecisionAdmission(run=existing_run, replayed=True)

    worst_case = model.worst_case_cost_usd
    run = PortfolioDecisionRunRecord(
        portfolio_id=snapshot_record.portfolio_id,
        portfolio_snapshot_id=snapshot_record.id,
        diagnostic_report_id=diagnostic_record.id,
        prompt_version_id=prompt.id,
        model_config_id=model.id,
        actor_id=actor_id,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        status="prepared",
        retry_count=0,
        decision_time=now,
        input_hash=input_hash,
        schema_hash=schema_hash,
        frozen_input=frozen.model_dump(mode="json"),
        prompt_hash=prompt.content_hash,
        model_hash=model.config_hash,
        tool_hash=tool_hash,
        evidence_hash=evidence_hash,
        evidence_manifest=evidence_manifest,
        tool_result_hash=None,
        tool_projection=None,
        provider_phase="none",
        reserved_cost_usd=worst_case,
        settled_cost_usd=None,
        pricing_version=model.version,
        input_tokens=None,
        output_tokens=None,
    )
    session.add(run)
    await session.flush()
    await reserve_portfolio_decision_budget(
        session,
        run_id=run.id,
        now=now,
        daily_limit_usd=daily_limit_usd,
        worst_case_usd=worst_case,
        pricing_version=model.version,
        pricing_fresh=True,
    )
    await enqueue_job(
        session,
        key=key,
        schema_version=_JOB_SCHEMA_VERSION,
        payload={"run_id": str(run.id)},
        queue=QueueName.GENERAL,
        retry_policy=RetryPolicy.NEVER,
        max_attempts=1,
        available_at=now,
    )
    claim.response_payload = {"run_id": str(run.id), "status": run.status}
    await session.flush()
    return PortfolioDecisionAdmission(run=run, replayed=False)


async def _load_prompt(
    session: AsyncSession, *, prompt_version_id: UUID | None, now: datetime
) -> AgentPromptVersionRecord:
    prompt = (
        await session.get(AgentPromptVersionRecord, prompt_version_id)
        if prompt_version_id is not None
        else await session.scalar(
            select(AgentPromptVersionRecord)
            .where(
                AgentPromptVersionRecord.role == _PROMPT_ROLE,
                AgentPromptVersionRecord.version == _PROMPT_VERSION,
            )
            .order_by(
                AgentPromptVersionRecord.created_at.desc(),
                AgentPromptVersionRecord.id.desc(),
            )
            .limit(1)
        )
    )
    if (
        prompt is None
        or prompt.role != _PROMPT_ROLE
        or prompt.version != _PROMPT_VERSION
        or prompt.created_at > now
        or hashlib.sha256(prompt.content.encode()).hexdigest() != prompt.content_hash
    ):
        raise ValueError("portfolio decision prompt authority is invalid")
    return prompt


async def _load_model(
    session: AsyncSession,
    *,
    model_config_id: UUID | None,
    required_model_id: str | None,
    required_model_version: str | None,
    now: datetime,
) -> AgentModelConfigRecord:
    if (required_model_id is None) != (required_model_version is None):
        raise ValueError("portfolio decision runtime model authority is incomplete")
    if model_config_id is not None:
        model = await session.get(AgentModelConfigRecord, model_config_id)
    else:
        query = select(AgentModelConfigRecord).where(
            AgentModelConfigRecord.pricing_available_at <= now,
            AgentModelConfigRecord.pricing_expires_at > now,
            AgentModelConfigRecord.max_tool_calls >= 1,
            AgentModelConfigRecord.version != "codex-sdk-strategy-rnd-v1",
        )
        if required_model_id is not None and required_model_version is not None:
            query = query.where(
                AgentModelConfigRecord.model_id == required_model_id,
                AgentModelConfigRecord.version == required_model_version,
            )
        model = await session.scalar(
            query.order_by(
                AgentModelConfigRecord.created_at.desc(), AgentModelConfigRecord.id.desc()
            ).limit(1)
        )
    if (
        model is None
        or model.max_tool_calls < 1
        or model.version == "codex-sdk-strategy-rnd-v1"
        or required_model_id is not None
        and model.model_id != required_model_id
        or required_model_version is not None
        and model.version != required_model_version
        or model.pricing_available_at > now
        or model.pricing_expires_at <= now
    ):
        raise ValueError("portfolio decision model authority is invalid")
    return model


def _model_content_hash(value: object) -> str:
    if not hasattr(value, "model_dump"):
        raise TypeError("portfolio authority must be a model")
    payload = value.model_dump(mode="json")
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return hashlib.sha256(encoded).hexdigest()
