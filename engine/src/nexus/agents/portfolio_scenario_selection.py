"""Persist one user's non-executable choice for a frozen portfolio scenario."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.portfolio_decision_persistence import select_valid_scenario_claim
from nexus.audit.service import append_audit_event
from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID, load_real_advisory_policy_v1
from nexus.diagnostics.repository import (
    load_frozen_snapshot,
    load_instrument_identities,
    read_report,
)
from nexus.market.ingestion import MarketObjectStore
from nexus.optimizer.scenario_contracts import (
    PortfolioScenarioEvaluationV2,
    PortfolioScenarioRequestV1,
    canonical_scenario_hash,
)
from nexus.optimizer.scenario_engine import evaluate_adjustment_scenario
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.optimization import PortfolioScenarioSelectionRecord
from nexus.persistence.models.portfolio import (
    Account,
    BrokerSyncRun,
    Portfolio,
    PortfolioSnapshotRecord,
)
from nexus.persistence.models.runtime import IdempotencyRecord
from nexus.providers.broker.base import PortfolioSnapshot
from nexus.reconciliation.admission_claim import claim_or_lock
from nexus.reconciliation.persistence_errors import IdempotencyConflict

_SELECTION_SCHEMA_VERSION = "portfolio_scenario_selection_v1"


@dataclass(frozen=True)
class PortfolioScenarioSelection:
    selection_id: UUID
    portfolio_snapshot_id: UUID
    diagnostic_report_id: UUID
    scenario_result_hash: str
    choice: str
    version: int
    selected_at: datetime
    replayed: bool


async def put_portfolio_scenario_selection(
    session: AsyncSession,
    store: MarketObjectStore,
    *,
    actor_id: str,
    idempotency_key: str,
    request_hash: str,
    request: PortfolioScenarioRequestV1,
    scenario_result_hash: str,
    choice: str,
    expected_version: int,
    now: datetime,
) -> PortfolioScenarioSelection:
    """Verify a frozen scenario claim, then create or CAS its user selection."""
    if now.tzinfo != UTC:
        raise ValueError("scenario selection time must use UTC")
    route = f"portfolio-scenario-selection:{scenario_result_hash}"
    claim, owner = await claim_or_lock(
        session,
        actor_id,
        route,
        idempotency_key,
        request_hash,
        _SELECTION_SCHEMA_VERSION,
    )
    if not owner:
        if claim.request_hash != request_hash or claim.schema_version != _SELECTION_SCHEMA_VERSION:
            raise IdempotencyConflict("scenario selection key conflicts with prior request")
        return _selection_from_claim(
            claim,
            scenario_result_hash=scenario_result_hash,
        )

    scenario_claim = await _verify_scenario_authority(
        session,
        store,
        actor_id=actor_id,
        request=request,
        scenario_result_hash=scenario_result_hash,
        now=now,
    )
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:selection_key, 0))"),
        {"selection_key": f"{actor_id}:{scenario_result_hash}"},
    )
    record = await session.scalar(
        select(PortfolioScenarioSelectionRecord)
        .where(
            PortfolioScenarioSelectionRecord.actor_id == actor_id,
            PortfolioScenarioSelectionRecord.scenario_result_hash == scenario_result_hash,
        )
        .with_for_update()
    )
    if record is None:
        if expected_version != 0:
            raise ValueError("VERSION_CONFLICT")
        record = PortfolioScenarioSelectionRecord(
            actor_id=actor_id,
            scenario_claim_id=scenario_claim.id,
            selection_claim_id=claim.id,
            portfolio_snapshot_id=request.portfolio_snapshot_id,
            diagnostic_report_id=request.diagnostic_report_id,
            scenario_result_hash=scenario_result_hash,
            choice=choice,
            version=1,
            selected_at=now,
        )
        session.add(record)
    else:
        if record.version != expected_version:
            raise ValueError("VERSION_CONFLICT")
        record.selection_claim_id = claim.id
        record.choice = choice
        record.version += 1
        record.selected_at = now
    await session.flush()
    selection = _selection_from_record(record, replayed=False)
    claim.response_payload = _selection_payload(selection)
    await append_audit_event(
        session,
        actor_id=actor_id,
        action="portfolio_scenario_selection_changed",
        schema_version="1",
        payload={
            "portfolio_snapshot_id": str(record.portfolio_snapshot_id),
            "diagnostic_report_id": str(record.diagnostic_report_id),
            "scenario_result_hash": record.scenario_result_hash,
            "choice": record.choice,
            "version": record.version,
            "execution_eligible": False,
            "execution_locked": True,
            "execution_authorized": False,
        },
        occurred_at=now,
    )
    await session.flush()
    return selection


async def get_portfolio_scenario_selection(
    session: AsyncSession, *, actor_id: str, scenario_result_hash: str
) -> PortfolioScenarioSelection | None:
    record = cast(
        PortfolioScenarioSelectionRecord | None,
        await session.scalar(
            select(PortfolioScenarioSelectionRecord).where(
                PortfolioScenarioSelectionRecord.actor_id == actor_id,
                PortfolioScenarioSelectionRecord.scenario_result_hash == scenario_result_hash,
            )
        ),
    )
    return None if record is None else _selection_from_record(record, replayed=False)


def _selection_from_record(
    record: PortfolioScenarioSelectionRecord, *, replayed: bool
) -> PortfolioScenarioSelection:
    return PortfolioScenarioSelection(
        selection_id=record.id,
        portfolio_snapshot_id=record.portfolio_snapshot_id,
        diagnostic_report_id=record.diagnostic_report_id,
        scenario_result_hash=record.scenario_result_hash,
        choice=record.choice,
        version=record.version,
        selected_at=record.selected_at,
        replayed=replayed,
    )


def _selection_payload(selection: PortfolioScenarioSelection) -> dict[str, object]:
    return {
        "selection_id": str(selection.selection_id),
        "portfolio_snapshot_id": str(selection.portfolio_snapshot_id),
        "diagnostic_report_id": str(selection.diagnostic_report_id),
        "scenario_result_hash": selection.scenario_result_hash,
        "choice": selection.choice,
        "version": selection.version,
        "selected_at": selection.selected_at.isoformat(),
    }


def _selection_from_claim(
    claim: IdempotencyRecord, *, scenario_result_hash: str
) -> PortfolioScenarioSelection:
    payload = claim.response_payload
    try:
        selection = PortfolioScenarioSelection(
            selection_id=UUID(cast(str, payload["selection_id"])),
            portfolio_snapshot_id=UUID(cast(str, payload["portfolio_snapshot_id"])),
            diagnostic_report_id=UUID(cast(str, payload["diagnostic_report_id"])),
            scenario_result_hash=cast(str, payload["scenario_result_hash"]),
            choice=cast(str, payload["choice"]),
            version=cast(int, payload["version"]),
            selected_at=datetime.fromisoformat(cast(str, payload["selected_at"])),
            replayed=True,
        )
    except (KeyError, TypeError, ValueError) as error:
        raise RuntimeError("scenario selection idempotency response is incomplete") from error
    if (
        selection.scenario_result_hash != scenario_result_hash
        or selection.choice not in {"accepted_for_analysis", "rejected", "watch"}
        or selection.version < 1
        or selection.selected_at.tzinfo != UTC
    ):
        raise RuntimeError("scenario selection idempotency record is invalid")
    return selection


async def _verify_scenario_authority(
    session: AsyncSession,
    store: MarketObjectStore,
    *,
    actor_id: str,
    request: PortfolioScenarioRequestV1,
    scenario_result_hash: str,
    now: datetime,
) -> IdempotencyRecord:
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
            PortfolioSnapshotRecord.as_of == request.as_of,
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
        or diagnostic_record.portfolio_id != snapshot_record.portfolio_id
        or diagnostic_record.portfolio_snapshot_id != snapshot_record.id
        or diagnostic_record.snapshot_as_of != request.as_of
        or diagnostic_record.policy_id != REAL_ADVISORY_POLICY_ID
        or diagnostic_record.created_at > now
    ):
        raise ValueError("portfolio scenario selection authority is invalid")
    snapshot = await load_frozen_snapshot(
        session,
        portfolio_id=snapshot_record.portfolio_id,
        snapshot_id=snapshot_record.id,
    )
    report = await read_report(session, diagnostic_record)
    identities = await load_instrument_identities(session, snapshot=snapshot)
    policy = await load_real_advisory_policy_v1(session)
    if diagnostic_record.policy_version != policy.version or any(
        ref.available_at > now for ref in policy.authority_refs
    ):
        raise ValueError("portfolio scenario selection authority is invalid")
    base_result = evaluate_adjustment_scenario(
        snapshot=snapshot,
        report=report,
        identities=identities,
        constraints=request.constraints,
        policy=policy,
        diagnostic_report_id=diagnostic_record.id,
        diagnostic_input_hash=diagnostic_record.diagnostic_input_hash,
        request_hash=canonical_scenario_hash(request),
    )
    return await _select_scenario_claim_for_selection(
        session,
        store,
        actor_id=actor_id,
        request=request,
        expected_result_hash=scenario_result_hash,
        snapshot=snapshot,
        base_result=base_result,
        now=now,
    )


async def _select_scenario_claim_for_selection(
    session: AsyncSession,
    store: MarketObjectStore,
    *,
    actor_id: str,
    request: PortfolioScenarioRequestV1,
    expected_result_hash: str,
    snapshot: PortfolioSnapshot,
    base_result: PortfolioScenarioEvaluationV2,
    now: datetime,
) -> IdempotencyRecord:
    try:
        claim, _ = await select_valid_scenario_claim(
            session,
            store,
            actor_id=actor_id,
            expected_result_hash=expected_result_hash,
            snapshot=snapshot,
            base_result=base_result,
            now=now,
        )
        return claim
    except ValueError:
        pass

    if base_result.result_hash != expected_result_hash:
        raise ValueError("portfolio scenario result authority changed")
    request_bytes = json.dumps(
        request.model_dump(mode="json"), separators=(",", ":")
    ).encode()
    legacy_claim = await session.scalar(
        select(IdempotencyRecord)
        .where(
            IdempotencyRecord.actor_id == actor_id,
            IdempotencyRecord.route
            == "/internal/v1/portfolio-scenarios/evaluate:portfolio_scenario_evaluation_v2",
            IdempotencyRecord.schema_version == "portfolio_scenario_v2",
            IdempotencyRecord.request_hash == hashlib.sha256(request_bytes).hexdigest(),
            IdempotencyRecord.created_at <= now,
        )
        .order_by(IdempotencyRecord.created_at, IdempotencyRecord.id)
        .limit(1)
    )
    if legacy_claim is None:
        raise ValueError("portfolio scenario result authority changed")
    return legacy_claim
