"""Explicit historical uncertainty acceptance; never a zero-side-effect certificate."""

from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Literal
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_formal_stage import require_formal_stage_model_profile
from nexus.audit.canonical import AuditChainInput, audit_event_hash, canonical_audit_event
from nexus.audit.service import append_audit_event
from nexus.jobs.models import job_input_hash
from nexus.paper import manual_preparation_close as stopped
from nexus.paper.history_seal import _row_hash
from nexus.paper.target_service import lock_portfolio_account
from nexus.persistence.models.manual_paper import ManualPaperRequest, ManualPaperRequestEvent
from nexus.persistence.models.paper import AgentPaperCycleEventRecord, AgentPaperCycleRecord
from nexus.persistence.models.runtime import AuditEvent, IdempotencyRecord
from nexus.settings import AgentRuntimeMode, EngineSettings

ACTION = "paper.preparation.unknown_acknowledged"
SCHEMA = "paper_prep_unknown_ack_v1"
ACTOR = "local-user:preparation-unknown-maintenance"
BLOCKED = "MANUAL_PREPARATION_ACK_BLOCKED"


class UnknownPreparationReceipt(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    request_id: UUID
    status: Literal["preparation_unknown_acknowledged"] = "preparation_unknown_acknowledged"
    acknowledged_items: Literal[5] = 5
    historical_usage: Literal["unknown"] = "unknown"
    historical_fees: Literal["unknown"] = "unknown"
    replayed: bool
    execution_locked: Literal[True] = True


async def _proof(
    session: AsyncSession, row: ManualPaperRequest, claim_ids: tuple[UUID, ...], *, at: datetime
) -> dict[str, object]:
    from nexus.paper.manual_request import read_manual_request

    if at.utcoffset() is None or len(claim_ids) != 4 or len(set(claim_ids)) != 4:
        raise ValueError(BLOCKED)
    view = await read_manual_request(session, row)
    events = tuple(
        await session.scalars(
            select(ManualPaperRequestEvent)
            .where(ManualPaperRequestEvent.request_id == row.id)
            .order_by(ManualPaperRequestEvent.sequence)
        )
    )
    if (
        view.status != "unknown"
        or view.cycle_id is not None
        or [e.status for e in events] != ["queued", "preparing", "unknown"]
        or any(e.cycle_id is not None or e.occurred_at > at for e in events)
    ):
        raise ValueError(BLOCKED)
    model = await require_formal_stage_model_profile(
        session, model_config_id=row.model_config_id, now=at
    )
    if (
        row.model_hash != model.config_hash
        or model.input_usd_per_million != 0
        or model.output_usd_per_million != 0
        or model.max_tool_calls != 0
    ):
        raise ValueError(BLOCKED)
    actor = "local-agent-paper-schwab:" + str(row.account_id)
    if (
        await session.scalar(
            select(IdempotencyRecord.id).where(
                IdempotencyRecord.actor_id == actor,
                IdempotencyRecord.idempotency_key.in_([str(row.id), "manual-entry:" + str(row.id)]),
            )
        )
        is not None
    ):
        raise ValueError(BLOCKED)
    if (
        await session.scalar(
            select(AgentPaperCycleEventRecord.cycle_id).where(
                AgentPaperCycleEventRecord.fact_ids["manual_request_id"].astext == str(row.id)
            )
        )
        is not None
    ):
        raise ValueError(BLOCKED)
    claims = tuple(
        await session.scalars(
            select(IdempotencyRecord)
            .where(IdempotencyRecord.id.in_(claim_ids))
            .order_by(IdempotencyRecord.id)
        )
    )
    if len(claims) != 4:
        raise ValueError(BLOCKED)
    slots = []
    for claim in claims:
        try:
            slot = datetime.fromisoformat(claim.idempotency_key)
        except ValueError:
            raise ValueError(BLOCKED) from None
        if (
            slot.utcoffset() != timedelta(0)
            or slot.isoformat() != claim.idempotency_key
            or slot.minute % 5 != 0
            or slot.second != 0
            or slot.microsecond != 0
            or not slot <= claim.created_at <= row.requested_at <= at
            or claim.actor_id != actor
            or claim.route != "agent-paper-schwab-trigger-v2"
            or claim.schema_version != "agent_paper_schwab_trigger_v2"
            or claim.response_payload != {}
            or claim.formal_budget_binding_id is not None
        ):
            raise ValueError(BLOCKED)
        slots.append(slot)
    if (
        await session.scalar(
            select(AgentPaperCycleRecord.id).where(
                AgentPaperCycleRecord.account_id == row.account_id,
                AgentPaperCycleRecord.decision_slot_at.in_(slots),
            )
        )
        is not None
    ):
        raise ValueError(BLOCKED)
    # This is explicitly NOT a claim about the old producer, calls, tokens or fees.
    return {
        "request_id": str(row.id),
        "account_id": str(row.account_id),
        "claim_ids": [str(c.id) for c in claims],
        "disposition": "operator_accepted_unknown_scheduling_only",
        "historical_effects": "unknown",
        "historical_usage": "unknown",
        "historical_fees": "unknown",
        "original_producer_proven": False,
        "fact_hash": job_input_hash(
            {
                "request": _row_hash(row),
                "events": [e.event_hash for e in events],
                "claims": [_row_hash(c) for c in claims],
                "request_model": _row_hash(model),
            }
        ),
    }


async def _audits(session: AsyncSession, account_id: UUID) -> tuple[AuditEvent, ...]:
    return tuple(
        await session.scalars(
            select(AuditEvent).where(
                AuditEvent.action == ACTION,
                AuditEvent.payload["account_id"].astext == str(account_id),
            )
        )
    )


def _claim_ids(payload: dict[str, object]) -> tuple[UUID, ...]:
    values = payload.get("claim_ids")
    if not isinstance(values, list) or not all(isinstance(value, str) for value in values):
        raise ValueError(BLOCKED)
    return tuple(UUID(value) for value in values)


async def _validate(
    session: AsyncSession, audit: AuditEvent, *, now: datetime
) -> dict[str, object]:
    try:
        request_id = UUID(str(audit.payload["request_id"]))
        claim_ids = _claim_ids(audit.payload)
        row = await session.get(ManualPaperRequest, request_id, populate_existing=True)
        if row is None or audit.occurred_at > now:
            raise ValueError(BLOCKED)
        proof = await _proof(session, row, claim_ids, at=audit.occurred_at)
        canonical = canonical_audit_event(
            AuditChainInput(
                event_seq=audit.event_seq,
                occurred_at=audit.occurred_at,
                actor_id=audit.actor_id,
                action=audit.action,
                schema_version=audit.schema_version,
                payload=audit.payload,
            )
        )
        previous = await session.scalar(
            select(AuditEvent.event_hash).where(AuditEvent.event_seq == audit.event_seq - 1)
        )
        if (
            audit.actor_id != ACTOR
            or audit.action != ACTION
            or audit.schema_version != SCHEMA
            or audit.payload != proof
            or audit.canonical_event != canonical
            or audit.chain_version != "audit_jcs_sha256_v1"
            or audit.previous_hash != ("0" * 64 if audit.event_seq == 1 else previous)
            or audit.event_hash != audit_event_hash(audit.previous_hash, canonical)
        ):
            raise ValueError(BLOCKED)
        return proof
    except (KeyError, TypeError, ValueError):
        raise ValueError(BLOCKED) from None


async def acknowledged_request(
    session: AsyncSession, row: ManualPaperRequest, *, now: datetime
) -> bool:
    audits = await _audits(session, row.account_id)
    if not audits:
        return False
    # One explicit five-item exception, not an expanding account-wide ignore list.
    if len(audits) != 1:
        raise ValueError(BLOCKED)
    proof = await _validate(session, audits[0], now=now)
    return proof["request_id"] == str(row.id)


async def _terminal_claim(
    session: AsyncSession, claim: IdempotencyRecord, *, account_id: UUID
) -> bool:
    from nexus.paper.agent_cycle import _TRIGGER_FAILURE_CODES
    from nexus.paper.agent_cycle_models import AgentPaperCycleResult

    schemas = {
        "agent-paper-schwab-trigger-v2": "agent_paper_schwab_trigger_v2",
        "agent-paper-explicit-manual-v1": "agent_paper_explicit_manual_v1",
        "agent-paper-manual-trigger-v1": "agent_paper_manual_trigger_v1",
        "agent-paper-synthesis-recovery-v1": "agent_paper_synth_recovery_v1",
    }
    if schemas.get(claim.route) != claim.schema_version:
        return False
    try:
        if claim.route == "agent-paper-schwab-trigger-v2":
            slot = datetime.fromisoformat(claim.idempotency_key)
            if slot.utcoffset() != timedelta(0) or slot.isoformat() != claim.idempotency_key:
                return False
        elif claim.route == "agent-paper-explicit-manual-v1":
            if str(UUID(claim.idempotency_key)) != claim.idempotency_key:
                return False
        else:
            prefix = (
                "manual-recovery:"
                if claim.route == "agent-paper-manual-trigger-v1"
                else "synthesis-recovery:"
            )
            session_day = date.fromisoformat(claim.idempotency_key.removeprefix(prefix))
            if claim.idempotency_key != prefix + session_day.isoformat():
                return False
    except ValueError:
        return False
    payload = claim.response_payload
    if not isinstance(payload, dict):
        return False
    if payload.get("outcome") == "failed":
        error_code = payload.get("error_code")
        return (
            set(payload) == {"outcome", "error_code"}
            and isinstance(error_code, str)
            and error_code in _TRIGGER_FAILURE_CODES
        )
    if set(payload) != {"outcome", "result"} or payload.get("outcome") != "completed":
        return False
    try:
        result = AgentPaperCycleResult.model_validate(payload["result"])
    except ValueError:
        return False
    cycle = await session.get(AgentPaperCycleRecord, result.cycle_id)
    if cycle is None or cycle.account_id != account_id:
        return False
    first = await session.scalar(
        select(AgentPaperCycleEventRecord).where(
            AgentPaperCycleEventRecord.cycle_id == cycle.id,
            AgentPaperCycleEventRecord.sequence == 1,
        )
    )
    if first is None:
        return False
    if claim.route == "agent-paper-explicit-manual-v1":
        from nexus.paper.manual_request import _require_manual_cycle_binding

        row = await session.get(ManualPaperRequest, UUID(claim.idempotency_key))
        if row is None or first.fact_ids.get("manual_request_id") != claim.idempotency_key:
            return False
        try:
            await _require_manual_cycle_binding(session, row, cycle.id)
        except ValueError:
            return False
        return True
    if first.fact_ids.get("manual_request_id") is not None:
        return False
    if claim.route == "agent-paper-schwab-trigger-v2":
        return claim.idempotency_key == cycle.decision_slot_at.isoformat()
    # Historical prefixes bind old receipts without imposing creation-time
    # cardinality on all future cycles. These timestamps do not prove execution.
    if cycle.decision_session != session_day or claim.created_at > cycle.created_at:
        return False
    cycles = tuple(
        await session.scalars(
            select(AgentPaperCycleRecord).where(
                AgentPaperCycleRecord.account_id == account_id,
                AgentPaperCycleRecord.decision_session == session_day,
                AgentPaperCycleRecord.created_at <= cycle.created_at,
            )
        )
    )
    actor = "local-agent-paper-schwab:" + str(account_id)
    if claim.actor_id != actor:
        return False
    if claim.route == "agent-paper-manual-trigger-v1":
        from nexus.paper.agent_cycle import _MANUAL_RECOVERY_FAILURE_CODES

        scheduled_slot = datetime.combine(
            session_day, time(10), tzinfo=ZoneInfo("America/New_York")
        ).astimezone(UTC)
        prior = await session.scalar(
            select(IdempotencyRecord).where(
                IdempotencyRecord.actor_id == actor,
                IdempotencyRecord.route == "agent-paper-schwab-trigger-v2",
                IdempotencyRecord.schema_version == "agent_paper_schwab_trigger_v2",
                IdempotencyRecord.idempotency_key == scheduled_slot.isoformat(),
            )
        )
        return (
            len(cycles) == 1
            and cycles[0].id == cycle.id
            and prior is not None
            and prior.created_at <= claim.created_at
            and isinstance(prior.response_payload, dict)
            and set(prior.response_payload) == {"outcome", "error_code"}
            and prior.response_payload.get("outcome") == "failed"
            and isinstance(prior.response_payload.get("error_code"), str)
            and prior.response_payload.get("error_code") in _MANUAL_RECOVERY_FAILURE_CODES
        )
    prior = await session.scalar(
        select(IdempotencyRecord).where(
            IdempotencyRecord.actor_id == actor,
            IdempotencyRecord.route == "agent-paper-manual-trigger-v1",
            IdempotencyRecord.schema_version == "agent_paper_manual_trigger_v1",
            IdempotencyRecord.idempotency_key == "manual-recovery:" + session_day.isoformat(),
        )
    )
    if (
        prior is None
        or not isinstance(prior.response_payload, dict)
        or prior.response_payload.get("outcome") != "completed"
        or not isinstance(prior.response_payload.get("result"), dict)
        or not await _terminal_claim(session, prior, account_id=account_id)
    ):
        return False
    try:
        prior_result = AgentPaperCycleResult.model_validate(prior.response_payload["result"])
    except ValueError:
        return False
    base = await session.get(AgentPaperCycleRecord, prior_result.cycle_id)
    return (
        base is not None
        and base.id != cycle.id
        and {item.id for item in cycles} == {base.id, cycle.id}
        and len(cycles) == 2
        and base.created_at < claim.created_at <= cycle.created_at
        and base.candidate_batch_id == cycle.candidate_batch_id
    )


async def require_acknowledged_native_claims(
    session: AsyncSession,
    *,
    account_id: UUID,
    now: datetime,
    excluding_request_id: UUID | None = None,
    current_trigger_key: str | None = None,
) -> None:
    audits = await _audits(session, account_id)
    from nexus.paper.current_preparation_acceptance import accepted_claim_ids
    from nexus.paper.scheduled_unknown_acceptance import accepted_scheduled_claim_ids

    allowed = await accepted_claim_ids(session, account_id)
    allowed.update(await accepted_scheduled_claim_ids(session, account_id))
    prior_request_id = None
    if audits:
        if len(audits) != 1:
            raise ValueError(BLOCKED)
        proof = await _validate(session, audits[0], now=now)
        allowed.update(_claim_ids(proof))
        prior_request_id = proof["request_id"]
    allowed_current = current_trigger_key
    current_route, current_schema = (
        ("agent-paper-manual-trigger-v1", "agent_paper_manual_trigger_v1")
        if current_trigger_key and current_trigger_key.startswith("manual-recovery:")
        else ("agent-paper-synthesis-recovery-v1", "agent_paper_synth_recovery_v1")
        if current_trigger_key and current_trigger_key.startswith("synthesis-recovery:")
        else ("agent-paper-schwab-trigger-v2", "agent_paper_schwab_trigger_v2")
    )
    if excluding_request_id is not None and str(excluding_request_id) != prior_request_id:
        from nexus.paper.manual_request import read_manual_request

        row = await session.get(ManualPaperRequest, excluding_request_id)
        if (
            row is not None
            and row.account_id == account_id
            and (await read_manual_request(session, row)).status == "preparing"
        ):
            allowed_current = str(row.id)
            current_route, current_schema = (
                "agent-paper-explicit-manual-v1",
                "agent_paper_explicit_manual_v1",
            )
    claims = tuple(
        await session.scalars(
            select(IdempotencyRecord).where(
                IdempotencyRecord.actor_id == "local-agent-paper-schwab:" + str(account_id)
            )
        )
    )
    for claim in claims:
        if claim.id in allowed:
            continue
        if (
            claim.idempotency_key == allowed_current
            and claim.response_payload == {}
            and claim.route == current_route
            and claim.schema_version == current_schema
        ):
            continue
        if not await _terminal_claim(session, claim, account_id=account_id):
            raise ValueError(BLOCKED)


async def require_acknowledged_operation_scope(
    session: AsyncSession,
    *,
    account_id: UUID,
    model_config_id: UUID | None,
    now: datetime,
    economic: bool = False,
) -> None:
    from nexus.paper.scheduled_unknown_acceptance import _audits as scheduled_audits

    if not await _audits(session, account_id) and not await scheduled_audits(session, account_id):
        return
    from nexus.agents.persistence import _lock_budget_day
    from nexus.paper.formal_budget_protection import latest_binding

    await lock_portfolio_account(session, account_id)
    await _lock_budget_day(session, now.date().isoformat())
    if economic or await latest_binding(session, business_date=now.date()) is not None:
        raise ValueError(BLOCKED)
    model = await require_formal_stage_model_profile(
        session, model_config_id=model_config_id, now=now
    )
    if (
        model.input_usd_per_million != 0
        or model.output_usd_per_million != 0
        or model.max_tool_calls != 0
    ):
        raise ValueError(BLOCKED)


async def acknowledge_unknown_preparations(
    session: AsyncSession,
    *,
    settings: EngineSettings,
    request_id: UUID,
    claim_ids: tuple[UUID, ...],
    accept_unknown_effects: bool,
    now: datetime,
    repo_root: Path,
    env_file: Path,
) -> UnknownPreparationReceipt:
    from nexus.agents.persistence import _lock_budget_day
    from nexus.paper.formal_budget_protection import latest_binding
    from nexus.paper.manual_request import require_manual_account_available

    account_id = settings.agent_paper_account_id
    if (
        accept_unknown_effects is not True
        or account_id is None
        or settings.agent_runtime_mode != AgentRuntimeMode.CODEX_SDK
        or settings.formal_budget_protection_enabled
    ):
        raise ValueError(BLOCKED)
    await lock_portfolio_account(session, account_id)
    await _lock_budget_day(session, now.date().isoformat())
    if await latest_binding(session, business_date=now.date()) is not None:
        raise ValueError(BLOCKED)
    row = await session.get(ManualPaperRequest, request_id, with_for_update=True)
    if (
        row is None
        or row.account_id != account_id
        or row.model_config_id != settings.agent_paper_formal_model_config_id
        or row.candidate_batch_id != settings.agent_paper_candidate_batch_id
    ):
        raise ValueError(BLOCKED)
    # Lock the exact explicit set in stable order; late writers cannot overwrite it at commit.
    tuple(
        await session.scalars(
            select(IdempotencyRecord)
            .where(IdempotencyRecord.id.in_(claim_ids))
            .order_by(IdempotencyRecord.id)
            .with_for_update()
        )
    )
    proof = await _proof(session, row, claim_ids, at=now)
    audits = await _audits(session, account_id)
    if audits:
        if len(audits) != 1 or await _validate(session, audits[0], now=now) != proof:
            raise ValueError(BLOCKED)
        return UnknownPreparationReceipt(request_id=request_id, replayed=True)
    stopped.require_preparation_producers_stopped(repo_root, env_file)
    # Audit and scheduling availability change atomically; a failed account guard rolls it back.
    await append_audit_event(
        session,
        actor_id=ACTOR,
        action=ACTION,
        schema_version=SCHEMA,
        payload=proof,
        occurred_at=now,
    )
    await require_manual_account_available(session, account_id=account_id, now=now)
    stopped.require_preparation_producers_stopped(repo_root, env_file)
    return UnknownPreparationReceipt(request_id=request_id, replayed=False)
