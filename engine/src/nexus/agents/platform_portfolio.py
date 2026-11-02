"""Compile a portfolio request and settle only the proven charge of its original attempt."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents import platform_native
from nexus.agents.codex_input_view import project_portfolio_payload
from nexus.agents.codex_sdk import (
    MAX_INPUT_TOKENS,
    MAX_OUTPUT_TOKENS,
    codex_request_wire,
)
from nexus.agents.http_failure import HttpFailureFacts
from nexus.agents.platform_pricing import SolPriceRecord, sol_price_record
from nexus.agents.platform_profile import SOL_BUSINESS_CONTRACT, sol_business_model_matches
from nexus.agents.platform_usage import price_platform_usage
from nexus.agents.platform_wire import _wire, platform_wire_profile
from nexus.agents.portfolio_synthesis_contracts import (
    PortfolioSynthesisInputV1,
    PortfolioSynthesisIntentV1,
    canonical_portfolio_synthesis_hash,
)
from nexus.agents.synthesis_lifecycle import SYNTHESIS_HANDLER_SECONDS
from nexus.audit.service import append_audit_event
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPortfolioSynthesisBudgetReservationRecord,
    AgentPortfolioSynthesisOutputRecord,
    AgentPortfolioSynthesisRunRecord,
    AgentPromptVersionRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.runtime import AuditEvent, Job, JobAttempt, JobLease

INVALID_PROOF = "PORTFOLIO_SYNTHESIS_PLATFORM_USAGE_PROOF_REQUIRED"
_SCHEMA = "sol_portfolio_responses_v1"


def wire_digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


async def read_receipt(
    session: AsyncSession, job_id: UUID, phase: str, *, as_of: datetime | None = None
) -> dict[str, Any] | None:
    row = (
        await session.scalars(
            select(AuditEvent)
            .where(
                AuditEvent.actor_id == f"sol-portfolio:{job_id}",
                AuditEvent.action == f"portfolio.responses.{phase}",
                AuditEvent.schema_version == _SCHEMA,
            )
            .limit(2)
        )
    ).one_or_none()
    if row is not None and as_of is not None and row.occurred_at > as_of:
        raise ValueError(INVALID_PROOF)
    return None if row is None else dict(row.payload)


async def append_receipt(
    session: AsyncSession, job: UUID, phase: str, value: dict[str, Any], at: datetime
) -> None:
    if await read_receipt(session, job, phase) is not None:
        raise ValueError("PORTFOLIO_PLATFORM_RECEIPT_EXISTS")
    await append_audit_event(
        session,
        actor_id=f"sol-portfolio:{job}",
        action=f"portfolio.responses.{phase}",
        schema_version=_SCHEMA,
        occurred_at=at,
        payload=value,
    )


async def native_unknown_receipts(
    session: AsyncSession, job: Job, attempt: JobAttempt, now: datetime
) -> list[AuditEvent]:
    """Explicit disposition can consume only the original, actually closed HTTP attempt."""
    events = list(
        await session.scalars(
            select(AuditEvent)
            .where(
                AuditEvent.actor_id == f"sol-portfolio:{job.id}",
                AuditEvent.schema_version == _SCHEMA,
            )
            .order_by(AuditEvent.event_seq)
        )
    )
    phases = [e.action.removeprefix("portfolio.responses.") for e in events]
    if phases not in (
        ["prepared", "authorized", "cleanup"],
        ["prepared", "authorized", "terminal", "cleanup"],
    ):
        raise ValueError(INVALID_PROOF)
    prep, authorization, close = events[0].payload, events[1].payload, events[-1].payload
    expected = dict(
        job_id=str(job.id),
        attempt=attempt.attempt_no,
        fence=attempt.fence_token,
        body_hash=prep.get("body_hash"),
    )
    if (
        prep.get("job_id") != str(job.id)
        or prep.get("input_hash") != job.input_hash
        or prep.get("run_id") != job.payload.get("run_id")
        or authorization != expected
        or close.get("identity") != expected
        or close.get("confirmed") is not True
        or (len(events) == 4 and events[2].payload.get("identity") != expected)
        or any(e.occurred_at > now for e in events)
    ):
        raise ValueError(INVALID_PROOF)
    run = await session.get(AgentPortfolioSynthesisRunRecord, UUID(str(job.payload["run_id"])))
    model = None if run is None else await session.get(AgentModelConfigRecord, run.model_config_id)
    if (
        run is None
        or model is None
        or not sol_business_model_matches(model)
        or prep.get("model_hash") != run.model_hash
        or prep.get("price_hash") != original_price(model).content_hash
        or run.pricing_version != prep.get("pricing_version")
    ):
        raise ValueError(INVALID_PROOF)
    if run.settled_cost_usd is not None or close.get("pre_send_rejected") is True:
        raise ValueError("PORTFOLIO_SYNTHESIS_USAGE_KNOWN")
    if len(events) == 4:
        observation = events[2].payload
        measured = price_platform_usage(
            observation.get("usage"),
            model=observation.get("model"),
            service_tier=observation.get("tier"),
            cache_mode="implicit",
            cache_ttl="30m",
            price_record=original_price(model),
        )
        if measured.exact_cost_usd is not None:
            raise ValueError("PORTFOLIO_SYNTHESIS_USAGE_KNOWN")
    return events


def original_price(model: AgentModelConfigRecord) -> SolPriceRecord:
    if model.platform_profile is None:
        raise ValueError(INVALID_PROOF)
    return sol_price_record(model.platform_profile["price_record"])


def compile_request(model: AgentModelConfigRecord, payload: Mapping[str, object]) -> bytes:
    price = original_price(model)
    # JSONB preserves values but not insertion order. Column projection uses key
    # order, so admission and the later Worker must start from the same ordering.
    ordered = json.loads(json.dumps(dict(payload), sort_keys=True, allow_nan=False))
    view = project_portfolio_payload(ordered)
    if len(codex_request_wire(view).encode()) > 64000:
        raise ValueError("PORTFOLIO_PLATFORM_INPUT_TOO_LARGE")
    wire = _wire(
        view,
        PortfolioSynthesisIntentV1,
        output_remaining=MAX_OUTPUT_TOKENS,
        profile=platform_wire_profile(price.version, price_record=price),
        forbidden=(),
        schema_name="portfolio_synthesis",
    )
    if len(wire) + 8192 > MAX_INPUT_TOKENS:
        raise ValueError("PORTFOLIO_PLATFORM_INPUT_TOO_LARGE")
    return wire


async def freeze_portfolio_request(
    session: AsyncSession,
    run: AgentPortfolioSynthesisRunRecord,
    model: AgentModelConfigRecord,
    job: Job,
    prompt: AgentPromptVersionRecord,
    now: datetime,
) -> None:
    if not sol_business_model_matches(model):
        raise ValueError(INVALID_PROOF)
    price = original_price(model)
    price.require_current(now=now, deadline=now + timedelta(seconds=SYNTHESIS_HANDLER_SECONDS))
    wire = compile_request(
        model,
        {
            "input_hash": run.input_hash,
            "instructions": prompt.content,
            "frozen_input": run.frozen_input,
        },
    )
    keys = set()
    for output_id in run.research_output_ids:
        output = await session.get(AgentResearchOutputRecord, output_id)
        research = (
            None if output is None else await session.get(AgentResearchRunRecord, output.run_id)
        )
        if research is None or research.model_config_id != model.id:
            raise ValueError(INVALID_PROOF)
        binding, _ = await platform_native.read_native_allocation(session, run=research)
        keys.add(binding["credential_binding_hash"])
    if len(keys) != 1:
        raise ValueError("PORTFOLIO_PLATFORM_SOURCE_CREDENTIAL_MISMATCH")
    await append_receipt(
        session,
        job.id,
        "prepared",
        dict(
            job_id=str(job.id),
            run_id=str(run.id),
            input_hash=run.input_hash,
            model_hash=model.config_hash,
            pricing_version=run.pricing_version,
            price_hash=price.content_hash,
            body_hash=wire_digest(wire),
            credential_binding_hash=keys.pop(),
        ),
        now,
    )


@dataclass(frozen=True)
class PortfolioCharge:
    input_tokens: int | None = None
    output_tokens: int | None = None
    cost: Decimal | None = None
    intent_hash: str | None = None
    unused: bool = False
    overrun_cost: Decimal | None = None


async def portfolio_charge(
    session: AsyncSession,
    run: AgentPortfolioSynthesisRunRecord,
    model: AgentModelConfigRecord,
    job_id: UUID | None,
    attempt_no: int | None,
    fence: int | None,
    now: datetime,
) -> PortfolioCharge:
    if job_id is None or attempt_no != 1 or fence is None or not sol_business_model_matches(model):
        raise ValueError(INVALID_PROOF)
    row = (
        await session.execute(
            select(Job, JobAttempt, JobLease)
            .join(JobAttempt, JobAttempt.job_id == Job.id)
            .join(JobLease, JobLease.job_id == Job.id)
            .where(
                Job.id == job_id,
                JobAttempt.attempt_no == attempt_no,
                JobAttempt.fence_token == fence,
                JobLease.lease_token == JobAttempt.lease_token,
                JobLease.fence_token == fence,
            )
        )
    ).one_or_none()
    if row is None:
        raise ValueError(INVALID_PROOF)
    job, attempt, lease = row
    live = job.status == "running" and attempt.outcome == "running" and now < lease.expires_at
    orphan = job.status == "dead_letter" and attempt.outcome == "lost" and now >= lease.expires_at
    if (
        not (live or orphan)
        or job.payload != {"run_id": str(run.id)}
        or job.job_type != "agent.portfolio_synthesis"
        or job.input_hash != run.input_hash
    ):
        raise ValueError(INVALID_PROOF)
    return await _receipt_charge(session, run, model, job_id, attempt_no, fence, now, orphan=orphan)


async def _receipt_charge(
    session: AsyncSession,
    run: AgentPortfolioSynthesisRunRecord,
    model: AgentModelConfigRecord,
    job_id: UUID,
    attempt_no: int,
    fence: int,
    now: datetime,
    *,
    orphan: bool,
) -> PortfolioCharge:
    prep = await read_receipt(session, job_id, "prepared", as_of=now)
    if prep is None and orphan:
        # Pre-receipt native identities can close their historical orphan only as unknown.
        for phase in ("authorized", "terminal", "cleanup"):
            if await read_receipt(session, job_id, phase) is not None:
                raise ValueError(INVALID_PROOF)
        return PortfolioCharge()
    if (
        prep is None
        or not isinstance(prep.get("body_hash"), str)
        or len(prep["body_hash"]) != 64
        or prep.get("run_id") != str(run.id)
        or prep.get("model_hash") != model.config_hash
        or prep.get("input_hash") != run.input_hash
        or prep.get("pricing_version") != run.pricing_version
        or prep.get("price_hash") != original_price(model).content_hash
        or prep.get("job_id") != str(job_id)
    ):
        raise ValueError(INVALID_PROOF)
    identities = dict(
        job_id=str(job_id), attempt=attempt_no, fence=fence, body_hash=prep["body_hash"]
    )
    auth = await read_receipt(session, job_id, "authorized", as_of=now)
    if auth is not None and auth != identities:
        raise ValueError(INVALID_PROOF)
    terminal = await read_receipt(session, job_id, "terminal", as_of=now)
    close = await read_receipt(session, job_id, "cleanup", as_of=now)
    closed = (
        close is not None and close.get("identity") == identities and close.get("confirmed") is True
    )
    if terminal is None:
        if (
            closed
            and close is not None
            and close.get("sent") is False
            and close.get("pre_send_rejected") is True
        ):
            return PortfolioCharge(cost=Decimal("0"), unused=True)
        return PortfolioCharge()
    if auth is None or terminal.get("identity") != identities:
        return PortfolioCharge()
    measured = price_platform_usage(
        terminal.get("usage"),
        model=terminal.get("model"),
        service_tier=terminal.get("tier"),
        cache_mode="implicit",
        cache_ttl="30m",
        price_record=original_price(model),
    )
    if (
        measured.exact_cost_usd is None
        or measured.input_tokens is None
        or measured.output_tokens is None
        or terminal.get("verified_totals") != [measured.input_tokens, measured.output_tokens]
    ):
        return PortfolioCharge()
    if (
        measured.input_tokens > MAX_INPUT_TOKENS
        or measured.output_tokens > MAX_OUTPUT_TOKENS
        or measured.exact_cost_usd > model.worst_case_cost_usd
    ):
        # The immutable authorized terminal already proves exposure. Settlement
        # and release still require their own bounds and confirmed cleanup.
        return PortfolioCharge(overrun_cost=measured.exact_cost_usd)
    if (
        not closed
        or close is None
        or close.get("terminal_write_confirmed") is not True
        or close.get("sent") is not True
    ):
        return PortfolioCharge()
    return PortfolioCharge(
        measured.input_tokens,
        measured.output_tokens,
        measured.exact_cost_usd,
        terminal.get("intent_hash")
        if close.get("failed") is False and terminal.get("status") == "completed"
        else None,
    )


async def portfolio_reserved_overrun(
    session: AsyncSession, hold: AgentPortfolioSynthesisBudgetReservationRecord
) -> Decimal | None:
    """Read exposure from the original attempt, including before or without cleanup."""
    if hold.state != "reserved" or not hold.pricing_version.startswith(SOL_BUSINESS_CONTRACT + "-"):
        return None
    job = (
        await session.scalars(
            select(Job).where(
                Job.job_type == "agent.portfolio_synthesis",
                Job.payload["run_id"].as_string() == str(hold.run_id),
            )
        )
    ).one_or_none()
    if job is None or await read_receipt(session, job.id, "terminal") is None:
        return None
    run = await session.get(AgentPortfolioSynthesisRunRecord, hold.run_id)
    model = None if run is None else await session.get(AgentModelConfigRecord, run.model_config_id)
    if (
        run is None
        or model is None
        or not sol_business_model_matches(model)
        or run.model_hash != model.config_hash
        or run.pricing_version != model.version
        or hold.pricing_version != model.version
        or hold.reserved_usd != model.worst_case_cost_usd
        or job.payload != {"run_id": str(run.id)}
        or job.input_hash != run.input_hash
        or job.scope_kind != "portfolio"
        or job.scope_id != run.frozen_input.get("portfolio_id")
        or job.effective_at != run.decision_time
        or job.attempt_count != 1
        or job.max_attempts != 1
        or job.manual_retry_count != 0
        or job.retry_policy != "never"
    ):
        raise ValueError(INVALID_PROOF)
    attempt = (
        await session.scalars(
            select(JobAttempt)
            .join(JobLease, JobLease.job_id == JobAttempt.job_id)
            .where(
                JobAttempt.job_id == job.id,
                JobAttempt.attempt_no == 1,
                JobAttempt.lease_token == JobLease.lease_token,
                JobAttempt.fence_token == JobLease.fence_token,
                JobAttempt.worker_id == JobLease.worker_id,
            )
        )
    ).one_or_none()
    if attempt is None:
        raise ValueError(INVALID_PROOF)
    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime):
        raise ValueError(INVALID_PROOF)
    charge = await _receipt_charge(
        session, run, model, job.id, 1, attempt.fence_token, now, orphan=False
    )
    return charge.overrun_cost


@dataclass(frozen=True)
class PortfolioReceiptRead:
    requested_model: str
    reported_model: str | None
    failure: HttpFailureFacts | None


async def require_portfolio_settlement_read(
    session: AsyncSession,
    run: AgentPortfolioSynthesisRunRecord,
    frozen: PortfolioSynthesisInputV1,
    output: AgentPortfolioSynthesisOutputRecord,
    model: AgentModelConfigRecord,
    as_of: datetime,
) -> PortfolioReceiptRead:
    """Check a finished ledger entry against its original receipts; never authorize a writer."""
    rows = (
        await session.execute(
            select(Job, JobAttempt, JobLease, AgentPortfolioSynthesisBudgetReservationRecord)
            .join(JobAttempt, JobAttempt.job_id == Job.id)
            .join(JobLease, JobLease.job_id == Job.id)
            .join(
                AgentPortfolioSynthesisBudgetReservationRecord,
                AgentPortfolioSynthesisBudgetReservationRecord.run_id == run.id,
            )
            .where(
                Job.job_type == "agent.portfolio_synthesis",
                Job.payload["run_id"].as_string() == str(run.id),
            )
        )
    ).all()
    if len(rows) != 1:
        raise ValueError(INVALID_PROOF)
    job, attempt, lease, hold = rows[0]
    if (
        job.payload != {"run_id": str(run.id)}
        or job.input_hash != run.input_hash
        or job.scope_kind != "portfolio"
        or job.scope_id != str(frozen.portfolio_id)
        or job.effective_at != run.decision_time
        or job.attempt_count != 1
        or job.max_attempts != 1
        or job.manual_retry_count != 0
        or job.retry_policy != "never"
        or (job.status, attempt.outcome)
        not in {("succeeded", "succeeded"), ("failed", "failed"), ("dead_letter", "lost")}
        or attempt.attempt_no != 1
        or attempt.finished_at is None
        or attempt.finished_at > as_of
        or job.finished_at != attempt.finished_at
        or attempt.lease_token != lease.lease_token
        or attempt.worker_id != lease.worker_id
        or attempt.fence_token != lease.fence_token
        or hold.business_date != frozen.admitted_at.date()
        or hold.created_at > as_of
        or hold.pricing_version != model.version
        or hold.reserved_usd != model.worst_case_cost_usd
        or run.completed_at is None
        or run.completed_at > as_of
    ):
        raise ValueError(INVALID_PROOF)
    charge = await _receipt_charge(
        session,
        run,
        model,
        job.id,
        attempt.attempt_no,
        attempt.fence_token,
        as_of,
        orphan=attempt.outcome == "lost",
    )
    state = "released" if charge.unused else "reserved" if charge.cost is None else "settled"
    if (
        (charge.input_tokens, charge.output_tokens, charge.cost)
        != (run.input_tokens, run.output_tokens, run.settled_cost_usd)
        or hold.state != state
        or hold.settled_usd != charge.cost
        or (
            output.intent is not None
            and canonical_portfolio_synthesis_hash(output.intent) != charge.intent_hash
        )
    ):
        raise ValueError(INVALID_PROOF)
    prepared = await read_receipt(session, job.id, "prepared", as_of=as_of)
    if prepared is None:
        return PortfolioReceiptRead(model.model_id, None, None)
    expected = dict(
        job_id=str(job.id),
        attempt=attempt.attempt_no,
        fence=attempt.fence_token,
        body_hash=prepared.get("body_hash"),
    )
    terminal = await read_receipt(session, job.id, "terminal", as_of=as_of)
    cleanup = await read_receipt(session, job.id, "cleanup", as_of=as_of)
    reported_model = None
    if terminal is not None:
        authorized = await read_receipt(session, job.id, "authorized", as_of=as_of)
        if terminal.get("identity") != expected or authorized != expected:
            raise ValueError(INVALID_PROOF)
        if terminal.get("model") == model.model_id:
            reported_model = model.model_id
    failure = None
    if cleanup is not None:
        if cleanup.get("identity") != expected:
            raise ValueError(INVALID_PROOF)
        if cleanup.get("failure_facts") is not None:
            failure = HttpFailureFacts.model_validate(cleanup["failure_facts"])
    return PortfolioReceiptRead(model.model_id, reported_model, failure)
