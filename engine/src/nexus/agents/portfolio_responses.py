"""Single-attempt Responses transport for the normal formal portfolio Worker."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

import httpx
from openai.types.responses import Response
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.agents import platform_native
from nexus.agents.codex_sdk import (
    _MAX_OUTPUT_BYTES,
    MAX_INPUT_TOKENS,
    MAX_OUTPUT_TOKENS,
)
from nexus.agents.platform_portfolio import (
    INVALID_PROOF,
    append_receipt,
    compile_request,
    original_price,
    read_receipt,
    wire_digest,
)
from nexus.agents.platform_pricing import SolPriceRecord
from nexus.agents.platform_profile import sol_business_model_matches
from nexus.agents.platform_usage import price_platform_usage
from nexus.agents.portfolio_synthesis_contracts import (
    PortfolioSynthesisIntentV1,
    canonical_portfolio_synthesis_hash,
)
from nexus.agents.responses_budget import UsageSnapshot
from nexus.agents.responses_request import send_response_stage
from nexus.agents.responses_stream import ResponsesStreamFacts
from nexus.agents.responses_transport import normalize_response
from nexus.agents.synthesis_lifecycle import SYNTHESIS_HANDLER_SECONDS, SynthesisDeadline
from nexus.jobs.repository import ClaimedJob, _lock_active_claim
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPortfolioSynthesisBudgetReservationRecord,
    AgentPortfolioSynthesisRunRecord,
)
from nexus.persistence.models.runtime import Job
from nexus.settings import EngineSettings

# Incomplete cleanup retains ownership even after the Worker returns. A later
# task exit never upgrades the original receipt or releases the held budget.
_PENDING_CLEANUP: set[asyncio.Task[object]] = set()


class PortfolioHTTPError(ValueError):
    """The attempt receipt owns the safe reason and the original transport error."""

    def __init__(self, code: str, cause: BaseException | None):
        super().__init__(code)
        self.cause = cause


class PortfolioAlreadyDispatched(RuntimeError):
    """Another invocation already owns this attempt; do not finalize it."""


class _ScopeRejected(ValueError):
    """The final synchronous check rejected the request before client.send."""


@dataclass(frozen=True)
class _Terminal:
    evidence: dict[str, Any]
    output: str | None
    usage: UsageSnapshot | None
    error: ValueError | None


def _observe_terminal(
    value: dict[str, Any], facts: ResponsesStreamFacts, price: SolPriceRecord
) -> _Terminal:
    raw_usage = value.get("usage")
    charged = price_platform_usage(
        raw_usage,
        model=value.get("model"),
        service_tier=value.get("service_tier"),
        cache_mode="implicit",
        cache_ttl="30m",
        price_record=price,
    )
    measured = None
    if (
        charged.exact_cost_usd is not None
        and charged.input_tokens is not None
        and charged.output_tokens is not None
        and charged.input_tokens <= MAX_INPUT_TOKENS
        and charged.output_tokens <= MAX_OUTPUT_TOKENS
        and facts.usage == (charged.input_tokens, charged.output_tokens)
    ):
        measured = UsageSnapshot(
            input_tokens=charged.input_tokens, output_tokens=charged.output_tokens, provider_calls=1
        )
    output: str | None = None
    intent_hash: str | None = None
    error: ValueError | None = None
    if measured is not None and value.get("status") == "completed":
        try:
            normalized = normalize_response(Response.model_validate(value))
            if (
                not normalized.output_text
                or len(normalized.output_text.encode()) > _MAX_OUTPUT_BYTES
                or any(
                    item.get("type") not in {"reasoning", "message"} for item in normalized.output
                )
            ):
                raise ValueError("PORTFOLIO_PLATFORM_OUTPUT_INVALID")
            candidate = PortfolioSynthesisIntentV1.model_validate_json(normalized.output_text)
            intent_hash = canonical_portfolio_synthesis_hash(candidate)
            output = normalized.output_text
        except ValueError as invalid:
            error = invalid
    return _Terminal(
        dict(
            model=value.get("model") if value.get("model") == "gpt-6.1-sol" else None,
            tier=value.get("service_tier") if value.get("service_tier") == "default" else None,
            response_hash=wire_digest(str(facts.response_id).encode()),
            status=facts.terminal_status,
            verified_totals=None if facts.usage is None else list(facts.usage),
            usage=_usage_fields(raw_usage),
            usage_issues=list(charged.issues),
            intent_hash=intent_hash,
        ),
        output,
        measured,
        error,
    )


def _fence(claim: ClaimedJob) -> dict[str, Any]:
    return dict(job_id=str(claim.job_id), attempt=claim.attempt_no, fence=claim.fence_token)


class NativePortfolioTransport:
    def __init__(self, factory: async_sessionmaker[AsyncSession], settings: EngineSettings):
        self.factory, self.settings = factory, settings
        self.claim: ClaimedJob | None = None

    def supports(self, model: AgentModelConfigRecord) -> bool:
        return (
            model.id == self.settings.agent_paper_formal_model_config_id
            and sol_business_model_matches(model)
        )

    async def require_job(self, session: AsyncSession, job: Job, now: datetime) -> None:
        prepared = await read_receipt(session, job.id, "prepared")
        credential = platform_native.credential_for_preparation(self.settings)
        if prepared is None or prepared["credential_binding_hash"] != credential.binding_hash:
            raise ValueError("PLATFORM_HTTP_CREDENTIAL_CHANGED")
        run = await session.get(AgentPortfolioSynthesisRunRecord, UUID(str(job.payload["run_id"])))
        if (
            run is None
            or run.status != ("prepared" if job.status == "queued" else "running")
            or run.input_hash != job.input_hash
            or run.model_config_id != self.settings.agent_paper_formal_model_config_id
        ):
            raise ValueError(INVALID_PROOF)
        from nexus.agents.portfolio_synthesis_persistence import load_frozen_synthesis_authority
        from nexus.agents.research_maintenance import require_research_open
        from nexus.paper.synthesis_usage_acceptance import require_synthesis_not_accepted

        require_research_open()
        await require_synthesis_not_accepted(session, run.id)
        hold = await session.scalar(
            select(AgentPortfolioSynthesisBudgetReservationRecord).where(
                AgentPortfolioSynthesisBudgetReservationRecord.run_id == run.id
            )
        )
        frozen = await load_frozen_synthesis_authority(session, run)
        model = await session.get(AgentModelConfigRecord, run.model_config_id)
        if (
            model is None
            or not self.supports(model)
            or prepared["model_hash"] != model.config_hash
            or prepared["run_id"] != str(run.id)
            or prepared["price_hash"] != original_price(model).content_hash
            or hold is None
            or hold.state != "reserved"
            or hold.settled_usd is not None
            or hold.pricing_version != model.version
            or hold.reserved_usd != model.worst_case_cost_usd
            or hold.business_date != now.date()
            or frozen.account_id != self.settings.agent_paper_account_id
        ):
            raise ValueError(INVALID_PROOF)
        original_price(model).require_current(
            now=now, deadline=now + timedelta(seconds=SYNTHESIS_HANDLER_SECONDS)
        )
        if now >= frozen.expires_at:
            raise ValueError("PORTFOLIO_SYNTHESIS_SESSION_EXPIRED")

    async def _write_evidence(
        self, claim: ClaimedJob, identity: dict[str, Any], phase: str, value: dict[str, Any]
    ) -> None:
        async with self.factory.begin() as session:
            await session.execute(text("SET LOCAL lock_timeout='1s'"))
            await session.execute(text("SET LOCAL statement_timeout='5s'"))
            at = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(at, datetime):
                raise ValueError(INVALID_PROOF)
            job, _, _ = await _lock_active_claim(session, claim=claim, now=at)
            if phase == "authorized":
                from nexus.agents.portfolio_synthesis_worker import formal_synthesis_job_qualified

                if await read_receipt(session, job.id, "authorized") is not None:
                    raise PortfolioAlreadyDispatched("PORTFOLIO_PLATFORM_ALREADY_AUTHORIZED")
                if not await formal_synthesis_job_qualified(
                    session, job, now=at, claimed=True, transport=self
                ):
                    raise _ScopeRejected("PORTFOLIO_PLATFORM_SOURCE_CHANGED")
                try:
                    await self.require_job(session, job, at)
                except ValueError as error:
                    if str(error) in {
                        "PLATFORM_PRICE_WINDOW_INVALID",
                        "PLATFORM_HTTP_CREDENTIAL_CHANGED",
                        "PORTFOLIO_SYNTHESIS_SESSION_EXPIRED",
                    }:
                        raise _ScopeRejected(str(error)) from error
                    raise
            elif phase == "terminal" and await read_receipt(session, job.id, "cleanup") is not None:
                raise ValueError("PORTFOLIO_PLATFORM_LATE_TERMINAL")
            else:
                auth = await read_receipt(session, job.id, "authorized")
                if auth != identity and not (
                    phase == "cleanup" and auth is None and value.get("sent") is False
                ):
                    raise ValueError(INVALID_PROOF)
            if phase == "terminal":
                from nexus.agents.persistence import _lock_budget_day
                from nexus.persistence.models.agents import (
                    AgentPortfolioSynthesisBudgetReservationRecord,
                )

                run_id = job.payload.get("run_id")
                if not isinstance(run_id, str):
                    raise ValueError(INVALID_PROOF)
                day = await session.scalar(
                    select(AgentPortfolioSynthesisBudgetReservationRecord.business_date).where(
                        AgentPortfolioSynthesisBudgetReservationRecord.run_id == UUID(run_id)
                    )
                )
                if day is None:
                    raise ValueError(INVALID_PROOF)
                await _lock_budget_day(session, day.isoformat())
            await append_receipt(session, job.id, phase, value, at)

    async def invoke(
        self,
        payload: Mapping[str, object],
        *,
        lifecycle: SynthesisDeadline | None = None,
        on_usage: Callable[[UsageSnapshot], None] | None = None,
    ) -> tuple[str, UsageSnapshot]:
        claim = self.claim
        if lifecycle is None or claim is None:
            raise ValueError(INVALID_PROOF)
        self.claim = None
        credential = platform_native.credential_for_preparation(self.settings)
        async with self.factory() as session:
            if await read_receipt(session, claim.job_id, "authorized") is not None:
                raise PortfolioAlreadyDispatched("PORTFOLIO_PLATFORM_ALREADY_AUTHORIZED")
            prep = await read_receipt(session, claim.job_id, "prepared")
            model = await session.get(
                AgentModelConfigRecord, self.settings.agent_paper_formal_model_config_id
            )
            if prep is None or model is None or not self.supports(model):
                raise ValueError(INVALID_PROOF)
            wire = compile_request(model, payload)
            if (
                prep["body_hash"] != wire_digest(wire)
                or prep["input_hash"] != payload["input_hash"]
                or prep["credential_binding_hash"] != credential.binding_hash
            ):
                raise ValueError("PORTFOLIO_PLATFORM_WIRE_CHANGED")
            if credential.api_key.get_secret_value().encode() in wire:
                raise ValueError("PORTFOLIO_PLATFORM_PRIVATE_VALUE")
            price = original_price(model)
        identity = _fence(claim) | {"body_hash": prep["body_hash"]}
        request = httpx.Request(
            "POST",
            "https://api.openai.com/v1/responses",
            content=wire,
            headers={"content-type": "application/json"},
        )

        async def record(phase: str, value: dict[str, Any]) -> None:
            if phase == "authorized":
                # A lost authorization acknowledgement never reaches client.send.
                await self._write_evidence(claim, identity, phase, value)
                return
            try:
                await self._write_evidence(claim, identity, phase, value)
            except Exception:
                # After I/O, only an exact committed observation can resolve lost
                # acknowledgement. No second write or duplicate provider call.
                async with self.factory.begin() as session:
                    await session.execute(text("SET TRANSACTION READ ONLY"))
                    await session.execute(text("SET LOCAL statement_timeout='5s'"))
                    existing = await read_receipt(session, claim.job_id, phase)
                if existing != value:
                    raise

        async def authorize() -> None:
            await record("authorized", identity)

        client = platform_native.native_http_client()

        def headers() -> None:
            if credential.binding_hash != prep["credential_binding_hash"]:
                raise ValueError(INVALID_PROOF)
            if (
                str(request.url) != "https://api.openai.com/v1/responses"
                or wire_digest(request.content) != prep["body_hash"]
                or any(
                    k in client.headers or k in request.headers
                    for k in ("openai-project", "openai-organization")
                )
                or client.auth is not None
                or client.follow_redirects
            ):
                raise _ScopeRejected("PORTFOLIO_PLATFORM_DESTINATION_INVALID")
            request.headers.update(credential.request_headers())

        publication: asyncio.Task[None] | None = None
        accepting_terminal = True
        result: _Terminal | None = None

        def terminal(value: dict[str, Any], facts: ResponsesStreamFacts) -> None:
            nonlocal result, publication
            if not accepting_terminal or publication is not None:
                raise ValueError(INVALID_PROOF)
            result = _observe_terminal(value, facts, price)
            publication = asyncio.create_task(
                record("terminal", result.evidence | {"identity": identity})
            )
            from nexus.agents.research_stages.support import consume_owned_task

            publication.add_done_callback(consume_owned_task)

        observation = await send_response_stage(
            client,
            request,
            pre_send=authorize,
            before_send=headers,
            on_terminal=terminal,
            work_deadline=lifecycle.work_at,
            cleanup_deadline=lifecycle.cleanup_at,
        )
        accepting_terminal = False
        for pending_task in observation.cleanup.pending:
            _PENDING_CLEANUP.add(pending_task)
            pending_task.add_done_callback(_PENDING_CLEANUP.discard)
        if isinstance(observation.error, PortfolioAlreadyDispatched):
            raise observation.error
        publication_error = None
        if publication is not None:
            try:
                await publication
            except (Exception, asyncio.CancelledError) as error:
                publication_error = error
        output_error = None if result is None else result.error
        measured = None if result is None else result.usage
        output = None if result is None else result.output
        from nexus.agents.http_failure import http_failure_facts

        cause = (
            observation.error
            or observation.cleanup.error
            or observation.cleanup.interrupted
            or publication_error
            or output_error
        )
        if cause is None and not observation.cleanup.confirmed:
            cause = ValueError("PORTFOLIO_PLATFORM_CLEANUP_UNCONFIRMED")
        facts = (
            None
            if cause is None
            else http_failure_facts(
                observation, cause, output_error=output_error, proof_error=publication_error
            )
        )
        try:
            await record(
                "cleanup",
                dict(
                    identity=identity,
                    sent=observation.owner.send_started,
                    confirmed=observation.cleanup.confirmed,
                    failed=cause is not None,
                    terminal_write_confirmed=publication_error is None,
                    pre_send_rejected=isinstance(observation.error, _ScopeRejected)
                    and not observation.owner.send_started,
                    failure_facts=None if facts is None else facts.model_dump(mode="json"),
                ),
            )
        except Exception as receipt_error:
            primary = observation.error or publication_error or receipt_error
            if isinstance(primary, asyncio.CancelledError):
                raise primary from receipt_error
            raise PortfolioHTTPError(
                "PORTFOLIO_SYNTHESIS_CALL_UNCERTAIN", primary
            ) from receipt_error
        if publication_error is not None:
            if isinstance(publication_error, asyncio.CancelledError):
                raise publication_error
            raise PortfolioHTTPError(
                "PORTFOLIO_SYNTHESIS_CALL_UNCERTAIN", publication_error
            ) from publication_error
        if measured is not None and observation.cleanup.confirmed and on_usage is not None:
            on_usage(measured)
        if (
            output is None
            or measured is None
            or observation.error is not None
            or not observation.cleanup.confirmed
        ):
            if isinstance(observation.error, asyncio.CancelledError):
                raise observation.error
            code = (
                "PORTFOLIO_SYNTHESIS_PRE_DISPATCH_PREFLIGHT"
                if isinstance(observation.error, _ScopeRejected)
                else "PORTFOLIO_SYNTHESIS_CALL_UNCERTAIN"
                if measured is None or not observation.cleanup.confirmed
                else "PORTFOLIO_SYNTHESIS_FAILED"
            )
            raise PortfolioHTTPError(code, observation.error) from observation.error
        return output, measured


def _usage_fields(raw: object) -> dict[str, Any]:
    """Persist actual known counter fields; never copy arbitrary provider extras into audit."""

    def counts(value: object, names: tuple[str, ...]) -> dict[str, Any]:
        if not isinstance(value, dict):
            return {}
        return {k: value[k] if type(value[k]) is int else None for k in names if k in value}

    root = counts(raw, ("input_tokens", "output_tokens", "total_tokens"))
    if isinstance(raw, dict):
        details = raw.get("input_tokens_details")
        root["input_tokens_details"] = counts(details, ("cached_tokens", "cache_write_tokens"))
        if isinstance(details, dict):
            root["input_tokens_details"]["cache_write_tokens_details"] = counts(
                details.get("cache_write_tokens_details"), ("ttl_30m_tokens", "ttl_12h_tokens")
            )
        root["output_tokens_details"] = counts(
            raw.get("output_tokens_details"), ("reasoning_tokens",)
        )
    return root
