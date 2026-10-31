"""Research-specific transaction owner; legacy Jobs keep their original executor."""

from __future__ import annotations

import asyncio
import json
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import timedelta
from typing import Any

from sqlalchemy import select, text

from nexus.agents import codex_sdk, codex_staged
from nexus.agents.budget import ResearchAccountingBusy
from nexus.agents.committee import ROLE_ORDER
from nexus.agents.database_tools import database_read_only_tools
from nexus.agents.execution import execute_research_run
from nexus.agents.execution_authority import load_frozen_evidence
from nexus.agents.models import ResearchRequest
from nexus.agents.research_dispatch import dispatch_failure_boundary
from nexus.agents.research_stages.lifecycle import invoke_stage
from nexus.agents.research_stages.protocol import (
    ProtocolRejected,
    StagedProtocol,
    evidence_digest,
    known_usage_lower_bound,
    stage_evidence,
    usage_known,
)
from nexus.agents.research_stages.routes import selected_route
from nexus.agents.research_stages.support import stop_preserving_error
from nexus.agents.synthesis import domain_evidence
from nexus.agents.tools import ReadOnlyTools
from nexus.jobs.repository import LeaseLostError, _lock_active_claim, execute_and_finalize_job
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.runtime import JobLease


class StagedCommitUnconfirmed(BaseException):
    """Durable outcome unknown; never enter generic Job failure/retry."""


class StagedFinalizationFailed(BaseException):
    """Research finalization requires original-identity readback."""


class Prepared(BaseException):
    def __init__(self, kwargs: Any) -> None:
        self.kwargs = kwargs


_CAPTURE: ContextVar[bool] = ContextVar("research_stage_prepare", default=False)


@contextmanager
def capture_preparation() -> Any:
    token = _CAPTURE.set(True)
    try:
        yield
    finally:
        _CAPTURE.reset(token)


async def execute_or_capture(session: Any, **kwargs: Any) -> Any:
    if _CAPTURE.get() and isinstance(
        kwargs["committee"], codex_staged.StructuredStagedCodexCommittee
    ):
        raise Prepared(kwargs)
    return await execute_research_run(session, **kwargs)


class ShortTools:
    names = ReadOnlyTools.names

    def __init__(self, factory: Any, run_id: Any, claim: Any, clock: Any, original: Any) -> None:
        self.factory, self.run_id, self.claim, self.clock = factory, run_id, claim, clock
        self.allow_sec_amendments = original.allow_sec_amendments
        self.filing_forms = original.filing_forms
        self.reads: list[str] = []

    async def execute(self, name: Any, payload: Any) -> Any:
        async with self.factory.begin() as session:
            await _lock_active_claim(session, claim=self.claim, now=self.clock())
            run = await session.get(AgentResearchRunRecord, self.run_id)
            tools = await database_read_only_tools(session, run, allow_fixture_evidence=False)
            result = await tools.execute(name, payload)
        self.reads.append(name)
        return result


class FrozenCommittee:
    """Final validation consumes in-memory results, never calls transport again."""

    retry_on_error = False
    failure_reason: str | None
    usage: Any

    def __init__(self, committee: Any, outputs: Any, request: Any, evidence: Any) -> None:
        self.committee, self.outputs = committee, outputs
        self.request, self.evidence = request, evidence

    def __getattr__(self, name: Any) -> Any:
        return getattr(self.committee, name)

    async def analyze(self, role: Any, request: Any, evidence_refs: Any, prompt: Any) -> Any:
        if request != self.request or evidence_refs != self.evidence:
            raise ProtocolRejected("FINAL_FROZEN_AUTHORITY_CHANGED")
        if role not in self.outputs or prompt != self.committee._prompts[role]:
            raise ProtocolRejected("FINAL_ROLE_UNAVAILABLE")
        return self.outputs[role]


async def execute_staged_claim(
    *, factory: Any, claim: Any, clock: Any, execute: Any, comparison: Any = None
) -> Any:
    from nexus.agents.research_maintenance import require_research_open

    require_research_open()
    wall_anchor = clock()
    monotonic_anchor = asyncio.get_running_loop().time()
    original_clock = clock

    def anchored_clock() -> Any:
        return max(
            original_clock(),
            wall_anchor + timedelta(seconds=asyncio.get_running_loop().time() - monotonic_anchor),
        )

    clock = anchored_clock
    async with factory() as session:
        route = await selected_route(session, claim.job_id)
        if route is not None and route["kind"] == "independent":
            from nexus.paper.independent_unknown_acceptance import (
                require_independent_execution_open,
            )

            await require_independent_execution_open(session, request_id=route["scope_id"])
    if route is None:
        raise ProtocolRejected("STAGED_ROUTE_REQUIRED")
    if (route["kind"] == "comparison") != (comparison is not None):
        raise ProtocolRejected("COMPARISON_EXECUTION_BINDING_REQUIRED")

    captured_preparation = False
    with capture_preparation():
        try:
            async with factory() as preparation, dispatch_failure_boundary(preparation):
                await preparation.begin()
                await preparation.execute(
                    text("SELECT id FROM research_stage_scope WHERE id=:scope FOR UPDATE"),
                    {"scope": route["scope_id"]},
                )
                await _lock_active_claim(preparation, claim=claim, now=clock())
                if comparison is not None:
                    await comparison.validate(preparation, claim, route)
                try:
                    result = await execute_and_finalize_job(
                        preparation, claim=claim, checked_at=clock(), execute=execute
                    )
                except Prepared as prepared:
                    # This is intentional capture, not a dispatch failure. Remove
                    # the failure listener before session exit rolls it back.
                    args = prepared.kwargs
                    captured_preparation = True
                else:
                    if await preparation.scalar(
                        text("SELECT 1 FROM research_stage_member WHERE job_id=:job"),
                        {"job": claim.job_id},
                    ):
                        raise ProtocolRejected("UNCALLED_PROOF_HAS_STAGE_MEMBER")
                    from nexus.paper.research_failure import (
                        require_claimed_uncalled_research_closed,
                    )
                    from nexus.paper.research_request import database_now

                    await require_claimed_uncalled_research_closed(
                        preparation,
                        run_id=route["run_id"],
                        job_id=claim.job_id,
                        now=await database_now(preparation),
                    )
                    await preparation.execute(
                        text("UPDATE research_stage_scope SET stopped=true WHERE id=:scope"),
                        {"scope": route["scope_id"]},
                    )
                    try:
                        await preparation.commit()
                    except Exception as error:
                        raise StagedCommitUnconfirmed("UNCALLED_COMMIT_UNCONFIRMED") from error
        except StagedCommitUnconfirmed:
            raise
        except Exception as error:
            raise StagedFinalizationFailed("UNCALLED_TRANSACTION_FAILED") from error
        else:
            if not captured_preparation:
                return result
    committee, run_id = (args["committee"], args["run_id"])
    assert isinstance(committee, codex_staged.StructuredStagedCodexCommittee)
    original_deadline = committee._deadline
    if original_deadline is None:
        raise ProtocolRejected("STAGED_DEADLINE_REQUIRED")
    work_deadline = wall_anchor + timedelta(seconds=original_deadline - monotonic_anchor)
    async with factory.begin() as session:
        scope = route["scope_id"]
        lease = await session.scalar(select(JobLease).where(JobLease.job_id == claim.job_id))
        original_expiry = lease.expires_at
        run = await session.get(AgentResearchRunRecord, run_id)
        platform_model = (
            await session.get(AgentModelConfigRecord, run.model_config_id)
            if comparison is not None and comparison.platform_funding_loader is not None
            else None
        )
        refs = tuple(
            domain_evidence(r)
            for r in await load_frozen_evidence(session, run, allow_fixture_evidence=False)
        )
        request = ResearchRequest(
            instrument_id=run.instrument_id,
            data_snapshot_id=run.data_snapshot_id,
            decision_time=run.decision_time,
        )
    from nexus.agents.research_dispatch import stop_current_dispatch

    protocol = StagedProtocol(factory, clock=clock, on_failure=stop_current_dispatch)
    await protocol.install(
        scope_id=scope,
        claims=(claim,),
        now=clock(),
        increment=route["kind"] == "increment",
        independent=route["kind"] == "independent",
        comparison=route["kind"] == "comparison",
        work_deadline=work_deadline,
        cleanup_deadline=min(original_expiry, work_deadline + timedelta(seconds=7)),
    )

    tools = ShortTools(factory, run_id, claim, clock, committee._tools)
    committee._tools = tools
    outputs = {}
    stage_number = 0

    async def invocation(payload: Any, **options: Any) -> Any:
        nonlocal stage_number
        stage_number += 1
        stage_deadline = wall_anchor + timedelta(
            seconds=min(original_deadline, options.get("deadline_at", original_deadline))
            - monotonic_anchor
        )
        chain = []
        if stage_number > 1:
            async with factory.begin() as check:
                await _lock_active_claim(check, claim=claim, now=clock())
                chain = await stage_evidence(check, run_id)
            if len(chain) != stage_number - 1 or not usage_known(chain):
                await protocol.stop(scope)
                raise ProtocolRejected("PERSISTED_PREDECESSOR_MISMATCH")
        from nexus.agents.platform_manifest import PlatformManifest

        platform_wire = None
        if comparison is not None and isinstance(comparison.manifest, PlatformManifest):
            platform_wire = comparison.platform_wire(payload, options["output_model"], chain)
            wire = platform_wire.wire
        else:
            wire = (
                comparison.wire(payload, options["output_model"])
                if comparison is not None
                else json.dumps(
                    {
                        "wire": codex_sdk.codex_request_wire(payload),
                        "schema": codex_sdk.codex_output_schema(options["output_model"]),
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            )
        try:
            ticket = await protocol.authorize(
                claim,
                stage=stage_number,
                now=clock(),
                wire=wire,
                not_after=stage_deadline,
                **({"platform_wire": platform_wire} if platform_wire is not None else {}),
            )
        except (ProtocolRejected, LeaseLostError, ResearchAccountingBusy):
            raise
        except Exception as error:
            await stop_preserving_error(protocol, scope, error)
            raise StagedCommitUnconfirmed("AUTHORIZATION_COMMIT_UNCONFIRMED") from error
        old_guard = options.get("dispatch_guard")

        def guard() -> Any:
            if protocol.local_stopped:
                raise codex_sdk.CodexInvocationError(
                    "CODEX_RESEARCH_STOPPED", provider_call_started=False
                )
            if platform_wire is not None:
                from nexus.agents.platform_funding import require_funding_binding

                assert comparison.platform_funding_loader is not None
                # This guard runs after the final async qualification/transaction
                # exit, with no further await before send. Re-read actual local
                # identity and both proof originals, not two cached objects.
                fresh = comparison.platform_funding_loader()
                require_funding_binding(comparison.manifest, fresh)
                at = clock()
                if (
                    platform_model is None
                    or not platform_model.pricing_available_at
                    <= at
                    < platform_model.pricing_expires_at
                ):
                    raise ProtocolRejected("PLATFORM_PRICE_WINDOW_INVALID")
                record = fresh.proof.record
                if not record.window_start <= at < fresh.proof.valid_until:
                    raise ProtocolRejected("PLATFORM_FUNDING_WINDOW_INVALID")
            if old_guard is not None:
                old_guard()

        options["dispatch_guard"] = guard
        if comparison is not None and comparison.transport == "responses":
            from nexus.agents.research_stages.http_lifecycle import invoke_http_stage

            work = min(original_deadline, options.get("deadline_at", original_deadline))
            cleanup = min(
                work + 7,
                original_deadline,
                monotonic_anchor + (original_expiry - wall_anchor).total_seconds(),
            )
            platform_options = {}
            if platform_wire is not None:
                from nexus.agents.platform_funding import require_platform_funding

                async def pre_send() -> Any:
                    assert comparison.platform_funding_loader is not None
                    funding = comparison.platform_funding_loader()
                    async with factory.begin() as check:
                        member = await protocol._member(check, claim)
                        await _lock_active_claim(check, claim=claim, now=clock())
                        if member["stopped"] or protocol.local_stopped or clock() >= stage_deadline:
                            raise ProtocolRejected("STOPPED_OR_DEADLINE")
                        await require_platform_funding(
                            check,
                            comparison.manifest,
                            funding,
                            required_valid_until=wall_anchor
                            + timedelta(seconds=cleanup - monotonic_anchor),
                        )
                    return funding.credential

                assert comparison.platform_funding is not None
                platform_options = dict(
                    platform_credential=comparison.platform_funding.credential,
                    platform_wire=platform_wire,
                    platform_pre_send=pre_send,
                )
            return await invoke_http_stage(
                protocol=protocol,
                scope=scope,
                ticket=ticket,
                wire=wire,
                output_model=options["output_model"],
                grant=comparison.grant,
                clock=clock,
                wall_anchor=wall_anchor,
                monotonic_anchor=monotonic_anchor,
                work_deadline=work,
                cleanup_deadline=cleanup,
                client=comparison.client_factory(),
                dispatch_guard=guard,
                **platform_options,
            )
        return await invoke_stage(
            protocol,
            scope,
            ticket,
            wire,
            clock,
            wall_anchor,
            monotonic_anchor,
            original_deadline,
            original_expiry,
            payload,
            options,
        )

    committee.stage_invoker = invocation
    for role in ROLE_ORDER:
        try:
            outputs[role] = await committee.analyze(role, request, refs, committee._prompts[role])
        except ResearchAccountingBusy as error:
            await stop_preserving_error(protocol, scope, error)
            raise
        except Exception:
            break
    assert committee._deadline == original_deadline
    frozen = FrozenCommittee(committee, outputs, request, refs)
    try:
        async with factory() as final, dispatch_failure_boundary(final):
            await final.begin()
            member = await protocol._member(final, claim)
            rows = await stage_evidence(final, run_id)
            matched = bool(rows) and all(row["verified"] is True for row in rows)
            known = usage_known(rows)
            complete = (
                known and len(rows) == 3 and all(row["confirmed"] and row["valid"] for row in rows)
            )
            publishing_stopped = member["stopped"] or protocol.local_stopped
            if publishing_stopped:
                frozen.outputs = {}
                frozen.failure_reason = (
                    "model_research_stopped" if known else "model_usage_uncertain"
                )
            elif not matched or (len(outputs) == len(ROLE_ORDER) and (not complete)):
                frozen.outputs = {}
                frozen.failure_reason = (
                    "model_research_stopped" if known else "model_usage_uncertain"
                )
            elif not known:
                frozen.failure_reason = "model_usage_uncertain"
            elif (
                len(outputs) < len(ROLE_ORDER)
                and committee.failure_reason == "model_usage_uncertain"
            ):
                frozen.failure_reason = "model_research_stopped"
            frozen.usage = known_usage_lower_bound(rows)

            async def finalize(target: Any) -> Any:
                from nexus.agents.persistence import require_reserved_daily_budget

                # This callback owns a short final transaction, not a legacy
                # provider wait. Acquire its financial boundary before the
                # shared executor can take the legacy blocking date check.
                await require_reserved_daily_budget(target, run_id=run_id, accounting=True)
                result = await execute_research_run(target, **{**args, "committee": frozen})
                from nexus.jobs.repository import JobCompletion
                from nexus.worker import _agent_result

                mapped = _agent_result(result.status)
                if result.status != "completed":
                    await protocol.stop_terminal_in_transaction(
                        target,
                        scope_id=scope,
                        run_id=run_id,
                        rows=rows,
                    )
                closed_at = clock()
                await target.execute(
                    text(
                        "INSERT INTO "
                        "research_stage_terminal(run_id,known,chain_hash,closed_at,observed_stages,"
                        "evidence) VALUES(:run,:known,:hash,:now,:observed,"
                        "CAST(:evidence AS jsonb))"
                    ),
                    {
                        "run": run_id,
                        "known": known,
                        "hash": evidence_digest(rows),
                        "now": closed_at,
                        "observed": [r["stage"] for r in rows if r["response_hash"] is not None],
                        "evidence": json.dumps(rows),
                    },
                )
                return JobCompletion(
                    outcome=mapped.outcome,
                    result_code=mapped.result_code,
                    error_code=mapped.error_code,
                    finished_at=closed_at,
                )

            result = await execute_and_finalize_job(
                final, claim=claim, checked_at=clock(), execute=finalize
            )
            assert (
                await final.scalar(select(JobLease).where(JobLease.job_id == claim.job_id))
            ).expires_at == original_expiry
            try:
                await protocol._commit(final, member["scope_id"])
            except Exception as error:
                raise StagedCommitUnconfirmed("TERMINAL_COMMIT_UNCONFIRMED") from error
    except StagedCommitUnconfirmed:
        raise
    except ResearchAccountingBusy as error:
        # The final transaction has exited. Preserve the explicit busy cause
        # and all previously committed provider/cleanup evidence, without retry.
        await stop_preserving_error(protocol, scope, error)
        raise
    except BaseException as error:
        await stop_preserving_error(protocol, scope, error)
        if isinstance(error, Exception):
            raise StagedFinalizationFailed("TERMINAL_TRANSACTION_FAILED") from error
        raise
    return result
