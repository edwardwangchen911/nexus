"""Research-specific transaction owner; legacy Jobs keep their original executor."""

from __future__ import annotations

import asyncio
import json
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import timedelta
from typing import Any, cast

from sqlalchemy import select, text

from nexus.agents import codex_sdk, codex_staged
from nexus.agents.budget import ResearchAccountingBusy
from nexus.agents.committee import ROLE_ORDER
from nexus.agents.database_tools import database_read_only_tools
from nexus.agents.execution import execute_research_run
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
from nexus.agents.tools import ReadOnlyTools
from nexus.jobs.repository import LeaseLostError, _lock_active_claim, execute_and_finalize_job
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.runtime import JobLease


class StagedCommitUnconfirmed(BaseException):
    """Durable outcome unknown; never enter generic Job failure/retry."""


class StagedFinalizationFailed(BaseException):
    """Research finalization requires original-identity readback."""


class Prepared(BaseException):
    def __init__(self, kwargs: Any, request: Any, evidence: Any) -> None:
        self.kwargs = kwargs
        self.request, self.evidence = request, evidence


_CAPTURE: ContextVar[bool] = ContextVar("research_stage_prepare", default=False)


@contextmanager
def capture_preparation() -> Any:
    token = _CAPTURE.set(True)
    try:
        yield
    finally:
        _CAPTURE.reset(token)


async def execute_or_capture(session: Any, **kwargs: Any) -> Any:
    return await execute_research_run(session, **kwargs)


def is_staged_preparation(committee: Any) -> bool:
    return _CAPTURE.get() and isinstance(committee, codex_staged.StructuredStagedCodexCommittee)


def capture_validated_preparation(*, request: Any, evidence: Any, **kwargs: Any) -> None:
    """Capture only the common executor's fully validated, unsent preparation."""
    if is_staged_preparation(kwargs["committee"]):
        raise Prepared(kwargs, request, evidence)


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
    *,
    factory: Any,
    claim: Any,
    clock: Any,
    execute: Any,
    comparison: Any = None,
    native_settings: Any = None,
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

    native = route.get("platform_binding") is not None
    if native != (native_settings is not None) or native and comparison is not None:
        raise ProtocolRejected("PLATFORM_NATIVE_EXECUTION_REQUIRED")

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
                    request, refs = prepared.request, prepared.evidence
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
        except (StagedCommitUnconfirmed, ResearchAccountingBusy):
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
            if native or comparison is not None and comparison.platform_funding_loader is not None
            else None
        )
        native_price, native_binding, native_allocation, native_credential = None, None, None, None
        if native:
            from nexus.agents.platform_native import (
                credential_for_preparation,
                read_native_allocation,
            )
            from nexus.agents.platform_pricing import sol_price_record

            native_binding, native_allocation = await read_native_allocation(session, run=run)
            native_credential = credential_for_preparation(native_settings)
            if (
                native_credential.binding_hash != native_binding["credential_binding_hash"]
                or committee._platform_allocation != native_allocation
            ):
                raise ProtocolRejected("PLATFORM_NATIVE_EXECUTION_BINDING_INVALID")
            if platform_model is None or platform_model.platform_profile is None:
                raise ProtocolRejected("PLATFORM_NATIVE_MODEL_BINDING_INVALID")
            native_price = sol_price_record(platform_model.platform_profile["price_record"])
    from nexus.agents.research_dispatch import stop_current_dispatch

    protocol = StagedProtocol(
        factory, clock=clock, on_failure=stop_current_dispatch, native_settings=native_settings
    )
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
    failure_phase = "input_preparation"
    stage_failure: dict[str, object] | None = None

    async def invocation(payload: Any, **options: Any) -> Any:
        nonlocal stage_number, failure_phase
        failure_phase = "compile"
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
            if len(chain) != stage_number - 1 or not usage_known(chain, model=platform_model):
                await protocol.stop(scope)
                raise ProtocolRejected("PERSISTED_PREDECESSOR_MISMATCH")
        from nexus.agents.platform_manifest import PlatformManifest

        platform_wire = None
        if native:
            from nexus.agents.platform_wire import (
                PriorPlatformUsage,
                Stage,
                compile_native_platform_stage,
                native_platform_input_hash,
            )

            if native_allocation is None or native_binding is None or native_price is None:
                raise ProtocolRejected("PLATFORM_NATIVE_EXECUTION_REQUIRED")
            input_hash = native_platform_input_hash(
                committee, request=request, evidence_refs=refs, inputs=committee._frozen_inputs
            )
            platform_wire = compile_native_platform_stage(
                input_hash=input_hash,
                allocation=native_allocation,
                expected_allocation_hash=native_binding["allocation_hash"],
                stage=cast(Stage, native_allocation.stage_order[stage_number - 1]),
                compiled_request=(payload, options["output_model"]),
                prior_usage=[
                    PriorPlatformUsage(
                        stage=r["stage"],
                        input_hash=input_hash,
                        verified=r["verified"] is True and r["eligible"] is True,
                        response=json.loads(r["platform_response"]),
                    )
                    for r in chain
                ],
                price_record=native_price,
            )
            wire = platform_wire.wire
        elif comparison is not None and isinstance(comparison.manifest, PlatformManifest):
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
        failure_phase = "authorization"
        try:
            ticket = await protocol.authorize(
                claim,
                stage=stage_number,
                now=clock(),
                wire=wire,
                not_after=stage_deadline,
                **({"platform_wire": platform_wire} if platform_wire is not None else {}),
            )
        except ProtocolRejected as error:
            from nexus.paper.independent_history_scope import SAFE_CODES
            from nexus.paper.research_failure import record_research_failure

            if route["kind"] == "independent" and str(error) in SAFE_CODES:
                try:
                    async with asyncio.timeout(1), factory.begin() as diagnostic:
                        await record_research_failure(
                            diagnostic,
                            request_id=route["scope_id"],
                            error=error,
                            stage=stage_number,
                        )
                except BaseException:
                    error.add_note("RESEARCH_FAILURE_AUDIT_UNCONFIRMED")
            raise
        except (LeaseLostError, ResearchAccountingBusy):
            raise
        except Exception as error:
            await stop_preserving_error(protocol, scope, error)
            raise StagedCommitUnconfirmed("AUTHORIZATION_COMMIT_UNCONFIRMED") from error
        failure_phase = "transport"
        old_guard = options.get("dispatch_guard")

        def guard() -> Any:
            if protocol.local_stopped:
                raise codex_sdk.CodexInvocationError(
                    "CODEX_RESEARCH_STOPPED", provider_call_started=False
                )
            if native:
                from nexus.agents.platform_native import credential_for_preparation

                if native_binding is None or native_price is None:
                    raise ProtocolRejected("PLATFORM_NATIVE_EXECUTION_REQUIRED")
                if (
                    credential_for_preparation(native_settings).binding_hash
                    != native_binding["credential_binding_hash"]
                ):
                    raise ProtocolRejected("PLATFORM_HTTP_CREDENTIAL_CHANGED")
                native_price.require_current(now=clock(), deadline=stage_deadline)
            elif platform_wire is not None:
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
        if native or comparison is not None and comparison.transport == "responses":
            from nexus.agents.research_stages.http_lifecycle import invoke_http_stage

            work = min(original_deadline, options.get("deadline_at", original_deadline))
            cleanup = min(
                work + 7,
                original_deadline,
                monotonic_anchor + (original_expiry - wall_anchor).total_seconds(),
            )
            platform_options: dict[str, Any] = {}
            if native:

                async def native_pre_send() -> Any:
                    from nexus.agents.platform_native import credential_for_preparation

                    async with factory.begin() as check:
                        member = await protocol._member(check, claim)
                        await _lock_active_claim(check, claim=claim, now=clock())
                        if member["stopped"] or protocol.local_stopped or clock() >= stage_deadline:
                            raise ProtocolRejected("STOPPED_OR_DEADLINE")
                        guard()
                    return credential_for_preparation(native_settings)

                platform_options = dict(
                    platform_credential=native_credential,
                    platform_wire=platform_wire,
                    platform_pre_send=native_pre_send,
                )
            elif platform_wire is not None:
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
            from nexus.agents.platform_native import native_http_client

            return await invoke_http_stage(
                protocol=protocol,
                scope=scope,
                ticket=ticket,
                wire=wire,
                output_model=options["output_model"],
                grant=None if native else comparison.grant,
                clock=clock,
                wall_anchor=wall_anchor,
                monotonic_anchor=monotonic_anchor,
                work_deadline=work,
                cleanup_deadline=cleanup,
                client=native_http_client() if native else comparison.client_factory(),
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
        failure_phase = "input_preparation"
        try:
            outputs[role] = await committee.analyze(role, request, refs, committee._prompts[role])
        except ResearchAccountingBusy as error:
            await stop_preserving_error(protocol, scope, error)
            raise
        except Exception as error:
            from nexus.paper.research_failure import PRECLAIM_CODES

            stage_failure = {
                "run_id": str(run_id),
                "job_id": str(claim.job_id),
                "attempt_no": claim.attempt_no,
                "fence_token": claim.fence_token,
                "scope_id": str(scope),
                "stage": ROLE_ORDER.index(role) + 1,
                "failure_phase": failure_phase,
                "failure_code": str(error)
                if type(error) in {ValueError, ProtocolRejected}
                and str(error)
                in PRECLAIM_CODES
                | {
                    "PLATFORM_PROFILE_INVALID",
                    "PLATFORM_WIRE_CHANGED",
                    "PLATFORM_COMPACT_INSTRUCTIONS_REQUIRED",
                    "PLATFORM_ALLOCATION_CHANGED",
                    "COMPARISON_INPUT_CHANGED",
                    "PLATFORM_CAPACITY_ENVELOPE_CHANGED",
                    "PLATFORM_INPUT_TOO_LARGE",
                    "PLATFORM_PRIOR_USAGE_INVALID",
                    "PLATFORM_OUTPUT_EXHAUSTED",
                    "PLATFORM_STAGE_INVALID",
                    "PLATFORM_NATIVE_REQUEST_REQUIRED",
                }
                else "RESEARCH_FAILURE_UNCLASSIFIED",
                "exception_kind": "timeout"
                if isinstance(error, TimeoutError)
                else "protocol_rejected"
                if type(error) is ProtocolRejected
                else "invalid_input"
                if type(error) is ValueError
                else "unexpected_exception",
            }
            break
    assert committee._deadline == original_deadline
    frozen = FrozenCommittee(committee, outputs, request, refs)
    try:
        async with factory() as final, dispatch_failure_boundary(final):
            await final.begin()
            member = await protocol._member(final, claim)
            rows = await stage_evidence(final, run_id)
            matched = bool(rows) and all(row["verified"] is True for row in rows)
            known = usage_known(rows, model=platform_model)
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
                if stage_failure is not None:
                    from nexus.audit.service import append_audit_event

                    await append_audit_event(
                        target,
                        actor_id="research-stage-worker",
                        action="research.stage_failure",
                        schema_version="research_stage_failure_v1",
                        occurred_at=closed_at,
                        payload=stage_failure,
                    )
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
