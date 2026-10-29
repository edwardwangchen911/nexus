"""Isolated Worker composition, never installed as a production entry.

The native claim/registry, committee, SDK boundary, tools and final validator
are real. Only the SDK process is controlled. Preparation is rolled back before
external work; no commit is inserted inside the legacy long handler.
"""

import asyncio
import hashlib
import json
import sys
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import timedelta
from types import SimpleNamespace

from helpers.staged_protocol import (
    ProtocolRejected,
    StagedProtocol,
    evidence_digest,
    stage_evidence,
    usage_known,
)
from nexus.agents import codex_sdk, codex_staged
from nexus.agents.committee import ROLE_ORDER
from nexus.agents.database_tools import database_read_only_tools
from nexus.agents.execution import execute_research_run
from nexus.agents.execution_authority import load_frozen_evidence
from nexus.agents.models import ResearchRequest
from nexus.agents.synthesis import domain_evidence
from nexus.agents.tools import ReadOnlyTools
from nexus.jobs import worker as jobs_worker
from nexus.jobs.repository import LeaseLostError, _lock_active_claim
from nexus.monitoring.admission import require_monitor_account_available as native_monitor_available
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.runtime import JobLease
from nexus.worker import run_agent_worker_once as native_worker
from sqlalchemy import select, text
from unit.agents.test_codex_sdk import _sdk


class StagedCommitUnconfirmed(BaseException):
    """Research-only boundary result; must bypass the legacy generic _fail."""


class StagedCleanupFailure(BaseException):
    """Private SDK/core transport only; never a protocol or accounting result."""

    def __init__(self, error):
        self.error = error


class StagedFinalizationFailed(BaseException):
    """Final processing failed; read original durable state, never use generic _fail.

    This type alone does not prove rollback: session exit may fail after commit.
    """


class StagedExecutionInterrupted(BaseException):
    """A controlled pre-final interruption cannot enter legacy generic _fail."""


class Prepared(BaseException):
    def __init__(self, kwargs):
        self.kwargs = kwargs


class WorkerBindings:
    """Fixed test seams, installed once; each async Worker owns its callbacks.

    No protocol or production dispatch logic lives here. In particular, SDK
    clients must be explicitly bound and can never fall through to a live SDK.
    """

    def __init__(self, monkeypatch):
        from nexus.agents.research_stages import executor as worker_module

        self.callbacks = ContextVar("staged_test_worker_callbacks", default={})
        self.original = {}
        for key, module, name in (
            ("finalize", jobs_worker, "execute_and_finalize_job"),
            ("claim", jobs_worker, "claim_next_job"),
            ("prepare", worker_module, "execute_or_capture"),
            ("stage", codex_staged, "invoke_codex"),
            ("create", codex_sdk, "AsyncCodex"),
            ("checked", codex_sdk, "_checked_codex_response"),
            ("close", codex_sdk, "_close_sdk_client"),
            ("core", codex_sdk, "_invoke_codex"),
            ("bounded_close", codex_sdk, "_close_portfolio_client"),
            ("boundary", codex_sdk, "_invocation_boundary"),
        ):
            self.original[key] = getattr(module, name)

            def route(*args, _key=key, **kwargs):
                callback = self.callbacks.get().get(_key)
                if callback is None:
                    if _key == "create":
                        raise AssertionError("UNBOUND_TEST_SDK_CLIENT")
                    callback = self.original[_key]
                return callback(*args, **kwargs)

            monkeypatch.setattr(module, name, route)

    @contextmanager
    def bind(self, **callbacks):
        assert callbacks.keys() <= self.original.keys()
        token = self.callbacks.set({**self.callbacks.get(), **callbacks})
        try:
            yield
        finally:
            self.callbacks.reset(token)


async def stop_preserving_error(protocol, scope, error, *, deadline=None, task=None):
    protocol.local_stopped = True
    try:
        if deadline is None:
            await protocol.stop(scope)
        else:
            if task is None:
                if asyncio.get_running_loop().time() >= deadline:
                    raise TimeoutError
                task = asyncio.create_task(protocol.stop(scope))
                task.add_done_callback(consume_owned_task)
            if task.done():
                task.result()
                return
            done, _ = await asyncio.wait(
                (task,), timeout=max(0, deadline - asyncio.get_running_loop().time())
            )
            if task not in done:
                task.cancel()
                raise TimeoutError
            task.result()
    except BaseException:
        if task is not None and not task.done():
            task.cancel()
        if deadline is not None:
            from nexus.paper.manual_host_exit import mark_host_drain_unconfirmed

            mark_host_drain_unconfirmed()
        error.add_note("SCOPE_STOP_UNCONFIRMED")


def consume_owned_task(task):
    # A late diagnostic completion is observed, never turned into a new send.
    try:
        task.result()
    except BaseException:
        pass


class ShortTools:
    names = ReadOnlyTools.names

    def __init__(self, factory, run_id, claim, clock, original):
        self.factory, self.run_id, self.claim, self.clock = factory, run_id, claim, clock
        self.allow_sec_amendments = original.allow_sec_amendments
        self.filing_forms = original.filing_forms
        self.reads = []

    async def execute(self, name, payload):
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

    def __init__(self, committee, outputs, request, evidence):
        self.committee, self.outputs = committee, outputs
        self.request, self.evidence = request, evidence

    def __getattr__(self, name):
        return getattr(self.committee, name)

    async def analyze(self, role, request, evidence_refs, prompt):
        if request != self.request or evidence_refs != self.evidence:
            raise ProtocolRejected("FINAL_FROZEN_AUTHORITY_CHANGED")
        if role not in self.outputs or prompt != self.committee._prompts[role]:
            raise ProtocolRejected("FINAL_ROLE_UNAVAILABLE")
        return self.outputs[role]


async def run_staged_worker(settings, *, monkeypatch, fixture, **worker_options):
    factory, _, _, captured, _, _, _ = fixture
    bindings = getattr(monkeypatch, "_staged_worker_bindings", None)
    if bindings is None:
        bindings = WorkerBindings(monkeypatch)
        monkeypatch._staged_worker_bindings = bindings

    wall_anchor = captured["clock"][0]
    monotonic_anchor = asyncio.get_running_loop().time()

    def clock():
        # One fixed mapping for this Worker, including time spent waiting for DB
        # locks. Advance the fixture's shared clock so native public readers see
        # the same time domain; never grant a fresh timeout after an await.
        current = wall_anchor + timedelta(
            seconds=asyncio.get_running_loop().time() - monotonic_anchor
        )
        captured["clock"][0] = max(captured["clock"][0], current)
        return captured["clock"][0]

    native_finalize = bindings.original["finalize"]
    invoke_fixture = bindings.original["stage"]

    async def adapter(outer_session, *, claim, checked_at, execute):
        from helpers.staged_routes import selected_route

        async with factory() as routing:
            route = await selected_route(routing, claim.job_id)
        if route is None:
            # Unselected historical Jobs retain their old protocol. Only a new
            # preparation transaction can select the staged route.
            return await native_finalize(
                outer_session, claim=claim, checked_at=checked_at, execute=execute
            )
        # The legacy outer session has not issued SQL. A dedicated future
        # executor will own this seam; never commit it out from under its caller.
        assert not outer_session.sync_session.identity_map

        async def capture(session, **kwargs):
            raise Prepared(kwargs)

        with bindings.bind(prepare=capture):
            try:
                async with factory() as preparation:
                    await preparation.begin()
                    # Match authorize/terminal close: scope before Job/Lease and
                    # budget, including a pure preparation stop with no member.
                    await preparation.execute(
                        text("SELECT id FROM stage_proto_scope WHERE id=:scope FOR UPDATE"),
                        {"scope": route["scope_id"]},
                    )
                    await _lock_active_claim(preparation, claim=claim, now=clock())
                    result = await native_finalize(
                        preparation, claim=claim, checked_at=clock(), execute=execute
                    )
                    if await preparation.scalar(
                        text("SELECT 1 FROM stage_proto_member WHERE job_id=:job"),
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
                        text("UPDATE stage_proto_scope SET stopped=true WHERE id=:scope"),
                        {"scope": route["scope_id"]},
                    )
                    hook = captured.get("uncalled_before_commit_hook")
                    if hook is not None:
                        await hook(preparation, claim)
                    try:
                        if "uncalled_commit_fault" in captured:
                            from helpers.commit_ack_fault import lose_commit_ack

                            with lose_commit_ack(
                                factory.kw["bind"], server_commits=captured["uncalled_commit_fault"]
                            ):
                                await preparation.commit()
                        else:
                            await preparation.commit()
                    except Exception as error:
                        raise StagedCommitUnconfirmed("UNCALLED_COMMIT_UNCONFIRMED") from error
            except Prepared as prepared:
                args = prepared.kwargs
            except StagedCommitUnconfirmed:
                raise
            except Exception as error:
                raise StagedFinalizationFailed("UNCALLED_TRANSACTION_FAILED") from error
            else:
                return result
        committee, run_id = args["committee"], args["run_id"]
        assert isinstance(committee, codex_staged.StructuredStagedCodexCommittee)
        original_deadline = committee._deadline
        work_deadline = wall_anchor + timedelta(seconds=original_deadline - monotonic_anchor)
        async with factory.begin() as session:
            scope = route["scope_id"]
            lease = await session.scalar(select(JobLease).where(JobLease.job_id == claim.job_id))
            original_expiry = lease.expires_at
            run = await session.get(AgentResearchRunRecord, run_id)
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
        captured["protocol"], captured["claim"] = protocol, claim
        await protocol.install(
            scope_id=scope,
            claims=(claim,),
            now=clock(),
            increment=route["kind"] == "increment",
            independent=route["kind"] == "independent",
            work_deadline=work_deadline,
            cleanup_deadline=min(original_expiry, work_deadline + timedelta(seconds=7)),
        )
        tools = ShortTools(factory, run_id, claim, clock, committee._tools)
        committee._tools = tools
        outputs = {}
        stage_number = 0

        async def invocation(payload, **options):
            nonlocal stage_number
            stage_number += 1
            # Preserve the SDK's original per-stage allocation as well as the
            # aggregate work deadline; neither can be renewed by database waits.
            stage_deadline = wall_anchor + timedelta(
                seconds=min(original_deadline, options.get("deadline_at", original_deadline))
                - monotonic_anchor
            )
            wire = json.dumps(
                {
                    "wire": codex_sdk.codex_request_wire(payload),
                    "schema": codex_sdk.codex_output_schema(options["output_model"]),
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
            if stage_number > 1:
                async with factory.begin() as check:
                    await _lock_active_claim(check, claim=claim, now=clock())
                    chain = await stage_evidence(check, run_id)
                if len(chain) != stage_number - 1 or not usage_known(chain):
                    await protocol.stop(scope)
                    raise ProtocolRejected("PERSISTED_PREDECESSOR_MISMATCH")
            try:
                if "authorization_commit_fault" in captured:
                    from helpers.commit_ack_fault import lose_commit_ack

                    with lose_commit_ack(
                        factory.kw["bind"], server_commits=captured["authorization_commit_fault"]
                    ):
                        ticket = await protocol.authorize(
                            claim,
                            stage=stage_number,
                            now=clock(),
                            wire=wire,
                            not_after=stage_deadline,
                        )
                else:
                    ticket = await protocol.authorize(
                        claim, stage=stage_number, now=clock(), wire=wire, not_after=stage_deadline
                    )
            except (ProtocolRejected, LeaseLostError):
                raise
            except Exception as error:
                # No ticket is a send right. Neither absence nor presence of
                # an authorization proves zero provider usage after ambiguity.
                await stop_preserving_error(protocol, scope, error)
                raise StagedCommitUnconfirmed("AUTHORIZATION_COMMIT_UNCONFIRMED") from error
            old_guard = options.get("dispatch_guard")

            def guard():
                if protocol.local_stopped:
                    raise codex_sdk.CodexInvocationError(
                        "CODEX_RESEARCH_STOPPED", provider_call_started=False
                    )
                if old_guard is not None:
                    old_guard()

            options["dispatch_guard"] = guard
            observed = None
            cleanup_seen = False
            cleanup_deadline = None
            scope_stop_task = None
            work_exception = None
            try:
                # Existing fixture generates the response (and the model-wait
                # Event); it never contacts a provider. Feed it through the real
                # SDK account/config/thread/run/response/cleanup code below.
                raw, usage = await invoke_fixture(payload, **options)
                if captured.get("stop_before_sdk"):
                    await protocol.stop(scope)
                # The fixture constructor is synchronous: capture its client
                # factory and restore the shared router before any await.
                with monkeypatch.context() as sdk_fixture:
                    client, thread, response, _ = _sdk(sdk_fixture)
                    create_client = codex_sdk.AsyncCodex
                with bindings.bind(create=create_client):
                    response.final_response = raw
                    response.usage.total.input_tokens = usage.input_tokens
                    response.usage.total.output_tokens = usage.output_tokens
                    process_state = {"exit": None}
                    process = SimpleNamespace(poll=lambda: process_state["exit"])

                    async def client_close():
                        if captured.get("sdk_cleanup_failure"):
                            raise RuntimeError("controlled SDK cleanup failure")
                        captured["sdk_close_calls"] = captured.get("sdk_close_calls", 0) + 1
                        process_state["exit"] = 0

                    async def model_run(*args, **kwargs):
                        captured["sdk_run_calls"] = captured.get("sdk_run_calls", 0) + 1
                        if captured.get("sdk_original_error") is not None and not isinstance(
                            captured["sdk_original_error"], asyncio.CancelledError
                        ):
                            raise captured["sdk_original_error"]
                        hook = captured.get("sdk_phase_hook")
                        if stage_number == 1 and hook is not None:
                            await hook("model", factory, settings)
                        return response

                    client.close.side_effect = client_close
                    thread.run.side_effect = model_run
                    client._client._sync = SimpleNamespace(_proc=process)
                    setup = captured.get("sdk_client_setup")
                    if setup is not None:
                        await setup(client, thread, response, captured)
                        process = client._client._sync._proc
                    native_check = bindings.original["checked"]

                    def checked(result, state):
                        nonlocal observed
                        # Cancel the calling SDK boundary, not its child task:
                        # asyncio.shield intentionally creates another cancellation
                        # object when a child task itself is cancelled.
                        if isinstance(captured.get("sdk_original_error"), asyncio.CancelledError):
                            raise captured["sdk_original_error"]
                        result_raw, result_usage = native_check(result, state)

                        async def write_observation():
                            payload_bytes = result_raw.encode()
                            tokens = (result_usage.input_tokens, result_usage.output_tokens)
                            if captured.get("observation_mutation") == "payload":
                                payload_bytes = b"different frozen response"
                            if captured.get("observation_mutation") == "tokens":
                                tokens = (1, 1)
                            if "observation_commit_fault" in captured:
                                from helpers.commit_ack_fault import lose_commit_ack

                                with lose_commit_ack(
                                    factory.kw["bind"],
                                    server_commits=captured["observation_commit_fault"],
                                ):
                                    await protocol.observe(
                                        ticket, now=clock(), payload=payload_bytes, tokens=tokens
                                    )
                            else:
                                await protocol.observe(
                                    ticket, now=clock(), payload=payload_bytes, tokens=tokens
                                )
                            # An observation may finish diagnostically after cancel.
                            # It cannot start a new trusted write outside this call's window.
                            if (
                                cleanup_deadline is None
                                or asyncio.get_running_loop().time() >= cleanup_deadline
                            ):
                                raise ProtocolRejected("VERIFICATION_WINDOW_CLOSED")
                            await protocol.verify(
                                ticket,
                                now=clock(),
                                wire_hash=hashlib.sha256(wire).hexdigest(),
                                payload=result_raw.encode(),
                                tokens=(result_usage.input_tokens, result_usage.output_tokens),
                                not_after=wall_anchor
                                + timedelta(seconds=cleanup_deadline - monotonic_anchor),
                            )

                        observed = asyncio.create_task(write_observation())
                        return result_raw, result_usage

                    @contextmanager
                    def boundary(phase, **kwargs):
                        nonlocal work_exception
                        try:
                            with bindings.original["boundary"](phase, **kwargs):
                                yield
                        except BaseException as error:
                            if phase != "client_close":
                                # Only a failure escaping this invocation's work
                                # boundaries establishes exception ownership.
                                work_exception = error
                            raise

                    async def core(payload, **kwargs):
                        state = kwargs["invocation_state"]
                        assert state.stage_deadline == options["deadline_at"]
                        state.cleanup_deadline = min(
                            state.stage_deadline + 7,
                            original_deadline,
                            monotonic_anchor + (original_expiry - wall_anchor).total_seconds(),
                        )
                        try:
                            return await bindings.original["core"](payload, **kwargs)
                        except StagedCleanupFailure as failure:
                            # The old SDK finally may mistake a caller's ambient
                            # exception for its own primary and swallow cleanup.
                            # Restore this call's error only after that finally
                            # has unwound, through the same native safe boundary.
                            elapsed = state.phase_elapsed_ms.get("client_close")
                            try:
                                with bindings.original["boundary"]("client_close", state=state):
                                    raise failure.error
                            finally:
                                # Reclassification must not replace the actual
                                # cleanup measurement with this zero-await hop.
                                if elapsed is not None:
                                    state.phase_elapsed_ms["client_close"] = elapsed

                    async def close(client, run_task, *, deadline):
                        nonlocal cleanup_seen, cleanup_deadline, scope_stop_task
                        from nexus.agents.research_dispatch import stop_current_dispatch
                        from nexus.paper.manual_host_exit import mark_host_drain_unconfirmed

                        loop = asyncio.get_running_loop()
                        cleanup_deadline = min(deadline, loop.time() + 7)
                        primary = sys.exc_info()[1] if work_exception is not None else None
                        primary_from_work = primary is not None
                        owned = captured.setdefault("cleanup_owned_tasks", [])
                        original_close = client.close
                        sdk_close_tasks = []

                        async def tracked_close():
                            task = asyncio.current_task()
                            sdk_close_tasks.append(task)
                            task.add_done_callback(consume_owned_task)
                            try:
                                return await original_close()
                            except BaseException as error:
                                # Native close may catch this and keep waiting for
                                # child exit. Fence siblings before that wait.
                                stop_dispatch(error)
                                raise

                        client.close = tracked_close

                        def own(task, *, stops_on_failure=False):
                            def collected(completed):
                                try:
                                    completed.result()
                                except BaseException as error:
                                    if stops_on_failure:
                                        # Do not wait for the other cleanup child
                                        # before fencing sibling stages/replacements.
                                        stop_dispatch(error)
                                finally:
                                    captured.setdefault("cleanup_consumed_tasks", []).append(
                                        completed
                                    )

                            owned.append(task)
                            task.add_done_callback(collected)
                            return task

                        def stop_dispatch(error):
                            nonlocal scope_stop_task
                            # Synchronous shared stop precedes every await. Persist
                            # concurrently so child termination cannot consume its budget.
                            protocol.local_stopped = True
                            stop_current_dispatch(error)
                            if scope_stop_task is None and loop.time() < cleanup_deadline:
                                scope_stop_task = own(asyncio.create_task(protocol.stop(scope)))

                        if primary is not None:
                            stop_dispatch(primary)

                        async def finish_cleanup():
                            nonlocal cleanup_seen
                            first = None
                            native = own(
                                asyncio.create_task(
                                    bindings.original["bounded_close"](
                                        client, run_task, deadline=cleanup_deadline
                                    )
                                ),
                                stops_on_failure=True,
                            )
                            waiting = {native}
                            if observed is not None:
                                waiting.add(own(observed, stops_on_failure=True))
                            done, pending = await asyncio.wait(
                                waiting, timeout=max(0, cleanup_deadline - loop.time())
                            )
                            for task in (observed, native):
                                if task is not None and task in done:
                                    try:
                                        task.result()
                                    except BaseException as error:
                                        if first is None:
                                            first = error
                                        if task is observed:
                                            captured["phase_error"] = error
                            if pending:
                                for task in pending:
                                    task.cancel()
                                first = first or ProtocolRejected("SDK_CLEANUP_UNCONFIRMED")
                                first.add_note("CLEANUP_NATIVE_OR_OBSERVATION_WAIT_EXPIRED")
                            # Exceptions in poll/router may bypass native helper's
                            # tail. Keep only the original child/task identities.
                            if first is not None:
                                mark_host_drain_unconfirmed()
                                try:
                                    if process.poll() is None:
                                        process.kill()
                                except BaseException:
                                    try:
                                        process.kill()
                                    except BaseException:
                                        pass
                            tracked = set(sdk_close_tasks)
                            if run_task is not None:
                                tracked.add(run_task)
                            for task in tracked:
                                if task not in owned:
                                    own(task)
                            if tracked:
                                _, unfinished = await asyncio.wait(
                                    tracked, timeout=max(0, cleanup_deadline - loop.time())
                                )
                                if unfinished:
                                    for task in unfinished:
                                        task.cancel()
                                    first = first or ProtocolRejected("SDK_CLEANUP_UNCONFIRMED")
                                    mark_host_drain_unconfirmed()
                            try:
                                exited = process.poll() is not None
                            except BaseException as error:
                                exited = False
                                first = first or error
                            if not exited:
                                first = first or ProtocolRejected("SDK_EXIT_UNCONFIRMED")
                            native_ok = (
                                native in done
                                and not native.cancelled()
                                and native.exception() is None
                            )
                            if native_ok and exited and loop.time() < cleanup_deadline:

                                async def publish_cleanup():
                                    hook = captured.get("sdk_phase_hook")
                                    if stage_number == 1 and hook is not None:
                                        await hook("cleanup", factory, settings)
                                    if loop.time() >= cleanup_deadline:
                                        raise ProtocolRejected("CLEANUP_WINDOW_CLOSED")
                                    await protocol.cleanup(
                                        ticket,
                                        now=clock(),
                                        confirmed=True,
                                        not_after=wall_anchor
                                        + timedelta(seconds=cleanup_deadline - monotonic_anchor),
                                    )
                                    if captured.get("stop_after_cleanup"):
                                        await protocol.stop(scope)

                                receipt = own(
                                    asyncio.create_task(publish_cleanup()), stops_on_failure=True
                                )
                                done_receipt, _ = await asyncio.wait(
                                    (receipt,), timeout=max(0, cleanup_deadline - loop.time())
                                )
                                if receipt in done_receipt:
                                    try:
                                        receipt.result()
                                        cleanup_seen = True
                                    except BaseException as error:
                                        first = first or error
                                else:
                                    receipt.cancel()
                                    first = first or ProtocolRejected("SDK_CLEANUP_UNCONFIRMED")
                                    first.add_note("CLEANUP_RECEIPT_WAIT_EXPIRED")
                            if not cleanup_seen:
                                first = first or ProtocolRejected("SDK_CLEANUP_UNCONFIRMED")
                            if first is not None:
                                mark_host_drain_unconfirmed()
                            return first

                        cleanup_task = own(asyncio.create_task(finish_cleanup()))
                        while not cleanup_task.done():
                            try:
                                await asyncio.shield(cleanup_task)
                            except asyncio.CancelledError as error:
                                stop_dispatch(error)
                                if primary is None:
                                    primary = error
                            except BaseException:
                                break
                        try:
                            failure = cleanup_task.result()
                        except BaseException as error:
                            mark_host_drain_unconfirmed()
                            failure = error
                        if primary is not None:
                            if failure is not None:
                                primary.add_note("SDK_CLEANUP_UNCONFIRMED")
                            if not primary_from_work:
                                raise primary
                            # The SDK is already unwinding its own work exception.
                            # Returning lets that original error propagate without
                            # presenting it to SDK finally as a cleanup failure.
                            return
                        if failure is not None:
                            raise StagedCleanupFailure(failure) from failure

                    with bindings.bind(
                        checked=checked, core=core, bounded_close=close, boundary=boundary
                    ):
                        result = await codex_sdk.invoke_codex(payload, **options)
                    assert thread.run.await_count == 1
                    assert cleanup_seen
                    return result
            except BaseException as error:
                captured["invocation_error"] = error
                await stop_preserving_error(
                    protocol, scope, error, deadline=cleanup_deadline, task=scope_stop_task
                )
                raise

        with bindings.bind(stage=invocation):
            for role in ROLE_ORDER:
                try:
                    outputs[role] = await committee.analyze(
                        role, request, refs, committee._prompts[role]
                    )
                except Exception as error:
                    captured["staged_error"] = error
                    if captured.get("sdk_phase_hook") is not None:
                        from nexus.agents.research_dispatch import stop_current_dispatch

                        stop_current_dispatch(error)
                        raise StagedExecutionInterrupted("STAGED_EXECUTION_INTERRUPTED") from error
                    break
        assert committee._deadline == original_deadline
        before_final = captured.get("before_final_hook")
        if before_final is not None:
            await before_final(factory, settings, captured)
        frozen = FrozenCommittee(committee, outputs, request, refs)
        try:
            async with factory() as final:
                await final.begin()
                member = await protocol._member(final, claim)
                rows = await stage_evidence(final, run_id)
                matched = bool(rows) and all(row["verified"] is True for row in rows)
                known = usage_known(rows)
                complete = (
                    known
                    and len(rows) == 3
                    and all(row["confirmed"] and row["valid"] for row in rows)
                )
                publishing_stopped = member["stopped"] or protocol.local_stopped
                if publishing_stopped:
                    frozen.outputs = {}
                    frozen.failure_reason = (
                        "model_research_stopped" if known else "model_usage_uncertain"
                    )

                elif not matched or (len(outputs) == len(ROLE_ORDER) and not complete):
                    # Never publish or spend from a chain that differs from the SDK
                    # observation. Keep only previously confirmed accounting facts.
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
                    # Every authorized invocation is durable and known; the failed
                    # later authorization is not an additional unknown SDK call.
                    frozen.failure_reason = "model_research_stopped"
                from nexus.agents.responses_budget import UsageSnapshot

                frozen.usage = UsageSnapshot(
                    input_tokens=sum(row["input_tokens"] or 0 for row in rows if row["eligible"])
                    if matched
                    else 0,
                    output_tokens=sum(row["output_tokens"] or 0 for row in rows if row["eligible"])
                    if matched
                    else 0,
                    provider_calls=len(rows),
                )

                async def finalize(target):
                    result = await execute_research_run(target, **{**args, "committee": frozen})
                    from nexus.jobs.repository import JobCompletion
                    from nexus.worker import _agent_result

                    mapped = _agent_result(result.status)
                    if result.status != "completed":
                        await target.execute(
                            text("UPDATE stage_proto_scope SET stopped=true WHERE id=:id"),
                            {"id": scope},
                        )
                    # One atomic terminal fact is shared by the proof and the
                    # native Job/Attempt; a second clock read changes identity.
                    closed_at = clock()
                    await target.execute(
                        text(
                            "INSERT INTO "
                            "stage_proto_terminal(run_id,known,chain_hash,closed_at,"
                            "observed_stages,evidence) "
                            "VALUES(:run,:known,:hash,:now,:observed,CAST(:evidence AS jsonb))"
                        ),
                        {
                            "run": run_id,
                            "known": known,
                            "hash": evidence_digest(rows),
                            "now": closed_at,
                            "observed": [
                                r["stage"] for r in rows if r["response_hash"] is not None
                            ],
                            "evidence": json.dumps(rows),
                        },
                    )
                    return JobCompletion(
                        outcome=mapped.outcome,
                        result_code=mapped.result_code,
                        error_code=mapped.error_code,
                        finished_at=closed_at,
                    )

                result = await native_finalize(
                    final, claim=claim, checked_at=clock(), execute=finalize
                )
                assert (
                    await final.scalar(select(JobLease).where(JobLease.job_id == claim.job_id))
                ).expires_at == original_expiry
                try:
                    if "terminal_commit_fault" in captured:
                        from helpers.commit_ack_fault import lose_commit_ack

                        with lose_commit_ack(
                            factory.kw["bind"], server_commits=captured["terminal_commit_fault"]
                        ):
                            await protocol._commit(final, member["scope_id"])
                    else:
                        await protocol._commit(final, member["scope_id"])
                except Exception as error:
                    raise StagedCommitUnconfirmed("TERMINAL_COMMIT_UNCONFIRMED") from error
        except StagedCommitUnconfirmed:
            raise
        except BaseException as error:
            # The entire final transaction has exited, not only its COMMIT.
            # Preserve stage facts and the original error; no legacy _fail may
            # manufacture a Job terminal without the research proof.
            await stop_preserving_error(protocol, scope, error)
            if isinstance(error, Exception):
                raise StagedFinalizationFailed("TERMINAL_TRANSACTION_FAILED") from error
            raise
        assert "get_price_features" in tools.reads
        return result

    async def observed_adapter(*args, **kwargs):
        try:
            return await adapter(*args, **kwargs)
        except BaseException as error:
            captured["adapter_error"] = error
            raise

    captured.pop("adapter_error", None)
    native_claim = bindings.original["claim"]

    async def staged_claim(session, **kwargs):
        # Transaction-local capability is set only by this selected executor.
        # DB guard prevents legacy Worker from claiming registered new Jobs.
        await session.execute(text("SET LOCAL nexus.stage_protocol = 'staged-experiment-v1'"))
        return await native_claim(session, **kwargs)

    with bindings.bind(finalize=observed_adapter, claim=staged_claim):
        boundary_error = None
        try:
            result = await native_worker(settings, **worker_options)
        except BaseException as error:
            result, boundary_error = None, error
        post = captured.get("post_worker_hook")
        if post is not None:
            await post(factory, settings, captured)
        if boundary_error is not None:
            if isinstance(boundary_error, StagedExecutionInterrupted):
                assert boundary_error.__cause__ is captured["staged_error"]
                raise boundary_error.__cause__
            raise boundary_error
        if captured.get("adapter_error") is not None:
            raise captured["adapter_error"]
        if captured.get("sdk_phase_hook") is not None and captured.get("staged_error") is not None:
            raise captured.get("phase_error", captured["staged_error"])
        return result


async def require_staged_monitor_account_available(session, *, account_id, now):
    if await session.scalar(text("SELECT to_regclass('stage_proto_scope')")) is not None:
        occupied = await session.scalar(
            text(
                "SELECT EXISTS(SELECT 1 FROM stage_proto_scope s "
                "JOIN research_monitor_increments i ON i.id=s.increment_id "
                "WHERE i.account_id=:account AND stage_proto_inflight(s.id)>0)"
            ),
            {"account": account_id},
        )
        if occupied:
            raise ProtocolRejected("MONITOR_ACCOUNT_BUSY_OR_UNKNOWN")
    await native_monitor_available(session, account_id=account_id, now=now)
