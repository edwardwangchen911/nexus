"""Native facts for one already-authorized HTTP stage, not a dispatch entry point.

The comparison runner must select its registered transport, claim and authorize
once before calling this adapter. The frozen comparison route selects it. It owns the HTTP
client/tasks, publishes terminal usage during stream cleanup, and never turns
local closure into remote cancellation or zero usage.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any
from uuid import UUID

import httpx
from openai.types.responses import Response
from pydantic import BaseModel

from nexus.agents.chatgpt_grant import Grant
from nexus.agents.codex_sdk import (
    _MAX_OUTPUT_BYTES,
    BASE_INSTRUCTIONS,
    DEVELOPER_INSTRUCTIONS,
    MODEL_ID,
    CodexInvocationError,
    codex_output_schema,
    codex_request_wire,
)
from nexus.agents.codex_versions import FULL_INPUT_CAPACITY_MAX_WIRE_BYTES
from nexus.agents.comparison_inputs import assert_outbound_safe
from nexus.agents.http_failure import HttpFailurePhase, http_failure_facts
from nexus.agents.research_stages.protocol import ProtocolRejected, Ticket
from nexus.agents.research_stages.support import consume_owned_task, stop_preserving_error
from nexus.agents.responses_budget import UsageSnapshot
from nexus.agents.responses_request import HttpRequestObservation, send_response_stage
from nexus.agents.responses_stream import ResponsesStreamFacts
from nexus.agents.responses_transport import normalize_response

if TYPE_CHECKING:
    from nexus.agents.platform_wire import PlatformStageWire
    from nexus.ops.platform_credentials import PlatformCredential


def http_stage_wire(
    payload: Mapping[str, object], *, output_model: type[BaseModel], forbidden: tuple[str, ...]
) -> bytes:
    """Same business input/schema as SDK; scan the entire additional HTTP envelope."""
    common = codex_request_wire(payload)
    if len(common.encode()) > FULL_INPUT_CAPACITY_MAX_WIRE_BYTES:
        raise CodexInvocationError("CODEX_INPUT_TOO_LARGE")
    body = {
        "model": MODEL_ID,
        "instructions": BASE_INSTRUCTIONS,
        "input": [
            {"role": "developer", "content": DEVELOPER_INSTRUCTIONS},
            {"role": "user", "content": common},
        ],
        "reasoning": {"effort": "low"},
        "tools": [],
        "stream": True,
        "store": False,
        "text": {
            "format": {
                "type": "json_schema",
                "name": "research_stage",
                "strict": True,
                "schema": codex_output_schema(output_model),
            }
        },
    }
    assert_outbound_safe(body, forbidden)
    wire = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode()
    assert_outbound_safe(wire.decode(), forbidden)
    return wire


class HttpStageFailure(CodexInvocationError):
    """Safe outward code; original transport/task identities stay locally owned."""

    def __init__(
        self,
        *,
        observation: HttpRequestObservation,
        usage: UsageSnapshot | None,
        cause: BaseException,
        owned: tuple[asyncio.Task[Any], ...],
    ) -> None:
        super().__init__(
            "CODEX_RESPONSE_INCOMPLETE",
            usage=usage,
            provider_call_started=observation.owner.send_started,
        )
        self.observation = observation
        self.primary = cause
        self.owned = owned
        self.proof_error: BaseException | None = None
        self.http_failure_facts = http_failure_facts(observation, cause)


async def invoke_http_stage(
    *,
    protocol: Any,
    scope: UUID,
    ticket: Ticket,
    wire: bytes,
    output_model: type[BaseModel],
    grant: Grant | None = None,
    clock: Callable[[], datetime],
    wall_anchor: datetime,
    monotonic_anchor: float,
    work_deadline: float,
    cleanup_deadline: float,
    client: httpx.AsyncClient,
    dispatch_guard: Callable[[], None] | None = None,
    platform_credential: PlatformCredential | None = None,
    platform_wire: PlatformStageWire | None = None,
    platform_pre_send: Callable[[], Awaitable[PlatformCredential]] | None = None,
) -> tuple[str, UsageSnapshot]:
    """Consume one fresh native ticket; no retries, refreshed grants or deadlines.

    The caller supplies the existing stage/parent/lease minimum as the absolute
    cleanup bound. Terminal publication runs concurrently with owned transport
    closure, so a slow close cannot erase already-received trustworthy usage.
    Platform calls require the frozen wire and an async qualification callback;
    the returned credential must retain the initial identity binding. Native
    authorization owns the allocation binding, not this transport adapter.
    """
    loop = asyncio.get_running_loop()
    publication: asyncio.Task[None] | None = None
    owned: list[asyncio.Task[Any]] = []
    raw: str | None = None
    counts: tuple[int, int] | None = None
    verified = False
    output_error: BaseException | None = None
    proof_error: BaseException | None = None
    proof_phase: HttpFailurePhase = "publication"
    stop_task: asyncio.Task[Any] | None = None
    cleanup_deadline = min(cleanup_deadline, work_deadline + 7)
    cut_off = wall_anchor + timedelta(seconds=cleanup_deadline - monotonic_anchor)
    request = httpx.Request(
        "POST",
        "https://api.openai.com/v1/responses",
        content=wire,
        headers={"content-type": "application/json"},
    )
    platform_requested = any(
        value is not None for value in (platform_credential, platform_wire, platform_pre_send)
    )
    checked_credential: PlatformCredential | None = None
    credential_binding: str | None = None

    def frozen_platform() -> PlatformCredential:
        # Keep the old Grant path independent of Platform configuration loading.
        from nexus.agents.platform_wire import PlatformStageWire
        from nexus.ops.platform_credentials import PLATFORM_ENDPOINT, PlatformCredential

        if (
            grant is not None
            or not isinstance(platform_credential, PlatformCredential)
            or not isinstance(platform_wire, PlatformStageWire)
            or platform_pre_send is None
        ):
            raise ProtocolRejected("PLATFORM_HTTP_AUTH_MISSING_OR_MIXED")
        if (
            platform_wire.endpoint != PLATFORM_ENDPOINT
            or platform_credential.endpoint != PLATFORM_ENDPOINT
            or platform_wire.credential_kind != "platform_project_key"
            or platform_wire.wire != wire
            or type(platform_wire.input_units) is not int
            or platform_wire.input_units != len(wire) + 8192
            or type(platform_wire.input_upper_bound) is not int
            or not 0 < platform_wire.input_upper_bound <= 272000
            or platform_wire.stage not in ("research", "critique", "synthesis")
            or type(ticket.stage) is not int
            or {"research": 1, "critique": 2, "synthesis": 3}.get(platform_wire.stage)
            != ticket.stage
        ):
            raise ProtocolRejected("PLATFORM_HTTP_WIRE_BINDING_INVALID")
        platform_wire.verify_wire(wire)
        binding = hashlib.sha256(
            json.dumps(
                dict(
                    allocation_hash=platform_wire.allocation_hash,
                    stage=platform_wire.stage,
                    body_sha256=platform_wire.body_sha256,
                ),
                sort_keys=True,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        if binding != platform_wire.binding_hash:
            raise ProtocolRejected("PLATFORM_HTTP_WIRE_BINDING_INVALID")
        platform_credential.request_headers()
        return platform_credential

    async def qualify_platform() -> None:
        nonlocal checked_credential, credential_binding
        from nexus.ops.platform_credentials import PlatformCredential

        initial = frozen_platform()
        if protocol.local_stopped:
            raise ProtocolRejected("PLATFORM_HTTP_STOPPED")
        credential_binding = initial.binding_hash
        assert platform_pre_send is not None
        checked_credential = await platform_pre_send()
        if (
            not isinstance(checked_credential, PlatformCredential)
            or checked_credential.binding_hash != credential_binding
        ):
            raise ProtocolRejected("PLATFORM_HTTP_CREDENTIAL_CHANGED")
        checked_credential.request_headers()

    def before_send() -> None:
        if platform_requested:
            initial = frozen_platform()
            if dispatch_guard is not None:
                dispatch_guard()
            if (
                protocol.local_stopped
                or checked_credential is None
                or initial.binding_hash != credential_binding
                or checked_credential.binding_hash != credential_binding
            ):
                raise ProtocolRejected("PLATFORM_HTTP_STOPPED_OR_CREDENTIAL_CHANGED")
            if checked_credential.key_scope_only and any(
                name in request.headers or name in client.headers
                for name in ("openai-organization", "openai-project")
            ):
                raise ProtocolRejected("PLATFORM_HTTP_SCOPE_CHANGED")
            request.headers.update(checked_credential.request_headers())
            return
        if protocol.local_stopped or grant is None or not grant.identity_verified:
            raise ProtocolRejected("STOPPED_OR_GRANT_UNVERIFIED")
        if dispatch_guard is not None:
            dispatch_guard()
        request.headers["authorization"] = grant.bearer(clock())

    def fence(error: BaseException) -> None:
        nonlocal stop_task
        from nexus.agents.research_dispatch import stop_current_dispatch

        protocol.local_stopped = True
        stop_current_dispatch(error)
        if stop_task is None and loop.time() < cleanup_deadline:
            stop_task = asyncio.create_task(protocol.stop(scope))
            stop_task.add_done_callback(consume_owned_task)
            owned.append(stop_task)

    def terminal(value: dict[str, Any], facts: ResponsesStreamFacts) -> None:
        nonlocal publication, raw, counts, output_error
        counts = facts.usage
        # This callback follows the stream parser's immutable identity/status/
        # arithmetic checks. Failed/incomplete usage is independent of output validity.
        payload = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
        try:
            response = Response.model_validate(value)
            normalized = normalize_response(response)
            if response.model != MODEL_ID or any(
                item.get("type") not in {"message", "reasoning"} for item in normalized.output
            ):
                raise CodexInvocationError("CODEX_UNEXPECTED_TOOL")
            raw = normalized.output_text
            if raw is None or len(raw.encode()) > _MAX_OUTPUT_BYTES:
                raise CodexInvocationError("CODEX_RESPONSE_INCOMPLETE")
            output_model.model_validate_json(raw)
        except BaseException as error:
            output_error = error

        async def publish() -> None:
            nonlocal verified
            await protocol.observe(
                ticket, now=clock(), payload=payload, tokens=counts, valid=output_error is None
            )
            if loop.time() >= cleanup_deadline:
                raise ProtocolRejected("VERIFICATION_WINDOW_CLOSED")
            verified = await protocol.verify(
                ticket,
                now=clock(),
                wire_hash=hashlib.sha256(wire).hexdigest(),
                payload=payload,
                tokens=counts,
                not_after=cut_off,
            )
            if not verified:
                raise ProtocolRejected("OBSERVATION_UNVERIFIED")

        publication = asyncio.create_task(publish())

        def published(task: asyncio.Task[None]) -> None:
            try:
                task.result()
            except BaseException as error:
                fence(error)

        publication.add_done_callback(published)
        owned.append(publication)

    # Own the transport coordinator too. The first cancellation stops its work;
    # repeated cancellation cannot cancel its original bounded cleanup again.
    started = False

    async def transport() -> HttpRequestObservation:
        nonlocal started
        started = True
        return await send_response_stage(
            client,
            request,
            work_deadline=work_deadline,
            cleanup_deadline=cleanup_deadline,
            pre_send=qualify_platform if platform_requested else None,
            before_send=before_send,
            on_terminal=terminal,
        )

    transport_task = asyncio.create_task(transport())
    transport_task.add_done_callback(consume_owned_task)
    owned.append(transport_task)
    interrupted: asyncio.CancelledError | None = None
    while not transport_task.done():
        try:
            await asyncio.shield(transport_task)
        except asyncio.CancelledError as error:
            fence(error)
            if interrupted is None:
                interrupted = error
                if started:
                    transport_task.cancel(*error.args)
                else:
                    # Let ownership initialize before delivering the stop.
                    loop.call_soon(transport_task.cancel, *error.args)
    observation = transport_task.result()
    stream_cleanup_error = (
        observation.stage.cleanup.error if observation.stage is not None else None
    )
    primary = (
        interrupted
        or observation.error
        or output_error
        or stream_cleanup_error
        or observation.cleanup.error
        or observation.cleanup.interrupted
    )
    if primary is not None:
        fence(primary)

    async def finish() -> None:
        nonlocal proof_error, proof_phase
        try:
            if publication is not None:
                done, _ = await asyncio.wait(
                    {publication}, timeout=max(0, cleanup_deadline - loop.time())
                )
                if publication not in done:
                    publication.cancel()
                    raise ProtocolRejected("OBSERVATION_UNCONFIRMED")
                publication.result()
        except BaseException as error:
            proof_error = error
            fence(error)
        try:
            if proof_error is None:
                proof_phase = "cleanup"
            if loop.time() >= cleanup_deadline:
                raise ProtocolRejected("CLEANUP_WINDOW_CLOSED")
            await protocol.cleanup(
                ticket, now=clock(), confirmed=observation.cleanup.confirmed, not_after=cut_off
            )
        except BaseException as error:
            proof_error = proof_error or error
            fence(error)

    finish_task = asyncio.create_task(finish())
    finish_task.add_done_callback(consume_owned_task)
    owned.append(finish_task)
    # A second cancellation must not discard ownership of native proof writes.
    while not finish_task.done() and loop.time() < cleanup_deadline:
        try:
            await asyncio.wait({finish_task}, timeout=max(0, cleanup_deadline - loop.time()))
        except asyncio.CancelledError as error:
            primary = primary or error
            fence(error)
    if not finish_task.done():
        finish_task.cancel()
        proof_error = proof_error or ProtocolRejected("CLEANUP_UNCONFIRMED")
    usage = UsageSnapshot(*counts, provider_calls=1) if verified and counts is not None else None
    if not observation.cleanup.confirmed or any(
        not task.done() for task in owned if task is not stop_task
    ):
        from nexus.paper.manual_host_exit import mark_host_drain_unconfirmed

        mark_host_drain_unconfirmed()
        proof_error = proof_error or ProtocolRejected("HTTP_CLEANUP_UNCONFIRMED")
    cause = primary or proof_error
    if cause is None and (raw is None or usage is None):
        cause = ProtocolRejected("HTTP_RESULT_UNCONFIRMED")
    if cause is not None:
        failure = HttpStageFailure(
            observation=observation, usage=usage, cause=cause, owned=tuple(owned)
        )
        fence(failure)
        await stop_preserving_error(
            protocol, scope, failure, deadline=cleanup_deadline, task=stop_task
        )
        failure.owned = tuple(owned)
        failure.proof_error = proof_error
        failure.http_failure_facts = http_failure_facts(
            observation,
            cause,
            output_error=output_error,
            proof_error=proof_error,
            proof_phase=proof_phase,
        )
        from nexus.agents.budget import ResearchAccountingBusy

        if isinstance(cause, asyncio.CancelledError | ResearchAccountingBusy):
            cause.http_stage_failure = failure  # type: ignore[union-attr]
            raise cause
        raise failure from None
    assert raw is not None and usage is not None
    return raw, usage
