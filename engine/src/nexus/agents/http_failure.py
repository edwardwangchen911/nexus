"""Allowlisted HTTP failure facts, independent of SDK RPC facts and usage authority."""

import asyncio
from typing import TYPE_CHECKING, Literal, cast, get_args

import httpx
from pydantic import BaseModel, ConfigDict, Field, model_validator

from nexus.agents.responses_stream import StreamProtocolError

if TYPE_CHECKING:
    from nexus.agents.responses_request import HttpRequestObservation

type HttpFailurePhase = Literal[
    "pre_send", "send", "headers", "stream", "response_validation", "publication", "cleanup"
]
type HttpFailureKind = Literal[
    "local_guard",
    "http_status",
    "stream_protocol",
    "timeout",
    "connection",
    "transport_protocol",
    "cancelled",
    "output_validation",
    "persistence",
    "cleanup",
    "unexpected",
]
type ProviderErrorCode = Literal[
    "invalid_api_key",
    "insufficient_quota",
    "rate_limit_exceeded",
    "model_not_found",
    "invalid_json_schema",
    "invalid_request_error",
    "unsupported_parameter",
    "invalid_parameter",
    "context_length_exceeded",
    "permission_denied",
    "account_deactivated",
    "billing_hard_limit_reached",
    "server_error",
    "internal_server_error",
]
type HttpProtocolCode = Literal[
    "RESPONSES_STREAM_UNAVAILABLE",
    "RESPONSES_EVENT_INVALID",
    "RESPONSES_TERMINAL_MISSING",
    "RESPONSES_EVENT_AFTER_TERMINAL",
    "RESPONSES_EVENT_TOO_LARGE",
    "RESPONSES_STREAM_TRUNCATED",
    "RESPONSES_IDENTITY_INVALID",
    "RESPONSES_IDENTITY_CHANGED",
    "RESPONSES_DUPLICATE_TERMINAL",
    "RESPONSES_STATUS_MISMATCH",
    "RESPONSES_USAGE_UNKNOWN",
    "RESPONSES_WORK_DEADLINE",
    "RESPONSES_DEADLINE_INVALID",
    "RESPONSES_REQUEST_ABANDONED",
]


class HttpResponseRejected(StreamProtocolError):
    """No raw response, provider message, headers or exception text in the outward error."""

    def __init__(self) -> None:
        # Preserve the public protocol code; typed facts carry the HTTP distinction.
        super().__init__("RESPONSES_STREAM_UNAVAILABLE")
        self.provider_error_code: ProviderErrorCode | None = None
        self.body_state: Literal["not_read", "parsed", "too_large", "malformed", "unavailable"] = (
            "not_read"
        )


class HttpFailureFacts(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", strict=True, hide_input_in_errors=True)
    phase: HttpFailurePhase
    error_kind: HttpFailureKind
    send_started: bool
    headers_received: bool
    http_status: int | None = Field(default=None, ge=100, le=599)
    provider_error_code: ProviderErrorCode | None = None
    error_body_state: (
        Literal["not_read", "parsed", "too_large", "malformed", "unavailable"] | None
    ) = None
    protocol_code: HttpProtocolCode | None = None
    terminal_seen: bool | None = None
    cleanup_confirmed: bool = Field(
        description="Transport closure only, not native cleanup proof or usage."
    )
    cleanup_error_kind: HttpFailureKind | None = None
    proof_error_phase: HttpFailurePhase | None = None

    @model_validator(mode="after")
    def consistent_observation(self) -> "HttpFailureFacts":
        if (
            self.headers_received != (self.http_status is not None)
            or self.headers_received
            and not self.send_started
            or self.provider_error_code is not None
            and self.error_body_state != "parsed"
            or self.phase == "pre_send"
            and self.send_started
        ):
            raise ValueError("HTTP_FAILURE_FACTS_INVALID")
        return self


def _kind(error: BaseException, phase: HttpFailurePhase) -> HttpFailureKind:
    if isinstance(error, asyncio.CancelledError):
        return "cancelled"
    if isinstance(error, HttpResponseRejected):
        return "http_status"
    if isinstance(error, TimeoutError | httpx.TimeoutException):
        return "timeout"
    if isinstance(error, httpx.ConnectError):
        return "connection"
    if isinstance(error, httpx.ProtocolError):
        return "transport_protocol"
    if isinstance(error, StreamProtocolError):
        return "stream_protocol"
    phases: dict[HttpFailurePhase, HttpFailureKind] = {
        "pre_send": "local_guard",
        "response_validation": "output_validation",
        "publication": "persistence",
        "cleanup": "cleanup",
    }
    return phases.get(phase, "unexpected")


def http_failure_facts(
    observation: "HttpRequestObservation",
    cause: BaseException,
    *,
    output_error: BaseException | None = None,
    proof_error: BaseException | None = None,
    proof_phase: HttpFailurePhase = "publication",
) -> HttpFailureFacts:
    owner, stage = observation.owner, observation.stage
    response = owner.response
    cleanup_error = (
        (stage.cleanup.error or stage.cleanup.interrupted if stage else None)
        or observation.cleanup.error
        or observation.cleanup.interrupted
    )
    if cause is output_error:
        phase: HttpFailurePhase = "response_validation"
    elif cause is proof_error:
        phase = proof_phase
    elif cause is cleanup_error:
        phase = "cleanup"
    elif not owner.send_started:
        phase = "pre_send"
    elif response is None:
        phase = "send"
    elif (
        stage is None
        or response.status_code != 200
        or (isinstance(cause, StreamProtocolError) and str(cause) == "RESPONSES_STREAM_UNAVAILABLE")
    ):
        phase = "headers"
    else:
        phase = "stream"
    rejection = stage.error if stage and isinstance(stage.error, HttpResponseRejected) else None
    code = (
        str(cause)
        if isinstance(cause, StreamProtocolError | TimeoutError)
        and not isinstance(cause, HttpResponseRejected)
        else None
    )
    return HttpFailureFacts(
        phase=phase,
        error_kind=_kind(cause, phase),
        send_started=owner.send_started,
        headers_received=response is not None,
        http_status=None if response is None else response.status_code,
        provider_error_code=None if rejection is None else rejection.provider_error_code,
        error_body_state=None if rejection is None else rejection.body_state,
        protocol_code=cast(HttpProtocolCode, code)
        if code in get_args(HttpProtocolCode.__value__)
        else None,
        terminal_seen=None if stage is None else stage.facts.terminal_status is not None,
        cleanup_confirmed=observation.cleanup.confirmed,
        cleanup_error_kind=None if cleanup_error is None else _kind(cleanup_error, "cleanup"),
        proof_error_phase=None if proof_error is None else proof_phase,
    )
