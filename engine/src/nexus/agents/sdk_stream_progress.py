"""Bounded metadata observer around the pinned SDK's existing turn reader.

Never consume a second stream or retain notification objects. Keep only typed
terminal tags, bounded allowlisted error codes and token counts, never prose
or account fields. Do not alter SDK collection, cancellation, retry or cleanup.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from time import perf_counter

from openai_codex.generated.v2_all import (
    CodexErrorInfoValue,
    ThreadTokenUsageUpdatedNotification,
    TurnCompletedNotification,
)
from openai_codex.models import Notification

from nexus.agents.committee import SdkFailureFacts, SdkStreamProgress
from nexus.agents.responses_budget import UsageSnapshot
from nexus.agents.sdk_error_details import provider_error_code, typed_http_status

_PROGRESS_METHODS = frozenset(
    {
        "item/agentMessage/delta",
        "item/reasoning/textDelta",
        "item/reasoning/summaryTextDelta",
    }
)


@dataclass
class SdkStreamObserver:
    measured_usage: UsageSnapshot | None = None
    _reported_usage: UsageSnapshot | None = None
    failure_facts: SdkFailureFacts | None = None
    available: bool = False
    notifications: int = 0
    progress_notifications: int = 0
    reasoning_notifications: int = 0
    answer_notifications: int = 0
    first_notification_ms: int | None = None
    last_notification_ms: int | None = None
    terminal_notification_seen: bool = False
    first_reasoning_ms: int | None = None
    first_answer_ms: int | None = None
    terminal_notification_ms: int | None = None
    _started: float = field(default_factory=lambda: perf_counter())
    _active: bool = False

    def observe(self, event: object, turn_id: str | None = None) -> None:
        if not self._active:
            return
        elapsed = max(0, int((perf_counter() - self._started) * 1000))
        self.notifications = min(1000000, self.notifications + 1)
        if self.first_notification_ms is None:
            self.first_notification_ms = elapsed
        self.last_notification_ms = elapsed
        method = getattr(event, "method", None)
        if type(method) is str and method in _PROGRESS_METHODS:
            self.progress_notifications = min(1000000, self.progress_notifications + 1)
            if method == "item/agentMessage/delta":
                self.answer_notifications = min(1000000, self.answer_notifications + 1)
                if self.first_answer_ms is None:
                    self.first_answer_ms = elapsed
            else:
                self.reasoning_notifications = min(1000000, self.reasoning_notifications + 1)
                if self.first_reasoning_ms is None:
                    self.first_reasoning_ms = elapsed
        if method == "thread/tokenUsage/updated" and isinstance(event, Notification):
            payload = event.payload
            if (
                isinstance(payload, ThreadTokenUsageUpdatedNotification)
                and payload.turn_id == turn_id
            ):
                usage = payload.token_usage.total
                if (
                    type(usage.input_tokens) is int
                    and type(usage.output_tokens) is int
                    and min(usage.input_tokens, usage.output_tokens) >= 0
                ):
                    self._reported_usage = UsageSnapshot(
                        input_tokens=usage.input_tokens,
                        output_tokens=usage.output_tokens,
                        provider_calls=1,
                    )
        if method == "turn/completed":
            terminal = event.payload if isinstance(event, Notification) else None
            if isinstance(terminal, TurnCompletedNotification) and terminal.turn.id == turn_id:
                turn = terminal.turn
                self.measured_usage = self._reported_usage
                reason = None
                http_status = None
                if turn.error is not None and turn.error.codex_error_info is not None:
                    info = turn.error.codex_error_info.root
                    http_status = typed_http_status(info)
                    if isinstance(info, CodexErrorInfoValue):
                        reason = info.value
                    else:
                        # Only the typed union discriminator is retained, never its body.
                        for key in (
                            "httpConnectionFailed",
                            "responseStreamConnectionFailed",
                            "responseStreamDisconnected",
                            "responseTooManyFailedAttempts",
                            "activeTurnNotSteerable",
                        ):
                            if key in type(info).model_fields:
                                reason = key
                                break
                        if reason is None:
                            for field in type(info).model_fields.values():
                                if field.alias in (
                                    "httpConnectionFailed",
                                    "responseStreamConnectionFailed",
                                    "responseStreamDisconnected",
                                    "responseTooManyFailedAttempts",
                                    "activeTurnNotSteerable",
                                ):
                                    reason = field.alias
                                    break
                self.failure_facts = SdkFailureFacts.model_validate(
                    {
                        "terminal_status": turn.status.value,
                        "provider_reason": reason,
                        "http_status_code": http_status,
                        "provider_error_code": provider_error_code(turn.error.message)
                        if turn.error is not None
                        else None,
                    }
                )
            self.terminal_notification_seen = True
            if self.terminal_notification_ms is None:
                self.terminal_notification_ms = elapsed

    def diagnostic_timestamps(self) -> dict[str, int | None]:
        """Private timing receipt only; the persisted business snapshot is unchanged."""
        return {
            "first_reasoning_ms": self.first_reasoning_ms,
            "first_answer_ms": self.first_answer_ms,
            "terminal_notification_ms": self.terminal_notification_ms,
        }

    def snapshot(self) -> SdkStreamProgress:
        return SdkStreamProgress(
            available=self.available,
            notifications=self.notifications,
            progress_notifications=self.progress_notifications,
            first_notification_ms=self.first_notification_ms,
            last_notification_ms=self.last_notification_ms,
            terminal_notification_seen=self.terminal_notification_seen,
            reasoning_notifications=self.reasoning_notifications if self.available else None,
            answer_notifications=self.answer_notifications if self.available else None,
        )


@contextmanager
def observe_sdk_notifications(client: object, observer: SdkStreamObserver | None) -> Iterator[None]:
    transport = getattr(client, "_client", None)
    reader = getattr(transport, "next_turn_notification", None)
    if observer is None or transport is None or not callable(reader):
        yield
        return
    # Per-client adapter only. The pinned SDK has one turn on this client;
    # its normal collector still owns routing, payload validation and usage.
    own_reader = vars(transport).get("next_turn_notification")

    async def observed_reader(turn_id: str) -> object:
        event = await reader(turn_id)
        observer.observe(event, turn_id)
        return event

    transport.next_turn_notification = observed_reader
    observer.available = observer._active = True
    observer._started = perf_counter()
    try:
        yield
    finally:
        observer._active = False
        if own_reader is None:
            del transport.next_turn_notification
        else:
            transport.next_turn_notification = own_reader
