"""Bounded metadata observer around the pinned SDK's existing turn reader.

Never consume a second stream, inspect payloads, retain notification objects,
or alter SDK collection, cancellation, retry or cleanup behavior.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from time import perf_counter

from nexus.agents.committee import SdkStreamProgress

_PROGRESS_METHODS = frozenset(
    {
        "item/agentMessage/delta",
        "item/reasoning/textDelta",
        "item/reasoning/summaryTextDelta",
    }
)


@dataclass
class SdkStreamObserver:
    available: bool = False
    notifications: int = 0
    progress_notifications: int = 0
    first_notification_ms: int | None = None
    last_notification_ms: int | None = None
    terminal_notification_seen: bool = False
    _started: float = field(default_factory=lambda: perf_counter())
    _active: bool = False

    def observe(self, event: object) -> None:
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
        if method == "turn/completed":
            self.terminal_notification_seen = True

    def snapshot(self) -> SdkStreamProgress:
        return SdkStreamProgress(
            available=self.available,
            notifications=self.notifications,
            progress_notifications=self.progress_notifications,
            first_notification_ms=self.first_notification_ms,
            last_notification_ms=self.last_notification_ms,
            terminal_notification_seen=self.terminal_notification_seen,
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
        observer.observe(event)
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
