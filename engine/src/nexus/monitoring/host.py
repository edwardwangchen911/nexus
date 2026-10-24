"""One bounded background price scan per observed UTC five-minute bucket."""

from asyncio import CancelledError, Task, create_task, shield, timeout
from collections.abc import Awaitable, Callable
from datetime import datetime
from math import isfinite

from nexus.domain.time import require_utc


class PriceMonitorCoordinator:
    """Coordinate scans on one host event loop, without blocking its heartbeat.

    A new bucket is consumed even when a prior scan is still busy. There is no
    catch-up, same-bucket retry, or wall-clock rollback replay. The callback must
    cooperate with asyncio cancellation, including cleaning up its own resources.
    """

    def __init__(
        self,
        scan: Callable[[], Awaitable[object]],
        *,
        timeout_seconds: float = 8.0,
    ) -> None:
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, int | float)
            or not isfinite(timeout_seconds)
            or timeout_seconds <= 0
        ):
            raise ValueError("MONITOR_SCAN_TIMEOUT_INVALID")
        self._scan = scan
        self._timeout_seconds = float(timeout_seconds)
        self._task: Task[None] | None = None
        self._closed = False
        self._last_bucket: datetime | None = None
        self._last_diagnostic: str | None = None

    @property
    def busy(self) -> bool:
        return self._task is not None and not self._task.done()

    @property
    def last_bucket(self) -> datetime | None:
        return self._last_bucket

    @property
    def last_diagnostic(self) -> str | None:
        return self._last_diagnostic

    def kick(self, now: datetime) -> bool:
        """Synchronously start at most one scan, returning whether one started."""
        if self._closed:
            return False
        utc = require_utc(now)
        bucket = utc.replace(minute=(utc.minute // 5) * 5, second=0, microsecond=0)
        if self._last_bucket is not None and bucket <= self._last_bucket:
            return False
        self._last_bucket = bucket
        if self.busy:
            return False
        self._last_diagnostic = None
        self._task = create_task(self._run_scan(), name="nexus-price-monitor")
        return True

    async def _run_scan(self) -> None:
        try:
            async with timeout(self._timeout_seconds):
                await self._scan()
        except TimeoutError:
            self._last_diagnostic = "MONITOR_SCAN_TIMEOUT"
        except CancelledError:
            self._last_diagnostic = "MONITOR_SCAN_CANCELLED"
        except Exception:
            # Never retain or expose provider/DB exception text or callback data.
            self._last_diagnostic = "MONITOR_SCAN_FAILED"

    async def close(self) -> None:
        """Stop admissions and collect the owned scan, including on cancellation."""
        self._closed = True
        task = self._task
        if task is None:
            return
        try:
            await shield(task)
        except CancelledError:
            task.cancel()
            try:
                await task
            except CancelledError:
                # A task cancelled before its coroutine starts cannot catch it.
                pass
            raise
