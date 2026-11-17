"""New-send permission only; never used by observation, usage verification or cleanup."""

from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


async def durable_batch(session: AsyncSession, run_id: UUID) -> UUID | None:
    present = await session.scalar(
        text("SELECT to_regclass('public.sec_event_research_bindings') IS NOT NULL")
    )
    if not present:
        return None
    value = await session.scalar(
        text(
            "SELECT m.batch_id FROM public.research_monitor_increments m "
            "JOIN public.research_monitor_batches b ON b.id=m.batch_id "
            "WHERE m.run_id=:run AND b.routing_version='durable_research_v2'"
        ),
        {"run": run_id},
    )
    if value is not None and not isinstance(value, UUID):
        raise ValueError("SEC_RESEARCH_PERMISSION_INVALID")
    return value


async def require_new_send(session: AsyncSession, run_id: UUID) -> bool:
    batch_id = await durable_batch(session, run_id)
    if batch_id is None:
        return False
    await session.execute(
        text("SELECT public.sec_research_require_current_batch(:batch)"), {"batch": batch_id}
    )
    return True


class SecRequestFence:
    """Hold request-send locks until headers, before observation or budget verification."""

    def __init__(self, factory: Any, run_id: UUID):
        self.factory, self.run_id = factory, run_id
        self.session: AsyncSession | None = None

    async def acquire(self, request: Any) -> None:
        session = self.factory()
        self.session = session
        try:
            await session.begin()
            active = await require_new_send(session, self.run_id)
            if not active:
                await self.release()
                return
            original = request.extensions.get("trace")

            async def traced(name: str, info: dict[str, Any]) -> None:
                # Headers complete is an irreversible send, never a delivery or usage proof.
                try:
                    if original is not None:
                        await original(name, info)
                finally:
                    if name in {
                        "http11.send_request_headers.complete",
                        "http2.send_request_headers.complete",
                        "http11.send_request_headers.failed",
                        "http2.send_request_headers.failed",
                    }:
                        await self.release()

            request.extensions["trace"] = traced
        except BaseException:
            await self.release()
            raise

    async def release(self) -> None:
        session, self.session = self.session, None
        if session is not None:
            try:
                await session.rollback()
            finally:
                await session.close()
