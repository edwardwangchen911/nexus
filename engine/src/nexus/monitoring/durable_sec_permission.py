"""New-send permission only; never used by observation, usage verification or cleanup."""

from asyncio import get_running_loop
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.monitoring.durable_sec_send_inputs import verify_send_inputs


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


async def has_sec_dependencies(session: AsyncSession, run_id: UUID) -> bool:
    present = await session.scalar(
        text("SELECT to_regclass('public.sec_monitor_rules') IS NOT NULL")
    )
    if not present:
        return False
    result = await session.scalar(
        text("SELECT public.sec_research_run_has_dependencies(:run)"), {"run": run_id}
    )
    if type(result) is not bool:
        raise ValueError("SEC_RESEARCH_PERMISSION_INVALID")
    return result


async def require_new_send(session: AsyncSession, run_id: UUID) -> bool:
    if not await has_sec_dependencies(session, run_id):
        return False
    try:
        await session.execute(
            text("SELECT public.sec_research_require_send_run(:run)"), {"run": run_id}
        )
        await verify_send_inputs(session, run_id)
    except DBAPIError as error:
        code = getattr(error.orig, "sqlstate", None)
        if code in {"23514", "55P03"}:
            raise ValueError(
                "SEC_RESEARCH_PERMISSION_BUSY"
                if code == "55P03"
                else "SEC_RESEARCH_PERMISSION_INVALID"
            ) from None
        raise
    return True


class SecRequestFence:
    """Hold request-send locks until headers, before observation or budget verification."""

    def __init__(self, factory: Any, run_id: UUID | None, *, account_id: UUID | None = None):
        self.factory, self.run_id = factory, run_id
        self.account_id = account_id
        self.session: AsyncSession | None = None
        self.work_deadline: float | None = None

    async def _validate(self, session: AsyncSession) -> bool:
        from nexus.monitoring.original_occupancy import require_for_account_send, require_for_send

        if self.run_id is None:
            return await require_for_account_send(session, self.account_id, fenced_transport=True)
        occupancy = await require_for_send(
            session, self.run_id, fenced_transport=True, hold_original=True
        )
        sec = await require_new_send(session, self.run_id)
        return sec or occupancy

    async def acquire(self, request: Any) -> None:
        from nexus.agents.research_stages.routes import stage_send_account
        from nexus.paper.host_probe_state import check_host_send_state, require_host_send

        session = self.factory()
        self.session = session
        try:
            await session.begin()
            active = await self._validate(session)
            account = (
                self.account_id
                if self.run_id is None
                else await stage_send_account(session, self.run_id)
            )
            if account is not None:
                await require_host_send(session, account)
            if not active and account is None:
                await self.release()
                return
            original = request.extensions.get("trace")
            request_method = getattr(request, "method", None)
            proxy_connect_headers = False

            async def traced(name: str, info: dict[str, Any]) -> None:
                nonlocal proxy_connect_headers
                # Run other hooks before the final permission/deadline boundary.
                try:
                    if original is not None:
                        await original(name, info)
                finally:
                    if not proxy_connect_headers and name in {
                        "http11.send_request_headers.complete",
                        "http2.send_request_headers.complete",
                        "http11.send_request_headers.failed",
                        "http2.send_request_headers.failed",
                    }:
                        await self.release()
                if name in {
                    "http11.send_request_headers.started",
                    "http2.send_request_headers.started",
                }:
                    # httpcore shares this trace with the proxy's CONNECT.
                    # Only the application request leaves the send fence.
                    proxy_connect_headers = (
                        request_method not in ("CONNECT", b"CONNECT")
                        and getattr(info.get("request"), "method", None) == b"CONNECT"
                    )
                    if self.session is None:
                        raise ValueError("SEC_RESEARCH_PERMISSION_INVALID")
                    self._require_deadline()
                    await self._validate(self.session)
                    if account is not None:
                        await check_host_send_state(self.session, account)
                    self._require_deadline()

            request.extensions["trace"] = traced
        except BaseException:
            await self.release()
            raise

    def _require_deadline(self) -> None:
        if self.work_deadline is not None and get_running_loop().time() >= self.work_deadline:
            raise TimeoutError("RESPONSES_WORK_DEADLINE")

    async def release(self) -> None:
        session, self.session = self.session, None
        if session is not None:
            try:
                await session.rollback()
            finally:
                await session.close()
