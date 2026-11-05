"""Read-only admission for a new service process, never recovery of old work."""

import asyncio
import re
from datetime import UTC, datetime, timedelta
from time import monotonic_ns

from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError

from nexus.agents.platform_native import native_runtime_matches
from nexus.agents.platform_profile import sol_business_model_matches
from nexus.agents.research_usage import require_research_accounting_clear
from nexus.market.current_identity import identity_parse_scope
from nexus.ops.agent_paper_once import _validate_local_database
from nexus.paper.candidate_batch import load_agent_paper_candidate_batch
from nexus.paper.manual_entry import manual_host_scope
from nexus.paper.manual_host_exit import valid_host_exit
from nexus.paper.manual_request import require_manual_account_available
from nexus.paper.read_service import PaperReadService
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.agents import AgentModelConfigRecord
from nexus.persistence.models.runtime import WorkerHeartbeat
from nexus.persistence.session import session_factory
from nexus.settings import AgentRuntimeMode, EngineSettings
from nexus.workbench.evidence_read_scope import evidence_read_scope

_KNOWN_CODES = frozenset(
    {
        "AGENT_PAPER_RECOVERY_SCHEMA_MISMATCH",
        "AGENT_PAPER_RECOVERY_SCOPE_INVALID",
        "AGENT_PAPER_RECOVERY_HOST_PRESENT",
        "MANUAL_ACCOUNT_BUSY_OR_UNKNOWN",
        "MANUAL_PREPARATION_ACK_BLOCKED",
        "MANUAL_PREPARATION_RECOVERY_BLOCKED",
        "MONITOR_ACCOUNT_BUSY_OR_UNKNOWN",
        "RESEARCH_ACCOUNTING_UNSETTLED",
        "INITIAL_PROJECTION_OCCUPIED_OR_UNKNOWN",
        "INITIAL_PROJECTION_OCCUPANCY_AUTHORITY_CONFLICT",
        "SINGLE_SOL_SERVICE_SCOPE_MISMATCH",
        "SINGLE_SOL_FROZEN_MODEL_CONFLICT",
        "SINGLE_SOL_BUDGET_SCOPE_INVALID",
    }
)


def require_recovery_process_quiescence() -> None:
    """Reuse exact SDK process identity checks; never stop or acknowledge a process."""
    from nexus.paper.history_seal import require_research_processes_quiescent

    require_research_processes_quiescent()


def _recovery_model_matches(
    model: AgentModelConfigRecord | None, now: datetime, *, sol: bool
) -> bool:
    if model is None or model.created_at > now:
        return False
    if sol:
        return (
            sol_business_model_matches(model)
            and model.pricing_available_at <= now < model.pricing_expires_at
        )
    return model.platform_profile is None


async def inspect_recovery_readiness(settings: EngineSettings) -> dict[str, object]:
    """Check cross-session occupancy without creating a heartbeat or invoking I/O producers.

    This is a current observation, not a permit: activation must run it again
    after obtaining the engine lifetime lock, before its first heartbeat.
    """
    checked_at = datetime.now(UTC)
    factory = None
    code = None
    diagnostic: dict[str, object] = {}
    budget_scope: dict[str, object] | None = None
    deadline = None
    stage, started = "configuration", monotonic_ns()
    try:
        _validate_local_database(settings.database_url)
        sol = native_runtime_matches(settings)
        if (
            not settings.agent_paper_cycle_enabled
            or (settings.agent_runtime_mode is not AgentRuntimeMode.CODEX_SDK and not sol)
            or settings.agent_paper_account_id is None
            or settings.agent_paper_candidate_batch_id is None
            or settings.agent_paper_formal_model_config_id is None
            or re.fullmatch(r"[0-9a-f]{40}", settings.release_revision or "") is None
        ):
            raise ValueError("AGENT_PAPER_RECOVERY_SCOPE_INVALID")
        stage, started = "database_session", monotonic_ns()
        factory = session_factory(settings)
        deadline = asyncio.timeout(10)
        async with deadline:
            async with factory() as session, session.begin():
                await session.execute(text("SET TRANSACTION READ ONLY"))
                await session.execute(text("SET LOCAL statement_timeout='5s'"))
                await session.execute(text("SET LOCAL lock_timeout='1s'"))
                stage, started = "database_clock", monotonic_ns()
                now = await session.scalar(select(func.clock_timestamp()))
                if not isinstance(now, datetime) or now.utcoffset() is None:
                    raise ValueError("AGENT_PAPER_RECOVERY_UNAVAILABLE")
                checked_at = now.astimezone(UTC)
                stage, started = "schema", monotonic_ns()
                if (
                    await session.scalar(text("SELECT version_num FROM alembic_version"))
                    != CURRENT_MIGRATION_HEAD
                ):
                    raise ValueError("AGENT_PAPER_RECOVERY_SCHEMA_MISMATCH")
                stage, started = "host_heartbeat", monotonic_ns()
                live = await session.scalar(
                    select(WorkerHeartbeat.id)
                    .where(
                        WorkerHeartbeat.queue == "agent_paper",
                        WorkerHeartbeat.seen_at >= now - timedelta(seconds=30),
                        ~valid_host_exit(),
                    )
                    .limit(1)
                )
                if live is not None:
                    raise ValueError("AGENT_PAPER_RECOVERY_HOST_PRESENT")
                stage, started = "account_scope", monotonic_ns()
                account, _ = await PaperReadService(session, now).account()
                batch = await load_agent_paper_candidate_batch(
                    session, batch_id=settings.agent_paper_candidate_batch_id
                )
                model = await session.get(
                    AgentModelConfigRecord, settings.agent_paper_formal_model_config_id
                )
                if (
                    account.id != settings.agent_paper_account_id
                    or batch is None
                    or batch.account_id != account.id
                    or batch.created_at > now
                    or not _recovery_model_matches(model, now, sol=sol)
                ):
                    raise ValueError("AGENT_PAPER_RECOVERY_SCOPE_INVALID")
                stage, started = "daily_budget_scope", monotonic_ns()
                from nexus.paper.single_sol_compat import inspect_service_budget_scope

                budget_scope = await inspect_service_budget_scope(session, settings, now=now)
                # Existing acceptance/seal and consumed-lease proofs remain authoritative.
                # Never replace them with a blanket historical cost/lease predicate.
                stage, started = "account_occupancy", monotonic_ns()
                with identity_parse_scope(), evidence_read_scope(session):
                    await require_manual_account_available(
                        session,
                        account_id=account.id,
                        now=now,
                        allow_scheduled_completion=True,
                        read_only=True,
                    )
                # Experimental runs have no business request to enumerate above.
                # Known-but-unsettled costs still prohibit resuming new work.
                stage, started = "research_accounting", monotonic_ns()
                await require_research_accounting_clear(session)
                stage, started = "database_close", monotonic_ns()
    except Exception as error:
        code = str(error) if isinstance(error, ValueError) else None
        if code not in _KNOWN_CODES:
            code = "AGENT_PAPER_RECOVERY_UNAVAILABLE"
        safe_code = code
        if isinstance(error, TimeoutError):
            safe_code = (
                "RECOVERY_CHECK_DEADLINE_EXCEEDED"
                if deadline is not None and deadline.expired()
                else "RECOVERY_DEPENDENCY_TIMEOUT"
            )
        elif isinstance(error, DBAPIError):
            # SQLSTATE does not identify who cancelled a query.
            match getattr(error.orig, "sqlstate", None):
                case "57014":
                    safe_code = "RECOVERY_QUERY_CANCELLED"
                case "55P03":
                    safe_code = "RECOVERY_DB_LOCK_UNAVAILABLE"
        diagnostic = {
            "failed_stage": stage,
            "safe_code": safe_code,
            "stage_elapsed_ms": (monotonic_ns() - started) // 1_000_000,
        }
    finally:
        if factory is not None:
            await factory.kw["bind"].dispose()
    return {
        **diagnostic,
        "status": "ready" if code is None else "blocked",
        "blocking_codes": [] if code is None else [code],
        "checked_at": checked_at.isoformat(),
        "execution_locked": True,
        "external_requests": 0,
        "migration_head": CURRENT_MIGRATION_HEAD if code is None else None,
        "scope_hash": manual_host_scope(settings) if code is None else None,
        "budget_scope": budget_scope,
    }
