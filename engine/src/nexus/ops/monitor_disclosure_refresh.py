"""Once per current UTC bucket SEC index refresh; no research admission or replay."""

import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import httpx
from sqlalchemy import func, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.domain.time import require_utc
from nexus.monitoring.batches import disclosure_source_current
from nexus.monitoring.disclosure_observations import (
    _bound_proposal,
    _capture,
    read_disclosure_observation,
)
from nexus.monitoring.storage import lock_security
from nexus.ops.agent_paper_once import _validate_local_database
from nexus.ops.monitor_batch_prepare import _archive_root
from nexus.ops.monitor_content_local import _contact
from nexus.ops.sec_index_local import collect_index
from nexus.paper.agent_cycle_authority import load_agent_paper_service_calendar_authority
from nexus.paper.candidate_batch import validate_agent_paper_candidate_batch
from nexus.paper.target_service import lock_portfolio_account
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD, SCHEMA_ADMISSION_LOCK_KEY
from nexus.persistence.models.monitoring import MonitorPlan
from nexus.persistence.models.paper import PaperAccountRecord
from nexus.persistence.models.runtime import AuditEvent
from nexus.persistence.session import session_factory
from nexus.research.sec_exhibit_storage import configured_exhibit_store
from nexus.settings import EngineSettings

CLAIM = "research.monitor.disclosure_refresh_claimed"
RESULT = "research.monitor.disclosure_refresh_result"
SCHEMA = "monitor_disclosure_refresh_v1"
FEED = "sec_disclosure_index"
TOTAL_TIMEOUT_SECONDS = 90


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(follow_redirects=False, timeout=30)


def _bucket(value: datetime) -> datetime:
    return require_utc(value).replace(minute=value.minute // 5 * 5, second=0, microsecond=0)


def _result(status: str, **extra: object) -> dict[str, object]:
    return {
        "status": status,
        "provider_requests": 0,
        "model_requests": 0,
        "job_created": False,
        "execution_locked": True,
        **extra,
    }


async def _scope(
    session: AsyncSession,
    settings: EngineSettings,
    account_id: UUID,
    calendar_id: UUID,
    scheduled: datetime,
) -> tuple[datetime, tuple[UUID, ...]]:
    await session.execute(text("SET LOCAL statement_timeout='5s'"))
    await session.execute(text("SET LOCAL lock_timeout='2s'"))
    await session.execute(
        text("SELECT pg_advisory_xact_lock_shared(hashtextextended(:key,0))"),
        {"key": SCHEMA_ADMISSION_LOCK_KEY},
    )
    if (
        await session.scalar(text("SELECT version_num FROM alembic_version"))
        != CURRENT_MIGRATION_HEAD
    ):
        raise ValueError("MONITOR_SCHEMA_UNAVAILABLE")
    await lock_portfolio_account(session, account_id)
    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime) or _bucket(now) != scheduled:
        raise ValueError("MONITOR_REFRESH_BUCKET_UNAVAILABLE")
    account = await session.get(PaperAccountRecord, account_id)
    if account is None or account.capability != "simulation_write_internal":
        raise ValueError("MONITOR_ACCOUNT_UNAVAILABLE")
    calendar = await load_agent_paper_service_calendar_authority(
        session, now=now, expected_calendar_authority_id=calendar_id
    )
    if not calendar.market_open_at <= now < calendar.market_close_at:
        return now, ()
    candidate, _ = await validate_agent_paper_candidate_batch(
        session,
        batch_id=settings.agent_paper_candidate_batch_id,
        account_id=account_id,
        as_of=now,
    )
    if len(candidate.instrument_ids) > 5:
        raise ValueError("MONITOR_SCOPE_EXCEEDED")
    return now, tuple(sorted(candidate.instrument_ids, key=str))


async def _claim(
    session: AsyncSession,
    settings: EngineSettings,
    *,
    account_id: UUID,
    calendar_id: UUID,
    instrument_id: UUID,
    scheduled: datetime,
    repo_root: Path,
    contact: str | None,
) -> dict[str, object] | None:
    now, instruments = await _scope(session, settings, account_id, calendar_id, scheduled)
    if instrument_id not in instruments:
        return None
    await lock_security(session, account_id=account_id, instrument_id=instrument_id)
    plan = await session.scalar(
        select(MonitorPlan)
        .where(
            MonitorPlan.account_id == account_id,
            MonitorPlan.instrument_id == instrument_id,
            MonitorPlan.kind == "disclosure",
        )
        .order_by(MonitorPlan.generation.desc())
        .limit(1)
    )
    if plan is None or not plan.effective_at <= now < plan.valid_until:
        return None
    if not await disclosure_source_current(session, plan, now):
        return None
    proposal = await _bound_proposal(session, plan, now)
    store = configured_exhibit_store(session)
    baseline_row, baseline = await _capture(
        session, store, snapshot_id=proposal.baseline_snapshot_id, as_of=proposal.baseline_as_of
    )
    if (baseline_row.revision, baseline.instrument_id, baseline.issuer_cik) != (
        proposal.baseline_hash,
        instrument_id,
        proposal.issuer_cik,
    ):
        raise ValueError("SEC_BASELINE_INVALID")
    observation = await read_disclosure_observation(session, plan=plan, now=now)
    if (
        observation.current_snapshot_id is not None
        and observation.reason
        in {None, "SEC_INDEX_COVERAGE_UNAVAILABLE", "SEC_INDEX_EVENT_CONFLICT"}
        and observation.received_at is not None
        and timedelta(0) <= now - observation.received_at <= timedelta(seconds=300)
    ):
        return None
    previous = await session.scalar(
        select(AuditEvent.id)
        .where(
            AuditEvent.action == CLAIM,
            AuditEvent.payload["account_id"].astext == str(account_id),
            AuditEvent.payload["instrument_id"].astext == str(instrument_id),
            AuditEvent.payload["feed"].astext == FEED,
            AuditEvent.payload["scheduled_at"].astext == scheduled.isoformat(),
        )
        .limit(1)
    )
    if previous is not None:
        return {"status": "already_claimed"}
    if contact is None:
        return {"status": "blocked", "reason": "SEC_MONITOR_CONTACT_REQUIRED"}
    # Object readback may take time; eligibility must still hold when the claim commits.
    claimed_at = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(claimed_at, datetime) or _bucket(claimed_at) != scheduled:
        raise ValueError("MONITOR_REFRESH_BUCKET_UNAVAILABLE")
    if not plan.effective_at <= claimed_at < plan.valid_until:
        return None
    operation = uuid4()
    archive = _archive_root(repo_root) / f"index-{baseline.issuer_cik}-{operation}"
    claim: dict[str, object] = {
        "operation_id": str(operation),
        "account_id": str(account_id),
        "instrument_id": str(instrument_id),
        "plan_id": str(plan.id),
        "baseline_snapshot_id": str(baseline_row.id),
        "baseline_hash": baseline_row.revision,
        "identity_snapshot_id": str(baseline.identity_snapshot_id),
        "issuer_cik": baseline.issuer_cik,
        "feed": FEED,
        "scheduled_at": scheduled.isoformat(),
        "archive_dir": str(archive),
    }
    await append_audit_event(
        session,
        actor_id="research-monitor",
        action=CLAIM,
        schema_version=SCHEMA,
        payload=claim,
        occurred_at=claimed_at,
    )
    return claim


def _request_count(archive: Path) -> int | None:
    path = archive / "result.json"
    try:
        if path.is_symlink() or path.stat().st_size > 65536:
            return None
        result = json.loads(path.read_bytes())
        if not isinstance(result, dict):
            return None
        count = result.get("requests")
        return count if type(count) is int and 0 <= count <= 2 else None
    except (OSError, ValueError, TypeError):
        return None


async def refresh_disclosure_observations(
    settings: EngineSettings,
    *,
    account_id: UUID,
    calendar_authority_id: UUID,
    scheduled_at: datetime,
    environment: dict[str, str],
    repo_root: Path,
) -> dict[str, object]:
    """Refresh exact installed issuers once; failures never authorize research or execution."""
    if (
        not settings.agent_paper_cycle_enabled
        or settings.agent_paper_formal_model_config_id is None
    ):
        return _result("disabled")
    if account_id != settings.agent_paper_account_id:
        return _result("blocked", reason="MONITOR_SCOPE_INVALID")
    scheduled_at = require_utc(scheduled_at)
    if _bucket(scheduled_at) != scheduled_at:
        return _result("blocked", reason="MONITOR_REFRESH_BUCKET_UNAVAILABLE")
    _validate_local_database(settings.database_url)
    # Match sec_index_local's local-store gate before any baseline object reads.
    if urlsplit(settings.object_store_endpoint or "").hostname not in {
        "127.0.0.1",
        "localhost",
        "::1",
    }:
        return _result("blocked", reason="SEC_INDEX_LOCAL_STORE_REQUIRED")
    # A missing contact is relevant only when an actual stale issuer needs collection.
    contact = None
    if environment.get("SEC_CONTACT"):
        try:
            _contact(environment["SEC_CONTACT"])
            contact = environment["SEC_CONTACT"]
        except ValueError:
            pass
    factory = session_factory(settings)
    items: list[dict[str, object]] = []
    requests: int | None = 0
    in_flight = False
    try:
        async with asyncio.timeout(TOTAL_TIMEOUT_SECONDS):
            async with factory() as session, session.begin():
                _, instruments = await _scope(
                    session, settings, account_id, calendar_authority_id, scheduled_at
                )
            if not instruments:
                return _result("market_closed")
            for instrument in instruments:
                try:
                    async with factory() as session, session.begin():
                        claim = await _claim(
                            session,
                            settings,
                            account_id=account_id,
                            calendar_id=calendar_authority_id,
                            instrument_id=instrument,
                            scheduled=scheduled_at,
                            repo_root=repo_root,
                            contact=contact,
                        )
                except ValueError:
                    items.append({"status": "unknown", "reason": "SEC_BASELINE_SCOPE_UNAVAILABLE"})
                    continue
                if claim is None:
                    continue
                if "status" in claim:
                    items.append(claim)
                    continue
                archive = Path(str(claim["archive_dir"]))
                in_flight = True
                try:
                    async with _client() as client:
                        collected = await collect_index(
                            settings,
                            identity_snapshot_id=UUID(str(claim["identity_snapshot_id"])),
                            archive_dir=archive,
                            contact=contact or "",
                            client=client,
                        )
                    async with factory() as session, session.begin():
                        now = await session.scalar(select(func.clock_timestamp()))
                        if not isinstance(now, datetime):
                            raise ValueError("MONITOR_CLOCK_UNAVAILABLE")
                        row, index = await _capture(
                            session,
                            configured_exhibit_store(session),
                            snapshot_id=UUID(str(collected["snapshot_id"])),
                            as_of=now,
                        )
                        if (index.instrument_id, index.issuer_cik, index.identity_snapshot_id) != (
                            instrument,
                            claim["issuer_cik"],
                            UUID(str(claim["identity_snapshot_id"])),
                        ) or not timedelta(0) <= now - index.received_at <= timedelta(seconds=300):
                            raise ValueError("SEC_INDEX_IDENTITY_INVALID")
                        item = {
                            "status": "refreshed",
                            "snapshot_id": str(row.id),
                            "snapshot_hash": row.revision,
                            "provider_requests": collected["provider_requests"],
                        }
                        await append_audit_event(
                            session,
                            actor_id="research-monitor",
                            action=RESULT,
                            schema_version=SCHEMA,
                            occurred_at=now,
                            payload={**claim, **item},
                        )
                except SQLAlchemyError:
                    # A database commit outcome is unknown; never fabricate a terminal result.
                    raise
                except Exception:
                    count = _request_count(archive)
                    item = {
                        "status": "failed",
                        "reason": "SEC_INDEX_REFRESH_FAILED",
                        "provider_requests": count,
                    }
                    async with factory() as session, session.begin():
                        now = await session.scalar(select(func.clock_timestamp()))
                        if not isinstance(now, datetime):
                            raise ValueError("MONITOR_CLOCK_UNAVAILABLE")
                        await append_audit_event(
                            session,
                            actor_id="research-monitor",
                            action=RESULT,
                            schema_version=SCHEMA,
                            occurred_at=now,
                            payload={**claim, **item},
                        )
                observed_count = item["provider_requests"]
                requests = (
                    requests + observed_count
                    if isinstance(requests, int) and isinstance(observed_count, int)
                    else None
                )
                items.append(item)
                in_flight = False
    except TimeoutError:
        return _result(
            "unknown", reason="SEC_INDEX_REFRESH_TIMEOUT", provider_requests=None, items=items
        )
    except Exception as error:
        missed = (
            isinstance(error, ValueError) and str(error) == "MONITOR_REFRESH_BUCKET_UNAVAILABLE"
        )
        return _result(
            "missed_bucket" if missed else "unknown",
            reason="MONITOR_REFRESH_BUCKET_UNAVAILABLE"
            if missed
            else "SEC_INDEX_REFRESH_UNAVAILABLE",
            provider_requests=None if in_flight else requests,
            items=items,
        )
    finally:
        await factory.kw["bind"].dispose()
    statuses = {item["status"] for item in items}
    if "already_claimed" in statuses or "unknown" in statuses:
        status = "unknown"
    elif statuses & {"failed", "blocked"}:
        status = "partial" if "refreshed" in statuses else "blocked"
    else:
        status = "refreshed" if "refreshed" in statuses else "unchanged"
    return _result(status, provider_requests=requests, items=items)
