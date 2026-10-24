"""One durable, bounded Schwab L1 observation per current five-minute source tick."""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.domain.time import require_utc
from nexus.monitoring.contracts import PriceReviewCondition
from nexus.monitoring.evaluation import evaluate_price
from nexus.monitoring.observations import read_latest_price
from nexus.monitoring.reconciliation import plan_covered
from nexus.monitoring.storage import lock_security
from nexus.paper.candidate_batch import CandidateInstrumentIdentity
from nexus.paper.target_service import lock_portfolio_account
from nexus.persistence.models.monitoring import MonitorPlan
from nexus.persistence.models.runtime import AuditEvent
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings

PRICE_REFRESH_SECONDS = 90
_ACTION = "research.monitor.price_refresh"
_SCHEMA = "monitor_price_refresh_v1"
_FEED = "schwab.l1"


@dataclass(frozen=True)
class PriceScope:
    account_id: UUID
    calendar_id: UUID
    candidate_id: UUID
    universe_id: UUID
    model_id: UUID
    model_hash: str
    plans: tuple[tuple[str, str], ...]
    identities: tuple[CandidateInstrumentIdentity, ...]
    needed: tuple[CandidateInstrumentIdentity, ...]
    observations: tuple[dict[str, object], ...]

    def binding(self) -> dict[str, object]:
        return dict(
            account_id=str(self.account_id),
            calendar_authority_id=str(self.calendar_id),
            candidate_batch_id=str(self.candidate_id),
            universe_snapshot_id=str(self.universe_id),
            model_config_id=str(self.model_id),
            model_hash=self.model_hash,
            plans=[list(row) for row in self.plans],
            identities=[row.model_dump(mode="json") for row in self.identities],
            feed=_FEED,
        )


class _Skip(ValueError):
    pass


def _bucket(value: datetime) -> datetime:
    value = require_utc(value)
    return value.replace(minute=value.minute // 5 * 5, second=0, microsecond=0)


def _result(
    status: str, reason: str, *, requests: int | None = 0, **facts: object
) -> dict[str, object]:
    return dict(
        status=status,
        reason=reason,
        provider_requests=requests,
        model_requests=0,
        job_created=False,
        execution_locked=True,
        **facts,
    )


async def _clock(session: AsyncSession) -> datetime:
    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime):
        raise ValueError("MONITOR_CLOCK_UNAVAILABLE")
    return require_utc(now)


async def _locks(session: AsyncSession, account_id: UUID) -> None:
    from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD, SCHEMA_ADMISSION_LOCK_KEY

    await session.execute(text("SET LOCAL statement_timeout='5s'"))
    await session.execute(text("SET LOCAL lock_timeout='2s'"))
    await session.execute(
        text("SELECT pg_advisory_xact_lock_shared(hashtextextended(:key, 0))"),
        {"key": SCHEMA_ADMISSION_LOCK_KEY},
    )
    if (
        await session.scalar(text("SELECT version_num FROM alembic_version"))
        != CURRENT_MIGRATION_HEAD
    ):
        raise ValueError("MONITOR_SCHEMA_UNAVAILABLE")
    await lock_portfolio_account(session, account_id)


async def _select_scope(
    session: AsyncSession,
    settings: EngineSettings,
    *,
    account_id: UUID,
    calendar_authority_id: UUID,
    now: datetime,
) -> PriceScope:
    from nexus.agents.codex_formal_stage import require_formal_preparation_model_profile
    from nexus.paper.agent_cycle_authority import load_agent_paper_service_calendar_authority
    from nexus.paper.candidate_batch import validate_agent_paper_candidate_batch

    calendar = await load_agent_paper_service_calendar_authority(
        session, now=now, expected_calendar_authority_id=calendar_authority_id
    )
    if not calendar.market_open_at <= now < calendar.market_close_at:
        raise _Skip("MONITOR_MARKET_CLOSED")
    model = await require_formal_preparation_model_profile(
        session, model_config_id=settings.agent_paper_formal_model_config_id, now=now
    )
    batch, identities = await validate_agent_paper_candidate_batch(
        session, batch_id=settings.agent_paper_candidate_batch_id, account_id=account_id, as_of=now
    )
    if not 1 <= len(identities) <= 5:
        raise ValueError("MONITOR_SCOPE_EXCEEDED")
    for identity in sorted(identities, key=lambda item: item.instrument_id.int):
        await lock_security(session, account_id=account_id, instrument_id=identity.instrument_id)
    plans = tuple(
        await session.scalars(
            select(MonitorPlan)
            .where(
                MonitorPlan.kind == "price",
                MonitorPlan.account_id == account_id,
                MonitorPlan.instrument_id.in_(batch.instrument_ids),
            )
            .distinct(MonitorPlan.instrument_id)
            .order_by(MonitorPlan.instrument_id, MonitorPlan.generation.desc())
        )
    )
    by_id = {row.instrument_id: row for row in identities}
    eligible, needed, bindings, observations = [], [], [], []
    for plan in plans:
        if not plan.created_at <= now or not plan.effective_at <= now < plan.valid_until:
            continue
        if not await plan_covered(session, plan=plan, now=now):
            continue
        condition = PriceReviewCondition.model_validate(plan.condition)
        if condition.semantic_hash != plan.condition_hash:
            raise ValueError("MONITOR_PLAN_INVALID")
        identity = by_id[plan.instrument_id]
        eligible.append(identity)
        bindings.append((str(plan.id), plan.condition_hash))
        observation = await read_latest_price(session, instrument_id=plan.instrument_id, now=now)
        if (
            evaluate_price(condition, observation, instrument_id=plan.instrument_id, now=now).result
            == "unknown"
        ):
            needed.append(identity)
        else:
            assert observation is not None
            observations.append(observation.model_dump(mode="json"))
    if not eligible:
        raise _Skip("MONITOR_NO_ELIGIBLE_PRICE_PLAN")
    return PriceScope(
        account_id,
        calendar_authority_id,
        batch.batch_id,
        batch.universe_snapshot_id,
        model.id,
        model.config_hash,
        tuple(bindings),
        tuple(eligible),
        tuple(needed),
        tuple(observations),
    )


def _configured(settings: EngineSettings, environment: dict[str, str], repo_root: Path) -> None:
    from nexus.ops.agent_paper_loop import _require_explicit_schwab_config
    from nexus.ops.schwab_market_stream import _require_local_services
    from nexus.providers.market.schwab_oauth import load_schwab_app_config

    _require_explicit_schwab_config(environment)
    _require_local_services(settings)
    if any(
        not value
        for value in (
            settings.object_store_region,
            settings.object_store_bucket,
            settings.object_store_prefix,
            settings.object_store_access_key_id,
            settings.object_store_secret_access_key,
        )
    ):
        raise ValueError("MONITOR_PRICE_ARCHIVE_CONFIG_UNAVAILABLE")
    load_schwab_app_config(repo_root, environment)


async def _produce(
    settings: EngineSettings,
    scope: PriceScope,
    *,
    environment: dict[str, str],
    repo_root: Path,
    claim: AuditEvent,
    scheduled_at: datetime,
) -> dict[str, object]:
    from nexus.market.object_store import S3ProviderEvidenceStore
    from nexus.ops.schwab_market_stream import capture_and_import_schwab_level_one_batch
    from nexus.providers.market.schwab_oauth import schwab_http_client

    factory = session_factory(settings)
    try:
        # No account/security/audit lock crosses provider I/O. Normal import preserves
        # source bytes, receipt, observed_at/available_at and actual database receipt time.
        async with factory() as session, session.begin():
            async with schwab_http_client(timeout=30) as client:
                imported = await capture_and_import_schwab_level_one_batch(
                    session,
                    S3ProviderEvidenceStore(settings, disable_environment_proxies=True),
                    repo_root=repo_root,
                    client=client,
                    bindings={row.symbol: row.instrument_id for row in scope.needed},
                    max_instruments=5,
                    process_environment=environment,
                )
            if set(imported.quote_ids) != {row.symbol for row in scope.needed}:
                raise ValueError("MONITOR_PRICE_CAPTURE_INCOMPLETE")
            # Source I/O has finished. Publish its metadata only with an exact
            # current-scope proof and the durable tick result in this transaction.
            await _locks(session, scope.account_id)
            now = await _clock(session)
            current = await _select_scope(
                session,
                settings,
                account_id=scope.account_id,
                calendar_authority_id=scope.calendar_id,
                now=now,
            )
            refs = {str(row["instrument_id"]): str(row["quote_id"]) for row in current.observations}
            expected = {
                str(row.instrument_id): str(imported.quote_ids[row.symbol]) for row in scope.needed
            }
            if (
                not scheduled_at <= now < scheduled_at + timedelta(minutes=5)
                or current.binding() != scope.binding()
                or current.needed
                or any(refs.get(key) != value for key, value in expected.items())
            ):
                raise ValueError("MONITOR_PRICE_REFRESH_UNVERIFIED")
            result = _result(
                "prepared",
                "MONITOR_PRICE_REFRESHED",
                requests=None,
                capture_count=1,
                observations=list(current.observations),
            )
            await _record(session, claim, result, now)
            return result
    finally:
        await factory.kw["bind"].dispose()


async def _events(
    session: AsyncSession, account_id: UUID, scheduled_at: datetime
) -> tuple[AuditEvent, ...]:
    return tuple(
        await session.scalars(
            select(AuditEvent)
            .where(
                AuditEvent.action == _ACTION,
                AuditEvent.payload["account_id"].astext == str(account_id),
                AuditEvent.payload["scheduled_at"].astext == scheduled_at.isoformat(),
                AuditEvent.payload["feed"].astext == _FEED,
            )
            .order_by(AuditEvent.event_seq)
        )
    )


def _prior(events: tuple[AuditEvent, ...]) -> dict[str, object] | None:
    from nexus.audit.canonical import AuditChainInput, audit_event_hash, canonical_audit_event

    if len(events) != 2:
        return None
    claim, terminal = events
    for event in events:
        canonical = canonical_audit_event(
            AuditChainInput(
                event_seq=event.event_seq,
                occurred_at=event.occurred_at,
                actor_id=event.actor_id,
                action=event.action,
                schema_version=event.schema_version,
                payload=event.payload,
            )
        )
        if (
            event.actor_id != "research-monitor"
            or event.schema_version != _SCHEMA
            or event.canonical_event != canonical
            or event.event_hash != audit_event_hash(event.previous_hash, canonical)
        ):
            return None
    if (
        claim.payload.get("phase") != "claimed"
        or terminal.payload.get("phase") != "result"
        or terminal.payload.get("claim_id") != str(claim.id)
        or terminal.payload.get("claim_hash") != claim.event_hash
    ):
        return None
    result = terminal.payload.get("result")
    return dict(result) if isinstance(result, dict) else None


async def _record(
    session: AsyncSession, claim: AuditEvent, result: dict[str, object], now: datetime
) -> None:
    await append_audit_event(
        session,
        actor_id="research-monitor",
        action=_ACTION,
        schema_version=_SCHEMA,
        occurred_at=now,
        payload=dict(
            phase="result",
            account_id=claim.payload["account_id"],
            scheduled_at=claim.payload["scheduled_at"],
            feed=_FEED,
            claim_id=str(claim.id),
            claim_hash=claim.event_hash,
            result=result,
        ),
    )


async def refresh_price_observations(
    settings: EngineSettings,
    *,
    account_id: UUID,
    calendar_authority_id: UUID,
    scheduled_at: datetime,
    environment: dict[str, str],
    repo_root: Path,
) -> dict[str, object]:
    """Refresh authorized current price plans once; never changes model occupancy."""
    if (
        not settings.agent_paper_cycle_enabled
        or settings.agent_paper_formal_model_config_id is None
    ):
        return _result("disabled", "MONITOR_PRICE_PROFILE_DISABLED")
    if settings.agent_paper_account_id != account_id:
        return _result("blocked", "MONITOR_HOST_SCOPE_CHANGED")
    try:
        if _bucket(scheduled_at) != scheduled_at:
            raise ValueError("MONITOR_TICK_INVALID")
        from nexus.ops.agent_paper_once import _validate_local_database

        _validate_local_database(settings.database_url)
    except ValueError:
        return _result("blocked", "MONITOR_TICK_INVALID")
    factory = session_factory(settings)
    claim = None
    try:
        async with asyncio.timeout(PRICE_REFRESH_SECONDS):
            async with factory() as session, session.begin():
                await _locks(session, account_id)
                now = await _clock(session)
                if _bucket(now) != scheduled_at:
                    return _result("blocked", "MONITOR_TICK_NOT_CURRENT")
                events = await _events(session, account_id, scheduled_at)
                if events:
                    prior = _prior(events)
                    if prior is None or prior.get("status") == "unknown":
                        return _result("unknown", "MONITOR_PRICE_REFRESH_UNKNOWN", requests=None)
                    if prior.get("status") != "prepared":
                        return prior
                    current = await _select_scope(
                        session,
                        settings,
                        account_id=account_id,
                        calendar_authority_id=calendar_authority_id,
                        now=now,
                    )
                    if current.binding() != events[0].payload.get("scope") or current.needed:
                        return _result("blocked", "MONITOR_PRICE_REFRESH_EXPIRED")
                    return _result(
                        "prepared",
                        "MONITOR_PRICE_ALREADY_FRESH",
                        observations=list(current.observations),
                        reused=True,
                    )
                scope = await _select_scope(
                    session,
                    settings,
                    account_id=account_id,
                    calendar_authority_id=calendar_authority_id,
                    now=now,
                )
                if not scope.needed:
                    return _result(
                        "prepared",
                        "MONITOR_PRICE_ALREADY_FRESH",
                        observations=list(scope.observations),
                    )
                try:
                    _configured(settings, environment, repo_root)
                except ValueError:
                    return _result("blocked", "MONITOR_PRICE_CONFIG_UNAVAILABLE")
                claim = await append_audit_event(
                    session,
                    actor_id="research-monitor",
                    action=_ACTION,
                    schema_version=_SCHEMA,
                    occurred_at=now,
                    payload=dict(
                        phase="claimed",
                        account_id=str(account_id),
                        scheduled_at=scheduled_at.isoformat(),
                        feed=_FEED,
                        scope=scope.binding(),
                        capture_instrument_ids=[str(row.instrument_id) for row in scope.needed],
                    ),
                )
            return await _produce(
                settings,
                scope,
                environment=dict(environment),
                repo_root=repo_root,
                claim=claim,
                scheduled_at=scheduled_at,
            )
    except BaseException as error:
        if claim is not None:
            result = _result("unknown", "MONITOR_PRICE_REFRESH_UNKNOWN", requests=None)

            async def close() -> None:
                async with factory() as session, session.begin():
                    await _locks(session, account_id)
                    # A commit acknowledgement may have been cancelled: never append
                    # a competing outcome if the terminal event already committed.
                    if len(await _events(session, account_id, scheduled_at)) == 1:
                        await _record(session, claim, result, await _clock(session))

            try:
                await asyncio.shield(close())
            except BaseException:
                pass  # The unresolved committed claim fences this tick permanently.
        elif isinstance(error, _Skip):
            result = _result("skipped", str(error))
        elif isinstance(error, ValueError | TypeError):
            result = _result("blocked", "MONITOR_PRICE_INPUTS_UNAVAILABLE")
        else:
            result = _result("unknown", "MONITOR_PRICE_REFRESH_UNKNOWN", requests=None)
        if isinstance(error, asyncio.CancelledError | KeyboardInterrupt | SystemExit):
            raise
        return result
    finally:
        await factory.kw["bind"].dispose()
