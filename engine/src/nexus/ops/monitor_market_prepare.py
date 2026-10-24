"""One durable, bounded single-stock market preparation; never create research work."""

import asyncio
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.monitoring.storage import lock_security
from nexus.paper.target_service import lock_portfolio_account
from nexus.persistence.models.monitoring import MonitorIncrement, MonitorObservationBatch
from nexus.persistence.models.runtime import AuditEvent
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings

MARKET_PREPARATION_SECONDS = 300
_ACTION = "research.monitor.market_preparation"
_SCHEMA = "monitor_market_preparation_v1"


@dataclass(frozen=True)
class MarketScope:
    batch: MonitorObservationBatch
    identity: Any
    candidate_batch_id: UUID
    universe_id: UUID
    calendar_id: UUID
    materials_id: UUID
    materials_hash: str
    model_id: UUID
    model_hash: str
    session_date: date
    fresh: dict[str, object] | None

    def binding(self) -> dict[str, object]:
        return dict(
            account_id=str(self.batch.account_id),
            batch_id=str(self.batch.id),
            instrument_id=str(self.batch.instrument_id),
            calendar_authority_id=str(self.calendar_id),
            candidate_batch_id=str(self.candidate_batch_id),
            universe_snapshot_id=str(self.universe_id),
            materials_snapshot_id=str(self.materials_id),
            materials_hash=self.materials_hash,
            model_config_id=str(self.model_id),
            model_hash=self.model_hash,
        )


def _result(batch_id: UUID, status: str, reason: str, **facts: object) -> dict[str, object]:
    return dict(
        status=status,
        reason=reason,
        batch_id=str(batch_id),
        model_requests=0,
        job_created=False,
        execution_locked=True,
        **facts,
    )


async def _clock(session: AsyncSession) -> datetime:
    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime):
        raise ValueError("MONITOR_CLOCK_UNAVAILABLE")
    return now


async def _locks(session: AsyncSession, account_id: UUID, batch_id: UUID) -> None:
    await session.execute(text("SET LOCAL lock_timeout='2s'"))
    await session.execute(text("SET LOCAL statement_timeout='5s'"))
    await lock_portfolio_account(session, account_id)


async def _preflight(
    session: AsyncSession,
    settings: EngineSettings,
    *,
    account_id: UUID,
    batch_id: UUID,
    calendar_authority_id: UUID,
    now: datetime,
) -> MarketScope:
    from nexus.agents.codex_formal_stage import require_formal_preparation_model_profile
    from nexus.agents.persistence import _shared_daily_budget_used
    from nexus.monitoring.batch_inputs import _inputs, read_batch_inputs
    from nexus.paper.agent_cycle_authority import (
        load_agent_paper_service_calendar_authority,
        load_quasi_realtime_cycle_authority,
        prepare_candidates,
    )
    from nexus.paper.candidate_batch import validate_agent_paper_candidate_batch
    from nexus.paper.formal_budget_protection import protected_demand
    from nexus.paper.manual_request import require_manual_account_available
    from nexus.paper.projection import load_paper_projection
    from nexus.persistence.models.data import DataSnapshot
    from nexus.persistence.models.monitoring import MonitorTrigger

    if (
        settings.agent_paper_account_id != account_id
        or not settings.agent_paper_cycle_enabled
        or settings.agent_paper_candidate_batch_id is None
        or settings.agent_paper_formal_model_config_id is None
    ):
        raise ValueError("MONITOR_HOST_SCOPE_CHANGED")
    batch = await session.get(MonitorObservationBatch, batch_id)
    if batch is None or batch.account_id != account_id or not 1 <= len(batch.trigger_ids) <= 32:
        raise ValueError("MONITOR_BATCH_UNAVAILABLE")
    await lock_security(session, account_id=account_id, instrument_id=batch.instrument_id)
    from nexus.monitoring.mixed_readback import unadmitted_stock_count

    if (
        await unadmitted_stock_count(
            session, account_id=account_id, instrument_id=batch.instrument_id
        )
        > 32
    ):
        raise ValueError("MONITOR_STOCK_PENDING_CAPACITY")
    if await session.scalar(
        select(MonitorIncrement.id).where(MonitorIncrement.batch_id == batch_id)
    ):
        raise ValueError("MONITOR_BATCH_ALREADY_ADMITTED")
    await require_manual_account_available(session, account_id=account_id, now=now)
    triggers = tuple(
        await session.scalars(select(MonitorTrigger).where(MonitorTrigger.batch_id == batch_id))
    )
    if len(triggers) != len(batch.trigger_ids) or any(
        t.status not in {"pending", "blocked"} or t.research_run_id for t in triggers
    ):
        raise ValueError("MONITOR_TRIGGER_UNAVAILABLE")
    material = await session.scalar(
        select(DataSnapshot)
        .where(
            DataSnapshot.source == "nexus_monitor_batch_inputs",
            DataSnapshot.provider_object_id == str(batch_id),
        )
        .order_by(DataSnapshot.created_at, DataSnapshot.id)
        .limit(1)
    )
    if material is None:
        raise ValueError("MONITOR_CONTENT_MISSING")
    original = await read_batch_inputs(session, snapshot_id=material.id, as_of=now)
    current = await _inputs(session, batch, now)
    if original is None or (original.parents, original.members, original.contents) != (
        current.parents,
        current.members,
        current.contents,
    ):
        raise ValueError("MONITOR_PREPARED_INPUTS_CHANGED")
    candidate, identities = await validate_agent_paper_candidate_batch(
        session, batch_id=settings.agent_paper_candidate_batch_id, account_id=account_id, as_of=now
    )
    identity = next((row for row in identities if row.instrument_id == batch.instrument_id), None)
    if identity is None:
        raise ValueError("MONITOR_SCOPE_INVALID")
    calendar = await load_agent_paper_service_calendar_authority(
        session, now=now, expected_calendar_authority_id=calendar_authority_id
    )
    if not calendar.market_open_at <= now < calendar.market_close_at:
        raise ValueError("MONITOR_MARKET_CLOSED")
    authority = await load_quasi_realtime_cycle_authority(
        session,
        account_id=account_id,
        now=now,
        expected_calendar_authority_id=calendar_authority_id,
        expected_universe_snapshot_id=candidate.universe_snapshot_id,
    )
    model = await require_formal_preparation_model_profile(
        session, model_config_id=settings.agent_paper_formal_model_config_id, now=now
    )
    used, prior_limit = (
        await session.execute(
            select(
                func.coalesce(func.sum(MonitorIncrement.reserved_sdk_calls), 0),
                func.min(MonitorIncrement.daily_sdk_call_limit),
            ).where(
                MonitorIncrement.account_id == account_id,
                MonitorIncrement.session_date == authority.decision_session,
            )
        )
    ).one()
    limit = (
        settings.agent_monitor_daily_sdk_call_limit
        if prior_limit is None
        else min(prior_limit, settings.agent_monitor_daily_sdk_call_limit)
    )
    if used + 3 > limit:
        raise ValueError("MONITOR_DAILY_CALL_LIMIT")
    demand = await protected_demand(session, business_date=now.date(), now=now)
    if demand is not None:
        if demand.daily_limit_usd != settings.agent_daily_budget_usd:
            raise ValueError("FORMAL_BUDGET_COVERAGE_UNKNOWN")
        total = demand.native_usd + demand.hold_usd + demand.future_usd
    else:
        total = await _shared_daily_budget_used(session, business_date=now.date())
    if total + model.worst_case_cost_usd > settings.agent_daily_budget_usd:
        raise ValueError("FORMAL_BUDGET_PROTECTED")
    projection = await load_paper_projection(session, account_id=account_id, as_of=now)
    if projection.nav <= 0:
        raise ValueError("MONITOR_CURRENT_INPUTS_UNAVAILABLE")
    candidates = await prepare_candidates(
        session,
        authority=authority,
        requested_instrument_ids=(batch.instrument_id,),
        max_instruments=1,
    )
    fresh: dict[str, object] | None = None
    if len(candidates) == 1:
        from nexus.persistence.models.market import LatestQuote

        quote = await session.get(LatestQuote, candidates[0].decision_quote_id)
        if quote is not None:
            fresh = dict(
                quote_id=str(quote.id),
                data_artifact_id=str(quote.data_artifact_id),
                decision_time=now.isoformat(),
                risk_authority_ids=[str(x) for x in candidates[0].risk_query_authority_ids],
            )
    return MarketScope(
        batch,
        identity,
        candidate.batch_id,
        candidate.universe_snapshot_id,
        calendar_authority_id,
        material.id,
        material.revision,
        model.id,
        model.config_hash,
        authority.decision_session,
        fresh,
    )


def _configured(settings: EngineSettings, environment: dict[str, str]) -> Any:
    from nexus.ops.agent_paper_loop import _FileTriggerSettings, _require_explicit_schwab_config
    from nexus.ops.agent_paper_once import _validate_trigger_dependencies

    _require_explicit_schwab_config(environment)
    # Explicit inputs only: never supplement missing private data from process env.
    values = {
        **settings.model_dump(),
        "alpaca_key": environment.get("ALPACA_API_KEY_ID", ""),
        "alpaca_secret": environment.get("ALPACA_API_SECRET_KEY", ""),
    }
    configured = _FileTriggerSettings.model_validate(values)
    _validate_trigger_dependencies(configured)
    return configured


async def _produce(
    settings: Any,
    scope: MarketScope,
    *,
    environment: dict[str, str],
    repo_root: Path,
    claim_id: UUID,
) -> None:
    from nexus.ops.agent_paper_once import (
        S3ProviderEvidenceStore,
        prepare_schwab_decision_authority,
        schwab_http_client,
    )

    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin(), schwab_http_client(timeout=30) as client:
            await prepare_schwab_decision_authority(
                session,
                S3ProviderEvidenceStore(settings, disable_environment_proxies=True),
                settings,
                account_id=scope.batch.account_id,
                decision_session=scope.session_date,
                decision_slot_at=scope.batch.scheduled_at,
                idempotency_key=f"monitor-market:{scope.batch.id}:{claim_id}",
                identities=(scope.identity,),
                expected_universe_snapshot_id=scope.universe_id,
                repo_root=repo_root,
                client=client,
                expected_calendar_authority_id=scope.calendar_id,
                process_environment=environment,
                expand_execution_scope=False,
                manual_market_window=True,
            )
    finally:
        await factory.kw["bind"].dispose()


async def prepare_batch_market_inputs(
    settings: EngineSettings,
    *,
    account_id: UUID,
    batch_id: UUID,
    calendar_authority_id: UUID,
    environment: dict[str, str],
    repo_root: Path | None = None,
) -> dict[str, object]:
    from nexus.ops.agent_paper_once import _validate_local_database

    _validate_local_database(settings.database_url)
    factory = session_factory(settings)
    claim: AuditEvent | None = None
    scope: MarketScope | None = None
    try:
        async with factory() as session, session.begin():
            await _locks(session, account_id, batch_id)
            now = await _clock(session)
            if await session.scalar(
                select(MonitorIncrement.id).where(MonitorIncrement.batch_id == batch_id)
            ):
                return _result(batch_id, "blocked", "MONITOR_BATCH_ALREADY_ADMITTED")
            events = tuple(
                await session.scalars(
                    select(AuditEvent)
                    .where(
                        AuditEvent.action == _ACTION,
                        AuditEvent.payload["batch_id"].astext == str(batch_id),
                    )
                    .order_by(AuditEvent.event_seq)
                )
            )
            if events:
                state = _history_state(events, account_id=account_id)
                if state not in {"prepared", "blocked"}:
                    return _result(
                        batch_id,
                        "unknown",
                        "MONITOR_MARKET_PREPARATION_UNKNOWN",
                        provider_effects="unknown",
                    )
                result = events[-1].payload["result"]
                assert isinstance(result, dict)
                if state == "blocked":
                    return dict(result)
                scope = await _preflight(
                    session,
                    settings,
                    account_id=account_id,
                    batch_id=batch_id,
                    calendar_authority_id=calendar_authority_id,
                    now=now,
                )
                if (
                    events[0].payload.get("scope") != scope.binding()
                    or scope.fresh is None
                    or scope.fresh.get("quote_id") != result.get("quote_id")
                ):
                    expired = _result(batch_id, "blocked", "MONITOR_MARKET_PREPARATION_EXPIRED")
                    await _record(session, events[0], expired, now)
                    return expired
                return {**result, "reused": True}
            scope = await _preflight(
                session,
                settings,
                account_id=account_id,
                batch_id=batch_id,
                calendar_authority_id=calendar_authority_id,
                now=now,
            )
            if scope.fresh is not None:
                return _result(
                    batch_id,
                    "prepared",
                    "MONITOR_MARKET_ALREADY_FRESH",
                    **scope.fresh,
                    provider_requests=0,
                )
            try:
                configured = _configured(settings, environment)
            except (ValueError, TypeError):
                return _result(
                    batch_id, "blocked", "MONITOR_MARKET_CONFIG_UNAVAILABLE", provider_requests=0
                )
            claim = await append_audit_event(
                session,
                actor_id="research-monitor",
                action=_ACTION,
                schema_version=_SCHEMA,
                occurred_at=now,
                payload=dict(
                    phase="claimed",
                    batch_id=str(batch_id),
                    account_id=str(account_id),
                    scope=scope.binding(),
                ),
            )
        assert claim is not None and scope is not None
        try:
            async with asyncio.timeout(MARKET_PREPARATION_SECONDS):
                await _produce(
                    configured,
                    scope,
                    environment=dict(environment),
                    repo_root=repo_root or Path(__file__).resolve().parents[4],
                    claim_id=claim.id,
                )
            async with factory() as session, session.begin():
                await _locks(session, account_id, batch_id)
                now = await _clock(session)
                current = await _preflight(
                    session,
                    settings,
                    account_id=account_id,
                    batch_id=batch_id,
                    calendar_authority_id=calendar_authority_id,
                    now=now,
                )
                if current.binding() != scope.binding() or current.fresh is None:
                    raise ValueError("MONITOR_MARKET_REFRESH_UNVERIFIED")
                result = _result(
                    batch_id,
                    "prepared",
                    "MONITOR_MARKET_PREPARED",
                    **current.fresh,
                    provider_effects="verified",
                )
                await _record(session, claim, result, now)
                return result
        except BaseException as error:
            result = _result(
                batch_id,
                "unknown",
                "MONITOR_MARKET_PREPARATION_UNKNOWN",
                provider_effects="unknown",
            )

            async def close() -> None:
                async with factory() as session, session.begin():
                    await _locks(session, account_id, batch_id)
                    await _record(session, claim, result, await _clock(session))

            try:
                await asyncio.shield(close())
            except BaseException:
                pass  # The committed unresolved claim is itself the permanent no-replay fence.
            if isinstance(error, asyncio.CancelledError | KeyboardInterrupt | SystemExit):
                raise
            return result
    except (ValueError, TypeError):
        return _result(
            batch_id, "blocked", "MONITOR_MARKET_INPUTS_UNAVAILABLE", provider_requests=0
        )
    except Exception:
        return _result(
            batch_id, "unknown", "MONITOR_MARKET_PREPARATION_UNKNOWN", provider_effects="unknown"
        )
    finally:
        await factory.kw["bind"].dispose()


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
            batch_id=claim.payload["batch_id"],
            account_id=claim.payload["account_id"],
            claim_id=str(claim.id),
            claim_hash=claim.event_hash,
            result=result,
        ),
    )


def _history_state(events: tuple[AuditEvent, ...], *, account_id: UUID | None = None) -> str:
    from nexus.audit.canonical import AuditChainInput, audit_event_hash, canonical_audit_event

    if not events:
        return "pending"
    claim = events[0]
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
            or canonical != event.canonical_event
            or audit_event_hash(event.previous_hash, canonical) != event.event_hash
            or event.payload.get("account_id") != claim.payload.get("account_id")
            or event.payload.get("batch_id") != claim.payload.get("batch_id")
        ):
            return "unknown"
    if claim.payload.get("phase") != "claimed" or (
        account_id is not None and claim.payload.get("account_id") != str(account_id)
    ):
        return "unknown"
    if len(events) not in {2, 3}:
        return "unknown"
    states = []
    for event in events[1:]:
        result = event.payload.get("result")
        if (
            event.payload.get("phase") != "result"
            or event.payload.get("claim_id") != str(claim.id)
            or event.payload.get("claim_hash") != claim.event_hash
            or not isinstance(result, dict)
        ):
            return "unknown"
        states.append(result.get("status"))
    if len(states) == 2 and states != ["prepared", "blocked"]:
        return "unknown"
    return str(states[-1]) if states[-1] in {"prepared", "blocked"} else "unknown"


async def inspect_batch_market_preparation(session: AsyncSession, batch_id: UUID) -> str:
    events = tuple(
        await session.scalars(
            select(AuditEvent)
            .where(
                AuditEvent.action == _ACTION, AuditEvent.payload["batch_id"].astext == str(batch_id)
            )
            .order_by(AuditEvent.event_seq)
        )
    )
    batch = await session.get(MonitorObservationBatch, batch_id)
    return _history_state(events, account_id=batch.account_id if batch is not None else None)


async def confirm_batch_market_preparation(
    settings: EngineSettings, *, account_id: UUID, batch_id: UUID
) -> bool:
    if (
        settings.agent_paper_account_id is not None
        and settings.agent_paper_account_id != account_id
    ):
        return False
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            batch = await session.get(MonitorObservationBatch, batch_id)
            if batch is not None and batch.account_id != account_id:
                return False
            events = tuple(
                await session.scalars(
                    select(AuditEvent)
                    .where(
                        AuditEvent.action == _ACTION,
                        AuditEvent.payload["batch_id"].astext == str(batch_id),
                    )
                    .order_by(AuditEvent.event_seq)
                )
            )
            return _history_state(events, account_id=account_id) in {
                "pending",
                "prepared",
                "blocked",
            }
    finally:
        await factory.kw["bind"].dispose()
