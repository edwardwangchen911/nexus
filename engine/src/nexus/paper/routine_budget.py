"""Shared ordinary-day protection; it grants no research, once or sending permission."""

from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import RowMapping, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.budget import BudgetExhausted
from nexus.agents.persistence import _lock_budget_day, _shared_daily_budget_used
from nexus.audit.service import append_audit_event
from nexus.domain.ids import new_uuid7
from nexus.paper.research_request import database_now
from nexus.paper.single_sol_obligations import closed_settlements, original_slot
from nexus.persistence.models.agents import AgentModelConfigRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.settings import EngineSettings


async def scope(session: AsyncSession, day: date) -> RowMapping | None:
    return (
        (
            await session.execute(
                text("""SELECT g.*,b.candidate_instrument_ids AS members
      FROM routine_budget_scopes g JOIN agent_paper_candidate_batches b ON b.id=g.batch_id
      WHERE g.budget_day=:day ORDER BY g.sequence DESC LIMIT 1"""),
                {"day": day},
            )
        )
        .mappings()
        .one_or_none()
    )


async def _policies(session: AsyncSession) -> list[DataSnapshot]:
    return list(
        await session.scalars(
            select(DataSnapshot).where(DataSnapshot.source == "nexus_auto_increment_policy")
        )
    )


async def _insert_credit(
    session: AsyncSession, guard: RowMapping, task: str, run: UUID, maximum: Decimal
) -> None:
    await session.execute(
        text("""INSERT INTO routine_budget_credits(budget_day,scope_id,task,run_id,maximum)
      VALUES(:day,:scope,:task,:run,:maximum)"""),
        dict(day=guard["budget_day"], scope=guard["id"], task=task, run=run, maximum=maximum),
    )


async def _adopt_original_owners(session: AsyncSession, guard: RowMapping) -> None:
    if not guard["day_is_session"]:
        return
    # A source from before lazy initialization may already have funded a task.
    # Match each exact native owner, not all runs mentioning the same security.
    candidates = (
        (
            await session.execute(
                text("""
      SELECT r.id AS run_id,r.instrument_id,b.reserved_usd AS maximum,r.created_at AS created_at,
             'research' AS kind
      FROM agent_research_runs r JOIN agent_daily_budget_reservations b ON b.run_id=r.id
      WHERE b.business_date=:day AND (r.actor_id='local-agent-paper-cycle' OR r.actor_id=:actor)
      UNION ALL SELECT s.id,NULL,b.reserved_usd,s.created_at,'synthesis'
      FROM agent_portfolio_synthesis_runs s
      JOIN agent_portfolio_synthesis_budget_reservations b ON b.run_id=s.id
      WHERE b.business_date=:day ORDER BY created_at,run_id"""),
                dict(day=guard["budget_day"], actor=f"monitor:{guard['account_id']}"),
            )
        )
        .mappings()
        .all()
    )
    recorded = set(
        await session.scalars(
            text("SELECT task FROM routine_budget_credits WHERE budget_day=:day"),
            {"day": guard["budget_day"]},
        )
    )
    for r in candidates:
        options = (
            ("synthesis",)
            if r["kind"] == "synthesis"
            else (f"research:{r['instrument_id']}", "increment")
        )
        for task in options:
            if task in recorded:
                continue
            valid = await session.scalar(
                text("SELECT routine_budget_owner_valid(:scope,:task,:run,:maximum)"),
                dict(scope=guard["id"], task=task, run=r["run_id"], maximum=r["maximum"]),
            )
            if valid is True:
                await _insert_credit(session, guard, task, r["run_id"], r["maximum"])
                recorded.add(task)
                break


async def _ensure_scope(
    session: AsyncSession, *, now: datetime, daily_limit_usd: Decimal
) -> RowMapping | None:
    policies = await _policies(session)
    guard = await scope(session, now.date())
    if not policies and guard is None:
        return None
    settings = session.info.get("engine_settings")
    if not isinstance(settings, EngineSettings) or len(policies) != 1:
        raise BudgetExhausted("ROUTINE_BUDGET_AUTHORITY_UNAVAILABLE")
    policy = policies[0]
    if (
        settings.agent_paper_account_id is None
        or settings.agent_paper_candidate_batch_id is None
        or daily_limit_usd != settings.agent_daily_budget_usd
        or daily_limit_usd != Decimal(13)
        or policy.provider_object_id != str(settings.agent_paper_account_id)
        or policy.payload.get("candidate_batch_id") != str(settings.agent_paper_candidate_batch_id)
    ):
        raise BudgetExhausted("ROUTINE_BUDGET_SCOPE_INVALID")
    if await session.scalar(text("SHOW transaction_isolation")) != "read committed":
        raise ValueError("ROUTINE_BUDGET_STALE_SNAPSHOT")
    fresh = await database_now(session)
    if fresh.date() != now.date():
        raise BudgetExhausted("ROUTINE_BUDGET_WINDOW_CLOSED")
    from nexus.agents.codex_formal_stage import require_formal_stage_model_profile
    from nexus.agents.platform_profile import sol_business_model_matches
    from nexus.paper.agent_cycle_authority import select_agent_paper_service_calendar_authority
    from nexus.paper.candidate_batch import validate_agent_paper_candidate_batch

    batch, _ = await validate_agent_paper_candidate_batch(
        session,
        account_id=settings.agent_paper_account_id,
        batch_id=settings.agent_paper_candidate_batch_id,
        as_of=fresh,
    )
    model = await require_formal_stage_model_profile(
        session, model_config_id=settings.agent_paper_formal_model_config_id, now=fresh
    )
    if not sol_business_model_matches(model) or len(batch.instrument_ids) != 3:
        raise BudgetExhausted("ROUTINE_BUDGET_AUTHORITY_UNAVAILABLE")
    if guard is not None:
        if (guard["account_id"], guard["batch_id"], guard["policy_id"], guard["daily_limit"]) != (
            settings.agent_paper_account_id,
            batch.batch_id,
            policy.id,
            daily_limit_usd,
        ):
            raise BudgetExhausted("ROUTINE_BUDGET_SCOPE_INVALID")
        if guard["model_id"] == model.id:
            await _adopt_original_owners(session, guard)
            return guard
        previous_model = await session.get(AgentModelConfigRecord, guard["model_id"])
        if (
            previous_model is None
            or model.pricing_available_at < previous_model.pricing_available_at
            or (
                model.pricing_available_at == previous_model.pricing_available_at
                and (model.platform_profile or {}).get("price_record")
                != (previous_model.platform_profile or {}).get("price_record")
            )
        ):
            raise BudgetExhausted("ROUTINE_BUDGET_MODEL_CHANGED")
        calendar_id = guard["calendar_id"]
    else:
        calendar_id = (
            await select_agent_paper_service_calendar_authority(session, now=fresh)
        ).calendar_authority_id
    slot = original_slot(fresh.date())
    is_session = await session.scalar(
        text(
            """SELECT EXISTS(SELECT FROM exchange_calendar_sessions
              WHERE calendar_authority_id=:calendar AND session=:day
              AND market_open_at<=:slot AND market_close_at>:slot AND created_at<=:at)"""
        ),
        dict(calendar=calendar_id, day=fresh.date(), slot=slot, at=fresh),
    )
    identity = new_uuid7()
    at = await database_now(session)
    if at.date() != now.date():
        raise BudgetExhausted("ROUTINE_BUDGET_WINDOW_CLOSED")
    await session.execute(
        text("""INSERT INTO routine_budget_scopes(id,budget_day,sequence,previous_id,
      account_id,batch_id,policy_id,model_id,calendar_id,slot,created_at,day_is_session,daily_limit,maximum)
      VALUES(:id,:day,:sequence,:previous,:account,:batch,:policy,:model,:calendar,
      :slot,:at,:is_session,:limit,:maximum)"""),
        dict(
            id=identity,
            day=at.date(),
            sequence=1 if guard is None else guard["sequence"] + 1,
            previous=None if guard is None else guard["id"],
            account=settings.agent_paper_account_id,
            batch=batch.batch_id,
            policy=policy.id,
            model=model.id,
            calendar=calendar_id,
            slot=slot,
            at=at,
            is_session=is_session,
            limit=daily_limit_usd,
            maximum=model.worst_case_cost_usd,
        ),
    )
    binding = await session.scalar(
        text(
            """SELECT (to_jsonb(g)-'maximum'-'daily_limit')||jsonb_build_object(
                'maximum',g.maximum::text,'daily_limit',g.daily_limit::text)
                FROM routine_budget_scopes g WHERE id=:id"""
        ),
        {"id": identity},
    )
    await append_audit_event(
        session,
        actor_id="research-monitor",
        action="research.routine_budget.scope",
        schema_version="routine_budget_scope_v1",
        occurred_at=at,
        payload={"binding": binding},
    )
    result = await scope(session, at.date())
    assert result is not None
    await _adopt_original_owners(session, result)
    return result


async def ensure_scope(
    session: AsyncSession, *, now: datetime, daily_limit_usd: Decimal
) -> RowMapping | None:
    try:
        return await _ensure_scope(session, now=now, daily_limit_usd=daily_limit_usd)
    except BudgetExhausted:
        raise
    except ValueError as error:
        # Deterministic input/price validation precedes authorization publication.
        # Database/lock/commit errors deliberately retain their original classes.
        raise BudgetExhausted("ROUTINE_BUDGET_AUTHORITY_UNAVAILABLE") from error


async def remaining(
    session: AsyncSession, guard: RowMapping, *, now: datetime
) -> tuple[set[str], set[str]]:
    tasks = (
        {f"research:{i}" for i in guard["members"]} | {"synthesis", "increment"}
        if guard["day_is_session"]
        else set()
    )
    rows = (
        (
            await session.execute(
                text("""SELECT c.*,
      (pg_xact_status(c.xmin::text::xid8)='in progress') AS provisional,
      CASE WHEN c.task='synthesis' THEN s.state ELSE r.state END AS state
      FROM routine_budget_credits c
      LEFT JOIN agent_daily_budget_reservations r ON r.run_id=c.run_id
      AND r.business_date=c.budget_day
      AND r.reserved_usd=c.maximum
      LEFT JOIN agent_portfolio_synthesis_budget_reservations s ON s.run_id=c.run_id
      AND s.business_date=c.budget_day
      AND s.reserved_usd=c.maximum
      WHERE c.budget_day=:day"""),
                {"day": guard["budget_day"]},
            )
        )
        .mappings()
        .all()
    )
    recorded = {r["task"] for r in rows}
    if not recorded <= tasks or any(
        r["state"] not in {"reserved", "settled", "released"} for r in rows
    ):
        raise BudgetExhausted("ROUTINE_BUDGET_CREDIT_INVALID")
    for r in rows:
        # A visible in-progress tuple belongs to this transaction, including its
        # released savepoints. Native routes follow complete pool reservation;
        # deferred SQL proves these credits at commit, committed owners here.
        if not r["provisional"] and (
            await session.scalar(
                text("SELECT routine_budget_owner_valid(:scope,:task,:run,:maximum)"),
                dict(scope=r["scope_id"], task=r["task"], run=r["run_id"], maximum=r["maximum"]),
            )
            is not True
        ):
            raise BudgetExhausted("ROUTINE_BUDGET_CREDIT_INVALID")
    closed = await closed_settlements(
        session, {r["run_id"] for r in rows if r["state"] == "settled"}, now=now
    )
    return tasks - {
        r["task"] for r in rows if r["state"] == "reserved" or r["run_id"] in closed
    }, recorded


async def require_capacity(
    session: AsyncSession,
    *,
    kind: str,
    run_id: UUID,
    now: datetime,
    daily_limit_usd: Decimal,
    amount: Decimal,
    allocation: object = None,
) -> bool:
    guard = await ensure_scope(session, now=now, daily_limit_usd=daily_limit_usd)
    if guard is None:
        return False
    if allocation is not None:
        raise BudgetExhausted("ROUTINE_BUDGET_ECONOMIC_SCOPE_CONFLICT")
    left, recorded = await remaining(session, guard, now=now)
    from nexus.paper.single_sol_budget import _original_obligation

    task = (
        await _original_obligation(session, guard, kind, run_id, now)
        if amount == guard["maximum"] and guard["day_is_session"]
        else None
    )
    credit = task is not None and task not in recorded
    used = await _shared_daily_budget_used(session, business_date=guard["budget_day"])
    if used + amount + (len(left) - int(credit)) * guard["maximum"] > daily_limit_usd:
        raise BudgetExhausted("ROUTINE_NATURAL_BUDGET_PROTECTED")
    if (await database_now(session)).date() != now.date():
        raise BudgetExhausted("ROUTINE_BUDGET_WINDOW_CLOSED")
    if credit and task is not None:
        await _insert_credit(session, guard, task, run_id, amount)
    return True


async def require_new_send(
    session: AsyncSession, *, business_date: date, daily_limit_usd: Decimal | None = None
) -> None:
    await _lock_budget_day(session, business_date.isoformat())
    now = await database_now(session)
    guard = await scope(session, business_date)
    settings = session.info.get("engine_settings")
    update_price = (
        guard is not None and now.date() == business_date and isinstance(settings, EngineSettings)
    )
    if update_price:
        assert guard is not None and isinstance(settings, EngineSettings)
        selected_model = await session.get(
            AgentModelConfigRecord, settings.agent_paper_formal_model_config_id
        )
        frozen_model = await session.get(AgentModelConfigRecord, guard["model_id"])
        update_price = (
            selected_model is not None
            and frozen_model is not None
            and (
                selected_model.pricing_available_at > frozen_model.pricing_available_at
                or (
                    selected_model.pricing_available_at == frozen_model.pricing_available_at
                    and (selected_model.platform_profile or {}).get("price_record")
                    == (frozen_model.platform_profile or {}).get("price_record")
                )
            )
        )
    if update_price:
        guard = await ensure_scope(
            session, now=now, daily_limit_usd=settings.agent_daily_budget_usd
        )
    if guard is None:
        from nexus.paper.single_sol_budget import has_guard, remaining_obligations

        if await has_guard(session, business_date):
            old = (
                (
                    await session.execute(
                        text(
                            """SELECT g.*,b.candidate_instrument_ids AS members
                            FROM single_sol_budget_scopes g JOIN agent_paper_candidate_batches b
                            ON b.id=g.batch_id WHERE g.budget_day=:day"""
                        ),
                        {"day": business_date},
                    )
                )
                .mappings()
                .one()
            )
            left, _ = await remaining_obligations(
                session, day=business_date, members=old["members"], now=now
            )
            total = await _shared_daily_budget_used(session, business_date=business_date)
            if total + len(left) * old["maximum"] > old["daily_limit"]:
                raise BudgetExhausted("SINGLE_SOL_NATURAL_BUDGET_PROTECTED")
            return
        from nexus.paper.formal_budget_protection import protected_demand

        formal = await protected_demand(session, business_date=business_date, now=now)
        if formal is not None:
            if formal.native_usd + formal.hold_usd + formal.future_usd > formal.daily_limit_usd:
                raise BudgetExhausted("FORMAL_BUDGET_PROTECTED")
            return
        policies = await _policies(session)
        if not policies:
            return
        if now.date() != business_date:
            raise BudgetExhausted("ROUTINE_BUDGET_ORIGINAL_SCOPE_UNAVAILABLE")
        settings = session.info.get("engine_settings")
        if not isinstance(settings, EngineSettings):
            raise BudgetExhausted("ROUTINE_BUDGET_AUTHORITY_UNAVAILABLE")
        guard = await ensure_scope(
            session, now=now, daily_limit_usd=settings.agent_daily_budget_usd
        )
    if guard is None:
        return
    if daily_limit_usd is not None and daily_limit_usd != guard["daily_limit"]:
        raise BudgetExhausted("ROUTINE_BUDGET_SCOPE_INVALID")
    left, _ = await remaining(session, guard, now=now)
    used = await _shared_daily_budget_used(session, business_date=business_date)
    if used + len(left) * guard["maximum"] > guard["daily_limit"]:
        raise BudgetExhausted("ROUTINE_NATURAL_BUDGET_PROTECTED")


async def require_native_new_send(session: AsyncSession, *, kind: str, run_id: UUID) -> None:
    tables = {
        "synthesis": (
            "agent_portfolio_synthesis_budget_reservations",
            "agent_portfolio_synthesis_runs",
        ),
        "portfolio_decision": ("portfolio_decision_budget_reservations", "portfolio_decision_runs"),
    }
    if kind not in tables:
        raise ValueError("ROUTINE_BUDGET_KIND_INVALID")
    reserve, runs = tables[kind]
    row = (
        await session.execute(
            text(f"""SELECT b.business_date,b.state,b.pricing_version,
      r.pricing_version AS run_pricing
      FROM {reserve} b JOIN {runs} r ON r.id=b.run_id WHERE b.run_id=:run FOR UPDATE OF b"""),
            {"run": run_id},
        )
    ).one_or_none()
    if row is None or row.state != "reserved" or row.pricing_version != row.run_pricing:
        raise BudgetExhausted("ROUTINE_BUDGET_RESERVATION_INVALID")
    await require_new_send(session, business_date=row.business_date)
