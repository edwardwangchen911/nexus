"""Real isolated allocation races and immutable owner checks; never a provider call."""

import asyncio
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from nexus.agents.budget import BudgetExhausted
from nexus.agents.persistence import (
    _shared_daily_budget_used,
    require_reserved_daily_budget,
    reserve_daily_budget,
)
from nexus.paper.routine_budget import remaining, scope
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.session import session_factory
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError


async def assert_allocations(settings, run_id, now):
    factory = session_factory(settings)
    try:
        async with factory.begin() as db:
            original = await db.get(AgentResearchRunRecord, run_id)
            guard = await scope(db, now.date())
            assert guard is not None
            left, recorded = await remaining(db, guard, now=now)
            assert len(left) == 4 and recorded == {"increment"}
            for _ in range(2):
                await require_reserved_daily_budget(
                    db, run_id=run_id, accounting=True, for_new_send=True
                )
            assert (
                len((await db.scalars(text("SELECT task FROM routine_budget_credits"))).all()) == 1
            )
            maximum = guard["maximum"]
            assert maximum > 0
            other = next(i for i in guard["members"] if i != original.instrument_id)
            frozen = await db.get(DataSnapshot, original.data_snapshot_id)
            parent = await db.get(
                AgentResearchRunRecord, UUID(frozen.payload["monitor_context"]["parent_run_id"])
            )
            base = {c.name: getattr(parent, c.name) for c in parent.__table__.columns}
            base.update(
                model_config_id=original.model_config_id,
                model_hash=original.model_hash,
                pricing_version=original.pricing_version,
                reserved_cost_usd=original.reserved_cost_usd,
                status="prepared",
                settled_cost_usd=None,
                input_tokens=None,
                output_tokens=None,
            )
            ids = [uuid4(), uuid4()]
            for identity in ids:
                db.add(
                    AgentResearchRunRecord(
                        **(
                            base
                            | dict(
                                id=identity,
                                instrument_id=other,
                                actor_id="ordinary-budget-test",
                                idempotency_key=str(identity),
                                input_hash=identity.hex * 2,
                                decision_time=now,
                                created_at=now,
                            )
                        )
                    )
                )

        async def reserve(identity):
            try:
                async with factory.begin() as db:
                    await reserve_daily_budget(
                        db,
                        run_id=identity,
                        now=now,
                        daily_limit_usd=Decimal(13),
                        worst_case_usd=maximum,
                        pricing_version=original.pricing_version,
                        pricing_fresh=True,
                    )
                return "funded"
            except BudgetExhausted as error:
                assert str(error) == "ROUTINE_NATURAL_BUDGET_PROTECTED"
                return "blocked"

        outcomes = await asyncio.gather(*(reserve(i) for i in ids))
        assert sorted(outcomes) == ["blocked", "funded"], outcomes
        async with factory.begin() as db:
            assert await _shared_daily_budget_used(db, business_date=now.date()) == 2 * maximum
            await require_reserved_daily_budget(
                db, run_id=run_id, accounting=True, for_new_send=True
            )
            for statement, args in [
                ("UPDATE routine_budget_scopes SET maximum=0 WHERE id=:id", {"id": guard["id"]}),
                ("DELETE FROM routine_budget_credits WHERE budget_day=:day", {"day": now.date()}),
                (
                    """INSERT INTO routine_budget_credits(budget_day,scope_id,task,run_id,maximum)
                   VALUES(:day,:scope,:task,:run,:maximum)""",
                    dict(
                        day=now.date(),
                        scope=guard["id"],
                        task="research:" + str(other),
                        run=ids[0],
                        maximum=maximum,
                    ),
                ),
            ]:
                with pytest.raises(DBAPIError):
                    async with db.begin_nested():
                        await db.execute(text(statement), args)
                        await db.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
            # The one global increment credit never changes owner.
            assert (
                await db.scalar(
                    text("SELECT run_id FROM routine_budget_credits WHERE task='increment'")
                )
                == run_id
            )
            assert await db.scalar(text("SELECT count(*) FROM routine_budget_credits")) == 1
    finally:
        await factory.kw["bind"].dispose()


async def assert_price_updates(settings, run_id, now):
    from datetime import datetime, timedelta
    from hashlib import sha256

    from nexus.agents.platform_profile import register_sol_business_profile
    from nexus.paper.routine_budget import ensure_scope
    from nexus.persistence.models.agents import AgentModelConfigRecord

    f = session_factory(settings)
    try:
        async with f() as db:
            original = await db.get(AgentResearchRunRecord, run_id)
            model = await db.get(AgentModelConfigRecord, original.model_config_id)
            low = dict(model.platform_profile["price_record"])
        high = {
            **low,
            **{
                key: str(Decimal(low[key]) * 2)
                for key in [
                    "ordinary_input_usd_per_million",
                    "cache_read_usd_per_million",
                    "cache_write_30m_usd_per_million",
                    "output_usd_per_million",
                ]
            },
        }
        high["verified_at"] = (
            datetime.fromisoformat(low["verified_at"]) + timedelta(seconds=1)
        ).isoformat()
        evidence = b"controlled higher-price evidence; never a real provider call"
        high["source_evidence_sha256"] = sha256(evidence).hexdigest()
        # Same verification time with a DIFFERENT higher price must stop the
        # normal new-send path even when the old H would still fit.
        conflict = {**high, "verified_at": low["verified_at"]}
        async with f.begin() as db:
            row = await register_sol_business_profile(
                db, price_record=conflict, source_evidence=evidence
            )
            conflict_id = row.id
        cf = session_factory(
            settings.model_copy(update={"agent_paper_formal_model_config_id": conflict_id})
        )
        try:
            with pytest.raises(BudgetExhausted, match="ROUTINE_BUDGET_MODEL_CHANGED"):
                async with cf.begin() as db:
                    await require_reserved_daily_budget(
                        db, run_id=run_id, accounting=True, for_new_send=True
                    )
        finally:
            await cf.kw["bind"].dispose()
        async with f.begin() as db:
            row = await register_sol_business_profile(
                db, price_record=high, source_evidence=evidence
            )
            high_id = row.id
        high_settings = settings.model_copy(update={"agent_paper_formal_model_config_id": high_id})
        hf = session_factory(high_settings)
        try:
            # All scope fields and binding are valid; only audit actor is wrong.
            from nexus.audit.service import append_audit_event

            with pytest.raises(DBAPIError, match="ROUTINE_BUDGET_AUDIT_INVALID"):
                async with hf.begin() as db:
                    identity = uuid4()
                    at = await db.scalar(text("SELECT clock_timestamp()"))
                    await db.execute(
                        text("""
                        INSERT INTO routine_budget_scopes
                        SELECT :id,budget_day,sequence+1,id,account_id,batch_id,policy_id,
                        :model,calendar_id,slot,:at,day_is_session,daily_limit,:maximum
                        FROM routine_budget_scopes WHERE sequence=1
                    """),
                        dict(
                            id=identity,
                            model=high_id,
                            at=at,
                            maximum=2 * original.reserved_cost_usd,
                        ),
                    )
                    binding = await db.scalar(
                        text("""
                        SELECT (to_jsonb(g)-'maximum'-'daily_limit')||jsonb_build_object(
                        'maximum',g.maximum::text,'daily_limit',g.daily_limit::text)
                        FROM routine_budget_scopes g WHERE id=:id
                    """),
                        {"id": identity},
                    )
                    await append_audit_event(
                        db,
                        actor_id="wrong-budget-source",
                        action="research.routine_budget.scope",
                        schema_version="routine_budget_scope_v1",
                        occurred_at=at,
                        payload={"binding": binding},
                    )
                    await db.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
            async with hf.begin() as db:
                g = await ensure_scope(db, now=now, daily_limit_usd=Decimal(13))
                assert g["sequence"] == 2 and g["maximum"] == 2 * original.reserved_cost_usd
                assert await db.scalar(text("SELECT count(*) FROM routine_budget_credits")) == 1
            with pytest.raises(BudgetExhausted, match="ROUTINE_NATURAL_BUDGET_PROTECTED"):
                async with hf.begin() as db:
                    await require_reserved_daily_budget(
                        db, run_id=run_id, accounting=True, for_new_send=True
                    )
        finally:
            await hf.kw["bind"].dispose()
        # A distinct, lower quote at the SAME verification instant is not equivalent.
        lower = {
            **low,
            "verified_at": high["verified_at"],
            "source_evidence_sha256": sha256(b"controlled lower quote").hexdigest(),
        }
        async with f.begin() as db:
            row = await register_sol_business_profile(
                db, price_record=lower, source_evidence=b"controlled lower quote"
            )
            lower_id = row.id
        lf = session_factory(
            settings.model_copy(update={"agent_paper_formal_model_config_id": lower_id})
        )
        try:
            with pytest.raises(BudgetExhausted, match="ROUTINE_BUDGET_MODEL_CHANGED"):
                async with lf.begin() as db:
                    await ensure_scope(db, now=now, daily_limit_usd=Decimal(13))
            with pytest.raises(DBAPIError, match="ROUTINE_BUDGET_SCOPE_INVALID"):
                async with lf.begin() as db:
                    await db.execute(
                        text(
                            """INSERT INTO routine_budget_scopes
                            SELECT :new,budget_day,3,id,account_id,batch_id,policy_id,:model,
                            calendar_id,slot,clock_timestamp(),day_is_session,daily_limit,:maximum
                            FROM routine_budget_scopes WHERE sequence=2"""
                        ),
                        {"new": uuid4(), "model": lower_id, "maximum": original.reserved_cost_usd},
                    )
        finally:
            await lf.kw["bind"].dispose()
        async with f.begin() as db:
            # Original reservation remains readable for observation/cleanup.
            await require_reserved_daily_budget(db, run_id=run_id, accounting=True)
        with pytest.raises(BudgetExhausted, match="ROUTINE_BUDGET_MODEL_CHANGED"):
            async with f.begin() as db:
                await require_reserved_daily_budget(
                    db, run_id=run_id, accounting=True, for_new_send=True
                )
        async with f() as db:
            assert await db.scalar(text("SELECT max(sequence) FROM routine_budget_scopes")) == 2
            assert (
                await db.scalar(
                    text("SELECT run_id FROM routine_budget_credits WHERE task='increment'")
                )
                == run_id
            )
    finally:
        await f.kw["bind"].dispose()
