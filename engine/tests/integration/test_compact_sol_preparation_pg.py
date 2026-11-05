"""Normal queue/prepare/read paths with old and compact immutable profiles in real PG."""

from time import monotonic

import pytest
from integration.test_sol_native_preparation_pg import admit, scope
from integration.test_sol_profile_pg import SOURCE
from nexus.agents.platform_native import prepare_native_pool, read_native_allocation
from nexus.agents.platform_profile import register_sol_business_profile
from nexus.agents.platform_schema import (
    COMPACT_PLATFORM_UNITS_VERSION,
    CURRENT_PLATFORM_UNITS_VERSION,
)
from nexus.paper.manual_request import claim_manual_request, read_manual_request
from nexus.paper.research_request import prepare_research_request
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.manual_paper import ManualPaperRequest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError


@pytest.mark.asyncio
@pytest.mark.parametrize("old_queued", [True, False])
async def test_queued_identity_selects_exact_format_despite_new_profile_registration(
    migrated_database_url, tmp_path, old_queued
):
    factory, settings, store, _ = await scope(migrated_database_url, tmp_path)
    try:
        if old_queued:
            request_id = await admit(factory, settings, store, two_flight=False, claim=False)
        async with factory.begin() as db:
            old = await db.get(AgentModelConfigRecord, settings.agent_paper_formal_model_config_id)
            old_facts = await db.scalar(
                text("SELECT to_jsonb(m) FROM agent_model_configs m WHERE id=:id"), {"id": old.id}
            )
            new = await register_sol_business_profile(
                db,
                price_record=old.platform_profile["price_record"],
                source_evidence=SOURCE,
                units_version=COMPACT_PLATFORM_UNITS_VERSION,
            )
            assert new.id != old.id and new.worst_case_cost_usd == old.worst_case_cost_usd
            new_id = new.id
            assert (
                await db.scalar(
                    text("SELECT to_jsonb(m) FROM agent_model_configs m WHERE id=:id"),
                    {"id": old.id},
                )
                == old_facts
            )
        if not old_queued:
            settings = settings.model_copy(update={"agent_paper_formal_model_config_id": new_id})
            request_id = await admit(factory, settings, store, two_flight=False)
        expected = CURRENT_PLATFORM_UNITS_VERSION if old_queued else COMPACT_PLATFORM_UNITS_VERSION
        async with factory.begin() as db:
            db.info["sec_exhibit_store"] = store
            request = await db.get(ManualPaperRequest, request_id)
            if old_queued:
                assert (await read_manual_request(db, request)).status == "queued"
                await claim_manual_request(db, request_id=request_id)
            before = await db.scalar(
                text("SELECT to_jsonb(q) FROM manual_paper_requests q WHERE id=:id"),
                {"id": request_id},
            )
            result = await prepare_research_request(db, settings, request_id=request_id)
            assert result.status == "active" and len(result.research.members) == 3
            assert (
                await db.scalar(
                    text("SELECT to_jsonb(q) FROM manual_paper_requests q WHERE id=:id"),
                    {"id": request_id},
                )
                == before
            )
        async with factory() as db:
            db.info["sec_exhibit_store"] = store
            for member in result.research.members:
                run = await db.get(AgentResearchRunRecord, member.run_id)
                _, allocation = await read_native_allocation(db, run=run)
                assert allocation.units_version == expected
                assert run.model_config_id == request.model_config_id
                recovered = await prepare_native_pool(db, settings, run, deadline=monotonic() + 120)
                assert recovered._platform_allocation == allocation
            assert (
                await read_manual_request(db, await db.get(ManualPaperRequest, request_id))
            ) == result
            assert await db.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 0
            assert (
                await db.scalar(
                    text("SELECT bool_and(state='reserved') FROM agent_daily_budget_reservations")
                )
                is True
            )
        with pytest.raises(DBAPIError, match="agent audit records are append-only"):
            async with factory.begin() as db:
                await db.execute(
                    text(
                        "UPDATE agent_model_configs SET "
                        "platform_profile=platform_profile-'output_units_version' WHERE id=:id"
                    ),
                    {"id": new_id},
                )
    finally:
        await factory.kw["bind"].dispose()
