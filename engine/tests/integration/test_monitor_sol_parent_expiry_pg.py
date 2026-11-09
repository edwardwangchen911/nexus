"""Normal Sol parent remains immutable when a refreshed Sol target is registered."""

from datetime import timedelta

import httpx
import pytest
from integration import test_sol_native_preparation_pg as preparation
from integration.test_sol_native_stage_pg import install_sol_http
from integration.test_sol_profile_pg import SOURCE, current_record
from nexus import worker
from nexus.agents.codex_formal_stage import require_formal_stage_model_profile
from nexus.agents.platform_native import native_increment_model_allowed
from nexus.agents.platform_profile import register_sol_business_profile
from nexus.monitoring.source_qualification import load_qualified_source
from nexus.paper.manual_request import admit_manual_request
from nexus.paper.research_request import finish_research_request, prepare_research_request
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchOutputRecord
from nexus.persistence.models.manual_paper import ManualPaperRequest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession


async def original_rows(session, old_model_id):
    """Compare exact frozen rows, including original prices, usage and cost holds."""
    result = {}
    for table in (
        "agent_research_runs",
        "agent_research_outputs",
        "agent_daily_budget_reservations",
        "research_stage_route",
        "research_stage_observation",
        "manual_paper_requests",
        "manual_paper_request_events",
        "jobs",
        "job_leases",
    ):
        result[table] = tuple(
            await session.scalars(
                text(f"SELECT to_jsonb(t)::text FROM {table} t ORDER BY to_jsonb(t)::text")
            )
        )
    result["model"] = await session.scalar(
        text("SELECT to_jsonb(t)::text FROM agent_model_configs t WHERE id=:id"),
        {"id": old_model_id},
    )
    return result


@pytest.mark.asyncio
async def test_completed_sol_source_survives_expiry_but_new_child_requires_current_price(
    migrated_database_url, monkeypatch, tmp_path
):
    network_calls = []
    real_send = httpx.AsyncClient.send

    async def mock_only(self, *args, **kwargs):
        if not isinstance(self._transport, httpx.MockTransport):
            network_calls.append("external_http")
            raise AssertionError("only the synthetic Sol MockTransport is allowed")
        return await real_send(self, *args, **kwargs)

    def no_sync(*args, **kwargs):
        network_calls.append("external_sync_http")
        raise AssertionError("no external HTTP or notification is allowed")

    monkeypatch.setattr(httpx.AsyncClient, "send", mock_only)
    monkeypatch.setattr(httpx.Client, "send", no_sync)
    real_register = preparation.register

    async def short_initial_profile(factory, record):
        async with factory() as session:
            now = await session.scalar(text("SELECT clock_timestamp()"))
        return await real_register(
            factory, record | {"expires_at": (now + timedelta(minutes=16)).isoformat()}
        )

    # Shorten only the initial registration input, never an immutable stored row.
    monkeypatch.setattr(preparation, "register", short_initial_profile)
    factory, settings, store, _ = await preparation.scope(migrated_database_url, tmp_path)
    requests, clients = install_sol_http(monkeypatch, store)
    try:
        request_id = await preparation.admit(factory, settings, store, two_flight=False)
        async with factory.begin() as session:
            session.info["sec_exhibit_store"] = store
            prepared = await prepare_research_request(session, settings, request_id=request_id)
        for member in prepared.research.members:
            assert await worker.run_agent_worker_once(
                settings, job_ids=(member.job_id,), formal_stage_capacity=True
            )
        async with factory.begin() as session:
            row = await session.get(ManualPaperRequest, request_id)
            assert (await finish_research_request(session, row=row)).status == "completed"
        assert requests == ["research", "critique", "synthesis"] * 3
        assert len(clients) == 9 and all(client.is_closed for client in clients)
        async with factory() as session:
            session.info["sec_exhibit_store"] = store
            output = await session.scalar(
                select(AgentResearchOutputRecord).where(
                    AgentResearchOutputRecord.run_id == prepared.research.members[0].run_id,
                    AgentResearchOutputRecord.status == "completed",
                    AgentResearchOutputRecord.role.is_(None),
                )
            )
            assert output is not None
            now = await session.scalar(text("SELECT clock_timestamp()"))
            source = await load_qualified_source(
                session,
                account_id=settings.agent_paper_account_id,
                output_id=output.id,
                now=now,
            )
            old = await session.get(AgentModelConfigRecord, source.run.model_config_id)
            assert old is not None and old.pricing_expires_at > now + timedelta(minutes=10)
            expires = old.pricing_expires_at
            assert source.intent.valid_until > expires
            before = await original_rows(session, old.id)
            old_id, output_id = old.id, output.id
        real_scalar = AsyncSession.scalar

        async def advanced_clock(session, statement, *args, **kwargs):
            if str(statement) in (
                "SELECT clock_timestamp()",
                "SELECT clock_timestamp() AS clock_timestamp_1",
            ):
                return expires
            return await real_scalar(session, statement, *args, **kwargs)

        monkeypatch.setattr(AsyncSession, "scalar", advanced_clock)
        record = await current_record(factory)
        async with factory.begin() as session:
            session.info["sec_exhibit_store"] = store
            new = await register_sol_business_profile(
                session, price_record=record, source_evidence=SOURCE
            )
            assert new.id != old_id
            assert new.pricing_available_at <= expires < new.pricing_expires_at
            assert await native_increment_model_allowed(
                session, parent=source.run, model_config_id=new.id
            )
            renewed_source = await load_qualified_source(
                session,
                account_id=settings.agent_paper_account_id,
                output_id=output_id,
                now=expires,
            )
            assert renewed_source.run.id == source.run.id
            assert renewed_source.intent == source.intent

            async def no_terminal_proof(*args, **kwargs):
                return None

            with monkeypatch.context() as missing_proof:
                missing_proof.setattr(
                    "nexus.agents.research_stages.proof.read_terminal_proof", no_terminal_proof
                )
                with pytest.raises(ValueError, match="^MONITOR_SOURCE_INVALID$"):
                    await load_qualified_source(
                        session,
                        account_id=settings.agent_paper_account_id,
                        output_id=output_id,
                        now=expires,
                    )
            with pytest.raises(ValueError, match="^MONITOR_SOURCE_INVALID$"):
                await load_qualified_source(
                    session,
                    account_id=settings.agent_paper_account_id,
                    output_id=output_id,
                    now=expires + timedelta(seconds=1),
                )
            assert (
                await require_formal_stage_model_profile(
                    session, model_config_id=new.id, now=expires
                )
            ).id == new.id
            with pytest.raises(ValueError, match="^CODEX_FORMAL_AUTHORITY_INVALID$"):
                await admit_manual_request(
                    session,
                    account_id=settings.agent_paper_account_id,
                    batch_id=settings.agent_paper_candidate_batch_id,
                    model_config_id=old_id,
                    mode="research_and_monitor",
                    contract_version=3,
                    idempotency_key="expired-parent-is-not-child-permission",
                    runtime_mode="responses",
                    daily_limit_usd=settings.agent_daily_budget_usd,
                    two_flight=False,
                )
            assert await original_rows(session, old_id) == before
            assert requests == ["research", "critique", "synthesis"] * 3
            assert network_calls == []
    finally:
        await factory.kw["bind"].dispose()
