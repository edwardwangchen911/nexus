"""Read immutable publications in a real PostgreSQL read-only transaction."""

from datetime import timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

import pytest
from helpers.paper_api import settings
from nexus.audit.service import append_audit_event
from nexus.paper.agent_challenger import (
    PORTFOLIO_CHALLENGER_PUBLICATION_ACTION,
    PortfolioChallengerPublicationV1,
)
from nexus.paper.economic_authorities import inspect_authorities
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentPromptVersionRecord
from nexus.persistence.models.runtime import AuditEvent, Job
from nexus.persistence.session import session_factory
from sqlalchemy import func, select, text


async def seed_pair(session, historical):
    now = await session.scalar(select(func.clock_timestamp()))
    model = AgentModelConfigRecord(
        id=UUID(int=1),
        model_id="gpt-5.6-terra",
        version="codex-sdk-chatgpt-v6-stage-time-budget",
        config_hash="a" * 64,
        max_input_tokens=96000,
        max_output_tokens=8192,
        max_tool_calls=0,
        input_usd_per_million=Decimal(0),
        output_usd_per_million=Decimal(0),
        pricing_available_at=now - timedelta(days=1),
        pricing_expires_at=now + timedelta(days=1),
    )
    incumbent = AgentPromptVersionRecord(
        id=UUID(int=2),
        role="portfolio_decision",
        version="portfolio-management-synthesis-v1",
        content="incumbent",
        release_channel="active",
        content_hash=sha256(b"incumbent").hexdigest(),
    )
    challenger = AgentPromptVersionRecord(
        id=UUID(int=3),
        role="portfolio_decision",
        version="test-challenger",
        content="challenger",
        release_channel="challenger",
        content_hash=sha256(b"challenger").hexdigest(),
    )
    session.add_all([model, incumbent, challenger])
    publication_model_id = model.id
    if historical:
        old = AgentModelConfigRecord(
            id=UUID(int=4),
            model_id=model.model_id,
            version="codex-sdk-chatgpt-v4-actionable",
            config_hash="c" * 64,
            max_input_tokens=96000,
            max_output_tokens=8192,
            max_tool_calls=0,
            input_usd_per_million=Decimal(0),
            output_usd_per_million=Decimal(0),
            pricing_available_at=model.pricing_available_at,
            pricing_expires_at=model.pricing_expires_at,
        )
        session.add(old)
        publication_model_id = old.id
    await session.flush()
    publication = PortfolioChallengerPublicationV1(
        schema_version="portfolio_challenger_pub_v1",
        proposal_hash="b" * 64,
        incumbent_prompt_version_id=incumbent.id,
        challenger_prompt_version_id=challenger.id,
        challenger_prompt_hash=challenger.content_hash,
        challenger_model_config_id=publication_model_id,
        replayed=False,
    )
    await append_audit_event(
        session,
        actor_id="isolated-fixture",
        action=PORTFOLIO_CHALLENGER_PUBLICATION_ACTION,
        schema_version=publication.schema_version,
        payload=publication.model_dump(mode="json"),
        occurred_at=now,
    )
    audit_count = await session.scalar(select(func.count()).select_from(AuditEvent))
    return audit_count


@pytest.mark.asyncio
@pytest.mark.parametrize("historical", [False, True])
async def test_exact_publication_model_in_read_only_database(migrated_database_url, historical):
    factory = session_factory(settings(migrated_database_url))
    async with factory.begin() as session:
        audit_count = await seed_pair(session, historical)
    async with factory.begin() as session:
        await session.execute(text("SET TRANSACTION READ ONLY"))
        assert await session.scalar(text("SHOW transaction_read_only")) == "on"
        result = await inspect_authorities(
            session,
            batch_id=UUID(int=99),
            proposal_hash="b" * 64,
            daily_limit_usd=Decimal(3),
        )
        expected = "MODEL_REVISION_MISMATCH" if historical else "VERSIONS_MATCH_AT_OBSERVATION"
        assert result.pair.reason_code == expected
        assert result.pair.activation_permitted is False
        assert result.formal_batch_status == "unavailable"
        assert result.formal_candidate_count is None
        assert result.shared_used_or_reserved_usd == 0
        assert result.atomic_evaluation_budget_reserved is False
        assert result.common_execution_window_verified is False
        assert audit_count == await session.scalar(select(func.count()).select_from(AuditEvent))
        assert await session.scalar(select(func.count()).select_from(Job)) == 0
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("historical,mispriced", [(False, False), (True, False), (False, True)])
async def test_freeze_exact_pair_without_republishing(migrated_database_url, historical, mispriced):
    from datetime import date

    from nexus.paper.economic_version_binding import bind_versions, decode_binding
    from nexus.paper.portfolio_economic_preflight import EconomicPlanRequest
    from nexus.paper.portfolio_economic_protocol import decode_protocol, register_protocol

    factory = session_factory(settings(migrated_database_url))
    async with factory.begin() as session:
        await seed_pair(session, historical)
        actual = await session.scalar(text("SELECT current_database()"))
        protocol = await register_protocol(
            session,
            EconomicPlanRequest(
                evaluation_key="binding-case",
                instruments=(
                    {"instrument_id": UUID(int=11), "symbol": "AVGO"},
                    {"instrument_id": UUID(int=12), "symbol": "NVDA"},
                ),
                start_session=date(2027, 1, 4),
                research_cost_usd="1" if mispriced else "0",
                synthesis_cost_usd="0",
                global_daily_limit_usd="3",
            ),
            expected_database=actual,
        )
        protocol_hash = decode_protocol(protocol).content_hash
        before = await session.scalar(select(func.count()).select_from(AuditEvent))
    if historical or mispriced:
        async with factory.begin() as session:
            with pytest.raises(
                ValueError,
                match="MODEL_REVISION_MISMATCH"
                if historical
                else "ECONOMIC_VERSION_COST_CLAIM_MISMATCH",
            ):
                await bind_versions(
                    session,
                    protocol_hash=protocol_hash,
                    proposal_hash="b" * 64,
                    expected_database=actual,
                )
        async with factory.begin() as session:
            assert before == await session.scalar(select(func.count()).select_from(AuditEvent))
    else:
        import asyncio

        async def register_one():
            async with factory.begin() as session:
                return await bind_versions(
                    session,
                    protocol_hash=protocol_hash,
                    proposal_hash="b" * 64,
                    expected_database=actual,
                )

        first, concurrent = await asyncio.gather(register_one(), register_one())
        assert first.id == concurrent.id
        async with factory.begin() as session:
            frozen = decode_binding(first)
            assert frozen.shared_model_id == UUID(int=1)
            assert frozen.incumbent_prompt_id == UUID(int=2)
            assert frozen.challenger_prompt_id == UUID(int=3)
            assert frozen.status == "versions_bound_not_activated"
        async with factory.begin() as session:
            second = await bind_versions(
                session,
                protocol_hash=protocol_hash,
                proposal_hash="b" * 64,
                expected_database=actual,
            )
            assert second.id == first.id
            assert before + 1 == await session.scalar(select(func.count()).select_from(AuditEvent))
        async with factory.begin() as session:
            with pytest.raises(ValueError, match="ECONOMIC_VERSION_BINDING_CONFLICT"):
                await bind_versions(
                    session,
                    protocol_hash=protocol_hash,
                    proposal_hash="c" * 64,
                    expected_database=actual,
                )
        async with factory.begin() as session:
            from nexus.paper.economic_version_binding import read_binding

            now = await session.scalar(select(func.clock_timestamp()))
            assert (await read_binding(session, as_of=first.occurred_at - timedelta(seconds=1)))[
                0
            ] == "unbound"
            assert (await read_binding(session, as_of=now))[1].content_hash == frozen.content_hash
            assert await session.scalar(select(func.count()).select_from(Job)) == 0
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(AuditEvent)
                    .where(AuditEvent.action == PORTFOLIO_CHALLENGER_PUBLICATION_ACTION)
                )
                == 1
            )
    await factory.kw["bind"].dispose()
