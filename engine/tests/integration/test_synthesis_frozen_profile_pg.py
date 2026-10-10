"""A structurally valid ordinary row cannot acquire a nonstandard prompt profile."""

import pytest
from nexus.agents.portfolio_synthesis_contracts import (
    PortfolioSynthesisInputV1,
    canonical_portfolio_synthesis_hash,
)
from nexus.agents.portfolio_synthesis_persistence import (
    admit_portfolio_synthesis,
    persist_portfolio_synthesis_terminal,
)
from nexus.persistence.models.agents import AgentPortfolioSynthesisRunRecord
from nexus.persistence.models.runtime import Job
from sqlalchemy import func, select

from engine.tests.integration.test_portfolio_synthesis_persistence_pg import seed_synthesis


@pytest.mark.asyncio
async def test_ordinary_historical_admission_rejects_nonstandard_frozen_prompt(
    migrated_database_url,
):
    engine, sessions, kwargs = await seed_synthesis(migrated_database_url)

    class RollBackFixture(Exception):
        pass

    try:
        async with sessions.begin() as session:
            with pytest.raises(RollBackFixture):
                async with session.begin_nested():
                    admitted = await admit_portfolio_synthesis(session, **kwargs)
                    values = {
                        c.name: getattr(admitted.run, c.name)
                        for c in AgentPortfolioSynthesisRunRecord.__table__.columns
                        if c.name not in {"id", "created_at"}
                    }
                    raise RollBackFixture()
            frozen = dict(values["frozen_input"])
            frozen["prompt_version"] = "unregistered-profile"
            values.update(
                frozen_input=frozen,
                input_hash=canonical_portfolio_synthesis_hash(
                    PortfolioSynthesisInputV1.model_validate(frozen)
                ),
            )
            bad = AgentPortfolioSynthesisRunRecord(**values)
            session.add(bad)
            await session.flush()
            before = await session.scalar(select(func.count()).select_from(Job))
            with pytest.raises(ValueError, match="PORTFOLIO_SYNTHESIS_PROMPT_INVALID"):
                await admit_portfolio_synthesis(session, **kwargs)
            with pytest.raises(ValueError, match="PORTFOLIO_SYNTHESIS_PROMPT_INVALID"):
                await persist_portfolio_synthesis_terminal(
                    session,
                    run_id=bad.id,
                    input_hash=bad.input_hash,
                    status="failed",
                    intent=None,
                    input_tokens=None,
                    output_tokens=None,
                    now=kwargs["now"],
                )
            assert await session.scalar(select(func.count()).select_from(Job)) == before
    finally:
        await engine.dispose()
