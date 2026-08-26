# ruff: noqa: E501
"""Forced real-PostgreSQL races for queued optimization job admission."""

import asyncio
from datetime import UTC, datetime
from uuid import UUID

import pytest
from nexus.optimizer.admission import admit_queued_job
from nexus.persistence.models.runtime import IdempotencyRecord, Job
from nexus.persistence.session import session_factory
from nexus.reconciliation.persistence_errors import IdempotencyConflict
from nexus.settings import EngineSettings
from sqlalchemy import func, select


@pytest.mark.asyncio
async def test_concurrent_same_body_claims_exactly_one_queued_job(
    migrated_database_url: str,
) -> None:
    """Moving the claim after Job creation would produce a duplicate or conflict in this forced race."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    barrier = asyncio.Barrier(2)

    async def admit(request_hash: str):
        async with factory() as session, session.begin():
            return await admit_queued_job(
                session,
                actor_id="test-actor",
                route="optimization-proposals",
                idempotency_key="same-key",
                request_hash=request_hash,
                payload={"policy": "optimizer_v1"},
                portfolio_id=UUID("0198d500-1000-7000-8000-000000000001"),
                effective_at=datetime(2026, 8, 27, tzinfo=UTC),
                after_authority=barrier.wait,
            )

    first, second = await asyncio.wait_for(
        asyncio.gather(admit("a" * 64), admit("a" * 64)), timeout=5
    )
    async with factory() as session:
        jobs = await session.scalar(select(func.count()).select_from(Job))
        records = await session.scalar(select(func.count()).select_from(IdempotencyRecord))
    assert first.job_id == second.job_id
    assert {first.replayed, second.replayed} == {False, True}
    assert (jobs, records) == (1, 1)


@pytest.mark.asyncio
async def test_concurrent_different_body_conflicts_without_second_job(
    migrated_database_url: str,
) -> None:
    """Reusing a key with another hash must fail closed after the owner commits its job."""
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    barrier = asyncio.Barrier(2)

    async def admit(request_hash: str):
        async with factory() as session, session.begin():
            return await admit_queued_job(
                session,
                actor_id="test-actor",
                route="optimization-proposals",
                idempotency_key="different-body",
                request_hash=request_hash,
                payload={"policy": "optimizer_v1"},
                portfolio_id=UUID("0198d500-1000-7000-8000-000000000001"),
                effective_at=datetime(2026, 8, 27, tzinfo=UTC),
                after_authority=barrier.wait,
            )

    results = await asyncio.gather(admit("b" * 64), admit("c" * 64), return_exceptions=True)
    assert sum(isinstance(result, IdempotencyConflict) for result in results) == 1
    assert sum(not isinstance(result, BaseException) for result in results) == 1
