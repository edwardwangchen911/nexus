from datetime import UTC, datetime

import httpx
import pytest
from helpers.paper_batch import seed_paper_batch
from nexus.notifications.worker import load_bark_worker_settings, run_bark_worker
from nexus.paper.service import create_paper_order_batch
from nexus.persistence.models.paper import PaperOrderBatchRecord
from nexus.persistence.models.runtime import OutboxDeliveryAttempt, OutboxEvent
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

_OUTBOX_TIME = datetime(2027, 1, 1, 0, 0, 1, tzinfo=UTC)


@pytest.mark.asyncio
async def test_bark_failure_is_recorded_without_changing_committed_paper_facts(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "nexus.paper.service.current_paper_order_price_authority_available",
        lambda: True,
    )
    seed = await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            result = await create_paper_order_batch(
                session,
                account_id=seed.account_id,
                strategy_decision_id=seed.decision_id,
                idempotency_key="bark-business-fact",
                request_time=seed.request_time,
            )

        settings = load_bark_worker_settings(
            {
                "NEXUS_BARK_ENABLED": "true",
                "DATABASE_URL": migrated_database_url,
                "NEXUS_BARK_SERVER_URL": "http://127.0.0.1:8080",
                "NEXUS_BARK_DEVICE_KEY": "test-device-key",
                "NEXUS_BARK_ENCRYPTION_KEY": "0123456789abcdef",
                "NEXUS_BARK_ACTIVATED_AT": "2025-12-31T23:59:59Z",
            }
        )

        async def unavailable(request: httpx.Request) -> httpx.Response:
            return httpx.Response(503, text="private APNs detail")

        async with httpx.AsyncClient(transport=httpx.MockTransport(unavailable)) as client:
            assert (
                await run_bark_worker(
                    settings,
                    run_forever=False,
                    client=client,
                    clock=lambda: _OUTBOX_TIME,
                )
                == 1
            )

        async with sessions() as session:
            batch_count = int(
                await session.scalar(select(func.count()).select_from(PaperOrderBatchRecord)) or 0
            )
            batch = await session.get(PaperOrderBatchRecord, result.batch_id)
            failed_event = await session.scalar(
                select(OutboxEvent).where(OutboxEvent.attempt_count == 1)
            )
            assert failed_event is not None
            attempt = await session.scalar(
                select(OutboxDeliveryAttempt).where(
                    OutboxDeliveryAttempt.outbox_event_id == failed_event.id
                )
            )

        assert batch_count == 1
        assert batch is not None and batch.batch_hash == result.batch_hash
        assert (failed_event.status, failed_event.last_error_code) == (
            "queued",
            "PUBLISHER_FAILED",
        )
        assert attempt is not None
        assert (attempt.outcome, attempt.error_code) == (
            "failed",
            "PUBLISHER_FAILED",
        )
    finally:
        await engine.dispose()
