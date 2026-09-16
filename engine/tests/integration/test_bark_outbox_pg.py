import base64
import json
from datetime import UTC, datetime

import httpx
import pytest
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from helpers.paper_batch import seed_paper_batch
from helpers.paper_execution import seed_paper_execution
from nexus.notifications.worker import load_bark_worker_settings, run_bark_worker
from nexus.paper.execution_service import advance_internal_paper_order
from nexus.paper.service import create_paper_order_batch
from nexus.persistence.models.paper import PaperOrderBatchRecord
from nexus.persistence.models.runtime import OutboxDeliveryAttempt, OutboxEvent
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

_OUTBOX_TIME = datetime(2027, 1, 1, 0, 0, 1, tzinfo=UTC)
_BARK_ENCRYPTION_KEY = b"0123456789abcdef"


def _decrypted_bark_notification(request: httpx.Request) -> dict[str, str]:
    payload = json.loads(request.content)
    assert payload["device_key"] == "test-device-key"
    iv = payload["iv"].encode("ascii")
    decryptor = Cipher(algorithms.AES(_BARK_ENCRYPTION_KEY), modes.CBC(iv)).decryptor()
    padded = decryptor.update(base64.b64decode(payload["ciphertext"])) + decryptor.finalize()
    unpadder = padding.PKCS7(algorithms.AES.block_size).unpadder()
    return json.loads((unpadder.update(padded) + unpadder.finalize()).decode("utf-8"))


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


@pytest.mark.asyncio
async def test_bark_delivers_only_frozen_rejected_and_filled_paper_outcomes(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A broad registry would incorrectly claim queued decision and pending-order events."""
    monkeypatch.setattr(
        "nexus.paper.execution_service._paper_execution_authority_available",
        lambda _source_policy_id: True,
    )
    seed = await seed_paper_execution(
        migrated_database_url,
        window_bar_volumes=(100, 2000),
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async with factory.begin() as session:
            partial = await advance_internal_paper_order(
                session,
                order_id=seed.order_id,
                evaluated_at=seed.evaluation_times[0],
                idempotency_key="bark-partial",
            )
        async with factory.begin() as session:
            filled = await advance_internal_paper_order(
                session,
                order_id=seed.order_id,
                evaluated_at=seed.evaluation_times[1],
                idempotency_key="bark-filled",
            )
        assert (partial.action, partial.status) == ("partially_filled", "partially_filled")
        assert (filled.action, filled.status) == ("filled", "filled")

        captured: list[httpx.Request] = []

        async def accepted(request: httpx.Request) -> httpx.Response:
            captured.append(request)
            return httpx.Response(200, json={"code": 200})

        settings = load_bark_worker_settings(
            {
                "NEXUS_BARK_ENABLED": "true",
                "DATABASE_URL": migrated_database_url,
                "NEXUS_BARK_SERVER_URL": "http://127.0.0.1:8080",
                "NEXUS_BARK_DEVICE_KEY": "test-device-key",
                "NEXUS_BARK_ENCRYPTION_KEY": _BARK_ENCRYPTION_KEY.decode("ascii"),
                "NEXUS_BARK_ACTIVATED_AT": "2025-12-31T23:59:59Z",
            }
        )
        async with factory() as session:
            rejected_event = await session.scalar(
                select(OutboxEvent).where(OutboxEvent.event_type == "paper.order.risk_rejected")
            )
            filled_event = await session.scalar(
                select(OutboxEvent).where(
                    OutboxEvent.event_type == "paper.execution.advance.filled"
                )
            )
        assert rejected_event is not None
        assert filled_event is not None and filled_event.aggregate_id == seed.order_id
        async with httpx.AsyncClient(transport=httpx.MockTransport(accepted)) as client:
            assert (
                await run_bark_worker(
                    settings,
                    run_forever=False,
                    client=client,
                    clock=lambda: _OUTBOX_TIME,
                )
                == 1
            )
            assert (
                await run_bark_worker(
                    settings,
                    run_forever=False,
                    client=client,
                    clock=lambda: _OUTBOX_TIME,
                )
                == 1
            )

        notifications = tuple(_decrypted_bark_notification(request) for request in captured)
        assert notifications == (
            {
                "title": "[内部模拟] TST3 买入 1 股 · 风控拒绝",
                "body": (
                    "参考价：$60.00｜计划金额 $60.00\n"
                    "原因：订单金额低于最小金额 $100。\n"
                    "账户未变化：TST3 0 股｜可用现金 $10,000.00\n"
                    "Agent：暂不可用\n"
                    "失效：暂不可用"
                ),
                "group": "NEXUS",
                "id": str(rejected_event.aggregate_id),
            },
            {
                "title": "[内部模拟] TST1 买入 14 股 · 已成交",
                "body": (
                    "成交均价：$100.25｜成交额 $1,403.50｜费用 $0.00\n"
                    "成交后：TST1 14 股｜可用现金 $7,586.50\n"
                    "Agent：暂不可用\n"
                    "失效：暂不可用"
                ),
                "group": "NEXUS",
                "id": str(filled_event.aggregate_id),
            },
        )

        async with factory() as session:
            events = tuple(
                await session.scalars(
                    select(OutboxEvent)
                    .order_by(OutboxEvent.created_at, OutboxEvent.id)
                    .where(
                        OutboxEvent.event_type.in_(
                            (
                                "strategy_decision.persisted",
                                "paper.order_batch.evaluated",
                                "paper.order.submitted",
                                "paper.execution.advance.partially_filled",
                            )
                        )
                    )
                )
            )
        assert events
        assert all(
            (event.status, event.attempt_count, event.published_at) == ("queued", 0, None)
            for event in events
        )
    finally:
        await factory.kw["bind"].dispose()
