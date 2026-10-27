import base64
import json
from datetime import UTC, datetime

import httpx
import pytest
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from helpers.paper_batch import seed_paper_batch
from helpers.paper_execution import seed_paper_execution
from nexus.domain.ids import new_uuid7
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
async def test_bark_worker_acknowledges_legacy_terminal_event_without_transport_or_retry(
    migrated_database_url: str,
) -> None:
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        legacy_event_id = new_uuid7()
        async with sessions.begin() as session:
            session.add(
                OutboxEvent(
                    id=legacy_event_id,
                    event_type="paper.order.risk_rejected",
                    aggregate_id=new_uuid7(),
                    schema_version="paper_event_v1",
                    payload={"legacy_order_id": "not-a-notification-snapshot"},
                    published_at=None,
                )
            )

        requests: list[httpx.Request] = []

        async def accepted(request: httpx.Request) -> httpx.Response:
            requests.append(request)
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

        async with sessions() as session:
            event = await session.get(OutboxEvent, legacy_event_id)
            attempt = await session.scalar(
                select(OutboxDeliveryAttempt).where(
                    OutboxDeliveryAttempt.outbox_event_id == legacy_event_id
                )
            )

        assert requests == []
        assert event is not None
        assert (event.status, event.attempt_count, event.last_error_code) == (
            "published",
            1,
            None,
        )
        assert attempt is not None
        assert (attempt.outcome, attempt.error_code) == ("published", None)
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


@pytest.mark.asyncio
async def test_diagnostic_one_identity_one_encrypted_delivery_no_business(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from nexus.notifications.diagnostic import (
        enqueue_diagnostic_notification,
        read_diagnostic_notification,
    )
    from nexus.persistence.models.agents import (
        AgentDailyBudgetReservationRecord,
        AgentResearchRunRecord,
    )
    from nexus.persistence.models.monitoring import MonitorTrigger
    from nexus.persistence.models.runtime import AuditEvent, Job

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    test_id = new_uuid7()
    activation = datetime(2020, 1, 1, tzinfo=UTC)
    try:
        async with factory.begin() as session:
            first = await enqueue_diagnostic_notification(
                session, test_id=test_id, activated_at=activation
            )
        async with factory.begin() as session:
            second = await enqueue_diagnostic_notification(
                session, test_id=test_id, activated_at=activation
            )
        assert first == second and first["status"] == "queued"
        settings = load_bark_worker_settings(
            {
                "NEXUS_BARK_ENABLED": "true",
                "DATABASE_URL": migrated_database_url,
                "NEXUS_BARK_DEVICE_KEY": "test-device-key",
                "NEXUS_BARK_ENCRYPTION_KEY": "0123456789abcdef",
                "NEXUS_BARK_ACTIVATED_AT": activation.isoformat(),
            }
        )
        notices = []

        async def accepted(request: httpx.Request) -> httpx.Response:
            notices.append(_decrypted_bark_notification(request))
            return httpx.Response(200, json={"code": 200})

        async with httpx.AsyncClient(transport=httpx.MockTransport(accepted)) as client:
            assert await run_bark_worker(settings, run_forever=False, client=client) == 1
            assert await run_bark_worker(settings, run_forever=False, client=client) == 0
        async with factory.begin() as session:
            receipt = await read_diagnostic_notification(session, test_id=test_id)
            assert receipt["status"] == "provider_accepted"
            assert receipt["device_delivery"] == "unverified"
            assert receipt["attempt_count"] == 1
            assert receipt["failure"] is None
            assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 1
            assert (
                await session.scalar(select(func.count()).select_from(OutboxDeliveryAttempt)) == 1
            )
            assert await session.scalar(select(func.count()).select_from(AuditEvent)) == 1
            for model in (
                Job,
                AgentResearchRunRecord,
                AgentDailyBudgetReservationRecord,
                MonitorTrigger,
                PaperOrderBatchRecord,
            ):
                assert await session.scalar(select(func.count()).select_from(model)) == 0
        assert len(notices) == 1
        assert notices[0]["title"] == "NEXUS 测试通知｜非投资预警"
        assert notices[0]["body"].startswith("NEXUS 测试通知｜非投资预警")
        assert "url" not in notices[0]
        from nexus.ops import notification_test

        monkeypatch.setattr(notification_test, "_validate_local_env", lambda root, path: path)
        monkeypatch.setattr(
            notification_test,
            "load_agent_paper_service_environment",
            lambda *args, **kwargs: {"DATABASE_URL": "invalid"},
        )
        # Preflight cannot read the persisted published receipt. It must not deny its history.
        import asyncio

        assert (
            await asyncio.to_thread(notification_test.main, ["read", "--test-id", str(test_id)])
            == 1
        )
        failed_read = json.loads(capsys.readouterr().out)
        assert failed_read["status"] == "unknown"
        assert failed_read["operation"] == "read" and failed_read["operation_status"] == "rejected"
        assert "submission" not in failed_read
        assert failed_read["next_action"] == "read_same_test_id"
        async with factory.begin() as session:
            assert await read_diagnostic_notification(session, test_id=test_id) == receipt
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_diagnostic_timeout_is_unknown_and_never_redelivered(
    migrated_database_url: str,
) -> None:
    from nexus.notifications.diagnostic import (
        enqueue_diagnostic_notification,
        read_diagnostic_notification,
    )

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    test_id = new_uuid7()
    activation = datetime(2020, 1, 1, tzinfo=UTC)
    calls = []
    try:
        async with factory.begin() as session:
            await enqueue_diagnostic_notification(session, test_id=test_id, activated_at=activation)
        settings = load_bark_worker_settings(
            {
                "NEXUS_BARK_ENABLED": "true",
                "DATABASE_URL": migrated_database_url,
                "NEXUS_BARK_DEVICE_KEY": "test-device-key",
                "NEXUS_BARK_ENCRYPTION_KEY": "0123456789abcdef",
                "NEXUS_BARK_ACTIVATED_AT": activation.isoformat(),
            }
        )

        async def timeout(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            raise httpx.ReadTimeout("private upstream", request=request)

        async with httpx.AsyncClient(transport=httpx.MockTransport(timeout)) as client:
            assert await run_bark_worker(settings, run_forever=False, client=client) == 1
            assert await run_bark_worker(settings, run_forever=False, client=client) == 0
        async with factory.begin() as session:
            receipt = await read_diagnostic_notification(session, test_id=test_id)
            assert receipt["status"] == "unknown"
            assert receipt["attempt_count"] == 1
            assert receipt["failure"] == {"stage": "transport", "reason": "READ_TIMEOUT"}
            from nexus.outbox.failure import FAILURE_ACTION
            from nexus.persistence.models.runtime import AuditEvent

            facts = list(
                await session.scalars(select(AuditEvent).where(AuditEvent.action == FAILURE_ACTION))
            )
            assert len(facts) == 1
            assert facts[0].payload == {
                "event_id": str(test_id),
                "attempt_no": 1,
                "stage": "transport",
                "reason": "READ_TIMEOUT",
            }
            assert "private" not in facts[0].canonical_event
            assert (
                await enqueue_diagnostic_notification(
                    session, test_id=test_id, activated_at=activation
                )
                == receipt
            )
            row = await session.get(OutboxEvent, test_id)
            assert row is not None and row.max_attempts == 1 and row.status == "dead_letter"
        assert len(calls) == 1
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_diagnostic_normal_entry_concurrent_id_and_disabled_read(
    migrated_database_url: str,
) -> None:
    import asyncio

    from nexus.ops.notification_test import run
    from nexus.persistence.models.runtime import AuditEvent

    test_id = new_uuid7()
    env = {
        "DATABASE_URL": migrated_database_url,
        "NEXUS_BARK_ENABLED": "true",
        "NEXUS_BARK_DEVICE_KEY": "test-device-key",
        "NEXUS_BARK_ENCRYPTION_KEY": "0123456789abcdef",
        "NEXUS_BARK_ACTIVATED_AT": "2020-01-01T00:00:00Z",
    }
    first, second = await asyncio.gather(
        run(env, action="enqueue", test_id=test_id), run(env, action="enqueue", test_id=test_id)
    )
    assert first == second and first["status"] == "queued"
    disabled = {"DATABASE_URL": migrated_database_url}
    assert await run(disabled, action="read", test_id=test_id) == first
    assert await run(disabled, action="enqueue", test_id=test_id) == first
    with pytest.raises(ValueError, match="DIAGNOSTIC_NOTIFICATION_DISABLED"):
        await run(disabled, action="enqueue", test_id=new_uuid7())
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async with factory.begin() as session:
            assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 1
            assert await session.scalar(select(func.count()).select_from(AuditEvent)) == 1
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_diagnostic_enqueue_rollback_preserves_no_partial_audit(
    migrated_database_url: str,
) -> None:
    from nexus.notifications.diagnostic import (
        enqueue_diagnostic_notification,
        read_diagnostic_notification,
    )
    from nexus.persistence.models.runtime import AuditEvent

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    test_id = new_uuid7()
    try:
        with pytest.raises(RuntimeError, match="abort"):
            async with factory.begin() as session:
                await enqueue_diagnostic_notification(
                    session, test_id=test_id, activated_at=datetime(2020, 1, 1, tzinfo=UTC)
                )
                raise RuntimeError("abort")
        async with factory.begin() as session:
            assert (await read_diagnostic_notification(session, test_id=test_id))[
                "status"
            ] == "not_found"
            assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 0
            assert await session.scalar(select(func.count()).select_from(AuditEvent)) == 0
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("lose_ack", [False, True])
async def test_diagnostic_claim_before_send_and_expired_lease_never_retries(
    migrated_database_url: str, lose_ack: bool
) -> None:
    import asyncio
    from datetime import timedelta

    from nexus.notifications.bark import BarkPublisher
    from nexus.notifications.diagnostic import (
        enqueue_diagnostic_notification,
        read_diagnostic_notification,
    )
    from nexus.notifications.worker import build_bark_registry
    from nexus.outbox.dispatcher import dispatch_outbox_once
    from nexus.outbox.models import OutboxMessage

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    test_id = new_uuid7()
    calls = []
    entered = asyncio.Event()
    release = asyncio.Event()
    try:
        async with factory.begin() as session:
            await enqueue_diagnostic_notification(
                session, test_id=test_id, activated_at=datetime(2020, 1, 1, tzinfo=UTC)
            )
        start = datetime.now(UTC)

        async def accepted(request: httpx.Request) -> httpx.Response:
            async with factory.begin() as session:
                row = await session.get(OutboxEvent, test_id)
                attempt = await session.scalar(
                    select(OutboxDeliveryAttempt).where(
                        OutboxDeliveryAttempt.outbox_event_id == test_id
                    )
                )
                assert row is not None and row.attempt_count == 1 and row.status == "delivering"
                assert attempt is not None and attempt.outcome == "delivering"
            calls.append(_decrypted_bark_notification(request))
            entered.set()
            await release.wait()
            return httpx.Response(200, json={"code": 200})

        async def lost_ack(message: OutboxMessage) -> None:
            raise RuntimeError("lost persistence after accepted")

        async with httpx.AsyncClient(transport=httpx.MockTransport(accepted)) as client:
            registry = build_bark_registry(
                BarkPublisher(
                    client=client,
                    server_url="http://127.0.0.1:8080",
                    device_key="test-device-key",
                    encryption_key=_BARK_ENCRYPTION_KEY,
                )
            )
            first = asyncio.create_task(
                dispatch_outbox_once(
                    factory,
                    registry=registry,
                    worker_id="first",
                    clock=lambda: start,
                    after_publish=lost_ack if lose_ack else None,
                    claim_registered_only=True,
                )
            )
            await asyncio.wait_for(entered.wait(), timeout=10)
            assert not await dispatch_outbox_once(
                factory,
                registry=registry,
                worker_id="second",
                clock=lambda: start,
                claim_registered_only=True,
            )
            release.set()
            if lose_ack:
                with pytest.raises(RuntimeError, match="lost persistence"):
                    await first
            else:
                assert await first
            assert not await dispatch_outbox_once(
                factory,
                registry=registry,
                worker_id="after-restart",
                clock=lambda: start + timedelta(seconds=61),
                claim_registered_only=True,
            )
        async with factory.begin() as session:
            receipt = await read_diagnostic_notification(session, test_id=test_id)
            assert receipt["status"] == ("unknown" if lose_ack else "provider_accepted")
            assert receipt["attempt_count"] == 1
        assert len(calls) == 1
    finally:
        release.set()
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_frozen_legacy_diagnostic_failure_keeps_unknown_and_never_replays(
    migrated_database_url: str,
) -> None:
    """Sanitized shape of the frozen one-attempt 2026-10-27 result, not a new send."""
    from nexus.notifications.diagnostic import (
        enqueue_diagnostic_notification,
        read_diagnostic_notification,
    )
    from nexus.persistence.models.runtime import AuditEvent

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    test_id = new_uuid7()
    activation = datetime(2020, 1, 1, tzinfo=UTC)
    try:
        async with factory.begin() as session:
            await enqueue_diagnostic_notification(session, test_id=test_id, activated_at=activation)
            event = await session.get(OutboxEvent, test_id)
            assert event is not None
            start = event.created_at
        from nexus.outbox.dispatcher import dispatch_outbox_once
        from nexus.outbox.models import OutboxMessage, PublisherSpec
        from nexus.outbox.registry import PublisherRegistry

        async def legacy_failure(message: OutboxMessage) -> None:
            raise RuntimeError("legacy failure has no structured evidence")

        assert await dispatch_outbox_once(
            factory,
            worker_id="frozen-test-consumer",
            clock=lambda: start,
            registry=PublisherRegistry(
                [
                    PublisherSpec(
                        "diagnostic.notification.test",
                        "diagnostic_notification_v1",
                        True,
                        legacy_failure,
                    )
                ]
            ),
        )
        settings = load_bark_worker_settings(
            {
                "NEXUS_BARK_ENABLED": "true",
                "DATABASE_URL": migrated_database_url,
                "NEXUS_BARK_DEVICE_KEY": "test-device-key",
                "NEXUS_BARK_ENCRYPTION_KEY": "0123456789abcdef",
                "NEXUS_BARK_ACTIVATED_AT": activation.isoformat(),
            }
        )
        calls = []

        def forbidden(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            raise AssertionError("old notification must never be sent")

        async with httpx.AsyncClient(transport=httpx.MockTransport(forbidden)) as client:
            assert await run_bark_worker(settings, run_forever=False, client=client) == 0
        async with factory.begin() as session:
            first = await read_diagnostic_notification(session, test_id=test_id)
            assert first["status"] == "unknown" and first["failure"] is None
            assert first["device_delivery"] == "unverified" and first["attempt_count"] == 1
            assert await read_diagnostic_notification(session, test_id=test_id) == first
            assert await session.scalar(select(func.count()).select_from(AuditEvent)) == 1
            assert (
                await session.scalar(select(func.count()).select_from(OutboxDeliveryAttempt)) == 1
            )
            event = await session.get(OutboxEvent, test_id)
            assert event is not None and event.last_error_code == "PUBLISHER_FAILED"
            assert event.max_attempts == 1 and event.status == "dead_letter"
        assert calls == []
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("audit_rollback", [False, True])
async def test_failure_fact_cannot_survive_rollback_or_late_expired_claim(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
    audit_rollback: bool,
) -> None:
    from datetime import timedelta

    from nexus.notifications.diagnostic import (
        enqueue_diagnostic_notification,
        read_diagnostic_notification,
    )
    from nexus.outbox.failure import FAILURE_ACTION
    from nexus.persistence.models.runtime import AuditEvent

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    test_id = new_uuid7()
    activation = datetime(2020, 1, 1, tzinfo=UTC)
    try:
        async with factory.begin() as session:
            await enqueue_diagnostic_notification(session, test_id=test_id, activated_at=activation)
            event = await session.get(OutboxEvent, test_id)
            assert event is not None
            start = event.created_at
        settings = load_bark_worker_settings(
            {
                "NEXUS_BARK_ENABLED": "true",
                "DATABASE_URL": migrated_database_url,
                "NEXUS_BARK_DEVICE_KEY": "test-device-key",
                "NEXUS_BARK_ENCRYPTION_KEY": "0123456789abcdef",
                "NEXUS_BARK_ACTIVATED_AT": activation.isoformat(),
            }
        )
        calls = []

        def timeout(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            raise httpx.ReadTimeout("private transport text", request=request)

        async def failed_audit(*args: object, **kwargs: object) -> None:
            raise RuntimeError("audit transaction rejected")

        if audit_rollback:
            monkeypatch.setattr("nexus.outbox.dispatcher.append_audit_event", failed_audit)
        moments = iter([start, start if audit_rollback else start + timedelta(seconds=61)])
        async with httpx.AsyncClient(transport=httpx.MockTransport(timeout)) as client:
            if audit_rollback:
                with pytest.raises(RuntimeError, match="audit transaction rejected"):
                    await run_bark_worker(
                        settings, run_forever=False, client=client, clock=lambda: next(moments)
                    )
            else:
                assert (
                    await run_bark_worker(
                        settings, run_forever=False, client=client, clock=lambda: next(moments)
                    )
                    == 1
                )
            async with factory.begin() as session:
                event = await session.get(OutboxEvent, test_id)
                assert (
                    event is not None and event.status == "delivering" and event.attempt_count == 1
                )
                assert (
                    await session.scalar(
                        select(func.count())
                        .select_from(AuditEvent)
                        .where(AuditEvent.action == FAILURE_ACTION)
                    )
                    == 0
                )
            assert (
                await run_bark_worker(
                    settings,
                    run_forever=False,
                    client=client,
                    clock=lambda: start + timedelta(seconds=62),
                )
                == 0
            )
        async with factory.begin() as session:
            result = await read_diagnostic_notification(session, test_id=test_id)
            assert result["status"] == "unknown" and result["failure"] is None
            event = await session.get(OutboxEvent, test_id)
            assert event is not None and event.status == "dead_letter" and event.max_attempts == 1
            assert event.last_error_code == "LEASE_EXPIRED"
            assert await session.scalar(select(func.count()).select_from(AuditEvent)) == 1
        assert len(calls) == 1
    finally:
        await factory.kw["bind"].dispose()
