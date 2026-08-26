from __future__ import annotations

import asyncio
from datetime import timedelta
from decimal import Decimal

import httpx
import pytest
from helpers.paper_api import app, headers, seed_ready_paper_api
from helpers.paper_performance import MARKET_CLOSE
from nexus.domain.ids import new_uuid7
from nexus.persistence.models.paper import (
    PaperDrawdownLockEventRecord,
    PaperOrderRecord,
)
from nexus.persistence.models.runtime import AuditEvent, IdempotencyRecord, OutboxEvent
from nexus.persistence.session import session_factory
from sqlalchemy import func, select


@pytest.mark.asyncio
async def test_drawdown_ack_appends_one_atomic_event_and_never_creates_an_order(
    migrated_database_url: str,
) -> None:
    seed = await seed_ready_paper_api(migrated_database_url)
    engine_app = app(migrated_database_url, now=MARKET_CLOSE + timedelta(minutes=3))
    factory = session_factory(engine_app.state.settings)
    async with factory.begin() as session:
        session.add_all(
            (
                PaperDrawdownLockEventRecord(
                    id=new_uuid7(),
                    account_id=seed.execution.account_id,
                    event_seq=2,
                    from_state="unlocked",
                    to_state="locked",
                    drawdown_ratio=Decimal("0.08"),
                    user_acknowledged=False,
                    reason_code="DRAWDOWN_LIMIT_REACHED",
                    occurred_at=MARKET_CLOSE + timedelta(minutes=1),
                    payload_hash="b" * 64,
                ),
                PaperDrawdownLockEventRecord(
                    id=new_uuid7(),
                    account_id=seed.execution.account_id,
                    event_seq=3,
                    from_state="locked",
                    to_state="recovery_eligible",
                    drawdown_ratio=Decimal("0.05"),
                    user_acknowledged=False,
                    reason_code="DRAWDOWN_RECOVERED",
                    occurred_at=MARKET_CLOSE + timedelta(minutes=2),
                    payload_hash="c" * 64,
                ),
            )
        )
    async with factory() as session:
        order_count_before = int(
            await session.scalar(select(func.count()).select_from(PaperOrderRecord)) or 0
        )

    path = "/internal/v1/risk/drawdown-lock/acknowledge"
    body = b'{"expected_event_seq":3}'
    action_headers = {
        **headers("POST", path, body),
        "Idempotency-Key": "ack-recovered-lock",
        "If-Match": '"3"',
    }
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=engine_app),
            base_url="http://testserver",
        ) as client:
            first, replay = await asyncio.gather(
                client.post(path, content=body, headers=action_headers),
                client.post(
                    path,
                    content=body,
                    headers={
                        **headers("POST", path, body),
                        "Idempotency-Key": "ack-recovered-lock",
                        "If-Match": '"3"',
                    },
                ),
            )
            stale = await client.post(
                path,
                content=body,
                headers={
                    **headers("POST", path, body),
                    "Idempotency-Key": "stale-ack",
                    "If-Match": '"3"',
                },
            )
        assert first.status_code == replay.status_code == 200
        assert first.headers["etag"] == replay.headers["etag"] == '"4"'
        assert {first.json()["data"]["state"], replay.json()["data"]["state"]} == {
            "unlocked"
        }
        assert first.json()["data"]["event_seq"] == replay.json()["data"]["event_seq"] == 4
        assert sorted(
            (first.json()["data"]["replayed"], replay.json()["data"]["replayed"])
        ) == [False, True]
        assert stale.status_code == 409
        assert stale.json()["error"]["code"] == "VERSION_CONFLICT"

        async with factory() as session:
            events = tuple(
                (
                    await session.scalars(
                        select(PaperDrawdownLockEventRecord).order_by(
                            PaperDrawdownLockEventRecord.event_seq
                        )
                    )
                ).all()
            )
            assert len(events) == 4
            assert events[-1].to_state == "unlocked"
            assert events[-1].user_acknowledged is True
            assert (
                int(await session.scalar(select(func.count()).select_from(PaperOrderRecord)) or 0)
                == order_count_before
            )
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(AuditEvent)
                    .where(AuditEvent.action == "drawdown.lock.acknowledged")
                )
                == 1
            )
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(OutboxEvent)
                    .where(OutboxEvent.event_type == "paper.drawdown_lock.acknowledged")
                )
                == 1
            )
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(IdempotencyRecord)
                    .where(IdempotencyRecord.idempotency_key == "ack-recovered-lock")
                )
                == 1
            )
    finally:
        await engine_app.state.session_factory.kw["bind"].dispose()
        await factory.kw["bind"].dispose()
