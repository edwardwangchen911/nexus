from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.notifications.durable_sec_gate import DurableSecPublisher, notice_claim_condition
from nexus.outbox.models import OutboxMessage
from nexus.persistence.models.runtime import OutboxEvent
from sqlalchemy import select
from sqlalchemy.dialects import postgresql


class Session:
    def __init__(self, row):
        self.scalars = AsyncMock(side_effect=[[], []])
        self.scalar = AsyncMock(side_effect=["read committed", row])

    @asynccontextmanager
    async def begin(self):
        yield self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False


def message():
    return OutboxMessage(
        event_id=UUID(int=1),
        aggregate_id=UUID(int=2),
        event_type="old",
        schema_version="old_v1",
        payload={"old": "unchanged"},
    )


@pytest.mark.asyncio
async def test_send_recheck_rejection_does_not_call_transport():
    publish = AsyncMock()
    wrapper = DurableSecPublisher(lambda: Session(None), publish)
    with pytest.raises(ValueError, match="DURABLE_SEC_NOTICE_NOT_ACTIVE"):
        await wrapper(message())
    publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_legacy_unaffected_payload_is_forwarded_unchanged():
    original = message()
    row = SimpleNamespace(
        event_type=original.event_type,
        schema_version=original.schema_version,
        aggregate_id=original.aggregate_id,
        payload=original.payload,
    )
    publish = AsyncMock()
    await DurableSecPublisher(lambda: Session(row), publish)(original)
    publish.assert_awaited_once_with(original)


def test_filter_is_correlated_to_the_selected_outbox():
    statement = select(OutboxEvent).where(notice_claim_condition())
    sql = str(statement.compile(dialect=postgresql.dialect()))
    assert "sec_notification_claims.outbox_id = outbox_events.id" in sql
    assert "sec_monitor_rule_states.sequence DESC" in sql
    assert "sec_monitor_rule_states.state" in sql
    assert "legacy_trigger_id IS NULL" in sql


@pytest.mark.asyncio
async def test_stale_snapshot_isolation_never_sends():
    session = Session(None)
    session.scalar = AsyncMock(return_value="repeatable read")
    publish = AsyncMock()
    with pytest.raises(ValueError, match="DURABLE_SEC_NOTICE_ISOLATION_INVALID"):
        await DurableSecPublisher(lambda: session, publish)(message())
    publish.assert_not_awaited()
