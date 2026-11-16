"""Pure control-flow tests; real PostgreSQL constraints require the integration suite."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from nexus.monitoring import durable_sec_claims as claims
from nexus.persistence.models.monitoring import MonitorIncrement
from nexus.persistence.models.sec_monitor import SecAutoIncrementDayClaim, SecNotificationClaim

NOW = datetime(2026, 1, 5, 15, tzinfo=UTC)
ACCOUNT, INSTRUMENT, OUTBOX, TRIGGER, CALENDAR = (UUID(int=i) for i in range(1, 6))
KEY = "sec:0000000001:0000000001-26-000001"


def session():
    return SimpleNamespace(
        scalar=AsyncMock(),
        get=AsyncMock(return_value=None),
        add=Mock(),
        flush=AsyncMock(),
        execute=AsyncMock(),
    )


async def notice(s):
    return await claims.claim_notice(
        s,
        account_id=ACCOUNT,
        instrument_id=INSTRUMENT,
        event_key=KEY,
        outbox_id=OUTBOX,
        legacy_trigger_id=TRIGGER,
        now=NOW,
    )


def member():
    return MonitorIncrement(
        id=UUID(int=6),
        account_id=ACCOUNT,
        instrument_id=INSTRUMENT,
        run_id=UUID(int=7),
        research_as_of=NOW,
        session_date=NOW.date(),
    )


@pytest.mark.asyncio
async def test_no_rule_preserves_old_notice_and_day_contract(monkeypatch):
    monkeypatch.setattr(claims, "_rule_state", AsyncMock(return_value=(None, False)))
    s = session()
    assert await notice(s)
    assert (
        await claims.claim_increment_day(s, member(), calendar_authority_id=CALENDAR, now=NOW)
        is None
    )
    s.add.assert_not_called()
    s.scalar.assert_not_called()


@pytest.mark.asyncio
async def test_inactive_rule_blocks_effects(monkeypatch):
    monkeypatch.setattr(claims, "_rule_state", AsyncMock(return_value=(object(), False)))
    s = session()
    assert not await notice(s)
    with pytest.raises(ValueError, match="DURABLE_SEC_RULE_INACTIVE"):
        await claims.claim_increment_day(s, member(), calendar_authority_id=CALENDAR, now=NOW)
    s.add.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("same", [False, True])
async def test_notice_existing_identity_is_exact(monkeypatch, same):
    monkeypatch.setattr(claims, "_rule_state", AsyncMock(return_value=(object(), True)))
    s = session()
    s.scalar.return_value = SecNotificationClaim(
        outbox_id=OUTBOX if same else UUID(int=99), legacy_trigger_id=TRIGGER
    )
    assert await notice(s) is same
    s.add.assert_not_called()


@pytest.mark.asyncio
async def test_legacy_trigger_already_occupies_notice_even_without_backfill(monkeypatch):
    monkeypatch.setattr(claims, "_rule_state", AsyncMock(return_value=(object(), True)))
    s = session()
    s.scalar.side_effect = [None, TRIGGER]
    assert not await notice(s)
    s.add.assert_not_called()


@pytest.mark.asyncio
async def test_new_notice_is_audited_before_return(monkeypatch):
    monkeypatch.setattr(claims, "_rule_state", AsyncMock(return_value=(object(), True)))
    audit = AsyncMock()
    monkeypatch.setattr(claims, "audit_fact", audit)
    s = session()
    s.scalar.side_effect = [None, None, SimpleNamespace(id=UUID(int=10)), None]
    assert await notice(s)
    row = s.add.call_args.args[0]
    assert (row.event_key, row.outbox_id, row.legacy_trigger_id) == (KEY, OUTBOX, TRIGGER)
    assert row.created_at == NOW
    audit.assert_awaited_once_with(s, row)


def day_setup(monkeypatch):
    monkeypatch.setattr(claims, "_rule_state", AsyncMock(return_value=(object(), True)))
    calendar = SimpleNamespace(
        session=NOW.date(),
        market_open_at=NOW - timedelta(minutes=30),
        market_close_at=NOW + timedelta(hours=6),
    )
    loader = AsyncMock(return_value=calendar)
    monkeypatch.setattr(claims, "load_agent_paper_service_calendar_authority", loader)
    audit = AsyncMock()
    monkeypatch.setattr(claims, "audit_fact", audit)
    return calendar, audit


@pytest.mark.asyncio
async def test_new_day_claim_binds_original_member_and_calendar(monkeypatch):
    _, audit = day_setup(monkeypatch)
    s = session()
    s.scalar.return_value = None
    item = member()
    row = await claims.claim_increment_day(s, item, calendar_authority_id=CALENDAR, now=NOW)
    assert row.increment_id == item.id and row.run_id == item.run_id
    assert row.session_date == NOW.date() and row.calendar_authority_id == CALENDAR
    assert row.created_at == NOW
    audit.assert_awaited_once_with(s, row)


@pytest.mark.asyncio
@pytest.mark.parametrize("same", [False, True])
async def test_day_claim_never_reuses_for_another_run(monkeypatch, same):
    day_setup(monkeypatch)
    s = session()
    item = member()
    old = SecAutoIncrementDayClaim(
        increment_id=item.id,
        run_id=item.run_id if same else UUID(int=99),
        calendar_authority_id=CALENDAR,
    )
    s.get.return_value = old
    if same:
        assert (
            await claims.claim_increment_day(s, item, calendar_authority_id=CALENDAR, now=NOW)
            is old
        )
    else:
        with pytest.raises(ValueError, match="DURABLE_SEC_DAY_ALREADY_CLAIMED"):
            await claims.claim_increment_day(s, item, calendar_authority_id=CALENDAR, now=NOW)
    s.add.assert_not_called()


@pytest.mark.asyncio
async def test_preexisting_unmigrated_increment_blocks_new_day(monkeypatch):
    day_setup(monkeypatch)
    s = session()
    s.scalar.return_value = UUID(int=99)
    with pytest.raises(ValueError, match="DURABLE_SEC_DAY_ALREADY_CLAIMED"):
        await claims.claim_increment_day(s, member(), calendar_authority_id=CALENDAR, now=NOW)
    s.add.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["day", "close", "future"])
async def test_bad_session_never_allocates(monkeypatch, fault):
    calendar, _ = day_setup(monkeypatch)
    s = session()
    item = member()
    if fault == "day":
        item.session_date -= timedelta(days=1)
    if fault == "close":
        calendar.market_close_at = NOW
    if fault == "future":
        item.research_as_of = NOW + timedelta(seconds=1)
    with pytest.raises(ValueError, match="DURABLE_SEC_SESSION_INVALID"):
        await claims.claim_increment_day(s, item, calendar_authority_id=CALENDAR, now=NOW)
    s.add.assert_not_called()


@pytest.mark.asyncio
async def test_audit_uses_database_json_binding_and_database_created_at(monkeypatch):
    s = session()
    row = SecNotificationClaim(id=UUID(int=8))
    binding = {"id": str(row.id), "exact": "sql representation"}
    s.execute.return_value = SimpleNamespace(
        one_or_none=lambda: SimpleNamespace(binding=binding, created_at=NOW)
    )
    append = AsyncMock()
    monkeypatch.setattr(claims, "append_audit_event", append)
    await claims.audit_fact(s, row)
    assert "to_jsonb(f)-'created_at'" in str(s.execute.call_args.args[0])
    assert append.call_args.kwargs["payload"] == {
        "table": "sec_notification_claims",
        "row_id": str(row.id),
        "binding": binding,
    }
    assert append.call_args.kwargs["occurred_at"] == NOW


@pytest.mark.asyncio
@pytest.mark.parametrize("conflict", [False, True])
async def test_unproven_or_conflicted_durable_event_cannot_notice(monkeypatch, conflict):
    monkeypatch.setattr(claims, "_rule_state", AsyncMock(return_value=(object(), True)))
    s = session()
    s.scalar.side_effect = (
        [None, None, SimpleNamespace(id=UUID(int=10)), UUID(int=11)]
        if conflict
        else [None, None, None]
    )
    assert not await notice(s)
    s.add.assert_not_called()
