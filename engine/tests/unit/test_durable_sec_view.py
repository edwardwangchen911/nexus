import json
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from nexus.api.durable_sec import project_check, project_event, read_event_research_status
from nexus.api.routes.research import _research_wire
from nexus.api.schemas.agents import ResearchRunEnvelope, ResearchRunResponse
from nexus.api.schemas.durable_sec import DurableSecCollectionView, DurableSecView
from nexus.api.schemas.integrations import SuccessMetaResponse
from nexus.monitoring.durable_sec_contracts import DurableSecPolicy
from nexus.persistence.models.sec_daily import SecDurableDailyEventReceipt
from nexus.persistence.models.sec_monitor import SecDiscoveryCheck, SecDiscoveryEvent

NOW = datetime(2026, 11, 17, 4, tzinfo=UTC)


@pytest.mark.parametrize(
    "code,expected",
    [
        ("DURABLE_SEC_COVERAGE_INCOMPLETE", "coverage_incomplete"),
        ("DURABLE_SEC_COVERAGE_CONFLICT", "coverage_conflict"),
        ("DURABLE_SEC_SOURCE_STALE", "source_stale"),
        ("untrusted detail must not escape", "evidence_unverified"),
    ],
)
def test_saved_unknown_reason_is_preserved_as_a_safe_category(code, expected):
    details = {"unread_history_files": 1, "invalid_rows": 0, "coverage_start_known": True}
    row = SecDiscoveryCheck(
        scheduled_at=NOW,
        checked_at=NOW,
        created_at=NOW,
        result="unknown",
        coverage="partial",
        reason=code,
        observation={"coverage_details": details},
    )
    view = project_check(row, NOW)
    assert view.reason == expected
    assert view.coverage_details.model_dump() == details
    assert (view.result, view.coverage) == ("unknown", "partial")


def test_partial_positive_is_not_recast_as_complete_or_unknown():
    row = SecDiscoveryCheck(
        scheduled_at=NOW,
        checked_at=NOW,
        created_at=NOW,
        result="true",
        coverage="partial",
        reason=None,
    )
    view = project_check(row, NOW)
    assert (view.result, view.coverage, view.reason) == ("true", "partial", None)


def test_unread_history_details_cannot_be_presented_as_complete_coverage():
    row = SecDiscoveryCheck(
        scheduled_at=NOW,
        checked_at=NOW,
        created_at=NOW,
        result="true",
        coverage="complete",
        reason=None,
        observation={
            "coverage_details": {
                "unread_history_files": 1,
                "invalid_rows": 0,
                "coverage_start_known": True,
            }
        },
    )
    with pytest.raises(ValueError, match="DURABLE_SEC_VIEW_INVALID"):
        project_check(row, NOW)


@pytest.mark.parametrize(
    "change",
    [
        {"result": "false", "coverage": "partial"},
        {"checked_at": NOW + timedelta(seconds=1)},
        {"scheduled_at": NOW + timedelta(seconds=1)},
    ],
)
def test_bad_check_never_becomes_negative(change):
    values = dict(
        scheduled_at=NOW,
        checked_at=NOW,
        created_at=NOW,
        result="true",
        coverage="partial",
        reason=None,
    )
    with pytest.raises(ValueError):
        project_check(SecDiscoveryCheck(**(values | change)), NOW)


def event():
    policy = DurableSecPolicy(
        account_id=uuid4(),
        instrument_id=uuid4(),
        issuer_cik="0000123456",
        effective_at=NOW - timedelta(hours=2),
    )
    row = SecDiscoveryEvent(
        id=uuid4(),
        account_id=policy.account_id,
        instrument_id=policy.instrument_id,
        event_key="sec:0000123456:0000123456-26-000099",
        created_at=NOW,
        first_seen_at=NOW,
        entry=dict(
            accession_number="0000123456-26-000099",
            form="8-K",
            accepted_at=(NOW - timedelta(hours=1)).isoformat(),
            filed_at=NOW.replace(hour=0).isoformat(),
            primary_document="new.htm",
        ),
    )
    return row, policy


def test_first_seen_is_not_acceptance_and_conflict_stays_visible():
    row, policy = event()
    view = project_event(row, policy, NOW, conflict=True)
    assert view.accepted_at < view.first_seen_at
    assert view.metadata_status == "conflict"


def test_event_can_show_persisted_research_without_relabeling_discovery():
    row, policy = event()
    view = project_event(row, policy, NOW, conflict=True, research_status="increment_failed")
    assert view.event_id == row.id
    assert view.research_status == "increment_failed"
    assert view.metadata_status == "conflict" and view.notice is None


def test_event_id_is_only_sent_to_an_opted_in_reader():
    row, policy = event()
    view = DurableSecView(
        observed_at=NOW,
        rule_id=uuid4(),
        state="active",
        effective_at=policy.effective_at,
        forms=policy.forms,
        latest_check=None,
        event_count=1,
        events=(project_event(row, policy, NOW, conflict=False),),
        execution_locked=True,
        latest_collection=DurableSecCollectionView(
            scheduled_at=NOW, claimed_at=NOW, finished_at=NOW, status="failed"
        ),
    )
    envelope = ResearchRunEnvelope.model_construct(
        data=ResearchRunResponse.model_construct(durable_disclosure=view),
        meta=SuccessMetaResponse(request_id=uuid4(), as_of=NOW),
    )
    prior = _research_wire(envelope, "1", include_sec_notices=True, include_sec_research=True)
    prior_view = json.loads(prior.body)["data"]["durable_disclosure"]
    assert "event_id" not in prior_view["events"][0] and "latest_collection" not in prior_view
    current = _research_wire(envelope, "1", include_sec_event_ids=True, include_sec_collection=True)
    current_view = json.loads(current.body)["data"]["durable_disclosure"]
    assert current_view["latest_collection"]["status"] == "failed"
    entry = current_view["events"][0]
    assert entry["event_id"] == str(row.id)
    assert "notice" not in entry and "research_status" not in entry


@pytest.mark.asyncio
async def test_an_unaudited_daily_receipt_does_not_mark_an_event_addressed():
    row, _ = event()
    row.id = uuid4()
    receipt = SecDurableDailyEventReceipt(
        id=uuid4(), event_id=row.id, observed_at=NOW, created_at=NOW
    )
    session = Mock(
        scalar=AsyncMock(side_effect=[receipt, False]), scalars=AsyncMock(return_value=Mock())
    )
    session.scalars.return_value.all.return_value = []

    assert await read_event_research_status(session, row, NOW) == "unverified"
    assert session.scalar.await_count == 2


@pytest.mark.asyncio
async def test_research_status_cannot_be_read_before_event_visibility():
    row, _ = event()
    session = Mock()

    assert (
        await read_event_research_status(session, row, NOW - timedelta(seconds=1)) == "unverified"
    )
    assert not session.mock_calls


@pytest.mark.parametrize("failure", ["scope", "key", "old", "future"])
def test_event_scope_and_time_are_not_inferred(failure):
    row, policy = event()
    if failure == "scope":
        row.account_id = uuid4()
    elif failure == "key":
        row.event_key = "sec:0000000001:0000123456-26-000099"
    else:
        row.entry["accepted_at"] = (
            policy.effective_at if failure == "old" else NOW + timedelta(seconds=1)
        ).isoformat()
    with pytest.raises(ValueError):
        project_event(row, policy, NOW, conflict=False)
