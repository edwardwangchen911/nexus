from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from nexus.api.durable_sec import project_check, project_event
from nexus.monitoring.durable_sec_contracts import DurableSecPolicy
from nexus.persistence.models.sec_monitor import SecDiscoveryCheck, SecDiscoveryEvent

NOW = datetime(2026, 11, 17, 4, tzinfo=UTC)


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
