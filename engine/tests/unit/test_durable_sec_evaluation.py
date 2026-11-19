from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from nexus.monitoring.durable_sec_contracts import DurableSecPolicy
from nexus.monitoring.durable_sec_evaluation import evaluate_discovery, stable_event_key
from nexus.research.sec_disclosure_index import SecDisclosureIndex, SecIndexEntry
from pydantic import ValidationError

T0 = datetime(2026, 1, 5, 14, tzinfo=UTC)
CIK = "0000000001"


def policy(**changes):
    return DurableSecPolicy(
        account_id=UUID(int=1),
        instrument_id=UUID(int=2),
        issuer_cik=CIK,
        effective_at=T0,
        **changes,
    )


def entry(number=1, seconds=10):
    return SecIndexEntry(
        accession_number=f"0000000001-26-{number:06d}",
        form="8-K",
        accepted_at=T0 + timedelta(seconds=seconds),
        filed_at=T0.replace(hour=0),
        primary_document="filing.htm",
    )


def source(entries=(), complete=False, **changes):
    value = SecDisclosureIndex(
        instrument_id=UUID(int=2),
        identity_snapshot_id=UUID(int=3),
        issuer_cik=CIK,
        received_at=T0 + timedelta(seconds=100),
        source_url=f"https://data.sec.gov/submissions/CIK{CIK}.json",
        raw_content_hash="a" * 64,
        entries=entries,
        coverage_start_at=T0 - timedelta(days=1),
        history_complete=complete,
    )
    return value.model_copy(update=changes)


def evaluate(current, **changes):
    args = dict(
        policy=policy(),
        current=current,
        available_at=T0 + timedelta(seconds=101),
        checked_at=T0 + timedelta(seconds=102),
        known_entries={},
        legacy_event_keys=frozenset(),
    )
    args.update(changes)
    return evaluate_discovery(**args)


def test_partial_new_and_late_seen_remain_positive_without_last_poll_watermark():
    for checked in (102, 300, 400):
        result = evaluate(source((entry(),)), checked_at=T0 + timedelta(seconds=checked))
        assert (result.result, result.coverage) == ("true", "partial")
        assert result.events[0].classification == "new"
        assert result.events[0].entry.accepted_at == T0 + timedelta(seconds=10)
        assert result.interval_start == T0 and result.interval_end == T0 + timedelta(seconds=100)


@pytest.mark.parametrize("seconds", [-1, 0, 1])
def test_strict_effective_boundary(seconds):
    result = evaluate(source((entry(seconds=seconds),)))
    assert result.events[0].classification == ("new" if seconds > 0 else "historical")
    assert result.result == ("true" if seconds > 0 else "unknown")


@pytest.mark.parametrize("legacy", [False, True])
def test_known_or_legacy_never_new(legacy):
    item = entry()
    key = stable_event_key(CIK, item.accession_number)
    result = evaluate(
        source((item,), complete=True),
        known_entries={} if legacy else {key: item},
        legacy_event_keys=frozenset({key}) if legacy else frozenset(),
    )
    assert (result.result, result.coverage) == ("unknown", "partial")
    assert result.events[0].classification == "known"


def test_conflict_only_quarantines_its_event():
    old, new = entry(), entry(2)
    changed = old.model_copy(update={"primary_document": "revised.htm"})
    result = evaluate(
        source((changed, new), complete=True),
        known_entries={stable_event_key(CIK, old.accession_number): old},
    )
    assert (result.result, result.coverage) == ("true", "partial")
    assert [e.classification for e in result.events] == ["unknown", "new"]
    assert result.events[0].reason == "DURABLE_SEC_EVENT_CONFLICT"


@pytest.mark.parametrize(
    "complete,start",
    [(False, T0), (True, T0), (True, T0 + timedelta(seconds=1))],
)
def test_structural_coverage_does_not_prove_accepted_time_cutoff(complete, start):
    result = evaluate(source(complete=complete, coverage_start_at=start))
    assert (result.result, result.coverage) == ("unknown", "partial")
    assert result.reason == "DURABLE_SEC_ACCEPTED_CUTOFF_UNPROVEN"
    assert result.issuer_cik == CIK and result.forms == ("8-K", "8-K/A")


def test_complete_missing_known_entry_is_not_absence_proof():
    item = entry()
    result = evaluate(
        source(complete=True), known_entries={stable_event_key(CIK, item.accession_number): item}
    )
    assert result.result == "unknown" and result.reason == "DURABLE_SEC_COVERAGE_CONFLICT"


@pytest.mark.parametrize("state", ["paused", "revoked"])
def test_inactive_rule_has_no_new_effects(state):
    result = evaluate(source((entry(),)), rule_state=state)
    assert result.result == "unknown"
    assert all(e.classification != "new" for e in result.events)


@pytest.mark.parametrize(
    "changes",
    [
        {"instrument_id": UUID(int=9)},
        {"issuer_cik": "0000000009"},
        {"received_at": T0 + timedelta(seconds=103)},
        {"received_at": T0 - timedelta(seconds=300)},
        {"received_at": T0.replace(tzinfo=None)},
        {"source_url": "https://untrusted.invalid/secret"},
    ],
)
def test_invalid_shared_source_quarantines_all(changes):
    result = evaluate(source((entry(),), **changes))
    assert result.result == "unknown" and result.coverage == "unknown"
    assert all(e.classification != "new" for e in result.events)
    assert "secret" not in result.model_dump_json()


@pytest.mark.parametrize(
    "changes",
    [
        {"available_at": T0 + timedelta(seconds=99)},
        {"available_at": T0 + timedelta(seconds=103)},
        {"checked_at": T0.replace(tzinfo=None)},
        {"checked_at": T0 - timedelta(seconds=1)},
    ],
)
def test_bad_read_time_is_unknown(changes):
    assert evaluate(source((entry(),)), **changes).result == "unknown"


@pytest.mark.parametrize("age,expected", [(300, "true"), (301, "unknown")])
def test_source_age_not_filing_age(age, expected):
    assert (
        evaluate(source((entry(),)), checked_at=T0 + timedelta(seconds=100 + age)).result
        == expected
    )


def test_future_event_is_unknown_but_other_new_stays_positive():
    result = evaluate(source().model_copy(update={"entries": (entry(seconds=101), entry(2))}))
    assert result.result == "true"
    assert [e.classification for e in result.events] == ["unknown", "new"]


def test_filtered_form_is_not_a_new_effect():
    result = evaluate(
        source((entry().model_copy(update={"form": "8-K/A"}),), complete=True),
        policy=policy(forms=("8-K",)),
    )
    assert result.result == "unknown" and result.events[0].classification == "out_of_scope"


def test_invalid_prior_binding_fails_closed():
    result = evaluate(source((entry(),)), known_entries={"wrong": entry()})
    assert result.result == "unknown" and result.coverage == "unknown"


def test_policy_and_result_are_frozen_and_strict():
    with pytest.raises(ValidationError):
        policy(forms=("8-K", "8-K"))
    with pytest.raises(ValidationError):
        policy(forms=())
    with pytest.raises(ValidationError):
        policy().effective_at = T0
    result = evaluate(source())
    with pytest.raises(ValidationError):
        result.result = "false"


def test_late_discovery_after_many_partial_polls_still_uses_original_activation():
    received = T0 + timedelta(days=30)
    current = source((entry(),), received_at=received)
    result = evaluate(current, available_at=received, checked_at=received)
    assert result.result == "true" and result.events[0].classification == "new"
    assert result.interval_start == T0 and result.interval_end == received


def test_conflict_without_independent_new_never_false_even_with_complete_coverage():
    old = entry()
    changed = old.model_copy(update={"primary_document": "changed.htm"})
    result = evaluate(
        source((changed,), complete=True),
        known_entries={stable_event_key(CIK, old.accession_number): old},
    )
    assert result.result == "unknown" and result.coverage == "partial"
    assert result.events[0].classification == "unknown"


def test_historical_discovery_does_not_become_new_publication():
    result = evaluate(source((entry(seconds=-1),), complete=True))
    assert result.result == "unknown"
    assert result.events[0].classification == "historical"
    assert result.events[0].entry.accepted_at < result.interval_start


def test_late_event_before_previous_capture_without_shards_is_still_new():
    prior = evaluate(source(complete=True))
    received = T0 + timedelta(minutes=6)
    result = evaluate(
        source((entry(),), complete=True, received_at=received),
        available_at=received,
        checked_at=received,
    )
    assert (prior.result, prior.coverage) == ("unknown", "partial")
    assert (result.result, result.coverage) == ("true", "partial")
    assert result.events[0].entry.accepted_at < prior.interval_end
    assert result.reason is None


def test_duplicate_shared_accession_invalidates_source():
    item = entry()
    result = evaluate(source().model_copy(update={"entries": (item, item)}))
    assert result.result == "unknown" and result.coverage == "unknown"
    assert result.events == ()


def test_pure_evaluation_does_not_modify_prior_facts():
    item = entry()
    known = {stable_event_key(CIK, item.accession_number): item}
    before = dict(known)
    current = source((item, entry(2)))
    original = current.model_dump_json()
    evaluate(current, known_entries=known)
    assert known == before and current.model_dump_json() == original
