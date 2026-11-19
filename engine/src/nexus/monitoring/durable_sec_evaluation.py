"""Evaluate trusted SEC index readback without I/O, effects, clock reads or mutable state."""

from collections.abc import Mapping
from datetime import datetime, timedelta

from nexus.domain.time import require_utc
from nexus.monitoring.durable_sec_contracts import (
    DiscoveryEvent,
    DiscoveryReason,
    DiscoveryResult,
    DurableSecPolicy,
    RuleState,
)
from nexus.research.sec_disclosure_index import SecDisclosureIndex, SecIndexEntry


def stable_event_key(issuer_cik: str, accession_number: str) -> str:
    return f"sec:{issuer_cik}:{accession_number}"


def evaluate_discovery(
    policy: DurableSecPolicy,
    current: SecDisclosureIndex,
    available_at: datetime,
    checked_at: datetime,
    known_entries: Mapping[str, SecIndexEntry],
    legacy_event_keys: frozenset[str],
    rule_state: RuleState = "active",
) -> DiscoveryResult:
    """New means accepted strictly after rule activation, never merely newly downloaded.

    Callers must verify original bytes/hash before this pure layer. A capture receipt
    provides no complete acceptance cutoff. New effects require acceptance after
    rule activation, regardless of any previous capture or research generation.
    """

    def unknown(reason: DiscoveryReason) -> DiscoveryResult:
        return DiscoveryResult(
            account_id=policy.account_id,
            instrument_id=policy.instrument_id,
            issuer_cik=policy.issuer_cik,
            forms=policy.forms,
            result="unknown",
            coverage="unknown",
            interval_start=None,
            interval_end=None,
            reason=reason,
        )

    if rule_state != "active":
        return unknown("DURABLE_SEC_RULE_INACTIVE")
    if (current.instrument_id, current.issuer_cik, current.source_url) != (
        policy.instrument_id,
        policy.issuer_cik,
        f"https://data.sec.gov/submissions/CIK{policy.issuer_cik}.json",
    ):
        return unknown("DURABLE_SEC_SOURCE_INVALID")
    try:
        now, available, received, effective = (
            require_utc(checked_at),
            require_utc(available_at),
            require_utc(current.received_at),
            require_utc(policy.effective_at),
        )
        coverage_start = (
            require_utc(current.coverage_start_at)
            if current.coverage_start_at is not None
            else None
        )
    except (ValueError, TypeError, AttributeError):
        return unknown("DURABLE_SEC_TIME_INVALID")
    if not effective <= received <= available <= now or (
        coverage_start is not None and coverage_start > received
    ):
        return unknown("DURABLE_SEC_TIME_INVALID")
    if now - received > timedelta(seconds=300):
        return unknown("DURABLE_SEC_SOURCE_STALE")
    if any(
        key != stable_event_key(policy.issuer_cik, item.accession_number)
        for key, item in known_entries.items()
    ):
        return unknown("DURABLE_SEC_HISTORY_INVALID")
    # A duplicate accession in one shared index is a malformed source, not two events.
    if len({item.accession_number for item in current.entries}) != len(current.entries):
        return unknown("DURABLE_SEC_SOURCE_INVALID")

    events: list[DiscoveryEvent] = []
    contradiction = False
    for item in current.entries:
        key = stable_event_key(policy.issuer_cik, item.accession_number)
        reason: DiscoveryReason | None = None
        try:
            accepted, filed = require_utc(item.accepted_at), require_utc(item.filed_at)
            time_invalid = max(accepted, filed) > received or (
                coverage_start is not None and accepted < coverage_start
            )
        except (ValueError, TypeError, AttributeError):
            time_invalid = True
            accepted = effective
        if time_invalid:
            classification = "unknown"
            reason = "DURABLE_SEC_EVENT_TIME_INVALID"
            contradiction = True
        elif key in known_entries and known_entries[key] != item:
            classification = "unknown"
            reason = "DURABLE_SEC_EVENT_CONFLICT"
            contradiction = True
        elif item.form not in policy.forms:
            classification = "out_of_scope"
        elif key in known_entries or key in legacy_event_keys:
            classification = "known"
        elif accepted <= effective:
            classification = "historical"
        else:
            classification = "new"
        events.append(
            DiscoveryEvent.model_validate(
                dict(
                    event_key=key,
                    entry=item,
                    classification=classification,
                    reason=reason,
                )
            )
        )

    present = {event.event_key for event in events}
    if current.history_complete and any(
        item.form in policy.forms
        and effective <= item.accepted_at <= received
        and key not in present
        for key, item in known_entries.items()
    ):
        contradiction = True
    has_new = any(event.classification == "new" for event in events)
    return DiscoveryResult(
        account_id=policy.account_id,
        instrument_id=policy.instrument_id,
        issuer_cik=policy.issuer_cik,
        forms=policy.forms,
        result="true" if has_new else "unknown",
        coverage="partial",
        interval_start=effective,
        interval_end=received,
        events=tuple(events),
        reason=(
            "DURABLE_SEC_COVERAGE_CONFLICT"
            if contradiction
            else None
            if has_new
            else "DURABLE_SEC_ACCEPTED_CUTOFF_UNPROVEN"
        ),
    )
