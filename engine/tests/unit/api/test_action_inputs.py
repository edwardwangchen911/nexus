"""Bounded display never rounds canonical observations or expands huge exponents."""

from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.api.action_inputs import project_action_observation
from nexus.api.schemas.agents import ResearchInputsSummary
from nexus.research.alpaca_actions import (
    ActionEventObservation,
    ActionPageReference,
    AlpacaActionObservationDocument,
)


def document(*, rates=(Decimal("0.123456789012345678901"),), unbound=0):
    received = datetime(2026, 9, 2, 4, 1, tzinfo=UTC)
    return AlpacaActionObservationDocument(
        instrument_id=UUID(int=1),
        symbol="TEST",
        query_start=date(2026, 8, 1),
        query_end=date(2026, 8, 9),
        available_at=received,
        manifest_content_hash="a" * 64,
        manifest_byte_length=100,
        pages=(
            ActionPageReference(
                page_number=1,
                raw_content_hash="b" * 64,
                raw_byte_length=100,
                receipt_content_hash="c" * 64,
                receipt_byte_length=100,
                received_at=received,
            ),
        ),
        events=tuple(
            ActionEventObservation(
                category="cash_dividends",
                provider_event_id=f"synthetic-{index}",
                page_number=1,
                index=index,
                symbol="TEST",
                rate=rate,
                currency="",
            )
            for index, rate in enumerate(rates)
        ),
        total_event_count=len(rates) + unbound,
        unbound_event_count=unbound,
    )


def project(value):
    return project_action_observation(
        value, evidence_ref_id=UUID(int=2), revision="d" * 64, content_hash="d" * 64
    )


@pytest.mark.parametrize(
    ("raw", "expected", "unavailable"),
    [
        (None, None, ()),
        (Decimal("0"), "0", ()),
        (Decimal("0E+40"), "0", ()),
        (Decimal("-0E+999999999"), "-0", ()),
        (Decimal("0E-999999999"), "0", ()),
        (Decimal("0.000001"), "0.000001", ()),
        (Decimal("1.23456789012345678901"), "1.23456789012345678901", ()),
        (Decimal("1E+3"), "1000", ()),
        (Decimal("1E+39"), "1" + "0" * 39, ()),
        (Decimal("1E-38"), "0." + "0" * 37 + "1", ()),
        (Decimal("-1E+38"), "-1" + "0" * 38, ()),
        (Decimal("1E+40"), None, ("rate",)),
        (Decimal("-1E-38"), None, ("rate",)),
        (Decimal("1E+999999999"), None, ("rate",)),
        (Decimal("1E-999999999"), None, ("rate",)),
    ],
)
def test_numeric_display_boundaries_preserve_underlying_value(raw, expected, unavailable):
    original = document(rates=(raw,))
    before = original.canonical_bytes()
    event = project(original).events[0]
    assert event.rate == expected
    assert event.unavailable_numeric_fields == unavailable
    assert event.currency == "" and event.ex_date is None
    assert original.canonical_bytes() == before


def test_display_is_bounded_and_retains_source_and_readonly_semantics():
    result = project(document(rates=(Decimal("0"),) * 35, unbound=2))
    assert len(result.events) == 32 and result.truncated
    assert result.bound_event_count == 35 and result.total_event_count == 37
    assert result.unbound_event_count == 2 and result.page_count == 1
    assert result.source == "alpaca_action_observation"
    assert result.source_completeness == "unknown"
    assert result.available_at_semantics == "maximum_page_receipt_not_historical_availability"
    assert result.revision_semantics == "local_content_hash"
    assert result.execution_locked and not result.ledger_eligible
    assert not result.historical_backtest_eligible
    assert result.events[0].provider_event_id == "synthetic-0"


def test_unbound_only_is_not_a_no_events_claim():
    result = project(document(rates=(), unbound=2))
    assert not result.events and result.bound_event_count == 0
    assert result.total_event_count == result.unbound_event_count == 2
    assert result.source_completeness == "unknown" and not result.truncated


def test_old_summary_remains_readable_without_action_field():
    summary = ResearchInputsSummary(
        display_symbol="TEST", filings=(), facts=(), total_facts=0, truncated=False
    )
    assert summary.corporate_action_observations == ()
