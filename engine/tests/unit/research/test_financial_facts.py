from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.domain.evidence import EvidenceRef
from nexus.providers.sec import SecNormalizationError, canonical_fact_for_raw_tag
from nexus.research.financial_facts import (
    CANONICAL_SEC_FACTS,
    FinancialFact,
    FinancialFactSelectionError,
    select_financial_fact,
)

INSTRUMENT_ID = UUID("0198d500-1000-7000-8000-000000000014")
ACCEPTED = datetime(2026, 8, 20, 20, tzinfo=UTC)


def _fact(
    canonical_fact: str = "revenue",
    *,
    value: object = "100.00",
    unit: str = "USD",
    kind: str = "duration",
    period_start: date | None = date(2026, 4, 1),
    period_end: date = date(2026, 6, 30),
    accepted_at: datetime = ACCEPTED,
    revision: str = "1",
) -> FinancialFact:
    return FinancialFact(
        instrument_id=INSTRUMENT_ID,
        canonical_fact=canonical_fact,
        value=value,
        unit=unit,
        period_kind=kind,
        period_start=period_start if kind == "duration" else None,
        period_end=period_end,
        fiscal_year=2026,
        fiscal_period="Q2",
        form="10-Q",
        filed_at=accepted_at,
        accepted_at=accepted_at,
        accession_number=f"0000000000-26-{revision.zfill(6)}",
        raw_taxonomy="us-gaap",
        raw_tag="Revenues",
        frame="CY2026Q2",
        revision=revision,
        evidence_ref=EvidenceRef(
            source="sec",
            object_id=f"filing-{revision}",
            available_at=accepted_at,
            location=f"memory://sec/filing-{revision}.json",
            content_hash="a" * 64,
        ),
    )


@pytest.mark.parametrize("canonical_fact", sorted(CANONICAL_SEC_FACTS))
def test_canonical_sec_facts_preserve_exact_decimal_and_raw_tag(canonical_fact: str) -> None:
    fact = _fact(canonical_fact, value="123.4500")

    assert fact.canonical_fact == canonical_fact
    assert fact.value == Decimal("123.4500")
    assert fact.raw_taxonomy == "us-gaap"
    assert fact.raw_tag == "Revenues"


def test_financial_fact_rejects_binary_float() -> None:
    with pytest.raises(TypeError, match="binary floats"):
        _fact(value=100.0)  # type: ignore[arg-type]


def test_restated_fact_is_not_visible_before_acceptance() -> None:
    original_fact = _fact(accepted_at=ACCEPTED, revision="1")
    restated_fact = _fact(value="110.00", accepted_at=ACCEPTED + timedelta(days=7), revision="2")

    selected = select_financial_fact(
        [original_fact, restated_fact], "revenue", restated_fact.accepted_at - timedelta(seconds=1)
    )

    assert selected == original_fact


def test_selection_chooses_latest_accepted_revision_only_after_acceptance() -> None:
    original_fact = _fact(accepted_at=ACCEPTED, revision="1")
    restated_fact = _fact(value="110.00", accepted_at=ACCEPTED + timedelta(days=7), revision="2")

    assert (
        select_financial_fact([original_fact, restated_fact], "revenue", restated_fact.accepted_at)
        == restated_fact
    )


def test_selection_fails_closed_for_incompatible_units() -> None:
    with pytest.raises(FinancialFactSelectionError, match="INCOMPATIBLE_UNIT"):
        select_financial_fact(
            [_fact(unit="USD"), _fact(unit="shares", revision="2")], "revenue", ACCEPTED
        )


def test_selection_fails_closed_for_instant_duration_mixing() -> None:
    with pytest.raises(FinancialFactSelectionError, match="INCOMPATIBLE_PERIOD_KIND"):
        select_financial_fact(
            [_fact(), _fact(kind="instant", period_end=date(2026, 6, 30), revision="2")],
            "revenue",
            ACCEPTED,
        )


def test_selection_fails_closed_for_overlapping_duration_periods() -> None:
    with pytest.raises(FinancialFactSelectionError, match="OVERLAPPING_DURATION_PERIODS"):
        select_financial_fact(
            [
                _fact(period_start=date(2026, 1, 1), period_end=date(2026, 6, 30)),
                _fact(period_start=date(2026, 4, 1), period_end=date(2026, 9, 30), revision="2"),
            ],
            "revenue",
            ACCEPTED,
        )


def test_sec_tag_mapping_rejects_an_unknown_or_ambiguous_tag_instead_of_guessing() -> None:
    assert canonical_fact_for_raw_tag("us-gaap", "Revenues") == "revenue"

    with pytest.raises(SecNormalizationError, match="AMBIGUOUS_OR_UNKNOWN_TAG"):
        canonical_fact_for_raw_tag("us-gaap", "AssetsAndRevenues")
