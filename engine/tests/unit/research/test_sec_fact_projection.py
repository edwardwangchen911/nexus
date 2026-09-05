"""The append plan cannot overwrite old facts or arbitrarily choose a source basis."""

import importlib
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.domain.evidence import EvidenceRef
from nexus.persistence.models.research import FinancialFactRecord
from nexus.research.financial_facts import FinancialFact


def source_fact():
    return FinancialFact(
        instrument_id=UUID("00000000-0000-4000-8000-000000000001"),
        canonical_fact="revenue",
        value=Decimal("123.456"),
        unit="USD",
        period_kind="duration",
        period_start=date(2026, 4, 1),
        period_end=date(2026, 6, 30),
        fiscal_year=2026,
        fiscal_period="Q2",
        form="10-Q",
        filed_at=datetime(2026, 8, 21, tzinfo=UTC),
        accepted_at=datetime(2026, 8, 20, 20, tzinfo=UTC),
        accession_number="0000000001-26-000001",
        raw_taxonomy="us-gaap",
        raw_tag="RevenueFromContractWithCustomerExcludingAssessedTax",
        frame=None,
        revision="0000000001-26-000001@2026-08-20T20:00:00+00:00",
        evidence_ref=EvidenceRef(
            source="sec",
            object_id="0000000001-26-000001",
            available_at=datetime(2026, 8, 20, 20, tzinfo=UTC),
            location="memory://sha256/" + "a" * 64,
            content_hash="a" * 64,
        ),
    )


def plan(facts, existing):
    module = importlib.import_module("nexus.research.sec_fact_projection")
    return module.missing_fact_projections(facts, existing)


def test_missing_projection_has_no_backdated_creation_clock():
    fact = source_fact()
    pending = plan([fact], [])
    assert len(pending) == 1
    assert pending[0] == fact.model_dump(exclude={"evidence_ref"})
    assert "created_at" not in pending[0]


def test_identical_projection_replay_is_empty_and_does_not_mutate_record():
    fact = source_fact()
    row = FinancialFactRecord(**fact.model_dump(exclude={"evidence_ref"}))
    before = row.value, row.raw_tag, row.revision
    assert plan([fact], [row]) == []
    assert (row.value, row.raw_tag, row.revision) == before


@pytest.mark.parametrize(
    "column,value",
    [
        ("value", Decimal("999")),
        ("raw_tag", "Revenues"),
        ("raw_taxonomy", "custom"),
        ("frame", "unexpected"),
    ],
)
def test_existing_conflicting_projection_is_not_overwritten(column, value):
    fact = source_fact()
    data = fact.model_dump(exclude={"evidence_ref"})
    data[column] = value
    row = FinancialFactRecord(**data)
    with pytest.raises(ValueError, match="SEC_FACT_PROJECTION_CONFLICT"):
        plan([fact], [row])
    assert getattr(row, column) == value


def test_conflicting_incoming_rows_do_not_choose_first_or_last_value():
    fact = source_fact()
    other = fact.model_copy(update={"value": Decimal("999")})
    for facts in ([fact, other], [other, fact]):
        with pytest.raises(ValueError, match="SEC_FACT_PROJECTION_CONFLICT"):
            plan(facts, [])
