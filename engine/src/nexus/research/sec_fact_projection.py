"""Append-only planning for newly recognized facts in existing SEC evidence."""

from collections.abc import Mapping, Sequence

from nexus.persistence.models.research import FinancialFactRecord
from nexus.research.financial_facts import FinancialFact

_KEY_FIELDS = (
    "instrument_id",
    "canonical_fact",
    "period_kind",
    "period_start",
    "period_end",
    "unit",
    "accession_number",
    "revision",
)


def missing_fact_projections(
    facts: Sequence[FinancialFact],
    existing: Sequence[FinancialFactRecord],
) -> list[dict[str, object]]:
    """Validate every row before returning additions; never supply a historic clock."""

    def key(data: Mapping[str, object]) -> tuple[object, ...]:
        return tuple(data[field] for field in _KEY_FIELDS)

    incoming: dict[tuple[object, ...], dict[str, object]] = {}
    for fact in facts:
        data = fact.model_dump(exclude={"evidence_ref"})
        row_key = key(data)
        prior = incoming.setdefault(row_key, data)
        if prior != data:
            raise ValueError("SEC_FACT_PROJECTION_CONFLICT")
    present = set()
    for record in existing:
        row_key = tuple(getattr(record, field) for field in _KEY_FIELDS)
        expected = incoming.get(row_key)
        if expected is None or any(
            getattr(record, field) != value for field, value in expected.items()
        ):
            raise ValueError("SEC_FACT_PROJECTION_CONFLICT")
        present.add(row_key)
    return [data for row_key, data in incoming.items() if row_key not in present]
