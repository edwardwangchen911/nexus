from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from nexus.domain.evidence import EvidenceRef
from nexus.providers.corporate_actions.alpaca import (
    CorporateActionValidationError,
    normalize_alpaca_corporate_action,
)


def _evidence() -> EvidenceRef:
    return EvidenceRef(
        source="alpaca",
        object_id="ca-001",
        available_at=datetime(2026, 8, 24, tzinfo=UTC),
        location="memory://evidence/alpaca/ca-001.json",
        content_hash="b" * 64,
    )


def test_complete_alpaca_action_is_ledger_eligible_without_mutating_a_ledger() -> None:
    action = normalize_alpaca_corporate_action(
        {
            "id": "ca-001",
            "type": "cash_dividend",
            "status": "complete",
            "process_date": "2026-08-24",
            "ex_date": "2026-08-20",
            "record_date": "2026-08-21",
            "payable_date": "2026-08-28",
            "rate": "0.25",
            "revision": "2",
        },
        evidence_ref=_evidence(),
    )

    assert action.provider_event_id == "ca-001"
    assert action.rate == Decimal("0.25")
    assert action.process_date == date(2026, 8, 24)
    assert action.ledger_eligible is True


def test_non_complete_alpaca_action_is_not_ledger_eligible() -> None:
    action = normalize_alpaca_corporate_action(
        {
            "id": "ca-002",
            "type": "split",
            "status": "announced",
            "process_date": "2026-08-24",
            "revision": "1",
        },
        evidence_ref=_evidence(),
    )

    assert action.ledger_eligible is False


def test_alpaca_action_rejects_binary_float_rate() -> None:
    with pytest.raises(CorporateActionValidationError, match="decimal string"):
        normalize_alpaca_corporate_action(
            {
                "id": "ca-003",
                "type": "split",
                "status": "complete",
                "process_date": "2026-08-24",
                "rate": 2.0,
                "revision": "1",
            },
            evidence_ref=_evidence(),
        )
