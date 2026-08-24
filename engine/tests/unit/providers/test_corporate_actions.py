import json
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from nexus.domain.evidence import EvidenceRef
from nexus.providers.corporate_actions.alpaca import (
    CorporateActionValidationError,
    normalize_alpaca_corporate_action,
)

FIXTURE = Path(__file__).resolve().parents[2] / "fixtures" / "corporate_actions" / "alpaca.json"


def _evidence(object_id: str = "ca-001") -> EvidenceRef:
    digest = "b" * 64
    return EvidenceRef(
        source="alpaca",
        object_id=object_id,
        available_at=datetime(2026, 8, 24, tzinfo=UTC),
        location=f"memory://sha256/{digest}",
        content_hash=digest,
    )


def test_complete_alpaca_action_is_ledger_eligible_without_mutating_a_ledger() -> None:
    action = normalize_alpaca_corporate_action(
        json.loads(FIXTURE.read_text())[0],
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
        evidence_ref=EvidenceRef(
            source="alpaca",
            object_id="ca-002",
            available_at=datetime(2026, 8, 24, tzinfo=UTC),
            location=f"memory://sha256/{'b' * 64}",
            content_hash="b" * 64,
        ),
    )

    assert action.ledger_eligible is False


def test_alpaca_action_rejects_evidence_owned_by_a_different_event() -> None:
    with pytest.raises(CorporateActionValidationError, match="EVIDENCE_OWNER_MISMATCH"):
        normalize_alpaca_corporate_action(
            {
                "id": "ca-002",
                "type": "split",
                "status": "announced",
                "process_date": "2026-08-24",
                "revision": "1",
            },
            evidence_ref=_evidence(),
        )


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
            evidence_ref=_evidence("ca-003"),
        )
