"""Frozen policy tests for paired Agent-version evaluation."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.paper.agent_version_models import (
    MAX_DRAWDOWN_DEGRADATION,
    MIN_ACTION_DIVERGENCES,
    MIN_CHALLENGER_FILLS,
    MIN_EXCESS_RETURN,
    MIN_POSITIVE_EXCESS_SESSIONS,
    OOS_SLOT_COUNT,
    POLICY_VERSION,
    TRAIN_SLOT_COUNT,
    AgentVersionEvaluationRequest,
    agent_version_market_evidence_hash,
    evaluation_policy_hash,
    evaluation_request_hash,
)
from pydantic import ValidationError

INCUMBENT_RUN_ID = UUID("71000000-0000-0000-0000-000000000001")
CHALLENGER_PROMPT_ID = UUID("71000000-0000-0000-0000-000000000002")
MODEL_ID = UUID("71000000-0000-0000-0000-000000000003")
INSTRUMENT_ID = UUID("71000000-0000-0000-0000-000000000004")


def _slot(day: int, minute: int = 0) -> datetime:
    return datetime(2026, 10, day, 14, minute, tzinfo=UTC)


TRAIN_SLOTS = (_slot(1), _slot(1, 5), _slot(2), _slot(2, 5))
OOS_SLOTS = tuple(
    slot
    for day in (5, 6, 7, 8, 9, 12, 13, 14, 15, 16)
    for slot in (_slot(day), _slot(day, 5))
)


def _request(**changes: object) -> AgentVersionEvaluationRequest:
    values: dict[str, object] = {
        "instrument_id": INSTRUMENT_ID,
        "incumbent_version_source_run_id": INCUMBENT_RUN_ID,
        "challenger_prompt_version_id": CHALLENGER_PROMPT_ID,
        "challenger_model_config_id": MODEL_ID,
        "initial_capital": Decimal("10000.00"),
        "train_slots": TRAIN_SLOTS,
        "oos_slots": OOS_SLOTS,
    }
    values.update(changes)
    return AgentVersionEvaluationRequest.model_validate(values)


def test_v1_policy_is_frozen() -> None:
    assert POLICY_VERSION == "agent_version_evaluation_v1"
    assert TRAIN_SLOT_COUNT == 4
    assert OOS_SLOT_COUNT == 20
    assert MIN_ACTION_DIVERGENCES == 3
    assert MIN_CHALLENGER_FILLS == 1
    assert MIN_EXCESS_RETURN == Decimal("0.005")
    assert MIN_POSITIVE_EXCESS_SESSIONS == 7
    assert MAX_DRAWDOWN_DEGRADATION == Decimal("0.0025")
    assert evaluation_policy_hash() == (
        "7af8c4c83a82b3c2c493c674a5efcb4e1c1a2480fb73ee09576edc6eb7313661"
    )


def test_request_hash_is_stable_for_decimal_and_utc_slots() -> None:
    request = _request()

    assert evaluation_request_hash(request) == evaluation_request_hash(
        request.model_copy(update={"initial_capital": Decimal("10000.000")})
    )
    assert len(evaluation_request_hash(request)) == 64


def test_market_evidence_hash_is_account_neutral_and_domain_separated() -> None:
    assert agent_version_market_evidence_hash("a" * 64) == (
        "297387ed7a079d973bc7e37938f69c3f9136cc38c385b12135d181b665885a88"
    )
    with pytest.raises(ValueError, match="AGENT_VERSION_MARKET_EVIDENCE_INVALID"):
        agent_version_market_evidence_hash("not-a-hash")


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"train_slots": TRAIN_SLOTS[:-1]}, "AGENT_VERSION_WINDOW_INVALID"),
        ({"oos_slots": OOS_SLOTS[:-1]}, "AGENT_VERSION_WINDOW_INVALID"),
        (
            {"oos_slots": TRAIN_SLOTS + OOS_SLOTS[4:]},
            "AGENT_VERSION_WINDOW_INVALID",
        ),
        (
            {"oos_slots": tuple(reversed(OOS_SLOTS))},
            "AGENT_VERSION_WINDOW_INVALID",
        ),
        (
            {"initial_capital": Decimal("0")},
            "AGENT_VERSION_CAPITAL_INVALID",
        ),
    ],
)
def test_v1_policy_rejects_invalid_requests(
    change: dict[str, object], reason: str
) -> None:
    with pytest.raises((ValidationError, ValueError), match=reason):
        _request(**change)


def test_v1_policy_rejects_more_than_two_oos_slots_per_session() -> None:
    invalid = list(OOS_SLOTS)
    invalid[2] = _slot(5, 10)

    with pytest.raises((ValidationError, ValueError), match="AGENT_VERSION_WINDOW_INVALID"):
        _request(oos_slots=tuple(sorted(invalid)))


def test_v1_policy_rejects_non_utc_slots() -> None:
    invalid = list(TRAIN_SLOTS)
    invalid[0] = invalid[0].replace(tzinfo=None)

    with pytest.raises((ValidationError, ValueError), match="AGENT_VERSION_WINDOW_INVALID"):
        _request(train_slots=tuple(invalid))


def test_v1_policy_rejects_train_and_oos_on_same_new_york_session() -> None:
    train_slots = (
        datetime(2026, 9, 30, 14, 0, tzinfo=UTC),
        datetime(2026, 9, 30, 14, 5, tzinfo=UTC),
        datetime(2026, 10, 1, 14, 0, tzinfo=UTC),
        datetime(2026, 10, 1, 14, 5, tzinfo=UTC),
    )
    oos_slots = tuple(
        slot
        for day in (1, 2, 5, 6, 7, 8, 9, 12, 13, 14)
        for slot in (_slot(day, 10), _slot(day, 15))
    )

    with pytest.raises((ValidationError, ValueError), match="AGENT_VERSION_WINDOW_INVALID"):
        _request(train_slots=train_slots, oos_slots=oos_slots)
