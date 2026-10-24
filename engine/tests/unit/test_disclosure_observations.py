from asyncio import CancelledError
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from nexus.monitoring.contracts import DisclosureReviewCondition
from nexus.monitoring.disclosure_observations import (
    DisclosureObservation,
    read_disclosure_observation,
)
from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError
from unit.monitoring.test_disclosure_contracts import condition


def fields():
    return dict(
        instrument_id=uuid4(),
        plan_id=uuid4(),
        baseline_snapshot_id=uuid4(),
        baseline_hash="a" * 64,
        current_snapshot_id=None,
        current_hash=None,
        issuer_cik="0000123456",
        baseline_as_of=datetime(2026, 10, 9, tzinfo=UTC),
        received_at=None,
        available_at=None,
        result="unknown",
        reason="SEC_BASELINE_INVALID",
        history_coverage="unknown",
        added=(),
    )


def test_observation_is_strict_frozen_and_unknown_has_no_added_facts():
    values = fields()
    result = DisclosureObservation(**values)
    assert DisclosureObservation.model_validate_json(result.model_dump_json()) == result
    with pytest.raises(ValidationError):
        result.reason = "other"
    for update in (
        {"instrument_id": str(values["instrument_id"])},
        {"result": "false", "reason": None},
        {"reason": None},
        {"current_hash": "b" * 64},
        {"baseline_as_of": datetime(2026, 10, 9)},
    ):
        with pytest.raises(ValidationError):
            DisclosureObservation(**{**values, **update})


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [SQLAlchemyError("private detail"), CancelledError()])
async def test_database_failure_and_cancellation_propagate(error):
    class Session:
        async def get(self, *args):
            raise error

    typed = DisclosureReviewCondition.model_validate(condition())
    plan = SimpleNamespace(
        kind="disclosure",
        baseline_snapshot_id=uuid4(),
        baseline_as_of=datetime(2026, 10, 9, tzinfo=UTC),
        condition=typed.model_dump(mode="json"),
        condition_hash=typed.semantic_hash,
        source_output_id=uuid4(),
    )
    with pytest.raises(type(error)) as raised:
        await read_disclosure_observation(Session(), plan=plan, now=datetime.now(UTC))
    assert raised.value is error
