import inspect
from datetime import UTC, datetime, timedelta, timezone
from uuid import UUID

import pytest
from nexus.paper.errors import PaperBatchError
from nexus.paper.service import (
    canonical_paper_batch_request_hash,
    create_paper_order_batch,
)


def test_public_batch_request_has_no_caller_supplied_order_fields() -> None:
    parameters = inspect.signature(create_paper_order_batch).parameters

    assert tuple(parameters) == (
        "session",
        "account_id",
        "strategy_decision_id",
        "idempotency_key",
        "request_time",
        "target_version_id",
        "hooks",
    )
    assert not {
        "instrument_id",
        "target_weight",
        "side",
        "quantity",
        "price",
        "risk_outcome",
    } & set(parameters)


def test_request_hash_is_stable_and_requires_utc() -> None:
    values = {
        "account_id": UUID(int=1),
        "strategy_decision_id": UUID(int=2),
        "request_time": datetime(2026, 8, 26, 21, tzinfo=UTC),
    }

    assert canonical_paper_batch_request_hash(**values) == (
        canonical_paper_batch_request_hash(**values)
    )
    with pytest.raises(PaperBatchError) as captured:
        canonical_paper_batch_request_hash(
            **{
                **values,
                "request_time": datetime(
                    2026,
                    8,
                    27,
                    5,
                    tzinfo=timezone(timedelta(hours=8)),
                ),
            }
        )
    assert captured.value.code == "STRATEGY_DECISION_INVALID"


def test_stable_errors_expose_only_safe_messages() -> None:
    error = PaperBatchError("RISK_AUTHORITY_INCOMPLETE")

    assert error.code == "RISK_AUTHORITY_INCOMPLETE"
    assert "SQL" not in error.safe_message
    assert "provider" not in error.safe_message.lower()
    assert "account" not in error.safe_message.lower()
