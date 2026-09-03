import asyncio
from datetime import UTC, date, datetime, timedelta, timezone
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.risk.authority import (
    RiskAuthorityInput,
    load_risk_market_snapshot,
    prepare_risk_market_snapshot,
)
from test_authority import _decision


def request(**changes):
    values = dict(
        decision_session=date(2026, 8, 21),
        execution_session=date(2026, 8, 24),
        decision_time=datetime(2026, 8, 21, 21, tzinfo=UTC),
        source_policy_id=UUID(int=1),
        calendar_authority_id=UUID(int=2),
        universe_snapshot_id=UUID(int=3),
        quality_report_id=UUID(int=4),
    )
    return RiskAuthorityInput(**(values | changes))


@pytest.mark.parametrize("ids", [(), (UUID(int=1), UUID(int=1))])
def test_prepare_rejects_empty_or_duplicate_set_before_database(ids):
    session = AsyncMock()
    with pytest.raises(ValueError, match="nonempty and unique"):
        asyncio.run(
            prepare_risk_market_snapshot(session, authority=request(), required_instrument_ids=ids)
        )
    session.execute.assert_not_called()


@pytest.mark.parametrize(
    "time",
    [datetime(2026, 8, 21, 21), datetime(2026, 8, 21, 21, tzinfo=timezone(timedelta(hours=8)))],
)
def test_prepare_input_requires_utc(time):
    with pytest.raises(ValueError, match="UTC"):
        request(decision_time=time)


def test_prepare_input_is_frozen_and_has_no_decision_or_hash():
    value = request()
    assert "authority_hash" not in RiskAuthorityInput.model_fields
    with pytest.raises(ValueError):
        value.quality_report_id = UUID(int=5)
    with pytest.raises(ValueError):
        request(skip_validation=True)


def test_existing_loader_checks_targets_before_database():
    session = AsyncMock()
    with pytest.raises(ValueError, match="omit decision targets"):
        asyncio.run(
            load_risk_market_snapshot(
                session, decision=_decision(), required_instrument_ids=(UUID(int=999),)
            )
        )
    session.execute.assert_not_called()
    session.get.assert_not_called()
