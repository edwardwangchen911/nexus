"""Market refresh has one finite, source-only envelope."""

import pytest


def test_market_preparation_entrypoint_exists():
    from nexus.ops.monitor_market_prepare import (
        MARKET_PREPARATION_SECONDS,
        prepare_batch_market_inputs,
    )

    assert MARKET_PREPARATION_SECONDS == 300
    assert callable(prepare_batch_market_inputs)


@pytest.mark.asyncio
async def test_small_batch_cannot_hide_stock_overflow(monkeypatch):
    from datetime import UTC, datetime
    from uuid import uuid4

    from nexus.ops import monitor_market_prepare as market
    from nexus.persistence.models.monitoring import MonitorObservationBatch
    from nexus.settings import EngineSettings

    account, batch_id, instrument, calendar = [uuid4() for _ in range(4)]
    batch = MonitorObservationBatch(
        id=batch_id, account_id=account, instrument_id=instrument, trigger_ids=[str(uuid4())] * 3
    )

    class Session:
        async def get(self, *args):
            return batch

        async def scalar(self, statement):
            return 33

    async def unlocked(*args, **kwargs):
        pass

    monkeypatch.setattr(market, "lock_security", unlocked)
    settings = EngineSettings(
        agent_paper_account_id=account,
        agent_paper_cycle_enabled=True,
        agent_paper_candidate_batch_id=uuid4(),
        agent_paper_formal_model_config_id=uuid4(),
    )
    with pytest.raises(ValueError, match="MONITOR_STOCK_PENDING_CAPACITY"):
        await market._preflight(
            Session(),
            settings,
            account_id=account,
            batch_id=batch_id,
            calendar_authority_id=calendar,
            now=datetime.now(UTC),
        )
