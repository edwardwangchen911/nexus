"""Authority resolver contract; DB readers remain separately covered by PostgreSQL tests."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest
from nexus.paper.economic_budget_hold import HoldRequest, PairHold, _require_new_transfer_authority
from nexus.paper.portfolio_economic_preflight import compile_plan
from nexus.persistence.models.agents import AgentModelConfigRecord

from engine.tests.unit.paper.test_economic_receiver import receiver


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        None,
        "missing_protocol",
        "future_day",
        "missing_binding",
        "wrong_candidate",
        "wrong_model_hash",
        "missing_model",
        "expired_price",
        "expired_binding_price",
        "wrong_cost",
        "changed_model_hash",
        "changed_model_version",
    ],
)
async def test_new_transfer_resolves_frozen_authorities_before_budget_write(monkeypatch, fault):
    from nexus.agents import paper_context
    from nexus.paper import economic_version_binding, portfolio_economic_protocol

    plan, binding, allocation, run = receiver()
    plan = compile_plan(
        plan.request.model_copy(
            update={"research_cost_usd": Decimal(0), "synthesis_cost_usd": Decimal(0)}
        ),
        now=datetime(2026, 10, 8, tzinfo=UTC),
    )
    binding = binding.model_copy(
        update={"protocol_hash": plan.content_hash, "configured_cost_usd": Decimal(0)}
    )
    now = plan.slots[0].decision_at
    hold = PairHold(
        request=HoldRequest(
            evaluation_key=plan.request.evaluation_key,
            protocol_hash=plan.content_hash,
            slot_ordinal=1,
            research_cost_usd="0",
            synthesis_cost_usd="0",
            global_limit_usd="20",
            uncovered_formal_usd="0",
        )
    )
    model = AgentModelConfigRecord(
        id=binding.shared_model_id,
        config_hash=binding.shared_model_hash,
        version=binding.shared_model_version,
        max_input_tokens=96000,
        max_output_tokens=8192,
        input_usd_per_million=Decimal(0),
        output_usd_per_million=Decimal(0),
        created_at=now - timedelta(days=1),
        pricing_available_at=now - timedelta(days=1),
        pricing_expires_at=now + timedelta(days=1),
    )
    observed = now + timedelta(days=1) if fault == "future_day" else now
    if fault == "wrong_candidate":
        run.instrument_id = plan.request.instruments[1].instrument_id
    if fault == "wrong_model_hash":
        run.model_hash = "0" * 64
    if fault == "expired_price":
        model.pricing_expires_at = now
    if fault == "expired_binding_price":
        binding = binding.model_copy(update={"pricing_expires_at": now})
    if fault == "wrong_cost":
        model.input_usd_per_million = Decimal(10)
    if fault == "changed_model_hash":
        model.config_hash = "0" * 64
    if fault == "changed_model_version":
        model.version = "older"
    session = AsyncMock()
    session.get.return_value = None if fault == "missing_model" else model
    protocol_reader = AsyncMock(
        return_value=("registered", None, None if fault == "missing_protocol" else plan)
    )
    binding_reader = AsyncMock(
        return_value=("bound", None if fault == "missing_binding" else binding)
    )
    monkeypatch.setattr(portfolio_economic_protocol, "read_protocol", protocol_reader)
    monkeypatch.setattr(economic_version_binding, "read_binding", binding_reader)
    # This legacy receiver has no Paper context; bound-cycle authority is covered by PG tests.
    context_reader = AsyncMock(return_value=None)
    monkeypatch.setattr(paper_context, "read_agent_paper_research_context", context_reader)
    if fault is None:
        await _require_new_transfer_authority(
            session, hold=hold, allocation=allocation, run=run, now=observed
        )
        protocol_reader.assert_awaited_once_with(session, as_of=now)
        binding_reader.assert_awaited_once_with(session, as_of=now)
        session.get.assert_awaited_once_with(AgentModelConfigRecord, run.model_config_id)
        context_reader.assert_awaited_once_with(
            session,
            snapshot_id=run.data_snapshot_id,
            instrument_id=run.instrument_id,
            as_of=run.decision_time,
        )
    else:
        with pytest.raises(ValueError, match="ECONOMIC_TRANSFER_"):
            await _require_new_transfer_authority(
                session, hold=hold, allocation=allocation, run=run, now=observed
            )
    session.add.assert_not_called()
    session.flush.assert_not_awaited()
