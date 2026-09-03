"""Synthetic upstream facts survive the actual provider and protected DB path."""

from copy import deepcopy
from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from helpers.broker_seed import ACCOUNT_ID, NOW, PORTFOLIO_ID, _seed_account_and_portfolio
from helpers.broker_snapshot import _snapshot, _sync_result
from nexus.persistence.models.portfolio import BrokerCashFlow, BrokerSyncAttempt
from nexus.persistence.session import session_factory
from nexus.providers.broker.fosun import FosunBrokerProvider
from nexus.providers.broker.gateway_contracts import GatewayResponsePage
from nexus.reconciliation.cash_flows import CashFlowSyncResult, fetch_cash_flows
from nexus.reconciliation.finalization import validate_broker_sync_result
from nexus.reconciliation.persistence import BrokerSyncRepository
from nexus.settings import EngineSettings
from nexus_fosun_gateway.adapter.normalization import normalize_provider_response
from pydantic import TypeAdapter
from sqlalchemy import select


async def upstream_flows(run_id, context, *, amount="12.500000000001"):
    page = normalize_provider_response(
        "fosun_list_cash_flows",
        {
            "code": 0,
            "requestId": "synthetic-cash-context",
            "data": {
                "start": 0,
                "count": 1,
                "total": 1,
                "list": [
                    {
                        "flowId": 1001,
                        "currency": "USD",
                        "amount": amount,
                        "createdAt": "2026-08-23 01:30:00",
                        "tradeDate": "2026-08-23",
                        "flowType": 703,
                        "direction": 2,
                        "remark": "synthetic private memo",
                        "description": "synthetic private text",
                        **context,
                    }
                ],
            },
        },
        received_at=NOW,
        context={"cursor": None, "limit": 100},
    )
    # Use a JSON boundary, not a direct hand-built Engine fact.
    wire = page.model_dump_json()
    gateway_page = GatewayResponsePage.model_validate_json(wire)
    client = SimpleNamespace(read_cash_flows=AsyncMock(return_value=gateway_page))
    provider = FosunBrokerProvider(
        account_id=ACCOUNT_ID,
        external_account_id="synthetic-account",
        client=client,
        instrument_ids={},
        cash_flow_kinds={},
        snapshot_time_basis="gateway_observed",
    )
    return await fetch_cash_flows(
        ACCOUNT_ID,
        provider,
        sync_run_id=run_id,
        initial_start=date(2026, 8, 23),
        end=date(2026, 8, 23),
        previous_watermark=None,
    )


@pytest.mark.asyncio
async def test_optional_context_is_append_only_and_never_changes_ledger_semantics(
    migrated_database_url,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    instrument = await _seed_account_and_portfolio(factory)
    archived = []
    contexts = [
        None,
        {},
        {"businessType": 987654, "productCode": "usSYNTH.A"},
        {"businessType": -1, "productCode": "usSYNTH.B"},
        {"businessType": None, "productCode": ""},
        {"businessType": 888},
    ]
    hashes = set()
    for index, context in enumerate(contexts):
        amount = "99" if index == 5 else "12.500000000001"
        async with factory() as session, session.begin():
            repo = BrokerSyncRepository(session)
            admission = await repo.admit(
                account_id=ACCOUNT_ID,
                portfolio_id=PORTFOLIO_ID,
                trigger_kind="AUTOMATIC",
                actor_id="synthetic-worker",
                route="broker-sync",
                idempotency_key=f"synthetic-context-{index}",
                request_hash=str(index) * 64,
                accepted_at=NOW + timedelta(seconds=index),
            )
            frozen = _sync_result(admission.run.id, _snapshot(instrument, uuid4(), "READY"))
            before = deepcopy(frozen.attempt_traces)
            flows = (
                None
                if context is None
                else await upstream_flows(admission.run.id, context, amount=amount)
            )
            stored = await repo.finalize(
                frozen, completed_at=NOW + timedelta(seconds=index), cash_flow_result=flows
            )
            hashes.add((stored.position_hash, stored.order_hash))
            # Archiving is supplementary: the original six-step evidence is untouched.
            assert frozen.attempt_traces == before
            validate_broker_sync_result(frozen)
        async with factory() as session:
            attempt = await session.scalar(
                select(BrokerSyncAttempt).where(BrokerSyncAttempt.sync_run_id == admission.run.id)
            )
            archived.append((attempt.id, deepcopy(attempt.raw_facts)))
            segment = attempt.raw_facts.get("_cash_flows")
            if flows is None:
                assert segment is None
            else:
                assert segment["schema_version"] == "cash_flow_observation_v1"
                assert segment["ledger_quality"] == ("PARTIAL" if index == 5 else "PASS")
                replayed = TypeAdapter(CashFlowSyncResult).validate_python(segment["result"])
                assert replayed == flows
                fact = replayed.facts[0]
                assert fact.business_type == context.get("businessType")
                assert fact.product_code == (context.get("productCode") or None)
                assert fact.kind == "fosun:703" and fact.occurred_at is None
                assert fact.provider_business_date == date(2026, 8, 23)
                assert fact.amount == -Decimal(amount)
                assert "private" not in str(segment)
            for prior_id, prior_raw in archived:
                prior = await session.get(BrokerSyncAttempt, prior_id)
                assert prior.raw_facts == prior_raw
    async with factory() as session:
        assert len(hashes) == 1
        ledger = (await session.scalars(select(BrokerCashFlow))).all()
        assert len(ledger) == 1
        assert ledger[0].amount == Decimal("-12.500000000001")
        assert ledger[0].kind == "fosun:703"
