"""One-shot command boundaries over the real paper services in isolated PostgreSQL."""

import asyncio
import json
import sys
from datetime import timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from helpers.paper_batch import seed_paper_batch
from helpers.paper_execution import seed_paper_execution
from nexus.ops import paper_once
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text


async def counts(database_url: str) -> tuple[int, ...]:
    factory = session_factory(EngineSettings(database_url=database_url))
    async with factory() as session:
        row = (
            await session.execute(
                text(
                    "SELECT (SELECT count(*) FROM paper_order_batches),"
                    "(SELECT count(*) FROM paper_orders),(SELECT count(*) FROM paper_fills),"
                    "(SELECT count(*) FROM paper_ledger_entries),"
                    "(SELECT count(*) FROM paper_order_events)"
                )
            )
        ).one()
        return tuple(row)


def admit_request(seed, **changes):
    return paper_once.PaperAdmitRequest.model_validate(
        {
            "action": "admit",
            "account_id": seed.account_id,
            "strategy_decision_id": seed.decision_id,
            "request_time": seed.request_time,
            "idempotency_key": "paper-once-admit",
            **changes,
        }
    )


def advance_request(seed, **changes):
    return paper_once.PaperAdvanceRequest.model_validate(
        {
            "action": "advance",
            "account_id": seed.account_id,
            "order_id": seed.order_id,
            "evaluated_at": seed.evaluation_times[0],
            "idempotency_key": "paper-once-advance",
            **changes,
        }
    )


def test_cli_entry_preserves_current_gate(migrated_database_url, monkeypatch, capsys):
    seed = asyncio.run(seed_paper_batch(migrated_database_url))
    before = asyncio.run(counts(migrated_database_url))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "paper_once",
            "admit",
            "--account-id",
            str(seed.account_id),
            "--strategy-decision-id",
            str(seed.decision_id),
            "--request-time",
            seed.request_time.isoformat(),
            "--idempotency-key",
            "cli-current-gate",
        ],
    )
    capsys.readouterr()
    with pytest.raises(SystemExit, match="1"):
        paper_once.main()
    captured = capsys.readouterr()
    assert captured.out == ""
    assert json.loads(captured.err) == {
        "status": "failed",
        "code": "STRATEGY_DECISION_INVALID",
    }
    assert asyncio.run(counts(migrated_database_url)) == before


@pytest.mark.asyncio
async def test_current_admission_gate_rejects_without_business_writes(migrated_database_url):
    seed = await seed_paper_batch(migrated_database_url)
    before = await counts(migrated_database_url)
    with pytest.raises(paper_once.PaperOnceError, match="STRATEGY_DECISION_INVALID"):
        await paper_once.run_once(
            EngineSettings(database_url=migrated_database_url), admit_request(seed)
        )
    assert await counts(migrated_database_url) == before


@pytest.mark.asyncio
async def test_historical_admission_replays_and_binds_parameters(
    migrated_database_url,
    monkeypatch,
):
    # This named historical fixture gate does not admit the current real price source.
    monkeypatch.setattr(
        "nexus.paper.service.current_paper_order_price_authority_available", lambda: True
    )
    seed = await seed_paper_batch(migrated_database_url)
    settings = EngineSettings(database_url=migrated_database_url)
    request = admit_request(seed)
    first = await paper_once.run_once(settings, request)
    after = await counts(migrated_database_url)
    assert after[0] == 1 and after[1] > 0 and after[2] == 0
    assert await paper_once.run_once(settings, request) == first
    assert await counts(migrated_database_url) == after
    with pytest.raises(paper_once.PaperOnceError, match="IDEMPOTENCY_CONFLICT"):
        await paper_once.run_once(
            settings, admit_request(seed, request_time=seed.request_time + timedelta(seconds=1))
        )
    with pytest.raises(paper_once.PaperOnceError, match="PAPER_ACCOUNT_INVALID"):
        await paper_once.run_once(settings, admit_request(seed, account_id=uuid4()))
    assert await counts(migrated_database_url) == after


@pytest.mark.asyncio
async def test_current_execution_gate_cannot_create_a_fill(migrated_database_url):
    seed = await seed_paper_execution(migrated_database_url, legacy_market_policy=False)
    settings = EngineSettings(database_url=migrated_database_url)
    result = await paper_once.run_once(settings, advance_request(seed))
    assert result["action"] == "no_data"
    after = await counts(migrated_database_url)
    assert after[2] == 0
    assert await paper_once.run_once(settings, advance_request(seed)) == result
    assert await counts(migrated_database_url) == after


@pytest.mark.asyncio
@pytest.mark.parametrize("volume, action", [("100", "partially_filled"), ("2000", "filled")])
async def test_historical_execution_replay_does_not_duplicate_fill_or_ledger(
    migrated_database_url,
    monkeypatch,
    volume,
    action,
):
    monkeypatch.setattr(
        "nexus.paper.execution_service._paper_execution_authority_available", lambda _: True
    )
    seed = await seed_paper_execution(migrated_database_url, window_bar_volumes=(Decimal(volume),))
    settings = EngineSettings(database_url=migrated_database_url)
    request = advance_request(seed)
    result = await paper_once.run_once(settings, request)
    assert result["action"] == action
    after = await counts(migrated_database_url)
    assert after[2] == 1
    assert await paper_once.run_once(settings, request) == result
    assert await counts(migrated_database_url) == after
    factory = session_factory(settings)
    async with factory() as session:
        imbalance = await session.scalar(
            text(
                "SELECT count(*) FROM (SELECT transaction_id FROM paper_ledger_entries "
                "GROUP BY transaction_id HAVING sum(debit_amount) <> sum(credit_amount)) x"
            )
        )
    assert imbalance == 0


@pytest.mark.asyncio
async def test_advance_rejects_another_account_without_writes(migrated_database_url):
    seed = await seed_paper_execution(migrated_database_url)
    before = await counts(migrated_database_url)
    with pytest.raises(paper_once.PaperOnceError, match="PAPER_ONCE_ACCOUNT_MISMATCH"):
        await paper_once.run_once(
            EngineSettings(database_url=migrated_database_url),
            advance_request(seed, account_id=uuid4()),
        )
    assert await counts(migrated_database_url) == before


@pytest.mark.asyncio
async def test_failure_after_batch_writes_rolls_back_the_command(
    migrated_database_url, monkeypatch
):
    monkeypatch.setattr(
        "nexus.paper.service.current_paper_order_price_authority_available", lambda: True
    )
    seed = await seed_paper_batch(migrated_database_url)
    before = await counts(migrated_database_url)
    original = paper_once.create_paper_order_batch

    async def fail_after_writes(*args, **kwargs):
        await original(*args, **kwargs)
        raise RuntimeError("private provider detail")

    monkeypatch.setattr(paper_once, "create_paper_order_batch", fail_after_writes)
    with pytest.raises(paper_once.PaperOnceError, match="PAPER_ONCE_RESULT_UNKNOWN"):
        await paper_once.run_once(
            EngineSettings(database_url=migrated_database_url), admit_request(seed)
        )
    assert await counts(migrated_database_url) == before
