"""Quantity continuity uses event prefixes, never just endpoint positions."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.paper.performance import PositionAdjustment
from nexus.strategy.rank_continuity import _ContinuityEvent, _fold_continuity

ACCOUNT, INSTRUMENT = UUID(int=1), UUID(int=2)
START = datetime(2026, 8, 20, 20, tzinfo=UTC)
PREVIOUS = START + timedelta(days=1)
END = START + timedelta(days=2)


def event(seq, delta, at, *, fill=True, created_at=None):
    adjustment = PositionAdjustment(
        event_id=UUID(int=seq + 10),
        account_id=ACCOUNT,
        instrument_id=INSTRUMENT,
        event_seq=seq,
        event_type="fill" if fill else "correction",
        quantity_delta=Decimal(delta),
        cost_delta=Decimal(delta) * 10,
        occurred_at=at,
        payload_hash=f"{seq:064x}",
    )
    return _ContinuityEvent(
        adjustment, created_at or at, UUID(int=seq + 100) if fill else None, f"{seq:064x}"
    )


def fold(*events, previous=PREVIOUS):
    return _fold_continuity(account_id=ACCOUNT, events=events, previous_cutoff=previous, cutoff=END)


def test_same_time_clear_and_reopen_is_not_continuous():
    result = fold(event(1, 10, START), event(2, -10, END), event(3, 10, END)).positions[0]
    assert result.quantity_at_previous == result.quantity_at_cutoff == 10
    assert result.kind == "reopened"
    assert result.opening_event_seq == 3
    assert result.opening_fill_id == UUID(int=103)
    assert not result.continuous_since_previous


def test_continuous_holding_and_closed_endpoint_are_retained():
    result = fold(event(1, 10, START), event(2, -2, END)).positions[0]
    assert result.kind == "continuous" and result.quantity_at_cutoff == 8
    closed = fold(event(1, 10, START), event(2, -10, END)).positions[0]
    assert closed.kind == "closed" and closed.quantity_at_cutoff == 0
    assert closed.last_event_seq == 2


def test_new_position_requires_verified_fill_opening():
    result = fold(event(1, 10, END)).positions[0]
    assert result.kind == "opened" and result.quantity_at_previous == 0
    unknown = fold(event(1, 10, END, fill=False)).positions[0]
    assert unknown.kind == "unavailable" and unknown.reason == "NON_FILL_OPENING"


@pytest.mark.parametrize(
    "events",
    [
        (event(2, 10, START),),
        (event(1, 10, END), event(2, 1, START)),
        (event(1, 10, START, created_at=END),),
        (event(1, 10, END, created_at=END + timedelta(seconds=1)),),
    ],
)
def test_missing_sequence_time_reversal_and_late_facts_are_rejected(events):
    with pytest.raises(ValueError):
        fold(*events)


def test_exact_cutoff_boundary_and_no_previous_cutoff_are_explicit():
    result = fold(event(1, 10, PREVIOUS), event(2, 5, END)).positions[0]
    assert result.quantity_at_previous == 10 and result.previous_event_seq == 1
    assert result.previous_prefix_hash != result.prefix_hash
    first = fold(event(1, 10, END), previous=None).positions[0]
    assert first.quantity_at_previous is None
    assert first.previous_prefix_hash is None


def fill_rows():
    from nexus.paper.execution_service import _decimal, _fact_hash, _time
    from nexus.paper.ledger import make_buy_fill_transaction
    from nexus.persistence.models.paper import (
        PaperExecutionWindowAuthorityRecord,
        PaperFillRecord,
        PaperLedgerTransactionRecord,
        PaperOrderRecord,
        PaperPositionAdjustmentEventRecord,
    )

    order = PaperOrderRecord(
        id=UUID(int=3), account_id=ACCOUNT, instrument_id=INSTRUMENT, side="buy"
    )
    window = PaperExecutionWindowAuthorityRecord(
        id=UUID(int=4),
        order_id=order.id,
        account_id=ACCOUNT,
        instrument_id=INSTRUMENT,
        evaluated_at=START,
        content_hash="a" * 64,
    )
    fill = PaperFillRecord(
        id=UUID(int=5),
        order_id=order.id,
        account_id=ACCOUNT,
        instrument_id=INSTRUMENT,
        quantity=Decimal(10),
        price=Decimal(10),
        notional=Decimal(100),
        fee=Decimal(1),
        fill_seq=1,
        occurred_at=START,
        evaluated_at=START,
        execution_window_authority_id=window.id,
        execution_model_hash="b" * 64,
        ledger_transaction_id=UUID(int=6),
    )
    fill.payload_hash = _fact_hash(
        "paper_fill",
        dict(
            order_id=str(order.id),
            fill_seq=1,
            instrument_id=str(INSTRUMENT),
            side="buy",
            quantity=_decimal(fill.quantity),
            price=_decimal(fill.price),
            notional=_decimal(fill.notional),
            fee=_decimal(fill.fee),
            execution_model_hash=fill.execution_model_hash,
            window_content_hash=window.content_hash,
            evaluated_at=_time(START),
        ),
    )
    ledger = make_buy_fill_transaction(
        transaction_id=fill.ledger_transaction_id,
        entry_ids=(UUID(int=7), UUID(int=8), UUID(int=9)),
        paper_account_id=ACCOUNT,
        reference_hash=fill.payload_hash,
        principal=fill.notional,
        fee=fill.fee,
        occurred_at=START,
    )
    transaction = PaperLedgerTransactionRecord(
        id=ledger.transaction_id, account_id=ACCOUNT, order_id=order.id, occurred_at=START
    )
    row = PaperPositionAdjustmentEventRecord(
        id=UUID(int=10),
        account_id=ACCOUNT,
        instrument_id=INSTRUMENT,
        event_seq=1,
        event_type="fill",
        quantity_delta=fill.quantity,
        cost_delta=fill.notional,
        occurred_at=START,
        source_fill_id=fill.id,
    )
    row.payload_hash = _fact_hash(
        "paper_position_adjustment",
        dict(
            account_id=str(ACCOUNT),
            instrument_id=str(INSTRUMENT),
            event_seq=1,
            event_type="fill",
            quantity_delta=_decimal(row.quantity_delta),
            cost_delta=_decimal(row.cost_delta),
            source_fill_id=str(fill.id),
            occurred_at=_time(START),
        ),
    )
    return row, fill, order, transaction, window, ledger


def test_fill_proof_checks_actual_hash_and_ledger_meaning():
    from nexus.strategy.rank_continuity import _verify_fill

    rows = fill_rows()
    assert len(_verify_fill(*rows)) == 64
    broken = rows[-1].model_copy(update={"entries": rows[-1].entries[:-1]})
    with pytest.raises(ValueError, match="LEDGER_BINDING"):
        _verify_fill(*rows[:-1], broken)


@pytest.mark.parametrize(
    "index,field,value",
    [
        (0, "account_id", UUID(int=99)),
        (1, "instrument_id", UUID(int=99)),
        (3, "order_id", UUID(int=99)),
        (4, "account_id", UUID(int=99)),
        (0, "quantity_delta", Decimal(11)),
        (1, "payload_hash", "f" * 64),
    ],
)
def test_inconsistent_fill_order_account_instrument_or_hash_is_rejected(index, field, value):
    from nexus.strategy.rank_continuity import _verify_fill

    rows = fill_rows()
    setattr(rows[index], field, value)
    with pytest.raises(ValueError, match="RANK_CONTINUITY"):
        _verify_fill(*rows)


def test_prefix_binds_adjustment_content_not_only_declared_payload_hash():
    cleared = fold(
        event(1, 10, START), event(2, -10, END, fill=False), event(3, 10, END, fill=False)
    ).positions[0]
    retained = fold(
        event(1, 10, START), event(2, -2, END, fill=False), event(3, 2, END, fill=False)
    ).positions[0]
    assert cleared.quantity_at_cutoff == retained.quantity_at_cutoff == 10
    assert cleared.previous_prefix_hash == retained.previous_prefix_hash
    assert cleared.prefix_hash != retained.prefix_hash
