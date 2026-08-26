from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from nexus.paper.ledger import (
    balance_ledger,
    make_buy_reservation_transaction,
    post_ledger_transaction,
)
from nexus.paper.models import Ledger, LedgerAccount, LedgerEntry, LedgerTransaction
from nexus.risk.canonical import canonical_paper_ledger_transaction_hash

NOW = datetime(2026, 8, 21, 20, tzinfo=UTC)
PAPER_ACCOUNT_ID = UUID(int=1)


def _ledger() -> Ledger:
    definitions = (
        ("asset.cash.available.usd", "debit"),
        ("asset.cash.reserved.usd", "debit"),
        ("asset.position_cost.usd", "debit"),
        ("equity.contributed_capital.usd", "credit"),
        ("income.realized_pnl.usd", "credit"),
        ("income.dividend.usd", "credit"),
        ("expense.fees.usd", "debit"),
    )
    return Ledger(
        paper_account_id=PAPER_ACCOUNT_ID,
        accounts=tuple(
            LedgerAccount(
                account_id=UUID(int=index),
                paper_account_id=PAPER_ACCOUNT_ID,
                code=code,
                normal_side=normal_side,
                currency="USD",
            )
            for index, (code, normal_side) in enumerate(definitions, start=10)
        ),
        transactions=(),
    )


def _funding(amount: Decimal) -> LedgerTransaction:
    draft = LedgerTransaction(
        transaction_id=UUID(int=100),
        account_id=PAPER_ACCOUNT_ID,
        transaction_type="funding",
        reference_type="initial_funding",
        reference_hash="f" * 64,
        occurred_at=NOW,
        entries=(
            LedgerEntry(
                entry_id=UUID(int=101),
                entry_seq=1,
                account_code="asset.cash.available.usd",
                currency="USD",
                debit_amount=amount,
                credit_amount=Decimal("0"),
            ),
            LedgerEntry(
                entry_id=UUID(int=102),
                entry_seq=2,
                account_code="equity.contributed_capital.usd",
                currency="USD",
                debit_amount=Decimal("0"),
                credit_amount=amount,
            ),
        ),
        transaction_hash="0" * 64,
    )
    return draft.model_copy(
        update={"transaction_hash": canonical_paper_ledger_transaction_hash(draft)}
    )


@settings(max_examples=100, deadline=None)
@given(amounts=st.lists(st.integers(min_value=1, max_value=1000), max_size=8))
def test_any_valid_reservation_sequence_stays_balanced(amounts: list[int]) -> None:
    funding = Decimal("10000")
    ledger = post_ledger_transaction(_ledger(), _funding(funding))
    for index, integer_amount in enumerate(amounts, start=1):
        ledger = post_ledger_transaction(
            ledger,
            make_buy_reservation_transaction(
                transaction_id=UUID(int=200 + index),
                debit_entry_id=UUID(int=400 + index * 2),
                credit_entry_id=UUID(int=401 + index * 2),
                paper_account_id=PAPER_ACCOUNT_ID,
                reference_hash=f"{index:x}".rjust(64, "0"),
                amount=Decimal(integer_amount),
                occurred_at=NOW,
            ),
        )

    balances = balance_ledger(ledger)
    reserved = sum((Decimal(value) for value in amounts), start=Decimal(0))
    assert balances["asset.cash.available.usd"] == funding - reserved
    assert balances["asset.cash.reserved.usd"] == reserved
    assert balances["equity.contributed_capital.usd"] == funding
    assert all(value >= 0 for value in balances.values())


@settings(max_examples=50, deadline=None)
@given(
    debit=st.integers(min_value=1, max_value=10000),
    difference=st.integers(min_value=1, max_value=10000),
)
def test_any_unbalanced_mutation_is_rejected(debit: int, difference: int) -> None:
    valid = _funding(Decimal(debit))
    invalid = valid.model_copy(
        update={
            "entries": (
                valid.entries[0],
                valid.entries[1].model_copy(update={"credit_amount": Decimal(debit + difference)}),
            )
        }
    )

    with pytest.raises(ValueError, match="balance"):
        post_ledger_transaction(_ledger(), invalid)
