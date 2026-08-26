from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.paper.ledger import (
    balance_ledger,
    make_buy_reservation_transaction,
    post_ledger_transaction,
)
from nexus.paper.models import Ledger, LedgerAccount, LedgerEntry, LedgerTransaction
from nexus.risk.canonical import canonical_paper_ledger_transaction_hash

NOW = datetime(2026, 8, 21, 20, tzinfo=UTC)
PAPER_ACCOUNT_ID = UUID(int=1)


def _account(index: int, code: str, normal_side: str) -> LedgerAccount:
    return LedgerAccount(
        account_id=UUID(int=index),
        paper_account_id=PAPER_ACCOUNT_ID,
        code=code,
        normal_side=normal_side,
        currency="USD",
    )


def _ledger() -> Ledger:
    return Ledger(
        paper_account_id=PAPER_ACCOUNT_ID,
        accounts=(
            _account(10, "asset.cash.available.usd", "debit"),
            _account(11, "asset.cash.reserved.usd", "debit"),
            _account(12, "asset.position_cost.usd", "debit"),
            _account(13, "equity.contributed_capital.usd", "credit"),
            _account(14, "income.realized_pnl.usd", "credit"),
            _account(15, "income.dividend.usd", "credit"),
            _account(16, "expense.fees.usd", "debit"),
        ),
        transactions=(),
    )


def _funding(
    *,
    amount: Decimal = Decimal("10000"),
    transaction_id: UUID = UUID(int=100),
) -> LedgerTransaction:
    draft = LedgerTransaction(
        transaction_id=transaction_id,
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


def test_funding_and_buy_reservation_keep_ledger_balanced() -> None:
    original = _ledger()
    funding = _funding()
    ledger = post_ledger_transaction(original, funding)
    reserve = make_buy_reservation_transaction(
        transaction_id=UUID(int=200),
        debit_entry_id=UUID(int=201),
        credit_entry_id=UUID(int=202),
        paper_account_id=PAPER_ACCOUNT_ID,
        reference_hash="1" * 64,
        amount=Decimal("1010"),
        occurred_at=NOW,
    )
    ledger = post_ledger_transaction(ledger, reserve)

    assert balance_ledger(ledger) == {
        "asset.cash.available.usd": Decimal("8990"),
        "asset.cash.reserved.usd": Decimal("1010"),
        "asset.position_cost.usd": Decimal("0"),
        "equity.contributed_capital.usd": Decimal("10000"),
        "expense.fees.usd": Decimal("0"),
        "income.dividend.usd": Decimal("0"),
        "income.realized_pnl.usd": Decimal("0"),
    }
    assert original.transactions == ()
    assert ledger.transactions[0] is funding


def test_post_rejects_unbalanced_or_zero_transaction() -> None:
    unbalanced = _funding().model_copy(
        update={
            "entries": (
                _funding().entries[0],
                _funding().entries[1].model_copy(update={"credit_amount": Decimal("9999")}),
            )
        }
    )
    with pytest.raises(ValueError, match="balance"):
        post_ledger_transaction(_ledger(), unbalanced)

    with pytest.raises(ValueError, match="entries"):
        post_ledger_transaction(
            _ledger(),
            _funding().model_copy(update={"entries": ()}),
        )


def test_post_rejects_unknown_account_wrong_currency_and_sequence_gap() -> None:
    unknown = _funding().model_copy(
        update={
            "entries": (
                _funding().entries[0].model_copy(update={"account_code": "unknown"}),
                _funding().entries[1],
            )
        }
    )
    with pytest.raises(ValueError, match="unknown ledger account"):
        post_ledger_transaction(_ledger(), unknown)

    wrong_currency = _funding().entries[0].model_copy(update={"currency": "HKD"})
    invalid_currency = _funding().model_copy(
        update={"entries": (wrong_currency, _funding().entries[1])}
    )
    with pytest.raises(ValueError, match="currency"):
        post_ledger_transaction(_ledger(), invalid_currency)

    gap = _funding().model_copy(
        update={
            "entries": (
                _funding().entries[0],
                _funding().entries[1].model_copy(update={"entry_seq": 3}),
            )
        }
    )
    with pytest.raises(ValueError, match="continuous"):
        post_ledger_transaction(_ledger(), gap)


def test_post_rejects_duplicate_transaction_id_and_hash() -> None:
    funded = post_ledger_transaction(_ledger(), _funding())

    with pytest.raises(ValueError, match="transaction id"):
        post_ledger_transaction(
            funded,
            _funding(transaction_id=UUID(int=100)).model_copy(
                update={"transaction_hash": "b" * 64}
            ),
        )
    with pytest.raises(ValueError, match="transaction hash"):
        post_ledger_transaction(
            funded,
            _funding(transaction_id=UUID(int=999)),
        )


def test_post_rejects_negative_account_balance() -> None:
    funded = post_ledger_transaction(_ledger(), _funding(amount=Decimal("100")))
    reserve = make_buy_reservation_transaction(
        transaction_id=UUID(int=200),
        debit_entry_id=UUID(int=201),
        credit_entry_id=UUID(int=202),
        paper_account_id=PAPER_ACCOUNT_ID,
        reference_hash="2" * 64,
        amount=Decimal("101"),
        occurred_at=NOW,
    )

    with pytest.raises(ValueError, match="negative ledger balance"):
        post_ledger_transaction(funded, reserve)


def test_post_allows_signed_realized_pnl_for_a_loss() -> None:
    funded = post_ledger_transaction(_ledger(), _funding(amount=Decimal("100")))
    draft = LedgerTransaction(
        transaction_id=UUID(int=300),
        account_id=PAPER_ACCOUNT_ID,
        transaction_type="adjustment",
        reference_type="adjustment",
        reference_hash="3" * 64,
        occurred_at=NOW,
        entries=(
            LedgerEntry(
                entry_id=UUID(int=301),
                entry_seq=1,
                account_code="income.realized_pnl.usd",
                currency="USD",
                debit_amount=Decimal("25"),
                credit_amount=Decimal("0"),
            ),
            LedgerEntry(
                entry_id=UUID(int=302),
                entry_seq=2,
                account_code="asset.cash.available.usd",
                currency="USD",
                debit_amount=Decimal("0"),
                credit_amount=Decimal("25"),
            ),
        ),
        transaction_hash="0" * 64,
    )
    loss = draft.model_copy(
        update={"transaction_hash": canonical_paper_ledger_transaction_hash(draft)}
    )

    posted = post_ledger_transaction(funded, loss)

    balances = balance_ledger(posted)
    assert balances["income.realized_pnl.usd"] == Decimal("-25")
    assert balances["asset.cash.available.usd"] == Decimal("75")


def test_ledger_catalog_rejects_wrong_normal_side() -> None:
    invalid = _ledger().model_copy(
        update={
            "accounts": (
                _account(10, "asset.cash.available.usd", "credit"),
                *_ledger().accounts[1:],
            )
        }
    )

    with pytest.raises(ValueError, match="normal side"):
        post_ledger_transaction(invalid, _funding())
