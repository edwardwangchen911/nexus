"""Pure double-entry posting and balance projection for internal paper cash."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from nexus.paper.models import (
    Ledger,
    LedgerAccount,
    LedgerEntry,
    LedgerTransaction,
)
from nexus.risk.canonical import canonical_paper_ledger_transaction_hash

_EXPECTED_NORMAL_SIDES = {
    "asset.cash.available.usd": "debit",
    "asset.cash.reserved.usd": "debit",
    "asset.position_cost.usd": "debit",
    "equity.contributed_capital.usd": "credit",
    "income.realized_pnl.usd": "credit",
    "income.dividend.usd": "credit",
    "expense.fees.usd": "debit",
}


def _validate_catalog(ledger: Ledger) -> dict[str, LedgerAccount]:
    accounts = {account.code: account for account in ledger.accounts}
    if set(accounts) != set(_EXPECTED_NORMAL_SIDES):
        raise ValueError("ledger account catalog is incomplete or contains unknown accounts")
    for code, expected in _EXPECTED_NORMAL_SIDES.items():
        account = accounts[code]
        if account.normal_side != expected:
            raise ValueError("ledger account normal side does not match the fixed catalog")
        if account.currency != "USD":
            raise ValueError("ledger account currency must be USD")
    return accounts


def _validate_transaction(
    transaction: LedgerTransaction,
    *,
    ledger: Ledger,
    accounts: dict[str, LedgerAccount],
) -> None:
    if transaction.account_id != ledger.paper_account_id:
        raise ValueError("ledger transaction belongs to another paper account")
    if not transaction.entries:
        raise ValueError("ledger transaction requires entries")
    entry_ids: set[UUID] = set()
    total_debit = Decimal(0)
    total_credit = Decimal(0)
    for expected_seq, entry in enumerate(transaction.entries, start=1):
        if entry.entry_id in entry_ids:
            raise ValueError("ledger entry ids must be unique")
        entry_ids.add(entry.entry_id)
        if entry.entry_seq != expected_seq:
            raise ValueError("ledger entry sequence must be continuous")
        account = accounts.get(entry.account_code)
        if account is None:
            raise ValueError("unknown ledger account")
        if entry.currency != account.currency:
            raise ValueError("ledger entry currency must match the account")
        debit_positive = entry.debit_amount > 0 and entry.credit_amount == 0
        credit_positive = entry.credit_amount > 0 and entry.debit_amount == 0
        if not (debit_positive ^ credit_positive):
            raise ValueError("ledger entry requires exactly one positive side")
        total_debit += entry.debit_amount
        total_credit += entry.credit_amount
    if total_debit <= 0 or total_debit != total_credit:
        raise ValueError("ledger transaction must balance with a positive total")
    if canonical_paper_ledger_transaction_hash(transaction) != transaction.transaction_hash:
        raise ValueError("ledger transaction hash does not match its content")


def balance_ledger(ledger: Ledger) -> dict[str, Decimal]:
    accounts = _validate_catalog(ledger)
    balances = {code: Decimal(0) for code in sorted(accounts)}
    transaction_ids: set[UUID] = set()
    transaction_hashes: set[str] = set()
    for transaction in ledger.transactions:
        if transaction.transaction_id in transaction_ids:
            raise ValueError("ledger transaction id must be unique")
        if transaction.transaction_hash in transaction_hashes:
            raise ValueError("ledger transaction hash must be unique")
        transaction_ids.add(transaction.transaction_id)
        transaction_hashes.add(transaction.transaction_hash)
        _validate_transaction(transaction, ledger=ledger, accounts=accounts)
        for entry in transaction.entries:
            account = accounts[entry.account_code]
            if account.normal_side == "debit":
                balances[entry.account_code] += entry.debit_amount - entry.credit_amount
            else:
                balances[entry.account_code] += entry.credit_amount - entry.debit_amount
    return balances


def post_ledger_transaction(
    ledger: Ledger,
    transaction: LedgerTransaction,
) -> Ledger:
    accounts = _validate_catalog(ledger)
    if transaction.transaction_id in {existing.transaction_id for existing in ledger.transactions}:
        raise ValueError("ledger transaction id must be unique")
    if transaction.transaction_hash in {
        existing.transaction_hash for existing in ledger.transactions
    }:
        raise ValueError("ledger transaction hash must be unique")
    _validate_transaction(transaction, ledger=ledger, accounts=accounts)
    updated = ledger.model_copy(update={"transactions": (*ledger.transactions, transaction)})
    balances = balance_ledger(updated)
    if any(balance < 0 for balance in balances.values()):
        raise ValueError("ledger posting would create a negative ledger balance")
    return updated


def make_buy_reservation_transaction(
    *,
    transaction_id: UUID,
    debit_entry_id: UUID,
    credit_entry_id: UUID,
    paper_account_id: UUID,
    reference_hash: str,
    amount: Decimal,
    occurred_at: datetime,
) -> LedgerTransaction:
    draft = LedgerTransaction(
        transaction_id=transaction_id,
        account_id=paper_account_id,
        transaction_type="cash_reservation",
        reference_type="cash_reservation",
        reference_hash=reference_hash,
        occurred_at=occurred_at,
        entries=(
            LedgerEntry(
                entry_id=debit_entry_id,
                entry_seq=1,
                account_code="asset.cash.reserved.usd",
                currency="USD",
                debit_amount=amount,
                credit_amount=Decimal(0),
            ),
            LedgerEntry(
                entry_id=credit_entry_id,
                entry_seq=2,
                account_code="asset.cash.available.usd",
                currency="USD",
                debit_amount=Decimal(0),
                credit_amount=amount,
            ),
        ),
        transaction_hash="0" * 64,
    )
    return draft.model_copy(
        update={"transaction_hash": canonical_paper_ledger_transaction_hash(draft)}
    )
