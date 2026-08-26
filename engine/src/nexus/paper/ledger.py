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
    exact_decimal,
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
_SIGNED_BALANCE_ACCOUNTS = frozenset({"income.realized_pnl.usd"})


def _amount(value: Decimal, *, field: str, positive: bool = False) -> Decimal:
    result = exact_decimal(value, field=field)
    if positive and result <= 0:
        raise ValueError(f"{field} must be positive")
    if not positive and result < 0:
        raise ValueError(f"{field} must be nonnegative")
    return result


def _entry_ids(value: tuple[UUID, ...], *, count: int) -> tuple[UUID, ...]:
    if len(value) != count or len(set(value)) != count:
        raise ValueError(f"fill transaction requires {count} unique entry ids")
    return value


def _hashed_transaction(draft: LedgerTransaction) -> LedgerTransaction:
    return draft.model_copy(
        update={"transaction_hash": canonical_paper_ledger_transaction_hash(draft)}
    )


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
    if any(
        balance < 0 and code not in _SIGNED_BALANCE_ACCOUNTS for code, balance in balances.items()
    ):
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
    return _hashed_transaction(draft)


def make_buy_fill_transaction(
    *,
    transaction_id: UUID,
    entry_ids: tuple[UUID, UUID, UUID],
    paper_account_id: UUID,
    reference_hash: str,
    principal: Decimal,
    fee: Decimal,
    occurred_at: datetime,
) -> LedgerTransaction:
    """Move one buy fill from reserved cash into cost basis and fee expense."""
    checked_ids = _entry_ids(entry_ids, count=3)
    checked_principal = _amount(principal, field="principal", positive=True)
    checked_fee = _amount(fee, field="fee")
    entries = [
        LedgerEntry(
            entry_id=checked_ids[0],
            entry_seq=1,
            account_code="asset.position_cost.usd",
            currency="USD",
            debit_amount=checked_principal,
            credit_amount=Decimal(0),
        )
    ]
    if checked_fee > 0:
        entries.append(
            LedgerEntry(
                entry_id=checked_ids[1],
                entry_seq=len(entries) + 1,
                account_code="expense.fees.usd",
                currency="USD",
                debit_amount=checked_fee,
                credit_amount=Decimal(0),
            )
        )
    entries.append(
        LedgerEntry(
            entry_id=checked_ids[2],
            entry_seq=len(entries) + 1,
            account_code="asset.cash.reserved.usd",
            currency="USD",
            debit_amount=Decimal(0),
            credit_amount=checked_principal + checked_fee,
        )
    )
    draft = LedgerTransaction(
        transaction_id=transaction_id,
        account_id=paper_account_id,
        transaction_type="fill",
        reference_type="fill",
        reference_hash=reference_hash,
        occurred_at=occurred_at,
        entries=tuple(entries),
        transaction_hash="0" * 64,
    )
    return _hashed_transaction(draft)


def make_cash_release_transaction(
    *,
    transaction_id: UUID,
    debit_entry_id: UUID,
    credit_entry_id: UUID,
    paper_account_id: UUID,
    reference_hash: str,
    amount: Decimal,
    occurred_at: datetime,
) -> LedgerTransaction:
    """Return unused buy-side cash from the reservation to available cash."""
    checked_amount = _amount(amount, field="release amount", positive=True)
    draft = LedgerTransaction(
        transaction_id=transaction_id,
        account_id=paper_account_id,
        transaction_type="release",
        reference_type="release",
        reference_hash=reference_hash,
        occurred_at=occurred_at,
        entries=(
            LedgerEntry(
                entry_id=debit_entry_id,
                entry_seq=1,
                account_code="asset.cash.available.usd",
                currency="USD",
                debit_amount=checked_amount,
                credit_amount=Decimal(0),
            ),
            LedgerEntry(
                entry_id=credit_entry_id,
                entry_seq=2,
                account_code="asset.cash.reserved.usd",
                currency="USD",
                debit_amount=Decimal(0),
                credit_amount=checked_amount,
            ),
        ),
        transaction_hash="0" * 64,
    )
    return _hashed_transaction(draft)


def make_sell_fill_transaction(
    *,
    transaction_id: UUID,
    entry_ids: tuple[UUID, UUID, UUID, UUID],
    paper_account_id: UUID,
    reference_hash: str,
    proceeds: Decimal,
    allocated_cost: Decimal,
    fee: Decimal,
    occurred_at: datetime,
) -> LedgerTransaction:
    """Post sell proceeds, gross realized P&L, released cost basis, and fees."""
    checked_ids = _entry_ids(entry_ids, count=4)
    checked_proceeds = _amount(proceeds, field="proceeds", positive=True)
    checked_cost = _amount(allocated_cost, field="allocated cost", positive=True)
    checked_fee = _amount(fee, field="fee")
    if checked_fee >= checked_proceeds:
        raise ValueError("fee must not equal or exceed proceeds")

    realized = checked_proceeds - checked_cost
    entries: list[LedgerEntry] = [
        LedgerEntry(
            entry_id=checked_ids[0],
            entry_seq=1,
            account_code="asset.cash.available.usd",
            currency="USD",
            debit_amount=checked_proceeds - checked_fee,
            credit_amount=Decimal(0),
        )
    ]
    if checked_fee > 0:
        entries.append(
            LedgerEntry(
                entry_id=checked_ids[len(entries)],
                entry_seq=len(entries) + 1,
                account_code="expense.fees.usd",
                currency="USD",
                debit_amount=checked_fee,
                credit_amount=Decimal(0),
            )
        )
    if realized < 0:
        entries.append(
            LedgerEntry(
                entry_id=checked_ids[len(entries)],
                entry_seq=len(entries) + 1,
                account_code="income.realized_pnl.usd",
                currency="USD",
                debit_amount=-realized,
                credit_amount=Decimal(0),
            )
        )
    entries.append(
        LedgerEntry(
            entry_id=checked_ids[len(entries)],
            entry_seq=len(entries) + 1,
            account_code="asset.position_cost.usd",
            currency="USD",
            debit_amount=Decimal(0),
            credit_amount=checked_cost,
        )
    )
    if realized > 0:
        entries.append(
            LedgerEntry(
                entry_id=checked_ids[len(entries)],
                entry_seq=len(entries) + 1,
                account_code="income.realized_pnl.usd",
                currency="USD",
                debit_amount=Decimal(0),
                credit_amount=realized,
            )
        )
    draft = LedgerTransaction(
        transaction_id=transaction_id,
        account_id=paper_account_id,
        transaction_type="fill",
        reference_type="fill",
        reference_hash=reference_hash,
        occurred_at=occurred_at,
        entries=tuple(entries),
        transaction_hash="0" * 64,
    )
    return _hashed_transaction(draft)
