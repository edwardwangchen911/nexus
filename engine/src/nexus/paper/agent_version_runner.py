"""Persistence-first coordination for isolated paired Agent-version slots."""

import hashlib
import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.ids import new_uuid7
from nexus.jobs.daily_research import (
    AGENT_VERSION_CHALLENGER_ACTOR,
    AGENT_VERSION_INCUMBENT_ACTOR,
)
from nexus.paper.agent_cycle import start_schwab_agent_paper_cycle
from nexus.paper.agent_cycle_authority import CycleAuthority
from nexus.paper.agent_version_models import AgentVersionSlotResult
from nexus.paper.models import LedgerEntry, LedgerTransaction
from nexus.paper.projection import load_paper_projection
from nexus.persistence.models.agent_versions import (
    AgentVersionCyclePairRecord,
    AgentVersionEvaluationRecord,
)
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.market import UniverseSnapshot
from nexus.persistence.models.paper import (
    AgentPaperCycleRecord,
    PaperAccountRecord,
    PaperDrawdownLockEventRecord,
    PaperLedgerAccountRecord,
    PaperLedgerEntryRecord,
    PaperLedgerTransactionRecord,
)
from nexus.risk.canonical import canonical_paper_ledger_transaction_hash
from nexus.risk.models import PaperPortfolioProjection

_LEDGER_CATALOG = {
    "asset.cash.available.usd": "debit",
    "asset.cash.reserved.usd": "debit",
    "asset.position_cost.usd": "debit",
    "equity.contributed_capital.usd": "credit",
    "income.realized_pnl.usd": "credit",
    "income.dividend.usd": "credit",
    "expense.fees.usd": "debit",
}
_NEW_YORK = ZoneInfo("America/New_York")


class _PairPreparationUnavailable(Exception):
    pass


async def advance_agent_version_slot(
    session: AsyncSession,
    *,
    evaluation_id: UUID,
    decision_slot_at: datetime,
    daily_limit_usd: Decimal,
    now: datetime,
) -> AgentVersionSlotResult:
    """Initialize equal internal accounts, then report the exact persisted slot state."""
    if (
        decision_slot_at.tzinfo is None
        or decision_slot_at.utcoffset() != UTC.utcoffset(decision_slot_at)
        or now.tzinfo is None
        or now.utcoffset() != UTC.utcoffset(now)
        or now < decision_slot_at
        or not daily_limit_usd.is_finite()
        or daily_limit_usd <= 0
    ):
        raise ValueError("AGENT_VERSION_SLOT_INVALID")
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 0))"),
        {"scope": f"nexus:agent-version-evaluation:{evaluation_id}"},
    )
    evaluation = await session.get(
        AgentVersionEvaluationRecord,
        evaluation_id,
        populate_existing=True,
    )
    if evaluation is None:
        raise ValueError("AGENT_VERSION_EVALUATION_UNKNOWN")
    phase = _phase(evaluation, decision_slot_at)
    pair = await session.scalar(
        select(AgentVersionCyclePairRecord).where(
            AgentVersionCyclePairRecord.evaluation_id == evaluation.id,
            AgentVersionCyclePairRecord.decision_slot_at == decision_slot_at,
        )
    )
    if pair is not None:
        return AgentVersionSlotResult(
            evaluation_id=evaluation.id,
            decision_slot_at=decision_slot_at,
            phase=phase,
            incumbent_cycle_id=pair.incumbent_cycle_id,
            challenger_cycle_id=pair.challenger_cycle_id,
            incumbent_run_id=pair.incumbent_run_id,
            challenger_run_id=pair.challenger_run_id,
            status="paired",
            reason_code="PAIR_RECORDED",
            replayed=True,
        )

    initialized = await _ensure_equal_internal_accounts(session, evaluation=evaluation)
    authorities = await _slot_authorities(
        session,
        evaluation=evaluation,
        decision_slot_at=decision_slot_at,
    )
    if authorities is None:
        return _incomparable(
            evaluation=evaluation,
            decision_slot_at=decision_slot_at,
            phase=phase,
            replayed=not initialized,
            reason_code="PAIR_AUTHORITY_UNAVAILABLE",
        )
    if decision_slot_at == evaluation.train_slots[0] and not await _equal_initial_state(
        session,
        evaluation=evaluation,
        decision_slot_at=decision_slot_at,
    ):
        return _incomparable(
            evaluation=evaluation,
            decision_slot_at=decision_slot_at,
            phase=phase,
            replayed=not initialized,
            reason_code="PAIR_START_STATE_MISMATCH",
        )

    try:
        async with session.begin_nested():
            incumbent = await start_schwab_agent_paper_cycle(
                session,
                authority=authorities[0],
                decision_slot_at=decision_slot_at,
                idempotency_key=(
                    f"agent-version:{evaluation.id}:incumbent:{decision_slot_at.isoformat()}"
                ),
                daily_limit_usd=daily_limit_usd,
                version_evaluation_id=evaluation.id,
                version_lane="incumbent",
                version_source_run_id=evaluation.incumbent_version_source_run_id,
            )
            challenger = await start_schwab_agent_paper_cycle(
                session,
                authority=authorities[1],
                decision_slot_at=decision_slot_at,
                idempotency_key=(
                    f"agent-version:{evaluation.id}:challenger:{decision_slot_at.isoformat()}"
                ),
                daily_limit_usd=daily_limit_usd,
                version_evaluation_id=evaluation.id,
                version_lane="challenger",
                version_prompt_id=evaluation.challenger_prompt_version_id,
                version_model_id=evaluation.challenger_model_config_id,
            )
            if (
                incumbent.status != "researching"
                or challenger.status != "researching"
                or len(incumbent.research_run_ids) != 1
                or len(challenger.research_run_ids) != 1
            ):
                raise _PairPreparationUnavailable
            await _validate_prepared_pair(
                session,
                evaluation=evaluation,
                incumbent_cycle_id=incumbent.cycle_id,
                challenger_cycle_id=challenger.cycle_id,
                incumbent_run_id=incumbent.research_run_ids[0],
                challenger_run_id=challenger.research_run_ids[0],
            )
    except _PairPreparationUnavailable:
        return _incomparable(
            evaluation=evaluation,
            decision_slot_at=decision_slot_at,
            phase=phase,
            replayed=not initialized,
            reason_code="PAIR_PREPARATION_UNAVAILABLE",
        )
    return AgentVersionSlotResult(
        evaluation_id=evaluation.id,
        decision_slot_at=decision_slot_at,
        phase=phase,
        incumbent_cycle_id=incumbent.cycle_id,
        challenger_cycle_id=challenger.cycle_id,
        incumbent_run_id=incumbent.research_run_ids[0],
        challenger_run_id=challenger.research_run_ids[0],
        status="running",
        reason_code="PAIR_JOBS_PREPARED",
        replayed=incumbent.replayed and challenger.replayed,
    )


def _phase(
    evaluation: AgentVersionEvaluationRecord, slot: datetime
) -> Literal["train", "oos"]:
    if slot in evaluation.train_slots:
        return "train"
    if slot in evaluation.oos_slots:
        return "oos"
    raise ValueError("AGENT_VERSION_SLOT_INVALID")


def _incomparable(
    *,
    evaluation: AgentVersionEvaluationRecord,
    decision_slot_at: datetime,
    phase: Literal["train", "oos"],
    replayed: bool,
    reason_code: str,
) -> AgentVersionSlotResult:
    return AgentVersionSlotResult(
        evaluation_id=evaluation.id,
        decision_slot_at=decision_slot_at,
        phase=phase,
        status="incomparable",
        reason_code=reason_code,
        replayed=replayed,
    )


async def _slot_authorities(
    session: AsyncSession,
    *,
    evaluation: AgentVersionEvaluationRecord,
    decision_slot_at: datetime,
) -> tuple[CycleAuthority, CycleAuthority] | None:
    decision_session = decision_slot_at.astimezone(_NEW_YORK).date()
    calendar = await session.scalar(
        select(ExchangeCalendarSessionRecord).where(
            ExchangeCalendarSessionRecord.calendar_authority_id
            == evaluation.calendar_authority_id,
            ExchangeCalendarSessionRecord.session == decision_session,
            ExchangeCalendarSessionRecord.market_open_at <= decision_slot_at,
            ExchangeCalendarSessionRecord.market_close_at >= decision_slot_at,
            ExchangeCalendarSessionRecord.created_at <= decision_slot_at,
        )
    )
    universe = await session.scalar(
        select(UniverseSnapshot)
        .where(
            UniverseSnapshot.key == "us_liquid",
            UniverseSnapshot.version == "us_liquid_v1",
            UniverseSnapshot.source == "fosun_local_read_only",
            UniverseSnapshot.effective_from <= decision_session,
            UniverseSnapshot.published_at <= decision_slot_at,
            UniverseSnapshot.created_at <= decision_slot_at,
        )
        .order_by(UniverseSnapshot.published_at.desc(), UniverseSnapshot.id.desc())
        .limit(1)
    )
    accounts = tuple(
        await session.scalars(
            select(PaperAccountRecord).where(
                PaperAccountRecord.id.in_(
                    (evaluation.incumbent_account_id, evaluation.challenger_account_id)
                )
            )
        )
    )
    account_by_id = {row.id: row for row in accounts}
    if calendar is None or universe is None or len(account_by_id) != 2:
        return None
    def authority(account_id: UUID) -> CycleAuthority:
        return CycleAuthority(
            account_id=account_id,
            portfolio_id=account_by_id[account_id].portfolio_id,
            decision_session=decision_session,
            decision_time=decision_slot_at,
            execution_session=decision_session,
            calendar_authority_id=evaluation.calendar_authority_id,
            universe_snapshot_id=universe.id,
            quality_report_id=None,
        )

    return (
        authority(evaluation.incumbent_account_id),
        authority(evaluation.challenger_account_id),
    )


async def _equal_initial_state(
    session: AsyncSession,
    *,
    evaluation: AgentVersionEvaluationRecord,
    decision_slot_at: datetime,
) -> bool:
    incumbent = await load_paper_projection(
        session,
        account_id=evaluation.incumbent_account_id,
        as_of=decision_slot_at,
    )
    challenger = await load_paper_projection(
        session,
        account_id=evaluation.challenger_account_id,
        as_of=decision_slot_at,
    )
    return _neutral_projection(incumbent) == _neutral_projection(challenger)


def _neutral_projection(projection: PaperPortfolioProjection) -> tuple[object, ...]:
    return (
        projection.available_cash,
        projection.reserved_cash,
        projection.positions,
        projection.open_orders,
        projection.nav,
        projection.peak_nav,
        projection.drawdown_state,
    )


async def _validate_prepared_pair(
    session: AsyncSession,
    *,
    evaluation: AgentVersionEvaluationRecord,
    incumbent_cycle_id: UUID,
    challenger_cycle_id: UUID,
    incumbent_run_id: UUID,
    challenger_run_id: UUID,
) -> None:
    incumbent_cycle = await session.get(AgentPaperCycleRecord, incumbent_cycle_id)
    challenger_cycle = await session.get(AgentPaperCycleRecord, challenger_cycle_id)
    incumbent_run = await session.get(AgentResearchRunRecord, incumbent_run_id)
    challenger_run = await session.get(AgentResearchRunRecord, challenger_run_id)
    if (
        incumbent_cycle is None
        or challenger_cycle is None
        or incumbent_run is None
        or challenger_run is None
        or incumbent_cycle.candidate_hash != challenger_cycle.candidate_hash
        or incumbent_cycle.candidate_manifest != challenger_cycle.candidate_manifest
        or incumbent_cycle.sizing_policy_hash != challenger_cycle.sizing_policy_hash
        or incumbent_run.actor_id != AGENT_VERSION_INCUMBENT_ACTOR
        or challenger_run.actor_id != AGENT_VERSION_CHALLENGER_ACTOR
        or incumbent_run.prompt_hash != evaluation.incumbent_prompt_hash
        or incumbent_run.model_hash != evaluation.incumbent_model_hash
        or challenger_run.prompt_hash != evaluation.challenger_prompt_hash
        or challenger_run.model_hash != evaluation.challenger_model_hash
        or challenger_run.prompt_version_id != evaluation.challenger_prompt_version_id
        or challenger_run.model_config_id != evaluation.challenger_model_config_id
        or _non_context_evidence(incumbent_run)
        != _non_context_evidence(challenger_run)
    ):
        raise _PairPreparationUnavailable


def _non_context_evidence(run: AgentResearchRunRecord) -> str:
    manifest = [
        row
        for row in run.evidence_manifest
        if isinstance(row, dict) and row.get("source") != "nexus_agent_paper_context"
    ]
    return hashlib.sha256(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


async def _ensure_equal_internal_accounts(
    session: AsyncSession,
    *,
    evaluation: AgentVersionEvaluationRecord,
) -> bool:
    account_ids = (evaluation.incumbent_account_id, evaluation.challenger_account_id)
    ledger_accounts = tuple(
        await session.scalars(
            select(PaperLedgerAccountRecord).where(
                PaperLedgerAccountRecord.paper_account_id.in_(account_ids)
            )
        )
    )
    transactions = tuple(
        await session.scalars(
            select(PaperLedgerTransactionRecord).where(
                PaperLedgerTransactionRecord.account_id.in_(account_ids)
            )
        )
    )
    transaction_ids = tuple(row.id for row in transactions)
    entries = (
        tuple(
            await session.scalars(
                select(PaperLedgerEntryRecord).where(
                    PaperLedgerEntryRecord.transaction_id.in_(transaction_ids)
                )
            )
        )
        if transaction_ids
        else ()
    )
    events = tuple(
        await session.scalars(
            select(PaperDrawdownLockEventRecord).where(
                PaperDrawdownLockEventRecord.account_id.in_(account_ids)
            )
        )
    )
    if ledger_accounts or transactions or entries or events:
        _validate_existing_initialization(
            account_ids=account_ids,
            ledger_accounts=ledger_accounts,
            transactions=transactions,
            entries=entries,
            events=events,
        )
        return False

    account_rows: dict[UUID, dict[str, PaperLedgerAccountRecord]] = {}
    for account_id in account_ids:
        rows = {
            code: PaperLedgerAccountRecord(
                id=new_uuid7(),
                paper_account_id=account_id,
                code=code,
                normal_side=normal_side,
                currency="USD",
            )
            for code, normal_side in _LEDGER_CATALOG.items()
        }
        account_rows[account_id] = rows
        session.add_all(rows.values())
    await session.flush()

    for account_id in account_ids:
        await _initialize_account(
            session,
            evaluation=evaluation,
            account_id=account_id,
            ledger_accounts=account_rows[account_id],
        )
    await session.flush()
    return True


async def _initialize_account(
    session: AsyncSession,
    *,
    evaluation: AgentVersionEvaluationRecord,
    account_id: UUID,
    ledger_accounts: dict[str, PaperLedgerAccountRecord],
) -> None:
    amount = await session.scalar(
        text("SELECT initial_capital FROM paper_accounts WHERE id=:account_id"),
        {"account_id": account_id},
    )
    capital = Decimal(str(amount))
    transaction_id = new_uuid7()
    entry_ids = (new_uuid7(), new_uuid7())
    reference_hash = _hash(
        f"agent_version_initial_funding_v1|{evaluation.id}|{account_id}|{capital}"
    )
    draft = LedgerTransaction(
        transaction_id=transaction_id,
        account_id=account_id,
        transaction_type="funding",
        reference_type="initial_funding",
        reference_hash=reference_hash,
        occurred_at=evaluation.created_at,
        entries=(
            LedgerEntry(
                entry_id=entry_ids[0],
                entry_seq=1,
                account_code="asset.cash.available.usd",
                currency="USD",
                debit_amount=capital,
                credit_amount=Decimal(0),
            ),
            LedgerEntry(
                entry_id=entry_ids[1],
                entry_seq=2,
                account_code="equity.contributed_capital.usd",
                currency="USD",
                debit_amount=Decimal(0),
                credit_amount=capital,
            ),
        ),
        transaction_hash="0" * 64,
    )
    transaction_hash = canonical_paper_ledger_transaction_hash(draft)
    session.add(
        PaperLedgerTransactionRecord(
            id=transaction_id,
            account_id=account_id,
            order_id=None,
            transaction_type="funding",
            reference_type="initial_funding",
            reference_hash=reference_hash,
            occurred_at=evaluation.created_at,
            transaction_hash=transaction_hash,
        )
    )
    await session.flush()
    session.add_all(
        PaperLedgerEntryRecord(
            id=entry.entry_id,
            transaction_id=transaction_id,
            account_id=account_id,
            entry_seq=entry.entry_seq,
            ledger_account_id=ledger_accounts[entry.account_code].id,
            account_code=entry.account_code,
            currency="USD",
            debit_amount=entry.debit_amount,
            credit_amount=entry.credit_amount,
        )
        for entry in draft.entries
    )
    session.add(
        PaperDrawdownLockEventRecord(
            id=new_uuid7(),
            account_id=account_id,
            event_seq=1,
            from_state=None,
            to_state="unlocked",
            drawdown_ratio=Decimal(0),
            user_acknowledged=False,
            reason_code="PAPER_ACCOUNT_INITIALIZED",
            occurred_at=evaluation.created_at,
            payload_hash=_hash(
                f"agent_version_initial_drawdown_v1|{evaluation.id}|{account_id}"
            ),
        )
    )


def _validate_existing_initialization(
    *,
    account_ids: tuple[UUID, UUID],
    ledger_accounts: tuple[PaperLedgerAccountRecord, ...],
    transactions: tuple[PaperLedgerTransactionRecord, ...],
    entries: tuple[PaperLedgerEntryRecord, ...],
    events: tuple[PaperDrawdownLockEventRecord, ...],
) -> None:
    for account_id in account_ids:
        catalog = {
            row.code: row.normal_side
            for row in ledger_accounts
            if row.paper_account_id == account_id
        }
        funding = tuple(row for row in transactions if row.account_id == account_id)
        account_entries = tuple(row for row in entries if row.account_id == account_id)
        account_events = tuple(row for row in events if row.account_id == account_id)
        if (
            catalog != _LEDGER_CATALOG
            or len(funding) != 1
            or funding[0].transaction_type != "funding"
            or funding[0].reference_type != "initial_funding"
            or len(account_entries) != 2
            or len(account_events) != 1
            or account_events[0].event_seq != 1
            or account_events[0].from_state is not None
            or account_events[0].to_state != "unlocked"
        ):
            raise ValueError("AGENT_VERSION_ACCOUNT_INITIALIZATION_INVALID")


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()
