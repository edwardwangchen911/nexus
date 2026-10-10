"""Normal isolated economic registration; no cycle, budget or execution activation."""

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.domain.ids import new_uuid7
from nexus.paper.economic_version_binding import ACTION as BINDING_ACTION
from nexus.paper.economic_version_binding import decode_binding
from nexus.paper.ledger import _EXPECTED_NORMAL_SIDES
from nexus.paper.models import LedgerEntry, LedgerTransaction
from nexus.paper.portfolio_economic_preflight import _hash
from nexus.paper.portfolio_economic_protocol import (
    PROTOCOL_ACTION,
    decode_protocol,
    require_economic_database_target,
    require_isolated_database,
)
from nexus.paper.projection import load_paper_projection
from nexus.paper.target_service import (
    approve_initial_target_projection,
    load_initial_approval_projection,
)
from nexus.persistence.models.economic import EconomicEvaluationRecord, EconomicPairSlotRecord
from nexus.persistence.models.paper import (
    PaperAccountRecord,
    PaperDrawdownLockEventRecord,
    PaperInitialTargetApprovalRecord,
    PaperLedgerAccountRecord,
    PaperLedgerEntryRecord,
    PaperLedgerTransactionRecord,
)
from nexus.persistence.models.portfolio import Account, Portfolio
from nexus.persistence.models.runtime import AuditEvent
from nexus.risk.canonical import canonical_paper_ledger_transaction_hash

_ACTOR = "local-economic-cycle-cli"


async def register_economic_identity(
    session: AsyncSession,
    *,
    protocol_hash: str,
    version_binding_hash: str,
    expected_database: str,
) -> EconomicEvaluationRecord:
    """Freeze fresh synthetic accounts and baseline in the caller's atomic transaction."""
    await require_economic_database_target(session, expected=expected_database)
    for value in (protocol_hash, version_binding_hash):
        if len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
            raise ValueError("ECONOMIC_PAIR_INPUT_INVALID")
    await session.execute(
        text(
            "SELECT pg_advisory_xact_lock(hashtextextended('portfolio-economic-protocol-v1',2026))"
        )
    )
    existing = await session.scalar(
        select(EconomicEvaluationRecord).where(
            EconomicEvaluationRecord.protocol_hash == protocol_hash
        )
    )
    if existing is not None and existing.binding_hash != version_binding_hash:
        raise ValueError("ECONOMIC_PAIR_IDEMPOTENCY_CONFLICT")
    now = await session.scalar(select(func.clock_timestamp()))
    assert isinstance(now, datetime)
    protocols = tuple(
        await session.scalars(
            select(AuditEvent)
            .where(
                AuditEvent.action == PROTOCOL_ACTION,
                AuditEvent.occurred_at <= now,
                AuditEvent.created_at <= now,
            )
            .limit(2)
        )
    )
    bindings = tuple(
        await session.scalars(
            select(AuditEvent)
            .where(
                AuditEvent.action == BINDING_ACTION,
                AuditEvent.occurred_at <= now,
                AuditEvent.created_at <= now,
            )
            .limit(2)
        )
    )
    if len(protocols) != 1 or len(bindings) != 1:
        raise ValueError("ECONOMIC_PAIR_AUTHORITY_UNAVAILABLE")
    protocol, binding = decode_protocol(protocols[0]), decode_binding(bindings[0])
    if (
        protocol.content_hash != protocol_hash
        or binding.content_hash != version_binding_hash
        or binding.protocol_hash != protocol_hash
    ):
        raise ValueError("ECONOMIC_PAIR_AUTHORITY_UNAVAILABLE")
    if existing is not None:
        if (
            existing.protocol_event_id != protocols[0].id
            or existing.binding_event_id != bindings[0].id
        ):
            raise ValueError("ECONOMIC_PAIR_IDEMPOTENCY_CONFLICT")
        slots = tuple(
            await session.scalars(
                select(EconomicPairSlotRecord)
                .where(EconomicPairSlotRecord.evaluation_id == existing.id)
                .order_by(EconomicPairSlotRecord.ordinal)
            )
        )
        if len(slots) != 24 or any(
            (
                stored.ordinal,
                stored.session,
                stored.decision_at,
                stored.cutoff_at,
                stored.market_close_at,
                stored.execution_locked,
            )
            != (
                frozen.ordinal,
                frozen.session,
                frozen.decision_at,
                frozen.decision_at,
                frozen.market_close_at,
                True,
            )
            for stored, frozen in zip(slots, protocol.slots, strict=True)
        ):
            raise ValueError("ECONOMIC_PAIR_EXISTING_INVALID")
        for account_id, portfolio_id in (
            (existing.incumbent_account_id, existing.incumbent_portfolio_id),
            (existing.challenger_account_id, existing.challenger_portfolio_id),
        ):
            approval = await session.scalar(
                select(PaperInitialTargetApprovalRecord).where(
                    PaperInitialTargetApprovalRecord.account_id == account_id
                )
            )
            if approval is None:
                raise ValueError("ECONOMIC_PAIR_EXISTING_INVALID")
            await load_initial_approval_projection(
                session, approval=approval, portfolio_id=portfolio_id, decision_time=now, now=now
            )
        return existing
    await require_isolated_database(session, expected=expected_database)
    if now >= protocol.slots[0].decision_at:
        raise ValueError("ECONOMIC_PAIR_FIRST_SLOT_MISSED")
    evaluation_id = new_uuid7()
    lanes = []
    for lane in ("incumbent", "challenger"):
        account_id, portfolio_id, paper_id = new_uuid7(), new_uuid7(), new_uuid7()
        session.add(Account(id=account_id, provider="internal", status="ACTIVE", created_at=now))
        await session.flush()
        session.add(
            Portfolio(
                id=portfolio_id, account_id=account_id, name=f"Economic {lane}", created_at=now
            )
        )
        await session.flush()
        session.add(
            PaperAccountRecord(
                id=paper_id,
                account_id=account_id,
                portfolio_id=portfolio_id,
                code=f"economic:{evaluation_id.hex}:{lane[0]}",
                currency="USD",
                account_model="cash",
                position_model="long_only",
                initial_capital=Decimal(10000),
                capability="simulation_write_internal",
                created_at=now,
            )
        )
        await session.flush()
        opening_id = await _initialize_lane(
            session, evaluation_id=evaluation_id, account_id=paper_id, now=now
        )
        projection = await load_paper_projection(session, account_id=paper_id, as_of=now)
        await approve_initial_target_projection(
            session,
            account_id=paper_id,
            projection_as_of=now,
            expected_projection_hash=projection.projection_hash,
            approved_by=_ACTOR,
            approved_at=now,
            idempotency_key=f"economic-baseline:{evaluation_id}:{lane}",
        )
        lanes.append((paper_id, portfolio_id, opening_id))
    identity = EconomicEvaluationRecord(
        id=evaluation_id,
        protocol_event_id=protocols[0].id,
        binding_event_id=bindings[0].id,
        protocol_hash=protocol_hash,
        binding_hash=version_binding_hash,
        incumbent_account_id=lanes[0][0],
        incumbent_portfolio_id=lanes[0][1],
        incumbent_opening_id=lanes[0][2],
        challenger_account_id=lanes[1][0],
        challenger_portfolio_id=lanes[1][1],
        challenger_opening_id=lanes[1][2],
        created_at=now,
    )
    session.add(identity)
    await session.flush()
    session.add_all(
        EconomicPairSlotRecord(
            id=new_uuid7(),
            evaluation_id=evaluation_id,
            ordinal=slot.ordinal,
            session=slot.session,
            decision_at=slot.decision_at,
            cutoff_at=slot.decision_at,
            market_close_at=slot.market_close_at,
            execution_locked=True,
            created_at=now,
        )
        for slot in protocol.slots
    )
    await session.flush()
    await append_audit_event(
        session,
        actor_id=_ACTOR,
        action="portfolio.economic.identity_registered",
        schema_version="portfolio_economic_identity_v1",
        occurred_at=now,
        payload={
            "evaluation_id": str(evaluation_id),
            "protocol_hash": protocol_hash,
            "binding_hash": version_binding_hash,
            "planned_slots": 24,
            "lane_count": 2,
            "status": "registered_not_started",
            "execution_locked": True,
        },
    )
    return identity


async def _initialize_lane(
    session: AsyncSession, *, evaluation_id: UUID, account_id: UUID, now: datetime
) -> UUID:
    catalog = {
        code: PaperLedgerAccountRecord(
            id=new_uuid7(),
            paper_account_id=account_id,
            code=code,
            normal_side=side,
            currency="USD",
            created_at=now,
        )
        for code, side in _EXPECTED_NORMAL_SIDES.items()
    }
    session.add_all(catalog.values())
    await session.flush()
    opening_id = new_uuid7()
    reference_hash = _hash(
        {
            "schema_version": "economic_initial_funding_v1",
            "evaluation_id": str(evaluation_id),
            "account_id": str(account_id),
            "capital_usd": "10000",
        }
    )
    draft = LedgerTransaction(
        transaction_id=opening_id,
        account_id=account_id,
        transaction_type="funding",
        reference_type="initial_funding",
        reference_hash=reference_hash,
        occurred_at=now,
        entries=(
            LedgerEntry(
                entry_id=new_uuid7(),
                entry_seq=1,
                account_code="asset.cash.available.usd",
                currency="USD",
                debit_amount=Decimal(10000),
                credit_amount=Decimal(0),
            ),
            LedgerEntry(
                entry_id=new_uuid7(),
                entry_seq=2,
                account_code="equity.contributed_capital.usd",
                currency="USD",
                debit_amount=Decimal(0),
                credit_amount=Decimal(10000),
            ),
        ),
        transaction_hash="0" * 64,
    )
    session.add(
        PaperLedgerTransactionRecord(
            id=opening_id,
            account_id=account_id,
            order_id=None,
            transaction_type=draft.transaction_type,
            reference_type=draft.reference_type,
            reference_hash=reference_hash,
            occurred_at=now,
            transaction_hash=canonical_paper_ledger_transaction_hash(draft),
            created_at=now,
        )
    )
    await session.flush()
    session.add_all(
        PaperLedgerEntryRecord(
            id=entry.entry_id,
            transaction_id=opening_id,
            account_id=account_id,
            entry_seq=entry.entry_seq,
            ledger_account_id=catalog[entry.account_code].id,
            account_code=entry.account_code,
            currency="USD",
            debit_amount=entry.debit_amount,
            credit_amount=entry.credit_amount,
            created_at=now,
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
            occurred_at=now,
            created_at=now,
            payload_hash=_hash(
                {
                    "schema_version": "economic_initial_drawdown_v1",
                    "evaluation_id": str(evaluation_id),
                    "account_id": str(account_id),
                }
            ),
        )
    )
    await session.flush()
    return opening_id
