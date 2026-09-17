"""Persisted evidence projection for user-owned live evaluation."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.api.schemas.live_evaluation import (
    LiveEvaluationEvidenceCountResponse,
    LiveEvaluationGateResponse,
    LiveEvaluationResponse,
)

_COUNTS_SQL = text(
    """
    SELECT
      (SELECT count(DISTINCT orders.id)
         FROM paper_orders orders
         JOIN strategy_decisions decisions ON decisions.id = orders.strategy_decision_id
        WHERE decisions.decision_origin = 'agent_research') AS agent_orders,
      (SELECT count(DISTINCT fills.id)
         FROM paper_fills fills
         JOIN paper_orders orders ON orders.id = fills.order_id
         JOIN strategy_decisions decisions ON decisions.id = orders.strategy_decision_id
        WHERE decisions.decision_origin = 'agent_research') AS agent_fills,
      (SELECT count(DISTINCT fills.id)
         FROM paper_fills fills
         JOIN paper_orders orders ON orders.id = fills.order_id
         JOIN strategy_decisions decisions ON decisions.id = orders.strategy_decision_id
        WHERE decisions.decision_origin = 'agent_research'
          AND EXISTS (
            SELECT 1
              FROM paper_ledger_entries entries
             WHERE entries.transaction_id = fills.ledger_transaction_id
             GROUP BY entries.transaction_id
            HAVING count(*) >= 2
               AND sum(entries.debit_amount) > 0
               AND sum(entries.debit_amount) = sum(entries.credit_amount)
          )) AS balanced_agent_fills,
      (SELECT count(*) FROM agent_version_evaluations) AS version_evaluations,
      (SELECT count(*) FROM agent_version_evaluation_decisions
        WHERE status = 'PASS') AS pass_decisions,
      (SELECT count(*) FROM broker_account_bindings
        WHERE reconciliation_status = 'matched' AND NOT locked
          AND capability_expires_at > :now) AS matched_bindings,
      (SELECT count(*) FROM broker_execution_intents
        WHERE status IN ('completed','reconciled')) AS completed_intents,
      (SELECT count(*) FROM broker_order_events
        WHERE order_status = 'filled') AS filled_order_events,
      (SELECT count(*) FROM broker_reconciliation_runs
        WHERE status = 'matched') AS matched_reconciliations,
      (SELECT count(*) FROM broker_command_attempts
        WHERE safe_outcome_code = 'result_unknown') AS result_unknown_attempts,
      (SELECT count(*)
         FROM broker_command_attempts attempts
         JOIN broker_commands commands ON commands.id = attempts.command_id
        WHERE attempts.safe_outcome_code = 'result_unknown'
          AND commands.status = 'reconciled') AS reconciled_unknown_commands
    """
)


class LiveEvaluationReadService:
    def __init__(self, session: AsyncSession, request_time: datetime) -> None:
        if request_time.tzinfo is None or request_time.utcoffset() is None:
            raise ValueError("request_time must be timezone-aware")
        self._session = session
        self._request_time = request_time.astimezone(UTC)

    async def read(self) -> LiveEvaluationResponse:
        row = (await self._session.execute(_COUNTS_SQL, {"now": self._request_time})).one()
        counts = {key: int(value) for key, value in row._mapping.items()}
        gates = (
            _gate(
                key="internal_paper_execution",
                ready=(
                    counts["agent_orders"] > 0
                    and counts["agent_fills"] > 0
                    and counts["balanced_agent_fills"] == counts["agent_fills"]
                ),
                ready_reason="AGENT_PAPER_EXECUTION_EVIDENCE_READY",
                missing_reason="AGENT_PAPER_EXECUTION_EVIDENCE_MISSING",
                evidence=(
                    ("agent_orders", counts["agent_orders"]),
                    ("agent_fills", counts["agent_fills"]),
                    ("balanced_agent_fills", counts["balanced_agent_fills"]),
                ),
            ),
            _gate(
                key="oos_agent_version",
                ready=counts["pass_decisions"] > 0,
                ready_reason="AGENT_VERSION_PASS_READY",
                missing_reason="AGENT_VERSION_PASS_MISSING",
                evidence=(
                    ("version_evaluations", counts["version_evaluations"]),
                    ("pass_decisions", counts["pass_decisions"]),
                ),
            ),
            _gate(
                key="broker_simulation_lifecycle",
                ready=all(
                    counts[key] > 0
                    for key in (
                        "matched_bindings",
                        "completed_intents",
                        "filled_order_events",
                        "matched_reconciliations",
                    )
                ),
                ready_reason="BROKER_SIMULATION_LIFECYCLE_READY",
                missing_reason="BROKER_SIMULATION_LIFECYCLE_MISSING",
                evidence=(
                    ("matched_bindings", counts["matched_bindings"]),
                    ("completed_intents", counts["completed_intents"]),
                    ("filled_order_events", counts["filled_order_events"]),
                    ("matched_reconciliations", counts["matched_reconciliations"]),
                ),
            ),
            LiveEvaluationGateResponse(
                key="broker_fee_variance",
                status="unknown",
                reason_code="BROKER_FEE_EVIDENCE_UNSUPPORTED",
                evidence=(
                    LiveEvaluationEvidenceCountResponse(
                        key="broker_fee_observations",
                        count=0,
                    ),
                ),
            ),
            _gate(
                key="broker_recovery_drill",
                ready=(
                    counts["result_unknown_attempts"] > 0
                    and counts["reconciled_unknown_commands"]
                    == counts["result_unknown_attempts"]
                ),
                ready_reason="BROKER_RECOVERY_EVIDENCE_READY",
                missing_reason="BROKER_RECOVERY_EVIDENCE_MISSING",
                evidence=(
                    ("result_unknown_attempts", counts["result_unknown_attempts"]),
                    (
                        "reconciled_unknown_commands",
                        counts["reconciled_unknown_commands"],
                    ),
                ),
            ),
        )
        status: Literal["not_ready", "eligible_for_user_review"] = (
            "eligible_for_user_review"
            if all(gate.status == "ready" for gate in gates)
            else "not_ready"
        )
        return LiveEvaluationResponse(
            status=status,
            execution_locked=True,
            live_write=False,
            user_decision_required=True,
            gates=gates,
        )


def _gate(
    *,
    key: Literal[
        "internal_paper_execution",
        "oos_agent_version",
        "broker_simulation_lifecycle",
        "broker_recovery_drill",
    ],
    ready: bool,
    ready_reason: str,
    missing_reason: str,
    evidence: tuple[tuple[str, int], ...],
) -> LiveEvaluationGateResponse:
    return LiveEvaluationGateResponse(
        key=key,
        status="ready" if ready else "not_ready",
        reason_code=ready_reason if ready else missing_reason,
        evidence=tuple(
            LiveEvaluationEvidenceCountResponse(key=name, count=count)
            for name, count in evidence
        ),
    )
