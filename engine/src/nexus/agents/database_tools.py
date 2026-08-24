"""Strict database-backed tools bound to one frozen Agent research run."""

from collections.abc import Mapping
from datetime import date, datetime
from typing import cast

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.execution_authority import load_frozen_evidence
from nexus.agents.tools import ReadOnlyTools, ToolExecutor, ToolInputError
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.data import MacroObservation
from nexus.persistence.models.research import Filing, FinancialFactRecord, ResearchEvidenceRef


async def database_read_only_tools(
    session: AsyncSession,
    run: AgentResearchRunRecord,
    *,
    allow_fixture_evidence: bool,
) -> ReadOnlyTools:
    """Build all fixed executors from immutable run authority, never caller authority."""
    evidence = await load_frozen_evidence(
        session, run, allow_fixture_evidence=allow_fixture_evidence
    )
    if not evidence:
        raise ToolInputError("frozen evidence authority is unavailable")
    bound = _BoundDatabaseExecutors(session, run, evidence)
    return ReadOnlyTools(executors=bound.executors)


class _BoundDatabaseExecutors:
    def __init__(
        self,
        session: AsyncSession,
        run: AgentResearchRunRecord,
        evidence: tuple[ResearchEvidenceRef, ...],
    ) -> None:
        self._session = session
        self._run_id = run.id
        self._instrument_id = run.instrument_id
        self._snapshot_id = run.data_snapshot_id
        self._decision_time = run.decision_time
        self._evidence_hash = run.evidence_hash
        self._evidence_ids = tuple(record.id for record in evidence)
        self._evidence = evidence

    @property
    def executors(self) -> Mapping[str, ToolExecutor]:
        return {
            "get_security_snapshot": self.security_snapshot,
            "get_financial_facts": self.financial_facts,
            "search_filing_evidence": self.filing_evidence,
            "get_price_features": self.price_features,
            "get_macro_snapshot": self.macro_snapshot,
            "get_portfolio_exposure": self.portfolio_unavailable,
            "get_portfolio_diagnostics": self.portfolio_unavailable,
            "get_proposal_comparison": self.portfolio_unavailable,
        }

    async def security_snapshot(self, payload: Mapping[str, object]) -> Mapping[str, object]:
        await self._require_snapshot_scope(payload)
        return self._result(
            "get_security_snapshot",
            [
                {
                    "evidence_ref_id": str(record.id),
                    "source": record.source,
                    "provider_object_id": record.provider_object_id,
                    "revision": record.revision,
                    "available_at": _timestamp(record.available_at),
                    "content_hash": record.content_hash,
                }
                for record in self._evidence
            ],
        )

    async def financial_facts(self, payload: Mapping[str, object]) -> Mapping[str, object]:
        await self._require_snapshot_scope(payload)
        fields = cast(tuple[str, ...], payload["fields"])
        records = tuple(
            await self._session.scalars(
                select(FinancialFactRecord)
                .where(
                    FinancialFactRecord.instrument_id == self._instrument_id,
                    FinancialFactRecord.evidence_ref_id.in_(self._evidence_ids),
                    FinancialFactRecord.accepted_at <= self._decision_time,
                    FinancialFactRecord.canonical_fact.in_(fields),
                )
                .order_by(
                    FinancialFactRecord.canonical_fact,
                    FinancialFactRecord.period_end.desc(),
                    FinancialFactRecord.id,
                )
                .limit(50)
            )
        )
        return self._result(
            "get_financial_facts",
            [
                {
                    "evidence_ref_id": str(record.evidence_ref_id),
                    "fact": record.canonical_fact,
                    "value": format(record.value, "f"),
                    "unit": record.unit,
                    "period_end": record.period_end.isoformat(),
                    "accepted_at": _timestamp(record.accepted_at),
                    "revision": record.revision,
                }
                for record in records
            ],
        )

    async def filing_evidence(self, payload: Mapping[str, object]) -> Mapping[str, object]:
        await self._require_snapshot_scope(payload)
        forms = cast(tuple[str, ...], payload["forms"])
        filed_from = payload["filed_from"]
        filed_to = payload["filed_to"]
        records = tuple(
            await self._session.scalars(
                select(Filing)
                .where(
                    Filing.instrument_id == self._instrument_id,
                    Filing.evidence_ref_id.in_(self._evidence_ids),
                    Filing.accepted_at <= self._decision_time,
                    Filing.form.in_(forms),
                    Filing.filed_at
                    >= datetime.combine(
                        cast(date, filed_from),
                        datetime.min.time(),
                        tzinfo=self._decision_time.tzinfo,
                    ),
                    Filing.filed_at
                    < datetime.combine(
                        cast(date, filed_to),
                        datetime.max.time(),
                        tzinfo=self._decision_time.tzinfo,
                    ),
                )
                .order_by(Filing.accepted_at.desc(), Filing.id)
                .limit(20)
            )
        )
        return self._result(
            "search_filing_evidence",
            [
                {
                    "evidence_ref_id": str(record.evidence_ref_id),
                    "accession_number": record.accession_number,
                    "form": record.form,
                    "filed_at": _timestamp(record.filed_at),
                    "accepted_at": _timestamp(record.accepted_at),
                    "revision": record.revision,
                }
                for record in records
            ],
        )

    async def price_features(self, payload: Mapping[str, object]) -> Mapping[str, object]:
        await self._require_snapshot_scope(payload)
        return self._result(
            "get_price_features",
            [],
            status="unavailable",
            reason="no frozen price-feature authority in this run",
        )

    async def macro_snapshot(self, payload: Mapping[str, object]) -> Mapping[str, object]:
        await self._require_common_scope(payload)
        if payload["data_snapshot_id"] != self._snapshot_id:
            raise ToolInputError("tool request exceeds frozen snapshot authority")
        series = cast(tuple[str, ...], payload["series"])
        records = tuple(
            await self._session.scalars(
                select(MacroObservation)
                .where(
                    MacroObservation.available_at <= self._decision_time,
                    MacroObservation.observed_at <= self._decision_time,
                    MacroObservation.series.in_(series),
                    or_(
                        MacroObservation.evidence_ref_id.in_(self._evidence_ids),
                        MacroObservation.calendar_evidence_ref_id.in_(self._evidence_ids),
                    ),
                )
                .order_by(MacroObservation.series, MacroObservation.observed_at.desc())
                .limit(50)
            )
        )
        return self._result(
            "get_macro_snapshot",
            [
                {
                    "series": record.series,
                    "value": format(record.value, "f"),
                    "unit": record.unit,
                    "observed_at": _timestamp(record.observed_at),
                    "available_at": _timestamp(record.available_at),
                    "revision": record.revision,
                    "evidence_ref_id": (
                        None if record.evidence_ref_id is None else str(record.evidence_ref_id)
                    ),
                }
                for record in records
            ],
        )

    async def portfolio_unavailable(self, payload: Mapping[str, object]) -> Mapping[str, object]:
        await self._require_common_scope(payload)
        raise ToolInputError("portfolio authority is not frozen in an instrument research run")

    async def _require_snapshot_scope(self, payload: Mapping[str, object]) -> None:
        await self._require_common_scope(payload)
        if (
            payload["instrument_id"] != self._instrument_id
            or payload["data_snapshot_id"] != self._snapshot_id
        ):
            raise ToolInputError("tool request exceeds frozen instrument snapshot authority")

    async def _require_common_scope(self, payload: Mapping[str, object]) -> None:
        if payload["as_of"] != self._decision_time:
            raise ToolInputError("tool request exceeds frozen decision-time authority")
        current = await self._session.get(AgentResearchRunRecord, self._run_id)
        if (
            current is None
            or current.instrument_id != self._instrument_id
            or current.data_snapshot_id != self._snapshot_id
            or current.decision_time != self._decision_time
            or current.evidence_hash != self._evidence_hash
        ):
            raise ToolInputError("frozen run authority is unavailable")

    def _result(
        self,
        tool: str,
        items: list[dict[str, object]],
        *,
        status: str = "ok",
        reason: str | None = None,
    ) -> Mapping[str, object]:
        return {
            "schema_version": "agent_read_only_tool_result_v1",
            "tool": tool,
            "instrument_id": str(self._instrument_id),
            "data_snapshot_id": str(self._snapshot_id),
            "as_of": _timestamp(self._decision_time),
            "evidence_hash": self._evidence_hash,
            "status": status,
            "reason": reason,
            "items": items,
        }


def _timestamp(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")
