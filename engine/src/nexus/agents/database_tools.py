"""Strict database-backed tools bound to one frozen Agent research run."""

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import cast
from uuid import UUID

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.execution_authority import MAX_FROZEN_EVIDENCE, load_frozen_evidence
from nexus.agents.portfolio_context import PortfolioResearchContext, read_portfolio_context
from nexus.agents.price_features import compute_price_features
from nexus.agents.proposal_context import assess_frozen_comparison
from nexus.agents.tools import ReadOnlyTools, ToolExecutor, ToolInputError
from nexus.market.price_bar_authority import (
    HASH_VERSION,
    HASH_VERSION_V2,
    POLICY_VERSION,
    POLICY_VERSION_V2,
    SOURCE_POLICY_ID,
    SOURCE_POLICY_V2_ID,
    prepare_price_bar_set,
)
from nexus.persistence.models.agents import (
    AgentDataSnapshotEvidenceRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import (
    MacroObservation,
    PriceBarRecord,
    PriceBarSetAuthority,
    PriceSeriesManifest,
)
from nexus.persistence.models.data import (
    SourcePolicy as SourcePolicyRecord,
)
from nexus.persistence.models.research import Filing, FinancialFactRecord, ResearchEvidenceRef
from nexus.quality.source_policy import (
    SOURCE_POLICY_KEY,
    SOURCE_POLICY_PAYLOAD,
    SOURCE_POLICY_V2_PAYLOAD,
    SOURCE_POLICY_V2_VERSION,
    SOURCE_POLICY_VERSION,
)
from nexus.research.alpaca_actions_storage import action_tool_payload, read_action_observations
from nexus.research.fmp_profile_storage import read_company_profiles
from nexus.research.sec_disclosures import read_disclosures
from nexus.research.ssga_distributions import read_distributions
from nexus.research.ssga_fund_observations import read_fund_observations
from nexus.research.ssga_nav_storage import nav_tool_payload, read_nav_histories


async def database_read_only_tools(
    session: AsyncSession,
    run: AgentResearchRunRecord,
    *,
    allow_fixture_evidence: bool,
    allow_fixture_price_evidence: bool = False,
) -> ReadOnlyTools:
    """Build all fixed executors from immutable run authority, never caller authority."""
    frozen_evidence = _frozen_evidence_authority(run, allow_fixture_evidence=allow_fixture_evidence)
    if frozen_evidence is None:
        raise ToolInputError("frozen evidence authority is unavailable")
    evidence = await load_frozen_evidence(
        session, run, allow_fixture_evidence=allow_fixture_evidence
    )
    bound = _BoundDatabaseExecutors(
        session,
        run,
        frozen_evidence,
        evidence,
        allow_fixture_price_evidence=allow_fixture_price_evidence,
        portfolio_context=await read_portfolio_context(
            session,
            run.data_snapshot_id,
            run.instrument_id,
            run.decision_time,
        ),
    )
    return ReadOnlyTools(executors=bound.executors)


@dataclass(frozen=True)
class _FrozenEvidenceAuthority:
    id: UUID
    source: str
    provider_object_id: str
    revision: str
    available_at: datetime
    location: str
    content_hash: str


def _context_payload(context: PortfolioResearchContext, *, exclude: set[str]) -> dict[str, object]:
    payload = context.model_dump(mode="json", exclude=exclude)
    comparison = payload.get("proposal_comparison")
    if isinstance(comparison, dict) and context.proposal_comparison is not None:
        comparison["economic_comparison"] = assess_frozen_comparison(context.proposal_comparison)
    return payload


class _BoundDatabaseExecutors:
    def __init__(
        self,
        session: AsyncSession,
        run: AgentResearchRunRecord,
        frozen_evidence: tuple[_FrozenEvidenceAuthority, ...],
        evidence: tuple[ResearchEvidenceRef, ...],
        *,
        allow_fixture_price_evidence: bool,
        portfolio_context: PortfolioResearchContext | None,
    ) -> None:
        self._session = session
        self._run_id = run.id
        self._instrument_id = run.instrument_id
        self._snapshot_id = run.data_snapshot_id
        self._decision_time = run.decision_time
        self._evidence_hash = run.evidence_hash
        self._frozen_evidence = frozen_evidence
        self._current_evidence_valid = bool(evidence)
        self._evidence_ids = tuple(record.id for record in evidence)
        self._evidence = evidence
        self._allow_fixture_price_evidence = allow_fixture_price_evidence
        self._portfolio_context = portfolio_context

    @property
    def executors(self) -> Mapping[str, ToolExecutor]:
        return {
            "get_security_snapshot": self.security_snapshot,
            "get_financial_facts": self.financial_facts,
            "search_filing_evidence": self.filing_evidence,
            "get_price_features": self.price_features,
            "get_macro_snapshot": self.macro_snapshot,
            "get_portfolio_exposure": self.portfolio_exposure,
            "get_portfolio_diagnostics": self.portfolio_diagnostics,
            "get_proposal_comparison": self.proposal_comparison,
        }

    async def security_snapshot(self, payload: Mapping[str, object]) -> Mapping[str, object]:
        await self._require_snapshot_scope(payload)
        if not await self._current_evidence_authority_valid():
            return self._invalid_frozen_evidence_authority("get_security_snapshot")
        distributions = await read_distributions(
            self._session,
            instrument_id=self._instrument_id,
            as_of=self._decision_time,
            evidence_ids=self._evidence_ids,
        )
        observations = await read_fund_observations(
            self._session,
            instrument_id=self._instrument_id,
            as_of=self._decision_time,
            evidence_ids=self._evidence_ids,
        )
        nav = await read_nav_histories(
            self._session,
            instrument_id=self._instrument_id,
            as_of=self._decision_time,
            evidence_ids=self._evidence_ids,
        )
        actions = await read_action_observations(
            self._session,
            instrument_id=self._instrument_id,
            as_of=self._decision_time,
            evidence_ids=self._evidence_ids,
        )
        profiles = await read_company_profiles(
            self._session,
            instrument_id=self._instrument_id,
            as_of=self._decision_time,
            evidence_ids=self._evidence_ids,
        )
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
                    **(
                        {"issuer_distribution": distributions[record.id].model_dump(mode="json")}
                        if record.id in distributions
                        else {}
                    ),
                    **(
                        {"issuer_fund_observation": observations[record.id].model_dump(mode="json")}
                        if record.id in observations
                        else {}
                    ),
                    **(
                        {"issuer_nav_history": nav_tool_payload(nav[record.id])}
                        if record.id in nav
                        else {}
                    ),
                    **(
                        {"corporate_action_observation": action_tool_payload(actions[record.id])}
                        if record.id in actions
                        else {}
                    ),
                    **(
                        {"company_profile": profiles[record.id].model_dump(mode="json")}
                        if record.id in profiles
                        else {}
                    ),
                }
                for record in self._evidence
            ],
        )

    async def financial_facts(self, payload: Mapping[str, object]) -> Mapping[str, object]:
        await self._require_snapshot_scope(payload)
        if not await self._current_evidence_authority_valid():
            return self._invalid_frozen_evidence_authority("get_financial_facts")
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
                    "period_start": record.period_start.isoformat()
                    if record.period_start
                    else None,
                    "period_kind": record.period_kind,
                    "accepted_at": _timestamp(record.accepted_at),
                    "revision": record.revision,
                }
                for record in records
            ],
        )

    async def filing_evidence(self, payload: Mapping[str, object]) -> Mapping[str, object]:
        await self._require_snapshot_scope(payload)
        if not await self._current_evidence_authority_valid():
            return self._invalid_frozen_evidence_authority("search_filing_evidence")
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
        disclosures = await read_disclosures(
            self._session,
            instrument_id=self._instrument_id,
            evidence_ids=tuple(record.evidence_ref_id for record in records),
            as_of=self._decision_time,
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
                    **(
                        {"disclosure": disclosures[record.evidence_ref_id].model_dump(mode="json")}
                        if record.evidence_ref_id in disclosures
                        else {}
                    ),
                }
                for record in records
            ],
        )

    async def price_features(self, payload: Mapping[str, object]) -> Mapping[str, object]:
        await self._require_snapshot_scope(payload)
        expected_price_evidence = tuple(
            record
            for record in self._frozen_evidence
            if record.provider_object_id.startswith("price-bar-set:")
            and record.location.startswith("db://price-bar-set/")
        )
        if not expected_price_evidence:
            return self._result(
                "get_price_features",
                [],
                status="unavailable",
                reason="no_frozen_price_authority",
            )
        if len(expected_price_evidence) != 1:
            return self._invalid_price_authority()
        expected = expected_price_evidence[0]

        fosun_source = and_(
            PriceSeriesManifest.source == "fosun",
            PriceSeriesManifest.feed == "l1",
            PriceSeriesManifest.delivery_mode.in_(("real_time", "delayed")),
            PriceSeriesManifest.adjustment == "raw",
            PriceSeriesManifest.interval == "1Day",
            PriceSeriesManifest.provider_capability_id.is_not(None),
            PriceSeriesManifest.provider_status_id.is_not(None),
            PriceSeriesManifest.provider_status_version.is_not(None),
            PriceSeriesManifest.provider_contract_hash.is_not(None),
            PriceSeriesManifest.provider_authorized_at.is_not(None),
            PriceSeriesManifest.provider_authorized_at
            <= AgentDataSnapshotEvidenceRecord.selected_at,
        )
        fixture_source = and_(
            PriceSeriesManifest.source == "fixture",
            PriceSeriesManifest.feed != "",
            PriceSeriesManifest.delivery_mode == "fixture",
            PriceSeriesManifest.interval == "1Day",
        )
        historical_alpaca_source = and_(
            PriceSeriesManifest.source == "alpaca",
            PriceSeriesManifest.feed == "sip",
            PriceSeriesManifest.delivery_mode == "delayed",
            PriceSeriesManifest.interval == "1Day",
        )
        fosun_v2_protocol = and_(
            fosun_source,
            PriceBarSetAuthority.source_policy_id == SOURCE_POLICY_V2_ID,
            PriceBarSetAuthority.hash_version == HASH_VERSION_V2,
            PriceBarSetAuthority.policy_version == POLICY_VERSION_V2,
            SourcePolicyRecord.id == SOURCE_POLICY_V2_ID,
            SourcePolicyRecord.key == SOURCE_POLICY_KEY,
            SourcePolicyRecord.version == SOURCE_POLICY_V2_VERSION,
            SourcePolicyRecord.schema_version == "2",
            SourcePolicyRecord.payload == SOURCE_POLICY_V2_PAYLOAD,
        )
        fixture_v1_protocol = and_(
            fixture_source,
            PriceBarSetAuthority.source_policy_id == SOURCE_POLICY_ID,
            PriceBarSetAuthority.hash_version == HASH_VERSION,
            PriceBarSetAuthority.policy_version == POLICY_VERSION,
            SourcePolicyRecord.id == SOURCE_POLICY_ID,
            SourcePolicyRecord.key == SOURCE_POLICY_KEY,
            SourcePolicyRecord.version == SOURCE_POLICY_VERSION,
            SourcePolicyRecord.schema_version == "1",
            SourcePolicyRecord.payload == SOURCE_POLICY_PAYLOAD,
        )
        historical_alpaca_v1_protocol = and_(
            historical_alpaca_source,
            PriceBarSetAuthority.source_policy_id == SOURCE_POLICY_ID,
            PriceBarSetAuthority.hash_version == HASH_VERSION,
            PriceBarSetAuthority.policy_version == POLICY_VERSION,
            SourcePolicyRecord.id == SOURCE_POLICY_ID,
            SourcePolicyRecord.key == SOURCE_POLICY_KEY,
            SourcePolicyRecord.version == SOURCE_POLICY_VERSION,
            SourcePolicyRecord.schema_version == "1",
            SourcePolicyRecord.payload == SOURCE_POLICY_PAYLOAD,
        )
        price_protocol = or_(fosun_v2_protocol, historical_alpaca_v1_protocol)
        if self._allow_fixture_price_evidence:
            price_protocol = or_(price_protocol, fixture_v1_protocol)
        authority_rows = tuple(
            (
                await self._session.execute(
                    select(
                        PriceBarSetAuthority,
                        ResearchEvidenceRef,
                        PriceSeriesManifest,
                        AgentDataSnapshotEvidenceRecord,
                    )
                    .join(
                        ResearchEvidenceRef,
                        ResearchEvidenceRef.id == AgentDataSnapshotEvidenceRecord.evidence_ref_id,
                    )
                    .join(
                        PriceBarSetAuthority,
                        PriceBarSetAuthority.evidence_ref_id == ResearchEvidenceRef.id,
                    )
                    .join(
                        PriceSeriesManifest,
                        PriceSeriesManifest.id == PriceBarSetAuthority.price_series_manifest_id,
                    )
                    .join(
                        SourcePolicyRecord,
                        SourcePolicyRecord.id == PriceBarSetAuthority.source_policy_id,
                    )
                    .where(
                        AgentDataSnapshotEvidenceRecord.data_snapshot_id == self._snapshot_id,
                        AgentDataSnapshotEvidenceRecord.instrument_id == self._instrument_id,
                        AgentDataSnapshotEvidenceRecord.authority_kind == "price",
                        AgentDataSnapshotEvidenceRecord.evidence_ref_id == expected.id,
                        AgentDataSnapshotEvidenceRecord.selected_at.is_not(None),
                        AgentDataSnapshotEvidenceRecord.selected_at <= self._decision_time,
                        AgentDataSnapshotEvidenceRecord.source == expected.source,
                        AgentDataSnapshotEvidenceRecord.provider_object_id
                        == expected.provider_object_id,
                        AgentDataSnapshotEvidenceRecord.revision == expected.revision,
                        AgentDataSnapshotEvidenceRecord.available_at == expected.available_at,
                        AgentDataSnapshotEvidenceRecord.location == expected.location,
                        AgentDataSnapshotEvidenceRecord.content_hash == expected.content_hash,
                        AgentDataSnapshotEvidenceRecord.source == ResearchEvidenceRef.source,
                        AgentDataSnapshotEvidenceRecord.provider_object_id
                        == ResearchEvidenceRef.provider_object_id,
                        AgentDataSnapshotEvidenceRecord.revision == ResearchEvidenceRef.revision,
                        AgentDataSnapshotEvidenceRecord.available_at
                        == ResearchEvidenceRef.available_at,
                        AgentDataSnapshotEvidenceRecord.location == ResearchEvidenceRef.location,
                        AgentDataSnapshotEvidenceRecord.content_hash
                        == ResearchEvidenceRef.content_hash,
                        ResearchEvidenceRef.id == expected.id,
                        ResearchEvidenceRef.source == expected.source,
                        ResearchEvidenceRef.provider_object_id == expected.provider_object_id,
                        ResearchEvidenceRef.revision == expected.revision,
                        ResearchEvidenceRef.available_at == expected.available_at,
                        ResearchEvidenceRef.location == expected.location,
                        ResearchEvidenceRef.content_hash == expected.content_hash,
                        price_protocol,
                        PriceBarSetAuthority.source == ResearchEvidenceRef.source,
                        PriceBarSetAuthority.provider_object_id
                        == ResearchEvidenceRef.provider_object_id,
                        PriceBarSetAuthority.revision == ResearchEvidenceRef.revision,
                        PriceBarSetAuthority.revision == PriceBarSetAuthority.content_hash,
                        PriceBarSetAuthority.available_at == ResearchEvidenceRef.available_at,
                        PriceBarSetAuthority.content_hash == ResearchEvidenceRef.content_hash,
                        PriceBarSetAuthority.byte_length == ResearchEvidenceRef.byte_length,
                        SourcePolicyRecord.created_at
                        <= AgentDataSnapshotEvidenceRecord.selected_at,
                        PriceSeriesManifest.instrument == str(self._instrument_id),
                        PriceSeriesManifest.source == PriceBarSetAuthority.source,
                        PriceSeriesManifest.row_count == PriceBarSetAuthority.bar_count,
                        PriceBarSetAuthority.available_at
                        <= AgentDataSnapshotEvidenceRecord.selected_at,
                        ResearchEvidenceRef.available_at
                        <= AgentDataSnapshotEvidenceRecord.selected_at,
                        PriceSeriesManifest.end_at <= AgentDataSnapshotEvidenceRecord.selected_at,
                    )
                    .limit(2)
                    .execution_options(populate_existing=True)
                )
            ).all()
        )
        if len(authority_rows) != 1:
            return self._invalid_price_authority()
        authority, evidence, manifest, binding = authority_rows[0]
        if (
            self._price_evidence(authority, evidence, manifest, expected) is None
            or binding.selected_at is None
            or binding.source != evidence.source
            or binding.provider_object_id != evidence.provider_object_id
            or binding.revision != evidence.revision
            or binding.available_at != evidence.available_at
            or binding.location != evidence.location
            or binding.content_hash != evidence.content_hash
        ):
            return self._invalid_price_authority()

        requested_features = cast(tuple[str, ...], payload["features"])
        if manifest.adjustment == "raw" and any(
            feature != "close" for feature in requested_features
        ):
            return self._result(
                "get_price_features",
                [],
                status="unavailable",
                reason="adjusted_price_authority_unavailable",
            )

        flagged_bar = await self._session.scalar(
            select(PriceBarRecord.id)
            .where(
                PriceBarRecord.price_series_manifest_id == manifest.id,
                PriceBarRecord.quality_flags != [],
            )
            .limit(1)
        )
        if flagged_bar is not None:
            return self._result(
                "get_price_features",
                [],
                status="unavailable",
                reason="flagged_price_bars",
            )

        prepared = await prepare_price_bar_set(
            self._session,
            manifest.id,
            as_of=binding.selected_at,
        )
        if prepared is None or (
            prepared.content_hash,
            prepared.byte_length,
            prepared.bar_count,
            prepared.first_session,
            prepared.last_session,
            prepared.available_at,
        ) != (
            authority.content_hash,
            authority.byte_length,
            authority.bar_count,
            authority.first_session,
            authority.last_session,
            authority.available_at,
        ):
            return self._invalid_price_authority()

        descending_bars = tuple(
            await self._session.scalars(
                select(PriceBarRecord)
                .where(PriceBarRecord.price_series_manifest_id == manifest.id)
                .order_by(
                    PriceBarRecord.session.desc(),
                    PriceBarRecord.observed_at.desc(),
                )
                .limit(200)
            )
        )
        if not descending_bars:
            return self._invalid_price_authority()
        bars = tuple(reversed(descending_bars))
        features = compute_price_features(bars, requested_features)
        item: dict[str, object] = {
            "evidence_ref_id": str(evidence.id),
            "source": manifest.source,
            "feed": manifest.feed,
            "adjustment": manifest.adjustment,
            "as_of_session": max(bar.session for bar in bars).isoformat(),
            "features": features.values,
        }
        if features.missing:
            item["missing_features"] = list(features.missing)
        return self._result(
            "get_price_features",
            [item],
            status="ok" if not features.missing else "partial",
            reason=None if not features.missing else "insufficient_price_history",
        )

    def _price_evidence(
        self,
        authority: PriceBarSetAuthority,
        evidence: ResearchEvidenceRef,
        manifest: PriceSeriesManifest,
        expected: _FrozenEvidenceAuthority,
    ) -> ResearchEvidenceRef | None:
        return (
            evidence
            if evidence.id == expected.id == authority.evidence_ref_id
            and evidence.source == expected.source == authority.source == manifest.source
            and evidence.provider_object_id
            == expected.provider_object_id
            == authority.provider_object_id
            == f"price-bar-set:{manifest.id}"
            and evidence.revision
            == expected.revision
            == authority.revision
            == authority.content_hash
            and evidence.available_at == expected.available_at == authority.available_at
            and evidence.location
            == expected.location
            == f"db://price-bar-set/{manifest.id}@sha256:{authority.content_hash}"
            and evidence.content_hash == expected.content_hash == authority.content_hash
            and evidence.byte_length == authority.byte_length
            else None
        )

    def _invalid_price_authority(self) -> Mapping[str, object]:
        return self._result(
            "get_price_features",
            [],
            status="unavailable",
            reason="invalid_price_bar_set_authority",
        )

    async def macro_snapshot(self, payload: Mapping[str, object]) -> Mapping[str, object]:
        await self._require_common_scope(payload)
        if payload["data_snapshot_id"] != self._snapshot_id:
            raise ToolInputError("tool request exceeds frozen snapshot authority")
        if not await self._current_evidence_authority_valid():
            return self._invalid_frozen_evidence_authority("get_macro_snapshot")
        series = cast(tuple[str, ...], payload["series"])
        records = tuple(
            await self._session.scalars(
                select(MacroObservation)
                .where(
                    MacroObservation.available_at <= self._decision_time,
                    MacroObservation.observed_at <= self._decision_time,
                    MacroObservation.authority_version == "research_macro_v1",
                    MacroObservation.series.in_(series),
                    MacroObservation.evidence_ref_id.in_(self._evidence_ids),
                    MacroObservation.calendar_evidence_ref_id.in_(self._evidence_ids),
                )
                .order_by(MacroObservation.series, MacroObservation.observed_at.desc())
                .limit(50)
            )
        )
        items: list[dict[str, object]] = [
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
        ]
        return self._result(
            "get_macro_snapshot",
            items,
            status="ok" if items else "unavailable",
            reason=None if items else "no_frozen_macro_authority",
        )

    async def portfolio_exposure(self, payload: Mapping[str, object]) -> Mapping[str, object]:
        await self._require_common_scope(payload)
        context = await self._require_portfolio_scope(payload)
        if context is None:
            return self._capability_unavailable("get_portfolio_exposure")
        return self._result(
            "get_portfolio_exposure",
            [
                _context_payload(
                    context,
                    exclude={
                        "diagnostics",
                        "diagnostic_input_hash",
                        "optimization_need",
                        "diagnostics_truncated",
                    },
                )
            ],
        )

    async def portfolio_diagnostics(self, payload: Mapping[str, object]) -> Mapping[str, object]:
        await self._require_common_scope(payload)
        context = await self._require_portfolio_scope(payload)
        if context is None or context.diagnostic_run_id is None:
            return self._capability_unavailable("get_portfolio_diagnostics")
        if payload["diagnostic_run_id"] != context.diagnostic_run_id:
            raise ToolInputError("tool request exceeds frozen diagnostic authority")
        return self._result(
            "get_portfolio_diagnostics",
            [
                _context_payload(
                    context,
                    exclude={
                        "position_weight",
                        "cash_weight",
                        "valuation_reason",
                    },
                )
            ],
        )

    async def _require_portfolio_scope(
        self,
        payload: Mapping[str, object],
    ) -> PortfolioResearchContext | None:
        context = await read_portfolio_context(
            self._session,
            self._snapshot_id,
            self._instrument_id,
            self._decision_time,
        )
        if (
            context is None
            or not await self._current_evidence_authority_valid()
            or context.evidence_ref_id not in self._evidence_ids
        ):
            return None
        if payload["portfolio_snapshot_id"] != context.portfolio_snapshot_id or payload.get(
            "instrument_id"
        ) not in (None, self._instrument_id):
            raise ToolInputError("tool request exceeds frozen portfolio authority")
        return context

    async def proposal_comparison(self, payload: Mapping[str, object]) -> Mapping[str, object]:
        await self._require_common_scope(payload)
        context = await read_portfolio_context(
            self._session, self._snapshot_id, self._instrument_id, self._decision_time,
        )
        if (
            context is None or context.proposal_comparison is None
            or not await self._current_evidence_authority_valid()
            or context.evidence_ref_id not in self._evidence_ids
        ):
            return self._capability_unavailable("get_proposal_comparison")
        comparison = context.proposal_comparison
        if payload["proposal_group_id"] != comparison.proposal_group_id:
            raise ToolInputError("tool request exceeds frozen proposal authority")
        return self._result("get_proposal_comparison", [{
            **comparison.model_dump(mode="json"),
            "economic_comparison": assess_frozen_comparison(comparison),
            "evidence_ref_id": str(context.evidence_ref_id),
            "content_hash": context.content_hash,
        }])

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
        current_id = await self._session.scalar(
            select(AgentResearchRunRecord.id).where(
                AgentResearchRunRecord.id == self._run_id,
                AgentResearchRunRecord.instrument_id == self._instrument_id,
                AgentResearchRunRecord.data_snapshot_id == self._snapshot_id,
                AgentResearchRunRecord.decision_time == self._decision_time,
                AgentResearchRunRecord.evidence_hash == self._evidence_hash,
            )
        )
        if current_id is None:
            raise ToolInputError("frozen run authority is unavailable")

    def _capability_unavailable(self, tool: str) -> Mapping[str, object]:
        return self._result(
            tool,
            [],
            status="unavailable",
            reason="authority_not_frozen_for_research_run",
        )

    async def _current_evidence_authority_valid(self) -> bool:
        if not self._current_evidence_valid:
            return False
        rows = (
            await self._session.execute(
                select(
                    ResearchEvidenceRef.id,
                    ResearchEvidenceRef.source,
                    ResearchEvidenceRef.provider_object_id,
                    ResearchEvidenceRef.revision,
                    ResearchEvidenceRef.available_at,
                    ResearchEvidenceRef.location,
                    ResearchEvidenceRef.content_hash,
                ).where(
                    ResearchEvidenceRef.id.in_(tuple(record.id for record in self._frozen_evidence))
                )
            )
        ).all()
        current = {row.id: row for row in rows}
        self._current_evidence_valid = len(current) == len(self._frozen_evidence) and all(
            (row := current.get(expected.id)) is not None
            and row.source == expected.source
            and row.provider_object_id == expected.provider_object_id
            and row.revision == expected.revision
            and row.available_at == expected.available_at
            and row.location == expected.location
            and row.content_hash == expected.content_hash
            for expected in self._frozen_evidence
        )
        if not self._current_evidence_valid:
            self._evidence_ids = ()
            self._evidence = ()
        return self._current_evidence_valid

    def _invalid_frozen_evidence_authority(self, tool: str) -> Mapping[str, object]:
        return self._result(
            tool,
            [],
            status="unavailable",
            reason="invalid_frozen_evidence_authority",
        )

    def _result(
        self,
        tool: str,
        items: list[dict[str, object]],
        *,
        status: str = "ok",
        reason: str | None = None,
    ) -> Mapping[str, object]:
        context = self._portfolio_context
        scope = (
            {
                "portfolio_scope": {
                    "portfolio_snapshot_id": str(context.portfolio_snapshot_id),
                    "diagnostic_run_id": str(context.diagnostic_run_id)
                    if context.diagnostic_run_id
                    else None,
                }
            }
            if context and context.evidence_ref_id in self._evidence_ids
            else {}
        )
        return {
            **scope,
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


def _frozen_evidence_authority(
    run: AgentResearchRunRecord,
    *,
    allow_fixture_evidence: bool,
) -> tuple[_FrozenEvidenceAuthority, ...] | None:
    manifest = run.evidence_manifest
    if (
        not isinstance(manifest, list)
        or not manifest
        or len(manifest) > MAX_FROZEN_EVIDENCE
        or hashlib.sha256(
            json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        != run.evidence_hash
    ):
        return None
    records: list[_FrozenEvidenceAuthority] = []
    required_fields = {
        "id",
        "source",
        "provider_object_id",
        "revision",
        "available_at",
        "location",
        "content_hash",
    }
    try:
        for item in manifest:
            if not isinstance(item, dict) or set(item) != required_fields:
                return None
            string_values = tuple(item[field] for field in required_fields - {"id"})
            if not all(isinstance(value, str) for value in string_values):
                return None
            available_at = datetime.fromisoformat(
                cast(str, item["available_at"]).replace("Z", "+00:00")
            )
            record = _FrozenEvidenceAuthority(
                id=UUID(cast(str, item["id"])),
                source=cast(str, item["source"]),
                provider_object_id=cast(str, item["provider_object_id"]),
                revision=cast(str, item["revision"]),
                available_at=available_at,
                location=cast(str, item["location"]),
                content_hash=cast(str, item["content_hash"]),
            )
            if (
                record.available_at.tzinfo is None
                or record.available_at > run.decision_time
                or (record.source == "fixture" and not allow_fixture_evidence)
                or len(record.content_hash) != 64
                or any(character not in "0123456789abcdef" for character in record.content_hash)
            ):
                return None
            records.append(record)
    except (TypeError, ValueError, AttributeError):
        return None
    if len({record.id for record in records}) != len(records):
        return None
    return tuple(records)
