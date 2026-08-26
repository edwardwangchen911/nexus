"""Point-in-time exact-set authority selection for Proposal Shadow sessions."""

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Literal, cast
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.market.price_bar_authority import prepare_price_bar_set
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import (
    DataSnapshot,
    PriceBarRecord,
    PriceBarSetAuthority,
    PriceSeriesManifest,
)
from nexus.persistence.models.market import Instrument
from nexus.persistence.models.research import CorporateActionRecord, ResearchEvidenceRef
from nexus.persistence.models.shadow import ShadowGroupRecord
from nexus.quality.source_policy import (
    SOURCE_POLICY_ID,
    load_source_policy_v1,
    price_manifest_admissible_under_source_policy_v1,
)
from nexus.shadow.canonical import (
    shadow_query_bundle_content_hash,
    shadow_session_content_hash,
    shadow_source_query_content_hash,
)
from nexus.shadow.models import (
    CalendarHashBinding,
    EventInputHash,
    MissingExecutionInputHash,
    PriceInputHash,
    ShadowSessionHashInput,
)
from nexus.simulation.models import (
    DelistingEvent,
    DividendEvent,
    SimulationBar,
    SplitEvent,
)

ShadowAuthorityStatus = Literal[
    "complete",
    "data_partial",
    "data_stale",
    "source_conflict",
]
ShadowQueryKind = Literal["price_bars", "corporate_actions"]
ShadowEventKind = Literal["split", "dividend", "delisting"]
_SOURCE_PRIORITY = ("fosun", "alpaca", "fixture")
_EVENT_ORDER = {"split": 0, "dividend": 1, "delisting": 2}
_QUERY_KEYS = {
    "schema",
    "shadow_group_id",
    "session",
    "kind",
    "source",
    "delivery_mode",
    "instrument_ids",
    "complete",
    "member_ids",
    "negative_instrument_ids",
    "query_hash",
}


def _require_utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{field} must be UTC")
    return value.astimezone(UTC)


def _ordered_ids(values: Iterable[UUID]) -> tuple[UUID, ...]:
    ordered = tuple(sorted(values, key=lambda item: item.int))
    if len(ordered) != len(set(ordered)):
        raise ValueError("Shadow authority instrument identifiers must be unique")
    return ordered


@dataclass(frozen=True)
class LockedShadowRoot:
    shadow_group_id: UUID
    calendar: CalendarHashBinding
    source_policy_id: UUID
    cost_policy_id: UUID
    signal_session: date
    evaluation_sessions: tuple[date, ...]
    grace_sessions: tuple[date, ...]

    def __post_init__(self) -> None:
        if len(self.evaluation_sessions) != 63 or len(self.grace_sessions) != 5:
            raise ValueError("Shadow root calendar window is incomplete")
        if tuple(sorted(self.evaluation_sessions)) != self.evaluation_sessions:
            raise ValueError("Shadow evaluation sessions must be ordered")
        if tuple(sorted(self.grace_sessions)) != self.grace_sessions:
            raise ValueError("Shadow grace sessions must be ordered")
        if len(set(self.evaluation_sessions)) != 63 or len(set(self.grace_sessions)) != 5:
            raise ValueError("Shadow root calendar sessions must be unique")
        if self.evaluation_sessions[-1] >= self.grace_sessions[0]:
            raise ValueError("Shadow grace sessions must follow evaluation")

    @classmethod
    def from_record(cls, record: ShadowGroupRecord) -> "LockedShadowRoot":
        return cls(
            shadow_group_id=record.id,
            calendar=CalendarHashBinding(
                authority_id=record.calendar_authority_id,
                version=record.calendar_version,
                content_hash=record.calendar_hash,
            ),
            source_policy_id=record.source_policy_id,
            cost_policy_id=record.cost_policy_id,
            signal_session=record.signal_session,
            evaluation_sessions=tuple(record.evaluation_sessions),
            grace_sessions=tuple(record.grace_sessions),
        )


@dataclass(frozen=True)
class ShadowBarCandidate:
    price_bar_record_id: UUID
    price_bar_set_authority_id: UUID
    instrument_id: UUID
    source: str
    delivery_mode: str
    revision: str
    available_at: datetime
    content_hash: str
    open: Decimal
    close: Decimal

    def __post_init__(self) -> None:
        if self.source not in _SOURCE_PRIORITY or not self.delivery_mode or not self.revision:
            raise ValueError("Shadow Bar candidate identity is invalid")
        _require_utc(self.available_at, "Bar candidate available_at")
        if len(self.content_hash) != 64 or any(
            character not in "0123456789abcdef" for character in self.content_hash
        ):
            raise ValueError("Shadow Bar candidate hash is invalid")
        SimulationBar(
            instrument_id=self.instrument_id,
            open=self.open,
            close=self.close,
        )


def select_shadow_bar_candidate(
    candidates: tuple[ShadowBarCandidate, ...],
    *,
    observed_at: datetime,
) -> ShadowBarCandidate | None:
    """Select by policy source, latest PIT availability, revision, then UUID."""
    observed_at = _require_utc(observed_at, "observed_at")
    visible = tuple(item for item in candidates if item.available_at <= observed_at)
    if not visible:
        return None
    highest_source_rank = min(_SOURCE_PRIORITY.index(item.source) for item in visible)
    same_source = tuple(
        item
        for item in visible
        if _SOURCE_PRIORITY.index(item.source) == highest_source_rank
    )
    return max(
        same_source,
        key=lambda item: (
            item.available_at,
            item.revision,
            -item.price_bar_set_authority_id.int,
            -item.price_bar_record_id.int,
        ),
    )


@dataclass(frozen=True)
class BoundShadowBar:
    price_bar_record_id: UUID
    price_bar_set_authority_id: UUID
    instrument_id: UUID
    usage: Literal["execution_and_mark", "mark"]
    source: str
    delivery_mode: str
    revision: str
    available_at: datetime
    content_hash: str
    open: Decimal
    close: Decimal

    def hash_input(self) -> PriceInputHash:
        return PriceInputHash(
            instrument_id=self.instrument_id,
            authority_id=self.price_bar_set_authority_id,
            source=self.source,
            delivery_mode=self.delivery_mode,
            revision=self.revision,
            available_at=self.available_at,
            content_hash=self.content_hash,
            purpose=self.usage,
        )

    def simulation_bar(self) -> SimulationBar:
        return SimulationBar(
            instrument_id=self.instrument_id,
            open=self.open,
            close=self.close,
        )


@dataclass(frozen=True)
class MissingExecutionInput:
    instrument_id: UUID
    source: str
    query_hash: str
    reason: Literal["missing_bar"] = "missing_bar"

    def hash_input(self) -> MissingExecutionInputHash:
        return MissingExecutionInputHash(
            instrument_id=self.instrument_id,
            source=self.source,
            query_hash=self.query_hash,
            reason=self.reason,
        )


@dataclass(frozen=True)
class BoundShadowEvent:
    corporate_action_id: UUID
    instrument_id: UUID
    event_kind: ShadowEventKind
    source: str
    provider_event_id: str
    revision: str
    available_at: datetime
    evidence_hash: str
    split: SplitEvent | None = None
    dividend: DividendEvent | None = None
    delisting: DelistingEvent | None = None

    def __post_init__(self) -> None:
        members = (self.split, self.dividend, self.delisting)
        if sum(item is not None for item in members) != 1:
            raise ValueError("Bound Shadow event requires exactly one kernel event")
        expected = (
            "split"
            if self.split is not None
            else "dividend"
            if self.dividend is not None
            else "delisting"
        )
        if self.event_kind != expected:
            raise ValueError("Bound Shadow event kind does not match its kernel event")

    def hash_input(self) -> EventInputHash:
        return EventInputHash(
            instrument_id=self.instrument_id,
            authority_id=self.corporate_action_id,
            event_kind=self.event_kind,
            source=self.source,
            provider_event_id=self.provider_event_id,
            revision=self.revision,
            available_at=self.available_at,
            evidence_hash=self.evidence_hash,
        )


@dataclass(frozen=True)
class ShadowSourceSummary:
    source: str
    kind: ShadowQueryKind
    delivery_mode: str
    complete: bool
    query_hash: str
    member_count: int
    negative_count: int
    available_at: datetime

    def __post_init__(self) -> None:
        _require_utc(self.available_at, "query proof available_at")
        if self.member_count < 0 or self.negative_count < 0:
            raise ValueError("query proof counts must be nonnegative")


@dataclass(frozen=True)
class ShadowSessionInput:
    status: ShadowAuthorityStatus
    reason: str | None
    session: date
    ordinal: int
    observed_at: datetime
    late_backfill: bool
    authority_hash: str | None
    bars: tuple[BoundShadowBar, ...]
    missing_execution_inputs: tuple[MissingExecutionInput, ...]
    events: tuple[BoundShadowEvent, ...]
    source_summary: tuple[ShadowSourceSummary, ...]
    delivery_mode_summary: tuple[str, ...]
    maximum_available_at: datetime | None

    def __post_init__(self) -> None:
        _require_utc(self.observed_at, "session observed_at")
        if self.maximum_available_at is not None:
            _require_utc(self.maximum_available_at, "session maximum_available_at")
            if self.maximum_available_at > self.observed_at:
                raise ValueError("session authority cannot contain future availability")
        if self.status == "complete":
            if self.reason is not None or self.authority_hash is None:
                raise ValueError("complete session authority requires only a hash")
        elif (
            not self.reason
            or self.authority_hash is not None
            or self.bars
            or self.missing_execution_inputs
            or self.events
            or self.delivery_mode_summary
        ):
            raise ValueError("failed session authority cannot expose partial inputs")
        if self.authority_hash is not None and (
            len(self.authority_hash) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.authority_hash
            )
        ):
            raise ValueError("session authority hash is invalid")
        bar_ids = [item.instrument_id for item in self.bars]
        missing_ids = [item.instrument_id for item in self.missing_execution_inputs]
        if (
            len(bar_ids) != len(set(bar_ids))
            or len(missing_ids) != len(set(missing_ids))
            or set(bar_ids) & set(missing_ids)
        ):
            raise ValueError("session price and negative inputs are inconsistent")
        if len({item.corporate_action_id for item in self.events}) != len(self.events):
            raise ValueError("session event authorities must be unique")

    @property
    def query_hash(self) -> str:
        return shadow_query_bundle_content_hash(cast(tuple[object, ...], self.source_summary))


@dataclass(frozen=True)
class _QueryProof:
    snapshot_id: UUID
    source: str
    kind: ShadowQueryKind
    delivery_mode: str
    complete: bool
    query_hash: str
    instrument_ids: tuple[UUID, ...]
    member_ids: tuple[UUID, ...]
    negative_instrument_ids: tuple[UUID, ...]
    available_at: datetime
    revision: str

    def summary(self) -> ShadowSourceSummary:
        return ShadowSourceSummary(
            source=self.source,
            kind=self.kind,
            delivery_mode=self.delivery_mode,
            complete=self.complete,
            query_hash=self.query_hash,
            member_count=len(self.member_ids),
            negative_count=len(self.negative_instrument_ids),
            available_at=self.available_at,
        )


@dataclass(frozen=True)
class _PriceRow:
    bar: PriceBarRecord
    authority: PriceBarSetAuthority
    manifest: PriceSeriesManifest
    instrument: Instrument

    @property
    def available_at(self) -> datetime:
        return max(
            self.bar.available_at,
            self.bar.received_at,
            self.authority.available_at,
        )


@dataclass(frozen=True)
class _EventRow:
    record: CorporateActionRecord
    evidence: ResearchEvidenceRef
    effective_session: date
    payable_session: date | None


async def load_shadow_session_input(
    session: AsyncSession,
    *,
    root: LockedShadowRoot,
    session_date: date,
    ordinal: int,
    observed_at: datetime,
    required_instruments: tuple[UUID, ...],
    initial_execution_instruments: tuple[UUID, ...],
) -> ShadowSessionInput:
    """Select one complete exact authority set under the frozen source policy."""
    observed_at = _require_utc(observed_at, "observed_at")
    required = _ordered_ids(required_instruments)
    initial = _ordered_ids(initial_execution_instruments)
    instruments = _ordered_ids((*required, *(item for item in initial if item not in required)))
    if ordinal < 1 or ordinal > 63 or root.evaluation_sessions[ordinal - 1] != session_date:
        raise ValueError("SHADOW_SESSION_BOUNDARY_INVALID")

    calendar = await session.get(
        ExchangeCalendarAuthorityRecord, root.calendar.authority_id
    )
    calendar_member = await session.get(
        ExchangeCalendarSessionRecord,
        {
            "calendar_authority_id": root.calendar.authority_id,
            "session": session_date,
        },
    )
    if (
        calendar is None
        or calendar_member is None
        or calendar.version != root.calendar.version
        or calendar.content_hash != root.calendar.content_hash
    ):
        raise ValueError("SHADOW_CALENDAR_AUTHORITY_INVALID")
    decision_at = _require_utc(calendar_member.decision_at, "calendar decision_at")
    late_backfill = observed_at > decision_at

    try:
        source_policy = await load_source_policy_v1(
            session, instrument_tick_size=None
        )
    except ValueError:
        return _failure(
            root,
            session_date,
            ordinal,
            observed_at,
            late_backfill,
            "data_partial",
            "SOURCE_POLICY_INVALID",
        )
    if (
        root.source_policy_id != SOURCE_POLICY_ID
        or source_policy.record_id != root.source_policy_id
        or source_policy.available_at > observed_at
    ):
        return _failure(
            root,
            session_date,
            ordinal,
            observed_at,
            late_backfill,
            "data_partial",
            "SOURCE_POLICY_INVALID",
        )

    price_proofs = await _load_query_proofs(
        session,
        root=root,
        session_date=session_date,
        kind="price_bars",
        instruments=instruments,
        cutoff=observed_at,
    )
    event_proofs = await _load_query_proofs(
        session,
        root=root,
        session_date=session_date,
        kind="corporate_actions",
        instruments=instruments,
        cutoff=min(decision_at, observed_at),
    )
    summaries = _ordered_summaries((*price_proofs, *event_proofs))
    maximum_proof_time = max(
        (proof.available_at for proof in (*price_proofs, *event_proofs)),
        default=None,
    )
    if any(not proof.complete for proof in price_proofs):
        return _failure(
            root,
            session_date,
            ordinal,
            observed_at,
            late_backfill,
            "data_partial",
            "SOURCE_QUERY_INCOMPLETE",
            summaries=summaries,
            maximum_available_at=maximum_proof_time,
        )

    event_result = await _load_bound_events(
        session,
        root=root,
        session_date=session_date,
        instruments=instruments,
        decision_at=decision_at,
        proofs=event_proofs,
    )
    if isinstance(event_result, str):
        status: ShadowAuthorityStatus = (
            "source_conflict"
            if event_result == "SOURCE_CONFLICT"
            else "data_partial"
        )
        return _failure(
            root,
            session_date,
            ordinal,
            observed_at,
            late_backfill,
            status,
            event_result,
            summaries=summaries,
            maximum_available_at=maximum_proof_time,
        )
    events = event_result
    resolved_delistings = {
        item.instrument_id for item in events if item.event_kind == "delisting"
    }

    price_rows = await _load_price_rows(
        session,
        session_date=session_date,
        instruments=instruments,
    )
    proof_by_source = {proof.source: proof for proof in price_proofs}
    visible_member_ids: dict[str, set[UUID]] = {}
    raw_candidates: dict[UUID, list[ShadowBarCandidate]] = {
        instrument_id: [] for instrument_id in instruments
    }
    future_raw_ids: set[UUID] = set()
    tick_sizes: dict[UUID, Decimal | None] = {}
    for row in price_rows:
        tick_sizes[row.bar.instrument_id] = row.instrument.tick_size
        if not _row_identity_valid(row, root):
            continue
        if row.available_at > observed_at:
            if row.manifest.adjustment == "raw":
                future_raw_ids.add(row.bar.instrument_id)
            continue
        visible_member_ids.setdefault(row.manifest.source, set()).add(row.bar.id)
        if row.manifest.adjustment != "raw":
            continue
        if not await _price_authority_matches_database(session, row):
            return _failure(
                root,
                session_date,
                ordinal,
                observed_at,
                late_backfill,
                "data_partial",
                "PRICE_AUTHORITY_HASH_MISMATCH",
                summaries=summaries,
                maximum_available_at=maximum_proof_time,
            )
        raw_candidates[row.bar.instrument_id].append(_candidate_from_row(row))

    for source in _SOURCE_PRIORITY:
        if source not in visible_member_ids and source not in proof_by_source:
            continue
        member_ids = visible_member_ids.get(source, set())
        proof = proof_by_source.get(source)
        if proof is None or not proof.complete:
            return _failure(
                root,
                session_date,
                ordinal,
                observed_at,
                late_backfill,
                "data_partial",
                "SOURCE_QUERY_INCOMPLETE",
                summaries=summaries,
                maximum_available_at=maximum_proof_time,
            )
        if set(proof.member_ids) != member_ids:
            return _failure(
                root,
                session_date,
                ordinal,
                observed_at,
                late_backfill,
                "data_partial",
                "SOURCE_QUERY_SET_MISMATCH",
                summaries=summaries,
                maximum_available_at=maximum_proof_time,
            )

    bars: list[BoundShadowBar] = []
    missing: list[MissingExecutionInput] = []
    for instrument_id in instruments:
        candidates = tuple(raw_candidates[instrument_id])
        by_source = _latest_candidate_by_source(candidates, observed_at)
        if _has_source_conflict(tuple(by_source.values()), tick_sizes.get(instrument_id)):
            return _failure(
                root,
                session_date,
                ordinal,
                observed_at,
                late_backfill,
                "source_conflict",
                "SOURCE_CONFLICT",
                summaries=summaries,
                maximum_available_at=maximum_proof_time,
            )
        selected = select_shadow_bar_candidate(candidates, observed_at=observed_at)
        if selected is not None:
            usage: Literal["execution_and_mark", "mark"] = (
                "execution_and_mark" if instrument_id in initial else "mark"
            )
            bars.append(_bind_candidate(selected, usage))
            continue
        if instrument_id in resolved_delistings:
            continue
        negative = _negative_proof(price_proofs, instrument_id)
        if instrument_id in initial and negative is not None:
            missing.append(
                MissingExecutionInput(
                    instrument_id=instrument_id,
                    source=negative.source,
                    query_hash=negative.query_hash,
                )
            )
            continue
        reason = (
            "MARK_BAR_MISSING"
            if instrument_id not in future_raw_ids
            else "BAR_NOT_YET_AVAILABLE"
        )
        status = "data_partial" if reason == "MARK_BAR_MISSING" else "data_stale"
        return _failure(
            root,
            session_date,
            ordinal,
            observed_at,
            late_backfill,
            status,
            reason,
            summaries=summaries,
            maximum_available_at=maximum_proof_time,
        )

    ordered_bars = tuple(sorted(bars, key=lambda item: item.instrument_id.int))
    ordered_missing = tuple(
        sorted(missing, key=lambda item: item.instrument_id.int)
    )
    ordered_events = tuple(
        sorted(
            events,
            key=lambda item: (
                _EVENT_ORDER[item.event_kind],
                item.instrument_id.int,
                item.corporate_action_id.int,
            ),
        )
    )
    hash_input = ShadowSessionHashInput(
        shadow_group_id=root.shadow_group_id,
        session=session_date,
        ordinal=ordinal,
        calendar=root.calendar,
        source_policy_id=root.source_policy_id,
        cost_policy_id=root.cost_policy_id,
        bars=tuple(item.hash_input() for item in ordered_bars),
        missing_execution_inputs=tuple(
            item.hash_input() for item in ordered_missing
        ),
        events=tuple(item.hash_input() for item in ordered_events),
    )
    maximum_available_at = max(
        (
            *(item.available_at for item in ordered_bars),
            *(item.available_at for item in ordered_events),
            *(item.available_at for item in summaries),
        ),
        default=None,
    )
    return ShadowSessionInput(
        status="complete",
        reason=None,
        session=session_date,
        ordinal=ordinal,
        observed_at=observed_at,
        late_backfill=late_backfill,
        authority_hash=shadow_session_content_hash(hash_input),
        bars=ordered_bars,
        missing_execution_inputs=ordered_missing,
        events=ordered_events,
        source_summary=summaries,
        delivery_mode_summary=tuple(
            sorted({item.delivery_mode for item in ordered_bars})
        ),
        maximum_available_at=maximum_available_at,
    )


async def _load_query_proofs(
    session: AsyncSession,
    *,
    root: LockedShadowRoot,
    session_date: date,
    kind: ShadowQueryKind,
    instruments: tuple[UUID, ...],
    cutoff: datetime,
) -> tuple[_QueryProof, ...]:
    rows = tuple(
        await session.scalars(
            select(DataSnapshot).where(
                DataSnapshot.schema_version == "shadow_source_query_v1",
                DataSnapshot.available_at <= cutoff,
            )
        )
    )
    parsed: list[_QueryProof] = []
    for row in rows:
        proof = _parse_query_proof(row)
        if (
            proof is not None
            and proof.kind == kind
            and proof.instrument_ids == instruments
            and row.payload.get("shadow_group_id") == str(root.shadow_group_id)
            and row.payload.get("session") == session_date.isoformat()
        ):
            parsed.append(proof)
    latest: dict[str, _QueryProof] = {}
    for proof in parsed:
        current = latest.get(proof.source)
        if current is None or (
            proof.available_at,
            proof.revision,
            -proof.snapshot_id.int,
        ) > (
            current.available_at,
            current.revision,
            -current.snapshot_id.int,
        ):
            latest[proof.source] = proof
    return tuple(
        latest[source]
        for source in _SOURCE_PRIORITY
        if source in latest
    )


def _parse_query_proof(record: DataSnapshot) -> _QueryProof | None:
    payload = record.payload
    try:
        if set(payload) != _QUERY_KEYS or payload["schema"] != "shadow_source_query_v1":
            return None
        kind = payload["kind"]
        source_value = payload["source"]
        delivery_mode = payload["delivery_mode"]
        complete = payload["complete"]
        query_hash = payload["query_hash"]
        if (
            kind not in {"price_bars", "corporate_actions"}
            or not isinstance(source_value, str)
            or source_value != record.source
            or source_value not in _SOURCE_PRIORITY
            or not isinstance(delivery_mode, str)
            or not isinstance(complete, bool)
            or not isinstance(query_hash, str)
        ):
            return None
        shadow_group_id = UUID(cast(str, payload["shadow_group_id"]))
        session_date = date.fromisoformat(cast(str, payload["session"]))
        instrument_ids = _uuid_tuple(payload["instrument_ids"])
        member_ids = _uuid_tuple(payload["member_ids"])
        negative_ids = _uuid_tuple(payload["negative_instrument_ids"])
        expected = shadow_source_query_content_hash(
            shadow_group_id=shadow_group_id,
            session=session_date,
            kind=cast(str, kind),
            source=source_value,
            delivery_mode=delivery_mode,
            instrument_ids=instrument_ids,
            complete=complete,
            member_ids=member_ids,
            negative_instrument_ids=negative_ids,
        )
    except (KeyError, TypeError, ValueError):
        return None
    if (
        query_hash != expected
        or record.revision != expected
        or not record.observed_at <= record.available_at <= record.received_at
    ):
        return None
    return _QueryProof(
        snapshot_id=record.id,
        source=source_value,
        kind=cast(ShadowQueryKind, kind),
        delivery_mode=delivery_mode,
        complete=complete,
        query_hash=query_hash,
        instrument_ids=instrument_ids,
        member_ids=member_ids,
        negative_instrument_ids=negative_ids,
        available_at=_require_utc(record.available_at, "query available_at"),
        revision=record.revision,
    )


def _uuid_tuple(value: object) -> tuple[UUID, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise TypeError("query proof UUID lists are invalid")
    return _ordered_ids(UUID(item) for item in value)


async def _load_price_rows(
    session: AsyncSession,
    *,
    session_date: date,
    instruments: tuple[UUID, ...],
) -> tuple[_PriceRow, ...]:
    if not instruments:
        return ()
    rows = (
        (
            await session.execute(
                select(
                    PriceBarRecord,
                    PriceBarSetAuthority,
                    PriceSeriesManifest,
                    Instrument,
                )
                .join(
                    PriceSeriesManifest,
                    PriceSeriesManifest.id
                    == PriceBarRecord.price_series_manifest_id,
                )
                .join(
                    PriceBarSetAuthority,
                    PriceBarSetAuthority.price_series_manifest_id
                    == PriceSeriesManifest.id,
                )
                .join(Instrument, Instrument.id == PriceBarRecord.instrument_id)
                .where(
                    PriceBarRecord.session == session_date,
                    PriceBarRecord.instrument_id.in_(instruments),
                )
            )
        )
        .tuples()
        .all()
    )
    return tuple(_PriceRow(*row) for row in rows)


def _row_identity_valid(row: _PriceRow, root: LockedShadowRoot) -> bool:
    return bool(
        row.authority.source_policy_id == root.source_policy_id
        and row.authority.policy_version == "agent_price_authority_v1"
        and row.authority.hash_version == "price_bar_set_sha256_v1"
        and row.authority.source == row.manifest.source
        and row.manifest.instrument == str(row.bar.instrument_id)
        and row.manifest.interval == "1Day"
        and row.bar.quality_flags == []
        and row.authority.first_session <= row.bar.session <= row.authority.last_session
        and price_manifest_admissible_under_source_policy_v1(
            source=row.manifest.source,
            feed=row.manifest.feed,
            delivery_mode=row.manifest.delivery_mode,
            fosun_admission_frozen=False,
        )
    )


async def _price_authority_matches_database(
    session: AsyncSession,
    row: _PriceRow,
) -> bool:
    prepared = await prepare_price_bar_set(session, row.manifest.id)
    return bool(
        prepared is not None
        and row.authority.content_hash == prepared.content_hash
        and row.authority.byte_length == prepared.byte_length
        and row.authority.bar_count == prepared.bar_count
        and row.authority.first_session == prepared.first_session
        and row.authority.last_session == prepared.last_session
        and row.authority.available_at == prepared.available_at
    )


def _candidate_from_row(row: _PriceRow) -> ShadowBarCandidate:
    return ShadowBarCandidate(
        price_bar_record_id=row.bar.id,
        price_bar_set_authority_id=row.authority.id,
        instrument_id=row.bar.instrument_id,
        source=row.manifest.source,
        delivery_mode=row.manifest.delivery_mode,
        revision=row.bar.revision,
        available_at=_require_utc(row.available_at, "Bar authority available_at"),
        content_hash=row.authority.content_hash,
        open=row.bar.open,
        close=row.bar.close,
    )


def _bind_candidate(
    candidate: ShadowBarCandidate,
    usage: Literal["execution_and_mark", "mark"],
) -> BoundShadowBar:
    return BoundShadowBar(
        price_bar_record_id=candidate.price_bar_record_id,
        price_bar_set_authority_id=candidate.price_bar_set_authority_id,
        instrument_id=candidate.instrument_id,
        usage=usage,
        source=candidate.source,
        delivery_mode=candidate.delivery_mode,
        revision=candidate.revision,
        available_at=candidate.available_at,
        content_hash=candidate.content_hash,
        open=candidate.open,
        close=candidate.close,
    )


def _latest_candidate_by_source(
    candidates: tuple[ShadowBarCandidate, ...],
    observed_at: datetime,
) -> dict[str, ShadowBarCandidate]:
    result: dict[str, ShadowBarCandidate] = {}
    for source in _SOURCE_PRIORITY:
        selected = select_shadow_bar_candidate(
            tuple(item for item in candidates if item.source == source),
            observed_at=observed_at,
        )
        if selected is not None:
            result[source] = selected
    return result


def _has_source_conflict(
    candidates: tuple[ShadowBarCandidate, ...],
    tick_size: Decimal | None,
) -> bool:
    if len(candidates) < 2:
        return False
    ordered = tuple(
        sorted(candidates, key=lambda item: _SOURCE_PRIORITY.index(item.source))
    )
    primary = ordered[0]
    for candidate in ordered[1:]:
        for first, second in (
            (primary.open, candidate.open),
            (primary.close, candidate.close),
        ):
            tolerance = max(
                tick_size or Decimal(0),
                min(abs(first), abs(second)) * Decimal("0.002"),
            )
            if abs(first - second) > tolerance:
                return True
    return False


def _negative_proof(
    proofs: tuple[_QueryProof, ...], instrument_id: UUID
) -> _QueryProof | None:
    candidates = tuple(
        proof
        for proof in proofs
        if proof.complete and instrument_id in proof.negative_instrument_ids
    )
    return (
        None
        if not candidates
        else min(candidates, key=lambda item: _SOURCE_PRIORITY.index(item.source))
    )


async def _load_bound_events(
    session: AsyncSession,
    *,
    root: LockedShadowRoot,
    session_date: date,
    instruments: tuple[UUID, ...],
    decision_at: datetime,
    proofs: tuple[_QueryProof, ...],
) -> tuple[BoundShadowEvent, ...] | str:
    if not proofs or any(not proof.complete for proof in proofs):
        return "EVENT_QUERY_INCOMPLETE"
    if not instruments:
        return ()
    rows = (
        (
            await session.execute(
                select(CorporateActionRecord, ResearchEvidenceRef)
                .join(
                    ResearchEvidenceRef,
                    ResearchEvidenceRef.id == CorporateActionRecord.evidence_ref_id,
                )
                .where(CorporateActionRecord.instrument_id.in_(instruments))
            )
        )
        .tuples()
        .all()
    )
    event_rows: list[_EventRow] = []
    for record, evidence in rows:
        mapped = _event_sessions(record, root.evaluation_sessions)
        if mapped is None:
            continue
        effective, payable = mapped
        if session_date not in {effective, payable}:
            continue
        if (
            record.status != "complete"
            or evidence.source != record.source
            or evidence.provider_object_id != record.provider_event_id
            or evidence.revision != record.revision
        ):
            return "EVENT_AUTHORITY_INVALID"
        event_rows.append(
            _EventRow(
                record=record,
                evidence=evidence,
                effective_session=effective,
                payable_session=payable,
            )
        )
    proof_by_source = {proof.source: proof for proof in proofs}
    eligible_by_source: dict[str, set[UUID]] = {}
    for row in event_rows:
        if row.evidence.available_at <= decision_at:
            eligible_by_source.setdefault(row.record.source, set()).add(row.record.id)
    for source, member_ids in eligible_by_source.items():
        proof = proof_by_source.get(source)
        if proof is None:
            return "EVENT_QUERY_INCOMPLETE"
        if set(proof.member_ids) != member_ids:
            return "EVENT_QUERY_SET_MISMATCH"
    for proof in proofs:
        if set(proof.member_ids) != eligible_by_source.get(proof.source, set()):
            return "EVENT_QUERY_SET_MISMATCH"

    semantic_groups: dict[
        tuple[UUID | None, str, date, date | None],
        set[tuple[str, date | None, date | None, Decimal | None]],
    ] = {}
    for row in event_rows:
        if row.evidence.available_at > decision_at:
            continue
        event_kind = (
            "split"
            if row.record.action_type in {"stock_split", "reverse_split"}
            else "dividend"
            if row.record.action_type == "cash_dividend"
            else "delisting"
        )
        semantic_groups.setdefault(
            (
                row.record.instrument_id,
                event_kind,
                row.effective_session,
                row.payable_session,
            ),
            set(),
        ).add(
            (
                row.record.action_type,
                row.record.record_date,
                row.record.payable_date,
                row.record.rate,
            )
        )
    if any(len(signatures) > 1 for signatures in semantic_groups.values()):
        return "SOURCE_CONFLICT"

    chosen_source = min(
        (proof.source for proof in proofs),
        key=_SOURCE_PRIORITY.index,
    )
    selected_rows = tuple(
        row
        for row in event_rows
        if row.record.source == chosen_source
        and row.record.id in proof_by_source[chosen_source].member_ids
    )
    bound: list[BoundShadowEvent] = []
    for row in selected_rows:
        if row.evidence.available_at > decision_at:
            return "EVENT_NOT_POINT_IN_TIME"
        event = _bind_event(row)
        if isinstance(event, str):
            return event
        bound.append(event)
    return tuple(bound)


def _event_sessions(
    record: CorporateActionRecord,
    evaluation_sessions: tuple[date, ...],
) -> tuple[date, date | None] | None:
    if record.action_type in {"cash_dividend", "stock_split", "reverse_split"}:
        if record.ex_date is None:
            return None
        effective = _mapped_session(record.ex_date, evaluation_sessions)
        payable = (
            _mapped_session(record.payable_date, evaluation_sessions)
            if record.action_type == "cash_dividend" and record.payable_date is not None
            else None
        )
        return None if effective is None else (effective, payable)
    if record.action_type in {"delisting", "bankruptcy", "merger_cash"}:
        effective = _mapped_session(record.process_date, evaluation_sessions)
        return None if effective is None else (effective, None)
    return None


def _mapped_session(
    provider_date: date | None,
    evaluation_sessions: tuple[date, ...],
) -> date | None:
    if provider_date is None:
        return None
    return next(
        (session_date for session_date in evaluation_sessions if session_date >= provider_date),
        None,
    )


def _bind_event(row: _EventRow) -> BoundShadowEvent | str:
    record = row.record
    evidence = row.evidence
    instrument_id = cast(UUID, record.instrument_id)
    available_at = _require_utc(evidence.available_at, "event available_at")
    if record.action_type in {"stock_split", "reverse_split"}:
        if record.rate is None or record.rate <= 0:
            return "EVENT_AUTHORITY_INVALID"
        return BoundShadowEvent(
            corporate_action_id=record.id,
            instrument_id=instrument_id,
            event_kind="split",
            source=record.source,
            provider_event_id=record.provider_event_id,
            revision=record.revision,
            available_at=available_at,
            evidence_hash=evidence.content_hash,
            split=SplitEvent(
                authority_id=record.id,
                instrument_id=instrument_id,
                ratio=record.rate,
            ),
        )
    if record.action_type == "cash_dividend":
        if record.rate is None or record.rate <= 0 or row.payable_session is None:
            return "EVENT_AUTHORITY_INVALID"
        return BoundShadowEvent(
            corporate_action_id=record.id,
            instrument_id=instrument_id,
            event_kind="dividend",
            source=record.source,
            provider_event_id=record.provider_event_id,
            revision=record.revision,
            available_at=available_at,
            evidence_hash=evidence.content_hash,
            dividend=DividendEvent(
                authority_id=record.id,
                instrument_id=instrument_id,
                amount_per_share=record.rate,
                entitlement_session=row.effective_session,
                payable_session=row.payable_session,
            ),
        )
    if record.action_type == "merger_cash":
        if record.rate is None or record.rate <= 0:
            return "EVENT_AUTHORITY_INVALID"
        recovery = record.rate
    elif record.action_type == "bankruptcy":
        if record.rate != 0:
            return "EVENT_AUTHORITY_INVALID"
        recovery = Decimal(0)
    else:
        return "UNRESOLVED_DELISTING"
    return BoundShadowEvent(
        corporate_action_id=record.id,
        instrument_id=instrument_id,
        event_kind="delisting",
        source=record.source,
        provider_event_id=record.provider_event_id,
        revision=record.revision,
        available_at=available_at,
        evidence_hash=evidence.content_hash,
        delisting=DelistingEvent(
            authority_id=record.id,
            instrument_id=instrument_id,
            recovery_price=recovery,
        ),
    )


def _ordered_summaries(
    proofs: tuple[_QueryProof, ...],
) -> tuple[ShadowSourceSummary, ...]:
    return tuple(
        proof.summary()
        for proof in sorted(
            proofs,
            key=lambda item: (
                _SOURCE_PRIORITY.index(item.source),
                0 if item.kind == "price_bars" else 1,
            ),
        )
    )


def _failure(
    root: LockedShadowRoot,
    session_date: date,
    ordinal: int,
    observed_at: datetime,
    late_backfill: bool,
    status: ShadowAuthorityStatus,
    reason: str,
    *,
    summaries: tuple[ShadowSourceSummary, ...] = (),
    maximum_available_at: datetime | None = None,
) -> ShadowSessionInput:
    return ShadowSessionInput(
        status=status,
        reason=reason,
        session=session_date,
        ordinal=ordinal,
        observed_at=observed_at,
        late_backfill=late_backfill,
        authority_hash=None,
        bars=(),
        missing_execution_inputs=(),
        events=(),
        source_summary=summaries,
        delivery_mode_summary=(),
        maximum_available_at=maximum_available_at,
    )
