from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID

import pytest
from nexus.monitoring.durable_sec_research import verify_event_evidence
from nexus.research.sec_disclosure_index import SecIndexEntry

NOW = datetime(2026, 11, 17, 15, tzinfo=UTC)


def test_exact_event_can_be_bound_without_notification_authority():
    entry = SecIndexEntry(
        accession_number="0000000001-26-000001",
        form="8-K",
        accepted_at=NOW - timedelta(minutes=2),
        filed_at=NOW.replace(hour=0),
        primary_document="a.htm",
    )
    event = SimpleNamespace(
        instrument_id=UUID(int=1),
        event_key="sec:0000000001:0000000001-26-000001",
        entry=entry.model_dump(mode="json"),
    )
    index = SimpleNamespace(
        instrument_id=event.instrument_id,
        issuer_cik="0000000001",
        received_at=NOW,
        entries=(entry,),
    )
    capture = SimpleNamespace(index=index, issues=())
    assert verify_event_evidence(event, capture, now=NOW) == entry
    for altered in (
        SimpleNamespace(
            index=SimpleNamespace(**{**vars(index), "received_at": NOW - timedelta(seconds=301)}),
            issues=(),
        ),
        SimpleNamespace(index=SimpleNamespace(**{**vars(index), "entries": ()}), issues=()),
        SimpleNamespace(
            index=index, issues=(SimpleNamespace(accession_number=entry.accession_number),)
        ),
    ):
        with pytest.raises(ValueError, match="DURABLE_SEC_RESEARCH_EVIDENCE_INVALID"):
            verify_event_evidence(event, altered, now=NOW)


def test_same_accession_metadata_conflict_is_not_a_new_binding():
    entry = SecIndexEntry(
        accession_number="0000000001-26-000001",
        form="8-K",
        accepted_at=NOW - timedelta(minutes=2),
        filed_at=NOW.replace(hour=0),
        primary_document="a.htm",
    )
    event = SimpleNamespace(
        instrument_id=UUID(int=1),
        event_key="sec:0000000001:0000000001-26-000001",
        entry=entry.model_dump(mode="json"),
    )
    capture = SimpleNamespace(
        index=SimpleNamespace(
            instrument_id=event.instrument_id,
            issuer_cik="0000000001",
            received_at=NOW,
            entries=(entry.model_copy(update={"primary_document": "changed.htm"}),),
        ),
        issues=(),
    )
    with pytest.raises(ValueError, match="DURABLE_SEC_RESEARCH_EVIDENCE_INVALID"):
        verify_event_evidence(event, capture, now=NOW)


@pytest.mark.asyncio
async def test_readback_rejects_changed_source_binding_before_archive_io():
    from unittest.mock import AsyncMock

    from nexus.monitoring.durable_sec_research import read_binding_observation

    plan = SimpleNamespace(
        id=UUID(int=1),
        account_id=UUID(int=2),
        instrument_id=UUID(int=3),
        source_run_id=UUID(int=4),
        source_output_id=UUID(int=5),
        source_hash="a" * 64,
        condition_hash="c" * 64,
    )
    binding = SimpleNamespace(**{**vars(plan), "plan_id": plan.id, "source_hash": "b" * 64})
    with pytest.raises(ValueError, match="DURABLE_SEC_RESEARCH_EVIDENCE_INVALID"):
        await read_binding_observation(
            SimpleNamespace(scalar=AsyncMock(return_value=binding)),
            check=SimpleNamespace(id=UUID(int=6)),
            plan=plan,
        )


def test_research_only_cli_has_exact_rule_and_no_authority_expansion():
    from nexus.ops.research_monitor import parser

    value = parser().parse_args(["prepare-sec-event-research", "--rule-id", str(UUID(int=8))])
    assert value.rule_id == UUID(int=8)
    with pytest.raises(SystemExit):
        parser().parse_args(["prepare-sec-event-research", "--instrument-id", str(UUID(int=9))])


def test_unidentified_bad_row_does_not_block_independent_event():
    entry = SecIndexEntry(
        accession_number="0000000001-26-000001",
        form="8-K",
        accepted_at=NOW - timedelta(minutes=2),
        filed_at=NOW.replace(hour=0),
        primary_document="a.htm",
    )
    event = SimpleNamespace(
        instrument_id=UUID(int=1),
        event_key="sec:0000000001:0000000001-26-000001",
        entry=entry.model_dump(mode="json"),
    )
    capture = SimpleNamespace(
        index=SimpleNamespace(
            instrument_id=event.instrument_id,
            issuer_cik="0000000001",
            received_at=NOW,
            entries=(entry,),
        ),
        issues=(SimpleNamespace(accession_number=None),),
    )
    assert verify_event_evidence(event, capture, now=NOW) == entry


@pytest.mark.asyncio
async def test_queued_binding_revision_blocks_new_eligibility_but_keeps_history(monkeypatch):
    from unittest.mock import AsyncMock

    from nexus.monitoring import durable_sec_research as bridge

    plan = SimpleNamespace(
        id=UUID(int=1),
        account_id=UUID(int=2),
        instrument_id=UUID(int=3),
        source_run_id=UUID(int=4),
        source_output_id=UUID(int=5),
        source_hash="a" * 64,
        condition_hash="b" * 64,
    )
    entry = SecIndexEntry(
        accession_number="0000000001-26-000001",
        form="8-K",
        accepted_at=NOW - timedelta(minutes=2),
        filed_at=NOW.replace(hour=0),
        primary_document="a.htm",
    )
    event = SimpleNamespace(
        id=UUID(int=6),
        instrument_id=plan.instrument_id,
        rule_id=UUID(int=7),
        metadata_hash="c" * 64,
        event_key="sec:0000000001:0000000001-26-000001",
        entry=entry.model_dump(mode="json"),
    )
    binding = SimpleNamespace(
        **{
            **vars(plan),
            "id": UUID(int=8),
            "plan_id": plan.id,
            "event_id": event.id,
            "rule_id": event.rule_id,
            "metadata_hash": event.metadata_hash,
            "bound_at": NOW,
            "capture_id": UUID(int=9),
            "capture_hash": "d" * 64,
        }
    )
    capture = SimpleNamespace(
        index=SimpleNamespace(
            instrument_id=plan.instrument_id,
            issuer_cik="0000000001",
            received_at=NOW,
            entries=(entry,),
        ),
        issues=(),
    )
    row = SimpleNamespace(id=binding.capture_id, revision=binding.capture_hash)
    monkeypatch.setattr(
        "nexus.research.durable_sec_index_storage.read_durable_index_capture",
        AsyncMock(return_value=(row, capture)),
    )
    monkeypatch.setattr(bridge, "configured_exhibit_store", lambda session: object())
    monkeypatch.setattr(bridge, "_lock", AsyncMock())
    monkeypatch.setattr(bridge, "_time", AsyncMock(return_value=NOW))
    monkeypatch.setattr(bridge, "rule_state", AsyncMock(return_value="active"))
    active = SimpleNamespace(
        scalar=AsyncMock(side_effect=[binding, UUID(int=10)]),
        get=AsyncMock(side_effect=[event, SimpleNamespace(id=event.rule_id)]),
    )
    with pytest.raises(ValueError, match="DURABLE_SEC_RESEARCH_EVIDENCE_INVALID"):
        await bridge.read_binding_observation(
            active,
            check=SimpleNamespace(id=UUID(int=11), checked_at=NOW, event_key=event.event_key),
            plan=plan,
            now=NOW,
        )
    historical = SimpleNamespace(
        scalar=AsyncMock(return_value=binding), get=AsyncMock(return_value=event)
    )
    saved = await bridge.read_binding_observation(
        historical,
        check=SimpleNamespace(id=UUID(int=11), checked_at=NOW, event_key=event.event_key),
        plan=plan,
        current_eligibility=False,
        now=NOW,
    )
    assert saved["event_id"] == str(event.id) and saved["entry"] == event.entry
    historical.scalar.assert_awaited_once()
