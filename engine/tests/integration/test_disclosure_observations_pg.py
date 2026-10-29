"""Exact SEC observation evidence; normal research and index imports use only local fixtures."""

from copy import deepcopy
from datetime import timedelta
from hashlib import sha256
from uuid import UUID

import pytest
from helpers.disclosure_observations import AT, append_capture, reader_source
from nexus.monitoring.disclosure_observations import (
    DisclosureObservation,
    read_disclosure_observation,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.research.sec_index_storage import IndexCapture
from sqlalchemy import text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode",
    [
        "new",
        "same",
        "partial",
        "new_partial",
        "conflict",
        "overflow",
        "stale",
        "corrupt",
        "store",
        "issuer",
    ],
)
async def test_exact_disclosure_observation(migrated_database_url, monkeypatch, tmp_path, mode):
    factory, settings, store, captured, plan = await reader_source(
        migrated_database_url, monkeypatch, tmp_path
    )
    result = await append_capture(
        settings,
        store,
        captured,
        factory,
        tmp_path,
        "current",
        added=33 if mode == "overflow" else 0 if mode in {"same", "partial"} else 1,
        partial=mode in {"partial", "new_partial"},
        conflict=mode == "conflict",
        imported_at=AT + timedelta(minutes=7) if mode == "stale" else None,
    )
    if mode == "issuer":
        async with factory() as session, session.begin():
            original = await session.get(DataSnapshot, UUID(result["snapshot_id"]))
            payload = deepcopy(original.payload)
            payload["index"]["issuer_cik"] = "0000654321"
            payload["index"]["source_url"] = "https://data.sec.gov/submissions/CIK0000654321.json"
            capture = IndexCapture.model_validate(payload)
            values = {
                column.name: getattr(original, column.name)
                for column in original.__table__.columns
                if column.name not in {"id", "created_at"}
            }
            values.update(
                payload=payload,
                revision=sha256(capture.canonical()).hexdigest(),
                available_at=captured["clock"][0] + timedelta(seconds=1),
                received_at=captured["clock"][0] + timedelta(seconds=1),
            )
            session.add(DataSnapshot(**values))
    async with factory() as session, session.begin():
        await session.execute(text("SET TRANSACTION READ ONLY"))
        session.info["sec_exhibit_store"] = store
        if mode == "corrupt":
            row = await session.get(DataSnapshot, UUID(result["snapshot_id"]))
            key = row.payload["originals"][3]["uri"].removeprefix("s3://synthetic/")
            store.objects[key] = b"broken"
        elif mode == "store":
            session.info.clear()
        now = max(
            captured["clock"][0] + timedelta(seconds=2),
            AT + (timedelta(minutes=7, seconds=1) if mode == "stale" else timedelta(seconds=10)),
        )
        observation = await read_disclosure_observation(session, plan=plan, now=now)
        expected = {
            "new": "true",
            "same": "false",
            "partial": "unknown",
            "new_partial": "true",
            "conflict": "unknown",
            "overflow": "true",
            "stale": "unknown",
            "corrupt": "unknown",
            "store": "unknown",
            "issuer": "unknown",
        }[mode]
        assert observation.result == expected
        assert len(observation.added) == (
            33 if mode == "overflow" else 1 if expected == "true" else 0
        )
        if mode in {"new", "new_partial", "overflow"}:
            assert observation.current_snapshot_id == UUID(result["snapshot_id"])
            assert observation.received_at < plan.effective_at
        if mode in {"partial", "new_partial"}:
            assert observation.history_coverage == "partial"
        assert (
            DisclosureObservation.model_validate_json(observation.model_dump_json()) == observation
        )
        assert (await read_disclosure_observation(session, plan=plan, now=now)) == observation
        if mode == "stale":
            assert observation.reason == "SEC_INDEX_STALE"
            assert now - observation.available_at == timedelta(seconds=1)
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_exact_baseline_and_no_source_fallback(migrated_database_url, monkeypatch, tmp_path):
    factory, settings, store, captured, plan = await reader_source(
        migrated_database_url, monkeypatch, tmp_path
    )
    async with factory() as session:
        session.info["sec_exhibit_store"] = store
        before = await read_disclosure_observation(
            session, plan=plan, now=AT + timedelta(seconds=4)
        )
    current = await append_capture(settings, store, captured, factory, tmp_path, "current")
    async with factory() as session:
        session.info["sec_exhibit_store"] = store
        # A later capture in the same bucket stays invisible to the older cutoff.
        assert (
            await read_disclosure_observation(session, plan=plan, now=AT + timedelta(seconds=4))
            == before
        )
        now = max(captured["clock"][0] + timedelta(seconds=1), AT + timedelta(seconds=10))
        observed = await read_disclosure_observation(session, plan=plan, now=now)
        assert observed.result == "true" and observed.current_snapshot_id == UUID(
            current["snapshot_id"]
        )
        for field, value in (
            ("condition_hash", "b" * 64),
            ("baseline_as_of", AT + timedelta(seconds=1)),
            ("kind", "price"),
            ("source_hash", "b" * 64),
        ):
            original = getattr(plan, field)
            setattr(plan, field, value)
            with pytest.raises(ValueError, match="MONITOR_DISCLOSURE_PLAN_INVALID"):
                await read_disclosure_observation(session, plan=plan, now=now)
            setattr(plan, field, original)
    repeated = await append_capture(
        settings, store, captured, factory, tmp_path, "repeated", received=AT + timedelta(seconds=3)
    )
    async with factory() as session:
        session.info["sec_exhibit_store"] = store
        again = await read_disclosure_observation(session, plan=plan, now=now)
        assert again.current_snapshot_id == UUID(repeated["snapshot_id"])
        assert again.current_snapshot_id != observed.current_snapshot_id
        assert again.added == observed.added
    await factory.kw["bind"].dispose()
