"""Normal legacy send gate must validate the current capture itself, never a prior one."""

import copy
import json
from datetime import UTC, datetime
from hashlib import sha256
from uuid import uuid4

import pytest
from sqlalchemy import select, text


@pytest.mark.asyncio
@pytest.mark.parametrize("probe", ["malformed_schema", "latest_original"])
async def test_current_capture_is_qualified_at_send(
    migrated_database_url, monkeypatch, tmp_path, probe
):
    from integration.test_durable_sec_legacy_send_pg import (
        test_legacy_mixed_pause_after_stage_one_has_no_next_send as normal_legacy,
    )
    from nexus.monitoring import durable_sec_permission as permission
    from nexus.monitoring import durable_sec_storage as durable
    from nexus.persistence.models.data import DataSnapshot
    from nexus.research.durable_sec_index_storage import (
        SOURCE,
        persist_durable_index_archive,
        read_durable_index_capture,
    )
    from nexus.research.sec_index_storage import _original

    original_gate = permission.require_new_send
    seen, blocked = [], []

    async def gate(session, run_id):
        result = await original_gate(session, run_id)
        latest = await session.scalar(
            select(DataSnapshot)
            .where(DataSnapshot.source == SOURCE)
            .order_by(
                DataSnapshot.observed_at.desc(),
                DataSnapshot.available_at.desc(),
                DataSnapshot.received_at.desc(),
                DataSnapshot.created_at.desc(),
                DataSnapshot.id.desc(),
            )
            .limit(1)
        )
        if seen or latest is None or not result:
            return result
        seen.append(latest.id)
        store = durable.configured_exhibit_store(session)
        now = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
        _, capture_a = await read_durable_index_capture(
            session, store, snapshot_id=latest.id, as_of=now
        )
        if probe == "malformed_schema":
            for variant in ("missing", "null"):
                nested = await session.begin_nested()
                try:
                    payload = copy.deepcopy(latest.payload)
                    if variant == "missing":
                        payload.pop("schema_version")
                    else:
                        payload["schema_version"] = None
                    at = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
                    bad = DataSnapshot(
                        id=uuid4(),
                        source=latest.source,
                        provider_object_id=latest.provider_object_id,
                        revision=sha256(
                            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
                        ).hexdigest(),
                        schema_version=latest.schema_version,
                        payload=payload,
                        created_at=at,
                        observed_at=at,
                        available_at=at,
                        received_at=at,
                    )
                    session.add(bad)
                    await (
                        session.flush()
                    )  # Insert itself must succeed: exercise actual send rejection.
                    with pytest.raises(ValueError, match="^SEC_RESEARCH_PERMISSION_INVALID$"):
                        await original_gate(session, run_id)
                    blocked.append(variant)
                finally:
                    await nested.rollback()
                assert await original_gate(session, run_id) is True
        else:
            directory = await _original(store, capture_a.originals[0])
            submissions = await _original(store, capture_a.originals[2])
            received = datetime.now(UTC)

            def receipt(raw, url):
                return json.dumps(
                    dict(
                        source_url=url,
                        received_at=received.isoformat(),
                        sha256=sha256(raw).hexdigest(),
                        byte_length=len(raw),
                        http_status=200,
                    )
                ).encode()

            b, _ = await persist_durable_index_archive(
                session,
                store,
                identity_snapshot_id=capture_a.index.identity_snapshot_id,
                directory=(
                    directory,
                    receipt(directory, "https://www.sec.gov/files/company_tickers_exchange.json"),
                ),
                submissions=(
                    submissions,
                    receipt(
                        submissions,
                        f"https://data.sec.gov/submissions/CIK{capture_a.index.issuer_cik}.json",
                    ),
                ),
                now=received,
            )
            assert b.id != latest.id
            now = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
            _, capture_b = await read_durable_index_capture(
                session, store, snapshot_id=b.id, as_of=now
            )
            key = f"raw/sec/disclosure-index/{capture_b.originals[3].sha256}.json"
            assert capture_b.originals[3].sha256 != capture_a.originals[3].sha256
            saved = store.objects[key]
            original_read = store.read_raw

            async def corrupted_read(request_key, *, uri):
                if request_key == key:
                    return saved + b"corrupt-synthetic-only"
                return await original_read(request_key, uri=uri)

            store.read_raw = corrupted_read
            try:
                with pytest.raises(ValueError, match="^SEC_INDEX_ARTIFACT_INVALID$"):
                    await read_durable_index_capture(session, store, snapshot_id=b.id, as_of=now)
                # A remains fully valid; only B's distinct receipt is unavailable/corrupt.
                await read_durable_index_capture(session, store, snapshot_id=latest.id, as_of=now)
                try:
                    permitted = await original_gate(session, run_id)
                except ValueError as error:
                    blocked.append(str(error))
                else:
                    blocked.append("PERMITTED" if permitted else "NO_SCOPE")
            finally:
                store.read_raw = original_read
            assert await original_gate(session, run_id) is True
        return result

    monkeypatch.setattr(permission, "require_new_send", gate)
    await normal_legacy(migrated_database_url, monkeypatch, tmp_path, sec_primary=False)
    assert len(seen) == 1
    if probe == "malformed_schema":
        assert blocked == ["missing", "null"]
    else:
        assert blocked == ["SEC_RESEARCH_PERMISSION_INVALID"], blocked
