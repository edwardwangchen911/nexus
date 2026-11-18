"""Native operator registration, integrity, time, and no-research regressions."""

import asyncio
import json
from contextlib import nullcontext
from datetime import timedelta
from hashlib import sha256
from uuid import UUID, uuid4

import pytest
from nexus.monitoring import filing_adoption as adoption
from nexus.ops.sec_graphic_adoption_local import parser, run
from nexus.persistence.models.data import DataSnapshot
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError


async def assert_graphic_adoption(
    factory, settings, material_path, root, monkeypatch, store, *, keep_policy=False
):
    material = json.loads(material_path.read_bytes())
    graphic = next(d for d in material["original_declarations"] if d["document_type"] == "GRAPHIC")
    value = {
        "material": material,
        "report_text": "Isolated independent visual review: header branding only.",
        "report_source": "isolated-independent-review",
        "owner_message_text": "Isolated explicit owner designation for this exact test package.",
        "owner_message_source": "isolated-observed-owner-message",
        "purpose": "nvda_000088_header_graphic_only",
        "graphic_filename": graphic["filename"],
        "graphic_sha256": graphic["raw_hash"],
        "research_qualified": False,
    }
    # Isolated fixture coordinates replace the one production allowcase. There
    # is no runtime policy/configuration option for replacing these constants.
    with nullcontext(monkeypatch) if keep_policy else monkeypatch.context() as patch:
        for key, item in {
            "ISSUER_CIK": material["issuer_cik"],
            "ACCESSION": material["entry"]["accession_number"],
            "GRAPHIC_FILENAME": value["graphic_filename"],
            "GRAPHIC_SHA256": value["graphic_sha256"],
            "OBJECT_ID": "isolated-graphic-adoption",
            "MATERIAL_SHA256": sha256(adoption.canonical(material)).hexdigest(),
            "REPORT_SHA256": sha256(value["report_text"].encode()).hexdigest(),
            "OWNER_SHA256": sha256(value["owner_message_text"].encode()).hexdigest(),
        }.items():
            patch.setattr(adoption, key, item)
        patch.setattr("nexus.ops.sec_graphic_adoption_local.OBJECT_ID", "isolated-graphic-adoption")
        patch.setattr(
            "nexus.monitoring.filing_adopted_content.GRAPHIC_CONTEXT",
            material["entry"]["primary_document"],
        )
        patch.setattr("nexus.monitoring.filing_archive.configured_exhibit_store", lambda _: store)

        async def read_evidence(key, *, uri):
            assert uri == "s3://synthetic/" + key
            return store.objects[key]

        if not hasattr(store, "read_evidence"):
            patch.setattr(store, "read_evidence", read_evidence, raising=False)
        async with factory.begin() as db:
            # One finite synthetic allowcase, only in an owned disposable DB.
            # Production SQL and its fixed policy are never edited by this test.
            import re

            database = await db.scalar(text("SELECT current_database()"))
            assert re.fullmatch(r"nexus_test_[0-9a-f]{32}", database)
            definition = await db.scalar(
                text(
                    "SELECT pg_get_functiondef("
                    "'public.sec_graphic_adoption_valid(uuid,timestamptz)'::regprocedure)"
                )
            )
            material_sha = adoption.MATERIAL_SHA256
            report_sha = adoption.REPORT_SHA256
            owner_sha = adoption.OWNER_SHA256
            graphic_sha = adoption.GRAPHIC_SHA256
            replacements = {
                "0001045810:0001045810-26-000088:nvdalogoa19.jpg": "isolated-graphic-adoption",
                "1ee097fa875484f5a51b8df1740c4c3aa83a521b46fd3bfcd84be9bfc91d32c3": material_sha,
                "37992ce996ac13cdbd2dec11fe852fdd11f04b6b670282d8d7396b8b414840c6": report_sha,
                "0dd184b199415fe361af5ab2823baaccdd35359fba34b5aacf8667a3a1af48a3": owner_sha,
                "578e5181e5d55b715e64c2e3e53fed5b6cbd2c512f6422105769f0f06056ed32": graphic_sha,
                "0001045810-26-000088": adoption.ACCESSION,
                "0001045810": adoption.ISSUER_CIK,
                "nvdalogoa19.jpg": adoption.GRAPHIC_FILENAME,
            }
            for old, new in replacements.items():
                assert old in definition
                definition = definition.replace(old, new)
            await db.execute(text(definition))
            from datetime import datetime

            from nexus.monitoring.filing_adopted_content import verify_dependency_closure
            from nexus.monitoring.filing_content import verify_text_document
            from nexus.monitoring.filing_transport import prove_document
            from nexus.research.sec_disclosure_index import SecIndexEntry

            directory = material_path.parent
            entry = SecIndexEntry.model_validate(material["entry"])
            source = (directory / (entry.accession_number + ".txt")).read_bytes()
            receipt = (directory / (entry.accession_number + ".txt.receipt.json")).read_bytes()
            documents, bodies = [], {}
            for declaration in material["original_declarations"]:
                if declaration["document_type"] == "GRAPHIC":
                    continue
                name = declaration["filename"]
                raw = (directory / name).read_bytes()
                item = verify_text_document(
                    entry=entry,
                    issuer_cik=material["issuer_cik"],
                    filename=name,
                    kind=declaration["document_type"],
                    size=declaration["declared_size"],
                    raw=raw,
                    receipt=(directory / (name + ".receipt.json")).read_bytes(),
                    now=datetime.fromisoformat(material["prepared_at"]),
                    submission=(source, receipt),
                )
                documents.append(item.model_dump(mode="json"))
                bodies[name] = prove_document(
                    source,
                    raw,
                    accession=entry.accession_number,
                    issuer_cik=material["issuer_cik"],
                    form=entry.form,
                    filename=name,
                    kind=declaration["document_type"],
                    declared_size=declaration["declared_size"],
                )
            closure = verify_dependency_closure(
                bodies,
                graphic=adoption.GRAPHIC_FILENAME,
                source=source,
                accession=entry.accession_number,
                issuer_cik=material["issuer_cik"],
            )
            content_definition = await db.scalar(
                text(
                    "SELECT pg_get_functiondef("
                    "'public.sec_adopted_content_valid(uuid,timestamptz)'::regprocedure)"
                )
            )
            for old, new in {
                "d29624c14794e4070bceb31054f43b9ec4ed50e2000ee48cc8a1f9ffff7d83cf": sha256(
                    adoption.canonical(documents)
                ).hexdigest(),
                "955a0ad99071b1d0f382f08dba85bc446f0abbfbe3da50f2ed8f8f1f1e718e21": sha256(
                    adoption.canonical(closure)
                ).hexdigest(),
            }.items():
                assert old in content_definition
                content_definition = content_definition.replace(old, new)
            await db.execute(text(content_definition))
            immutable = await db.scalar(
                text(
                    "SELECT pg_get_functiondef("
                    "'public.sec_adopted_content_immutable()'::regprocedure)"
                )
            )
            origin = await db.get(DataSnapshot, UUID(material["original_snapshot_id"]))
            for old, new in {
                "1e2b85a50497507f3cad8170e03de9422c35016768ddcf99b556d4a2ba7a6b65": material[
                    "original_hash"
                ],
                "ed8d29fabc52df29b4e8237c4011999b5ee6eaa0393c25ca2494ffe9194a38cb": origin.payload[
                    "index_hash"
                ],
            }.items():
                assert old in immutable
                immutable = immutable.replace(old, new)
            await db.execute(text(immutable))
        path = root / "designation.json"
        path.write_text(json.dumps(value))
        args = parser().parse_args(["register", "--designation", str(path), "--operator-confirmed"])

        async def failed_audit(*args, **kwargs):
            raise ValueError("ISOLATED_ADOPTION_AUDIT_FAILURE")

        with monkeypatch.context() as fault:
            fault.setattr(adoption, "append_audit_event", failed_audit)
            with pytest.raises(ValueError, match="ISOLATED_ADOPTION_AUDIT_FAILURE"):
                await run(settings, args)
        async with factory() as db:
            assert (
                await db.scalar(
                    text("SELECT count(*) FROM data_snapshots WHERE source=:source"),
                    {"source": adoption.SOURCE},
                )
                == 0
            )
        async with factory.begin() as db:
            at = await db.scalar(text("SELECT clock_timestamp()"))
            orphan = DataSnapshot(
                source=adoption.SOURCE,
                schema_version=adoption.SCHEMA,
                provider_object_id=adoption.OBJECT_ID,
                revision=sha256(adoption.canonical(value)).hexdigest(),
                payload=value,
                created_at=at,
                observed_at=at,
                received_at=at,
                available_at=at,
            )
            db.add(orphan)
            await db.flush()
            with pytest.raises(ValueError, match=adoption.ERROR):
                await adoption.read_adoption(
                    db, snapshot_id=orphan.id, account_id=settings.agent_paper_account_id, as_of=at
                )
            with pytest.raises(DBAPIError, match="SEC_GRAPHIC_ADOPTION_INVALID"):
                await db.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
            await db.rollback()
        pair = await asyncio.gather(run(settings, args), run(settings, args))
        assert sum(item["created"] for item in pair) == 1
        assert pair[0]["snapshot_id"] == pair[1]["snapshot_id"]
        first = next(item for item in pair if item["created"])
        assert first["created"] and first["research_qualified"] is False
        assert first["model_requests"] == first["provider_requests"] == 0
        sid = UUID(first["snapshot_id"])
        again = await run(settings, args)
        assert not again["created"] and again["snapshot_id"] == str(sid)
        inspected = await run(settings, parser().parse_args(["inspect", "--snapshot-id", str(sid)]))
        assert inspected["snapshot_id"] == str(sid)
        logical = await run(settings, parser().parse_args(["inspect"]))
        assert logical["snapshot_id"] == str(sid)
        with pytest.raises(ValueError, match="SEC_DURABLE_SCOPE_INVALID"):
            await run(settings.model_copy(update={"agent_paper_account_id": uuid4()}), args)
        async with factory.begin() as db:
            row = await db.get(DataSnapshot, sid)
            with pytest.raises(ValueError, match=adoption.ERROR):
                await adoption.read_adoption(
                    db,
                    snapshot_id=sid,
                    account_id=settings.agent_paper_account_id,
                    as_of=row.created_at - timedelta(microseconds=1),
                )
        # The unchanged source is still verified independently; this receipt is
        # not a FilingArchive or a Run qualification.
        from nexus.monitoring.filing_archive import read_filing_content

        async with factory() as db:
            assert (
                await read_filing_content(
                    db, snapshot_id=sid, as_of=await db.scalar(text("SELECT clock_timestamp()"))
                )
                is None
            )
        from nexus.ops.research_monitor import parser as monitor_parser
        from nexus.ops.research_monitor import run as monitor_run

        inspected = await monitor_run(
            settings,
            monitor_parser().parse_args(
                ["inspect-reviewed-content", "--adoption-snapshot-id", str(sid)]
            ),
        )
        assert inspected["status"] == "reviewed_content_verified_readonly"
        args = monitor_parser().parse_args(
            ["publish-reviewed-content", "--adoption-snapshot-id", str(sid)]
        )
        # A late-created publication anchor cannot support an earlier cutoff.
        from nexus.persistence.models.runtime import AuditEvent
        from sqlalchemy import event

        def late_anchor(mapper, connection, target):
            if (
                target.action == "research.monitor.filing_content_received"
                and target.schema_version == "sec_monitor_filing_v3"
            ):
                target.created_at = datetime.fromisoformat(material["prepared_at"]) + timedelta(
                    seconds=30
                )

        event.listen(AuditEvent, "before_insert", late_anchor)
        try:
            with pytest.raises(ValueError, match="SEC_MONITOR_ARCHIVE_INVALID"):
                await monitor_run(settings, args)
        finally:
            event.remove(AuditEvent, "before_insert", late_anchor)
        async with factory.begin() as db:
            at = await db.scalar(text("SELECT clock_timestamp()"))
            plain = DataSnapshot(
                source="fixture_plain",
                schema_version="plain",
                provider_object_id=str(uuid4()),
                revision="0" * 64,
                payload={},
                created_at=at,
                available_at=at,
                received_at=at,
                observed_at=at,
            )
            db.add(plain)
            await db.flush()
            with pytest.raises(DBAPIError, match="SEC_ADOPTED_CONTENT_IMMUTABLE"):
                await db.execute(
                    text(
                        "UPDATE data_snapshots SET schema_version='sec_monitor_filing_v3' "
                        "WHERE id=:id"
                    ),
                    {"id": plain.id},
                )
            await db.rollback()
        from nexus.monitoring import filing_archive

        visible = asyncio.Event()
        release = asyncio.Event()
        normal_publication_audit = filing_archive.append_audit_event

        async def publication_barrier(*args, **kwargs):
            result = await normal_publication_audit(*args, **kwargs)
            if kwargs["schema_version"] == "sec_monitor_filing_v3":
                visible.set()
                await release.wait()
            return result

        with monkeypatch.context() as racing:
            racing.setattr(filing_archive, "append_audit_event", publication_barrier)
            task = asyncio.create_task(monitor_run(settings, args))
            try:
                await asyncio.wait_for(visible.wait(), timeout=15)
                async with factory.begin() as other:
                    with pytest.raises(DBAPIError, match="SEC_ADOPTED_CONTENT_IMMUTABLE"):
                        await other.execute(
                            text("UPDATE data_snapshots SET revision=:bad WHERE id=:id"),
                            {"bad": "f" * 64, "id": UUID(material["original_snapshot_id"])},
                        )
                    await other.rollback()
            finally:
                release.set()
                published = await task
        assert published["status"] == "created" and published["documents"] == 1
        repeated = await monitor_run(settings, args)
        assert (
            repeated["status"] == "existing" and repeated["snapshot_id"] == published["snapshot_id"]
        )
        async with factory() as db:
            at = await db.scalar(text("SELECT clock_timestamp()"))
            verified = await read_filing_content(
                db, snapshot_id=UUID(published["snapshot_id"]), as_of=at
            )
            assert verified is not None and verified.archive.graphic_adoption_id == sid
            inspected_at = (await db.get(DataSnapshot, sid)).created_at
            assert verified.archive.dependency_proof["graphic_references"] == 1
            origin = await db.get(DataSnapshot, UUID(material["original_snapshot_id"]))
            assert (
                verified.archive.model_dump(mode="json")["originals"] == origin.payload["originals"]
            )
        with pytest.raises(ValueError, match="SEC_DURABLE_SCOPE_INVALID"):
            await monitor_run(settings.model_copy(update={"agent_paper_account_id": uuid4()}), args)
        async with factory.begin() as db:
            with pytest.raises(DBAPIError, match="SEC_ADOPTED_CONTENT_IMMUTABLE"):
                await db.execute(
                    text("UPDATE data_snapshots SET revision=:hash WHERE id=:id"),
                    {"hash": "f" * 64, "id": sid},
                )
            await db.rollback()
        async with factory() as db:
            at = await db.scalar(text("SELECT clock_timestamp()"))
            assert await db.scalar(
                text("SELECT sec_adopted_content_valid(:id,:at)"),
                {"id": UUID(published["snapshot_id"]), "at": at},
            )
            assert not await db.scalar(
                text("SELECT sec_adopted_content_valid(:id,:at)"),
                {
                    "id": UUID(published["snapshot_id"]),
                    "at": inspected_at - timedelta(microseconds=1),
                },
            )
            verified = await read_filing_content(
                db, snapshot_id=UUID(published["snapshot_id"]), as_of=at
            )
            frozen = verified.model_dump(mode="json")
            await db.execute(
                text("SELECT sec_adopted_refs_complete(CAST(:v AS jsonb),:at)"),
                {"v": json.dumps({"nested": {"contents": [frozen]}}), "at": at},
            )
            with pytest.raises(DBAPIError, match="SEC_ADOPTED_CONTENT_INVALID"):
                await db.execute(
                    text("SELECT sec_adopted_refs_complete(CAST(:v AS jsonb),:at)"),
                    {
                        "v": json.dumps({"nested": {"contents": [frozen]}}),
                        "at": inspected_at - timedelta(microseconds=1),
                    },
                )
        # Neither a forged v1 label nor an omitted archive hides a real v3 row.
        for shape in ("old_label", "no_schema", "no_archive"):
            forged = json.loads(json.dumps(frozen))
            if shape == "old_label":
                forged["archive"]["schema_version"] = "sec_monitor_filing_v1"
            elif shape == "no_schema":
                del forged["archive"]["schema_version"]
            else:
                del forged["archive"]
            for source_kind in (
                "nexus_monitor_batch_inputs",
                "nexus_monitor_increment_v2",
                "nexus_research",
            ):
                async with factory.begin() as db:
                    at = await db.scalar(text("SELECT clock_timestamp()"))
                    payload = {"nested": {"contents": [forged]}}
                    counterfeit = DataSnapshot(
                        source=source_kind,
                        schema_version="counterfeit_test_v1",
                        provider_object_id=str(uuid4()),
                        revision=sha256(adoption.canonical(payload)).hexdigest(),
                        payload=payload,
                        created_at=at,
                        observed_at=at,
                        received_at=at,
                        available_at=at,
                    )
                    db.add(counterfeit)
                    await db.flush()
                    with pytest.raises(DBAPIError, match="SEC_ADOPTED_CONTENT_INVALID"):
                        await db.execute(
                            text("SET CONSTRAINTS sec_adopted_content_complete IMMEDIATE")
                        )
                    await db.rollback()
        from pathlib import Path

        from alembic import command
        from alembic.config import Config

        config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
        with pytest.raises(DBAPIError, match="SEC_ADOPTED_CONTENT_COMPATIBLE_ROLLBACK_REQUIRED"):
            await asyncio.to_thread(command.downgrade, config, "0135_routine_budget")
        async with factory() as db:
            assert (
                await db.scalar(text("SELECT version_num FROM alembic_version"))
                == "0136_sec_adopted_content"
            )
        # Even with valid SQL metadata, a changed immutable object fails strong read.
        original_id = UUID(material["original_snapshot_id"])
        async with factory() as db:
            origin = await db.get(DataSnapshot, original_id)
            binding = origin.payload["originals"][value["graphic_filename"]][0]
        key = binding["uri"].removeprefix("s3://synthetic/").split("?", 1)[0]
        exact = store.objects[key]
        store.objects[key] = exact[:-1] + bytes([exact[-1] ^ 1])
        original_read = store.read_raw

        async def changed_raw(path, *, uri):
            if path == key:
                return store.objects[path]
            return await original_read(path, uri=uri)

        try:
            with monkeypatch.context() as damaged:
                damaged.setattr(store, "read_raw", changed_raw)
                async with factory() as db:
                    assert (
                        await read_filing_content(
                            db,
                            snapshot_id=UUID(published["snapshot_id"]),
                            as_of=await db.scalar(text("SELECT clock_timestamp()")),
                        )
                        is None
                    )
        finally:
            store.objects[key] = exact
        return published
