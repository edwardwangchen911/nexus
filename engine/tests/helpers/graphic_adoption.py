"""Native operator registration, integrity, time, and no-research regressions."""

import asyncio
import json
from datetime import timedelta
from hashlib import sha256
from uuid import UUID, uuid4

import pytest
from nexus.monitoring import filing_adoption as adoption
from nexus.ops.sec_graphic_adoption_local import parser, run
from nexus.persistence.models.data import DataSnapshot
from sqlalchemy import text


async def assert_graphic_adoption(factory, settings, material_path, root, monkeypatch):
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
    with monkeypatch.context() as patch:
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
            orphan = DataSnapshot(
                source=row.source,
                schema_version=row.schema_version,
                provider_object_id=row.provider_object_id,
                revision=row.revision,
                payload={**row.payload, "report_source": row.payload["report_source"] + "-orphan"},
                created_at=row.created_at,
                observed_at=row.observed_at,
                received_at=row.received_at,
                available_at=row.available_at,
            )
            # Complete valid designation and hash, deliberately missing its audit.
            orphan.revision = sha256(adoption.canonical(orphan.payload)).hexdigest()
            db.add(orphan)
            await db.flush()
            with pytest.raises(ValueError, match=adoption.ERROR):
                await adoption.read_adoption(
                    db,
                    snapshot_id=orphan.id,
                    account_id=settings.agent_paper_account_id,
                    as_of=row.created_at,
                )
            await db.rollback()
        async with factory.begin() as db:
            row = await db.get(DataSnapshot, sid)
            await db.execute(
                text("UPDATE data_snapshots SET revision=:hash WHERE id=:id"),
                {"hash": "f" * 64, "id": sid},
            )
            db.expire(row)
            with pytest.raises(ValueError, match=adoption.ERROR):
                await adoption.read_adoption(
                    db,
                    snapshot_id=sid,
                    account_id=settings.agent_paper_account_id,
                    as_of=await db.scalar(text("SELECT clock_timestamp()")),
                )
            await db.rollback()
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
        return sid
