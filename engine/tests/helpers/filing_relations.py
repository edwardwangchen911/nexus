"""Exercise relation CLI against a normally registered native foundation scope."""

import binascii
import json
from hashlib import sha256
from uuid import UUID, uuid4

import pytest
from nexus.monitoring.filing_archive import read_filing_content
from nexus.monitoring.filing_content import filing_root
from nexus.monitoring.filing_originals import import_originals, read_originals
from nexus.monitoring.sec_index_reader import read_monitor_index
from nexus.ops.research_monitor import parser, run
from sqlalchemy import text


async def assert_relation_cli(
    factory,
    settings,
    store,
    capture_id,
    root,
    monkeypatch,
    *,
    target_accession=None,
    keep_policy=False,
):
    monkeypatch.setattr(
        "nexus.monitoring.filing_originals.configured_exhibit_store", lambda _: store
    )
    async with factory.begin() as db:
        db.info["sec_exhibit_store"] = store
        now = await db.scalar(text("SELECT pg_catalog.clock_timestamp()"))
        _, index = await read_monitor_index(db, store, snapshot_id=UUID(capture_id), as_of=now)
        entry = next(
            e
            for e in index.entries
            if e.form == "8-K"
            and (target_accession is None or e.accession_number == target_accession)
        )
        base = filing_root(index.issuer_cik, entry.accession_number)
        raw = {
            entry.primary_document: (
                b'<img src="image.jpg"><p>Item 2.02. Quarterly results and reconciliation.</p>'
            )
        }
        graphic = b"\xff\xd8" + b"\x00" * 87 + b"\xff\xd9"
        raw["image.jpg"] = graphic
        members = []
        rows = []
        for sequence, (name, body) in enumerate(raw.items(), 1):
            kind = "8-K" if sequence == 1 else "GRAPHIC"
            content = body
            if kind == "GRAPHIC":
                content = (
                    b"begin 644 image.jpg\n"
                    + b"".join(binascii.b2a_uu(body[i : i + 45]) for i in range(0, len(body), 45))
                    + b"\nend"
                )
            members.append(
                f"<DOCUMENT>\n<TYPE>{kind}\n<SEQUENCE>{sequence}\n<FILENAME>{name}\n<TEXT>\n".encode()
                + content
                + b"\n</TEXT>\n</DOCUMENT>\n"
            )
            rows.append(
                f'<tr><td>{sequence}</td><td>Document</td><td><a href="{name}">{name}</a></td>'
                f"<td>{kind}</td><td>{len(body)}</td></tr>"
            )
        raw[entry.accession_number + "-index.html"] = (
            '<table summary="Document Format Files">' + "".join(rows) + "</table>"
        ).encode()
        raw[entry.accession_number + ".txt"] = (
            f"ACCESSION NUMBER: {entry.accession_number}\nCENTRAL INDEX KEY: {index.issuer_cik}\n"
            "CONFORMED SUBMISSION TYPE: 8-K\n</SEC-HEADER>\n"
        ).encode() + b"".join(members)
        root.mkdir(mode=0o700)
        for name, body in raw.items():
            (root / name).write_bytes(body)
            (root / (name + ".receipt.json")).write_text(
                json.dumps(
                    dict(
                        source_url=base + name,
                        received_at=now.isoformat(),
                        sha256=sha256(body).hexdigest(),
                        byte_length=len(body),
                        http_status=200,
                    )
                )
            )
        saved = await import_originals(
            db,
            index_snapshot_id=UUID(capture_id),
            accession=entry.accession_number,
            archive_dir=root,
            now=now,
        )
        old_id, old_revision, old_payload = saved.id, saved.revision, saved.payload.copy()
        before = [
            await db.scalar(text(f"SELECT count(*) FROM {table}"))
            for table in (
                "jobs",
                "agent_research_runs",
                "agent_daily_budget_reservations",
                "outbox_events",
                "sec_auto_increment_day_claims",
            )
        ]
    first = await run(
        settings, parser().parse_args(["prove-content-relations", "--snapshot-id", str(old_id)])
    )
    async with factory() as db:
        before_export = [
            await db.scalar(text(f"SELECT count(*) FROM {t}"))
            for t in ("data_snapshots", "audit_events")
        ]
    export_dir = root.parent / "review-export"
    export_args = parser().parse_args(
        ["prepare-content-review", "--snapshot-id", str(old_id), "--output-dir", str(export_dir)]
    )
    exported = await run(settings, export_args)
    assert exported["status"] == "prepared_unqualified"
    assert exported["research_qualified"] is False and exported["authority_status"] == "unverified"
    raw_material = (export_dir / "material.json").read_bytes()
    material = json.loads(raw_material)
    complete = json.loads((export_dir / "complete.json").read_bytes())
    assert (
        sha256(raw_material).hexdigest()
        == exported["material_sha256"]
        == complete["material_sha256"]
    )
    assert material["original_hash"] == old_revision
    assert (
        material["research_qualified"] is False and material["reviewer_status"] == "not_designated"
    )
    assert material["original_declarations"][-1]["submission_relation"] == "unverified"
    assert material["derived_relations"]["declarations"][-1]["submission_relation"] == "verified"
    for binding in material["originals"]:
        path = export_dir / binding["filename"]
        assert sha256(path.read_bytes()).hexdigest() == binding["sha256"]
        assert path.stat().st_mode & 0o077 == 0
        receipt = export_dir / (binding["filename"] + ".receipt.json")
        assert sha256(receipt.read_bytes()).hexdigest() == binding["receipt_sha256"]
    with pytest.raises(ValueError, match="SEC_MONITOR_COLLECTION_DIRECTORY_INVALID"):
        await run(settings, export_args)
    denied = root.parent / "review-denied"
    denied_args = parser().parse_args(
        ["prepare-content-review", "--snapshot-id", str(old_id), "--output-dir", str(denied)]
    )
    with pytest.raises(ValueError, match="SEC_DURABLE_SCOPE_INVALID"):
        await run(settings.model_copy(update={"agent_paper_account_id": uuid4()}), denied_args)
    assert not denied.exists()
    foreign_source = root.parent / "review-wrong-source"
    with pytest.raises(ValueError, match="SEC_MONITOR_ARCHIVE_UNAVAILABLE"):
        await run(
            settings,
            parser().parse_args(
                [
                    "prepare-content-review",
                    "--snapshot-id",
                    first["snapshot_id"],
                    "--output-dir",
                    str(foreign_source),
                ]
            ),
        )
    assert not foreign_source.exists()
    from nexus.ops import filing_review_local

    failed_export = root.parent / "review-write-failed"
    normal_write = filing_review_local.write_private

    def fail_receipt_write(path, content):
        if path.name.endswith(".receipt.json"):
            raise OSError("isolated export write failure")
        normal_write(path, content)

    with monkeypatch.context() as patch:
        patch.setattr(filing_review_local, "write_private", fail_receipt_write)
        with pytest.raises(OSError, match="isolated export write failure"):
            await run(
                settings,
                parser().parse_args(
                    [
                        "prepare-content-review",
                        "--snapshot-id",
                        str(old_id),
                        "--output-dir",
                        str(failed_export),
                    ]
                ),
            )
    assert not (failed_export / "complete.json").exists()
    assert not (failed_export / "material.json").exists()
    assert len(list(failed_export.iterdir())) == 1
    async with factory() as db:
        assert before_export == [
            await db.scalar(text(f"SELECT count(*) FROM {t}"))
            for t in ("data_snapshots", "audit_events")
        ]
    from helpers.graphic_adoption import assert_graphic_adoption

    adopted = await assert_graphic_adoption(
        factory,
        settings,
        export_dir / "material.json",
        root.parent,
        monkeypatch,
        store,
        keep_policy=keep_policy,
    )
    for action, sid in (
        ("prove-content-relations", old_id),
        ("inspect-content-relations", first["snapshot_id"]),
    ):
        args = parser().parse_args([action, "--snapshot-id", str(sid)])
        observed = await run(settings, args)
        assert observed["snapshot_id"] == first["snapshot_id"]
        assert observed["original_snapshot_id"] == str(old_id)
        assert observed["research_qualified"] is False
        assert observed["nontext_content_status"] == "uninterpreted"
        assert observed["provider_requests"] == observed["model_requests"] == 0
        assert observed["job_created"] is False
        assert observed["declarations"][0]["submission_relation"] == "verified"
        with pytest.raises(ValueError, match="SEC_DURABLE_SCOPE_INVALID"):
            await run(settings.model_copy(update={"agent_paper_account_id": uuid4()}), args)
    async with factory.begin() as db:
        db.info["sec_exhibit_store"] = store
        now = await db.scalar(text("SELECT pg_catalog.clock_timestamp()"))
        old = await read_originals(db, snapshot_id=old_id, as_of=now)
        assert (
            old.canonical()
            == json.dumps(old_payload, sort_keys=True, separators=(",", ":")).encode()
        )
        assert sha256(old.canonical()).hexdigest() == old_revision
        assert old.declarations[-1]["submission_relation"] == "unverified"
        assert (
            await read_filing_content(db, snapshot_id=UUID(first["snapshot_id"]), as_of=now) is None
        )
        assert before == [
            await db.scalar(text(f"SELECT count(*) FROM {table}"))
            for table in (
                "jobs",
                "agent_research_runs",
                "agent_daily_budget_reservations",
                "outbox_events",
                "sec_auto_increment_day_claims",
            )
        ]

    return adopted
