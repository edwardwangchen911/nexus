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


async def assert_relation_cli(factory, settings, store, capture_id, root, monkeypatch):
    monkeypatch.setattr(
        "nexus.monitoring.filing_originals.configured_exhibit_store", lambda _: store
    )
    async with factory.begin() as db:
        db.info["sec_exhibit_store"] = store
        now = await db.scalar(text("SELECT pg_catalog.clock_timestamp()"))
        _, index = await read_monitor_index(db, store, snapshot_id=UUID(capture_id), as_of=now)
        entry = next(e for e in index.entries if e.form == "8-K")
        base = filing_root(index.issuer_cik, entry.accession_number)
        raw = {
            entry.primary_document: b"<p>Item 2.02. Quarterly results and reconciliation.</p>"
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
