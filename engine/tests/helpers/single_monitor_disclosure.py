"""Synthetic SEC originals through existing normal import/read APIs.

These helpers do not change clocks, source eligibility, requests, triggers or
claims. The caller owns its disposable database's shared controlled clock.
"""

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256
from pathlib import Path
from uuid import UUID

from nexus.monitoring.disclosure_context import DisclosureMonitoringContext
from nexus.monitoring.disclosure_contracts import disclosure_from_inputs
from nexus.ops.research_monitor import run as monitor_entry
from nexus.ops.sec_index_local import import_index
from nexus.persistence.models.data import DataSnapshot
from nexus.research.sec_exhibit_storage import _read_identity
from nexus.research.sec_index_storage import read_index_capture
from sqlalchemy import select, text


@dataclass(frozen=True)
class DisclosureArchiveScope:
    instrument_id: UUID
    identity_snapshot_id: UUID
    baseline_snapshot_id: UUID
    symbol: str
    cik: str
    source_exchange: str
    archive_dir: Path


def _write_source(path: Path, name: str, document: dict, url: str, received_at: datetime) -> None:
    raw = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    path.joinpath(name + ".json").write_bytes(raw)
    path.joinpath(name + "-receipt.json").write_text(
        json.dumps(
            {
                "source_url": url,
                "received_at": received_at.isoformat(),
                "sha256": sha256(raw).hexdigest(),
                "byte_length": len(raw),
                "http_status": 200,
            }
        )
    )


def _archive(
    root: Path,
    *,
    symbol: str,
    cik: str,
    source_exchange: str,
    received_at: datetime,
    entries: list[dict],
    sibling_symbols: tuple[str, ...] = (),
) -> Path:
    root.mkdir(mode=0o700, parents=True)
    symbols = tuple(dict.fromkeys((symbol, *sibling_symbols)))
    _write_source(
        root,
        "directory",
        {
            "fields": ["cik", "name", "ticker", "exchange"],
            "data": [[int(cik), "Synthetic", item, source_exchange] for item in symbols],
        },
        "https://www.sec.gov/files/company_tickers_exchange.json",
        received_at,
    )
    _write_source(
        root,
        symbol + "-submissions",
        {
            "cik": int(cik),
            "tickers": list(symbols),
            "exchanges": [source_exchange] * len(symbols),
            "filings": {
                "files": [],
                "recent": {
                    "accessionNumber": [entry["accession_number"] for entry in entries],
                    "form": [entry["form"] for entry in entries],
                    "acceptanceDateTime": [entry["accepted_at"] for entry in entries],
                    "filingDate": [entry["filed_at"] for entry in entries],
                    "primaryDocument": [entry["primary_document"] for entry in entries],
                },
            },
        },
        f"https://data.sec.gov/submissions/CIK{cik}.json",
        received_at,
    )
    return root


def _entry(cik: str, number: int, accepted_at: datetime) -> dict:
    return {
        "accession_number": f"{cik}-{accepted_at.year % 100:02d}-{number:06d}",
        "form": "8-K",
        "accepted_at": accepted_at.isoformat(),
        "filed_at": accepted_at.date().isoformat(),
        "primary_document": "first.htm" if number == 1 else f"new{number}.htm",
    }


async def baseline_for_existing_identity(
    factory,
    settings,
    store,
    instrument_id: UUID,
    root: Path,
    *,
    received_at: datetime,
    sibling_symbols: tuple[str, ...] = (),
) -> DisclosureArchiveScope:
    """Import a v1 baseline against the existing verified listing, without new identity."""
    async with factory() as db:
        at = await db.scalar(text("SELECT clock_timestamp()"))
        binding = await db.scalar(
            select(DataSnapshot)
            .where(
                DataSnapshot.source == "research_current_identity",
                DataSnapshot.provider_object_id == str(instrument_id),
                DataSnapshot.created_at <= at,
            )
            .order_by(DataSnapshot.created_at.desc(), DataSnapshot.id.desc())
            .limit(1)
        )
        assert binding is not None, "fixture needs an actual current identity binding"
        identity_id = UUID(binding.payload["identity_snapshot_id"])
        proof = await _read_identity(db, store, snapshot_id=identity_id, as_of=at)
        assert proof.instrument_id == instrument_id
        symbol, cik = proof.identity.symbol, proof.identity.cik
        exchanges = {
            "XNYS": "NYSE",
            "NYSE": "NYSE",
            "XNAS": "Nasdaq",
            "NASDAQ": "Nasdaq",
            "Nasdaq": "Nasdaq",
        }
        source_exchange = exchanges[proof.identity.exchange]
        assert received_at <= at
    directory = _archive(
        root,
        symbol=symbol,
        cik=cik,
        source_exchange=source_exchange,
        received_at=received_at,
        entries=[_entry(cik, 1, received_at - timedelta(days=1))],
        sibling_symbols=sibling_symbols,
    )
    result = await import_index(
        settings,
        identity_snapshot_id=identity_id,
        archive_dir=directory,
        store=store,
    )
    assert result["status"] in {"created", "existing"}
    identifier = UUID(result["snapshot_id"])
    async with factory() as db:
        at = await db.scalar(text("SELECT clock_timestamp()"))
        row, index = await read_index_capture(db, store, snapshot_id=identifier, as_of=at)
        assert index.instrument_id == instrument_id
        assert index.issuer_cik == cik and len(index.entries) == 1
        assert row.source == "sec_disclosure_index"
    return DisclosureArchiveScope(
        instrument_id,
        identity_id,
        identifier,
        symbol,
        cik,
        source_exchange,
        directory,
    )


async def append_recent_capture(
    factory,
    settings,
    store,
    scope: DisclosureArchiveScope,
    root: Path,
    *,
    received_at: datetime,
    added_numbers: tuple[int, ...] = (2,),
) -> UUID:
    """Append a complete v1 capture, retaining the baseline and adding distinct accessions."""
    assert added_numbers and len(set(added_numbers)) == len(added_numbers)
    assert all(number > 1 for number in added_numbers)
    original = json.loads(
        scope.archive_dir.joinpath(scope.symbol + "-submissions.json").read_text()
    )
    recent = original["filings"]["recent"]
    entries = [
        {
            "accession_number": recent["accessionNumber"][0],
            "form": recent["form"][0],
            "accepted_at": recent["acceptanceDateTime"][0],
            "filed_at": recent["filingDate"][0],
            "primary_document": recent["primaryDocument"][0],
        }
    ]
    # Discovery time is the new capture receipt. Already listed accessions keep
    # exact accepted/filed metadata across later complete captures (as in the
    # repository's append_capture helper); only new accession membership changes.
    accepted_at = datetime.fromisoformat(recent["acceptanceDateTime"][0])
    entries.extend(_entry(scope.cik, number, accepted_at) for number in added_numbers)
    directory = _archive(
        root,
        symbol=scope.symbol,
        cik=scope.cik,
        source_exchange=scope.source_exchange,
        received_at=received_at,
        entries=entries,
        sibling_symbols=tuple(symbol for symbol in original["tickers"] if symbol != scope.symbol),
    )
    result = await import_index(
        settings,
        identity_snapshot_id=scope.identity_snapshot_id,
        archive_dir=directory,
        store=store,
    )
    identifier = UUID(result["snapshot_id"])
    async with factory() as db:
        at = await db.scalar(text("SELECT clock_timestamp()"))
        row, index = await read_index_capture(db, store, snapshot_id=identifier, as_of=at)
        assert index.instrument_id == scope.instrument_id and index.issuer_cik == scope.cik
        assert len(index.entries) == len(added_numbers) + 1
        assert row.source == "sec_disclosure_index"
    return identifier


def disclosure_response(body: dict, payload: dict, *, price: bool = False) -> None:
    """Use the actual frozen SEC baseline and exact prior proposal hashes.

    Compose after the caller's ordinary economic assessment callback. This helper
    changes only optional monitoring fields, preserving all claims and semantics.
    """
    stage = payload["stage"]
    at = datetime.fromisoformat(payload["request"]["decision_time"])
    if not price:
        for role in body.get("roles", ()):
            role["monitoring_proposal" if stage == "research" else "monitoring_review"] = None
        if stage == "synthesis":
            body["monitoring_adoption_hash"] = None
    proposal = payload.get("disclosure_proposal")
    if stage == "synthesis":
        body["disclosure_adoption_hash"] = proposal["content_hash"] if proposal else None
        return
    context = disclosure_from_inputs(payload["frozen_inputs"])
    frozen = DisclosureMonitoringContext.model_validate_json(json.dumps(context))
    assert frozen.ready and frozen.baseline is not None, "real frozen SEC baseline is required"
    reference = next(
        row["index"]
        for row in payload["evidence"]
        if row["source"]
        in {
            "nexus_agent_paper_context",
            "nexus_monitor_increment_v2",
            "nexus_independent_monitor_research",
        }
    )
    for role in body["roles"]:
        if stage == "research":
            role["disclosure_proposal"] = None
            if role["role"] == "market_state":
                role["disclosure_proposal"] = {
                    "condition": {
                        "signal": "sec.issuer_disclosure_added",
                        "operator": "exists_new",
                        "threshold": True,
                        "unit": "filing",
                        "purpose": "review",
                        "source": "sec",
                        "forms": ["8-K", "8-K/A"],
                        "max_age_seconds": 300,
                        "time_authority": "frozen_index_vs_local_receipt",
                    },
                    "baseline_hash": frozen.baseline.snapshot_hash,
                    "rationale": "新增披露触发原文核验及单股复核。",
                    "citations": [reference],
                    "review_due_at": (at + timedelta(days=6)).isoformat(),
                    "valid_until": (at + timedelta(days=7)).isoformat(),
                }
        elif stage == "critique":
            assert role["role"] in {"bear", "evidence_audit"}
            assert proposal is not None
            role["disclosure_review"] = {
                "proposal_hash": proposal["content_hash"],
                "decision": "accepted",
                "rationale": "已核对冻结基线、披露类别与截止时点。",
                "citations": [reference],
            }


def _filing_archive(root: Path, entry: dict, cik: str, received_at: datetime) -> Path:
    root.mkdir(mode=0o700, parents=True)
    accession = entry["accession_number"]
    base = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession.replace('-', '')}/"
    documents = {
        entry["primary_document"]: (
            entry["form"],
            b"<p>Item 2.02. Quarterly results furnished; fiscal definitions unchanged.</p>",
        ),
        "ex99.htm": (
            "EX-99.1",
            b"<p>Unaudited revenue USD 100 million; quarter, not year to date.</p>",
        ),
    }
    rows = "".join(
        f'<tr><td>{number}</td><td>Document</td><td><a href="{base}{name}">{name}</a></td>'
        f"<td>{kind}</td><td>{len(raw)}</td></tr>"
        for number, (name, (kind, raw)) in enumerate(documents.items(), 1)
    )
    inventory = (
        '<table summary="Document Format Files"><tr><th>Seq</th><th>Description</th>'
        "<th>Document</th><th>Type</th><th>Size</th></tr>" + rows + "</table>"
    ).encode()
    for name, raw in {
        **{name: raw for name, (_, raw) in documents.items()},
        accession + "-index.html": inventory,
    }.items():
        root.joinpath(name).write_bytes(raw)
        root.joinpath(name + ".receipt.json").write_text(
            json.dumps(
                {
                    "source_url": base + name,
                    "received_at": received_at.isoformat(),
                    "sha256": sha256(raw).hexdigest(),
                    "byte_length": len(raw),
                    "http_status": 200,
                }
            )
        )
    return root


async def import_content_and_prepare(
    factory,
    settings,
    store,
    scope: DisclosureArchiveScope,
    index_snapshot_id: UUID,
    batch_id: UUID,
    root: Path,
    *,
    received_at: datetime,
) -> UUID:
    """Normal import-content and prepare-batch; then independently read typed material."""
    from nexus.monitoring.batch_inputs import read_batch_inputs

    async with factory() as db:
        at = await db.scalar(text("SELECT clock_timestamp()"))
        _, index = await read_index_capture(db, store, snapshot_id=index_snapshot_id, as_of=at)
    # Read the actual sealed batch's events instead of importing unrelated filings.
    from nexus.persistence.models.monitoring import MonitorObservationBatch, MonitorTrigger

    async with factory() as db:
        batch = await db.get(MonitorObservationBatch, batch_id)
        assert batch is not None
        triggers = (
            await db.scalars(
                select(MonitorTrigger).where(
                    MonitorTrigger.batch_id == batch_id,
                    MonitorTrigger.kind == "disclosure",
                )
            )
        ).all()
        events = {trigger.event_key for trigger in triggers}
    entries = [
        entry for entry in index.entries if f"sec:{scope.cik}:{entry.accession_number}" in events
    ]
    assert len(entries) == len(events) and entries
    for entry in entries:
        archive = _filing_archive(
            root / entry.accession_number,
            entry.model_dump(mode="json"),
            scope.cik,
            received_at,
        )
        result = await monitor_entry(
            settings,
            argparse.Namespace(
                action="import-content",
                index_snapshot_id=index_snapshot_id,
                accession=entry.accession_number,
                archive_dir=archive,
            ),
        )
        assert result["status"] in {"imported", "existing", "created"}, result
    prepared = await monitor_entry(
        settings, argparse.Namespace(action="prepare-batch", batch_id=batch_id)
    )
    assert prepared["status"] == "prepared", prepared
    identifier = UUID(prepared["snapshot_id"])
    async with factory() as db:
        db.info["sec_exhibit_store"] = store
        at = await db.scalar(text("SELECT clock_timestamp()"))
        material = await read_batch_inputs(db, snapshot_id=identifier, as_of=at)
        assert material is not None and material.batch_id == batch_id
        assert set(material.parents[i]["kind"] for i in range(len(material.parents))) <= {
            "price",
            "disclosure",
        }
        assert all(parent["plan_id"] for parent in material.parents)
    return identifier


def cached_stage_usage() -> dict:
    """A valid synthetic terminal receipt; token totals and original prices stay exact."""
    return {
        "input_tokens": 12000,
        "input_tokens_details": {
            "cached_tokens": 9000,
            "cache_write_tokens": 0,
            "cache_write_tokens_details": {"ttl_30m_tokens": 0, "ttl_12h_tokens": 0},
        },
        "output_tokens": 1000,
        "output_tokens_details": {"reasoning_tokens": 900},
        "total_tokens": 13000,
    }
