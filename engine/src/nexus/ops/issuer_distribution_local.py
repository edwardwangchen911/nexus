"""Import two already-received official originals into current, local research."""

import argparse
import asyncio
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import make_url

from nexus.agents.snapshots import prepare_research_snapshot
from nexus.domain.time import require_utc
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops.fosun_local import load_local_environment
from nexus.ops.local_recovery import write_private
from nexus.persistence.models.data import DataArtifact
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.research import Filing
from nexus.persistence.session import session_factory
from nexus.research.sec_disclosures import read_disclosures
from nexus.research.ssga_distributions import (
    SOURCE_URL,
    parse_distribution,
    persist_distribution,
    read_distributions,
)
from nexus.settings import EngineSettings


def read_original(path: Path, receipt_path: Path) -> tuple[bytes, str, datetime]:
    if path.stat().st_size > 8 * 1024 * 1024:
        raise ValueError("ISSUER_ORIGINAL_TOO_LARGE")
    raw = path.read_bytes()
    receipt = json.loads(receipt_path.read_bytes())
    received = require_utc(datetime.fromisoformat(receipt["received_at"]))
    if (
        received > datetime.now(UTC)
        or receipt["sha256"] != hashlib.sha256(raw).hexdigest()
        or not isinstance(receipt["url"], str)
    ):
        raise ValueError("ISSUER_ORIGINAL_RECEIPT_INVALID")
    return raw, receipt["url"], received


async def import_originals(
    settings: EngineSettings,
    *,
    instrument_id: UUID,
    workbook: tuple[bytes, str, datetime],
    identity: tuple[bytes, str, datetime],
) -> dict[str, object]:
    local = {"localhost", "127.0.0.1", "::1"}
    if (
        not settings.database_url
        or make_url(settings.database_url).host not in local
        or urlsplit(settings.object_store_endpoint or "").hostname not in local
        or workbook[1] != SOURCE_URL
    ):
        raise ValueError("ISSUER_LOCAL_OR_OFFICIAL_SOURCE_REQUIRED")
    received = max(workbook[2], identity[2])
    now = datetime.now(UTC)
    if received > now:
        raise ValueError("ISSUER_ORIGINAL_RECEIPT_INVALID")
    factory = session_factory(settings)
    async with factory() as session, session.begin():
        instrument = await session.get(Instrument, instrument_id)
        aliases = list(
            await session.scalars(
                select(InstrumentAlias.symbol).where(
                    InstrumentAlias.instrument_id == instrument_id,
                    InstrumentAlias.alias_type == "ticker",
                    InstrumentAlias.valid_from <= received.date(),
                    or_(
                        InstrumentAlias.valid_to.is_(None),
                        InstrumentAlias.valid_to >= received.date(),
                    ),
                )
            )
        )
        if instrument is None or instrument.currency != "USD" or len(aliases) != 1:
            raise ValueError("ISSUER_LOCAL_INSTRUMENT_IDENTITY_REQUIRED")
        # Require the already verified SEC fund/share identity as well as the issuer page.
        sec_ids = tuple(
            await session.scalars(
                select(Filing.evidence_ref_id)
                .where(
                    Filing.instrument_id == instrument_id,
                    Filing.source == "sec",
                    Filing.form == "497K",
                    Filing.created_at <= now,
                )
                .order_by(Filing.accepted_at.desc())
                .limit(20)
            )
        )
        disclosures = await read_disclosures(
            session, instrument_id=instrument_id, evidence_ids=sec_ids, as_of=now
        )
        if not any(
            item.form == "497K"
            and item.fund_identity is not None
            and item.fund_identity.symbol == aliases[0]
            for item in disclosures.values()
        ):
            raise ValueError("ISSUER_SEC_SHARE_IDENTITY_REQUIRED")
        document = parse_distribution(
            raw=workbook[0],
            identity_html=identity[0],
            instrument_id=instrument_id,
            symbol=aliases[0],
            identity_url=identity[1],
            received_at=received,
        )
        store = S3ProviderEvidenceStore(settings)
        for kind, original, mime in (
            (
                "distributions",
                workbook,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            ),
            ("identity", identity, "text/html"),
        ):
            raw, _, receipt_time = original
            digest = hashlib.sha256(raw).hexdigest()
            key = f"raw/ssga/{kind}/{digest}.bin"
            uri = await store.put_if_absent(key=key, content=raw)
            if await store.read_raw(key, uri=uri) != raw:
                raise ValueError("ISSUER_ORIGINAL_ARCHIVE_MISMATCH")
            await session.execute(
                insert(DataArtifact)
                .values(
                    uri=uri,
                    sha256=digest,
                    byte_size=len(raw),
                    mime_type=mime,
                    schema_version="issuer_raw_v1",
                    source="ssga",
                    observed_at=receipt_time,
                    available_at=receipt_time,
                    received_at=receipt_time,
                )
                .on_conflict_do_nothing(index_elements=["sha256"])
            )
            stored = await session.scalar(select(DataArtifact).where(DataArtifact.sha256 == digest))
            if (
                stored is None
                or stored.byte_size != len(raw)
                or stored.source != "ssga"
                or stored.uri != uri
                or stored.schema_version != "issuer_raw_v1"
            ):
                raise ValueError("ISSUER_ORIGINAL_BINDING_CONFLICT")
        content = document.canonical_bytes()
        digest = hashlib.sha256(content).hexdigest()
        uri = await store.put_if_absent(
            key=f"evidence/ssga/distributions/{digest}.json", content=content
        )
        evidence = await persist_distribution(session, document, location=uri)
        cutoff = datetime.now(UTC)
        snapshot = await prepare_research_snapshot(session, instrument_id, as_of=cutoff)
        records = await read_distributions(
            session,
            instrument_id=instrument_id,
            as_of=cutoff,
            evidence_ids=(evidence.id,),
        )
        frozen_ids = [] if snapshot is None else snapshot.payload.get("evidence_ids")
        if (
            snapshot is None
            or evidence.id not in records
            or not isinstance(frozen_ids, list)
            or str(evidence.id) not in frozen_ids
        ):
            raise ValueError("ISSUER_FROZEN_READBACK_FAILED")
        return {
            "status": "verified",
            "instrument_id": str(instrument_id),
            "evidence_id": str(evidence.id),
            "research_snapshot_id": str(snapshot.id),
            "as_of": cutoff.isoformat(),
            "source_received_at": received.isoformat(),
            "raw_objects_verified": True,
            "frozen_readback_verified": True,
            "historical_backtest_eligible": False,
            "model_called": False,
        }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--instrument-id", type=UUID, required=True)
    for name in ("workbook", "workbook-receipt", "identity", "identity-receipt", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    try:
        if (
            args.output.exists()
            or args.output.is_symlink()
            or args.output.parent.stat().st_mode & 0o077
        ):
            raise ValueError("ISSUER_PRIVATE_NEW_OUTPUT_REQUIRED")
        settings = EngineSettings.model_validate(load_local_environment(args.repo_root, {}))
        result = asyncio.run(
            import_originals(
                settings,
                instrument_id=args.instrument_id,
                workbook=read_original(args.workbook, args.workbook_receipt),
                identity=read_original(args.identity, args.identity_receipt),
            )
        )
        write_private(args.output, json.dumps(result).encode())
    except (Exception, KeyboardInterrupt):
        print('{"status":"failed","error":"ISSUER_IMPORT_FAILED"}', file=sys.stderr)
        return 1
    print('{"status":"verified","frozen_readback_verified":true,"model_called":false}')
    return 0


if __name__ == "__main__":
    sys.exit(main())
