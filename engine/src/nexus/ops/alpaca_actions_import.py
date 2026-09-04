"""Import one completed local action archive into current frozen read-only research."""

import argparse
import asyncio
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import NoReturn
from urllib.parse import urlsplit
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.snapshots import prepare_research_snapshot
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.market.scope import US_EQUITY_EXCHANGES
from nexus.ops.fosun_local import load_local_environment
from nexus.ops.local_recovery import write_private
from nexus.persistence.models.data import DataArtifact
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.session import session_factory
from nexus.research.alpaca_actions import SCHEMA
from nexus.research.alpaca_actions_archive import load_action_collection
from nexus.research.alpaca_actions_storage import (
    persist_action_observation,
    read_action_observations,
)
from nexus.settings import EngineSettings


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise ValueError("ALPACA_ACTIONS_IMPORT_ARGUMENT_INVALID")


def _local_services(settings: EngineSettings) -> None:
    local = {"localhost", "127.0.0.1", "::1"}
    endpoint = urlsplit(settings.object_store_endpoint or "")
    database = make_url(settings.database_url) if settings.database_url else None
    if (
        database is None
        or database.host not in local
        or "host" in database.query
        or endpoint.scheme not in {"http", "https"}
        or endpoint.hostname not in local
        or endpoint.username is not None
        or endpoint.password is not None
        or endpoint.path
        or endpoint.query
        or endpoint.fragment
    ):
        raise ValueError("ALPACA_ACTIONS_LOCAL_SERVICES_REQUIRED")


async def _save_artifact(
    session: AsyncSession,
    store: S3ProviderEvidenceStore,
    *,
    content: bytes,
    schema: str,
    received_at: datetime,
    canonical: bool = False,
) -> str:
    digest = hashlib.sha256(content).hexdigest()
    key = (
        f"evidence/alpaca/action-observation/{digest}.json"
        if canonical
        else f"raw/alpaca/actions/{schema}/{digest}.json"
    )
    uri = await store.put_if_absent(key=key, content=content)
    saved = (
        await store.read_evidence(key, uri=uri) if canonical else await store.read_raw(key, uri=uri)
    )
    if saved != content:
        raise ValueError("ALPACA_ACTIONS_OBJECT_READBACK_FAILED")
    await session.execute(
        insert(DataArtifact)
        .values(
            uri=uri,
            sha256=digest,
            byte_size=len(content),
            mime_type="application/json",
            schema_version=schema,
            source="alpaca",
            observed_at=received_at,
            available_at=received_at,
            received_at=received_at,
        )
        .on_conflict_do_nothing(index_elements=["sha256"])
    )
    artifact = await session.scalar(select(DataArtifact).where(DataArtifact.sha256 == digest))
    if artifact is None or (
        artifact.uri != uri
        or artifact.source != "alpaca"
        or artifact.schema_version != schema
        or artifact.byte_size != len(content)
        or artifact.available_at > received_at
        or artifact.received_at > received_at
        or artifact.observed_at > received_at
    ):
        raise ValueError("ALPACA_ACTIONS_ARTIFACT_CONFLICT")
    return uri


async def import_collection(
    settings: EngineSettings,
    *,
    instrument_id: UUID,
    collection_dir: Path,
    store: S3ProviderEvidenceStore | None = None,
) -> dict[str, object]:
    """Objects precede DB commit; a failure can leave immutable orphan objects."""
    _local_services(settings)
    now = datetime.now(UTC)
    factory = session_factory(settings)
    async with factory() as session, session.begin():
        instrument = await session.get(Instrument, instrument_id)
        symbols = list(
            await session.scalars(
                select(InstrumentAlias.symbol).where(
                    InstrumentAlias.instrument_id == instrument_id,
                    InstrumentAlias.alias_type == "ticker",
                    InstrumentAlias.valid_from <= now.date(),
                    or_(InstrumentAlias.valid_to.is_(None), InstrumentAlias.valid_to >= now.date()),
                    InstrumentAlias.created_at <= now,
                )
            )
        )
        if (
            instrument is None
            or instrument.exchange not in US_EQUITY_EXCHANGES
            or instrument.asset_type not in {"equity", "common_stock", "equity_etf"}
            or instrument.created_at > now
            or len(symbols) != 1
        ):
            raise ValueError("ALPACA_ACTIONS_CURRENT_IDENTITY_REQUIRED")
        archive = load_action_collection(
            collection_dir, instrument_id=instrument_id, symbol=symbols[0], now=now
        )
        document = archive.document
        object_store = store or S3ProviderEvidenceStore(settings, disable_environment_proxies=True)
        for original in archive.originals:
            await _save_artifact(
                session,
                object_store,
                content=original.content,
                schema=original.schema_version,
                received_at=original.received_at,
            )
        uri = await _save_artifact(
            session,
            object_store,
            content=document.canonical_bytes(),
            schema=SCHEMA,
            received_at=document.available_at,
            canonical=True,
        )
        evidence = await persist_action_observation(session, document, location=uri)
        cutoff = datetime.now(UTC)
        snapshot = await prepare_research_snapshot(session, instrument_id, as_of=cutoff)
        documents = await read_action_observations(
            session, instrument_id=instrument_id, as_of=cutoff, evidence_ids=(evidence.id,)
        )
        frozen_ids = [] if snapshot is None else snapshot.payload.get("evidence_ids")
        if (
            snapshot is None
            or evidence.id not in documents
            or not isinstance(frozen_ids, list)
            or str(evidence.id) not in frozen_ids
        ):
            raise ValueError("ALPACA_ACTIONS_FROZEN_READBACK_FAILED")
        return {
            "status": "verified",
            "instrument_id": str(instrument_id),
            "evidence_id": str(evidence.id),
            "research_snapshot_id": str(snapshot.id),
            "as_of": cutoff.isoformat().replace("+00:00", "Z"),
            "raw_objects_verified": True,
            "frozen_readback_verified": True,
            "historical_backtest_eligible": False,
            "execution_locked": True,
            "model_called": False,
        }


def main() -> int:
    parser = _Parser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--instrument-id", type=UUID, required=True)
    parser.add_argument("--collection-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    try:
        args = parser.parse_args()
        if (
            args.output.exists()
            or args.output.is_symlink()
            or args.output.parent.stat().st_mode & 0o077
        ):
            raise ValueError("ALPACA_ACTIONS_PRIVATE_NEW_OUTPUT_REQUIRED")
        settings = EngineSettings.model_validate(load_local_environment(args.repo_root, {}))
        result = asyncio.run(
            import_collection(
                settings, instrument_id=args.instrument_id, collection_dir=args.collection_dir
            )
        )
        write_private(args.output, json.dumps(result).encode())
    except (Exception, KeyboardInterrupt):
        print('{"status":"failed","error":"ALPACA_ACTIONS_IMPORT_FAILED"}', file=sys.stderr)
        return 1
    print('{"status":"verified","frozen_readback_verified":true,"model_called":false}')
    return 0


if __name__ == "__main__":
    sys.exit(main())
