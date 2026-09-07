"""Import a previously collected news page using the normal local configuration."""

import argparse
import asyncio
import json
import os
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID

from sqlalchemy.engine import make_url

from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.ops.fosun_local import load_local_environment
from nexus.persistence.session import session_factory
from nexus.research.news import import_news
from nexus.research.news_evidence import register_news_evidence
from nexus.settings import EngineSettings


async def run(
    settings: EngineSettings,
    instrument_id: UUID,
    directory: Path | None,
    *,
    news_snapshot_id: UUID | None = None,
) -> dict[str, object]:
    if (directory is None) == (news_snapshot_id is None):
        raise ValueError("NEWS_IMPORT_SCOPE_INVALID")
    local = {"localhost", "127.0.0.1", "::1"}
    database = make_url(settings.database_url or "sqlite://")
    endpoint = urlsplit(settings.object_store_endpoint or "")
    if (
        database.host not in local
        or "host" in database.query
        or endpoint.hostname not in local
        or endpoint.scheme not in {"http", "https"}
        or endpoint.username
        or endpoint.password
        or endpoint.query
        or endpoint.fragment
        or endpoint.path
    ):
        raise ValueError("NEWS_LOCAL_SERVICES_REQUIRED")
    store = S3ProviderEvidenceStore(settings, disable_environment_proxies=True)
    async with session_factory(settings).begin() as session:
        if directory is not None:
            snapshot_id = await import_news(
                session, store, instrument_id=instrument_id, directory=directory
            )
            return {"status": "stored", "snapshot_id": str(snapshot_id), "execution_locked": True}
        assert news_snapshot_id is not None
        evidence = await register_news_evidence(
            session,
            store,
            instrument_id=instrument_id,
            news_snapshot_id=news_snapshot_id,
        )
    return {
        "status": "stored_research_evidence",
        "snapshot_id": str(news_snapshot_id),
        "evidence_ref_id": str(evidence.id),
        "execution_locked": True,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--instrument-id", type=UUID, required=True)
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument("--collection-dir", type=Path)
    scope.add_argument("--news-snapshot-id", type=UUID)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    started = False
    try:
        # Reserve the receipt before a state change; never overwrite a prior result.
        fd = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as handle:
            json.dump({"status": "started"}, handle)
            handle.flush()
            os.fsync(handle.fileno())
            settings = EngineSettings.model_validate(
                load_local_environment(args.repo_root, os.environ)
            )
            started = True
            result = asyncio.run(
                run(settings, args.instrument_id, None, news_snapshot_id=args.news_snapshot_id)
                if args.news_snapshot_id is not None
                else run(settings, args.instrument_id, args.collection_dir)
            )
            handle.seek(0)
            json.dump(result, handle)
            handle.truncate()
            handle.flush()
            os.fsync(handle.fileno())
        print(
            json.dumps(
                {"status": result["status"], "execution_locked": True, "model_called": False}
            )
        )
    except Exception:
        print(
            json.dumps(
                {
                    "status": "unknown" if started else "failed",
                    "code": "NEWS_IMPORT_CHECK_PERSISTED_STATE"
                    if started
                    else "NEWS_IMPORT_FAILED",
                }
            )
        )
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
