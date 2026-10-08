"""Register existing receipt-bound identity archives for future research snapshots only."""

import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import NoReturn
from uuid import UUID

from sqlalchemy.engine import make_url

from nexus.ops.fosun_local import load_local_environment
from nexus.persistence.session import session_factory
from nexus.research.current_identity_storage import import_identity_binding
from nexus.research.sec_exhibit_storage import ExhibitStore, configured_exhibit_store
from nexus.settings import EngineSettings


async def register_identity(
    settings: EngineSettings,
    *,
    identity_snapshot_id: UUID,
    collection_dir: Path,
    store: ExhibitStore | None = None,
) -> dict[str, object]:
    if settings.database_url is None or make_url(settings.database_url).host not in {
        "127.0.0.1",
        "localhost",
        "::1",
    }:
        raise ValueError("RESEARCH_IDENTITY_LOCAL_DATABASE_REQUIRED")
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            evidence = await import_identity_binding(
                session,
                store or configured_exhibit_store(session),
                identity_snapshot_id=identity_snapshot_id,
                collection_dir=collection_dir,
            )
            result: dict[str, object] = {
                "status": "registered",
                "evidence_id": str(evidence.id),
                "execution_locked": True,
                "purpose": "research_issuer_security_binding_only",
            }
        return result
    finally:
        await factory.kw["bind"].dispose()


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise ValueError("RESEARCH_IDENTITY_IMPORT_REQUEST_INVALID")


def main() -> None:
    parser = _Parser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--identity-snapshot-id", type=UUID, required=True)
    parser.add_argument("--collection-dir", type=Path, required=True)
    try:
        args = parser.parse_args()
        settings = EngineSettings.model_validate(load_local_environment(args.repo_root, os.environ))
        result = asyncio.run(
            register_identity(
                settings,
                identity_snapshot_id=args.identity_snapshot_id,
                collection_dir=args.collection_dir,
            )
        )
        print(json.dumps(result))
    except Exception:
        print(json.dumps({"status": "unavailable", "code": "RESEARCH_IDENTITY_IMPORT_FAILED"}))
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
