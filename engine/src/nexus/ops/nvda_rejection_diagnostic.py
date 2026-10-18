"""One new rejection diagnostic bound to the consumed v1 observation, never a retry."""

import asyncio
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from nexus.ops.historical_pool_diagnostic import CaseStore, _hash, preflight_frozen_pool
from nexus.ops.research_budget_diagnostic import _private_directory

CONTRACT = "nvda-rejection-detail-v2"
DETAIL_TABLE_SQL = (
    "CREATE TABLE IF NOT EXISTS historical_rejection_details ("
    "singleton INTEGER PRIMARY KEY CHECK(singleton=1), payload TEXT NOT NULL,digest TEXT NOT NULL)"
)


def diagnostic_code_hash() -> str:
    root = Path(__file__).resolve().parents[1]
    paths = (
        "ops/nvda_rejection_diagnostic.py",
        "ops/historical_nvda_observation.py",
        "ops/historical_pool_diagnostic.py",
        "ops/historical_rejection_archive.py",
        "agents/codex_staged.py",
        "agents/codex_sdk.py",
        "agents/synthesis.py",
    )
    return _hash({p: hashlib.sha256((root / p).read_bytes()).hexdigest() for p in paths})


def diagnostic_directory(root: Path, cycle_id: UUID) -> Path:
    return root / ".secrets/nexus-historical-nvda-observations" / f"{cycle_id}-rejection-v2"


def read_parent(root: Path, cycle_id: UUID) -> tuple[dict[str, Any], str]:
    from nexus.ops.historical_nvda_observation import (
        observation_directory,
        read_source,
        target_index,
    )

    source, source_receipt = read_source(root, cycle_id)
    with CaseStore.open(
        observation_directory(root, cycle_id) / "case.sqlite3", read_only=True
    ) as store:
        store.db.execute("BEGIN")
        parent, summary = store.case(), store.summary()
        index = target_index(parent)
        metadata = parent["observation"]
        restored = {k: v for k, v in parent.items() if k != "observation"}
        restored["profile"] = source["profile"]
        if (
            metadata["version"] != 1
            or metadata["repo_root"] != str(root)
            or restored != source
            or metadata["source_receipt_hash"] != source_receipt
            or summary["status"] != "unknown"
            or summary["stage"] != f"research_{index + 1}"
            or summary["observation_started_at"] is None
            or summary["observation_finished_at"] is None
            or summary["output_count"] != 0
            or summary["diagnostic"].get("code") != "HISTORICAL_RESEARCH_NOT_ACTIONABLE"
            or summary["diagnostic"].get("research_rejection", {}).get("unresolved_review_count", 0)
            < 1
            or store.private_rejection() is not None
        ):
            raise ValueError("HISTORICAL_OBSERVATION_INVALID")
        receipt = _hash(
            dict(
                case=list(store.db.execute("SELECT * FROM historical_case")),
                outputs=list(store.db.execute("SELECT * FROM historical_outputs ORDER BY ordinal")),
            )
        )
        return parent, receipt


def check_detail_storage(store: CaseStore) -> None:
    """Prove private detail write/read support without leaving synthetic evidence."""
    try:
        store.db.execute("BEGIN IMMEDIATE")
        store.db.execute(DETAIL_TABLE_SQL)
        store.db.execute(
            "INSERT INTO historical_rejection_details VALUES (1,?,?)", ("{}", "0" * 64)
        )
        if store.db.execute("SELECT payload FROM historical_rejection_details").fetchone() != (
            "{}",
        ):
            raise ValueError
    except Exception:
        raise ValueError("HISTORICAL_DIAGNOSTIC_STORAGE_UNAVAILABLE") from None
    finally:
        store.db.rollback()


async def prepare_rejection_diagnostic(root: Path, cycle_id: UUID) -> Path:
    from nexus.ops.historical_nvda_observation import check_current_configuration

    root = root.resolve()
    directory = diagnostic_directory(root, cycle_id)
    _private_directory(root / ".secrets")
    _private_directory(directory.parent)
    if directory.exists():
        raise ValueError("HISTORICAL_CASE_ALREADY_EXISTS")
    parent, receipt = read_parent(root, cycle_id)
    case = json.loads(json.dumps(parent))
    case["observation"].update(
        version=2,
        id=str(uuid4()),
        prepared_at=datetime.now(UTC).isoformat(),
        parent_observation_id=parent["observation"]["id"],
        parent_receipt_hash=receipt,
        diagnostic_contract=CONTRACT,
        diagnostic_code_hash=diagnostic_code_hash(),
    )
    async with asyncio.timeout(195):
        await check_current_configuration(root, case)
        await preflight_frozen_pool(case)
    directory.mkdir(mode=0o700)
    path = directory / "case.sqlite3"
    with CaseStore.create(path, case) as store:
        try:
            check_detail_storage(store)
        except Exception:
            store.claim()
            store.diagnostic(dict(code="HISTORICAL_DIAGNOSTIC_STORAGE_UNAVAILABLE"))
            store.finish("unknown")
            raise
    return path


def validate_rejection_identity(store: CaseStore, *, for_execution: bool = True) -> None:
    case = store.case()
    metadata = case["observation"]
    code_hash = metadata.get("diagnostic_code_hash")
    if (
        not isinstance(code_hash, str)
        or len(code_hash) != 64
        or any(c not in "0123456789abcdef" for c in code_hash)
    ):
        raise ValueError("HISTORICAL_OBSERVATION_INVALID")
    root = Path(metadata["repo_root"])
    parent, receipt = read_parent(root, UUID(case["source_cycle_id"]))
    expected = json.loads(json.dumps(parent))
    expected["observation"].update(
        version=2,
        id=metadata["id"],
        prepared_at=metadata["prepared_at"],
        parent_observation_id=parent["observation"]["id"],
        parent_receipt_hash=receipt,
        diagnostic_contract=CONTRACT,
        diagnostic_code_hash=diagnostic_code_hash() if for_execution else code_hash,
    )
    if expected != case:
        raise ValueError("HISTORICAL_OBSERVATION_SOURCE_CHANGED")
    if for_execution:
        check_detail_storage(store)
