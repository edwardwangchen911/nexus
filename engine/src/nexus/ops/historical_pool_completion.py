"""A new private comparison using one accepted observation and two new researches."""

import asyncio
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from nexus.ops.historical_pool_diagnostic import (
    COMPLETION_PROFILE,
    PROFILE,
    CaseStore,
    _hash,
    preflight_frozen_pool,
)
from nexus.ops.nvda_rejection_diagnostic import check_detail_storage, diagnostic_directory
from nexus.ops.research_budget_diagnostic import _private_directory

LIMITS = dict(
    new_research_calls=6,
    synthesis_calls=1,
    research_seconds=300,
    research_input_tokens=96000,
    research_output_tokens=8192,
    synthesis_seconds=195,
    synthesis_input_tokens=64000,
    synthesis_output_tokens=8192,
)


def code_hash() -> str:
    from nexus.ops.nvda_rejection_diagnostic import diagnostic_code_hash

    agents = Path(__file__).resolve().parents[1] / "agents"
    return _hash(
        dict(
            base=diagnostic_code_hash(),
            completion=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            portfolio={
                name: hashlib.sha256((agents / name).read_bytes()).hexdigest()
                for name in (
                    "portfolio_synthesis_worker.py",
                    "portfolio_synthesis_contracts.py",
                    "codex_input_view.py",
                )
            },
        )
    )


def completion_directory(root: Path, cycle_id: UUID) -> Path:
    return root / ".secrets/nexus-historical-three-stock" / f"{cycle_id}-completion-v1"


def read_accepted(root: Path, cycle_id: UUID) -> tuple[dict[str, Any], str, int, dict[str, Any]]:
    from nexus.ops.historical_nvda_observation import read_source, target_index

    source, _ = read_source(root, cycle_id)
    with CaseStore.open(
        diagnostic_directory(root, cycle_id) / "case.sqlite3", read_only=True
    ) as store:
        store.db.execute("BEGIN")
        parent, summary = store.case(), store.summary()
        index = target_index(parent)
        restored = {k: v for k, v in parent.items() if k != "observation"}
        restored["profile"] = PROFILE
        if (
            restored != source
            or parent["observation"]["version"] != 2
            or parent["observation"]["repo_root"] != str(root)
            or summary["status"] != "observed"
            or summary["output_count"] != 1
            or summary["observation_finished_at"] is None
            or summary["diagnostic"]
        ):
            raise ValueError("HISTORICAL_OBSERVATION_INVALID")
        rows = list(store.db.execute("SELECT ordinal,payload,digest FROM historical_outputs"))
        receipt = _hash(
            dict(case=list(store.db.execute("SELECT * FROM historical_case")), outputs=rows)
        )
        return parent, receipt, index, json.loads(rows[0][1])


def validate_completion(store: CaseStore, *, execution: bool = False) -> tuple[int, dict[str, Any]]:
    case = store.case()
    metadata = case["completion"]
    root = Path(metadata["repo_root"])
    cycle = UUID(case["source_cycle_id"])
    UUID(metadata["id"])
    prepared = datetime.fromisoformat(metadata["prepared_at"])
    digest = metadata["code_hash"]
    if (
        root != root.resolve()
        or prepared.tzinfo is None
        or not isinstance(digest, str)
        or len(digest) != 64
        or any(c not in "0123456789abcdef" for c in digest)
        or Path(store.db.execute("PRAGMA database_list").fetchone()[2])
        != completion_directory(root, cycle) / "case.sqlite3"
    ):
        raise ValueError("HISTORICAL_OBSERVATION_INVALID")
    parent, receipt, index, output = read_accepted(root, cycle)
    expected = {k: v for k, v in parent.items() if k != "observation"}
    expected.update(
        profile=COMPLETION_PROFILE,
        completion=dict(
            version=1,
            id=metadata["id"],
            repo_root=str(root),
            prepared_at=metadata["prepared_at"],
            parent_observation_id=parent["observation"]["id"],
            parent_receipt_hash=receipt,
            reused_ordinal=index,
            reused_output_hash=_hash(output),
            limits=dict(LIMITS),
            code_hash=code_hash() if execution else digest,
        ),
    )
    if case != expected:
        raise ValueError("HISTORICAL_OBSERVATION_SOURCE_CHANGED")
    ordinals = [
        r[0] for r in store.db.execute("SELECT ordinal FROM historical_outputs ORDER BY ordinal")
    ]
    status = store.db.execute("SELECT status FROM historical_case").fetchone()[0]
    if (
        ordinals != list(range(len(ordinals)))
        or len(ordinals) > 4
        or (status == "observed" and ordinals != [0, 1, 2, 3])
    ):
        raise ValueError("HISTORICAL_CASE_READBACK_INVALID")
    saved = store.db.execute(
        "SELECT payload FROM historical_outputs WHERE ordinal=?", (index,)
    ).fetchone()
    if saved is not None and json.loads(saved[0]) != output:
        raise ValueError("HISTORICAL_CASE_READBACK_INVALID")
    if execution:
        check_detail_storage(store)
    return index, output


async def prepare_completion(root: Path, cycle_id: UUID) -> Path:
    from nexus.ops.historical_nvda_observation import check_current_configuration

    root = root.resolve()
    directory = completion_directory(root, cycle_id)
    _private_directory(root / ".secrets")
    _private_directory(directory.parent)
    if directory.exists():
        raise ValueError("HISTORICAL_CASE_ALREADY_EXISTS")
    parent, receipt, index, output = read_accepted(root, cycle_id)
    case = {k: v for k, v in parent.items() if k != "observation"}
    case.update(
        profile=COMPLETION_PROFILE,
        completion=dict(
            version=1,
            id=str(uuid4()),
            repo_root=str(root),
            prepared_at=datetime.now(UTC).isoformat(),
            parent_observation_id=parent["observation"]["id"],
            parent_receipt_hash=receipt,
            reused_ordinal=index,
            reused_output_hash=_hash(output),
            limits=dict(LIMITS),
            code_hash=code_hash(),
        ),
    )
    async with asyncio.timeout(195):
        await check_current_configuration(root, case, budget_units=3)
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
