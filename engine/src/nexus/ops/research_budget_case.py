"""Prepare an isolated research case from an explicit Run's public evidence only."""

import argparse
import asyncio
import hashlib
import json
import re
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
from uuid import UUID, uuid4

from sqlalchemy import func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from nexus.agents.codex_versions import DEADLINE_BUDGET_CONFIG_VERSION
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.diagnostics.calendar_policy import XNYS_CALENDAR_ADOPTION_ID
from nexus.ops.openai_local import local_settings, prepare_database
from nexus.ops.research_budget_diagnostic import read_database_url, require_diagnostic_url
from nexus.persistence.base import Base
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.session import session_factory
from nexus.research.current_price_trends import PURPOSE_POLICY_ID
from nexus.settings import EngineSettings

PUBLIC_SOURCES = frozenset(
    {"sec", "fred", "exchange_calendars", "alpaca_current_trends", "fosun_security_classification"}
)
PUBLIC_TABLES = (
    "instruments",
    "instrument_aliases",
    "diagnostic_policies",
    "research_evidence_refs",
    "data_artifacts",
    "data_snapshots",
    "filings",
    "financial_facts",
    "macro_observations",
)
_HASH = re.compile(r"^[0-9a-f]{64}$")


class PublicReadbackError(ValueError):
    def __init__(self, bound: set[str], refs: list[dict[str, object]]) -> None:
        super().__init__("DIAGNOSTIC_PUBLIC_SOURCE_READBACK_INCOMPLETE")
        self.missing_sources = sorted(
            {str(row["source"]) for row in refs if str(row["id"]) not in bound}
        )
        self.extra_count = len(bound - {str(row["id"]) for row in refs})


def public_ids(manifest: list[dict[str, object]]) -> tuple[UUID, ...]:
    return tuple(UUID(str(item["id"])) for item in manifest if item.get("source") in PUBLIC_SOURCES)


def require_public_readback(bound: set[str], refs: list[dict[str, object]]) -> None:
    # Calendar authority is a real dependency explicitly frozen by macro inputs.
    if bound != {str(row["id"]) for row in refs}:
        raise PublicReadbackError(bound, refs)


def identities(value: object) -> tuple[set[UUID], set[str]]:
    """Select metadata identifiers only; never export arbitrary referenced tables."""
    ids: set[UUID] = set()
    hashes: set[str] = set()
    values: Iterable[object]
    if isinstance(value, dict):
        values = value.values()
    elif isinstance(value, list):
        values = value
    else:
        values = (value,)
    for item in values:
        if isinstance(item, dict | list):
            more_ids, more_hashes = identities(item)
            ids.update(more_ids)
            hashes.update(more_hashes)
        elif isinstance(item, str):
            if _HASH.fullmatch(item):
                hashes.add(item)
            else:
                try:
                    ids.add(UUID(item))
                except ValueError:
                    pass
    return ids, hashes


async def public_rows(
    session: AsyncSession, source_run_id: UUID
) -> tuple[UUID, dict[str, list[dict[str, object]]]]:
    run = await session.get(AgentResearchRunRecord, source_run_id)
    if run is None:
        raise ValueError("DIAGNOSTIC_SOURCE_RUN_MISSING")
    tables = Base.metadata.tables
    selected = public_ids(run.evidence_manifest)
    if not selected or len(selected) > 100:
        raise ValueError("DIAGNOSTIC_PUBLIC_EVIDENCE_REQUIRED")
    rows: dict[str, list[dict[str, object]]] = {}

    async def collect(name: str, condition: ColumnElement[bool], maximum: int = 5000) -> None:
        result = [
            dict(row)
            for row in (
                await session.execute(select(tables[name]).where(condition).limit(maximum + 1))
            ).mappings()
        ]
        if len(result) > maximum:
            raise ValueError("DIAGNOSTIC_PUBLIC_ROWS_LIMIT")
        rows[name] = result

    await collect("instruments", tables["instruments"].c.id == run.instrument_id, 1)
    await collect(
        "instrument_aliases", tables["instrument_aliases"].c.instrument_id == run.instrument_id, 20
    )
    await collect(
        "diagnostic_policies",
        tables["diagnostic_policies"].c.id.in_((XNYS_CALENDAR_ADOPTION_ID, PURPOSE_POLICY_ID)),
        2,
    )
    refs = tables["research_evidence_refs"]
    await collect("research_evidence_refs", refs.c.id.in_(selected), 100)
    if len(rows["research_evidence_refs"]) != len(selected) or any(
        row["source"] not in PUBLIC_SOURCES for row in rows["research_evidence_refs"]
    ):
        raise ValueError("DIAGNOSTIC_PUBLIC_IDENTITY_INVALID")
    snapshots = tables["data_snapshots"]
    await collect(
        "data_snapshots",
        or_(
            *(
                (snapshots.c.source == "fred")
                & (snapshots.c.schema_version == "fred_vintage_json_v1")
                & (snapshots.c.revision == row["content_hash"])
                if row["source"] == "fred"
                else (snapshots.c.source == row["source"])
                & (snapshots.c.provider_object_id == row["provider_object_id"])
                & (snapshots.c.revision == row["revision"])
                for row in rows["research_evidence_refs"]
            )
        ),
        100,
    )
    ids, hashes = identities([row["payload"] for row in rows["data_snapshots"]])
    hashes.update(str(row["content_hash"]) for row in rows["research_evidence_refs"])
    artifacts = tables["data_artifacts"]
    await collect(
        "data_artifacts", or_(artifacts.c.id.in_(ids), artifacts.c.sha256.in_(hashes)), 300
    )
    await collect("filings", tables["filings"].c.evidence_ref_id.in_(selected), 100)
    await collect("financial_facts", tables["financial_facts"].c.evidence_ref_id.in_(selected))
    await collect(
        "macro_observations", tables["macro_observations"].c.evidence_ref_id.in_(selected)
    )
    return run.instrument_id, rows


async def prepare_case(
    source: EngineSettings, target: EngineSettings, source_run_id: UUID
) -> dict[str, str | int]:
    require_diagnostic_url(target.database_url or "")
    source_factory = session_factory(source)
    target_factory = session_factory(target)
    try:
        async with source_factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            instrument_id, rows = await public_rows(session, source_run_id)
        async with target_factory() as session, session.begin():
            database = await session.scalar(text("SELECT current_database()"))
            if not isinstance(database, str) or not database.startswith("nexus_diag_"):
                raise ValueError("DIAGNOSTIC_DATABASE_REQUIRED")
            for name in (
                *PUBLIC_TABLES,
                "accounts",
                "jobs",
                "agent_research_runs",
                "broker_account_bindings",
                "agent_paper_cycles",
                "paper_orders",
            ):
                if await session.scalar(
                    select(func.count()).select_from(Base.metadata.tables[name])
                ):
                    raise ValueError("DIAGNOSTIC_EMPTY_DATABASE_REQUIRED")
            for name in PUBLIC_TABLES:
                if rows[name]:
                    await session.execute(Base.metadata.tables[name].insert(), rows[name])
            now = await session.scalar(select(func.clock_timestamp()))
            assert isinstance(now, datetime)
            snapshot = await prepare_research_snapshot(session, instrument_id, as_of=now)
            if snapshot is None:
                raise ValueError("DIAGNOSTIC_PUBLIC_SNAPSHOT_UNAVAILABLE")
            # Do not continue if any selected source was silently dropped during strict readback.
            bound_ids = snapshot.payload["evidence_ids"]
            if not isinstance(bound_ids, list):
                raise ValueError("DIAGNOSTIC_PUBLIC_SOURCE_READBACK_INCOMPLETE")
            bound = set(bound_ids)
            require_public_readback(bound, rows["research_evidence_refs"])
            snapshot_id = snapshot.id
        await prepare_database(
            target,
            structured_synthesis=True,
            aggregate_input_tokens=96000,
            max_wire_bytes=64000,
            inline_citations=True,
            actionable_paper=True,
            stage_time_budget=True,
        )
        async with target_factory() as session, session.begin():
            model = await session.scalar(
                select(AgentModelConfigRecord).where(
                    AgentModelConfigRecord.version == DEADLINE_BUDGET_CONFIG_VERSION
                )
            )
            assert model is not None
            now = await session.scalar(select(func.clock_timestamp()))
            assert isinstance(now, datetime)
            request = ResearchRequest(
                instrument_id=instrument_id, data_snapshot_id=snapshot_id, decision_time=now
            )
            idempotency_key = f"budget-diagnostic:{uuid4()}"
            payload = {
                "request": request.model_dump(mode="json"),
                "model_config_id": str(model.id),
                "idempotency_key": idempotency_key,
            }
            digest = hashlib.sha256(
                json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
            admission = await admit_research_run(
                session,
                actor_id="local-research-cli",
                route="nexus-openai-local/admit-research",
                idempotency_key=idempotency_key,
                request_hash=digest,
                request=request,
                prompt_version_id=None,
                model_config_id=model.id,
                daily_limit_usd=target.agent_daily_budget_usd,
                now=now,
            )
            return {
                "status": "DIAGNOSTIC_CASE_PREPARED",
                "run_id": str(admission.run.id),
                "public_evidence_count": len(bound),
                "model_requests": 0,
                "portfolio_context": "absent",
                "formal_acceptance": False,
            }
    finally:
        await source_factory.kw["bind"].dispose()
        await target_factory.kw["bind"].dispose()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--database-url-file", type=Path, required=True)
    parser.add_argument("--source-run-id", type=UUID, required=True)
    args = parser.parse_args()
    try:
        source = local_settings(args.repo_root, backend="codex_sdk")
        target = source.model_copy(
            update={"database_url": read_database_url(args.database_url_file)}
        )
        result = asyncio.run(prepare_case(source, target, args.source_run_id))
    except Exception as error:
        code = str(error) if isinstance(error, ValueError) else "UNKNOWN"
        safe = {
            "DIAGNOSTIC_SOURCE_RUN_MISSING",
            "DIAGNOSTIC_PUBLIC_EVIDENCE_REQUIRED",
            "DIAGNOSTIC_PUBLIC_ROWS_LIMIT",
            "DIAGNOSTIC_PUBLIC_IDENTITY_INVALID",
            "DIAGNOSTIC_DATABASE_REQUIRED",
            "DIAGNOSTIC_EMPTY_DATABASE_REQUIRED",
            "DIAGNOSTIC_PUBLIC_SNAPSHOT_UNAVAILABLE",
            "DIAGNOSTIC_PUBLIC_SOURCE_READBACK_INCOMPLETE",
        }
        failure_result: dict[str, object] = {
            "status": "DIAGNOSTIC_PREPARATION_STOPPED",
            "code": code if code in safe else "UNKNOWN",
            "model_requests": 0,
        }
        if isinstance(error, PublicReadbackError):
            failure_result["missing_sources"] = error.missing_sources
            failure_result["extra_count"] = error.extra_count
        print(json.dumps(failure_result))
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
