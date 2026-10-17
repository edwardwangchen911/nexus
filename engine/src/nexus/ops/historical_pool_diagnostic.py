"""Private historical research observations; never formal Jobs, orders or acceptance."""

import argparse
import asyncio
import hashlib
import json
import os
import sqlite3
import stat
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Self, cast
from uuid import UUID, uuid4

from nexus.agents.codex_bounded_budget import bounded_stage_config_hash
from nexus.agents.codex_capacity import CapacityPoolMember, preflight_capacity_pool
from nexus.agents.codex_sdk import MODEL_ID
from nexus.agents.codex_staged import StructuredStagedCodexCommittee
from nexus.agents.codex_versions import BOUNDED_STAGE_BUDGET_CONFIG_VERSION
from nexus.agents.committee import ROLE_ORDER, PromptAsset
from nexus.agents.models import ResearchRequest
from nexus.agents.portfolio_context import PortfolioResearchContext
from nexus.agents.portfolio_synthesis_contracts import (
    PortfolioSecurityOutputV1,
    PortfolioSynthesisInputV1,
    PortfolioSynthesisIntentV1,
    canonical_portfolio_synthesis_hash,
    validate_portfolio_synthesis_intent,
)
from nexus.agents.portfolio_synthesis_worker import CodexSynthesisTransport
from nexus.agents.synthesis import citations_bound, staged_intent_semantics_valid, synthesize_intent
from nexus.agents.tools import ReadOnlyTools
from nexus.domain.evidence import EvidenceRef
from nexus.ops.research_budget_diagnostic import _private_directory
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.risk.canonical import canonical_paper_projection_hash

PROFILE = "historical-three-stock-v11-v1"

_SAFE_CODES = frozenset(
    {
        "HISTORICAL_CASE_ALREADY_ATTEMPTED",
        "HISTORICAL_CASE_ALREADY_EXISTS",
        "HISTORICAL_CASE_BUDGET_UNAVAILABLE",
        "HISTORICAL_CASE_CALENDAR_UNAVAILABLE",
        "HISTORICAL_CASE_CLOCK_INVALID",
        "HISTORICAL_CASE_COMPLETE_POOL_REQUIRED",
        "HISTORICAL_CASE_CONTEXT_INVALID",
        "HISTORICAL_CASE_DATABASE_INVALID",
        "HISTORICAL_CASE_DIRECTORY_UNSAFE",
        "HISTORICAL_CASE_ENTRY_INVALID",
        "HISTORICAL_CASE_EVIDENCE_CHANGED",
        "HISTORICAL_CASE_EVIDENCE_INVALID",
        "HISTORICAL_CASE_HASH_MISMATCH",
        "HISTORICAL_CASE_INPUT_CHANGED",
        "HISTORICAL_CASE_INVALID",
        "HISTORICAL_CASE_POOL_INVALID",
        "HISTORICAL_CASE_PROJECTION_UNAVAILABLE",
        "HISTORICAL_CASE_PROMPT_INVALID",
        "HISTORICAL_CASE_READBACK_INVALID",
        "HISTORICAL_CASE_SOURCE_INVALID",
        "HISTORICAL_CASE_TARGET_INVALID",
        "HISTORICAL_RESEARCH_INVALID",
        "HISTORICAL_RESEARCH_NOT_ACTIONABLE",
        "HISTORICAL_SYNTHESIS_BUDGET_INVALID",
    }
)


def safe_error_code(error: BaseException) -> str:
    if len(error.args) == 1 and isinstance(error.args[0], str) and error.args[0] in _SAFE_CODES:
        return error.args[0]
    return "UNKNOWN"


def _wire(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _hash(value: Any) -> str:
    return hashlib.sha256(_wire(value).encode()).hexdigest()


class CaseStore:
    """A private SQLite artifact with no business schema or production connection."""

    def __init__(self, db: sqlite3.Connection) -> None:
        self.db = db

    @classmethod
    def create(cls, path: Path, payload: dict[str, Any]) -> Self:
        _private_directory(path.parent)
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
        os.close(fd)
        db = sqlite3.connect(path)
        db.executescript("""
            CREATE TABLE historical_case (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1), payload TEXT NOT NULL,
                digest TEXT NOT NULL, status TEXT NOT NULL, stage TEXT NOT NULL,
                started_at TEXT, finished_at TEXT, diagnostic TEXT NOT NULL);
            CREATE TABLE historical_outputs (
                ordinal INTEGER PRIMARY KEY, payload TEXT NOT NULL, digest TEXT NOT NULL);
        """)
        db.execute(
            "INSERT INTO historical_case VALUES (1,?,?,?, ?,NULL,NULL,'{}')",
            (_wire(payload), _hash(payload), "prepared", "prepared"),
        )
        db.commit()
        return cls(db)

    @classmethod
    def open(cls, path: Path) -> Self:
        _private_directory(path.parent)
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError("HISTORICAL_CASE_DIRECTORY_UNSAFE")
        db = sqlite3.connect(f"{path.as_uri()}?mode=rw", uri=True)
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if tables != {"historical_case", "historical_outputs"}:
            db.close()
            raise ValueError("HISTORICAL_CASE_DATABASE_INVALID")
        return cls(db)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.db.close()

    def case(self) -> dict[str, Any]:
        raw, digest = self.db.execute("SELECT payload,digest FROM historical_case").fetchone()
        payload = json.loads(raw)
        if _hash(payload) != digest:
            raise ValueError("HISTORICAL_CASE_HASH_MISMATCH")
        return cast(dict[str, Any], payload)

    def claim(self) -> None:
        # Commit uncertainty before any model dispatch. A crash is not retry permission.
        with self.db:
            changed = self.db.execute(
                "UPDATE historical_case SET status='unknown',stage='pool_preflight',started_at=? "
                "WHERE singleton=1 AND status='prepared'",
                (datetime.now(UTC).isoformat(),),
            ).rowcount
            if changed != 1:
                raise ValueError("HISTORICAL_CASE_ALREADY_ATTEMPTED")

    def stage(self, stage: str) -> None:
        with self.db:
            self.db.execute("UPDATE historical_case SET stage=? WHERE singleton=1", (stage,))

    def append(self, ordinal: int, output: dict[str, Any]) -> None:
        with self.db:
            self.db.execute(
                "INSERT INTO historical_outputs VALUES (?,?,?)",
                (ordinal, _wire(output), _hash(output)),
            )

    def diagnostic(self, detail: dict[str, Any]) -> None:
        with self.db:
            self.db.execute(
                "UPDATE historical_case SET diagnostic=? WHERE singleton=1", (_wire(detail),)
            )

    def finish(self, status: str) -> None:
        with self.db:
            self.db.execute(
                "UPDATE historical_case SET status=?,finished_at=? WHERE singleton=1",
                (status, datetime.now(UTC).isoformat()),
            )

    def summary(self) -> dict[str, Any]:
        self.case()
        status, stage, started, finished, detail = self.db.execute(
            "SELECT status,stage,started_at,finished_at,diagnostic FROM historical_case"
        ).fetchone()
        outputs = []
        for raw, digest in self.db.execute(
            "SELECT payload,digest FROM historical_outputs ORDER BY ordinal"
        ):
            if _hash(json.loads(raw)) != digest:
                raise ValueError("HISTORICAL_CASE_HASH_MISMATCH")
            outputs.append(json.loads(raw))
        if status == "observed":
            verify_results(self.case(), outputs)
        return dict(
            profile=PROFILE,
            status=status,
            stage=stage,
            output_count=self.db.execute("SELECT count(*) FROM historical_outputs").fetchone()[0],
            formal_acceptance=False,
            execution_locked=True,
            business_writes=0,
            execution_status="EXECUTION_WINDOW_MISSING",
            retry_allowed=False,
            evidence_as_of=self.case().get("evidence_as_of"),
            observation_started_at=started,
            observation_finished_at=finished,
            diagnostic=json.loads(detail),
        )


def verify_results(case: dict[str, Any], outputs: list[dict[str, Any]]) -> None:
    validate_case(case)
    if len(outputs) != 4:
        raise ValueError("HISTORICAL_CASE_READBACK_INVALID")
    frozen = PortfolioSynthesisInputV1.model_validate(outputs[3]["frozen_input"])
    if canonical_portfolio_synthesis_hash(frozen) != outputs[3]["input_hash"]:
        raise ValueError("HISTORICAL_CASE_READBACK_INVALID")
    expected = PortfolioSynthesisInputV1.model_validate(
        {
            **case["synthesis_template"],
            "security_outputs": frozen.security_outputs,
            "admitted_at": frozen.admitted_at,
            "expires_at": frozen.expires_at,
        }
    )
    if frozen != expected:
        raise ValueError("HISTORICAL_CASE_READBACK_INVALID")
    for row, result, security in zip(
        case["members"], outputs[:3], frozen.security_outputs, strict=True
    ):
        refs = [EvidenceRef.model_validate(v) for v in row["evidence"]]
        if (
            str(security.research_output_id) != result["output_id"]
            or security.content_hash != _hash(result)
            or security.intent_hash != _hash(result["intent"])
            or security.intent.model_dump(mode="json") != result["intent"]
            or tuple(refs) != security.intent.evidence_refs
            or tuple(UUID(i) for i in row["evidence_ids"]) != security.evidence_ref_ids
        ):
            raise ValueError("HISTORICAL_CASE_READBACK_INVALID")
    validate_portfolio_synthesis_intent(
        PortfolioSynthesisIntentV1.model_validate(outputs[3]["intent"]), frozen
    )


class FrozenCommittee(StructuredStagedCodexCommittee):
    def __init__(self, member: dict[str, Any]) -> None:
        self.member = member
        self.request = ResearchRequest.model_validate(member["request"])
        super().__init__(
            prompts={
                role: PromptAsset.model_validate(value) for role, value in member["prompts"].items()
            },
            tools=ReadOnlyTools(
                executors={}
            ),  # No database/provider tool is reachable: _inputs is entirely frozen.
            deadline=asyncio.get_running_loop().time() + 300,
            aggregate_input_tokens=96000,
            max_wire_bytes=100479,
            inline_citations=True,
            paper_stage_scope=True,
            stage_time_budget=True,
            stage_budget_version=BOUNDED_STAGE_BUDGET_CONFIG_VERSION,
        )

    async def _inputs(self, request: ResearchRequest) -> dict[str, object]:
        if request != self.request:
            raise ValueError("HISTORICAL_CASE_INPUT_CHANGED")
        return cast(dict[str, object], json.loads(_wire(self.member["inputs"])))


def validate_case(case: dict[str, Any]) -> None:
    if (
        case.get("profile") != PROFILE
        or case.get("model_hash") != bounded_stage_config_hash()
        or len(case.get("members", [])) != 3
    ):
        raise ValueError("HISTORICAL_CASE_INVALID")
    members = case["members"]
    requests = [ResearchRequest.model_validate(row["request"]) for row in members]
    ids = [str(row.instrument_id) for row in requests]
    if (
        len(set(ids)) != 3
        or ids != case["synthesis_template"]["candidate_instrument_ids"]
        or any(row.decision_time.isoformat() != case["evidence_as_of"] for row in requests)
        or datetime.fromisoformat(case["compiled_at"]) < requests[0].decision_time
    ):
        raise ValueError("HISTORICAL_CASE_POOL_INVALID")
    for row in members:
        prompts = {
            role: PromptAsset.model_validate(value) for role, value in row["prompts"].items()
        }
        if set(prompts) != set(ROLE_ORDER) or any(
            p.role != role or hashlib.sha256(p.content.encode()).hexdigest() != p.content_hash
            for role, p in prompts.items()
        ):
            raise ValueError("HISTORICAL_CASE_PROMPT_INVALID")
        refs = tuple(EvidenceRef.model_validate(value) for value in row["evidence"])
        if (
            row.get("paper_context_validated") is not True
            or not refs
            or len(refs) != len(row["evidence_ids"])
            or len(set(row["evidence_ids"])) != len(refs)
            or any(ref.available_at > requests[0].decision_time for ref in refs)
        ):
            raise ValueError("HISTORICAL_CASE_EVIDENCE_INVALID")
    template = case["synthesis_template"]
    projection = PortfolioSynthesisInputV1._read_projection(template["projection"])
    from nexus.risk.models import PaperPortfolioProjection

    projection = PaperPortfolioProjection.model_validate(projection)
    weights = template["active_target_weights"]
    cash = template["active_target_cash_weight"]
    if (
        case["model_id"] != MODEL_ID
        or template["model_hash"] != case["model_hash"]
        or template["pricing_version"] != BOUNDED_STAGE_BUDGET_CONFIG_VERSION
        or projection.projection_hash != canonical_paper_projection_hash(projection)
        or projection.projection_hash != template["projection_hash"]
        or projection.as_of != requests[0].decision_time
        or str(projection.account_id) != template["account_id"]
        or str(projection.portfolio_id) != template["portfolio_id"]
        or template["decision_time"] != case["evidence_as_of"]
        or (template["active_target_version_id"] is None) != (cash is None)
        or [w["instrument_id"] for w in weights] != template["active_target_instrument_ids"]
        or (
            cash is not None
            and sum((Decimal(w["target_weight"]) for w in weights), Decimal(cash)) != 1
        )
    ):
        raise ValueError("HISTORICAL_CASE_CONTEXT_INVALID")
    prompt = case["synthesis_prompt"]
    if (
        hashlib.sha256(prompt["content"].encode()).hexdigest() != prompt["content_hash"]
        or template["prompt_hash"] != prompt["content_hash"]
    ):
        raise ValueError("HISTORICAL_CASE_PROMPT_INVALID")


async def preflight_frozen_pool(
    case: dict[str, Any],
) -> tuple[list[FrozenCommittee], list[tuple[EvidenceRef, ...]]]:
    validate_case(case)
    committees = [FrozenCommittee(row) for row in case["members"]]
    refs = [
        tuple(EvidenceRef.model_validate(v) for v in row["evidence"]) for row in case["members"]
    ]
    # Tokenization and full-pool capacity checks only; no transport or source refresh.
    async with asyncio.timeout(195):
        await preflight_capacity_pool(
            [CapacityPoolMember(c, c.request, r) for c, r in zip(committees, refs, strict=True)],
            expected_instrument_ids=tuple(c.request.instrument_id for c in committees),
        )
    return committees, refs


async def execute_case(store: CaseStore) -> dict[str, Any]:
    """One new historical observation using v11, not replay/resume of a source Run."""
    store.claim()
    stage = "pool_preflight"
    active: FrozenCommittee | None = None
    try:
        case = store.case()
        committees, refs = await preflight_frozen_pool(case)
        security = []
        for index, (row, committee, evidence) in enumerate(
            zip(case["members"], committees, refs, strict=True)
        ):
            active = committee
            stage = f"research_{index + 1}"
            store.stage(stage)
            # Claim-relative v11 clock: preparation never consumes a later member's budget.
            committee._deadline = asyncio.get_running_loop().time() + 295
            outputs = []
            async with asyncio.timeout(300):
                for role in ROLE_ORDER:
                    outputs.append(
                        await committee.analyze(
                            role, committee.request, evidence, committee._prompts[role]
                        )
                    )
            if (
                any(o.conflicting_facts or not citations_bound(o.claims, evidence) for o in outputs)
                or committee.usage.provider_calls != 3
                or committee.usage.tool_calls
                or committee.usage.input_tokens > 96000
                or committee.usage.output_tokens > 8192
            ):
                raise ValueError("HISTORICAL_RESEARCH_INVALID")
            run = AgentResearchRunRecord(
                id=uuid4(),
                instrument_id=committee.request.instrument_id,
                data_snapshot_id=committee.request.data_snapshot_id,
                decision_time=committee.request.decision_time,
            )
            model = AgentModelConfigRecord(
                model_id=case["model_id"], version=BOUNDED_STAGE_BUDGET_CONFIG_VERSION
            )
            intent = synthesize_intent(
                run,
                row["symbol"],
                DataSnapshot(revision=row["snapshot_revision"]),
                {
                    role: AgentPromptVersionRecord(version=p.version)
                    for role, p in committee._prompts.items()
                },
                model,
                evidence,
                outputs,
                price_authority_valid=row["paper_context_validated"],
                portfolio_context=None
                if row["portfolio_context"] is None
                else PortfolioResearchContext.model_validate(row["portfolio_context"]),
            )
            if (
                not staged_intent_semantics_valid(intent, model.version)
                or intent.quality_status != "complete"
            ):
                raise ValueError("HISTORICAL_RESEARCH_NOT_ACTIONABLE")
            result = dict(
                output_id=str(run.id),
                intent=intent.model_dump(mode="json"),
                roles=[o.model_dump(mode="json") for o in outputs],
                usage=dict(
                    input_tokens=committee.usage.input_tokens,
                    output_tokens=committee.usage.output_tokens,
                    provider_calls=committee.usage.provider_calls,
                ),
            )
            store.append(index, result)
            security.append(
                PortfolioSecurityOutputV1(
                    instrument_id=run.instrument_id,
                    research_output_id=run.id,
                    content_hash=_hash(result),
                    intent_hash=_hash(result["intent"]),
                    evidence_ref_ids=tuple(UUID(v) for v in row["evidence_ids"]),
                    intent=intent,
                )
            )
        active = None
        stage = "portfolio_synthesis"
        store.stage(stage)
        now = datetime.now(UTC)
        # decision_time stays historical; admitted/expires describe only this observation.
        frozen = PortfolioSynthesisInputV1.model_validate(
            {
                **case["synthesis_template"],
                "security_outputs": security,
                "admitted_at": now,
                "expires_at": now + timedelta(seconds=195),
            }
        )
        payload = dict(
            instructions=case["synthesis_prompt"]["content"],
            input_hash=canonical_portfolio_synthesis_hash(frozen),
            frozen_input=frozen.model_dump(mode="json"),
        )
        async with asyncio.timeout(195):
            raw, usage = await CodexSynthesisTransport(formal_stage_capacity=True).invoke(payload)
        if (
            usage.input_tokens > 64000
            or usage.output_tokens > 8192
            or usage.provider_calls != 1
            or usage.tool_calls
        ):
            raise ValueError("HISTORICAL_SYNTHESIS_BUDGET_INVALID")
        portfolio_intent = PortfolioSynthesisIntentV1.model_validate_json(raw)
        validate_portfolio_synthesis_intent(portfolio_intent, frozen)
        store.append(
            3,
            dict(
                intent=portfolio_intent.model_dump(mode="json"),
                input_hash=payload["input_hash"],
                frozen_input=frozen.model_dump(mode="json"),
                usage=dict(
                    input_tokens=usage.input_tokens,
                    output_tokens=usage.output_tokens,
                    provider_calls=usage.provider_calls,
                ),
            ),
        )
        store.finish("observed")
    except (Exception, asyncio.CancelledError) as error:
        # Fixed codes only. Never persist exception messages, model text or command arguments.
        code = (
            "CANCELLED"
            if isinstance(error, asyncio.CancelledError)
            else (
                "TIMEOUT"
                if isinstance(error, TimeoutError)
                else "VALIDATION_REJECTED"
                if isinstance(error, ValueError)
                else "UNKNOWN"
            )
        )
        specific = safe_error_code(error)
        detail: dict[str, Any] = {
            "code": code if specific == "UNKNOWN" else specific,
            "usage_complete": False,
            "unknown_reservations": "held",
        }
        if active is not None:
            detail["known_usage"] = dict(
                input_tokens=active.usage.input_tokens,
                output_tokens=active.usage.output_tokens,
                provider_calls=active.usage.provider_calls,
            )
            if active.failure_diagnostic is not None:
                detail["stage_failure"] = active.failure_diagnostic.model_dump(mode="json")
        store.diagnostic(detail)
        # Unknown usage remains held: claim and stage survive cancellation and process death.
        store.finish("unknown")
        raise
    return store.summary()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    prepare = sub.add_parser("prepare")
    prepare.add_argument("--repo-root", type=Path, required=True)
    prepare.add_argument("--source-cycle-id", type=UUID, required=True)
    for name in ("run", "read"):
        sub.add_parser(name).add_argument("--case-dir", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "prepare":
            from nexus.ops.historical_pool_source import prepare_historical_case

            path = asyncio.run(prepare_historical_case(args.repo_root, args.source_cycle_id))
        else:
            path = args.case_dir.absolute() / "case.sqlite3"
        execution_error = False
        with CaseStore.open(path) as store:
            if args.command == "run":
                try:
                    asyncio.run(execute_case(store))
                except (Exception, KeyboardInterrupt):
                    execution_error = True
            result = store.summary()
        if execution_error:
            result["invocation_status"] = "rejected_or_incomplete"
        print(json.dumps(result))
        return 0 if not execution_error and result["status"] in {"prepared", "observed"} else 1
    except Exception as error:
        print(json.dumps(dict(status="blocked", stage=args.command, code=safe_error_code(error))))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
