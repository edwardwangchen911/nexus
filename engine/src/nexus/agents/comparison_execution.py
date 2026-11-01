"""Execute a fixed experimental position using the normal committee and stage owner.

No business producer/Worker selects this entry. It accepts only an already
registered position; native claim and stage tickets remain the send authority.
"""

import copy
import json
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import httpx
from pydantic import BaseModel
from sqlalchemy import text

from nexus.agents.budget import ResearchAccountingBusy
from nexus.agents.chatgpt_grant import Grant
from nexus.agents.codex_capacity import CapacityPoolMember, preflight_capacity_pool
from nexus.agents.codex_sdk import (
    BASE_INSTRUCTIONS,
    DEVELOPER_INSTRUCTIONS,
    codex_output_schema,
    codex_request_wire,
)
from nexus.agents.codex_staged import StructuredStagedCodexCommittee
from nexus.agents.codex_versions import BOUNDED_STAGE_BUDGET_CONFIG_VERSION
from nexus.agents.committee import ROLE_ORDER, PromptAsset
from nexus.agents.comparison_inputs import PreparedComparisonInput, assert_outbound_safe
from nexus.agents.comparison_manifest import ComparisonManifest
from nexus.agents.comparison_preparation import prepare_archive
from nexus.agents.models import ResearchRequest
from nexus.agents.platform_funding import PlatformFunding, require_funding_binding
from nexus.agents.platform_manifest import PlatformManifest
from nexus.agents.platform_wire import PlatformAllocation
from nexus.agents.tools import ReadOnlyTools
from nexus.domain.evidence import EvidenceRef
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.data import DataSnapshot


class FrozenComparisonCommittee(StructuredStagedCodexCommittee):
    def __init__(
        self,
        prepared: PreparedComparisonInput,
        *,
        deadline: float,
        allocation: PlatformAllocation | None = None,
    ) -> None:
        self.prepared_input = copy.deepcopy(prepared)
        self.frozen_request = ResearchRequest.model_validate(prepared.payload["request"])
        super().__init__(
            prompts={
                r: PromptAsset.model_validate(prepared.payload["prompts"][r]) for r in ROLE_ORDER
            },
            tools=ReadOnlyTools(executors={}),
            deadline=deadline,
            aggregate_input_tokens=96000
            if allocation is None
            else sum(allocation.stage_input_upper_bounds),
            max_wire_bytes=100479
            if allocation is None
            else max(allocation.stage_input_upper_bounds) - 8192,
            platform_allocation=allocation,
            inline_citations=True,
            paper_stage_scope=True,
            stage_time_budget=True,
            stage_budget_version=BOUNDED_STAGE_BUDGET_CONFIG_VERSION,
        )

    async def _inputs(self, request: ResearchRequest) -> dict[str, object]:
        if (
            request != self.frozen_request
            or self.prepared_input.input_hash != self.prepared_input.proof["input_hash"]
        ):
            raise ValueError("COMPARISON_INPUT_CHANGED")
        return copy.deepcopy(self.prepared_input.payload["inputs"])


async def prepare_execution_pool(
    *, source: bytes, prepared: dict[str, Any], manifest: ComparisonManifest, deadline: float
) -> tuple[FrozenComparisonCommittee, ...]:
    expected = prepare_archive(
        source,
        expected_source_hash=manifest.source_manifest_hash,
        experiment_id=manifest.experiment_id,
        purpose=manifest.purpose,
        **(
            {
                "platform_price_version": manifest.price_version,
                "platform_units_version": manifest.allocations[0].units_version,
            }
            if isinstance(manifest, PlatformManifest)
            else {}
        ),
    )
    if (
        json.dumps({k: v for k, v in prepared.items() if k != "rebuilt_at"}, sort_keys=True)
        != json.dumps(expected, sort_keys=True)
        or tuple(expected["input_hashes"]) != manifest.input_hashes
        or tuple(UUID(x) for x in expected["instrument_ids"]) != manifest.instrument_ids
        or expected["identity_equivalence_hash"] != manifest.identity_equivalence_hash
        or expected["outbound_safety_hash"] != manifest.outbound_safety_hash
    ):
        raise ValueError("COMPARISON_PREPARATION_CHANGED")
    pool = tuple(
        FrozenComparisonCommittee(
            PreparedComparisonInput(
                payload=m["payload"], proof=m["proof"], forbidden=tuple(m["forbidden"])
            ),
            deadline=deadline,
            allocation=manifest.allocations[index]
            if isinstance(manifest, PlatformManifest)
            else None,
        )
        for index, m in enumerate(expected["members"])
    )
    if isinstance(manifest, PlatformManifest):
        from nexus.agents.codex_sdk import codex_request_hash
        from nexus.agents.platform_wire import freeze_platform_allocation

        if (
            tuple(
                freeze_platform_allocation(
                    c.prepared_input, profile=a.profile, units_version=a.units_version
                )
                for c, a in zip(pool, manifest.allocations, strict=True)
            )
            != manifest.allocations
        ):
            raise ValueError("PLATFORM_ALLOCATION_CHANGED")
        if tuple(c.frozen_request.instrument_id for c in pool) != manifest.instrument_ids:
            raise ValueError("COMPARISON_INPUT_CHANGED")
        for c in pool:
            refs = tuple(
                EvidenceRef.model_validate(r) for r in c.prepared_input.payload["evidence"]
            )
            payload, schema = c.build_stage_request(
                stage="research",
                request=c.frozen_request,
                evidence_refs=refs,
                frozen_inputs=c.prepared_input.payload["inputs"],
                previous=[],
            )
            c._capacity_pool_manifest = manifest.digest
            c._capacity_pool_request = c.frozen_request
            c._capacity_pool_evidence = refs
            c._capacity_research_hash = codex_request_hash(payload, schema)
        return pool
    await preflight_capacity_pool(
        tuple(
            CapacityPoolMember(
                c,
                c.frozen_request,
                tuple(EvidenceRef.model_validate(r) for r in c.prepared_input.payload["evidence"]),
            )
            for c in pool
        ),
        expected_instrument_ids=manifest.instrument_ids,
    )
    return pool


class ComparisonInvocation:
    def __init__(
        self,
        *,
        manifest: ComparisonManifest,
        ordinal: int,
        committee: FrozenComparisonCommittee,
        grant: Grant | None,
        platform_funding_loader: Callable[[], PlatformFunding] | None = None,
        client_factory: Callable[[], httpx.AsyncClient] | None = None,
    ) -> None:
        self.manifest, self.ordinal, self.committee, self.grant = (
            manifest,
            ordinal,
            committee,
            grant,
        )
        self.platform_funding_loader = platform_funding_loader
        self.platform_funding = (
            None if platform_funding_loader is None else platform_funding_loader()
        )
        if isinstance(manifest, PlatformManifest):
            if grant is not None:
                raise ValueError("PLATFORM_CREDENTIAL_BINDING_MISMATCH")
            require_funding_binding(manifest, self.platform_funding)
        elif platform_funding_loader is not None:
            raise ValueError("COMPARISON_NATIVE_BINDING_INVALID")
        self.transport = manifest.positions[ordinal - 1][2]
        self.client_factory = client_factory or (
            lambda: httpx.AsyncClient(
                timeout=None, follow_redirects=False, auth=None, trust_env=True
            )
        )

    async def validate(self, session: Any, claim: Any, route: Any) -> None:
        from nexus.agents.comparison_routes import comparison_owner
        from nexus.persistence.models.runtime import Job

        run = await session.get(AgentResearchRunRecord, route["run_id"])
        job = await session.get(Job, claim.job_id)
        if (
            run is None
            or job is None
            or route["kind"] != "comparison"
            or await comparison_owner(session, run, job) != self.manifest.experiment_id
            or run.idempotency_key != f"comparison:{self.manifest.experiment_id}:{self.ordinal}"
            or run.input_hash != self.committee.prepared_input.input_hash
            or run.model_hash
            != (
                self.manifest.sdk_config_hash
                if self.transport == "sdk"
                else self.manifest.api_config_hash
            )
        ):
            raise ValueError("COMPARISON_NATIVE_BINDING_INVALID")
        snapshot = await session.get(DataSnapshot, run.data_snapshot_id)
        if (
            snapshot is None
            or snapshot.payload.get("frozen") != self.committee.prepared_input.payload
        ):
            raise ValueError("COMPARISON_INPUT_CHANGED")

    def platform_wire(self, payload: Any, output_model: type[BaseModel], rows: Any) -> Any:
        from nexus.agents.platform_stage_accounting import platform_stage_accounting
        from nexus.agents.platform_wire import PriorPlatformUsage, compile_platform_stage

        assert isinstance(self.manifest, PlatformManifest)
        prepared = self.committee.prepared_input
        if rows and platform_stage_accounting(rows).exact_cost_usd is None:
            raise ValueError("PLATFORM_PRIOR_USAGE_INVALID")
        prior = tuple(
            PriorPlatformUsage(
                stage=r["stage"],
                input_hash=r["platform_binding"]["input_hash"],
                verified=r["verified"],
                response=json.loads(r["platform_response"]),
            )
            for r in rows
        )
        a = self.manifest.allocations[self.ordinal - 1]
        return compile_platform_stage(
            prepared,
            allocation=a,
            expected_allocation_hash=a.allocation_hash,
            stage=payload["stage"],
            previous=[],
            prior_usage=prior,
            compiled_request=(payload, output_model),
        )

    def wire(self, payload: Any, output_model: type[BaseModel]) -> bytes:
        from nexus.agents.research_stages.http_lifecycle import http_stage_wire

        prepared = self.committee.prepared_input
        if self.transport == "responses":
            return http_stage_wire(payload, output_model=output_model, forbidden=prepared.forbidden)
        body = {"wire": codex_request_wire(payload), "schema": codex_output_schema(output_model)}
        assert_outbound_safe(
            {"body": body, "base": BASE_INSTRUCTIONS, "developer": DEVELOPER_INSTRUCTIONS},
            prepared.forbidden,
        )
        wire = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
        assert_outbound_safe(wire.decode(), prepared.forbidden)
        return wire


async def execute_comparison_position(
    *,
    factory: Any,
    source: bytes,
    prepared: dict[str, Any],
    manifest: ComparisonManifest,
    ordinal: int,
    worker_id: str,
    grant: Grant | None = None,
    platform_funding_loader: Callable[[], PlatformFunding] | None = None,
    client_factory: Callable[[], httpx.AsyncClient] | None = None,
) -> Any:
    """Run exactly one admitted position, never admit/replay or refresh credentials."""
    import asyncio

    from nexus.agents.comparison_claim import claim_comparison_position
    from nexus.agents.research_stages.executor import execute_or_capture, execute_staged_claim

    now = datetime.now(UTC)
    if isinstance(manifest, PlatformManifest):
        if grant is not None or platform_funding_loader is None:
            raise ValueError("PLATFORM_FUNDING_REQUIRED")
        require_funding_binding(manifest, platform_funding_loader())
    else:
        if grant is None:
            raise ValueError("COMPARISON_GRANT_WINDOW_INVALID")
        grant.bearer(now)
        if not grant.identity_verified or grant.expires_at <= now.timestamp() + 315:
            raise ValueError("COMPARISON_GRANT_WINDOW_INVALID")
    manifest.input_hash_for(ordinal)
    pool = await prepare_execution_pool(
        source=source,
        prepared=prepared,
        manifest=manifest,
        deadline=asyncio.get_running_loop().time() + 300,
    )
    committee = next(
        c for c in pool if c.prepared_input.input_hash == manifest.input_hash_for(ordinal)
    )
    # Pool validation may consume time. Do not take a once-only claim with an
    # already elapsed envelope or a grant that lost the required lifetime.
    now = datetime.now(UTC)
    if grant is not None:
        grant.bearer(now)
        if grant.expires_at <= now.timestamp() + 315:
            raise ValueError("COMPARISON_GRANT_WINDOW_INVALID")
    if committee._deadline is None or asyncio.get_running_loop().time() >= committee._deadline:
        raise ValueError("COMPARISON_EXECUTION_WINDOW_ELAPSED")
    context = ComparisonInvocation(
        manifest=manifest,
        ordinal=ordinal,
        committee=committee,
        grant=grant,
        platform_funding_loader=platform_funding_loader,
        client_factory=client_factory,
    )
    async with factory() as inspection:
        await inspection.execute(text("SET TRANSACTION READ ONLY"))
        registered = await inspection.scalar(
            text("SELECT manifest_hash FROM research_comparison_manifest WHERE id=:id"),
            {"id": manifest.experiment_id},
        )
        if registered != manifest.digest:
            raise ValueError("COMPARISON_REGISTERED_MANIFEST_CHANGED")
    claim = await claim_comparison_position(
        factory,
        experiment_id=manifest.experiment_id,
        ordinal=ordinal,
        worker_id=worker_id,
        **(
            {"platform_funding_loader": platform_funding_loader}
            if isinstance(manifest, PlatformManifest)
            else {}
        ),
    )

    async def execute(session: Any) -> Any:
        result = await execute_or_capture(
            session,
            run_id=UUID(str(claim.payload["run_id"])),
            committee=committee,
            allow_fixture_evidence=False,
        )
        from nexus.jobs.repository import JobCompletion
        from nexus.worker import _agent_result

        mapped = _agent_result(result.status)
        return JobCompletion(
            outcome=mapped.outcome,
            result_code=mapped.result_code,
            error_code=mapped.error_code,
            finished_at=datetime.now(UTC),
        )

    try:
        return await execute_staged_claim(
            factory=factory,
            claim=claim,
            clock=lambda: datetime.now(UTC),
            execute=execute,
            comparison=context,
        )
    except ResearchAccountingBusy:
        # The CLI retains unknown + no replay for an already claimed position,
        # while exposing the fixed accounting cause instead of erasing it.
        raise
    except Exception as error:
        # A durable attempt already exists. Do not advertise a later validation
        # error as an unclaimed rejection that could be retried.
        raise RuntimeError("COMPARISON_EXECUTION_RESULT_UNKNOWN") from error
