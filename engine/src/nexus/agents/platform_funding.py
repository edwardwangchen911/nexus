"""Read the existing shared ledger for a single frozen Platform qualification."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.persistence import _lock_budget_day
from nexus.agents.platform_manifest import PlatformManifest
from nexus.agents.platform_usage import PLATFORM_PRICE_VERSION
from nexus.agents.research_usage import research_reserved_occupancy
from nexus.ops.platform_credentials import (
    ExplicitPlatformSpendAuthorization,
    PlatformCredential,
    PlatformFundingProof,
    check_platform_eligibility,
    load_platform_funding_proof,
    resolve_platform_credential,
)
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)


@dataclass(frozen=True)
class PlatformFunding:
    credential: PlatformCredential
    proof: PlatformFundingProof


def require_funding_binding(manifest: PlatformManifest, funding: PlatformFunding | None) -> None:
    if funding is None:
        raise ValueError("PLATFORM_FUNDING_REQUIRED")
    if (
        funding.proof.proof_hash != manifest.funding_proof_hash
        or funding.credential.binding_hash != manifest.credential_binding_hash
    ):
        raise ValueError("PLATFORM_FUNDING_BINDING_CHANGED")
    record = funding.proof.record
    if isinstance(record, ExplicitPlatformSpendAuthorization):
        if (
            record.experiment_id != manifest.experiment_id
            or record.input_hashes != manifest.input_hashes
        ):
            raise ValueError("PLATFORM_PROOF_BINDING_MISMATCH")
        if manifest.total_reservation_usd > record.max_total_usd:
            raise ValueError("PLATFORM_SPEND_LIMIT_EXCEEDED")


def load_bound_platform_funding(
    root: Path, path: Path, manifest: PlatformManifest
) -> PlatformFunding:
    proof = load_platform_funding_proof(path)
    credential = resolve_platform_credential(
        root,
        expected_fingerprint=proof.record.credential_fingerprint,
        organization_id=proof.record.organization_id,
        project_id=proof.record.project_id,
        endpoint=proof.record.endpoint,
        key_scope_only=isinstance(proof.record, ExplicitPlatformSpendAuthorization),
    )
    result = PlatformFunding(credential, proof)
    require_funding_binding(manifest, result)
    return result


async def require_platform_funding(
    session: AsyncSession,
    manifest: PlatformManifest,
    funding: PlatformFunding | None,
    *,
    required_valid_until: datetime | None = None,
) -> None:
    """Short native accounting transaction, never a new balance or reservation.

    The entire frozen three-stock upper bound remains required even after a
    position settles or the UTC date changes. Own holds and spend are therefore
    counted once, not replenished. Other Platform holds/spend are additional;
    without authoritative posting timestamps, do not assume historic local
    spend is reflected in the manual console snapshot. Normal daily-budget
    admission independently retains all SDK and business occupancy.
    Explicit spend authorization caps only this experiment; it does not invent
    a provider balance against which other activity could be subtracted.
    """
    require_funding_binding(manifest, funding)
    assert funding is not None
    now = await session.scalar(text("SELECT clock_timestamp()"))
    await _lock_budget_day(session, now.date().isoformat())
    now = await session.scalar(text("SELECT clock_timestamp()"))
    own = {
        row.run_id: row.ordinal
        for row in (
            await session.execute(
                text(
                    "SELECT run_id,ordinal FROM research_comparison_chain WHERE experiment_id=:id"
                ),
                {"id": manifest.experiment_id},
            )
        ).all()
    }
    held, spent = Decimal(0), Decimal(0)
    records = await session.execute(
        select(AgentDailyBudgetReservationRecord, AgentResearchRunRecord)
        .join(
            AgentResearchRunRecord,
            AgentResearchRunRecord.id == AgentDailyBudgetReservationRecord.run_id,
        )
        .where(AgentResearchRunRecord.pricing_version == PLATFORM_PRICE_VERSION)
        .execution_options(populate_existing=True)
    )
    for budget, run in records:
        if run.id in own:
            bound = manifest.reservation_for(own[run.id])
            if (
                budget.reserved_usd != bound
                or run.reserved_cost_usd != bound
                or budget.settled_usd is not None
                and budget.settled_usd > bound
                or budget.state == "reserved"
                and run.status in ("failed", "abstained", "completed")
            ):
                raise ValueError("PLATFORM_PRIOR_USAGE_UNCONFIRMED")
        elif budget.state == "reserved":
            held += await research_reserved_occupancy(session, budget)
        elif budget.state == "settled":
            if budget.settled_usd is None or budget.settled_usd != run.settled_cost_usd:
                raise ValueError("PLATFORM_PRIOR_USAGE_UNCONFIRMED")
            spent += budget.settled_usd
    check_platform_eligibility(
        funding.credential,
        funding.proof,
        purpose=manifest.purpose,
        now=now,
        required_usd=manifest.total_reservation_usd,
        local_held_usd=held,
        local_spent_since_proof_usd=spent,
        required_valid_until=required_valid_until or now + timedelta(seconds=3 * 330),
        experiment_id=manifest.experiment_id,
        input_hashes=manifest.input_hashes,
    )
