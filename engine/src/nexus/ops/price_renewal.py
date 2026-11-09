"""Exact immutable price renewal identity; grants no recovery or dispatch authority."""

from uuid import UUID

from nexus.agents.platform_pricing import sol_price_record
from nexus.agents.platform_profile import sol_business_model_fields, sol_business_model_matches
from nexus.agents.platform_schema import CURRENT_PLATFORM_UNITS_VERSION
from nexus.persistence.models.agents import AgentModelConfigRecord

_INVALID = "AGENT_PAPER_EXIT_RECONCILIATION_PRICE_RENEWAL_INVALID"


def require_equivalent_price_renewal(
    old: AgentModelConfigRecord, new: AgentModelConfigRecord
) -> dict[str, str]:
    """Preserve historical identity; current target eligibility is checked separately.

    Reconstruct the target with the original price record to compare every
    supported capability, wire property, economic field and output contract.
    Only new verification material and its bounded window may differ.
    """
    if (
        not isinstance(old.id, UUID)
        or not isinstance(new.id, UUID)
        or old.id == new.id
        or not sol_business_model_matches(old)
        or not sol_business_model_matches(new)
    ):
        raise ValueError(_INVALID)
    assert old.platform_profile is not None and new.platform_profile is not None
    old_price = sol_price_record(old.platform_profile["price_record"])
    new_price = sol_price_record(new.platform_profile["price_record"])
    allowed = {"verified_at", "expires_at", "source_evidence_sha256"}
    # Compare the four prices as exact integer picodollars, preserving their
    # original spelling and hashes (for example 0.10 and 0.1 are the same fee).
    rates = {
        "ordinary_input_usd_per_million",
        "cache_read_usd_per_million",
        "cache_write_30m_usd_per_million",
        "output_usd_per_million",
    }
    old_terms = old_price.model_dump(mode="json")
    new_terms = new_price.model_dump(mode="json")
    if (
        {key: value for key, value in old_terms.items() if key not in allowed | rates}
        != {key: value for key, value in new_terms.items() if key not in allowed | rates}
        or old_price.token_picodollars != new_price.token_picodollars
        or new_price.verified_at <= old_price.verified_at
        or new_price.expires_at <= old_price.expires_at
    ):
        raise ValueError(_INVALID)
    units_version = new.platform_profile.get("output_units_version", CURRENT_PLATFORM_UNITS_VERSION)
    if not isinstance(units_version, str):
        raise ValueError(_INVALID)
    reconstructed = sol_business_model_fields(old_price, units_version=units_version)
    if (
        any(getattr(old, key, None) != value for key, value in reconstructed.items())
        or old.worst_case_cost_usd != new.worst_case_cost_usd
    ):
        raise ValueError(_INVALID)
    return {
        "old_model_id": str(old.id),
        "new_model_id": str(new.id),
        "old_model_hash": old.config_hash,
        "new_model_hash": new.config_hash,
        "old_price_hash": old_price.content_hash,
        "new_price_hash": new_price.content_hash,
    }
