"""Pure proposal lifecycle transitions; no transition creates execution artifacts."""

from nexus.optimizer.models import OptimizationProposalGroup, ProposalGroupStatus


class ProposalLifecycleError(ValueError):
    """Stable lifecycle failure whose message is safe for API error mapping."""


_ALLOWED: dict[str, set[str]] = {
    "draft": {"compared", "rejected", "expired"},
    "compared": {"shadowing", "rejected", "expired"},
    "shadowing": {"validated", "rejected", "expired"},
    "validated": set(),
    "rejected": set(),
    "expired": set(),
}


def transition_proposal_group(
    group: OptimizationProposalGroup,
    target: ProposalGroupStatus,
    expected_version: int,
) -> OptimizationProposalGroup:
    """Return the next immutable lifecycle projection after deterministic eligibility checks."""
    if group.version != expected_version:
        raise ProposalLifecycleError("VERSION_CONFLICT")
    if target not in _ALLOWED[group.status]:
        raise ProposalLifecycleError("VALIDATION_ERROR")
    if target == "shadowing" and not _eligible_for_shadowing(group):
        raise ProposalLifecycleError("PROPOSAL_NOT_EXECUTABLE")
    return group.model_copy(update={"status": target, "version": group.version + 1})


def _eligible_for_shadowing(group: OptimizationProposalGroup) -> bool:
    strategy = group.proposals[2]
    return (
        strategy.strategy_evidence_complete
        and not strategy.analysis_only
        and group.shadow_authority is not None
        and group.shadow_authority.executable
    )
