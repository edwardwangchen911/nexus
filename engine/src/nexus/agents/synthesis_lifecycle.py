"""Immutable Job-version budgets; old synthesis facts never acquire new time."""

from dataclasses import dataclass

LEGACY_SYNTHESIS_JOB = "portfolio_synthesis_job_v1"
BOUNDED_SYNTHESIS_JOB = "portfolio_synthesis_job_v2"
SYNTHESIS_JOB_VERSIONS = frozenset({LEGACY_SYNTHESIS_JOB, BOUNDED_SYNTHESIS_JOB})
SYNTHESIS_WORK_SECONDS = 83
SYNTHESIS_CLEANUP_SECONDS = 7
SYNTHESIS_HANDLER_SECONDS = 105
SYNTHESIS_LEASE_SECONDS = 120


@dataclass(frozen=True)
class SynthesisDeadline:
    started_at: float

    @property
    def work_at(self) -> float:
        return self.started_at + SYNTHESIS_WORK_SECONDS

    @property
    def cleanup_at(self) -> float:
        return self.work_at + SYNTHESIS_CLEANUP_SECONDS

    @property
    def handler_at(self) -> float:
        return self.started_at + SYNTHESIS_HANDLER_SECONDS
