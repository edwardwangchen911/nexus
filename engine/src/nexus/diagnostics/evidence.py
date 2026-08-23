"""Point-in-time validation and exact ref selection for diagnostic inputs."""

from dataclasses import dataclass
from datetime import date, datetime

from nexus.diagnostics.models import EvidenceBinding
from nexus.domain.evidence import EvidenceRef


@dataclass(frozen=True)
class EvidenceResult:
    refs: tuple[EvidenceRef, ...] = ()
    reason: str | None = None


class EvidenceResolver:
    """Resolve typed evidence against one frozen snapshot instant."""

    def __init__(self, snapshot_as_of: datetime | None) -> None:
        self._snapshot_as_of = snapshot_as_of

    def resolve(
        self,
        binding: EvidenceBinding | None,
        reason_prefix: str,
        *,
        expected_as_of: datetime | None = None,
        expected_session: date | None = None,
    ) -> EvidenceResult:
        if binding is None or self._snapshot_as_of is None:
            return EvidenceResult(reason=f"{reason_prefix}_EVIDENCE_MISSING")
        snapshot_day = self._snapshot_as_of.date()
        if (
            binding.available_at > self._snapshot_as_of
            or binding.as_of > self._snapshot_as_of
            or (binding.session is not None and binding.session > snapshot_day)
            or any(ref.available_at > self._snapshot_as_of for ref in binding.refs)
        ):
            return EvidenceResult(reason=f"{reason_prefix}_EVIDENCE_FUTURE")
        if (
            binding.as_of > binding.available_at
            or any(ref.available_at > binding.available_at for ref in binding.refs)
            or (expected_as_of is not None and binding.as_of != expected_as_of)
            or (expected_session is not None and binding.session != expected_session)
        ):
            return EvidenceResult(reason=f"{reason_prefix}_EVIDENCE_INCONSISTENT")
        return EvidenceResult(refs=binding.refs)


def combine_refs(*groups: tuple[EvidenceRef, ...]) -> tuple[EvidenceRef, ...]:
    """Deduplicate immutable refs while retaining deterministic input order."""
    result: list[EvidenceRef] = []
    seen: set[tuple[str, str, datetime, str, str]] = set()
    for refs in groups:
        for ref in refs:
            key = (ref.source, ref.object_id, ref.available_at, ref.location, ref.content_hash)
            if key not in seen:
                seen.add(key)
                result.append(ref)
    return tuple(result)
