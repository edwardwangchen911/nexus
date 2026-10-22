"""Bounded pure preparation values for one foreground research request only."""

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field

from nexus.market.current_identity import identity_parse_scope

MAX_PROJECTIONS = 32
MAX_PROJECTION_BYTES = 8 * 1024 * 1024
MAX_ENCODINGS = 128


@dataclass(slots=True)
class PreparationReuse:
    active: bool = True
    projections: dict[tuple[bool, bool, bytes], dict[str, object]] = field(default_factory=dict)
    # Keep the actual encoder alive: recycled object IDs cannot reuse an old count.
    encodings: dict[tuple[str, int, bytes], tuple[object, int]] = field(default_factory=dict)
    projection_bytes: int = 0
    counts: dict[str, int] = field(
        default_factory=lambda: {
            "projection_hits": 0,
            "projection_misses": 0,
            "token_hits": 0,
            "token_misses": 0,
        }
    )


_REUSE: ContextVar[PreparationReuse | None] = ContextVar("research_preparation_reuse", default=None)


def current_reuse() -> PreparationReuse | None:
    state = _REUSE.get()
    return state if state is not None and state.active else None


@contextmanager
def research_preparation_scope() -> Iterator[PreparationReuse]:
    """Never retain an authority decision, committee, session, deadline or model output."""
    state = PreparationReuse()
    token = _REUSE.set(state)
    try:
        with identity_parse_scope():
            yield state
    finally:
        # Inherited task contexts may outlive cancellation; disable their shared state too.
        state.active = False
        state.projections.clear()
        state.encodings.clear()
        state.projection_bytes = 0
        _REUSE.reset(token)
