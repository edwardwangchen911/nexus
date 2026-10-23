"""Pure object selection, independent of admission and service health."""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal
from uuid import UUID

ActivitySelection = Literal[
    "active", "historical_unresolved", "today", "historical", "none", "ambiguous", "unavailable"
]


@dataclass(frozen=True)
class ActivityObject:
    identity: UUID
    at: datetime
    today: bool
    unresolved: bool


def choose_activity(
    objects: list[ActivityObject],
) -> tuple[ActivityObject | None, ActivitySelection]:
    active = [item for item in objects if item.unresolved]
    if len(active) > 1:
        return None, "ambiguous"
    if active:
        item = active[0]
        return item, "active" if item.today else "historical_unresolved"
    current = [item for item in objects if item.today]
    selected = max(current or objects, key=lambda item: (item.at, item.identity), default=None)
    return selected, "none" if selected is None else "today" if current else "historical"
