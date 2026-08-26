"""Closed exact event-type/schema publisher registry."""

from __future__ import annotations

from collections.abc import Iterable

from nexus.outbox.models import PublisherSpec


class PublisherRegistry:
    def __init__(self, specs: Iterable[PublisherSpec] = ()) -> None:
        self._specs: dict[tuple[str, str], PublisherSpec] = {}
        for spec in specs:
            self.register(spec)

    def register(self, spec: PublisherSpec | None) -> None:
        if spec is None:
            raise ValueError("publisher spec is required")
        key = (spec.event_type, spec.schema_version)
        if key in self._specs:
            raise ValueError(f"duplicate publisher: {key[0]}@{key[1]}")
        self._specs[key] = spec

    def resolve(self, event_type: str, schema_version: str) -> PublisherSpec | None:
        return self._specs.get((event_type, schema_version))

    @property
    def keys(self) -> frozenset[tuple[str, str]]:
        return frozenset(self._specs)
