"""Recognize immutable Agent Paper research scopes without widening legacy lanes."""

from nexus.persistence.models.market import UniverseSnapshot


def is_formal_agent_paper_universe(universe: UniverseSnapshot) -> bool:
    if (
        universe.key == "us_liquid"
        and universe.version == "us_liquid_v1"
        and universe.source == "fosun_local_read_only"
    ):
        return True
    digest = universe.content_hash
    return (
        universe.key == "agent_paper_research"
        and universe.source == "nexus_local_reviewed_research"
        and len(digest) == 64
        and all(character in "0123456789abcdef" for character in digest)
        and universe.version == f"reviewed_v1_{digest[:16]}"
    )
