"""Runtime release identity stays canonical and visible without exposing configuration."""

import pytest
from nexus.ops import agent_paper_loop as loop
from nexus.settings import EngineSettings
from pydantic import ValidationError

_REVISION = "a" * 40


def test_engine_settings_accepts_only_canonical_release_revisions() -> None:
    settings = EngineSettings.model_validate({"NEXUS_RELEASE_REVISION": _REVISION})

    assert settings.release_revision == _REVISION
    with pytest.raises(ValidationError):
        EngineSettings.model_validate({"NEXUS_RELEASE_REVISION": "A" * 40})
    with pytest.raises(ValidationError):
        EngineSettings.model_validate({"NEXUS_RELEASE_REVISION": "abc123"})


def test_agent_paper_process_version_carries_the_release_revision() -> None:
    assert loop.agent_paper_process_version(_REVISION) == (
        f"{loop.AGENT_PAPER_LOOP_PROCESS_VERSION}+{_REVISION}"
    )
    assert loop.agent_paper_process_version(None) == loop.AGENT_PAPER_LOOP_PROCESS_VERSION


def test_agent_paper_process_version_parser_preserves_legacy_unknown() -> None:
    assert loop.parse_agent_paper_process_version(loop.AGENT_PAPER_LOOP_PROCESS_VERSION) == (
        loop.AGENT_PAPER_LOOP_PROCESS_VERSION,
        None,
    )
    assert loop.parse_agent_paper_process_version(
        f"{loop.AGENT_PAPER_LOOP_PROCESS_VERSION}+{_REVISION}"
    ) == (loop.AGENT_PAPER_LOOP_PROCESS_VERSION, _REVISION)
    assert loop.parse_agent_paper_process_version("agent-paper-loop-v2+invalid") == (None, None)
