"""Normal monitoring entry uses protected configured scope and database time."""

import pytest
from nexus.ops.research_monitor import parser, run
from nexus.settings import EngineSettings


def test_install_requires_exact_output_and_does_not_accept_clock_or_account_override():
    for args in (
        ["install"],
        ["install", "--output-id", "00000000-0000-0000-0000-000000000001", "--now", "2026-10-09"],
        ["install", "--output-id", "00000000-0000-0000-0000-000000000001", "--account-id", "x"],
    ):
        with pytest.raises(SystemExit):
            parser().parse_args(args)


@pytest.mark.asyncio
async def test_unconfigured_scope_rejected_before_connection(monkeypatch):
    from nexus.ops import research_monitor

    def forbidden(*_args, **_kwargs):
        raise AssertionError("unconfigured monitor reached database")

    monkeypatch.setattr(research_monitor, "session_factory", forbidden)
    args = parser().parse_args(["install", "--output-id", "00000000-0000-0000-0000-000000000001"])
    with pytest.raises(ValueError, match="MONITOR_ACCOUNT_UNCONFIGURED"):
        await run(
            EngineSettings(database_url="postgresql+asyncpg://localhost/nexus_test_monitor"), args
        )
