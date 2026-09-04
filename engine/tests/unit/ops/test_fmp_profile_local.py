"""FMP CLI rejects ambiguous input without reading credentials or printing values."""

import json
import sys

import pytest
from nexus.ops import fmp_profile_local as entry


@pytest.mark.parametrize(
    "args",
    [
        [],
        ["collect"],
        ["import"],
        ["collect", "--symbol", "AAPL,MSFT", "--output-dir", "private-canary"],
        [
            "collect",
            "--symbol",
            "AAPL",
            "--output-dir",
            "private-canary",
            "--apikey",
            "secret-canary",
        ],
        ["import", "--symbol", "AAPL", "--instrument-id", "private-canary"],
    ],
)
def test_bad_cli_input_never_loads_environment_or_exposes_values(args, monkeypatch, capsys):
    def no_environment(*args):
        pytest.fail("invalid request must not load credentials")

    monkeypatch.setattr(entry, "load_local_environment", no_environment)
    monkeypatch.setattr(sys, "argv", ["fmp-profile", *args])
    with pytest.raises(SystemExit, match="1"):
        entry.main()
    captured = capsys.readouterr()
    assert captured.out == "" and "canary" not in captured.err
    assert json.loads(captured.err) == {"status": "failed", "code": "FMP_PROFILE_FAILED"}


@pytest.mark.parametrize("action", [[], ["collect"], ["import"]])
def test_help_does_not_load_any_settings(action, monkeypatch, capsys):
    def no_environment(*args):
        pytest.fail("help must not load credentials")

    monkeypatch.setattr(entry, "load_local_environment", no_environment)
    monkeypatch.setattr(sys, "argv", ["fmp-profile", *action, "--help"])
    with pytest.raises(SystemExit) as stopped:
        entry.main()
    assert stopped.value.code == 0 and capsys.readouterr().err == ""
