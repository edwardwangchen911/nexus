"""Closed prepare input and no-I/O CLI rejection boundaries."""

import json
import os
import subprocess
import sys
from uuid import uuid4

import pytest
from nexus.backtest.bundle import BundleRequest
from nexus.ops import backtest_once
from pydantic import ValidationError


def values():
    return {
        "strategy_version": "baseline_v1",
        "start": "2021-01-01",
        "end": "2021-01-08",
        "initial_nav": "10000.01",
        "idempotency_key": "synthetic",
        **{
            field: str(uuid4())
            for field in (
                "calendar_authority_id",
                "universe_collection_id",
                "sector_collection_id",
                "corporate_action_collection_id",
                "spy_instrument_alias_id",
                "equal_weight_universe_snapshot_id",
                "source_policy_id",
                "cost_policy_id",
            )
        },
    }


@pytest.mark.parametrize(
    "extra",
    [
        {"complete": True},
        {"source_selection_reports": []},
        {"content_hash": "a" * 64},
        {"as_of": "2020-01-01T00:00:00Z"},
        {"member_ids": []},
        {"initial_nav": 1.5},
        {"initial_nav": "NaN"},
        {"initial_nav": "0"},
        {"initial_nav": True},
        {"idempotency_key": "x" * 101},
        {"start": "2021-02-01"},
    ],
)
def test_prepare_request_cannot_invent_authority(extra):
    with pytest.raises(ValidationError):
        BundleRequest.model_validate({**values(), **extra})


@pytest.mark.parametrize("case", ["mixed", "oversized", "bad_json", "symlink"])
def test_prepare_bad_file_or_mixed_mode_never_loads_settings(case, tmp_path, monkeypatch, capsys):
    path = tmp_path / "private-canary.json"
    path.write_text(json.dumps(values()))
    args = ["backtest_once", "--prepare-request-file", str(path)]
    if case == "mixed":
        args.extend(["--as-of", "2020-01-01T00:00:00Z"])
    elif case == "oversized":
        path.write_bytes(b" " * (1024 * 1024 + 1))
    elif case == "bad_json":
        path.write_text("private-canary")
    elif case == "symlink":
        link = tmp_path / "link"
        link.symlink_to(path)
        args[-1] = str(link)

    def no_settings():
        pytest.fail("settings must not be loaded for invalid prepare requests")

    monkeypatch.setattr(backtest_once, "EngineSettings", no_settings)
    monkeypatch.setattr(sys, "argv", args)
    with pytest.raises(SystemExit, match="1"):
        backtest_once.main()
    captured = capsys.readouterr()
    assert captured.out == "" and "private-canary" not in captured.err
    assert json.loads(captured.err)["status"] == "failed"


def test_prepare_fifo_is_bounded_in_an_isolated_process(tmp_path):
    fifo = tmp_path / "fifo"
    os.mkfifo(fifo)
    process = subprocess.run(
        [sys.executable, "-m", "nexus.ops.backtest_once", "--prepare-request-file", str(fifo)],
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert process.returncode == 1 and process.stdout == ""
    assert json.loads(process.stderr)["status"] == "failed"


def test_input_payload_helper_preserves_legacy_hashes():
    from nexus.backtest.engine import _audited_data_hash, _authority_bundle_hash, run_backtest
    from nexus.backtest.persistence import (
        expected_data_snapshot_payload,
        input_data_snapshot_payload,
    )

    from engine.tests.integration.test_backtest_writer import _raw_inputs
    from engine.tests.unit.backtest.test_engine import _dataset

    strategy, bars, sessions, protocol = _raw_inputs()
    data = _dataset(bars=bars, sessions=sessions, universe_members=(bars[0].instrument_id,))
    result = run_backtest(strategy, data, protocol)
    assert input_data_snapshot_payload(
        data,
        authority_hash=_authority_bundle_hash(data),
        data_hash=_audited_data_hash(data, protocol),
    ) == expected_data_snapshot_payload(data, result)
