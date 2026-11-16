"""Pending-read failures remain fail closed while preserving bounded diagnostics."""

import asyncio
import json
from datetime import UTC, datetime

import pytest
from nexus.ops.agent_paper_loop import _emit_agent_paper_unknown_failure
from nexus.ops.manual_paper_host import _mark_pending_failure
from nexus.ops.pending_probe import ProbeTrace, trace


@pytest.mark.parametrize(
    ("error", "kind", "code"),
    [
        (ConnectionRefusedError("private-host"), "connection_refused", None),
        (NameError("private-variable"), "name_error", None),
        (ValueError("MANUAL_RESULT_UNKNOWN"), "validation", "MANUAL_RESULT_UNKNOWN"),
        (ValueError("private-payload"), "validation", None),
    ],
)
def test_pending_failure_keeps_first_read_stage_without_exception_text(capsys, error, kind, code):
    token = trace.set(ProbeTrace(stage="native_claims"))
    try:
        _mark_pending_failure(error, "manual_requests")
        _mark_pending_failure(error, "session_close")
    finally:
        trace.reset(token)
    _emit_agent_paper_unknown_failure(
        "inspect",
        datetime(2026, 11, 16, tzinfo=UTC),
        error=error,
        inspection_site="pending_work",
    )
    captured = capsys.readouterr().err
    data = json.loads(captured)
    assert data["pending_work_phase"] == "manual_requests"
    assert data["pending_read_stage"] == "native_claims"
    assert data["pending_work_kind"] == kind
    assert data.get("pending_work_code") == code
    assert "private" not in captured


def test_cancelled_probe_diagnostics_do_not_replace_timer_identity():
    error = asyncio.CancelledError("private-cancel")
    _mark_pending_failure(error, "manual_requests")
    assert getattr(error, "_nexus_pending_work_phase") == "manual_requests"


def test_untrusted_pending_diagnostics_are_not_emitted(capsys):
    error = RuntimeError("private-message")
    error._nexus_pending_work_phase = "manual_requests"
    error._nexus_pending_work_kind = "private-kind"
    error._nexus_pending_read_stage = "private-stage"
    error._nexus_pending_work_code = "private-code"
    _emit_agent_paper_unknown_failure(
        "inspect",
        datetime(2026, 11, 16, tzinfo=UTC),
        error=error,
        inspection_site="pending_work",
    )
    data = json.loads(capsys.readouterr().err)
    assert not {"pending_work_kind", "pending_work_code", "pending_read_stage"} & data.keys()
