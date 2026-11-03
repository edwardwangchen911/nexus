"""Only explicit registration may create an intraday authorization."""

import pytest
from nexus.ops.manual_paper import parser


def test_intraday_intent_has_a_separate_explicit_entry_and_readback():
    cli = parser()
    for action in ("register-formal-intent", "read-formal-intent"):
        parsed = cli.parse_args([action, "--idempotency-key", "one-authorization"])
        assert parsed.action == action
        assert parsed.idempotency_key == "one-authorization"
    for extra in (["--session", "tomorrow"], ["--account-id", "override"], ["--retry"]):
        with pytest.raises(SystemExit):
            cli.parse_args(["register-formal-intent", "--idempotency-key", "one", *extra])
