from uuid import uuid4

import pytest
from nexus.ops.research_budget_case import (
    PUBLIC_TABLES,
    identities,
    public_ids,
    require_public_readback,
)


def test_formal_context_and_unknown_sources_are_not_exported() -> None:
    public, private, unknown = uuid4(), uuid4(), uuid4()
    assert public_ids(
        [
            {"id": str(public), "source": "sec"},
            {"id": str(private), "source": "nexus_agent_paper_context"},
            {"id": str(unknown), "source": "broker_account"},
        ]
    ) == (public,)
    assert not set(PUBLIC_TABLES) & {
        "accounts",
        "portfolios",
        "paper_accounts",
        "agent_paper_cycles",
        "paper_orders",
        "account_external_id_envelopes",
        "broker_account_bindings",
        "agent_research_runs",
    }


def test_nested_artifact_identity_extraction_does_not_return_contents() -> None:
    artifact = uuid4()
    raw = "unrelated public original text"
    ids, hashes = identities(
        {"captures": [{"artifact_id": str(artifact), "content_hash": "a" * 64, "raw_text": raw}]}
    )
    assert ids == {artifact}
    assert hashes == {"a" * 64}


def test_macro_calendar_must_be_preserved_in_exact_readback() -> None:
    economic, calendar = str(uuid4()), str(uuid4())
    refs = [{"id": economic, "source": "fred"}, {"id": calendar, "source": "exchange_calendars"}]
    require_public_readback({economic, calendar}, refs)
    with pytest.raises(ValueError, match="DIAGNOSTIC_PUBLIC_SOURCE_READBACK_INCOMPLETE"):
        require_public_readback({economic}, refs)
