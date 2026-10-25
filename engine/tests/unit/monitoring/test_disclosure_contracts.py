"""SEC monitoring is a fixed discovery signal, never an impact classifier."""

import pytest
from nexus.monitoring import contracts
from pydantic import ValidationError


def condition():
    return {
        "signal": "sec.issuer_disclosure_added",
        "operator": "exists_new",
        "threshold": True,
        "unit": "filing",
        "purpose": "review",
        "source": "sec",
        "forms": ["8-K", "8-K/A"],
        "max_age_seconds": 300,
        "time_authority": "frozen_index_vs_local_receipt",
    }


def test_disclosure_is_a_separate_closed_condition():
    cls = getattr(contracts, "DisclosureReviewCondition", None)
    assert cls is not None, "reliable disclosure signal must have its own closed type"
    value = cls.model_validate(condition())
    assert len(value.semantic_hash) == 64
    assert value.model_dump(mode="json") == condition()
    for key, bad in (
        ("threshold", 1),
        ("forms", ["10-K"]),
        ("source", "model"),
        ("max_age_seconds", 600),
        ("time_authority", "accepted_at"),
    ):
        with pytest.raises(ValidationError):
            cls.model_validate({**condition(), key: bad})
    with pytest.raises(ValidationError):
        cls.model_validate({**condition(), "materiality": "important"})
