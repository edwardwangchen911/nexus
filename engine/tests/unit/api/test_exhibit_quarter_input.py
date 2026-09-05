"""The input DTO must retain the exact optional frozen exhibit projection."""

from nexus.api.research_inputs import _exhibit_input
from unit.research.test_sec_exhibit_quarter_metrics import exhibit_html
from unit.research.test_sec_exhibits import DIRECTORY, archive, build, exhibit_archives


def test_exhibit_input_preserves_frozen_quarter_projection():
    archives = exhibit_archives()
    archives[2] = archive(exhibit_html(), DIRECTORY + "ex99.htm")
    document = build(archives, include_quarter_metrics=True)
    result = _exhibit_input(document)
    assert result is not None
    assert result.model_dump(mode="json")["quarter_metrics"] == (
        document.quarter_metrics.model_dump(mode="json")
    )


def test_legacy_input_does_not_invent_quarter_projection():
    result = _exhibit_input(build())
    assert result is not None
    assert result.model_dump().get("quarter_metrics") is None
