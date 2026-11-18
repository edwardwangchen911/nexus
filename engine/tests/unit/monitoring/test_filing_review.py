"""Review material observes references, never asserts content qualification."""

import pytest
from nexus.monitoring.filing_review import observe_html_dependencies


def test_dependency_observation_keeps_images_scripts_schema_and_unsupported_paths():
    raw = b"""<html><head><base href="https://other.invalid/">
    <style>.x{background:url(chart.png)} @import "other.css";</style></head>
    <body><img src="logo.jpg" srcset="chart.jpg 2x" onload="x()">
    <link:schemaref xlink:href="report.xsd"><object data="facts.pdf"></object>
    <script src="/delivery"></script><svg><image href="graph.svg"></image></svg>
    <div style="background:image-set(url(other.jpg) 1x)">Fiscal results</div></body></html>"""
    result = observe_html_dependencies(raw)
    assert result["status"] == "observed_not_qualified"
    assert result["research_qualified"] is False
    resources = result["references"]
    assert any(r["attributes"].get("srcset") == "chart.jpg 2x" for r in resources)
    assert any(r["attributes"].get("xlink:href") == "report.xsd" for r in resources)
    assert any(r["attributes"].get("href") == "https://other.invalid/" for r in resources)
    assert any(r["tag"] == "object" and r["attributes"]["data"] == "facts.pdf" for r in resources)
    assert result["css_observations"]["url_functions"] == 2
    assert result["css_observations"]["imports"] == 1
    assert result["css_observations"]["image_sets"] == 1
    assert result["event_attribute_count"] == 1


def test_plain_document_does_not_turn_absent_observed_resources_into_authority():
    result = observe_html_dependencies(b"<p>Complete fiscal table, as disclosed.</p>")
    assert result["references"] == []
    assert result["status"] == "observed_not_qualified"
    assert result["research_qualified"] is False


def test_duplicate_reference_attributes_keep_every_observed_value():
    result = observe_html_dependencies(b'<img src="first.jpg" src="other.jpg">')
    image = result["references"][0]
    assert image["duplicate_attributes"] == ["src"]
    assert image["attribute_occurrences"] == [["src", "first.jpg"], ["src", "other.jpg"]]


@pytest.mark.parametrize("raw", [b"\xff", b"x" * (8 * 1024 * 1024 + 1)])
def test_invalid_or_oversized_material_is_rejected(raw):
    with pytest.raises(ValueError, match="SEC_MONITOR_REVIEW_MATERIAL_INVALID"):
        observe_html_dependencies(raw)
