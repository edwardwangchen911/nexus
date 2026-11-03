from nexus.api.monitoring_applicability import read_applicability


def test_frozen_research_only_version_is_distinct_from_unknown_manifests():
    legacy = (
        "research-only-v2-" + "54e9774671d390ff39a82ac6ecbbabb55cedf305b4e2d9bcdc5aee4be1b50bc9"
    )
    assert read_applicability({"portfolio": {"version": legacy}}).price == "not_applicable"
    assert read_applicability({"portfolio": {"version": legacy}}).disclosure == "not_applicable"
    for version in [None, [], "future", "research-only-v2-unrecognized"]:
        assert read_applicability({"portfolio": {"version": version}}).price == "unknown"


def test_price_and_event_monitoring_are_independently_versioned():
    view = read_applicability(
        {"portfolio": {"version": "local-portfolio-synthesis-v8-price-monitor"}}
    )
    assert (view.price, view.disclosure) == ("applicable", "not_applicable")
