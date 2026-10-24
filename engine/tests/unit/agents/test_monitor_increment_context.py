"""Incremental purpose is explicit and cannot silently become ordinary research."""

from types import SimpleNamespace

from nexus.monitoring.context import PURPOSE, SOURCE, is_monitor_snapshot


def test_any_incremental_marker_requires_its_dedicated_authority():
    for field, value in [
        ("source", SOURCE),
        ("schema_version", PURPOSE),
        ("payload", {"purpose": PURPOSE}),
    ]:
        row = {"source": "unrelated", "schema_version": "other", "payload": {}}
        row[field] = value
        assert is_monitor_snapshot(SimpleNamespace(**row))
    assert not is_monitor_snapshot(
        SimpleNamespace(source="nexus_research", schema_version="research_filings_v1", payload={})
    )
    assert not is_monitor_snapshot(None)
