import copy

from nexus.monitoring.daily_contracts import daily_context_identity
from nexus.monitoring.mixed_contracts import canonical_hash


def test_daily_view_keeps_exact_text_and_theses_once_and_binds_full_frozen_hash():
    from nexus.monitoring.daily_v2_view import daily_context_model_view

    parent = {"output_id": "out", "intent": {"stance": "watch"}, "role_outputs": ["audit"]}
    original = {
        "purpose": "daily_monitor_context_v2",
        "trigger_checks": [{"trigger_id": "00000000-0000-0000-0000-000000000001"}],
        "materials": [
            {
                "payload": {
                    "parents": [dict(parent, kind="price"), dict(parent, kind="disclosure")],
                    "contents": [
                        {
                            "archive": {
                                "documents": [
                                    {"text": "Exact complete filing text", "raw_hash": "a" * 64}
                                ],
                                "originals": [{"body_base64": "duplicated custody bytes"}],
                            }
                        }
                    ],
                }
            }
        ],
    }
    before = copy.deepcopy(original)
    digest = canonical_hash(original)
    view = daily_context_model_view(original, frozen_context_hash=digest)
    assert original == before
    assert len(view["parent_theses"]) == 1
    assert all(
        "intent" not in row and "role_outputs" not in row
        for row in view["materials"][0]["payload"]["parents"]
    )
    archive = view["materials"][0]["payload"]["contents"][0]["archive"]
    assert (
        archive["documents"]
        == original["materials"][0]["payload"]["contents"][0]["archive"]["documents"]
    )
    assert "originals" not in archive
    assert daily_context_identity(view)[0] == digest
