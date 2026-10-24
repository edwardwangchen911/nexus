"""Synthetic per-event SDK auxiliary output for normal mixed-v2 staged fixtures."""


def add_mixed_fields(body, payload):
    """Mutate a normal stage response; payload must already be decompressed."""
    context = next(
        row["monitor_context"]
        for row in payload["frozen_inputs"]["security_snapshot"]["items"]
        if row["source"] == "nexus_monitor_increment_v2"
    )
    citation = next(
        row["index"] for row in payload["evidence"] if row["source"] == "nexus_monitor_increment_v2"
    )
    if payload["stage"] == "research":
        content_ids = {
            (
                f"sec:{row['archive']['issuer_cik']}:{row['archive']['entry']['accession_number']}"
            ): row["snapshot_id"]
            for row in context["materials"]["contents"]
        }
        for row in body["roles"]:
            row["mixed_event_proposal"] = None
            if row["role"] == "market_state":
                row["mixed_event_proposal"] = {
                    "context_hash": payload["mixed_event_context_hash"],
                    "items": [
                        {
                            "trigger_id": item["trigger_id"],
                            "kind": item["kind"],
                            "event_key": item["event_key"],
                            "status": "addressed",
                            "rationale": "已复核冻结原件及原论点，本项判断仅限当前单股研究。",
                            "content_snapshot_id": content_ids[item["event_key"]]
                            if item["kind"] == "disclosure"
                            else None,
                            "citations": [citation],
                        }
                        for item in context["trigger_checks"]
                    ],
                }
    elif payload["stage"] == "critique":
        for row in body["roles"]:
            row["mixed_event_review"] = {
                "proposal_hash": payload["mixed_event_proposal"]["content_hash"],
                "decision": "accepted",
                "rationale": "逐项核验原始触发、正文绑定和投资影响判断。",
                "citations": [citation],
            }
    else:
        body["mixed_event_adoption_hash"] = payload["mixed_event_proposal"]["content_hash"]
    return body
