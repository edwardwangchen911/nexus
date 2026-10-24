"""Research view removes duplicate custody bytes, never document text or judgments."""

from copy import deepcopy
from typing import Any


def daily_context_model_view(value: dict[str, Any], *, frozen_context_hash: str) -> dict[str, Any]:
    result = deepcopy(value)
    theses: dict[str, dict[str, Any]] = {}
    for material in result["materials"]:
        payload = material["payload"]
        parents = []
        for parent in payload["parents"]:
            identity = parent["output_id"]
            thesis = {
                key: part
                for key, part in parent.items()
                if key not in {"kind", "plan_id", "generation"}
            }
            if identity in theses and theses[identity] != thesis:
                raise ValueError("DAILY_EVENT_PARENT_CONFLICT")
            theses[identity] = thesis
            parents.append(
                {key: part for key, part in parent.items() if key not in {"intent", "role_outputs"}}
            )
        payload["parents"] = parents
        for content in payload["contents"]:
            content["archive"].pop("originals", None)
    result["parent_theses"] = list(theses.values())
    result["frozen_context_hash"] = frozen_context_hash
    return result
