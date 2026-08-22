"""Minimal authenticated internal fixture routes."""

from datetime import UTC, datetime

from fastapi import APIRouter, Depends
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import require_service_actor

router = APIRouter()


@router.get("/internal/v1/system-status")
async def system_status(
    actor: VerifiedServiceActor = Depends(require_service_actor),
) -> dict[str, object]:
    """Return a fixture-only status envelope without exposing configuration."""
    return {
        "data": {"status": "ok"},
        "meta": {
            "request_id": str(actor.request_id),
            "schema_version": "1",
            "as_of": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        },
    }
