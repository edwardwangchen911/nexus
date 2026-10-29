"""Explicit historical identities and controlled native claim interruption."""

from datetime import datetime

import pytest


def use_historical_research_preparation(monkeypatch):
    """Build one archived batch; later fresh preparation keeps native selection."""
    from nexus.agents.research_stages import routes

    register = routes.register_new_prepared_runs

    async def historical(session, run_ids):
        # The old batch is selected at birth, never by deleting durable routes.
        # Restore immediately so next-round admission still tests current code.
        monkeypatch.setattr(routes, "register_new_prepared_runs", register)
        return None

    monkeypatch.setattr(routes, "register_new_prepared_runs", historical)


async def claim_without_dispatch(settings, monkeypatch, *, job_id, now):
    """Stop after the real Worker commits its guarded claim, before any SDK work."""
    from nexus.agents.research_stages import executor
    from nexus.worker import run_agent_worker_once

    class Claimed(BaseException):
        pass

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return now if tz is None else now.astimezone(tz)

    claims = []

    async def interrupted(**kwargs):
        claims.append(kwargs["claim"])
        raise Claimed

    with monkeypatch.context() as patch:
        patch.setattr("nexus.worker.datetime", Clock)
        patch.setattr("nexus.worker._ONCE_STARTED_AT", now)
        patch.setattr("nexus.worker._ONCE_WORKER_ID", f"controlled-claim-{job_id}")
        patch.setattr(executor, "execute_staged_claim", interrupted)
        with pytest.raises(Claimed):
            await run_agent_worker_once(settings, job_ids=(job_id,), formal_stage_capacity=True)
    assert len(claims) == 1
    assert claims[0].job_id == job_id and claims[0].attempt_no == 1
    return claims[0]
