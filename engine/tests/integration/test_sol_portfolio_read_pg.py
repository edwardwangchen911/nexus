"""Historical portfolio reads use the same frozen three research sources as admission."""

import json
import os
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
from helpers.monitoring_readview_browser import verify_monitoring_browser
from helpers.paper_api import app, headers
from integration.test_sol_native_stage_pg import (
    test_normal_sol_formal_research_reconciles_monitor_plans_once as normal_research,
)
from integration.test_sol_portfolio_responses_pg import _run_native_portfolio_http
from sqlalchemy import func, select


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["complete", "timeout"])
async def test_sol_portfolio_read(migrated_database_url, monkeypatch, tmp_path, case):
    from nexus.agents.research_stages import proof
    from nexus.paper.management_read_authority import load_synthesis_read_authority
    from nexus.persistence.models.paper import AgentPaperCycleRecord

    async def read(factory, host, portfolio, job, at, patch, scenario, root, env_file):
        await _run_native_portfolio_http(
            factory, host, portfolio, job, at, patch, scenario, root, env_file
        )
        async with factory() as session:
            cycle = await session.get(AgentPaperCycleRecord, portfolio.cycle_id)
            read_at = await session.scalar(select(func.clock_timestamp()))
            later = read_at + timedelta(days=10)
            authority = await load_synthesis_read_authority(session, cycle=cycle, read_as_of=later)
            assert authority.run.id == portfolio.id
            assert (authority.intent is not None) == (case == "complete")

            async def missing_source(*args, **kwargs):
                return None

            with patch.context() as fault:
                fault.setattr(proof, "read_terminal_proof", missing_source)
                with pytest.raises(ValueError, match="PORTFOLIO_SYNTHESIS_AUTHORITY_CONFLICT"):
                    await load_synthesis_read_authority(session, cycle=cycle, read_as_of=later)

            if case == "complete":
                from nexus.agents import platform_portfolio

                read_receipt = platform_portfolio.read_receipt

                async def missing_cleanup(session, job_id, phase, *, as_of=None):
                    if phase == "cleanup":
                        return None
                    return await read_receipt(session, job_id, phase, as_of=as_of)

                with patch.context() as fault:
                    fault.setattr(platform_portfolio, "read_receipt", missing_cleanup)
                    with pytest.raises(ValueError, match="PLATFORM_USAGE_PROOF_REQUIRED"):
                        await load_synthesis_read_authority(session, cycle=cycle, read_as_of=later)
            else:
                from nexus.paper.synthesis_usage_acceptance import _collect_proof

                receipt = await _collect_proof(
                    session, cycle_id=cycle.id, run_id=portfolio.id, now=later
                )
                assert receipt["budget_preserved"] and receipt["usage"] == "unknown"

        # The isolated staged Worker uses a controlled natural slot. Exact-run and
        # management views must observe that same instant, including monitoring receipts.
        patch.setattr("nexus.api.routes.research.utc_now", lambda: read_at)
        application = app(migrated_database_url, now=read_at)
        path = f"/internal/v1/paper/management/cycles/{cycle.id}"
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application), base_url="http://testserver"
        ) as client:
            response = await client.get(path, headers=headers("GET", path))
            assert response.status_code == 200, response.text
            detail = response.json()["data"]
            assert detail["orders"] == [] and detail["cycle"]["execution_locked"]
            assert detail["proposal"] is None and detail["activation_result"] is None
            assert len(detail["security_research"]) == 3
            usage = detail["synthesis"]["responses_usage"]
            assert usage["requested_model"] == "gpt-6.1-sol"
            if case == "complete":
                assert usage["reported_model"] == "gpt-6.1-sol" and usage["failure_kind"] is None
                assert Decimal(usage["settled_cost_usd"]) == Decimal("0.0299")
                assert detail["synthesis_usage_disposition"] is None
                assert all(c["action"] == "defer" for c in detail["synthesis"]["choices"])
            else:
                assert usage["reported_model"] is None and usage["failure_kind"] == "timeout"
                assert usage["failure_phase"] == "send"
                assert usage["settled_cost_usd"] is None
                assert detail["synthesis_usage_disposition"]["status"] == "not_accepted"
            stocks = []
            for source in detail["security_research"]:
                stock_path = f"/internal/v1/research-runs/{source['research_run_id']}"
                response = await client.get(stock_path, headers=headers("GET", stock_path))
                assert response.status_code == 200, response.text
                stock = response.json()["data"]
                assert Decimal(stock["settled_cost_usd"]) == Decimal("0.0897")
                assert stock["billing_mode"] == "platform_responses"
                assert stock["quality_status"] == source["quality_status"]
                assert stock["stage_trace"]["status"] == "verified"
                stocks.append(stock)
            reasons = [stock["monitoring"]["coverage_reason"] for stock in stocks]
            assert reasons.count("audit_rejected") == 1
            assert sum(stock["monitoring"]["plan_id"] is not None for stock in stocks) == 2
        if directory := os.environ.get("NEXUS_SOL_READ_BROWSER_DIR"):
            evidence = Path(directory).resolve() / case
            evidence.mkdir(mode=0o700, parents=True, exist_ok=True)
            (evidence / "ready.json").write_text(json.dumps({"cycle": detail, "stocks": stocks}))
            await verify_monitoring_browser(
                application, directory=evidence, spec_path="tests/e2e/sol-portfolio-live.spec.ts"
            )

    await normal_research(migrated_database_url, monkeypatch, tmp_path, case, read)
