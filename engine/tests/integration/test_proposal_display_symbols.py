"""Proposal labels use identities known and effective at the saved valuation time."""

from datetime import UTC, date, datetime, timedelta
from uuid import UUID

import httpx
import pytest
from helpers.optimization_api import seed_readable_group
from helpers.portfolio_api import app, headers, seed_portfolio_list, settings
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.optimization import ProposalPositionRecord
from nexus.persistence.session import session_factory


@pytest.mark.asyncio
async def test_proposal_symbols_preserve_valuation_identity_and_unknowns(
    migrated_database_url: str,
) -> None:
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    cutoff = datetime(2026, 8, 24, 3, 59, tzinfo=UTC)
    ids = {
        name: UUID(int=90000 + index)
        for index, name in enumerate(
            (
                "known",
                "late_alias",
                "future_alias",
                "expired",
                "missing",
                "other_alias",
                "late_instrument",
                "at_cutoff",
                "microsecond_late",
                "starts_today",
                "ends_today",
            )
        )
    }
    async with factory() as session, session.begin():
        group_id, proposal_id = await seed_readable_group(session)
        for name, instrument_id in ids.items():
            session.add(
                Instrument(
                    id=instrument_id,
                    asset_type="equity",
                    exchange="XNYS",
                    currency="USD",
                    tick_size="0.01",
                    risk_bucket="us_equity",
                    created_at=cutoff + timedelta(days=1)
                    if name == "late_instrument"
                    else cutoff - timedelta(days=2),
                )
            )
        await session.flush()
        for index, (name, instrument_id) in enumerate(ids.items()):
            session.add(
                ProposalPositionRecord(
                    proposal_id=proposal_id,
                    instrument_id=instrument_id,
                    current_weight="0.01",
                    target_weight="0.01",
                    action="maintain",
                    reason_code="synthetic",
                )
            )
            if name == "missing":
                continue
            session.add(
                InstrumentAlias(
                    instrument_id=instrument_id,
                    symbol=f"SYNTH{chr(65 + index)}",
                    alias_type="provider" if name == "other_alias" else "ticker",
                    valid_from=date(2026, 8, 25)
                    if name == "future_alias"
                    else cutoff.date()
                    if name == "starts_today"
                    else date(2020, 1, 1),
                    valid_to=date(2026, 8, 23)
                    if name == "expired"
                    else cutoff.date()
                    if name == "ends_today"
                    else None,
                    created_at=cutoff + timedelta(days=1)
                    if name == "late_alias"
                    else cutoff + timedelta(microseconds=1)
                    if name == "microsecond_late"
                    else cutoff
                    if name == "at_cutoff"
                    else cutoff - timedelta(days=1),
                )
            )
    path = f"/internal/v1/optimization-proposals/{group_id}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url)), base_url="http://testserver"
    ) as client:
        first = await client.get(path, headers=headers("GET", path))
        assert first.status_code == 200
        data = first.json()["data"]
        proposal = next(row for row in data["proposals"] if row["proposal_id"] == str(proposal_id))
        positions = {row["instrument_id"]: row for row in proposal["positions"]}
        for index, (name, instrument_id) in enumerate(ids.items()):
            assert positions[str(instrument_id)]["display_symbol"] == (
                f"SYNTH{chr(65 + index)}"
                if name in {"known", "at_cutoff", "starts_today", "ends_today"}
                else None
            )
        async with factory() as session, session.begin():
            session.add(
                InstrumentAlias(
                    instrument_id=ids["missing"],
                    symbol="LATER",
                    alias_type="ticker",
                    valid_from=date(2020, 1, 1),
                    valid_to=None,
                    created_at=cutoff + timedelta(days=2),
                )
            )
        second = await client.get(path, headers=headers("GET", path))
        assert second.status_code == 200
        assert second.json()["data"] == data
    await factory.kw["bind"].dispose()
