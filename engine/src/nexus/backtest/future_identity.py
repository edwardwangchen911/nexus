"""Read-only current identity bindings for a future, independently versioned universe."""

import json
from datetime import date, datetime
from hashlib import sha256
from typing import Any

from sqlalchemy import func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.backtest.universe import load_us_liquid_v1_definition, require_future_xnys_session
from nexus.market.current_identity import SOURCE
from nexus.market.current_identity_storage import IdentityStore, read_current_identity
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.market import Instrument, InstrumentAlias


def _hash(payload: object) -> str:
    return sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


async def prepare_future_identity_bindings(
    session: AsyncSession, store: IdentityStore, *, effective_session: date
) -> dict[str, Any]:
    await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
    cutoff = await session.scalar(select(func.clock_timestamp()))
    assert isinstance(cutoff, datetime)
    require_future_xnys_session(effective_session, cutoff)
    definition = load_us_liquid_v1_definition(effective_session)
    snapshots = await session.scalars(
        select(DataSnapshot)
        .where(
            DataSnapshot.source == SOURCE,
            DataSnapshot.created_at <= cutoff,
            DataSnapshot.available_at <= cutoff,
            DataSnapshot.received_at <= cutoff,
            DataSnapshot.observed_at <= cutoff,
        )
        .order_by(
            DataSnapshot.available_at.desc(), DataSnapshot.created_at.desc(), DataSnapshot.id.desc()
        )
    )
    latest: dict[str, DataSnapshot] = {}
    for observed in snapshots:
        latest.setdefault(observed.provider_object_id, observed)

    members: list[dict[str, Any]] = []
    for member in definition.members:
        binding: dict[str, Any] = {
            "symbol": member.symbol,
            "asset_type": member.asset_type,
            "risk_bucket": member.risk_bucket,
            "definition_instrument_id": str(member.instrument_id),
            "instrument_id": None,
            "proof_snapshot_id": None,
            "proof_hash": None,
            "alias_id": None,
            "binding_basis": "current_source_proof_and_unique_future_ticker",
            "gap": None,
        }
        snapshot = latest.get(f"identity:{member.symbol}")
        if member.asset_type != "common_stock":
            binding["gap"] = "IDENTITY_TYPE_UNSUPPORTED"
        elif snapshot is None:
            binding["gap"] = "IDENTITY_PROOF_MISSING"
        else:
            try:
                proof = await read_current_identity(session, store, snapshot_id=snapshot.id)
                instrument = await session.get(Instrument, proof.instrument_id)
                if (
                    proof.checked_at > cutoff
                    or proof.identity.symbol != member.symbol
                    or proof.definition_instrument_id != member.instrument_id
                    or instrument is None
                    or instrument.created_at > cutoff
                ):
                    raise ValueError("IDENTITY_PROOF_INVALID")
                future = tuple(
                    await session.scalars(
                        select(InstrumentAlias).where(
                            InstrumentAlias.alias_type == "ticker",
                            or_(
                                InstrumentAlias.symbol == member.symbol,
                                InstrumentAlias.instrument_id == proof.instrument_id,
                            ),
                            InstrumentAlias.created_at <= cutoff,
                            InstrumentAlias.valid_from <= effective_session,
                            or_(
                                InstrumentAlias.valid_to.is_(None),
                                InstrumentAlias.valid_to >= effective_session,
                            ),
                        )
                    )
                )
                if len(future) != 1 or future[0].id != proof.alias_id:
                    binding["gap"] = "FUTURE_TICKER_BINDING_INVALID"
                else:
                    binding.update(
                        instrument_id=str(proof.instrument_id),
                        proof_snapshot_id=str(snapshot.id),
                        proof_hash=_hash(proof.model_dump(mode="json")),
                        alias_id=str(proof.alias_id),
                    )
            except ValueError:
                binding["gap"] = "IDENTITY_PROOF_INVALID"
        members.append(binding)

    ready = all(member["gap"] is None for member in members)
    candidate = None
    if ready:
        candidate = {
            "key": "us_liquid",
            "version": "us_liquid_current_identity_v1",
            "source": "nexus_current_identity_bindings_v1",
            "source_definition_hash": definition.content_hash,
            "effective_session": effective_session.isoformat(),
            "checked_at": cutoff.isoformat(),
            "members": members,
            "historical_backtest_eligible": False,
            "execution_locked": True,
        }
        candidate["content_hash"] = _hash(candidate)
    result = {
        "schema_version": "future_universe_identity_preparation_v1",
        "status": "READY" if ready else "BLOCKED",
        "candidate_version": "us_liquid_current_identity_v1",
        "source_definition_hash": definition.content_hash,
        "effective_session": effective_session.isoformat(),
        "checked_at": cutoff.isoformat(),
        "members": members,
        "candidate": candidate,
        "historical_backtest_eligible": False,
        "execution_locked": True,
    }
    result["preparation_hash"] = _hash(result)
    return result
