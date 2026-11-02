"""Normal local Sol price/profile registration and historical inspection.

No provider/Key access, schedule mutation, research creation or budget release.
Source evidence is the operator's verified archived document, not a live invoice.
"""

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import make_url

from nexus.agents.platform_pricing import sol_price_record
from nexus.agents.platform_profile import (
    read_sol_business_profile,
    register_sol_business_profile,
)
from nexus.ops.openai_local import local_settings
from nexus.ops.platform_credentials import _private_bytes
from nexus.persistence.session import session_factory


def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("PLATFORM_PRICE_RECORD_INVALID")
        result[key] = value
    return result


async def profile_command(root: Path, args: argparse.Namespace) -> dict[str, object]:
    settings = local_settings(root, load_model_key=False)
    if settings.database_url is None:
        raise ValueError("PLATFORM_PROFILE_LOCAL_DATABASE_REQUIRED")
    url = make_url(settings.database_url)
    if url.host not in {"127.0.0.1", "localhost", "::1"} or set(url.query) & {
        "host",
        "hostaddr",
        "port",
        "dsn",
        "database",
        "dbname",
    }:
        raise ValueError("PLATFORM_PROFILE_LOCAL_DATABASE_REQUIRED")
    inspecting = args.action == "inspect-sol-profile"
    record, source = None, b""
    if not inspecting:
        record = sol_price_record(
            json.loads(_private_bytes(args.price_record, 16 * 1024), object_pairs_hook=_unique)
        )
        source = _private_bytes(args.source_evidence, 2 * 1024 * 1024)
    factory = session_factory(settings)
    receipt: dict[str, object] = {
        "inspection_only": inspecting,
        "dispatch_authority": False,
        "model_requests": 0,
        "provider_requests": 0,
        "execution_locked": True,
    }
    committed = False
    try:
        async with factory() as session:
            transaction = await session.begin()
            if inspecting:
                await session.execute(text("SET TRANSACTION READ ONLY"))
            await session.execute(text("SET LOCAL statement_timeout='5s'"))
            await session.execute(text("SET LOCAL lock_timeout='1s'"))
            if inspecting:
                model = await read_sol_business_profile(
                    session, model_config_id=args.model_config_id
                )
                receipt["database_writes"] = 0
            else:
                model = await register_sol_business_profile(
                    session, price_record=record, source_evidence=source
                )
                receipt["registration_requested"] = True
            receipt["status"] = "profile_not_registered" if model is None else "profile_registered"
            if model is not None:
                receipt.update(
                    model_config_id=str(model.id),
                    config_hash=model.config_hash,
                    version=model.version,
                    model=model.model_id,
                )
            try:
                await transaction.commit()
            except BaseException:
                if inspecting:
                    raise
                # Keep the prepared receipt so the only follow-up is exact-ID
                # readback. Do not renew, retry or report this as a rejection.
                receipt.update(
                    status="unknown", code="PLATFORM_PROFILE_REGISTRATION_RESULT_UNKNOWN"
                )
            else:
                committed = True
            return receipt
    except BaseException:
        if inspecting or not (committed or receipt.get("status") == "unknown"):
            raise
        receipt["cleanup_code"] = "PLATFORM_PROFILE_CONNECTION_CLEANUP_UNCONFIRMED"
        return receipt
    finally:
        try:
            async with asyncio.timeout(1):
                await factory.kw["bind"].dispose()
        except BaseException:
            if inspecting or not (committed or receipt.get("status") == "unknown"):
                raise
            receipt["cleanup_code"] = "PLATFORM_PROFILE_CONNECTION_CLEANUP_UNCONFIRMED"
