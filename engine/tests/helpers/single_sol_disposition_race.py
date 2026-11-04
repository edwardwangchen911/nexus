"""Force both commit orders using the original receipt writer and a new owner's ticket."""

import asyncio
import hashlib
import json

import pytest
from integration.test_sol_native_stage_pg import prepare_successor_stage
from nexus.agents.research_stages.protocol import ProtocolRejected, StagedProtocol
from sqlalchemy import text
from unit.agents.test_platform_usage import usage


async def verify_order(
    factory, settings, store, order, old_ticket, at, payload, tokens, monkeypatch
):
    _, member, _, claim, successor, now, compiled = await prepare_successor_stage(
        factory, settings, store
    )
    old = StagedProtocol(factory)
    entered, release = asyncio.Event(), asyncio.Event()
    pids = {}
    owner = successor if order == "grant_first" else old
    commit = owner._commit

    async def pause_commit(db, scope):
        pids["owner"] = await db.scalar(text("SELECT pg_backend_pid()"))
        entered.set()
        await asyncio.wait_for(release.wait(), 15)
        await commit(db, scope)

    monkeypatch.setattr(owner, "_commit", pause_commit)

    async def grant(stage):
        return await successor.authorize(
            claim, stage=stage, now=now, wire=compiled.wire, platform_wire=compiled
        )

    async def write_late():
        await old.observe(old_ticket, now=at, payload=payload, tokens=tokens)

    async with asyncio.timeout(25):
        if order == "grant_first":
            member_read = old._member
            waiting = asyncio.Event()

            async def note_writer(db, claim):
                pids["late"] = await db.scalar(text("SELECT pg_backend_pid()"))
                waiting.set()
                return await member_read(db, claim)

            monkeypatch.setattr(old, "_member", note_writer)
            async with asyncio.TaskGroup() as group:
                grant_task = group.create_task(grant(1))
                await entered.wait()
                group.create_task(write_late())
                try:
                    await waiting.wait()
                    async with factory() as observer:
                        while not await observer.scalar(
                            text("SELECT :owner = ANY(pg_blocking_pids(:late))"), pids
                        ):
                            await asyncio.sleep(0.01)
                finally:
                    release.set()
            ticket = grant_task.result()
            raw = json.dumps(
                dict(model="gpt-6.1-sol", service_tier="default", status="completed", usage=usage())
            ).encode()
            await successor.observe(ticket, now=now, payload=raw, tokens=(12000, 1000))
            assert await successor.verify(
                ticket,
                now=now,
                wire_hash=hashlib.sha256(compiled.wire).hexdigest(),
                payload=raw,
                tokens=(12000, 1000),
            )
            await successor.cleanup(ticket, now=now, confirmed=True)
        else:
            async with asyncio.TaskGroup() as group:
                group.create_task(write_late())
                try:
                    await entered.wait()
                    with pytest.raises(
                        ProtocolRejected, match="SINGLE_SOL_SUCCESSOR_HISTORY_BLOCKED"
                    ):
                        await grant(1)
                finally:
                    release.set()

        with pytest.raises(ProtocolRejected, match="SINGLE_SOL_SUCCESSOR_HISTORY_BLOCKED"):
            await grant(2 if order == "grant_first" else 1)
    async with factory() as observer:
        stages = (
            await observer.scalars(
                text(
                    "SELECT stage FROM research_stage_authorization "
                    "WHERE run_id=:run ORDER BY stage"
                ),
                {"run": member.run_id},
            )
        ).all()
        assert stages == ([1] if order == "grant_first" else [])
        if stages:
            assert (
                await observer.scalar(
                    text(
                        "SELECT confirmed FROM research_stage_cleanup WHERE run_id=:run AND stage=1"
                    ),
                    {"run": member.run_id},
                )
                is True
            )
