"""Explicit source boundary for tests whose fixtures already froze qualified inputs."""

from nexus.agents.persistence import select_authoritative_snapshot
from nexus.ops import formal_research_preparation as preparation
from nexus.persistence.session import session_factory


def use_seeded_formal_preparation(monkeypatch, *, account_id, batch_id):
    async def prepared(settings, *, context, repo_root, process_environment):
        assert context.account_id == account_id
        assert context.reservation.candidate_batch_id == batch_id
        # Keep native execution / claim / batch / profile / calendar validation.
        scope = await preparation.read_preparation_scope(settings, context)
        factory = session_factory(settings)
        try:
            async with factory() as session:
                for candidate in scope.candidates:
                    assert (
                        await select_authoritative_snapshot(
                            session, instrument_id=candidate.instrument_id, as_of=scope.now
                        )
                        is not None
                    )
        finally:
            await factory.kw["bind"].dispose()

    monkeypatch.setattr(preparation, "prepare_formal_trigger_inputs", prepared)
