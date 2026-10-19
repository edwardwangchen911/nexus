"""Snapshot selection scans past high-frequency history without hiding conflicts."""

from datetime import date
from types import SimpleNamespace

import pytest
from nexus.agents.snapshots import _macro_context_ids
from nexus.research import fred_storage
from unit.agents.test_macro_context_selection import observation


@pytest.mark.asyncio
async def test_latest_context_scans_past_512_and_preserves_invalid_and_conflicting_ties(
    monkeypatch,
):
    daily = [observation(identity=i + 1) for i in range(513)]
    cpi = observation(identity=600, series="CPIAUCSL", observed_on=date(2026, 7, 1))
    unemployment = observation(identity=601, series="UNRATE", observed_on=date(2026, 7, 1))
    conflict = observation(identity=602, series="DGS10", value=daily[0].value + 1)
    invalid = observation(identity=603, series="UNRATE", observed_on=date(2026, 8, 1))
    values = [*daily, cpi, unemployment, conflict, invalid]
    closed = []

    class Rows:
        def __aiter__(self):
            async def iterate():
                for value in values:
                    yield value, SimpleNamespace(id=value.evidence_ref_id), None

            return iterate()

        async def close(self):
            closed.append(True)

    class Session:
        async def stream(self, query):
            assert query._limit_clause is None
            assert query.get_execution_options()["yield_per"] == 512
            return Rows()

    async def read(_session, row, *_args):
        if row == invalid:
            raise ValueError("invalid original")
        return row

    monkeypatch.setattr(fred_storage, "_read_one", read)
    result = await fred_storage.read_fred_observations(
        Session(), as_of=cpi.available_at, latest_per_series=True
    )
    assert closed == [True]
    assert {row.series for row in result.values()} == {"DGS10", "CPIAUCSL", "UNRATE"}
    assert invalid.evidence_ref_id not in result
    assert conflict.evidence_ref_id in result  # Never truncate a conflicting latest tie.
    assert _macro_context_ids(result) == {
        cpi.evidence_ref_id,
        unemployment.evidence_ref_id,
        cpi.calendar_evidence_ref_id,
    }


@pytest.mark.asyncio
async def test_latest_series_replaces_older_group_and_never_downgrades(monkeypatch):
    # Normal entry wiring is covered in PG; this file also covers streaming rank replacement.
    from dataclasses import replace

    old = observation()
    latest = replace(old, evidence_ref_id=observation(2).evidence_ref_id, vintage=date(2026, 8, 30))

    class Session:
        async def stream(self, _query):
            class Rows:
                def __aiter__(self):
                    async def rows():
                        for item in (old, latest, old):
                            yield item, None, None

                    return rows()

                async def close(self):
                    pass

            return Rows()

    async def read(_session, row, *_args):
        return row

    monkeypatch.setattr(fred_storage, "_read_one", read)
    result = await fred_storage.read_fred_observations(
        Session(), as_of=old.available_at, latest_per_series=True
    )
    assert result == {latest.evidence_ref_id: latest}
