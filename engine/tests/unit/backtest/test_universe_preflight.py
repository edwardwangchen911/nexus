"""The internal universe candidate is fixed, deterministic, and forward-only."""

import csv
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from nexus.backtest.universe import (
    UniversePreflightError,
    load_us_liquid_v1_definition,
    require_future_xnys_session,
)

SEED = Path(__file__).resolve().parents[3] / "seed/universe/us_liquid_v1.csv"


def _rows() -> tuple[list[str], list[dict[str, str]]]:
    with SEED.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames is not None
        return reader.fieldnames, list(reader)


def _write(path: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def test_fixed_seed_has_stable_candidate_hash_independent_of_row_order(tmp_path: Path) -> None:
    session = date(2026, 9, 8)
    expected = load_us_liquid_v1_definition(session)
    fields, rows = _rows()
    reordered = tmp_path / "universe.csv"
    _write(reordered, fields, list(reversed(rows)))

    actual = load_us_liquid_v1_definition(session, seed_path=reordered)

    assert len(actual.members) == 49
    assert actual.key == "us_liquid"
    assert actual.version == "us_liquid_v1"
    assert actual.source == "nexus_internal_us_liquid_v1"
    assert actual.content_hash == expected.content_hash
    assert actual.content_hash == "4a9a292df983ccabbe1f7bf668427a49cf81d7a527818f8400fc90d9bc966726"
    assert tuple(member.instrument_id for member in actual.members) == tuple(
        sorted((member.instrument_id for member in actual.members), key=str)
    )


@pytest.mark.parametrize("problem", ["header", "missing", "duplicate_symbol", "duplicate_id", "id"])
def test_fixed_seed_rejects_structural_or_identity_changes(problem: str, tmp_path: Path) -> None:
    fields, rows = _rows()
    if problem == "header":
        for row in rows:
            row["unexpected"] = row.pop("source")
        fields[-1] = "unexpected"
    elif problem == "missing":
        rows.pop()
    elif problem == "duplicate_symbol":
        rows[1]["symbol"] = rows[0]["symbol"]
    elif problem == "duplicate_id":
        rows[1]["instrument_id"] = rows[0]["instrument_id"]
    else:
        rows[0]["instrument_id"] = "00000000-0000-0000-0000-000000000001"
    path = tmp_path / "invalid.csv"
    _write(path, fields, rows)

    with pytest.raises(UniversePreflightError, match="BACKTEST_UNIVERSE_DEFINITION_INVALID"):
        load_us_liquid_v1_definition(date(2026, 9, 8), seed_path=path)


@pytest.mark.parametrize(
    "session,cutoff,code",
    [
        (
            date(2026, 9, 5),
            datetime(2026, 9, 4, 0, tzinfo=UTC),
            "BACKTEST_UNIVERSE_SESSION_INVALID",
        ),
        (
            date(2026, 9, 4),
            datetime(2026, 9, 4, 15, tzinfo=UTC),
            "BACKTEST_UNIVERSE_SESSION_NOT_FUTURE",
        ),
    ],
)
def test_effective_session_must_be_a_strictly_future_xnys_session(
    session: date, cutoff: datetime, code: str
) -> None:
    with pytest.raises(UniversePreflightError, match=code):
        require_future_xnys_session(session, cutoff)


def test_future_xnys_session_returns_its_market_open() -> None:
    market_open = require_future_xnys_session(date(2026, 9, 8), datetime(2026, 9, 5, 0, tzinfo=UTC))
    assert market_open == datetime(2026, 9, 8, 13, 30, tzinfo=UTC)
