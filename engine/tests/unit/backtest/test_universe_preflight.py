"""The internal universe candidate is fixed, deterministic, and forward-only."""

import csv
import os
import subprocess
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zipfile import ZipFile

import nexus.backtest.universe as universe
import pytest
from nexus.backtest.universe import (
    UniversePreflightError,
    load_us_liquid_v1_definition,
    require_future_xnys_session,
)

SEED = Path(universe.__file__).resolve().parent / "data/us_liquid_v1.csv"


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


def test_fixed_seed_is_owned_by_the_importable_nexus_package() -> None:
    packaged_seed = Path(universe.__file__).resolve().parent / "data/us_liquid_v1.csv"

    assert packaged_seed.is_file()
    assert load_us_liquid_v1_definition(date(2026, 9, 8), seed_path=packaged_seed).members


def test_fixed_seed_is_loadable_from_a_built_wheel_without_the_source_tree(
    tmp_path: Path,
) -> None:
    engine_root = Path(__file__).resolve().parents[3]
    dist = tmp_path / "dist"
    dist.mkdir()
    subprocess.run(
        [
            "uv",
            "build",
            "--offline",
            "--no-python-downloads",
            "--no-build-logs",
            "--wheel",
            str(engine_root),
            "--out-dir",
            str(dist),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    (wheel,) = dist.glob("*.whl")
    unpacked = tmp_path / "site-packages"
    with ZipFile(wheel) as archive:
        assert "nexus/backtest/data/us_liquid_v1.csv" in archive.namelist()
        archive.extractall(unpacked)

    environment = dict(os.environ)
    environment.pop("PYTHONPATH", None)
    smoke = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; from datetime import date; from pathlib import Path; "
                f"sys.path.insert(0, {str(unpacked)!r}); "
                "import nexus.backtest.universe as module; "
                f"assert Path(module.__file__).is_relative_to(Path({str(unpacked)!r})); "
                "assert len(module.load_us_liquid_v1_definition(date(2026, 9, 8)).members) == 49"
            ),
        ],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert smoke.returncode == 0, smoke.stderr


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


@pytest.mark.parametrize(
    "session,market_open",
    [
        (date(2026, 1, 5), datetime(2026, 1, 5, 14, 30, tzinfo=UTC)),
        (date(2026, 7, 6), datetime(2026, 7, 6, 13, 30, tzinfo=UTC)),
    ],
)
def test_future_session_boundary_is_exact_across_standard_and_daylight_time(
    session: date, market_open: datetime
) -> None:
    assert (
        require_future_xnys_session(session, market_open - timedelta(microseconds=1)) == market_open
    )
    with pytest.raises(UniversePreflightError, match="BACKTEST_UNIVERSE_SESSION_NOT_FUTURE"):
        require_future_xnys_session(session, market_open)
