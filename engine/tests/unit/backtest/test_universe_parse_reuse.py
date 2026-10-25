"""Pure definition reuse never caches current file or parser authority."""

import asyncio
import csv
import io
import os
from contextlib import nullcontext
from contextvars import Context
from datetime import date
from pathlib import Path

import pytest
from nexus.backtest import universe as u

DAY = date(2026, 9, 8)
RAW = u._SEED.read_text()


def scope():
    # The native implementation is also exercised by the initial RED check.
    return getattr(u, "universe_definition_parse_scope", nullcontext)()


def outcome(path, day=DAY, *, uncached=False):
    try:
        if uncached:
            result = Context().run(u.load_us_liquid_v1_definition, day, seed_path=path)
        else:
            result = u.load_us_liquid_v1_definition(day, seed_path=path)
        return ("ok", result)
    except Exception as error:
        return ("error", type(error).__name__, str(error))


@pytest.fixture
def parse_calls(monkeypatch):
    calls = []
    native = csv.DictReader

    def counted(*args, **kwargs):
        calls.append(1)
        return native(*args, **kwargs)

    monkeypatch.setattr(csv, "DictReader", counted)
    return calls


def changed(field, value):
    reader = csv.DictReader(io.StringIO(RAW))
    rows = list(reader)
    rows[0][field] = value
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=reader.fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue()


def test_reuse_parsing_keeps_every_file_open(tmp_path, monkeypatch, parse_calls):
    path = tmp_path / "seed.csv"
    path.write_text(RAW)
    opened = []
    native = Path.open

    def tracked(self, *args, **kwargs):
        if self == path:
            opened.append(self)
        return native(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", tracked)
    with scope():
        first = outcome(path)
        second = outcome(path)
    assert len(opened) == 2
    assert len(parse_calls) == 1
    assert first == second == outcome(path, uncached=True)


@pytest.mark.parametrize(
    "case",
    [
        "risk_bucket",
        "source",
        "symbol",
        "instrument_id",
        "sector_code",
        "valid_from",
        "valid_to",
        "universe_version",
        "asset_type",
        "header",
        "missing_row",
        "invalid_utf8",
        "deleted",
        "directory",
        "second_day",
        "same_mtime",
        "oversize",
    ],
)
def test_file_change_is_never_hidden(tmp_path, case):
    path = tmp_path / "seed.csv"
    path.write_text(RAW)
    stamp = path.stat()
    with scope():
        assert outcome(path) == outcome(path, uncached=True)
        values = {
            "risk_bucket": "updated",
            "source": "wrong",
            "symbol": "bad?",
            "instrument_id": "00000000-0000-0000-0000-000000000001",
            "sector_code": "",
            "valid_from": "2026-01-02",
            "valid_to": "2026-12-31",
            "universe_version": "v2",
            "asset_type": "other",
        }
        if case in values:
            path.write_text(changed(case, values[case]))
        elif case == "header":
            path.write_text(RAW.replace("source", "source_extra", 1))
        elif case == "missing_row":
            path.write_text("\n".join(RAW.splitlines()[:-1]) + "\n")
        elif case == "invalid_utf8":
            path.write_bytes(b"\xff")
        elif case in {"deleted", "directory"}:
            path.unlink()
            if case == "directory":
                path.mkdir()
        elif case == "same_mtime":
            path.write_text(changed("source", "wrong"))
            os.utime(path, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
        elif case == "oversize":
            path.write_text(changed("risk_bucket", "X" * 140_000))
        day = date(2026, 9, 9) if case == "second_day" else DAY
        assert outcome(path, day) == outcome(path, day, uncached=True)


@pytest.mark.parametrize("ending", ["\n", "\r\n", "\r"])
def test_line_endings_and_row_order(tmp_path, ending):
    path = tmp_path / "seed.csv"
    lines = RAW.splitlines()
    path.write_bytes((ending.join([lines[0], *reversed(lines[1:])]) + ending).encode())
    with scope():
        assert outcome(path) == outcome(path, uncached=True)
        assert (
            outcome(path)[1].content_hash
            == "4a9a292df983ccabbe1f7bf668427a49cf81d7a527818f8400fc90d9bc966726"
        )


def test_nested_scope_and_new_call_are_bounded(tmp_path, parse_calls):
    path = tmp_path / "seed.csv"
    path.write_text(RAW)
    with scope():
        outcome(path)
        with scope():
            outcome(path)
        outcome(path)
    assert len(parse_calls) == 1
    with scope():
        outcome(path)
    assert len(parse_calls) == 2
    outcome(path)
    outcome(path)
    assert len(parse_calls) == 4


@pytest.mark.parametrize("cancel", [False, True])
def test_inherited_task_cannot_reuse_after_owner_exit(tmp_path, parse_calls, cancel):
    path = tmp_path / "seed.csv"
    path.write_text(RAW)

    async def run():
        ready, release = asyncio.Event(), asyncio.Event()
        children = []

        async def child():
            ready.set()
            await release.wait()
            return outcome(path)

        async def owner():
            with scope():
                outcome(path)
                children.append(asyncio.create_task(child()))
                if cancel:
                    await asyncio.Event().wait()

        task = asyncio.create_task(owner())
        await ready.wait()
        if cancel:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            await task
        release.set()
        assert (await children[0])[0] == "ok"

    asyncio.run(run())
    assert len(parse_calls) == 2


@pytest.mark.parametrize("setting", ["limit", "dialect"])
def test_parser_settings_are_current(tmp_path, setting):
    path = tmp_path / "seed.csv"
    path.write_text(RAW)
    old_limit, old_dialect = csv.field_size_limit(), csv.get_dialect("excel")
    try:
        with scope():
            assert outcome(path)[0] == "ok"
            if setting == "limit":
                csv.field_size_limit(8)
            else:
                csv.register_dialect("excel", delimiter=";")
            assert (
                outcome(path)
                == outcome(path, uncached=True)
                == ("error", "UniversePreflightError", "BACKTEST_UNIVERSE_DEFINITION_INVALID")
            )
    finally:
        csv.field_size_limit(old_limit)
        csv.register_dialect("excel", old_dialect)


def test_nonseekable_and_oversize_use_stream_parser(tmp_path, monkeypatch):
    path = tmp_path / "stream.csv"
    native = Path.open

    class Nonseekable(io.StringIO):
        def seekable(self):
            return False

        def seek(self, *args):
            raise io.UnsupportedOperation("not seekable")

    def stream(self, *args, **kwargs):
        return Nonseekable(RAW, newline="") if self == path else native(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", stream)
    monkeypatch.setattr(u, "_MAX_DEFINITION_TEXT", 64, raising=False)
    with scope():
        assert outcome(path) == outcome(path, uncached=True)
        assert outcome(path)[0] == "ok"


def test_failed_file_can_be_replaced(tmp_path):
    path = tmp_path / "seed.csv"
    path.write_text("bad")
    with scope():
        assert outcome(path)[0] == "error"
        path.write_text(RAW)
        assert outcome(path) == outcome(path, uncached=True)


def test_capacity_does_not_retain_unbounded_inputs(tmp_path, parse_calls):
    path = tmp_path / "seed.csv"
    path.write_text(RAW)
    with scope():
        for day in range(1, 20):
            assert outcome(path, date(2026, 9, day))[0] == "ok"
        assert len(parse_calls) == 19
        outcome(path, date(2026, 9, 1))
        assert len(parse_calls) == 19
        outcome(path, date(2026, 9, 19))
        assert len(parse_calls) == 20


@pytest.mark.parametrize("scoped", [False, True])
def test_invalid_path_keeps_fixed_error_code(scoped):
    with scope() if scoped else nullcontext():
        assert outcome(Path("invalid\x00.csv")) == (
            "error",
            "UniversePreflightError",
            "BACKTEST_UNIVERSE_DEFINITION_INVALID",
        )
