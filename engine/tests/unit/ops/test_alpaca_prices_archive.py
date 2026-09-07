"""Negative archive contracts through the normal synthetic CLI fixture."""

import json
import subprocess
import sys
import time
from datetime import UTC, date, datetime
from hashlib import sha256

import httpx
import pytest
from unit.ops.test_alpaca_prices_local import invoke as _invoke
from unit.ops.test_alpaca_prices_local import page

invoke = _invoke


def test_oversized_http_body_is_not_archived(invoke):
    code, directory, captured = invoke(
        [
            httpx.Response(
                200,
                headers={"content-length": str(8 * 1024 * 1024 + 1)},
                content=b"raw-private-canary",
            )
        ]
    )
    assert code == 1
    assert json.loads(captured.err)["code"] == "ALPACA_PRICES_RESPONSE_TOO_LARGE"
    assert not (directory / "page-001.json").exists()
    assert len(invoke.requests) == 1


def test_empty_nonterminal_page_continues_to_explicit_null(invoke):
    code, directory, _ = invoke(
        [
            httpx.Response(200, content=b'{"symbol":"AAPL","bars":[],"next_page_token":"next"}'),
            httpx.Response(200, content=b'{"symbol":"AAPL","bars":[],"next_page_token":null}'),
        ]
    )
    assert code == 0
    assert len(invoke.requests) == 2
    assert json.loads((directory / "manifest.json").read_bytes())["research_eligible"] is False


@pytest.mark.parametrize(
    "url",
    [
        "postgresql+asyncpg://private:private@example.com/db",
        "postgresql+asyncpg://private:private@127.0.0.1/db?host=example.com",
    ],
)
def test_nonlocal_or_overridden_database_is_never_connected(invoke, monkeypatch, url):
    monkeypatch.setenv("DATABASE_URL", url)
    code, directory, captured = invoke([])
    assert code == 1
    assert json.loads(captured.err)["code"] == "ALPACA_PRICES_LOCAL_DATABASE_REQUIRED"
    assert not invoke.queries and not invoke.requests and not directory.exists()


def test_reader_exposes_explicit_identity_date_and_exact_decimal_original(invoke):
    from decimal import Decimal
    from uuid import UUID

    from nexus.providers.market.alpaca_http import load_exact_json
    from nexus.research.alpaca_prices_archive import load_price_collection

    code, directory, _ = invoke(
        [httpx.Response(200, content=page("next")), httpx.Response(200, content=page())],
        extra=("--identity-asof", "2025-01-02"),
    )
    assert code == 0
    loaded = load_price_collection(
        directory, instrument_id=UUID("00000000-0000-4000-8000-000000000001")
    )
    assert loaded.identity_asof == date(2025, 1, 2)
    assert len(invoke.requests) == 2
    assert all(request.url.params["asof"] == "2025-01-02" for request in invoke.requests)
    payload = load_exact_json(loaded.originals[0].content)
    assert payload["bars"][0]["o"] == Decimal("100.10000000000000001")
    assert payload["bars"][0]["future_field"] is None


@pytest.mark.parametrize(
    "raw",
    [
        b"not-json",
        b'{"symbol":"OTHER","bars":[],"next_page_token":null}',
        b'{"symbol":"AAPL","bars":[]}',
        b'{"symbol":"AAPL","bars":false,"next_page_token":null}',
    ],
)
def test_bad_success_response_is_archived_before_rejection(invoke, raw):
    code, directory, captured = invoke([httpx.Response(200, content=raw)])
    assert code == 1
    assert json.loads(captured.err)["code"] == "ALPACA_PRICES_PARSE_FAILED"
    assert (directory / "page-001.json").read_bytes() == raw
    assert (directory / "page-001.receipt.json").exists()
    assert json.loads((directory / "manifest.json").read_bytes())["pagination_exhausted"] is False
    assert len(invoke.requests) == 1


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (301, "ALPACA_PRICES_REDIRECT_REJECTED"),
        (401, "ALPACA_PRICES_STATUS_REJECTED"),
        (429, "ALPACA_PRICES_RETRY_BUDGET_EXHAUSTED"),
        (500, "ALPACA_PRICES_RETRY_BUDGET_EXHAUSTED"),
    ],
)
def test_http_failure_never_archives_body_or_retries(invoke, status, expected):
    code, directory, captured = invoke([httpx.Response(status, content=b"raw-private-canary")])
    assert code == 1
    assert json.loads(captured.err)["code"] == expected
    assert set(p.name for p in directory.iterdir()) == {"manifest.json"}
    assert len(invoke.requests) == 1


def test_same_output_directory_is_never_overwritten(invoke, tmp_path):
    directory = tmp_path / "archive"
    directory.mkdir()
    sentinel = directory / "keep"
    sentinel.write_bytes(b"original")
    code, _, captured = invoke([], output=directory)
    assert code == 1
    assert json.loads(captured.err)["code"] == "ALPACA_PRICES_NEW_OUTPUT_REQUIRED"
    assert sentinel.read_bytes() == b"original"
    assert not invoke.requests


@pytest.mark.parametrize(
    "extra",
    [
        ("--adjustment", "raw"),
        ("--adjustment", "all"),
        ("--start", "2024-11-29", "--end", "2024-11-29"),
    ],
)
def test_adjustment_choice_and_early_close_are_actual_request_facts(invoke, extra):
    code, directory, _ = invoke([httpx.Response(200, content=page())], extra=extra)
    assert code == 0
    query = dict(json.loads((directory / "manifest.json").read_bytes())["query"])
    if "--adjustment" in extra:
        assert query["adjustment"] == extra[1]
    else:
        assert query["start"] == "2024-11-29T05:00:00Z"
        assert query["end"] == "2024-11-29T18:30:00Z"
        assert query["asof"] == "2024-11-29"


@pytest.mark.parametrize(
    "extra",
    [
        ("--start", "2024-06-15"),
        ("--end", "2099-01-01"),
        ("--start", "2020-01-02"),
        ("--max-pages", "0"),
        ("--max-pages", "11"),
        ("--adjustment", "dividend"),
        ("--instrument-id", "raw-private-canary"),
    ],
)
def test_bad_arguments_make_no_http_or_archive(invoke, extra):
    code, directory, _ = invoke([], extra=extra)
    assert code == 1
    assert not directory.exists()
    assert not invoke.requests


@pytest.mark.parametrize(
    "end,now,allowed",
    [
        (date(2024, 11, 29), datetime(2024, 11, 29, 18, 29, 59, tzinfo=UTC), False),
        (date(2024, 11, 29), datetime(2024, 11, 29, 18, 30, tzinfo=UTC), True),
    ],
)
def test_current_session_must_have_completed_buffer(end, now, allowed):
    from uuid import UUID

    from nexus.research.alpaca_prices_archive import collection_window

    identifier = UUID("00000000-0000-4000-8000-000000000001")
    if allowed:
        request, _ = collection_window(identifier, end, end, "raw", now=now)
        assert request.end == now
    else:
        with pytest.raises(ValueError, match="SESSION_NOT_COMPLETED"):
            collection_window(identifier, end, end, "raw", now=now)


@pytest.mark.parametrize(
    "max_pages,tokens,code",
    [
        ("1", ["next"], "ALPACA_PRICES_PAGE_LIMIT"),
        ("3", ["next", "next"], "ALPACA_PRICES_CURSOR_CYCLE"),
    ],
)
def test_page_bounds_stop_once_keep_originals_and_deny_readback(invoke, max_pages, tokens, code):
    from uuid import UUID

    from nexus.research.alpaca_prices_archive import load_price_collection

    status, directory, captured = invoke(
        [httpx.Response(200, content=page(token)) for token in tokens],
        extra=("--max-pages", max_pages),
    )
    assert status == 1
    assert json.loads(captured.err)["code"] == code
    assert len(invoke.requests) == len(tokens)
    assert len(list(directory.glob("*.receipt.json"))) == len(tokens)
    with pytest.raises(ValueError, match="ARCHIVE_INVALID"):
        load_price_collection(directory, instrument_id=UUID("00000000-0000-4000-8000-000000000001"))


@pytest.mark.parametrize(
    "raw",
    [
        b"not-json",
        b'{"symbol":"AAPL","symbol":"AAPL"}',
        b'{"symbol":"AAPL","bars":[],"next_page_token":NaN}',
    ],
)
def test_rehashed_bad_original_has_one_safe_reader_error(invoke, raw):
    from uuid import UUID

    from nexus.research.alpaca_prices_archive import load_price_collection

    code, directory, _ = invoke([httpx.Response(200, content=page())])
    assert code == 0
    (directory / "page-001.json").write_bytes(raw)
    receipt_path = directory / "page-001.receipt.json"
    receipt = json.loads(receipt_path.read_bytes())
    receipt.update(sha256=sha256(raw).hexdigest(), byte_length=len(raw))
    receipt_path.write_text(json.dumps(receipt))
    manifest_path = directory / "manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    manifest["pages"][0].update(
        raw_sha256=sha256(raw).hexdigest(),
        receipt_sha256=sha256(receipt_path.read_bytes()).hexdigest(),
    )
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="^ALPACA_PRICES_ARCHIVE_INVALID$"):
        load_price_collection(directory, instrument_id=UUID("00000000-0000-4000-8000-000000000001"))


@pytest.mark.parametrize("scope", ["manifest", "receipt", "entry"])
def test_fixed_metadata_rejects_undeclared_fields(invoke, scope):
    from uuid import UUID

    from nexus.research.alpaca_prices_archive import load_price_collection

    code, directory, _ = invoke([httpx.Response(200, content=page())])
    assert code == 0
    manifest_path = directory / "manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    if scope == "manifest":
        manifest["headers"] = {"Authorization": "raw-private-canary"}
    elif scope == "entry":
        manifest["pages"][0]["extra"] = "raw-private-canary"
    else:
        path = directory / "page-001.receipt.json"
        receipt = json.loads(path.read_bytes())
        receipt["headers"] = {"Authorization": "raw-private-canary"}
        path.write_text(json.dumps(receipt))
        manifest["pages"][0]["receipt_sha256"] = sha256(path.read_bytes()).hexdigest()
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="^ALPACA_PRICES_ARCHIVE_INVALID$"):
        load_price_collection(directory, instrument_id=UUID("00000000-0000-4000-8000-000000000001"))


def test_synchronous_archive_work_cannot_claim_success_past_deadline(invoke, monkeypatch):
    original = invoke.cli.write_private
    monkeypatch.setattr(invoke.cli, "_COLLECTION_SECONDS", 0.02)

    def slow_write(path, content):
        if path.name == "page-001.json":
            time.sleep(0.03)
        original(path, content)

    monkeypatch.setattr(invoke.cli, "write_private", slow_write)
    code, directory, captured = invoke([httpx.Response(200, content=page())])
    assert code == 1
    assert json.loads(captured.err)["code"] == "ALPACA_PRICES_DEADLINE_EXCEEDED"
    assert json.loads((directory / "manifest.json").read_bytes())["pagination_exhausted"] is False


@pytest.mark.parametrize("phase", ["manifest", "readback", "publish", "cleanup"])
def test_final_local_stages_cannot_publish_success_past_deadline(invoke, monkeypatch, phase):
    from uuid import UUID

    from nexus.research.alpaca_prices_archive import load_price_collection

    monkeypatch.setattr(invoke.cli, "_COLLECTION_SECONDS", 0.05)
    if phase == "manifest":
        original = invoke.cli.write_private

        def slow_write(path, content):
            if path.name.startswith("manifest"):
                time.sleep(0.06)
            original(path, content)

        monkeypatch.setattr(invoke.cli, "write_private", slow_write)
    elif phase == "readback":
        original_reader = invoke.cli._verify_pending_collection

        def slow_reader(*args, **kwargs):
            result = original_reader(*args, **kwargs)
            time.sleep(0.06)
            return result

        monkeypatch.setattr(invoke.cli, "_verify_pending_collection", slow_reader)
    elif phase == "publish":
        original_link = invoke.cli.os.link

        def slow_link(*args, **kwargs):
            result = original_link(*args, **kwargs)
            time.sleep(0.06)
            return result

        monkeypatch.setattr(invoke.cli.os, "link", slow_link)
    else:
        original_unlink = invoke.cli.Path.unlink

        def slow_unlink(path, *args, **kwargs):
            result = original_unlink(path, *args, **kwargs)
            if path.name == "manifest.pending.json":
                time.sleep(0.06)
            return result

        monkeypatch.setattr(invoke.cli.Path, "unlink", slow_unlink)
    code, directory, captured = invoke([httpx.Response(200, content=page())])
    assert code == 1
    assert json.loads(captured.err)["code"] == "ALPACA_PRICES_DEADLINE_EXCEEDED"
    assert (directory / "page-001.json").exists()
    assert (directory / "page-001.receipt.json").exists()
    assert (directory / "manifest.pending.json").exists()
    assert not (directory / "manifest.json").exists()
    assert len(invoke.requests) == 1
    with pytest.raises(ValueError, match="ARCHIVE_INVALID"):
        load_price_collection(directory, instrument_id=UUID("00000000-0000-4000-8000-000000000001"))


@pytest.mark.parametrize(
    "kind",
    [
        "raw",
        "receipt",
        "missing",
        "extra",
        "public",
        "symlink",
        "identity",
        "future_clock",
        "query",
        "promote",
        "bool_size",
    ],
)
def test_exact_reader_rejects_archive_tampering(invoke, kind):
    from uuid import UUID

    from nexus.research.alpaca_prices_archive import load_price_collection

    code, directory, _ = invoke([httpx.Response(200, content=page())])
    assert code == 0
    raw_path = directory / "page-001.json"
    receipt_path = directory / "page-001.receipt.json"
    manifest_path = directory / "manifest.json"
    manifest = json.loads(manifest_path.read_bytes())
    if kind == "raw":
        raw_path.write_bytes(b"{}")
    elif kind == "receipt":
        receipt_path.write_bytes(b"{}")
    elif kind == "missing":
        raw_path.unlink()
    elif kind == "extra":
        (directory / "extra").write_bytes(b"extra")
    elif kind == "public":
        raw_path.chmod(0o644)
    elif kind == "symlink":
        raw_path.unlink()
        raw_path.symlink_to(receipt_path)
    elif kind == "identity":
        manifest["instrument_id"] = "00000000-0000-4000-8000-000000000002"
    elif kind == "future_clock":
        manifest["completed_at"] = "2099-01-01T00:00:00+00:00"
    elif kind == "query":
        manifest["query"][0][1] = "raw"
    elif kind == "promote":
        manifest["research_eligible"] = True
    elif kind == "bool_size":
        receipt = json.loads(receipt_path.read_bytes())
        receipt["byte_length"] = True
        receipt_path.write_text(json.dumps(receipt))
        manifest["pages"][0]["receipt_sha256"] = sha256(receipt_path.read_bytes()).hexdigest()
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="ALPACA_PRICES_ARCHIVE_INVALID"):
        load_price_collection(directory, instrument_id=UUID("00000000-0000-4000-8000-000000000001"))


def test_price_archive_cli_is_a_normal_module_entrypoint():
    result = subprocess.run(
        [sys.executable, "-m", "nexus.ops.alpaca_prices_local", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "--instrument-id" in result.stdout


def test_explicit_identity_date_changes_sql_and_http_not_data_window(invoke):
    code, directory, _ = invoke(
        [httpx.Response(200, content=page())], extra=("--identity-asof", "2025-01-02")
    )
    assert code == 0
    sql = invoke.queries[1].compile()
    assert date(2025, 1, 2) in sql.params.values()
    manifest = json.loads((directory / "manifest.json").read_bytes())
    assert manifest["identity_asof"] == "2025-01-02"
    query = dict(manifest["query"])
    assert query["asof"] == "2025-01-02"
    assert query["start"] == "2024-06-10T04:00:00Z"
    assert query["end"] == "2024-06-14T20:30:00Z"
    assert all(request.url.params["asof"] == "2025-01-02" for request in invoke.requests)


def test_future_identity_date_never_reads_master_or_calls_http(invoke):
    code, directory, _ = invoke([], extra=("--identity-asof", "2099-01-01"))
    assert code == 1
    assert not invoke.queries and not invoke.requests and not directory.exists()
