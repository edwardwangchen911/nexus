"""Manual collection archives exact bytes without interpreting ledger eligibility."""

import json
import stat
from datetime import UTC, datetime
from hashlib import sha256
from time import monotonic

import httpx
import pytest
from nexus.ops import alpaca_actions_local as cli


def page(token=None):
    return json.dumps(
        {
            "corporate_actions": {
                "future_category": [{"id": "synthetic-event", "unknown": {"amount": None}}]
            },
            "next_page_token": token,
        }
    ).encode()


@pytest.fixture
def invoke(tmp_path, monkeypatch, capsys):
    repo = tmp_path / "repo"
    (repo / "infra").mkdir(parents=True)
    (repo / "infra" / ".env.local").write_text(
        "ALPACA_API_KEY_ID=synthetic-key\nALPACA_API_SECRET_KEY=synthetic-secret\n"
    )
    monkeypatch.delenv("ALPACA_API_KEY_ID", raising=False)
    monkeypatch.delenv("ALPACA_API_SECRET_KEY", raising=False)
    original = httpx.AsyncClient
    requests = []
    clients = []
    times = []

    def run(responses, *, extra=(), output=None, handler=None):
        directory = output or tmp_path / "archive"

        async def respond(request):
            requests.append(request)
            times.append(monotonic())
            if handler:
                return await handler(request)
            return responses[len(requests) - 1]

        def client(**kwargs):
            assert kwargs["follow_redirects"] is False
            assert kwargs.get("trust_env", True) is True
            result = original(**kwargs, transport=httpx.MockTransport(respond))
            clients.append(result)
            return result

        monkeypatch.setattr(cli.httpx, "AsyncClient", client)
        monkeypatch.setattr(
            "sys.argv",
            [
                "alpaca_actions_local",
                "--repo-root",
                str(repo),
                "--symbol",
                "SPY",
                "--start",
                "2026-08-01",
                "--end",
                "2026-08-31",
                "--output-dir",
                str(directory),
                *extra,
            ],
        )
        code = 0
        try:
            cli.main()
        except SystemExit as error:
            code = error.code
        captured = capsys.readouterr()
        assert "synthetic-key" not in captured.out + captured.err
        assert "synthetic-secret" not in captured.out + captured.err
        assert all(client.is_closed for client in clients)
        return code, directory, captured

    run.requests = requests
    run.times = times
    run.repo = repo
    return run


def test_pagination_preserves_exact_originals_receipts_and_unknowns(invoke):
    raw = [page("opaque+/="), page()]
    before = datetime.now(UTC)
    code, directory, captured = invoke([httpx.Response(200, content=item) for item in raw])
    after = datetime.now(UTC)
    assert code == 0
    assert json.loads(captured.out) == {
        "status": "collected",
        "pagination_exhausted": True,
        "source_completeness": "unknown",
        "execution_locked": True,
        "historical_backtest_eligible": False,
    }
    manifest = json.loads((directory / "manifest.json").read_bytes())
    assert manifest["status"] == "collected"
    assert manifest["source_completeness"] == "unknown"
    assert manifest["execution_locked"] is True
    assert manifest["historical_backtest_eligible"] is False
    expected = [
        ["symbols", "SPY"],
        ["start", "2026-08-01"],
        ["end", "2026-08-31"],
        ["region", "us"],
        ["data_quality", "all"],
        ["sort", "asc"],
        ["limit", "1000"],
    ]
    assert manifest["query"] == expected
    assert len(manifest["pages"]) == 2
    for index, content in enumerate(raw, 1):
        assert (directory / f"page-{index:03}.json").read_bytes() == content
        receipt = json.loads((directory / f"page-{index:03}.receipt.json").read_bytes())
        assert receipt["sha256"] == sha256(content).hexdigest()
        assert receipt["byte_length"] == len(content)
        assert before <= datetime.fromisoformat(receipt["received_at"]) <= after
        assert receipt["query"] == expected + ([["page_token", "opaque+/="]] if index == 2 else [])
    for index, request in enumerate(invoke.requests, 1):
        assert request.url.host == "data.alpaca.markets"
        assert request.url.path == "/v1/corporate-actions"
        assert request.headers["APCA-API-KEY-ID"] == "synthetic-key"
        assert request.headers["APCA-API-SECRET-KEY"] == "synthetic-secret"
        assert "types" not in request.url.params and "date_type" not in request.url.params
        assert list(map(list, request.url.params.multi_items())) == expected + (
            [["page_token", "opaque+/="]] if index == 2 else []
        )
    assert invoke.times[1] - invoke.times[0] >= 0.35
    assert stat.S_IMODE(directory.stat().st_mode) == 0o700
    assert all(stat.S_IMODE(path.stat().st_mode) == 0o600 for path in directory.iterdir())


def test_empty_page_with_cursor_does_not_stop_collection(invoke):
    first = b'{"corporate_actions":{},"next_page_token":"next"}'
    code, directory, _ = invoke(
        [httpx.Response(200, content=first), httpx.Response(200, content=page())]
    )
    assert code == 0
    assert len(invoke.requests) == 2
    assert json.loads((directory / "manifest.json").read_bytes())["pagination_exhausted"] is True


@pytest.mark.parametrize(
    ("responses", "extra", "count", "status", "error_code"),
    [
        (
            [httpx.Response(200, content=page("repeat"))] * 2,
            (),
            2,
            "partial",
            "ALPACA_ACTIONS_CURSOR_CYCLE",
        ),
        (
            [httpx.Response(200, content=page("next"))],
            ("--max-pages", "1"),
            1,
            "partial",
            "ALPACA_ACTIONS_PAGE_LIMIT",
        ),
        (
            [
                httpx.Response(200, content=page("next")),
                httpx.Response(429, text="synthetic-secret"),
            ],
            (),
            2,
            "partial",
            "ALPACA_ACTIONS_HTTP_FAILED",
        ),
        (
            [httpx.Response(429, text="synthetic-secret")],
            (),
            1,
            "failed",
            "ALPACA_ACTIONS_HTTP_FAILED",
        ),
        (
            [httpx.Response(200, content=b"not-json synthetic-secret")],
            (),
            1,
            "failed",
            "ALPACA_ACTIONS_PARSE_FAILED",
        ),
        (
            [httpx.Response(302, headers={"location": "https://example.invalid/secret"})],
            (),
            1,
            "failed",
            "ALPACA_ACTIONS_HTTP_FAILED",
        ),
    ],
)
def test_failures_stop_without_retries_and_preserve_received_originals(
    invoke, responses, extra, count, status, error_code
):
    code, directory, captured = invoke(responses, extra=extra)
    assert code == 1
    assert len(invoke.requests) == count
    manifest = json.loads((directory / "manifest.json").read_bytes())
    assert manifest["status"] == status
    assert manifest["pagination_exhausted"] is False
    assert manifest["error_code"] == error_code
    assert json.loads(captured.err)["code"] == error_code
    for index, response in enumerate(responses, 1):
        if response.status_code == 200:
            assert (directory / f"page-{index:03}.json").read_bytes() == response.content
            assert (directory / f"page-{index:03}.receipt.json").exists()


@pytest.mark.parametrize(
    "extra",
    [
        ("--symbol", "private-canary?"),
        ("--start", "private-canary"),
        ("--end", "2026-07-01"),
        ("--max-pages", "0"),
        ("--max-pages", "101"),
        ("--unexpected", "private-canary"),
    ],
)
def test_invalid_arguments_never_create_archive_or_network(invoke, extra):
    code, directory, captured = invoke([], extra=extra)
    assert code == 1
    assert not invoke.requests
    assert not directory.exists()
    assert "private-canary" not in captured.out + captured.err


def test_missing_keys_and_existing_output_are_preflight_failures(invoke, tmp_path):
    (invoke.repo / "infra" / ".env.local").write_text("")
    code, directory, _ = invoke([])
    assert code == 1 and not directory.exists() and not invoke.requests
    (invoke.repo / "infra" / ".env.local").write_text(
        "ALPACA_API_KEY_ID=synthetic-key\nALPACA_API_SECRET_KEY=synthetic-secret\n"
    )
    directory.mkdir()
    original = directory / "page-001.json"
    original.write_bytes(b"existing-private-evidence")
    code, _, _ = invoke([], output=directory)
    assert code == 1 and not invoke.requests
    assert original.read_bytes() == b"existing-private-evidence"


def test_total_deadline_stops_without_replay_and_writes_failure_manifest(invoke, monkeypatch):
    import asyncio

    monkeypatch.setattr(cli, "_COLLECTION_SECONDS", 0.02)

    async def stalled(request):
        await asyncio.sleep(1)
        pytest.fail("collection deadline did not cancel HTTP request")

    code, directory, _ = invoke([], handler=stalled)
    assert code == 1 and len(invoke.requests) == 1
    assert json.loads((directory / "manifest.json").read_bytes())["pagination_exhausted"] is False


def test_response_size_limit_rejects_before_parsing_without_retry(invoke):
    code, directory, _ = invoke([httpx.Response(200, content=b"x" * (8 * 1024 * 1024 + 1))])
    assert code == 1 and len(invoke.requests) == 1
    manifest = json.loads((directory / "manifest.json").read_bytes())
    assert manifest["status"] == "failed"
    assert manifest["error_code"] == "ALPACA_ACTIONS_HTTP_FAILED"
    assert manifest["pages"] == []


def test_deadline_during_spacing_never_starts_another_request(invoke, monkeypatch):
    clock = [0.0]

    async def pass_spacing(seconds):
        clock[0] += seconds

    monkeypatch.setattr(cli, "monotonic", lambda: clock[0])
    monkeypatch.setattr(cli.asyncio, "sleep", pass_spacing)
    monkeypatch.setattr(cli, "_COLLECTION_SECONDS", 60.0)
    monkeypatch.setattr(cli, "_REQUEST_SPACING_SECONDS", 61.0)
    code, directory, _ = invoke([httpx.Response(200, content=page("next"))])
    assert code == 1 and len(invoke.requests) == 1
    manifest = json.loads((directory / "manifest.json").read_bytes())
    assert manifest["status"] == "partial"
    assert manifest["error_code"] == "ALPACA_ACTIONS_DEADLINE_EXCEEDED"
    assert len(manifest["pages"]) == 1


def test_synchronous_parse_cannot_report_collected_after_deadline(invoke, monkeypatch):
    clock = [0.0]
    original = cli.parse_corporate_actions_page

    def parse(raw):
        result = original(raw)
        clock[0] = 121.0
        return result

    monkeypatch.setattr(cli, "monotonic", lambda: clock[0])
    monkeypatch.setattr(cli, "parse_corporate_actions_page", parse)
    code, directory, _ = invoke([httpx.Response(200, content=page())])
    assert code == 1
    manifest = json.loads((directory / "manifest.json").read_bytes())
    assert manifest["pagination_exhausted"] is False
    assert manifest["error_code"] == "ALPACA_ACTIONS_DEADLINE_EXCEEDED"
    assert (directory / "page-001.json").read_bytes() == page()
    assert (directory / "page-001.receipt.json").is_file()


def test_keyboard_interrupt_is_safe_and_keeps_already_archived_originals(invoke, monkeypatch):
    def interrupted(raw):
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "parse_corporate_actions_page", interrupted)
    try:
        code, directory, captured = invoke([httpx.Response(200, content=page())])
    except KeyboardInterrupt:
        pytest.fail("operator interruption escaped the CLI safe-error boundary")
    assert code == 1
    assert json.loads(captured.err) == {"status": "failed", "code": "ALPACA_ACTIONS_INTERRUPTED"}
    assert (directory / "page-001.json").read_bytes() == page()
    assert (directory / "page-001.receipt.json").is_file()
    assert not (directory / "manifest.json").exists()
