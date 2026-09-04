"""Normal local CLI with synthetic SQL master and HTTP, never business data."""

import importlib
import json
import stat
from datetime import UTC, date, datetime
from hashlib import sha256

import httpx
import pytest


def module():
    name = "nexus.ops.alpaca_prices_local"
    assert importlib.util.find_spec(name) is not None, "normal price archive CLI is missing"
    return importlib.import_module(name)


def page(token=None, *, symbol="AAPL"):
    return (
        '{"symbol":' + json.dumps(symbol) + ',"bars":[{"t":"2024-06-10T04:00:00Z",'
        '"o":100.10000000000000001,"h":101,"l":99,"c":100,"v":0,'
        '"future_field":null}],"next_page_token":' + json.dumps(token) + "}"
    ).encode()


@pytest.fixture
def invoke(tmp_path, monkeypatch, capsys):
    cli = module()
    repo = tmp_path / "repo"
    (repo / "infra").mkdir(parents=True)
    (repo / "infra/.env.local").write_text(
        "ALPACA_API_KEY_ID=synthetic-key\nALPACA_API_SECRET_KEY=synthetic-secret\n"
        "DATABASE_URL=postgresql+asyncpg://fixture:fixture@127.0.0.1:55436/unused\n"
    )
    for key in ("DATABASE_URL", "ALPACA_API_KEY_ID", "ALPACA_API_SECRET_KEY"):
        monkeypatch.delenv(key, raising=False)
    queries = []

    class Context:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    class Session(Context):
        def begin(self):
            return Context()

        async def execute(self, statement):
            queries.append(statement)
            return self

        def all(self):
            return [("AAPL", "NASDAQ", "USD")]

    class Engine:
        async def dispose(self):
            pass

    monkeypatch.setattr(cli, "create_async_engine", lambda *a, **kw: Engine())
    monkeypatch.setattr(cli, "AsyncSession", lambda *a, **kw: Session())
    original = httpx.AsyncClient
    requests = []
    clients = []

    def run(responses, *, extra=(), output=None, handler=None):
        directory = output or tmp_path / "archive"

        async def respond(request):
            requests.append(request)
            if handler:
                return await handler(request)
            return responses[len(requests) - 1]

        def client(**kwargs):
            result = original(**kwargs, transport=httpx.MockTransport(respond))
            clients.append(result)
            return result

        monkeypatch.setattr(cli.httpx, "AsyncClient", client)
        monkeypatch.setattr(
            "sys.argv",
            [
                "alpaca_prices_local",
                "--repo-root",
                str(repo),
                "--instrument-id",
                "00000000-0000-4000-8000-000000000001",
                "--start",
                "2024-06-10",
                "--end",
                "2024-06-14",
                "--adjustment",
                "split",
                "--output-dir",
                str(directory),
                *extra,
            ],
        )
        code = cli.main()
        captured = capsys.readouterr()
        assert not any(
            v in captured.out + captured.err
            for v in ("synthetic-key", "synthetic-secret", "raw-private-canary")
        )
        assert all(item.is_closed for item in clients)
        return code, directory, captured

    run.requests = requests
    run.queries = queries
    run.cli = cli
    run.repo = repo
    return run


def test_normal_cli_archives_before_parse_and_exactly_reads_back(invoke):
    originals = [page("next"), b'{"symbol":"AAPL","bars":[],"next_page_token":null}']
    before = datetime.now(UTC)
    code, directory, captured = invoke([httpx.Response(200, content=x) for x in originals])
    after = datetime.now(UTC)
    assert code == 0
    result = json.loads(captured.out)
    assert result == {
        "status": "collected",
        "pagination_exhausted": True,
        "readback_verified": True,
        "source_completeness": "unknown",
        "trading_session_completeness": "unknown",
        "research_eligible": False,
        "historical_backtest_eligible": False,
        "execution_locked": True,
    }
    manifest = json.loads((directory / "manifest.json").read_bytes())
    assert manifest["schema"] == "nexus_alpaca_prices_collection_v1"
    assert manifest["identity_asof"] == "2024-06-14"
    assert manifest["resolved_symbol"] == "AAPL"
    assert "headers" not in manifest
    assert str(invoke.queries[0]) == "SET TRANSACTION READ ONLY"
    sql = invoke.queries[1].compile()
    assert date(2024, 6, 14) in sql.params.values()
    for index, raw in enumerate(originals, 1):
        assert (directory / f"page-{index:03}.json").read_bytes() == raw
        receipt = json.loads((directory / f"page-{index:03}.receipt.json").read_bytes())
        assert receipt["sha256"] == sha256(raw).hexdigest()
        assert before <= datetime.fromisoformat(receipt["received_at"]) <= after
        query = dict(receipt["query"])
        assert query["asof"] == "2024-06-14"
        assert query["start"] == "2024-06-10T04:00:00Z"
        assert query["end"] == "2024-06-14T20:30:00Z"
        assert (query["feed"], query["currency"], query["timeframe"], query["adjustment"]) == (
            "sip",
            "USD",
            "1Day",
            "split",
        )
        assert query.get("page_token") == (None if index == 1 else "next")
        assert list(invoke.requests[index - 1].url.params.multi_items()) == [
            tuple(item) for item in receipt["query"]
        ]
    assert stat.S_IMODE(directory.stat().st_mode) == 0o700
    assert all(stat.S_IMODE(p.stat().st_mode) == 0o600 for p in directory.iterdir())
