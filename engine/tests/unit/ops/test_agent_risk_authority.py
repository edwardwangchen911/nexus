import asyncio
import json
import stat
from datetime import UTC, date, datetime, timedelta
from email.utils import format_datetime
from urllib.parse import urlsplit
from uuid import UUID
from zoneinfo import ZoneInfo

import httpx
import pytest
from nexus.ops import agent_risk_authority as cli
from nexus.ops import alpaca_actions_local
from pydantic import SecretStr


def _responses(published_at: datetime, *, directory_session: date | None = None):
    directory_created_at = published_at.astimezone(ZoneInfo("America/New_York"))
    if directory_session is not None:
        directory_created_at = directory_created_at.replace(
            year=directory_session.year,
            month=directory_session.month,
            day=directory_session.day,
        )
    halt = (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<rss version="2.0" xmlns:ndaq="http://www.nasdaqtrader.com/">'
        "<channel><title>NASDAQTrader.com</title>"
        f"<pubDate>{format_datetime(published_at, usegmt=True)}</pubDate>"
        "<ndaq:numItems>1</ndaq:numItems>"
        "<item><title>MSFT</title><ndaq:HaltDate>09/14/2026</ndaq:HaltDate>"
        "<ndaq:IssueSymbol>MSFT</ndaq:IssueSymbol><ndaq:ReasonCode>T1</ndaq:ReasonCode>"
        "<ndaq:ResumptionTradeTime /></item></channel></rss>"
    ).encode()
    directory = (
        "Symbol|Security Name|Market Category|Test Issue|Financial Status|"
        "Round Lot Size|ETF|NextShares\n"
        "AAPL|Apple Inc. - Common Stock|Q|N|N|40|N|N\n"
        f"File Creation Time: {directory_created_at:%m%d%Y%H:%M}|||||||\n"
    ).encode()
    action = json.dumps(
        {
            "corporate_actions": {
                "cash_dividends": [{"id": "event-1", "symbol": "AAPL", "ex_date": "2026-09-14"}]
            },
            "next_page_token": None,
        }
    ).encode()
    return action, halt, directory


def test_collects_and_archives_exact_three_source_proofs(tmp_path, monkeypatch) -> None:
    published = datetime.now(UTC) - timedelta(seconds=5)
    decision_session = published.astimezone(ZoneInfo("America/New_York")).date()
    action, halt, directory = _responses(published)
    requested: list[str] = []

    async def respond(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url))
        if request.url.host == "data.alpaca.markets":
            return httpx.Response(200, content=action)
        if request.url.path == "/rss.aspx":
            return httpx.Response(200, content=halt)
        return httpx.Response(200, content=directory)

    original = httpx.AsyncClient

    def client(**kwargs):
        return original(**kwargs, transport=httpx.MockTransport(respond))

    monkeypatch.setattr(cli.httpx, "AsyncClient", client)
    output = tmp_path / "risk-authority"

    proofs = asyncio.run(
        cli.collect_source_proofs(
            symbol="AAPL",
            instrument_id=UUID("51000000-0000-0000-0000-000000000001"),
            decision_session=decision_session,
            key_id=SecretStr("synthetic-key"),
            secret=SecretStr("synthetic-secret"),
            output_dir=output,
            cutoff_at=None,
        )
    )

    assert [(proof.kind, proof.blocked) for proof in proofs] == [
        ("corporate_action", True),
        ("delisting", False),
        ("suspension", False),
    ]
    assert next(proof for proof in proofs if proof.kind == "suspension").available_at == (
        published.replace(microsecond=0)
    )
    assert len(requested) == 3
    assert [urlsplit(url).path for url in requested] == [
        "/v1/corporate-actions",
        "/dynamic/SymDir/nasdaqlisted.txt",
        "/rss.aspx",
    ]
    assert stat.S_IMODE(output.stat().st_mode) == 0o700
    assert stat.S_IMODE((output / "actions").stat().st_mode) == 0o700
    assert all(
        stat.S_IMODE(path.stat().st_mode) == 0o600 for path in output.iterdir() if path.is_file()
    )
    manifest = json.loads((output / "manifest.json").read_bytes())
    assert manifest["status"] == "collected"
    assert manifest["execution_locked"] is True
    assert manifest["model_called"] is False


def test_prior_session_directory_fails_closed_and_preserves_original(tmp_path, monkeypatch) -> None:
    published = datetime.now(UTC) - timedelta(seconds=5)
    decision_session = published.astimezone(ZoneInfo("America/New_York")).date()
    action, halt, directory = _responses(
        published, directory_session=decision_session - timedelta(days=1)
    )

    async def respond(request: httpx.Request) -> httpx.Response:
        if request.url.host == "data.alpaca.markets":
            return httpx.Response(200, content=action)
        if request.url.path == "/rss.aspx":
            return httpx.Response(200, content=halt)
        return httpx.Response(200, content=directory)

    original = httpx.AsyncClient
    monkeypatch.setattr(
        cli.httpx,
        "AsyncClient",
        lambda **kwargs: original(**kwargs, transport=httpx.MockTransport(respond)),
    )
    output = tmp_path / "risk-authority"

    with pytest.raises(ValueError, match="AGENT_RISK_SOURCE_COLLECTION_FAILED"):
        asyncio.run(
            cli.collect_source_proofs(
                symbol="AAPL",
                instrument_id=UUID("51000000-0000-0000-0000-000000000001"),
                decision_session=decision_session,
                key_id=SecretStr("synthetic-key"),
                secret=SecretStr("synthetic-secret"),
                output_dir=output,
                cutoff_at=None,
            )
        )

    assert (output / "nasdaqlisted.txt").read_bytes() == directory
    manifest = json.loads((output / "manifest.json").read_bytes())
    assert manifest["status"] == "failed"
    assert manifest["execution_locked"] is True


def test_halt_response_received_after_decision_cutoff_is_rejected(tmp_path, monkeypatch) -> None:
    cutoff = datetime(2026, 9, 14, 20, 30, tzinfo=UTC)
    action, halt, directory = _responses(cutoff - timedelta(seconds=30))
    received_times = iter(
        (
            cutoff - timedelta(seconds=10),
            cutoff - timedelta(seconds=5),
            cutoff + timedelta(seconds=5),
        )
    )

    class SequencedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            value = next(received_times)
            return value if tz is not None else value.replace(tzinfo=None)

    class ActionReceiptDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            value = cutoff - timedelta(seconds=10)
            return value if tz is not None else value.replace(tzinfo=None)

    async def respond(request: httpx.Request) -> httpx.Response:
        if request.url.host == "data.alpaca.markets":
            return httpx.Response(200, content=action)
        if request.url.path == "/rss.aspx":
            return httpx.Response(200, content=halt)
        return httpx.Response(200, content=directory)

    original = httpx.AsyncClient
    monkeypatch.setattr(
        cli.httpx,
        "AsyncClient",
        lambda **kwargs: original(**kwargs, transport=httpx.MockTransport(respond)),
    )
    monkeypatch.setattr(cli, "datetime", SequencedDatetime)
    monkeypatch.setattr(alpaca_actions_local, "datetime", ActionReceiptDatetime)

    with pytest.raises(ValueError, match="AGENT_RISK_COLLECTION_WINDOW_INVALID"):
        asyncio.run(
            cli.collect_source_proofs(
                symbol="AAPL",
                instrument_id=UUID("51000000-0000-0000-0000-000000000001"),
                decision_session=date(2026, 9, 14),
                key_id=SecretStr("synthetic-key"),
                secret=SecretStr("synthetic-secret"),
                output_dir=tmp_path / "risk-authority",
                cutoff_at=cutoff,
            )
        )


def test_cli_emits_only_bounded_result_and_never_requires_cycle_enablement(
    tmp_path, monkeypatch, capsys
) -> None:
    environment = {
        "DATABASE_URL": "postgresql+asyncpg://user:password@127.0.0.1:55433/nexus",
        "ALPACA_API_KEY_ID": "synthetic-key",
        "ALPACA_API_SECRET_KEY": "synthetic-secret",
    }
    seen = {}

    async def run_once(
        settings,
        *,
        decision_session,
        decision_time,
        output_dir,
        now=None,
        source_policy_id,
    ):
        seen["database_url"] = settings.database_url
        seen["decision_session"] = decision_session
        seen["decision_time"] = decision_time
        seen["output_dir"] = output_dir
        seen["source_policy_id"] = source_policy_id
        return {
            "status": "verified",
            "authority_count": 3,
            "blocked_kinds": [],
            "execution_locked": True,
            "model_called": False,
        }

    monkeypatch.setattr(cli, "load_local_environment", lambda root, values: environment)
    monkeypatch.setattr(cli, "run_once", run_once)
    output = tmp_path / "risk"

    code = cli.main(
        [
            "--decision-session",
            "2026-09-14",
            "--decision-time",
            "2026-09-14T20:30:00Z",
            "--output-dir",
            str(output),
        ]
    )

    assert code == 0
    assert json.loads(capsys.readouterr().out) == {
        "status": "verified",
        "authority_count": 3,
        "blocked_kinds": [],
        "execution_locked": True,
        "model_called": False,
    }
    assert seen["decision_session"] == date(2026, 9, 14)
    assert seen["decision_time"] == datetime(2026, 9, 14, 20, 30, tzinfo=UTC)
    assert seen["output_dir"] == output
    assert seen["database_url"].endswith("/nexus")
    assert seen["source_policy_id"] == cli.PAPER_DECISION_SOURCE_POLICY_ID


def test_cli_selects_same_session_schwab_policy_explicitly(tmp_path, monkeypatch, capsys) -> None:
    environment = {
        "DATABASE_URL": "postgresql+asyncpg://user:password@127.0.0.1:55433/nexus",
        "ALPACA_API_KEY_ID": "synthetic-key",
        "ALPACA_API_SECRET_KEY": "synthetic-secret",
    }
    seen = {}

    async def run_once(
        settings,
        *,
        decision_session,
        decision_time,
        output_dir,
        now=None,
        source_policy_id,
    ):
        seen["source_policy_id"] = source_policy_id
        return {
            "status": "verified",
            "authority_count": 3,
            "blocked_kinds": [],
            "execution_locked": True,
            "model_called": False,
        }

    monkeypatch.setattr(cli, "load_local_environment", lambda root, values: environment)
    monkeypatch.setattr(cli, "run_once", run_once)

    code = cli.main(
        [
            "--decision-session",
            "2026-09-14",
            "--decision-time",
            "2026-09-14T14:00:00Z",
            "--output-dir",
            str(tmp_path / "risk"),
            "--same-session-schwab",
        ]
    )

    assert code == 0
    assert json.loads(capsys.readouterr().out)["status"] == "verified"
    assert seen["source_policy_id"] == cli.PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID


def test_cli_invalid_date_returns_fixed_code_without_echoing_input(
    capsys,
) -> None:
    secret_input = "synthetic-sensitive-value"

    code = cli.main(
        [
            "--decision-session",
            secret_input,
            "--decision-time",
            "2026-09-14T20:30:00Z",
            "--output-dir",
            "/tmp/not-used",
        ]
    )

    captured = capsys.readouterr()
    assert code == 1
    assert json.loads(captured.out) == {"error_code": "AGENT_RISK_ARGUMENT_INVALID"}
    assert secret_input not in captured.out + captured.err


def test_batch_fetches_shared_nasdaq_once_and_actions_once_per_instrument(tmp_path, monkeypatch):
    published = datetime.now(UTC) - timedelta(seconds=5)
    decision_session = published.astimezone(ZoneInfo("America/New_York")).date()
    _, halt, directory = _responses(published)
    directory = directory.replace(b"File Creation", b"MSFT|Microsoft|Q|N|N|100|N|N\nFile Creation")
    requested = []

    async def respond(request):
        requested.append(request)
        if request.url.host == "data.alpaca.markets":
            return httpx.Response(200, json={"corporate_actions": {}, "next_page_token": None})
        return httpx.Response(200, content=halt if request.url.path == "/rss.aspx" else directory)

    original = httpx.AsyncClient
    monkeypatch.setattr(
        cli.httpx,
        "AsyncClient",
        lambda **kw: original(**kw, transport=httpx.MockTransport(respond)),
    )
    assert hasattr(cli, "collect_source_proofs_batch"), "bounded batch collector missing"
    candidates = (cli.RiskCandidate("AAPL", UUID(int=1)), cli.RiskCandidate("MSFT", UUID(int=2)))
    proofs = asyncio.run(
        cli.collect_source_proofs_batch(
            candidates=candidates,
            max_instruments=5,
            decision_session=decision_session,
            key_id=SecretStr("synthetic-key"),
            secret=SecretStr("synthetic-secret"),
            output_dir=tmp_path / "batch",
            cutoff_at=None,
        )
    )
    assert set(proofs) == {UUID(int=1), UUID(int=2)}
    assert [(p.kind, p.blocked) for p in proofs[UUID(int=1)]] == [
        ("corporate_action", False),
        ("delisting", False),
        ("suspension", False),
    ]
    assert [(p.kind, p.blocked) for p in proofs[UUID(int=2)]] == [
        ("corporate_action", False),
        ("delisting", False),
        ("suspension", True),
    ]
    assert [r.url.path for r in requested].count("/dynamic/SymDir/nasdaqlisted.txt") == 1
    assert [r.url.path for r in requested].count("/rss.aspx") == 1
    assert sorted(
        r.url.params["symbols"] for r in requested if r.url.host == "data.alpaca.markets"
    ) == ["AAPL", "MSFT"]


@pytest.mark.parametrize("max_instruments", [5, 10, 30])
def test_batch_dispatches_once_at_configured_capacity(
    tmp_path, monkeypatch, max_instruments
):
    published = datetime.now(UTC) - timedelta(seconds=5)
    decision_session = published.astimezone(ZoneInfo("America/New_York")).date()
    _, halt, directory = _responses(published)
    requested = []

    async def respond(request):
        requested.append(request)
        if request.url.host == "data.alpaca.markets":
            return httpx.Response(200, json={"corporate_actions": {}, "next_page_token": None})
        return httpx.Response(200, content=halt if request.url.path == "/rss.aspx" else directory)

    original = httpx.AsyncClient

    def client(**kwargs):
        return original(**kwargs, transport=httpx.MockTransport(respond))

    monkeypatch.setattr(cli.httpx, "AsyncClient", client)
    candidates = tuple(
        cli.RiskCandidate(f"T{index:02d}", UUID(int=index + 1))
        for index in range(max_instruments)
    )
    proofs = asyncio.run(
        cli.collect_source_proofs_batch(
            candidates=candidates,
            max_instruments=max_instruments,
            decision_session=decision_session,
            key_id=SecretStr("synthetic-key"),
            secret=SecretStr("synthetic-secret"),
            output_dir=tmp_path / f"capacity-{max_instruments}",
            cutoff_at=None,
        )
    )

    assert len(proofs) == max_instruments
    assert [r.url.path for r in requested].count("/dynamic/SymDir/nasdaqlisted.txt") == 1
    assert [r.url.path for r in requested].count("/rss.aspx") == 1
    assert sum(r.url.host == "data.alpaca.markets" for r in requested) == max_instruments


@pytest.mark.parametrize(
    ("candidate_count", "max_instruments", "error_code"),
    [
        (6, 5, "AGENT_PAPER_PROVIDER_CAPACITY_EXCEEDED"),
        (1, 31, "AGENT_PAPER_PROVIDER_CAPACITY_INVALID"),
    ],
)
def test_batch_rejects_provider_capacity_before_any_source_dispatch(
    tmp_path,
    monkeypatch,
    candidate_count,
    max_instruments,
    error_code,
):
    dispatched = 0

    def forbidden_client(**_kwargs):
        nonlocal dispatched
        dispatched += 1
        raise AssertionError("source dispatch must not start")

    monkeypatch.setattr(cli.httpx, "AsyncClient", forbidden_client)
    candidates = tuple(
        cli.RiskCandidate(f"T{index}", UUID(int=index + 1))
        for index in range(candidate_count)
    )

    with pytest.raises(ValueError, match=f"^{error_code}$"):
        asyncio.run(
            cli.collect_source_proofs_batch(
                candidates=candidates,
                max_instruments=max_instruments,
                decision_session=date(2026, 9, 19),
                key_id=SecretStr("synthetic-key"),
                secret=SecretStr("synthetic-secret"),
                output_dir=tmp_path / "capacity",
                cutoff_at=None,
            )
        )

    assert dispatched == 0
