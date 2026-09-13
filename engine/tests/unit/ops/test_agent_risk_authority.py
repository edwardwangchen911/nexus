import asyncio
import json
import stat
from datetime import UTC, date, datetime, timedelta
from email.utils import format_datetime
from uuid import UUID
from zoneinfo import ZoneInfo

import httpx
import pytest
from nexus.ops import agent_risk_authority as cli
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
            )
        )

    assert (output / "nasdaqlisted.txt").read_bytes() == directory
    manifest = json.loads((output / "manifest.json").read_bytes())
    assert manifest["status"] == "failed"
    assert manifest["execution_locked"] is True


def test_cli_emits_only_bounded_result_and_never_requires_cycle_enablement(
    tmp_path, monkeypatch, capsys
) -> None:
    environment = {
        "DATABASE_URL": "postgresql+asyncpg://user:password@127.0.0.1:55433/nexus",
        "ALPACA_API_KEY_ID": "synthetic-key",
        "ALPACA_API_SECRET_KEY": "synthetic-secret",
    }
    seen = {}

    async def run_once(settings, *, decision_session, decision_time, output_dir, now=None):
        seen["database_url"] = settings.database_url
        seen["decision_session"] = decision_session
        seen["decision_time"] = decision_time
        seen["output_dir"] = output_dir
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
