"""Normal CLI wiring and credential/output boundaries, entirely offline."""

import json
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from nexus_fosun_gateway.simulation_readonly_cli import (
    SimulationReadSettings,
    load_settings,
    main,
    restrict_sdk_requests,
)


@pytest.fixture(name="tmp_path")
def private_config_root():
    # CI basetemp is inside the checkout; real credential storage must not be.
    with TemporaryDirectory(prefix="nexus-simulation-test-") as root:
        yield Path(root).resolve()


def config(tmp_path: Path) -> Path:
    tmp_path.chmod(0o700)
    path = tmp_path / "provider.env"
    path.write_text(
        "FOSUN_SIMULATION_API_KEY=synthetic-key\n"
        "FOSUN_SIMULATION_CLIENT_PRIVATE_KEY=synthetic-private\n"
        "FOSUN_SIMULATION_SERVER_PUBLIC_KEY=synthetic-public\n"
        "FOSUN_SIMULATION_ACCOUNT_ID=synthetic-account\n"
    )
    path.chmod(0o600)
    return path


def test_configuration_never_falls_back_to_live_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("FSOPENAPI_API_KEY", "synthetic-live-secret")
    path = config(tmp_path)
    settings = load_settings(path)
    assert settings.api_key.get_secret_value() == "synthetic-key"
    assert "synthetic" not in repr(settings)
    assert settings.simulation_write_enabled is False
    assert settings.execution_locked is True
    path.write_text("")
    with pytest.raises(ValueError):
        load_settings(path)


@pytest.mark.parametrize(
    "field,value",
    [
        ("FOSUN_SIMULATION_WRITE_ENABLED", "true"),
        ("FOSUN_SIMULATION_EXECUTION_LOCKED", "false"),
    ],
)
def test_flags_cannot_enable_writes(tmp_path, field, value):
    path = config(tmp_path)
    with path.open("a") as file:
        file.write(f"{field}={value}\n")
    with pytest.raises(ValueError):
        load_settings(path)


def test_insecure_or_symlink_config_refused(tmp_path):
    path = config(tmp_path)
    path.chmod(0o644)
    with pytest.raises(ValueError):
        load_settings(path)
    path.chmod(0o600)
    link = tmp_path / "link.env"
    link.symlink_to(path)
    with pytest.raises(ValueError):
        load_settings(link)


def test_preflight_is_network_free_and_sanitized(tmp_path, capsys, monkeypatch):
    import nexus_fosun_gateway.simulation_readonly_cli as cli

    monkeypatch.setattr(cli, "build_reader", lambda *_: pytest.fail("preflight opened SDK"))
    assert main(["--config", str(config(tmp_path))]) == 0
    assert json.loads(capsys.readouterr().out)["network_attempted"] is False
    assert not (tmp_path / "reads").exists()
    assert main(["--config", str(tmp_path / "missing")]) == 2
    out = capsys.readouterr().out
    assert "synthetic" not in out and str(tmp_path) not in out


def test_low_level_sdk_guard_blocks_writes_wrong_accounts_and_generic_urls():
    class Client:
        def __init__(self):
            self.calls = []

        def _request(self, *args, **kwargs):
            self.calls.append((args, kwargs))
            return {"code": 0}

    client = Client()
    restrict_sdk_requests(client, "synthetic-account")
    for method, path, data in [
        ("POST", "/v1/trade/OrderCreate", {"subAccountId": "synthetic-account"}),
        ("POST", "/v1/account/SimAccountCreate", {}),
        ("POST", "/v1/account/SimAccountReset", {}),
        ("POST", "/v1/portfolio/CashSummary", {"subAccountId": "synthetic-real"}),
        ("POST", "https://invalid.example", {}),
        ("GET", "/v1/account/Accounts", {}),
        ("POST", "/v1/account/Accounts", {"clientId": 123}),
    ]:
        with pytest.raises(Exception, match="SIMULATION_OPERATION_REFUSED"):
            client._request(method, path, data=data)
    assert not client.calls
    client._request("POST", "/v1/account/Accounts", data={})
    assert len(client.calls) == 1


def test_normal_read_entry_persists_private_snapshot_without_printing_it(
    tmp_path,
    capsys,
    monkeypatch,
):
    from contextlib import contextmanager

    import nexus_fosun_gateway.simulation_readonly_cli as cli

    class Reader:
        async def collect(self, *_):
            return {
                "private_fact": "synthetic-account",
                "execution_locked": True,
                "simulation_write_enabled": False,
            }

    @contextmanager
    def build(settings: SimulationReadSettings):
        yield Reader()

    monkeypatch.setattr(cli, "build_reader", build)
    assert (
        main(
            [
                "--config",
                str(config(tmp_path)),
                "--read",
                "--date-from",
                "2026-09-01",
                "--date-to",
                "2026-09-17",
            ]
        )
        == 0
    )
    output = capsys.readouterr().out
    assert "synthetic-account" not in output
    result = json.loads(output)
    run_dir = tmp_path / "reads" / result["run_id"]
    assert (run_dir / "snapshot.json").stat().st_mode & 0o777 == 0o600
    assert run_dir.stat().st_mode & 0o777 == 0o700
    assert json.loads((run_dir / "status.json").read_text())["status"] == "read_completed"


def test_failed_read_records_safe_status_and_never_retries(tmp_path, capsys, monkeypatch):
    from contextlib import contextmanager

    import nexus_fosun_gateway.simulation_readonly_cli as cli

    calls = []

    class Reader:
        async def collect(self, *_):
            calls.append(1)
            raise RuntimeError("synthetic-sensitive-provider-response")

    @contextmanager
    def build(settings):
        yield Reader()

    monkeypatch.setattr(cli, "build_reader", build)
    assert (
        main(
            [
                "--config",
                str(config(tmp_path)),
                "--read",
                "--date-from",
                "2026-09-01",
                "--date-to",
                "2026-09-17",
            ]
        )
        == 2
    )
    output = capsys.readouterr().out
    assert "synthetic" not in output
    run_dir = tmp_path / "reads" / json.loads(output)["run_id"]
    assert not (run_dir / "snapshot.json").exists()
    assert "synthetic" not in (run_dir / "status.json").read_text()
    assert calls == [1]


def test_interruption_leaves_durable_unknown_dispatch_marker(tmp_path, monkeypatch):
    from contextlib import contextmanager

    import nexus_fosun_gateway.simulation_readonly_cli as cli

    class Reader:
        async def collect(self, *_):
            raise KeyboardInterrupt

    @contextmanager
    def build(settings):
        yield Reader()

    monkeypatch.setattr(cli, "build_reader", build)
    with pytest.raises(KeyboardInterrupt):
        main(
            [
                "--config",
                str(config(tmp_path)),
                "--read",
                "--date-from",
                "2026-09-01",
                "--date-to",
                "2026-09-17",
            ]
        )
    (run_dir,) = (tmp_path / "reads").iterdir()
    marker = json.loads((run_dir / "dispatching.json").read_text())
    assert marker["network_attempted"] is True
    assert marker["status"] == "dispatching"
    assert not (run_dir / "status.json").exists()


def test_http_budget_refuses_auth_replay_and_business_overrun():
    from nexus_fosun_gateway.simulation_readonly_cli import _HttpBudget

    budget = _HttpBudget()
    budget.before_dispatch("auth")
    with pytest.raises(Exception, match="SIMULATION_HTTP_BUDGET_EXCEEDED"):
        budget.before_dispatch("auth")
    for _ in range(9):
        budget.before_dispatch("business")
    with pytest.raises(Exception, match="SIMULATION_HTTP_BUDGET_EXCEEDED"):
        budget.before_dispatch("business")
