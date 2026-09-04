"""Public CLI fails safely before settings/HTTP on invalid requests."""

import sys

import pytest


@pytest.mark.parametrize(
    "arguments",
    [
        ["--feed", "secret-canary"],
        ["--unknown", "secret-canary"],
        [],
    ],
)
def test_invalid_arguments_do_not_load_configuration(monkeypatch, capsys, arguments):
    from nexus.ops import shadow_price_query as cli

    def forbidden(*args, **kwargs):
        pytest.fail("Invalid input reached environment configuration")

    monkeypatch.setattr(cli, "load_local_environment", forbidden)
    monkeypatch.setattr(sys, "argv", ["shadow-price-query", *arguments])
    with pytest.raises(SystemExit) as result:
        cli.main()
    assert result.value.code == 2
    output = capsys.readouterr()
    assert "SHADOW_PRICE_REQUEST_INVALID" in output.err
    assert "secret-canary" not in output.err + output.out


def test_help_does_not_load_configuration(monkeypatch, capsys):
    from nexus.ops import shadow_price_query as cli

    monkeypatch.setattr(cli, "load_local_environment", lambda *args: pytest.fail("settings"))
    monkeypatch.setattr(sys, "argv", ["shadow-price-query", "--help"])
    with pytest.raises(SystemExit) as result:
        cli.main()
    assert result.value.code == 0
    assert "--shadow-group-id" in capsys.readouterr().out


@pytest.mark.asyncio
async def test_external_http_keeps_existing_environment_route(monkeypatch):
    from uuid import UUID

    from nexus.ops import shadow_price_query as cli

    settings = cli.ShadowPriceSettings.model_validate(
        {
            "DATABASE_URL": "postgresql+asyncpg://test:test@localhost/test",
            "OBJECT_STORE_ENDPOINT": "http://localhost:9000",
            "OBJECT_STORE_REGION": "us-east-1",
            "OBJECT_STORE_BUCKET": "test",
            "OBJECT_STORE_ACCESS_KEY_ID": "test",
            "OBJECT_STORE_SECRET_ACCESS_KEY": "test",
            "ALPACA_API_KEY_ID": "test",
            "ALPACA_API_SECRET_KEY": "test",
        }
    )

    class StopHere(Exception):
        pass

    def client(**kwargs):
        assert kwargs.get("trust_env", True) is True
        assert kwargs["follow_redirects"] is False
        raise StopHere

    monkeypatch.setattr(cli, "S3ProviderEvidenceStore", lambda *args, **kwargs: object())
    monkeypatch.setattr(cli.httpx, "AsyncClient", client)
    with pytest.raises(StopHere):
        await cli.run(
            settings,
            cli.ShadowPriceQueryRequest(
                shadow_group_id=UUID(int=1),
                session_date="2026-09-02",
                idempotency_key="test",
                feed="sip",
                delivery_mode="delayed",
            ),
        )


@pytest.mark.parametrize("defect", ["fixture_store", "remote_db", "redirect_db", "remote_store"])
def test_cli_configuration_rejects_fixture_or_redirected_services(defect):
    from nexus.ops import shadow_price_query as cli

    values = {
        "DATABASE_URL": "postgresql+asyncpg://test:test@localhost/test",
        "OBJECT_STORE_ENDPOINT": "http://localhost:9000",
        "OBJECT_STORE_REGION": "us-east-1",
        "OBJECT_STORE_BUCKET": "test",
        "OBJECT_STORE_ACCESS_KEY_ID": "test",
        "OBJECT_STORE_SECRET_ACCESS_KEY": "test",
        "ALPACA_API_KEY_ID": "test",
        "ALPACA_API_SECRET_KEY": "test",
    }
    if defect == "fixture_store":
        del values["OBJECT_STORE_REGION"]
    elif defect == "remote_db":
        values["DATABASE_URL"] = "postgresql+asyncpg://test:test@external.invalid/test"
    elif defect == "redirect_db":
        values["DATABASE_URL"] += "?hostaddr=external.invalid"
    else:
        values["OBJECT_STORE_ENDPOINT"] = "http://external.invalid:9000"
    with pytest.raises(ValueError, match="SHADOW_PRICE_CONFIG_INVALID"):
        cli.validate_local_config(cli.ShadowPriceSettings.model_validate(values))
