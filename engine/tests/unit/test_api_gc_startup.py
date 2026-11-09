"""Only the dedicated API CLI may prepare the process GC graph."""

import pytest
from nexus import main


@pytest.mark.parametrize("enabled", [True, False])
def test_cli_freezes_once_before_serving_without_changing_gc_policy(monkeypatch, enabled):
    events = []
    app = object()
    monkeypatch.setattr(main, "_API_GC_PREPARED", False, raising=False)
    monkeypatch.setattr(main, "create_engine_app", lambda: (events.append("app"), app)[1])
    monkeypatch.setattr(main.gc, "isenabled", lambda: enabled)
    monkeypatch.setattr(main.gc, "collect", lambda: events.append("collect"))
    monkeypatch.setattr(main.gc, "freeze", lambda: events.append("freeze"))
    monkeypatch.setattr(main.gc, "disable", lambda: pytest.fail("must not disable GC"))
    monkeypatch.setattr(
        main.gc, "set_threshold", lambda *a: pytest.fail("must not tune thresholds")
    )

    def serve(value, **kwargs):
        assert value is app and kwargs == {"host": "0.0.0.0", "port": 8000}
        events.append("serve")

    monkeypatch.setattr(main.uvicorn, "run", serve)
    main.run()
    main.run()
    assert events == (
        ["app", "collect", "freeze", "serve", "app", "serve"]
        if enabled
        else ["app", "serve", "app", "serve"]
    )


@pytest.mark.asyncio
async def test_reusable_factory_does_not_freeze_the_callers_process(monkeypatch):
    from nexus.settings import EngineSettings

    called = []
    monkeypatch.setattr(main.gc, "freeze", lambda: called.append(True))
    monkeypatch.setattr(main, "_API_GC_PREPARED", False)
    app = main.create_engine_app(
        EngineSettings(),
        readiness_object_store=object(),
        diagnostic_artifact_reader=lambda uri: b"",
        scenario_evidence_store=object(),
    )
    assert not called and main._API_GC_PREPARED is False
    await app.state.session_factory.kw["bind"].dispose()
