"""Controlled external SDK transport for real formal Worker integration tests.

Legacy fixtures still own response contents, failures and barriers. Only the SDK
process edge is replaced: stage authorization, SDK validation/cleanup, settlement
and public proof remain production code. Missing response bindings fail closed.
"""

from contextvars import ContextVar

from nexus.agents.codex_sdk import invoke_codex as _NATIVE_INVOKE

_OPTIONS = ContextVar("controlled_sdk_options", default=None)


def current_sdk_options():
    value = _OPTIONS.get()
    assert value is not None, "UNBOUND_INTEGRATION_SDK_OPTIONS"
    return value


def controlled_sdk(monkeypatch):
    import json
    from types import SimpleNamespace

    from nexus.agents import codex_sdk, codex_staged
    from unit.agents.test_codex_sdk import _sdk

    calls = []
    invoke = codex_sdk.invoke_codex

    async def invoke_with_options(payload, **kwargs):
        token = _OPTIONS.set(kwargs)
        try:
            return await invoke(payload, **kwargs)
        finally:
            _OPTIONS.reset(token)

    monkeypatch.setattr(codex_sdk, "invoke_codex", invoke_with_options)

    class PatchCapture:
        def setattr(self, *args):
            pass

    def create(config):
        client, thread, response, _ = _sdk(PatchCapture())
        sdk_close = client.close
        exited = [False]
        client._client._sync = SimpleNamespace(
            _proc=SimpleNamespace(
                poll=lambda: 0 if exited[0] else None,
                kill=lambda: exited.__setitem__(0, True),
            )
        )

        async def run(wire, **kwargs):
            assert codex_staged.invoke_codex is not _NATIVE_INVOKE, "UNBOUND_INTEGRATION_SDK_CLIENT"
            # Preserve the original deadline, schema and dispatch guard expected
            # by legacy response fixtures; the real SDK already consumed them.
            current = current_sdk_options()
            raw, usage = await codex_staged.invoke_codex(json.loads(wire), **current)
            response.final_response = raw
            response.usage.total.input_tokens = usage.input_tokens
            response.usage.total.output_tokens = usage.output_tokens
            calls.append((sdk_close, thread))
            return response

        async def close():
            exited[0] = True

        thread.run.side_effect = run
        client.close.side_effect = close
        return client

    monkeypatch.setattr(codex_sdk, "AsyncCodex", create)
    return calls
