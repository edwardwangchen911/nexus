from types import SimpleNamespace

import pytest
from nexus_fosun_gateway.security.provider_tls import enforce_sdk_tls_verification


def test_frozen_sdk_session_and_request_paths_force_certificate_verification() -> None:
    observed: list[tuple[str, object, object]] = []

    class FakeSession:
        def request(self, *_args: object, **kwargs: object) -> object:
            observed.append(
                ("request", kwargs.get("verify"), kwargs.get("allow_redirects"))
            )
            return object()

    class FakeRequests:
        class RequestException(Exception):
            pass

        def post(self, *_args: object, **kwargs: object) -> object:
            observed.append(
                ("session", kwargs.get("verify"), kwargs.get("allow_redirects"))
            )
            return object()

    client = SimpleNamespace(_http_session=FakeSession())
    auth_module = SimpleNamespace(requests=FakeRequests())

    enforce_sdk_tls_verification(client, auth_module)
    client._http_session.request("POST", "https://provider.invalid", verify=False)
    auth_module.requests.post("https://provider.invalid", verify=False)

    assert observed == [("request", True, False), ("session", True, False)]


@pytest.mark.parametrize(
    ("client", "auth_module"),
    [
        (SimpleNamespace(), SimpleNamespace(requests=object())),
        (SimpleNamespace(_http_session=object()), SimpleNamespace(requests=object())),
        (
            SimpleNamespace(_http_session=SimpleNamespace(request=lambda: None)),
            SimpleNamespace(),
        ),
        (
            SimpleNamespace(_http_session=SimpleNamespace(request=lambda: None)),
            SimpleNamespace(requests=object()),
        ),
    ],
)
def test_sdk_tls_remediation_rejects_any_subject_surface_drift(
    client: object,
    auth_module: object,
) -> None:
    with pytest.raises(RuntimeError, match="TLS verification surface"):
        enforce_sdk_tls_verification(client, auth_module)
