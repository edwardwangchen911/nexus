"""Production mTLS configuration for the gateway server."""

import ssl
from pathlib import Path
from typing import TypedDict

from nexus_fosun_gateway.domain import GatewayError
from nexus_fosun_gateway.settings import Environment, GatewaySettings


class UvicornTlsKwargs(TypedDict):
    ssl_certfile: str
    ssl_keyfile: str
    ssl_ca_certs: str
    ssl_cert_reqs: ssl.VerifyMode


def production_uvicorn_kwargs(settings: GatewaySettings) -> UvicornTlsKwargs:
    """Return fail-closed Uvicorn server TLS arguments for production."""
    if settings.environment is not Environment.PRODUCTION:
        raise GatewayError(
            "TLS_CONFIGURATION_INVALID",
            "Production TLS settings were requested outside production.",
            status_code=503,
            next_action="REVIEW_DEPLOYMENT_CONFIG",
        )
    cert, key, ca = _required_paths(settings)
    return {
        "ssl_certfile": str(cert),
        "ssl_keyfile": str(key),
        "ssl_ca_certs": str(ca),
        "ssl_cert_reqs": ssl.CERT_REQUIRED,
    }


def create_server_ssl_context(
    certfile: Path,
    keyfile: Path,
    client_ca: Path,
) -> ssl.SSLContext:
    """Build the same client-certificate policy used by production Uvicorn."""
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.verify_mode = ssl.CERT_REQUIRED
    context.load_cert_chain(certfile=str(certfile), keyfile=str(keyfile))
    context.load_verify_locations(cafile=str(client_ca))
    return context


def _required_paths(settings: GatewaySettings) -> tuple[Path, Path, Path]:
    if (
        settings.server_cert is None
        or settings.server_key is None
        or settings.server_client_ca is None
    ):
        raise GatewayError(
            "TLS_CONFIGURATION_INVALID",
            "Production mTLS configuration is incomplete.",
            status_code=503,
            next_action="REVIEW_DEPLOYMENT_CONFIG",
        )
    return settings.server_cert, settings.server_key, settings.server_client_ca
