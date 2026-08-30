"""Production mTLS configuration for the gateway server."""

import ssl
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, TypedDict

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization

from nexus_fosun_gateway.domain import GatewayError
from nexus_fosun_gateway.settings import (
    BROKER_LISTENER_PORT,
    MARKET_LISTENER_PORT,
    Environment,
    GatewaySettings,
)


class UvicornTlsKwargs(TypedDict):
    ssl_certfile: str
    ssl_keyfile: str
    ssl_ca_certs: str
    ssl_cert_reqs: ssl.VerifyMode


@dataclass(frozen=True)
class ProductionListenerConfig:
    principal: Literal["broker", "market"]
    port: int
    tls_kwargs: UvicornTlsKwargs


def production_listener_configs(
    settings: GatewaySettings,
) -> tuple[ProductionListenerConfig, ProductionListenerConfig]:
    """Return the two non-interchangeable production TLS listeners."""
    _validate_dedicated_client_cas(settings)
    return (
        ProductionListenerConfig(
            principal="broker",
            port=BROKER_LISTENER_PORT,
            tls_kwargs=_production_tls_kwargs(settings, "broker"),
        ),
        ProductionListenerConfig(
            principal="market",
            port=MARKET_LISTENER_PORT,
            tls_kwargs=_production_tls_kwargs(settings, "market"),
        ),
    )


def production_uvicorn_kwargs(
    settings: GatewaySettings,
    principal: Literal["broker", "market"] = "broker",
) -> UvicornTlsKwargs:
    """Return fail-closed Uvicorn server TLS arguments for production."""
    _validate_dedicated_client_cas(settings)
    return _production_tls_kwargs(settings, principal)


def _production_tls_kwargs(
    settings: GatewaySettings,
    principal: Literal["broker", "market"],
) -> UvicornTlsKwargs:
    if settings.environment is not Environment.PRODUCTION:
        raise _invalid_tls_configuration()
    cert, key, ca = _required_paths(settings, principal)
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


def _required_paths(
    settings: GatewaySettings,
    principal: Literal["broker", "market"],
) -> tuple[Path, Path, Path]:
    client_ca = (
        settings.server_broker_client_ca
        if principal == "broker"
        else settings.server_market_client_ca
    )
    if settings.server_cert is None or settings.server_key is None or client_ca is None:
        raise GatewayError(
            "TLS_CONFIGURATION_INVALID",
            "Production mTLS configuration is incomplete.",
            status_code=503,
            next_action="REVIEW_DEPLOYMENT_CONFIG",
        )
    return settings.server_cert, settings.server_key, client_ca


def _validate_dedicated_client_cas(settings: GatewaySettings) -> None:
    if settings.environment is not Environment.PRODUCTION:
        raise _invalid_tls_configuration()
    broker_ca = settings.server_broker_client_ca
    market_ca = settings.server_market_client_ca
    if broker_ca is None or market_ca is None:
        raise _invalid_tls_configuration()
    broker_certificate, broker_public_key = _single_ca_identity(broker_ca)
    market_certificate, market_public_key = _single_ca_identity(market_ca)
    if broker_certificate == market_certificate or broker_public_key == market_public_key:
        raise _invalid_tls_configuration()


def _single_ca_identity(path: Path) -> tuple[bytes, bytes]:
    try:
        with path.open("rb") as ca_file:
            data = ca_file.read(64 * 1024 + 1)
        if len(data) > 64 * 1024:
            raise ValueError
        certificates = x509.load_pem_x509_certificates(data)
        if len(certificates) != 1:
            raise ValueError
        certificate = certificates[0]
        constraint = certificate.extensions.get_extension_for_class(x509.BasicConstraints).value
        if not constraint.ca:
            raise ValueError
        public_key = certificate.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        return certificate.fingerprint(hashes.SHA256()), public_key
    except (OSError, ValueError, x509.ExtensionNotFound):
        raise _invalid_tls_configuration() from None


def _invalid_tls_configuration() -> GatewayError:
    return GatewayError(
        "TLS_CONFIGURATION_INVALID",
        "Production mTLS configuration is invalid.",
        status_code=503,
        next_action="REVIEW_DEPLOYMENT_CONFIG",
    )
