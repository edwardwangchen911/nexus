import socket
import ssl
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import nexus_fosun_gateway.security.tls as tls_module
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from nexus_fosun_gateway.domain import GatewayError
from nexus_fosun_gateway.security.tls import (
    create_server_ssl_context,
    production_uvicorn_kwargs,
)
from nexus_fosun_gateway.settings import GatewaySettings


def test_production_uvicorn_requires_client_certificate(tmp_path: Path) -> None:
    """Changing CERT_REQUIRED would let a caller without the engine identity connect."""
    material = _write_ephemeral_mtls_material(tmp_path)
    settings = _production_settings(material)

    kwargs = production_uvicorn_kwargs(settings)

    assert kwargs == {
        "ssl_certfile": str(material["server_cert"]),
        "ssl_keyfile": str(material["server_key"]),
        "ssl_ca_certs": str(material["ca_cert"]),
        "ssl_cert_reqs": ssl.CERT_REQUIRED,
    }


def test_broker_listener_uses_only_its_dedicated_client_ca(tmp_path: Path) -> None:
    """A shared client-CA bundle would make Provider certificates valid for Broker TLS."""
    material = _write_ephemeral_mtls_material(tmp_path)
    settings = _production_settings(material)

    kwargs = production_uvicorn_kwargs(settings)

    assert kwargs["ssl_ca_certs"] == str(material["ca_cert"])


def test_market_listener_uses_only_its_dedicated_client_ca(tmp_path: Path) -> None:
    """The Provider listener must never trust the Broker client-CA domain."""
    material = _write_ephemeral_mtls_material(tmp_path)
    settings = _production_settings(material)

    kwargs = production_uvicorn_kwargs(settings, "market")

    assert kwargs["ssl_ca_certs"] == str(material["market_ca_cert"])


def test_production_declares_two_principal_bound_listeners(tmp_path: Path) -> None:
    """Collapsing production back to one listener would erase certificate isolation."""
    material = _write_ephemeral_mtls_material(tmp_path)
    settings = _production_settings(material)
    factory = getattr(tls_module, "production_listener_configs", lambda _: ())

    listeners = factory(settings)

    assert [
        (listener.principal, listener.port, listener.tls_kwargs["ssl_ca_certs"])
        for listener in listeners
    ] == [
        ("broker", 8081, str(material["ca_cert"])),
        ("market", 8082, str(material["market_ca_cert"])),
    ]


def test_production_rejects_same_client_ca_under_different_paths(tmp_path: Path) -> None:
    """Path aliases must not turn one trust root into both principal identities."""
    material = _write_ephemeral_mtls_material(tmp_path)
    broker_ca = tmp_path / "broker-client-ca.pem"
    market_ca = tmp_path / "market-client-ca.pem"
    broker_ca.write_bytes(material["ca_cert"].read_bytes())
    market_ca.write_bytes(material["ca_cert"].read_bytes())
    settings = _production_settings(material).model_copy(
        update={
            "server_broker_client_ca": broker_ca,
            "server_market_client_ca": market_ca,
        }
    )

    with pytest.raises(GatewayError) as exc_info:
        tls_module.production_listener_configs(settings)

    assert exc_info.value.code == "TLS_CONFIGURATION_INVALID"


def test_production_rejects_reissued_ca_using_same_public_key(tmp_path: Path) -> None:
    """A different certificate must not disguise reuse of the same CA key."""
    material = _write_ephemeral_mtls_material(tmp_path)
    settings = _production_settings(material).model_copy(
        update={"server_market_client_ca": material["alias_ca_cert"]}
    )

    with pytest.raises(GatewayError) as exc_info:
        tls_module.production_listener_configs(settings)

    assert exc_info.value.code == "TLS_CONFIGURATION_INVALID"


def test_market_listener_rejects_broker_client_certificate_before_hmac(
    tmp_path: Path,
) -> None:
    """A valid Broker certificate cannot cross into the market HMAC listener."""
    material = _write_ephemeral_mtls_material(tmp_path)
    listeners = {
        listener.principal: listener
        for listener in tls_module.production_listener_configs(_production_settings(material))
    }
    server_context = create_server_ssl_context(
        material["server_cert"],
        material["server_key"],
        Path(listeners["market"].tls_kwargs["ssl_ca_certs"]),
    )
    broker_context = ssl.create_default_context(cafile=str(material["ca_cert"]))
    broker_context.check_hostname = False
    broker_context.load_cert_chain(
        certfile=str(material["client_cert"]),
        keyfile=str(material["client_key"]),
    )
    server_socket, client_socket = socket.socketpair()
    server_errors: list[BaseException] = []

    def accept() -> None:
        try:
            with server_context.wrap_socket(server_socket, server_side=True):
                pass
        except BaseException as error:
            server_errors.append(error)

    thread = threading.Thread(target=accept)
    thread.start()
    try:
        with pytest.raises((ssl.SSLError, BrokenPipeError)):
            with broker_context.wrap_socket(
                client_socket,
                server_hostname="localhost",
            ) as peer:
                peer.sendall(b"signed-hmac-never-reaches-asgi")
                peer.recv(1)
    finally:
        client_socket.close()
        thread.join(timeout=2)
        server_socket.close()

    assert len(server_errors) == 1
    assert isinstance(server_errors[0], ssl.SSLError)
    assert not thread.is_alive()


def test_tls_handshake_without_client_certificate_fails(tmp_path: Path) -> None:
    """The generated production server context must reject a certificate-less peer."""
    material = _write_ephemeral_mtls_material(tmp_path)
    server_context = create_server_ssl_context(
        material["server_cert"], material["server_key"], material["ca_cert"]
    )
    client_context = ssl.create_default_context(cafile=str(material["ca_cert"]))
    client_context.check_hostname = False
    server_socket, client_socket = socket.socketpair()
    server_errors: list[BaseException] = []

    def accept() -> None:
        try:
            with server_context.wrap_socket(server_socket, server_side=True):
                pass
        except BaseException as error:
            server_errors.append(error)

    thread = threading.Thread(target=accept)
    thread.start()
    try:
        # With TLS 1.3 the server's certificate-required alert can reach the
        # client during handshake, read, or write. macOS may surface the last
        # case as BrokenPipeError after the server has already rejected it.
        with pytest.raises((ssl.SSLError, BrokenPipeError)):
            with client_context.wrap_socket(client_socket, server_hostname="localhost") as peer:
                peer.sendall(b"prove-client-auth")
                peer.recv(1)
    finally:
        client_socket.close()
        thread.join(timeout=2)
        server_socket.close()

    assert len(server_errors) == 1
    assert isinstance(server_errors[0], ssl.SSLError)
    assert not thread.is_alive()


def test_tls_handshake_with_ephemeral_client_certificate_succeeds(tmp_path: Path) -> None:
    """The same temporary CA must admit the intended client certificate."""
    material = _write_ephemeral_mtls_material(tmp_path)
    server_context = create_server_ssl_context(
        material["server_cert"], material["server_key"], material["ca_cert"]
    )
    client_context = ssl.create_default_context(cafile=str(material["ca_cert"]))
    client_context.check_hostname = False
    client_context.load_cert_chain(
        certfile=str(material["client_cert"]),
        keyfile=str(material["client_key"]),
    )
    server_socket, client_socket = socket.socketpair()
    server_errors: list[BaseException] = []

    def accept() -> None:
        try:
            with server_context.wrap_socket(server_socket, server_side=True) as peer:
                if peer.recv(1) == b"x":
                    peer.sendall(b"y")
        except BaseException as error:
            server_errors.append(error)

    thread = threading.Thread(target=accept)
    thread.start()
    try:
        with client_context.wrap_socket(client_socket, server_hostname="localhost") as peer:
            peer.sendall(b"x")
            assert peer.recv(1) == b"y"
    finally:
        client_socket.close()
        thread.join(timeout=2)
        server_socket.close()

    assert server_errors == []
    assert not thread.is_alive()


def _write_ephemeral_mtls_material(tmp_path: Path) -> dict[str, Path]:
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "NEXUS ephemeral CA")])
    now = datetime.now(UTC)
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .sign(ca_key, hashes.SHA256())
    )
    market_ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    market_ca_name = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, "NEXUS ephemeral market CA")]
    )
    market_ca_cert = (
        x509.CertificateBuilder()
        .subject_name(market_ca_name)
        .issuer_name(market_ca_name)
        .public_key(market_ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .sign(market_ca_key, hashes.SHA256())
    )
    alias_ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "NEXUS reissued broker CA")])
    alias_ca_cert = (
        x509.CertificateBuilder()
        .subject_name(alias_ca_name)
        .issuer_name(alias_ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .sign(ca_key, hashes.SHA256())
    )
    server_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    server_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    server_cert = (
        x509.CertificateBuilder()
        .subject_name(server_name)
        .issuer_name(ca_name)
        .public_key(server_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName("localhost")]),
            critical=False,
        )
        .add_extension(
            x509.ExtendedKeyUsage([x509.oid.ExtendedKeyUsageOID.SERVER_AUTH]),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )
    client_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    client_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "NEXUS engine client")])
    client_cert = (
        x509.CertificateBuilder()
        .subject_name(client_name)
        .issuer_name(ca_name)
        .public_key(client_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(
            x509.ExtendedKeyUsage([x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH]),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )
    paths = {
        "ca_cert": tmp_path / "ca.pem",
        "alias_ca_cert": tmp_path / "alias-ca.pem",
        "market_ca_cert": tmp_path / "market-ca.pem",
        "server_cert": tmp_path / "server.pem",
        "server_key": tmp_path / "server-key.pem",
        "client_cert": tmp_path / "client.pem",
        "client_key": tmp_path / "client-key.pem",
    }
    paths["ca_cert"].write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))
    paths["alias_ca_cert"].write_bytes(alias_ca_cert.public_bytes(serialization.Encoding.PEM))
    paths["market_ca_cert"].write_bytes(market_ca_cert.public_bytes(serialization.Encoding.PEM))
    paths["server_cert"].write_bytes(server_cert.public_bytes(serialization.Encoding.PEM))
    paths["server_key"].write_bytes(
        server_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    paths["client_cert"].write_bytes(client_cert.public_bytes(serialization.Encoding.PEM))
    paths["client_key"].write_bytes(
        client_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return paths


def _production_settings(material: dict[str, Path]) -> GatewaySettings:
    return GatewaySettings.model_validate(
        {
            "NEXUS_ENV": "production",
            "FOSUN_GATEWAY_CAPABILITY": "READ_ONLY",
            "FOSUN_GATEWAY_TRANSPORT_MODE": "read_only",
            "FOSUN_READ_ONLY_SCOPE_CONTRACT_VERSION": "v1",
            "FOSUN_READ_ONLY_SCOPE": "core",
            "FOSUN_GATEWAY_REPLAY_STORE_PATH": material["server_cert"].parent / "replay.sqlite3",
            "FOSUN_GATEWAY_HMAC_CURRENT_KEY_ID": "synthetic-current",
            "FOSUN_GATEWAY_HMAC_CURRENT_SECRET": "synthetic-current-secret-32-bytes",
            "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_KEY_ID": "synthetic-market-current",
            "FOSUN_MARKET_GATEWAY_HMAC_CURRENT_SECRET": (
                "synthetic-market-current-secret-32-bytes"
            ),
            "FOSUN_GATEWAY_SERVER_CERT": material["server_cert"],
            "FOSUN_GATEWAY_SERVER_KEY": material["server_key"],
            "FOSUN_GATEWAY_SERVER_BROKER_CLIENT_CA": material["ca_cert"],
            "FOSUN_GATEWAY_SERVER_MARKET_CLIENT_CA": material["market_ca_cert"],
            "FOSUN_SDK_ADMITTED": True,
            "FOSUN_ACTIVATION_PROBE_ARTIFACT": "/synthetic/fosun-probe.json",
            "FOSUN_ACTIVATION_PROBE_RECEIPT": "/synthetic/fosun-probe-receipt.json",
            "FOSUN_ACTIVATION_PROBE_PUBLISHER_PUBLIC_KEY_B64U_FILE": (
                "/synthetic/fosun-publisher-public-key.b64u"
            ),
            "FOSUN_ACTIVATION_CONTRACT_HASH_FILE": (
                "/synthetic/fosun-activation-contract.sha256"
            ),
            "FOSUN_PROVIDER_MANIFEST": "/synthetic/fosun-provider-manifest.json",
            "FSOPENAPI_BASE_URL": "https://openapi.fosunxcz.com",
            "FSOPENAPI_API_KEY": "synthetic-api-key",
            "FSOPENAPI_CLIENT_PRIVATE_KEY": "synthetic-private-key",
            "FSOPENAPI_SERVER_PUBLIC_KEY": "synthetic-public-key",
        }
    )
