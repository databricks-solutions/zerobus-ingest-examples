"""Local certificate authority and server certificate for the mock's TLS listeners.

The real SDKs only connect over TLS (the plaintext channel exists in the Rust
core behind a test-only feature that the Python and Java SDKs don't expose).
So the mock serves TLS with a certificate from a local CA. Clients trust that
CA by adding ``ca.pem`` to their trust store, for example by pointing
``SSL_CERT_FILE`` at a bundle that contains it. The CA is also served at
``GET /_mock/ca.pem``.

The server certificate is regenerated when the requested host names change.
"""

from __future__ import annotations

import datetime as _dt
import ipaddress
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID


@dataclass(frozen=True)
class TlsFiles:
    ca_cert: Path
    server_cert: Path
    server_key: Path


def _now() -> _dt.datetime:
    return _dt.datetime.now(_dt.timezone.utc)


def _san_names(cert: x509.Certificate) -> set:
    try:
        ext = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName)
    except x509.ExtensionNotFound:
        return set()
    names = set(ext.value.get_values_for_type(x509.DNSName))
    names |= {str(ip) for ip in ext.value.get_values_for_type(ipaddress.IPv4Address)}
    return names


def ensure_tls(cert_dir: Path, host_names: Iterable[str]) -> TlsFiles:
    cert_dir = Path(cert_dir)
    cert_dir.mkdir(parents=True, exist_ok=True)
    files = TlsFiles(
        ca_cert=cert_dir / "ca.pem",
        server_cert=cert_dir / "server.pem",
        server_key=cert_dir / "server.key",
    )
    hosts = sorted({h for h in host_names} | {"localhost", "127.0.0.1"})

    ca_key = None
    if not files.ca_cert.exists():
        ca_key = ec.generate_private_key(ec.SECP256R1())
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "zerobus-mock-server local test CA")])
        ca = (
            x509.CertificateBuilder()
            .subject_name(name)
            .issuer_name(name)
            .public_key(ca_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(_now() - _dt.timedelta(minutes=5))
            .not_valid_after(_now() + _dt.timedelta(days=825))
            .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
            .add_extension(x509.KeyUsage(
                digital_signature=True, key_cert_sign=True, crl_sign=True,
                content_commitment=False, key_encipherment=False, data_encipherment=False,
                key_agreement=False, encipher_only=False, decipher_only=False,
            ), critical=True)
            .sign(ca_key, hashes.SHA256())
        )
        files.ca_cert.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
        (cert_dir / "ca.key").write_bytes(
            ca_key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        )
    else:
        ca = x509.load_pem_x509_certificate(files.ca_cert.read_bytes())
        ca_key = serialization.load_pem_private_key(
            (cert_dir / "ca.key").read_bytes(), password=None
        )

    if files.server_cert.exists() and files.server_key.exists():
        current = x509.load_pem_x509_certificate(files.server_cert.read_bytes())
        if _san_names(current) >= set(hosts) and current.not_valid_after_utc > _now() + _dt.timedelta(days=1):
            return files

    key = ec.generate_private_key(ec.SECP256R1())
    sans = []
    for h in hosts:
        try:
            sans.append(x509.IPAddress(ipaddress.ip_address(h)))
        except ValueError:
            sans.append(x509.DNSName(h))
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, hosts[0])]))
        .issuer_name(ca.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(_now() - _dt.timedelta(minutes=5))
        .not_valid_after(_now() + _dt.timedelta(days=397))
        .add_extension(x509.SubjectAlternativeName(sans), critical=False)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.ExtendedKeyUsage([x509.oid.ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
        .sign(ca_key, hashes.SHA256())
    )
    files.server_cert.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    files.server_key.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return files
