"""TLS for the mock: a local CA (created once in cert_dir) and a server certificate issued on each start. Clients trust ca.pem."""

from __future__ import annotations

import datetime as dt
import ipaddress
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID


@dataclass(frozen=True)
class TlsFiles:
    ca_cert: Path
    server_cert: Path
    server_key: Path


def _is_ip(host: str) -> bool:
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        return False


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _builder(subject: x509.Name, issuer: x509.Name, key, days: int):
    return (x509.CertificateBuilder().subject_name(subject).issuer_name(issuer).public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(_now() - dt.timedelta(minutes=5))
            .not_valid_after(_now() + dt.timedelta(days=days)))


def ensure_tls(cert_dir: Path, host_names: Iterable[str]) -> TlsFiles:
    cert_dir = Path(cert_dir)
    cert_dir.mkdir(parents=True, exist_ok=True)
    files = TlsFiles(cert_dir / "ca.pem", cert_dir / "server.pem", cert_dir / "server.key")
    if files.ca_cert.exists():
        ca = x509.load_pem_x509_certificate(files.ca_cert.read_bytes())
        ca_key = serialization.load_pem_private_key((cert_dir / "ca.key").read_bytes(), password=None)
    else:
        ca_key = ec.generate_private_key(ec.SECP256R1())
        name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "zerobus-mock-server local test CA")])
        ca = (_builder(name, name, ca_key, 825)
              .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
              .add_extension(x509.KeyUsage(True, False, False, False, False, True, True, False, False),
                             critical=True)  # digital_signature, key_cert_sign, crl_sign
              .sign(ca_key, hashes.SHA256()))
        files.ca_cert.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
        (cert_dir / "ca.key").write_bytes(ca_key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))

    hosts = sorted(set(host_names) | {"localhost", "127.0.0.1"})
    sans = [x509.IPAddress(ipaddress.ip_address(h)) if _is_ip(h) else x509.DNSName(h) for h in hosts]
    key = ec.generate_private_key(ec.SECP256R1())
    cert = (_builder(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, hosts[0])]), ca.subject, key, 397)
            .add_extension(x509.SubjectAlternativeName(sans), critical=False)
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
            .sign(ca_key, hashes.SHA256()))
    files.server_cert.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    files.server_key.write_bytes(key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    return files
