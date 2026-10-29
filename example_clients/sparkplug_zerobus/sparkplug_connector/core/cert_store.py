"""The certificates and keys the web page uploads (``web_ui.certs_dir``, default ``~/.sparkplug_connector/certs``).

A source's TLS paths (``security.ca_cert_path``, ``client_cert_path``, ``client_key_path``) may name a file here by its
bare name (no directory); ``resolve_tls_paths`` turns those into paths before the source starts (a bare name with no
file here stays as written: a path from the working directory, as before the store). Names: letters,
digits, ``_`` and ``-``, with at most one ``.`` that is neither first nor last (so no path can be built from one);
at most 1 MB a file; written atomically with mode 0600.
"""
from __future__ import annotations

import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

MAX_SIZE = 1024 * 1024
MAX_NAME_LENGTH = 128
NAME = re.compile(r"[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)?")
TLS_PATH_KEYS = ("ca_cert_path", "client_cert_path", "client_key_path")
DEFAULT_DIR = "~/.sparkplug_connector/certs"


class CertError(Exception):
    """A refused upload or delete; ``status`` is the HTTP status for it."""

    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class CertFile:
    name: str
    size: int
    modified: int           # epoch ms


def valid_name(name: Any) -> bool:
    return isinstance(name, str) and len(name) <= MAX_NAME_LENGTH and NAME.fullmatch(name) is not None


def certs_dir(config: dict[str, Any]) -> Path:
    """The store's directory from the configuration (``web_ui.certs_dir``)."""
    value = str(((config or {}).get("web_ui") or {}).get("certs_dir") or "").strip() or DEFAULT_DIR
    return Path(value).expanduser()


class CertStore:
    def __init__(self, directory: Path):
        self.dir = Path(directory).expanduser().resolve()

    def list(self) -> list[CertFile]:
        if not self.dir.is_dir():
            return []
        files = []
        for entry in self.dir.iterdir():
            if valid_name(entry.name) and entry.is_file():
                st = entry.stat()
                files.append(CertFile(entry.name, st.st_size, int(st.st_mtime * 1000)))
        return sorted(files, key=lambda f: f.name.lower())

    def path(self, name: str) -> Path:
        """The file for a valid name, inside the directory (CertError 400 otherwise, e.g. an entry that is a symlink
        out of it)."""
        if not valid_name(name):
            raise CertError(400, f"Invalid file name: {name!r}")
        path = (self.dir / name).resolve()
        if path.parent != self.dir:
            raise CertError(400, f"{name} is not a file of the store (a link out of {self.dir}?): change it there.")
        return path

    def save(self, name: str, content: bytes, overwrite: bool) -> CertFile:
        if not valid_name(name):
            raise CertError(400, "File names may only contain letters, digits, '_', '-' and a single '.'.")
        target = self.path(name)
        if target.exists() and not overwrite:
            raise CertError(409, f"A file named {name} already exists.")
        if len(content) > MAX_SIZE:
            raise CertError(413, "Files are limited to 1 MB.")
        if not content:
            raise CertError(400, "The file is empty.")
        self.dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd, tmp = tempfile.mkstemp(dir=self.dir, prefix=".upload-")
        try:
            os.chmod(tmp, 0o600)
            with os.fdopen(fd, "wb") as f:
                f.write(content)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, target)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise
        st = target.stat()
        return CertFile(name, st.st_size, int(st.st_mtime * 1000))

    def delete(self, name: str) -> bool:
        if not valid_name(name):
            return False
        try:
            self.path(name).unlink()
            return True
        except FileNotFoundError:
            return False


def used_by(sources: list[dict[str, Any]], name: str) -> list[str]:
    """The sources whose TLS settings name the file (by its bare name)."""
    return [str(s.get("name")) for s in sources or []
            if any((s.get("security") or {}).get(k) == name for k in TLS_PATH_KEYS)]


def resolve_tls_paths(source: dict[str, Any], directory: Path) -> dict[str, Any]:
    """The source with each TLS path that is a bare file name of a file in the store made its path there (others
    unchanged: ``ca.pem`` with no such file in the store stays ./ca.pem)."""
    security = source.get("security")
    if not isinstance(security, dict):
        return source
    resolved = dict(security)
    for key in TLS_PATH_KEYS:
        value = security.get(key)
        if isinstance(value, str) and valid_name(value.strip()):
            path = Path(directory).expanduser() / value.strip()
            if path.is_file():
                resolved[key] = str(path)
    return {**source, "security": resolved}
