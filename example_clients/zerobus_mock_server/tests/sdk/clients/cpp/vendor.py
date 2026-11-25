"""Vendor the crates of the SDK's Cargo.lock into a directory cargo can use offline.

Runs in the Python image (the Rust image has no curl). Each registry package in Cargo.lock is
downloaded from static.crates.io, checked against the lock checksum, and unpacked under
<cache>/vendor/<name>-<version>/ with the .cargo-checksum.json cargo expects.

    python vendor.py /sdk/rust/Cargo.lock /cache/vendor
"""

import hashlib
import io
import os
import re
import sys
import tarfile
import socket
import urllib.request

# Some hosts stall on IPv6 for crates.io; resolve IPv4 only.
_getaddrinfo = socket.getaddrinfo
socket.getaddrinfo = lambda host, port, family=0, *a, **k: _getaddrinfo(host, port, socket.AF_INET, *a, **k)


def packages(lock_path):
    text = open(lock_path, encoding="utf-8").read()
    for block in text.split("[[package]]")[1:]:
        name = re.search(r'^name = "([^"]+)"', block, re.M)
        ver = re.search(r'^version = "([^"]+)"', block, re.M)
        src = re.search(r'^source = "([^"]+)"', block, re.M)
        chk = re.search(r'^checksum = "([0-9a-f]+)"', block, re.M)
        if name and ver and src and chk and src.group(1).startswith("registry+"):
            yield name.group(1), ver.group(1), chk.group(1)


def main(lock_path, out):
    os.makedirs(out, exist_ok=True)
    done_marker = os.path.join(out, ".complete")
    if os.path.exists(done_marker):
        print("vendor: already complete")
        return 0
    count = 0
    for name, ver, checksum in packages(lock_path):
        dest = os.path.join(out, f"{name}-{ver}")
        if os.path.isdir(dest):
            continue
        url = f"https://static.crates.io/crates/{name}/{name}-{ver}.crate"
        data = urllib.request.urlopen(url, timeout=120).read()
        digest = hashlib.sha256(data).hexdigest()
        if digest != checksum:
            raise SystemExit(f"checksum mismatch for {name} {ver}")
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
            tar.extractall(out, filter="data")
        with open(os.path.join(dest, ".cargo-checksum.json"), "w", encoding="utf-8") as fh:
            fh.write('{"files":{},"package":"%s"}\n' % checksum)
        count += 1
    open(done_marker, "w").close()
    print(f"vendor: downloaded {count} crate(s) into {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], sys.argv[2]))
