"""Verdict for one language x format x scenario cell of the SDK matrix.

Runs on the host with the standard library only. It reads the client's result line and
the mock's rows (control API) and prints PASS or FAIL with the reason. Exit code 0 is PASS.

Scenarios (N records per run; record 5 carries an extra "region" field in "mismatch"):

  normal    the SDK reports success; N distinct rows are stored.
  abort     a retryable mid-stream abort is injected after two submissions. The SDK
            reconnects and resends; all N distinct rows are stored.
  token401  the token endpoint answers 401 invalid_client. The SDK reports an error that
            names it, and nothing is stored.
  mismatch  a field that is not a column of the table (4044). JSON rejects record 5, so
            records 0-4 are stored. Proto, Arrow and Avro refuse the stream at create,
            because the schema itself does not fit the table.

Usage:
  check.py --mock URL --cacert CA --table T --format F --scenario S --n N --result FILE
"""

from __future__ import annotations

import argparse
import json
import ssl
import sys
import urllib.parse
import urllib.request


def _get(base: str, ctx: ssl.SSLContext, path: str):
    with urllib.request.urlopen(base + path, context=ctx, timeout=30) as resp:
        return json.loads(resp.read().decode())


def verdict(args) -> tuple[bool, str]:
    ctx = ssl.create_default_context(cafile=args.cacert)
    try:
        with open(args.result, encoding="utf-8") as fh:
            lines = [ln for ln in fh.read().splitlines() if ln.startswith("ZB_RESULT ")]
        result = json.loads(lines[-1][len("ZB_RESULT "):]) if lines else {"ok": False, "error": "no result line from the client"}
    except (OSError, ValueError) as exc:
        result = {"ok": False, "error": f"client result unreadable: {exc}"}

    table = urllib.parse.quote(args.table, safe="")
    stored = _get(args.mock, ctx, f"/_mock/tables/{table}/rows?limit=10000")["rows"]
    rows = len(stored)
    distinct = len({r["values"].get("device_name") for r in stored})
    error = str(result.get("error") or "")
    ok = bool(result.get("ok"))
    n = args.n

    if args.scenario == "normal":
        good = ok and rows == n and distinct == n
        why = f"ok={ok} rows={rows} distinct={distinct} (want {n})"
    elif args.scenario == "abort":
        good = ok and distinct == n and rows >= n
        why = f"ok={ok} rows={rows} distinct={distinct}"
    elif args.scenario == "token401":
        good = (not ok) and ("invalid_client" in error or "401" in error) and rows == 0
        why = f"ok={ok} rows={rows} error={error[:160]!r}"
    elif args.scenario == "mismatch":
        want_rows = 5 if args.format == "json" else 0
        marked = ("FIELD_NOT_IN_TABLE" in error) or ("4044" in error) or ("does not fit" in error)
        good = (not ok) and marked and rows == want_rows
        why = f"ok={ok} rows={rows} (want {want_rows}) error={error[:160]!r}"
    else:
        good, why = False, f"unknown scenario {args.scenario}"
    return good, why


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mock", required=True)
    ap.add_argument("--cacert", required=True)
    ap.add_argument("--table", required=True)
    ap.add_argument("--format", required=True)
    ap.add_argument("--scenario", required=True)
    ap.add_argument("--n", type=int, default=20)
    ap.add_argument("--result", required=True)
    args = ap.parse_args(argv)
    good, why = verdict(args)
    print(("PASS" if good else "FAIL") + f" {args.format}/{args.scenario}: {why}")
    return 0 if good else 1


if __name__ == "__main__":
    sys.exit(main())
