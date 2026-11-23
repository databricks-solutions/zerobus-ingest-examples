#!/usr/bin/env bash
# Runs the contract check first (check_contract_version.py), then the unit tests of every mock library,
# one language after another, and prints a pass/fail table.
#
#   ./run_tests.sh                 all languages
#   ./run_tests.sh java go         only the named ones (python java typescript go rust)
#
# Each language builds its own pinned image with --network host and runs its tests in a container.
# Proxies come from the environment when set: MAVEN_MIRROR (else MVNW_REPOURL/maven2, else Maven Central),
# NPM_CONFIG_REGISTRY, GOPROXY, PIP_INDEX_URL (or UV_DEFAULT_INDEX). Logs go to a new temp directory.
set -u

HERE="$(cd "$(dirname "$0")" && pwd)"
LOGS="$(mktemp -d "${TMPDIR:-/tmp}/zerobus-mock-tests.XXXXXX")"

MAVEN_MIRROR="${MAVEN_MIRROR:-${MVNW_REPOURL:+${MVNW_REPOURL%/}/maven2}}"
MAVEN_MIRROR="${MAVEN_MIRROR:-https://repo.maven.apache.org/maven2}"
NPM_REGISTRY="${NPM_CONFIG_REGISTRY:-https://registry.npmjs.org/}"
GO_PROXY="${GOPROXY:-https://proxy.golang.org,direct}"
PIP_INDEX="${PIP_INDEX_URL:-${UV_DEFAULT_INDEX:-https://pypi.org/simple}}"

declare -A RESULT TESTS
ALL="python java typescript go rust"
WANTED="${*:-$ALL}"

# Runs one language: build (if any), then run; sets RESULT and TESTS for the table.
run_lang() {
  local lang="$1" log="$LOGS/$1.log" image="zbmock-$1:test" build_status=0 run_status=0
  echo "== $lang"
  case "$lang" in
    python)
      docker run --rm --network host -e PIP_INDEX_URL="$PIP_INDEX" -e PYTHONDONTWRITEBYTECODE=1 \
        -e PYTHONPATH=/zbmock_lib/python -v "$HERE:/zbmock_lib:ro" python:3.12-slim sh -c \
        'pip install -q pytest databricks-zerobus-ingest-sdk==1.11.0 >/dev/null 2>&1 || pip install -q pytest
         cd /zbmock_lib/python && python -m pytest -q -p no:cacheprovider tests' >"$log" 2>&1
      run_status=$?
      TESTS[$lang]="$(grep -Eo '[0-9]+ passed' "$log" | tail -1 | grep -Eo '[0-9]+')"
      ;;
    java)
      docker build --network host --build-arg MAVEN_MIRROR="$MAVEN_MIRROR" -f "$HERE/java/Dockerfile" -t "$image" "$HERE" >"$log" 2>&1 || build_status=$?
      if [ $build_status -eq 0 ]; then
        docker run --rm --network host "$image" >>"$log" 2>&1; run_status=$?
      fi
      TESTS[$lang]="$(grep -E '^\[INFO\] Tests run:' "$log" | tail -1 | sed -E 's/.*Tests run: ([0-9]+).*/\1/')"
      ;;
    typescript)
      docker build --network host --build-arg NPM_CONFIG_REGISTRY="$NPM_REGISTRY" -f "$HERE/typescript/Dockerfile" -t "$image" "$HERE" >"$log" 2>&1 || build_status=$?
      if [ $build_status -eq 0 ]; then
        docker run --rm --network host "$image" >>"$log" 2>&1; run_status=$?
      fi
      TESTS[$lang]="$(grep -E '^# pass ' "$log" | tail -1 | awk '{print $3}')"
      ;;
    go)
      docker build --network host --build-arg GOPROXY="$GO_PROXY" -f "$HERE/go/Dockerfile" -t "$image" "$HERE" >"$log" 2>&1 || build_status=$?
      if [ $build_status -eq 0 ]; then
        docker run --rm --network host "$image" >>"$log" 2>&1; run_status=$?
      fi
      TESTS[$lang]="$(grep -c -- '--- PASS:' "$log")"
      ;;
    rust)
      docker build --network host -f "$HERE/rust/Dockerfile" -t "$image" "$HERE" >"$log" 2>&1 || build_status=$?
      if [ $build_status -eq 0 ]; then
        docker run --rm --network host "$image" >>"$log" 2>&1; run_status=$?
      fi
      TESTS[$lang]="$(grep -E '^test result: ' "$log" | awk '{s+=$4} END {print s+0}')"
      ;;
    *)
      echo "unknown language: $lang" >&2; RESULT[$lang]="SKIP"; return ;;
  esac
  if [ $build_status -ne 0 ]; then
    RESULT[$lang]="FAIL (build)"
  elif [ $run_status -eq 0 ]; then
    RESULT[$lang]="PASS"
  else
    RESULT[$lang]="FAIL"
  fi
}

# Step 1: the contract check. Standard library only, so it runs on this host with python3 -I.
overall=0
echo "== contract"
if python3 -I "$HERE/check_contract_version.py" >"$LOGS/contract.log" 2>&1; then
  RESULT[contract]=PASS
else
  RESULT[contract]=FAIL
  overall=1
fi
cat "$LOGS/contract.log"

for lang in $WANTED; do
  run_lang "$lang"
  [ "${RESULT[$lang]}" = "PASS" ] || overall=1
done

echo
printf '%-12s %-14s %-8s %s\n' "language" "result" "tests" "log"
printf '%-12s %-14s %-8s %s\n' "--------" "------" "-----" "---"
printf '%-12s %-14s %-8s %s\n' "contract" "${RESULT[contract]}" "-" "$LOGS/contract.log"
for lang in $WANTED; do
  printf '%-12s %-14s %-8s %s\n' "$lang" "${RESULT[$lang]:-SKIP}" "${TESTS[$lang]:-?}" "$LOGS/$lang.log"
done
echo
if [ $overall -eq 0 ]; then echo "all languages passed"; else echo "some languages failed (logs above)"; fi
exit $overall
