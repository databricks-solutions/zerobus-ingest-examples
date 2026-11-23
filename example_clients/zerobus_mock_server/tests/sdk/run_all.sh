#!/usr/bin/env bash
# SDK matrix: language x record format, against the mock, in Docker.
#
#   tests/sdk/run_all.sh                 # every language in matrix.conf
#   tests/sdk/run_all.sh python rust     # a subset
#
# For each language it builds the client image (tests/sdk/clients/<lang>/Dockerfile), runs
# the client's build step where the SDK is compiled from the monorepo source, then runs
# four scenarios per supported format (normal, abort, token401, mismatch). Each run resets
# the table, injects the scenario's fault through the mock's control API, runs the client,
# and checks the result with check.py. The matrix is printed at the end.
#
# Environment:
#   ZEROBUS_SDK_SRC  the zerobus-sdk monorepo checkout (sparse: rust, go, purego, cpp,
#                    typescript, java, python). Required.
#   ZB_RUN           scratch directory for the CA, results and build caches. Default:
#                    /tmp/zb-sdk-matrix. Nothing outside it is written.
#   PIP_INDEX_URL, NPM_CONFIG_REGISTRY, GOPROXY, MAVEN_MIRROR  package proxies used at image build.
#
# Exit status: 0 when every supported cell passes, 1 otherwise.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
PKG="$(cd "$HERE/../.." && pwd)"
SDK_SRC="${ZEROBUS_SDK_SRC:-}"
RUN="${ZB_RUN:-/tmp/zb-sdk-matrix}"
MOCK_NAME=zb-sdk-matrix-mock
MOCK_IMAGE=zerobus-mock-server:test
TABLE=main.default.sdk_matrix
N=20
FORMATS="json proto arrow avro"
SCENARIOS="${ZB_SCENARIOS:-normal abort token401 mismatch}"  # ZB_SCENARIOS=normal runs ingest only
PIP_INDEX_URL="${PIP_INDEX_URL:-https://pypi.org/simple}"
NPM_CONFIG_REGISTRY="${NPM_CONFIG_REGISTRY:-https://registry.npmjs.org}"
GOPROXY="${GOPROXY:-https://proxy.golang.org}"
MAVEN_MIRROR="${MAVEN_MIRROR:-${MVNW_REPOURL:+${MVNW_REPOURL%/}/maven2}}"  # else MVNW_REPOURL/maven2, as mock_lib/run_tests.sh
MAVEN_MIRROR="${MAVEN_MIRROR:-https://repo.maven.apache.org/maven2}"

mkdir -p "$RUN/res" "$RUN/tls" "$RUN/cache"
MOCK_URL="https://localhost:18543"
CA="$RUN/tls/ca.pem"

cleanup() { docker rm -f "$MOCK_NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT

die() { echo "error: $*" >&2; exit 2; }
[ -n "$SDK_SRC" ] || die "set ZEROBUS_SDK_SRC to a checkout of https://github.com/databricks/zerobus-sdk (git clone --depth 1 https://github.com/databricks/zerobus-sdk.git)"
[ -d "$SDK_SRC" ] || die "SDK source not found: $SDK_SRC (set ZEROBUS_SDK_SRC)"

# ---------------------------------------------------------------- mock

if ! docker image inspect "$MOCK_IMAGE" >/dev/null 2>&1; then
  (cd "$PKG" && docker build --network host --target test \
     --build-arg PIP_INDEX_URL="$PIP_INDEX_URL" -t "$MOCK_IMAGE" .) >"$RUN/build-mock.log" 2>&1 || die "mock image build failed (see $RUN/build-mock.log)"
fi
cleanup
# The mock reads its config from a file: write it into the run directory and mount that.
cp "$HERE/mock-config.yaml" "$RUN/mock-config.yaml"
docker run -d --name "$MOCK_NAME" --network host -v "$RUN:/run" --entrypoint python "$MOCK_IMAGE" \
  -m zerobus_mock_server --config /run/mock-config.yaml >/dev/null || die "could not start the mock"
for _ in $(seq 1 60); do
  curl -s --cacert "$CA" "$MOCK_URL/_mock/health" >/dev/null 2>&1 && break
  sleep 1
done
curl -s --cacert "$CA" "$MOCK_URL/_mock/health" >/dev/null 2>&1 || die "the mock did not answer (docker logs $MOCK_NAME)"

api() { curl -s --cacert "$CA" -H 'content-type: application/json' "$@"; }

# ---------------------------------------------------------------- matrix

declare -A CELL
LANGS="${*:-$(awk '!/^#/ && NF {print $1}' "$HERE/matrix.conf")}"

supported() { # lang format -> 0 if listed in matrix.conf
  awk -v l="$1" -v f="$2" '!/^#/ && $1==l { for (i=2;i<=NF;i++) if ($i==f) found=1 } END { exit !found }' "$HERE/matrix.conf"
}

build_client() { # lang -> builds image and compiles the client when needed
  local lang="$1" dir="$HERE/clients/$1"
  docker build --network host --build-arg PIP_INDEX_URL="$PIP_INDEX_URL" \
    --build-arg NPM_CONFIG_REGISTRY="$NPM_CONFIG_REGISTRY" --build-arg GOPROXY="$GOPROXY" \
    --build-arg MAVEN_MIRROR="$MAVEN_MIRROR" \
    -t "zb-sdk-$lang" "$dir" >"$RUN/build-$lang.log" 2>&1 || return 1
  if [ -f "$dir/vendor.py" ]; then
    mkdir -p "$RUN/cache/$lang/vendor"
    docker run --rm --network host -v "$SDK_SRC:/sdk:ro" -v "$RUN/cache/$lang:/cache" \
      -v "$dir:/client:ro" python:3.12-slim python /client/vendor.py /sdk/rust/Cargo.lock /cache/vendor \
      >>"$RUN/build-$lang.log" 2>&1 || return 1
  fi
  if [ -f "$dir/fixtures.py" ]; then
    # Fixtures are generated with pyarrow in the Python client image (zb-sdk-python).
    docker image inspect zb-sdk-python >/dev/null 2>&1 || docker build --network host \
      --build-arg PIP_INDEX_URL="$PIP_INDEX_URL" -t zb-sdk-python "$HERE/clients/python" >"$RUN/build-python.log" 2>&1 || return 1
    mkdir -p "$RUN/cache/$lang"
    docker run --rm -v "$dir:/client:ro" -v "$RUN/cache/$lang:/cache" --entrypoint python zb-sdk-python \
      /client/fixtures.py /cache/fixtures >>"$RUN/build-$lang.log" 2>&1 || return 1
  fi
  if [ -f "$dir/build.sh" ]; then
    mkdir -p "$RUN/cache/$lang"
    docker run --rm --network host -e GOPROXY="$GOPROXY" -e NPM_CONFIG_REGISTRY="$NPM_CONFIG_REGISTRY" \
      -v "$SDK_SRC:/sdk:ro" -v "$RUN/cache/$lang:/cache" \
      -v "$HERE/clients/$lang:/client:ro" --entrypoint /client/build.sh "zb-sdk-$lang" \
      >>"$RUN/build-$lang.log" 2>&1 || return 1
  fi
}

run_cell() { # lang format -> sets CELL["lang/fmt"] to PASS or FAIL; prints per-scenario lines
  local lang="$1" fmt="$2" verdict=PASS line rc
  for sc in $SCENARIOS; do
    api -X POST "$MOCK_URL/_mock/reset" -d "{\"tables\": [\"$TABLE\"]}" >/dev/null
    case "$sc" in
      abort)    api -X POST "$MOCK_URL/_mock/fail-next" -d "{\"scenario\": \"stream-abort-retryable\", \"after\": 2, \"table\": \"$TABLE\"}" >/dev/null ;;
      token401) api -X POST "$MOCK_URL/_mock/fail-next" -d "{\"scenario\": \"auth-failure\", \"table\": \"$TABLE\"}" >/dev/null ;;
    esac
    local res="$RUN/res/$lang-$fmt-$sc.out"
    : >"$res"
    timeout 600 docker run --rm --network host -e SSL_CERT_FILE=/tls/ca.pem \
      -v "$RUN/tls:/tls:ro" -v "$RUN/cache/$lang:/cache:ro" "zb-sdk-$lang" "$fmt" "$sc" --table "$TABLE" \
      --endpoint "https://localhost:15551" --uc "$MOCK_URL" \
      --client-id mock-client-id --client-secret mock-client-secret >"$res" 2>>"$RUN/res/$lang-$fmt-$sc.err"
    line=$(python3.12 "$HERE/check.py" --mock "$MOCK_URL" --cacert "$CA" --table "$TABLE" \
      --format "$fmt" --scenario "$sc" --n "$N" --result "$res" 2>&1)
    rc=$?
    echo "    $sc: $line"
    [ $rc -eq 0 ] || verdict=FAIL
  done
  CELL["$lang/$fmt"]="$verdict"
}

for lang in $LANGS; do
  if ! build_client "$lang"; then
    for fmt in $FORMATS; do CELL["$lang/$fmt"]="FAIL (image or client build; see $RUN/build-$lang.log)"; done
    echo "[$lang] build failed, see $RUN/build-$lang.log"
    continue
  fi
  for fmt in $FORMATS; do
    if ! supported "$lang" "$fmt"; then
      CELL["$lang/$fmt"]="not supported (SDK)"
      continue
    fi
    echo "[$lang/$fmt] running $SCENARIOS"
    run_cell "$lang" "$fmt"
  done
done

# ---------------------------------------------------------------- report

{
  printf '| language |'; for fmt in $FORMATS; do printf ' %s |' "$fmt"; done; printf '\n'
  printf '|---|'; for _ in $FORMATS; do printf -- '---|'; done; printf '\n'
  for lang in $LANGS; do
    row="| $lang"
    for fmt in $FORMATS; do row+=" | ${CELL["$lang/$fmt"]:-not run}"; done
    echo "$row |"
  done
} | tee "$RUN/matrix.md"

fails=0
for k in "${!CELL[@]}"; do
  case "${CELL[$k]}" in PASS|"not supported (SDK)") ;; *) fails=$((fails + 1)) ;; esac
done
echo "cells failing: $fails"
[ "$fails" -eq 0 ]
