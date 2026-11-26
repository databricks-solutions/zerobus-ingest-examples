#!/usr/bin/env bash
# Runs the connector's unit tests (mvn test in a Maven container, Java 17) and prints the count.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
M2_CACHE="${M2_CACHE:-$HOME/.cache/zerobus-kafka-connect-m2}"
mkdir -p "$M2_CACHE"
MOUNT_ARGS=()
MVN_ARGS=()
if [[ -n "${MAVEN_SETTINGS:-}" ]]; then
  MOUNT_ARGS=(-v "$MAVEN_SETTINGS:/mvn/settings.xml:ro")
  MVN_ARGS=(-s /mvn/settings.xml)
fi
docker run --rm --network host \
  -v "$ROOT:/src" -v "$M2_CACHE:/root/.m2/repository" "${MOUNT_ARGS[@]}" \
  -w /src maven:3-eclipse-temurin-17 \
  mvn -B clean test "${MVN_ARGS[@]}" > "$ROOT/target-test.log" 2>&1 || { echo "unit tests FAILED (see $ROOT/target-test.log)"; exit 1; }
python3 - "$ROOT/target/surefire-reports" <<'PY'
import glob, re, sys
t = f = e = 0
for p in glob.glob(sys.argv[1] + "/TEST-*.xml"):
    m = re.search(r'<testsuite[^>]*', open(p).read()).group(0)
    g = lambda k: int(re.search(k + r'="(\d+)"', m).group(1))
    t += g("tests"); f += g("failures"); e += g("errors")
print("unit tests: %d run, %d failures, %d errors" % (t, f, e))
sys.exit(1 if f or e else 0)
PY
