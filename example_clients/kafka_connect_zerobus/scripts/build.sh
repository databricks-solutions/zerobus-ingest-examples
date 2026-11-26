#!/usr/bin/env bash
# Builds the connector in a Maven container (Java 17), runs the unit tests, and stages the plugin jar for compose.
# Usage: scripts/build.sh [--skip-tests]
# Optional: MAVEN_SETTINGS=/path/to/settings.xml (a mirror), M2_CACHE=/path/to/cache (default: ~/.cache/zerobus-kafka-connect-m2).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
M2_CACHE="${M2_CACHE:-$HOME/.cache/zerobus-kafka-connect-m2}"
mkdir -p "$M2_CACHE"
GOAL="package"
[[ "${1:-}" == "--skip-tests" ]] && GOAL="package -DskipTests"
MOUNT_ARGS=()
MVN_ARGS=()
if [[ -n "${MAVEN_SETTINGS:-}" ]]; then
  MOUNT_ARGS=(-v "$MAVEN_SETTINGS:/mvn/settings.xml:ro")
  MVN_ARGS=(-s /mvn/settings.xml)
fi
docker run --rm --network host \
  -v "$ROOT:/src" -v "$M2_CACHE:/root/.m2/repository" "${MOUNT_ARGS[@]}" \
  -w /src maven:3-eclipse-temurin-17 \
  mvn -B -q "${MVN_ARGS[@]}" clean $GOAL
mkdir -p "$ROOT/compose/plugins/zerobus"
cp "$ROOT/target/zerobus-kafka-connect-sink.jar" "$ROOT/compose/plugins/zerobus/"
echo "staged $(ls -la "$ROOT/compose/plugins/zerobus/zerobus-kafka-connect-sink.jar" | awk '{print $5, $9}')"
