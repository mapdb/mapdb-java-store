#!/usr/bin/env bash
# The baseline gate, runnable with no GitHub in the loop. Run before every
# push/merge; .github/workflows/ci.yml runs the same jobs on every push once
# the commit reaches the remote. Any failure fails the gate (set -e).
#
# The hosted gate runs the verify step on temurin 17 AND 21 (17 is the compiler
# release target, the newer LTS catches behavioural drift); locally it runs on
# whatever JDK is first on PATH — check `java -version` if the hosted matrix
# disagrees with a local pass.
#
# The `*IT.java` stress suites (tens of minutes, several GB of temp files) stay
# behind the integration-tests profile, mirroring the hosted schedule-only job:
# opt in with `ci/check.sh --integration`.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== verify (unit suites, assertions enabled) =="
# `clean` first: hosted CI builds from a fresh checkout, and a stale target/
# can fail the JAR content check below (or worse, pass it vacuously).
mvn -B --no-transfer-progress clean verify

echo "== package (licence and notice are in the JAR) =="
mvn -B --no-transfer-progress -DskipTests package
jar="$(ls target/mapdb-java-store-*.jar)"
for f in LICENSE-EPL-1.0.txt LICENSE-EDL-1.0.txt NOTICE.md; do
  unzip -l "$jar" | grep -q "META-INF/$f" \
    || { echo "missing from JAR: META-INF/$f"; exit 1; }
done

if [[ "${1:-}" == "--integration" ]]; then
  echo "== integration (the long-running suites) =="
  mvn -B --no-transfer-progress -P integration-tests verify
fi

echo "== gate PASSED =="
