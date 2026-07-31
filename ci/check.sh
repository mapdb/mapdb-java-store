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
# Resolve the artifact explicitly and say which one is under test: a bare glob
# silently yields an empty or multi-line "$jar" (no jar yet, or a sources/tests
# jar alongside it), and the loop below then reports the FIRST licence file as
# missing from a JAR it never opened — a confusing failure that looks like a
# packaging regression.
mapfile -t jars < <(find target -maxdepth 1 -name 'mapdb-java-store-*.jar' | sort)
[[ ${#jars[@]} -eq 1 ]] \
  || { echo "expected exactly one jar in target/, found ${#jars[@]}: ${jars[*]-}"; exit 1; }
jar="${jars[0]}"
echo "   jar under test: $jar"
# Listing captured once: `unzip -l | grep -q` closes the pipe on the first match
# and, under `set -o pipefail`, an unzip killed by SIGPIPE fails the pipeline.
listing="$(unzip -l "$jar")"
for f in LICENSE-EPL-1.0.txt LICENSE-EDL-1.0.txt NOTICE.md; do
  grep -q "META-INF/$f" <<<"$listing" \
    || { echo "missing from JAR: META-INF/$f"; exit 1; }
done

if [[ "${1:-}" == "--integration" ]]; then
  echo "== integration (the long-running suites) =="
  mvn -B --no-transfer-progress -P integration-tests verify
fi

echo "== gate PASSED =="
