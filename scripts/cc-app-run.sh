#!/usr/bin/env bash
#
# Run the demo CLI against Confluent Cloud — single command per terminal.
# Pulls credentials via scripts/cc-cli-env.sh, then invokes `./gradlew
# :app:run` with the right `-D` flags. The arguments after the script
# name are forwarded to App.java as `--args`.
#
# Usage:
#   scripts/cc-app-run.sh sink iso_final
#   scripts/cc-app-run.sh hop iso_mid iso_final svc-C
#   scripts/cc-app-run.sh hop iso_start iso_mid svc-B
#   scripts/cc-app-run.sh send iso_start svc-A 'hello world'
#
# Prereqs:
#   - `make cc-flink-reports-up` has succeeded — Terraform owns both
#     the Kafka and SR API keys; cc-cli-env.sh pulls them from
#     `terraform output`. No manual key creation needed.
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [ $# -eq 0 ]; then
    echo "Usage: $(basename "$0") <send|hop|sink> <args...>"
    echo "Example: $(basename "$0") sink iso_final"
    exit 2
fi

# Source the env helper. It sets BOOTSTRAP, SR_URL, KAFKA_KEY,
# KAFKA_SECRET, SR_KEY, SR_SECRET, JAAS — and echoes a masked summary.
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/cc-cli-env.sh"

# Hard-fail if anything is missing rather than handing gradle empty -D
# values that would otherwise blow up at first network call.
for var in BOOTSTRAP SR_URL KAFKA_KEY KAFKA_SECRET SR_KEY SR_SECRET JAAS; do
    if [ -z "${!var:-}" ]; then
        echo "✘ \$${var} is empty after sourcing scripts/cc-cli-env.sh."
        echo "  Set SR_KEY/SR_SECRET in your shell, or re-run 'make cc-flink-reports-up'."
        exit 1
    fi
done

# Join the args ($@) with spaces for App.java's main() — `--args` is a
# single space-separated string in gradle's JavaExec form.
APP_ARGS="$*"

cd "${REPO_ROOT}"
exec ./gradlew :app:run -q \
    "-Dkafka.bootstrap=${BOOTSTRAP}" \
    "-Dkafka.security.protocol=SASL_SSL" \
    "-Dkafka.sasl.mechanism=PLAIN" \
    "-Dkafka.sasl.jaas.config=${JAAS}" \
    "-Dschema.registry.url=${SR_URL}" \
    "-Dschema.registry.basic.auth.user.info=${SR_KEY}:${SR_SECRET}" \
    --args="${APP_ARGS}"
