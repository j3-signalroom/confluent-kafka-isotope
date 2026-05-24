#!/usr/bin/env bash
#
# Run the demo CLI against Confluent Cloud — single command per terminal.
# Pulls credentials via scripts/cc-cli-env.sh, then invokes `./gradlew
# :app:run` with the right `-D` flags. The arguments after the script
# name are forwarded to App.java as `--args`.
#
# Usage — pipeline-position verbs (recommended):
#   scripts/cc-app-run.sh place [PAYLOAD]   # produce to orders.placed
#   scripts/cc-app-run.sh enrich            # hop orders.placed   → orders.enriched
#   scripts/cc-app-run.sh fulfill           # hop orders.enriched → orders.fulfilled
#   scripts/cc-app-run.sh ship              # terminal-consume orders.fulfilled
#
# Generic verbs (raw App.java passthrough — for ad-hoc inspection or pipelines
# that don't fit the orders.* shape):
#   scripts/cc-app-run.sh send    <topic> <service> <payload>
#   scripts/cc-app-run.sh hop     <in-topic> <out-topic> <service>
#   scripts/cc-app-run.sh consume <topic> <service>
#   scripts/cc-app-run.sh sink    <topic>
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
    cat <<EOF
Usage: $(basename "$0") <verb> [args]

Pipeline-position verbs (recommended):
  place [PAYLOAD]   send orders.placed as order-intake-service (default payload: hello)
  enrich            hop  orders.placed   → orders.enriched   as order-enrichment-service
  fulfill           hop  orders.enriched → orders.fulfilled  as order-fulfillment-service
  ship              terminal-consume orders.fulfilled        as shipping-notification-service

Generic verbs (raw App.java passthrough):
  send    <topic> <service> <payload>
  hop     <in-topic> <out-topic> <service>
  consume <topic> <service>
  sink    <topic>

Example (full 4-terminal demo, in pipeline order):
  scripts/cc-app-run.sh place 'hello' # terminal A — kick the chain off
  scripts/cc-app-run.sh enrich    &   # terminal B
  scripts/cc-app-run.sh fulfill   &   # terminal C
  scripts/cc-app-run.sh ship      &   # terminal D — terminal consumer (emits marker)
EOF
    exit 2
fi

# Verb dispatch (place/enrich/fulfill/ship + send/hop/consume/sink) is
# handled inside App.java, so all positional args pass straight through to
# `--args` below — this script's only job is the CCAF auth flags.
#
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
