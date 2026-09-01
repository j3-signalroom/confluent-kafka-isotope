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
#   scripts/cc-app-run.sh sink    <topic> [max-records]
#
# Self-checking test:
#   scripts/cc-app-run.sh verify-inband [SAMPLE]
#     Proves the in-band propagation model end to end on CCAF: reads a
#     bounded snapshot of orders.placed and orders.flink_enriched, then
#     asserts that traces present on both carry ONE more hop downstream,
#     and that the extra hop was written by the Flink collector rather
#     than by an interceptor. Exits non-zero on failure, so it works in
#     CI. See docs/flink-collector.md 1.0.
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
  sink    <topic> [max-records]

Test:
  verify-inband [SAMPLE]   assert the Flink collector appended a hop in-band
                           (default SAMPLE: 20 records per topic)

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

# ---------------------------------------------------------------------------
# verify-inband — the in-band propagation test.
#
# WHAT IT PROVES
# Two propagation models coexist in this project and emit the identical wire
# format (docs/flink-collector.md). Out-of-band, IsotopeProducerInterceptor
# stamps a hop from a ThreadLocal on send(). In-band, the Flink collector
# rehydrates the isotope from the record's OWN Kafka headers and appends a hop
# in SQL — which is the only model that can survive a shuffle, since a
# ThreadLocal cannot cross a serialization boundary into another TaskManager
# JVM.
#
# The assertion: for every trace present on BOTH topics, the copy on
# orders.flink_enriched must carry the same trace identity and exactly one MORE
# hop, whose service is the Flink collector. Same trace + incremented hop count
# is the proof the UDF appended to the inbound isotope rather than minting a
# fresh one.
#
# WHY A BOUNDED READ
# `sink` without a record count runs until Ctrl-C. The bound (and its idle
# drain) is what lets this terminate on its own and return an exit code.
#
# PREREQS: `make cc-flink-reports-up` has run, and traffic exists on
# orders.placed (scripts/cc-app-run.sh place hello). The collector is a 1:1
# projection with no window, so its output appears within seconds — no
# watermark wait, unlike the TUMBLE reports.
# ---------------------------------------------------------------------------
SOURCE_TOPIC="orders.placed"
COLLECTOR_TOPIC="orders.flink_enriched"
COLLECTOR_SERVICE="flink-enrich"

run_app() {
    # Same gradle invocation as the exec below, but callable more than once.
    ( cd "${REPO_ROOT}" && ./gradlew :app:run -q --console=plain \
        "-Dkafka.bootstrap=${BOOTSTRAP}" \
        "-Dkafka.security.protocol=SASL_SSL" \
        "-Dkafka.sasl.mechanism=PLAIN" \
        "-Dkafka.sasl.jaas.config=${JAAS}" \
        "-Dschema.registry.url=${SR_URL}" \
        "-Dschema.registry.basic.auth.user.info=${SR_KEY}:${SR_SECRET}" \
        --args="$*" )
}

verify_inband() {
    local sample="${1:-20}"
    local tmp; tmp="$(mktemp -d)"
    trap 'rm -rf "${tmp}"' RETURN

    echo "→ Reading up to ${sample} record(s) from ${SOURCE_TOPIC}..."
    run_app "sink ${SOURCE_TOPIC} ${sample}"    > "${tmp}/placed.txt"    2>&1 || true
    echo "→ Reading up to ${sample} record(s) from ${COLLECTOR_TOPIC}..."
    run_app "sink ${COLLECTOR_TOPIC} ${sample}" > "${tmp}/collected.txt" 2>&1 || true

    SOURCE_TOPIC="${SOURCE_TOPIC}" COLLECTOR_TOPIC="${COLLECTOR_TOPIC}" \
    COLLECTOR_SERVICE="${COLLECTOR_SERVICE}" \
    python3 - "${tmp}/placed.txt" "${tmp}/collected.txt" <<'PYEOF'
import os, re, sys

SRC   = os.environ["SOURCE_TOPIC"]
DST   = os.environ["COLLECTOR_TOPIC"]
SVC   = os.environ["COLLECTOR_SERVICE"]

def parse(path):
    """trace_id -> {'hops': int, 'last': (service, topic)} from `sink` output."""
    out, cur = {}, None
    for line in open(path, encoding="utf-8", errors="replace"):
        m = re.match(r"\s*trace_id : (\S+)", line)
        if m:
            cur = out.setdefault(m.group(1), {"hops": 0, "last": None})
            continue
        m = re.match(r"\s*hops     : (\d+)", line)
        if m and cur is not None:
            cur["hops"] = int(m.group(1))
            continue
        m = re.match(r"\s*\d+\. (\S+)\s+→ (\S+)", line)
        if m and cur is not None:
            cur["last"] = (m.group(1), m.group(2))
    return out

src, dst = parse(sys.argv[1]), parse(sys.argv[2])
print(f"\n  {SRC:<28} {len(src)} trace(s)")
print(f"  {DST:<28} {len(dst)} trace(s)")

if not src:
    sys.exit(f"\n✘ No traced records on {SRC}. Run: scripts/cc-app-run.sh place hello")
if not dst:
    sys.exit(f"\n✘ No records on {DST}. Is the collector INSERT running?\n"
             f"  Check the isotope-reports-1m statement in Confluent Cloud.")

both = sorted(set(src) & set(dst))
if not both:
    sys.exit(f"\n✘ No trace appears on both topics — widen the sample "
             f"(scripts/cc-app-run.sh verify-inband 200) or send fresh traffic.")

failures = []
for t in both:
    s, d = src[t], dst[t]
    if d["hops"] != s["hops"] + 1:
        failures.append(f"{t}: {s['hops']} hop(s) on {SRC} but {d['hops']} on {DST} (expected {s['hops'] + 1})")
    elif d["last"] is None or d["last"][0] != SVC or d["last"][1] != DST:
        failures.append(f"{t}: last hop is {d['last']}, expected ('{SVC}', '{DST}')")

print(f"\n  {len(both)} trace(s) on both topics:")
for t in both[:3]:
    print(f"    {t}  {src[t]['hops']} hop → {dst[t]['hops']} hops, appended by {dst[t]['last'][0]}")
if len(both) > 3:
    print(f"    … and {len(both) - 3} more")

if failures:
    print("\n✘ In-band propagation FAILED:")
    for f in failures:
        print(f"    {f}")
    sys.exit(1)

print(f"\n✔ In-band propagation verified — every shared trace kept its identity")
print(f"  across the shuffle and gained exactly one hop, written by '{SVC}'")
print(f"  in Flink SQL rather than by an interceptor.")
PYEOF
}

if [ "${1:-}" = "verify-inband" ]; then
    shift
    verify_inband "${1:-20}"
    exit $?
fi

# Split incoming args into JVM system-property flags (-D…) and App.java
# verbs/args. Leading -D flags are forwarded to gradle so the build can
# pass them through to the app JVM (app/build.gradle forwards keys under
# metrics./isotope./kafka./schema.) — this is how the metrics exporter is
# enabled on the CCAF path, e.g.:
#   scripts/cc-app-run.sh -Dmetrics.prometheus.enabled=true \
#     -Dmetrics.prometheus.port=9410 enrich
# Everything else is joined with spaces into the single `--args` string.
EXTRA_D=()
APP_ARGV=()
for arg in "$@"; do
    case "$arg" in
        -D*) EXTRA_D+=("$arg") ;;
        *)   APP_ARGV+=("$arg") ;;
    esac
done
APP_ARGS="${APP_ARGV[*]}"

cd "${REPO_ROOT}"
exec ./gradlew :app:run -q \
    "-Dkafka.bootstrap=${BOOTSTRAP}" \
    "-Dkafka.security.protocol=SASL_SSL" \
    "-Dkafka.sasl.mechanism=PLAIN" \
    "-Dkafka.sasl.jaas.config=${JAAS}" \
    "-Dschema.registry.url=${SR_URL}" \
    "-Dschema.registry.basic.auth.user.info=${SR_KEY}:${SR_SECRET}" \
    ${EXTRA_D[@]+"${EXTRA_D[@]}"} \
    --args="${APP_ARGS}"
