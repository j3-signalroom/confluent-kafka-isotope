#!/usr/bin/env bash
# Deploy (or tear down) the Phase-1 + Phase-2 Flink reports against the
# CP Flink session cluster running on Minikube.
#
# Usage:
#   scripts/deploy-flink-reports.sh up       # build JAR, upload to JM, apply all FQL
#   scripts/deploy-flink-reports.sh down     # apply teardown FQL only
#
# Prereqs:
#   - Minikube + CP up (`make cp-up`)
#   - Flink session cluster up (`make flink-up`)
#   - Kafka port-forwards optional for this script itself, but you'll
#     want `make kafka-pf-up` running for the demo CLI to feed the reports.
set -euo pipefail

NAMESPACE=confluent
JAR_HOST_PATH=ptf/build/libs/isotope-flink-udf.jar
JAR_POD_PATH=/opt/flink/lib/isotope-flink-udf.jar

ACTION="${1:-up}"
if [ "${ACTION}" != "up" ] && [ "${ACTION}" != "down" ]; then
    echo "Usage: $0 {up|down}" >&2
    exit 2
fi

# Discover JM pod once.
JM_POD=$(kubectl get pods -n "${NAMESPACE}" -l component=jobmanager \
    --no-headers -o custom-columns=":metadata.name" 2>/dev/null | head -1)
if [ -z "${JM_POD}" ]; then
    echo "✘ No Flink JobManager pod found in '${NAMESPACE}'." >&2
    echo "  Did you run 'make flink-up'?" >&2
    exit 1
fi
echo "→ JobManager pod: ${JM_POD}"

## Copies a file into the JM pod without requiring `tar` (which the
## minimal Flink image lacks, so `kubectl cp` fails). Streams the bytes
## through `kubectl exec` instead.
copy_to_jm() {
    local local_path="$1"
    local pod_path="$2"
    kubectl exec -i -n "${NAMESPACE}" "${JM_POD}" -- \
        sh -c "cat > ${pod_path}" < "${local_path}"
}

apply_fql() {
    local f="$1"
    local base
    base=$(basename "$f")
    echo "→ Applying ${f} ..."
    copy_to_jm "$f" "/tmp/${base}"
    kubectl exec -n "${NAMESPACE}" "${JM_POD}" -- \
        bash -lc "/opt/flink/bin/sql-client.sh -f /tmp/${base}"
}

if [ "${ACTION}" = "up" ]; then
    # 1. Build the PTF/UDAF JAR if it isn't already there.
    if [ ! -f "${JAR_HOST_PATH}" ]; then
        echo "→ Building PTF shadow JAR..."
        ./gradlew :ptf:shadowJar -q
    fi

    # 2. Copy JAR to the JobManager pod (via cat-over-exec since the image
    # lacks `tar` and so `kubectl cp` doesn't work).
    echo "→ Copying $(basename "${JAR_HOST_PATH}") → ${JM_POD}:${JAR_POD_PATH}"
    copy_to_jm "${JAR_HOST_PATH}" "${JAR_POD_PATH}"

    # 3. Concatenate DDL into a single script and apply in one session.
    # Each `sql-client.sh -f` invocation gets a fresh in-memory catalog, so
    # cross-file references (e.g. `latency_report` depending on `isotope`
    # view) only resolve if they're all in the same session.
    # 60_stuck_trace_report.fql is intentionally excluded on CP Flink.
    # The PTF call uses Confluent's `name => TABLE foo PARTITION BY col`
    # syntax which CCAF accepts but Apache Flink 2.1.1's parser rejects
    # (parse error: "PARTITION" not expected after `=>`). The Java PTF
    # itself + the FQL are correct as written and will deploy on CCAF.
    UP_FILES=(
        flink/sql/cp/00_source_table.fql
        flink/sql/cp/01_register_functions.fql
        flink/sql/shared/05_isotope_view.fql
        flink/sql/shared/10_latency_report.fql
        flink/sql/shared/20_topology_report.fql
        flink/sql/shared/30_hop_distribution.fql
        flink/sql/shared/40_coverage_report.fql
        flink/sql/shared/70_latency_percentiles_report.fql
    )
    COMBINED=$(mktemp)
    trap 'rm -f "${COMBINED}"' EXIT
    for f in "${UP_FILES[@]}"; do
        printf -- '-- ===== %s =====\n' "$f" >> "${COMBINED}"
        cat "$f" >> "${COMBINED}"
        printf '\n' >> "${COMBINED}"
    done
    echo "→ Applying ${#UP_FILES[@]} FQL files as one session ..."
    copy_to_jm "${COMBINED}" /tmp/isotope-reports.fql
    kubectl exec -n "${NAMESPACE}" "${JM_POD}" -- \
        bash -lc "/opt/flink/bin/sql-client.sh -f /tmp/isotope-reports.fql"

    echo ""
    echo "✔ Reports registered (8/9 — stuck_trace_alerts is CCAF-only). Try interactively:"
    echo "    make flink-sql"
    echo "    Flink SQL> SELECT * FROM latency_report_1m;"
    echo "    Flink SQL> SELECT * FROM topology_report_1m;"
    echo "    Flink SQL> SELECT * FROM hop_distribution_1m;"
    echo "    Flink SQL> SELECT * FROM coverage_report_1m;"
    echo "    Flink SQL> SELECT * FROM latency_percentiles_flat_1m;"
    echo ""
    echo "Feed traffic via the demo CLI (any iso-* topic name works):"
    echo "    ./gradlew :app:run --args=\"send iso-start svc-A 'hello'\" -q"
else
    apply_fql flink/sql/cp/99_teardown.fql
    echo ""
    echo "✔ Reports torn down."
fi
