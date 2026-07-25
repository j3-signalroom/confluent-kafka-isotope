#!/usr/bin/env bash
# Deploy (or tear down) the seven isotope Flink reports as a single Flink 2.1
# **Confluent Manager for Apache Flink (CMF) Application** — one managed job that
# runs all seven reports, visible in CMF and Control Center's Flink tab.
#
# WHY AN APPLICATION (not CMF SQL statements)
# CMF's SQL-statement runtime (io.confluent.flink.FlinkCompiledPlanExecutor)
# ships only in the cp-flink-sql image, which exists only at Flink 1.19 — and two
# of the reports are ProcessTableFunctions, a Flink 2.x feature. Running the
# reports as an application on cp-flink 2.1 keeps everything on one Flink 2.1
# runtime, runs the PTFs natively, and reuses the existing open-source-dialect
# .fql verbatim. The application's entry point is IsotopeReportsJob (in the ptf
# module), which reads the .fql from the jar and runs all seven reports as one
# StatementSet. This replaced the former sql-client/session-cluster path.
#
# Flow (up):
#   1. pre-create source + sink Kafka topics
#   2. upload the shadow jar to CMF as a cmf:// artifact (MinIO-backed)
#   3. render + POST the FlinkApplication (image=$POOL_IMAGE, jarURI=cmf://…)
#   4. wait for the application job to reach RUNNING
#
# Usage: scripts/deploy-cmf-flink-reports.sh {up|down}
# Prereqs: make minio-up, cmf-install (2.4.x), cmf-env-create, flink-image-build,
#          and the app jar built (./gradlew :ptf:shadowJar).
set -euo pipefail

NAMESPACE="${NAMESPACE:-confluent}"
CMF_ENV="${CMF_ENV_NAME:-dev-local}"
APP_NAME="${APP_NAME:-isotope-reports}"
ARTIFACT_NAME="${APP_ARTIFACT_NAME:-isotope-reports-app}"
APP_MANIFEST="${APP_MANIFEST:-k8s/base/cmf-flink-application.json}"
APP_JAR="${APP_JAR:-ptf/build/libs/isotope-flink-udf.jar}"
POOL_IMAGE="${POOL_IMAGE:-isotope-cp-flink-sql:local}"
APP_FLINK_VERSION="${APP_FLINK_VERSION:-v2_1}"
MINIO_S3_ENDPOINT="${MINIO_S3_ENDPOINT:-http://minio.confluent.svc:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin123}"

CMF_API="http://localhost:18080/cmf/api/v1"
ENV_API="${CMF_API}/environments/${CMF_ENV}"

ACTION="${1:-up}"
[ "${ACTION}" = "up" ] || [ "${ACTION}" = "down" ] || { echo "Usage: $0 {up|down}" >&2; exit 2; }

EVENT_TOPICS=(orders.placed orders.enriched orders.fulfilled isotope_consume_edge_markers)
SINK_TOPICS=(
    isotope_report_latency_1m isotope_report_topology_1m isotope_report_bipartite_topology_1m
    isotope_report_hop_distribution_1m isotope_report_coverage_1m
    isotope_report_stuck_trace_1m isotope_report_latency_percentiles_1m
)

KAFKA_POD=$(kubectl get pods -n "${NAMESPACE}" --no-headers -o custom-columns=":metadata.name" 2>/dev/null \
    | grep -E '^kafka-[0-9]+$' | head -1)
[ -n "${KAFKA_POD}" ] || { echo "✘ No Kafka broker pod in '${NAMESPACE}'. Run 'make cp-up'." >&2; exit 1; }

create_topic() { kubectl exec -n "${NAMESPACE}" "${KAFKA_POD}" -- kafka-topics --bootstrap-server localhost:9071 \
    --create --if-not-exists --topic "$1" --partitions 1 --replication-factor 1 >/dev/null; }
delete_topic() { kubectl exec -n "${NAMESPACE}" "${KAFKA_POD}" -- kafka-topics --bootstrap-server localhost:9071 \
    --delete --if-exists --topic "$1" >/dev/null 2>&1 || true; }

PF_PID=""
cmf_pf_start() {
    kubectl port-forward -n "${NAMESPACE}" svc/cmf-service 18080:80 >/dev/null 2>&1 &
    PF_PID=$!
    for _ in $(seq 1 20); do curl -sf -o /dev/null "${ENV_API}" 2>/dev/null && return 0; sleep 1; done
    echo "✘ CMF REST not reachable on localhost:18080." >&2; exit 1
}
cmf_pf_stop() { [ -n "${PF_PID}" ] && kill "${PF_PID}" 2>/dev/null || true; }
trap cmf_pf_stop EXIT

if [ "${ACTION}" = "up" ]; then
    [ -f "${APP_JAR}" ] || { echo "✘ ${APP_JAR} not found. Run './gradlew :ptf:shadowJar'." >&2; exit 1; }
    echo "→ Pre-creating ${#EVENT_TOPICS[@]} source + ${#SINK_TOPICS[@]} sink topics on ${KAFKA_POD}..."
    for t in "${EVENT_TOPICS[@]}" "${SINK_TOPICS[@]}"; do echo "  ↳ ${t}"; create_topic "${t}"; done

    cmf_pf_start

    echo "→ Uploading ${APP_JAR} as CMF artifact '${ARTIFACT_NAME}'..."
    printf '{"apiVersion":"cmf.confluent.io/v1","kind":"Artifact","metadata":{"name":"%s"},"spec":{}}' \
        "${ARTIFACT_NAME}" > /tmp/cmf-artifact.json
    UP=$(curl -s -w "\n%{http_code}" -X POST "${ENV_API}/artifacts" \
        -F "artifact=@/tmp/cmf-artifact.json;type=application/json" -F "file=@${APP_JAR}")
    CODE=$(printf '%s' "${UP}" | tail -1)
    if [ "${CODE}" != "201" ] && [ "${CODE}" != "200" ]; then
        echo "✘ artifact upload failed (HTTP ${CODE}): $(printf '%s' "${UP}" | head -1)"; exit 1
    fi
    VERSION=$(printf '%s' "${UP}" | sed '$d' | python3 -c "import sys,json;print(json.load(sys.stdin).get('status',{}).get('version',1))" 2>/dev/null || echo 1)
    echo "  ✔ artifact '${ARTIFACT_NAME}' version ${VERSION} → cmf://${CMF_ENV}/${ARTIFACT_NAME}?version=${VERSION}"

    echo "→ Deploying FlinkApplication '${APP_NAME}' (image=${POOL_IMAGE}, ${APP_FLINK_VERSION})..."
    APP_NAME="${APP_NAME}" POOL_IMAGE="${POOL_IMAGE}" APP_FLINK_VERSION="${APP_FLINK_VERSION}" \
        CMF_ENV_NAME="${CMF_ENV}" APP_ARTIFACT_NAME="${ARTIFACT_NAME}" APP_ARTIFACT_VERSION="${VERSION}" \
        MINIO_S3_ENDPOINT="${MINIO_S3_ENDPOINT}" MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY}" MINIO_SECRET_KEY="${MINIO_SECRET_KEY}" \
        envsubst < "${APP_MANIFEST}" > /tmp/cmf-app.json
    # Recreate for idempotency (spec/jar version may change between runs).
    curl -s -o /dev/null -X DELETE "${ENV_API}/applications/${APP_NAME}"; sleep 3
    CODE=$(curl -s -o /tmp/cmf-app-out.json -w "%{http_code}" -X POST "${ENV_API}/applications" \
        -H "Content-Type: application/json" --data @/tmp/cmf-app.json)
    if [ "${CODE}" != "200" ] && [ "${CODE}" != "201" ]; then
        echo "✘ application create failed (HTTP ${CODE}):"; cat /tmp/cmf-app-out.json; exit 1
    fi

    echo "→ Waiting for the application job to reach RUNNING (up to ~3m)..."
    for _ in $(seq 1 30); do
        STATE=$(curl -s "${ENV_API}/applications/${APP_NAME}" \
            | python3 -c "import sys,json;print(json.load(sys.stdin).get('status',{}).get('jobStatus',{}).get('state',''))" 2>/dev/null)
        [ "${STATE}" = "RUNNING" ] && { echo "✔ Application '${APP_NAME}' job is RUNNING (all 7 reports)."; break; }
        sleep 6
    done
    [ "${STATE:-}" = "RUNNING" ] || echo "⚠ Not RUNNING yet (last: ${STATE:-unknown}). Check: kubectl logs -n ${NAMESPACE} -l app=${APP_NAME}"
    echo "  Inspect: Control Center → Flink, or the CMF applications API."
else
    cmf_pf_start
    echo "→ Deleting FlinkApplication '${APP_NAME}'..."
    curl -s -o /dev/null -w "  delete app: HTTP %{http_code}\n" -X DELETE "${ENV_API}/applications/${APP_NAME}"
    echo "→ Deleting artifact '${ARTIFACT_NAME}'..."
    curl -s -o /dev/null -w "  delete artifact: HTTP %{http_code}\n" -X DELETE "${ENV_API}/artifacts/${ARTIFACT_NAME}"
    echo "→ Deleting ${#SINK_TOPICS[@]} sink topics..."
    for t in "${SINK_TOPICS[@]}"; do echo "  ↳ ${t}"; delete_topic "${t}"; done
    echo "✔ Reports application torn down."
fi
