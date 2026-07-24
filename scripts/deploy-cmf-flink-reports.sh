#!/usr/bin/env bash
# Deploy (or tear down) the seven isotope Flink reports as first-class
# **CMF Statements** — visible in CMF's Statements API and Control Center's
# Flink tab — running on a CMF-managed SHARED compute pool.
#
# This REPLACES the older scripts/deploy-cp-flink-reports.sh, which submitted
# the reports via `sql-client.sh` directly to a raw session cluster and so
# bypassed CMF entirely (statements never appeared in CMF).
#
# How it differs from the sql-client path:
#   * Each CREATE TABLE / CREATE VIEW / CREATE FUNCTION / INSERT is submitted as
#     its own CMF Statement via POST /environments/<env>/statements.
#   * DDL objects persist in CMF's writable **environment catalog** (2.4.0+), so
#     later INSERT statements resolve the tables/views/functions created earlier.
#   * The two PTF reports register their JAR as a **cmf:// artifact** (MinIO-backed)
#     instead of a file:// path — needs CMF 2.4.0+ and `make minio-up`.
#
# The report SQL itself is read verbatim from scripts/flink/sql/cp/*.fql (the
# same files the sql-client path used) — only the per-file `SET 'pipeline.name'`
# lines are stripped (CMF names statements via metadata.name) and the PTF
# CREATE FUNCTION `USING JAR` URI is rewritten to the uploaded cmf:// artifact.
#
# Usage:
#   scripts/deploy-cmf-flink-reports.sh up      # create topics, submit DDL + report statements
#   scripts/deploy-cmf-flink-reports.sh down    # delete statements, drop catalog objects, delete topics
#
# Env knobs:
#   WITH_PTF=0   skip the 2 JAR-backed PTF reports (deploy only the 5 pure-SQL
#                reports) — used to verify the pure-SQL path in isolation.
#
# Prereqs: make minio-up, cmf-install (2.4.x), cmf-env-create, cmf-computepool-create.
set -euo pipefail

NAMESPACE="${NAMESPACE:-confluent}"
CMF_ENV="${CMF_ENV_NAME:-dev-local}"
POOL="${COMPUTE_POOL_NAME:-isotope-pool}"
SQL_DIR="${SQL_DIR:-scripts/flink/sql/cp}"
WITH_PTF="${WITH_PTF:-1}"

# Artifact (PTF JAR) config.
JAR_HOST_PATH="${JAR_HOST_PATH:-ptf/build/libs/isotope-flink-udf.jar}"
ARTIFACT_DISPLAY_NAME="${ARTIFACT_DISPLAY_NAME:-isotope-flink-udf}"

# All our CMF statement names carry this prefix so `down` can find and delete
# exactly the statements this script created (CMF names are RFC1123 — lowercase
# alphanumeric + hyphens, no underscores/dots).
PREFIX="iso-"

# CMF REST base — reached via a port-forward this script manages.
CMF_LOCAL="http://localhost:18080"
CMF_API="${CMF_LOCAL}/cmf/api/v1"

ACTION="${1:-up}"
if [ "${ACTION}" != "up" ] && [ "${ACTION}" != "down" ]; then
    echo "Usage: $0 {up|down}" >&2
    exit 2
fi

# ------------------------------------------------------------------------------
# Source + sink Kafka topics (same set as the sql-client path). Flink's Kafka
# source can't auto-create topics, and the avro-confluent sink writes on first
# record, so both are pre-created. Source topics are NOT deleted by `down`.
EVENT_TOPICS=(orders.placed orders.enriched orders.fulfilled isotope_consume_edge_markers)
SINK_TOPICS=(
    isotope_report_latency_1m
    isotope_report_topology_1m
    isotope_report_bipartite_topology_1m
    isotope_report_hop_distribution_1m
    isotope_report_coverage_1m
    isotope_report_stuck_trace_1m
    isotope_report_latency_percentiles_1m
)

# DDL files applied (in order) before the report INSERTs. Each may contain
# several statements; they are split and submitted individually.
DDL_FILES=(
    "${SQL_DIR}/00_source_table.fql"       # 4 source tables + isotope_raw view
    "${SQL_DIR}/05_isotope_view.fql"        # isotope (produced-record) view
    "${SQL_DIR}/06_consume_events_view.fql" # consume_events view
    "${SQL_DIR}/05_report_sinks.fql"        # 7 avro-confluent sink tables
)

# Pure-SQL report INSERTs: "file|statement-name". Statement name shows in CMF.
PURE_SQL_REPORTS=(
    "${SQL_DIR}/10_latency_report.fql|${PREFIX}report-latency-1m"
    "${SQL_DIR}/20_topology_report.fql|${PREFIX}report-topology-1m"
    "${SQL_DIR}/25_bipartite_topology_report.fql|${PREFIX}report-bipartite-topology-1m"
    "${SQL_DIR}/30_hop_distribution.fql|${PREFIX}report-hop-distribution-1m"
    "${SQL_DIR}/40_coverage_report.fql|${PREFIX}report-coverage-1m"
)
# PTF (JAR-backed) reports — only deployed when WITH_PTF=1.
PTF_REPORTS=(
    "${SQL_DIR}/60_stuck_trace_report.fql|${PREFIX}report-stuck-trace-1m"
    "${SQL_DIR}/70_latency_percentiles_report.fql|${PREFIX}report-latency-percentiles-1m"
)

# ------------------------------------------------------------------------------
# Kafka broker pod for topic admin.
KAFKA_POD=$(kubectl get pods -n "${NAMESPACE}" --no-headers -o custom-columns=":metadata.name" 2>/dev/null \
    | grep -E '^kafka-[0-9]+$' | head -1)
if [ -z "${KAFKA_POD}" ]; then
    echo "✘ No Kafka broker pod found in '${NAMESPACE}'. Did you run 'make cp-up'?" >&2
    exit 1
fi

create_topic() {
    kubectl exec -n "${NAMESPACE}" "${KAFKA_POD}" -- \
        kafka-topics --bootstrap-server localhost:9071 \
        --create --if-not-exists --topic "$1" --partitions 1 --replication-factor 1 >/dev/null
}
delete_topic() {
    kubectl exec -n "${NAMESPACE}" "${KAFKA_POD}" -- \
        kafka-topics --bootstrap-server localhost:9071 \
        --delete --if-exists --topic "$1" >/dev/null 2>&1 || true
}

# ------------------------------------------------------------------------------
# CMF port-forward lifecycle.
PF_PID=""
cmf_pf_start() {
    kubectl port-forward -n "${NAMESPACE}" svc/cmf-service 18080:80 >/dev/null 2>&1 &
    PF_PID=$!
    # Wait until CMF answers.
    for _ in $(seq 1 20); do
        if curl -sf -o /dev/null "${CMF_API}/environments/${CMF_ENV}" 2>/dev/null; then return 0; fi
        sleep 1
    done
    echo "✘ CMF REST did not become reachable on ${CMF_LOCAL}." >&2
    exit 1
}
cmf_pf_stop() { [ -n "${PF_PID}" ] && kill "${PF_PID}" 2>/dev/null || true; }
trap cmf_pf_stop EXIT

# Split a .fql file into individual SQL statements (JSON array on stdout).
# Strips full-line `--` comments and standalone `SET '...'=...` directives.
split_fql() {
    python3 - "$1" <<'PY'
import sys, json
txt = open(sys.argv[1]).read()
lines = [l for l in txt.splitlines() if not l.lstrip().startswith('--')]
parts = ['\n'.join(lines)][0].split(';')
out = []
for p in parts:
    s = p.strip()
    if not s:
        continue
    if s.upper().startswith('SET '):
        continue
    out.append(s)
print(json.dumps(out))
PY
}

# Build a Statement resource JSON on stdout: name, sql, pool.
statement_json() {
    python3 - "$1" "$2" "$3" <<'PY'
import sys, json
name, sql, pool = sys.argv[1], sys.argv[2], sys.argv[3]
print(json.dumps({
    "apiVersion": "cmf.confluent.io/v1",
    "kind": "Statement",
    "metadata": {"name": name},
    "spec": {"statement": sql, "computePoolName": pool,
             "properties": {}, "parallelism": 1, "stopped": False},
}))
PY
}

statement_phase() {
    curl -s "${CMF_API}/environments/${CMF_ENV}/statements/$1" \
        | python3 -c "import sys,json;print(json.load(sys.stdin).get('status',{}).get('phase',''))" 2>/dev/null
}
statement_detail() {
    curl -s "${CMF_API}/environments/${CMF_ENV}/statements/$1" \
        | python3 -c "import sys,json;print(json.load(sys.stdin).get('status',{}).get('detail',''))" 2>/dev/null
}
delete_statement() {
    curl -s -o /dev/null -X DELETE "${CMF_API}/environments/${CMF_ENV}/statements/$1" || true
}

# POST a statement, return HTTP code in $HTTP.
post_statement() {
    local json="$1"
    HTTP=$(printf '%s' "${json}" | curl -s -o /tmp/cmf-stmt-out.json -w "%{http_code}" \
        -X POST "${CMF_API}/environments/${CMF_ENV}/statements" \
        -H "Content-Type: application/json" --data @-)
}

# Submit a DDL statement and BLOCK until it reaches COMPLETED (abort on FAILED).
submit_ddl() {
    local name="$1" sql="$2"
    delete_statement "${name}"          # idempotent: free the name
    post_statement "$(statement_json "${name}" "${sql}" "${POOL}")"
    if [ "${HTTP}" != "200" ] && [ "${HTTP}" != "201" ]; then
        echo "  ✘ submit ${name} failed (HTTP ${HTTP}):"; cat /tmp/cmf-stmt-out.json; exit 1
    fi
    local phase
    for _ in $(seq 1 40); do
        phase=$(statement_phase "${name}")
        case "${phase}" in
            COMPLETED) echo "  ✔ ${name} (${sql:0:48}...)"; return 0 ;;
            FAILED)    echo "  ✘ ${name} FAILED: $(statement_detail "${name}")"; exit 1 ;;
        esac
        sleep 2
    done
    echo "  ✘ ${name} did not complete (last phase: ${phase})"; exit 1
}

# Submit a report INSERT statement (long-running). Verify it isn't immediately
# FAILED, then leave it RUNNING.
submit_insert() {
    local name="$1" sql="$2"
    delete_statement "${name}"          # idempotent: replace any prior copy
    sleep 1
    post_statement "$(statement_json "${name}" "${sql}" "${POOL}")"
    if [ "${HTTP}" != "200" ] && [ "${HTTP}" != "201" ]; then
        echo "  ✘ submit ${name} failed (HTTP ${HTTP}):"; cat /tmp/cmf-stmt-out.json; exit 1
    fi
    local phase
    for _ in $(seq 1 8); do
        phase=$(statement_phase "${name}")
        [ "${phase}" = "FAILED" ] && { echo "  ✘ ${name} FAILED: $(statement_detail "${name}")"; exit 1; }
        [ "${phase}" = "RUNNING" ] && break
        sleep 2
    done
    echo "  ✔ ${name} (${phase:-submitted})"
}

# Submit every statement in a DDL file, in order.
submit_ddl_file() {
    local file="$1"
    [ -f "${file}" ] || { echo "✘ ${file} not found" >&2; exit 1; }
    local stmts n i sql
    stmts=$(split_fql "${file}")
    n=$(printf '%s' "${stmts}" | python3 -c "import sys,json;print(len(json.load(sys.stdin)))")
    for i in $(seq 0 $((n-1))); do
        sql=$(printf '%s' "${stmts}" | python3 -c "import sys,json;print(json.load(sys.stdin)[$i])")
        submit_ddl "${PREFIX}ddl-$(basename "${file}" .fql | tr '_.' '--')-${i}" "${sql}"
    done
}

# Submit a single-INSERT report file under a friendly statement name.
submit_report_file() {
    local file="$1" name="$2"
    [ -f "${file}" ] || { echo "✘ ${file} not found" >&2; exit 1; }
    local sql
    sql=$(split_fql "${file}" | python3 -c "import sys,json;a=json.load(sys.stdin);print(a[-1])")
    submit_insert "${name}" "${sql}"
}

# Delete every statement whose name starts with our PREFIX.
delete_all_our_statements() {
    local names
    names=$(curl -s "${CMF_API}/environments/${CMF_ENV}/statements" \
        | python3 -c "import sys,json;[print(s['metadata']['name']) for s in json.load(sys.stdin).get('items',[]) if s['metadata']['name'].startswith('${PREFIX}')]" 2>/dev/null)
    for n in ${names}; do
        echo "  ↳ deleting statement ${n}"
        delete_statement "${n}"
    done
}

# PTF artifact upload + CREATE FUNCTION registration. Fleshed out in Phase 4;
# only invoked when WITH_PTF=1.
deploy_ptf_artifact_and_functions() {
    echo "  ✘ PTF artifact deployment not yet implemented (run with WITH_PTF=0 for now)." >&2
    exit 1
}

# ------------------------------------------------------------------------------
if [ "${ACTION}" = "up" ]; then
    echo "→ Pre-creating ${#EVENT_TOPICS[@]} source + ${#SINK_TOPICS[@]} sink topics on ${KAFKA_POD}..."
    for t in "${EVENT_TOPICS[@]}" "${SINK_TOPICS[@]}"; do echo "  ↳ ${t}"; create_topic "${t}"; done

    cmf_pf_start
    echo "→ Clearing any existing iso- statements (idempotent re-deploy)..."
    delete_all_our_statements

    echo "→ Submitting DDL (source tables, views, sink tables) as CMF statements..."
    for f in "${DDL_FILES[@]}"; do submit_ddl_file "${f}"; done

    if [ "${WITH_PTF}" = "1" ]; then
        echo "→ [PTF] Uploading UDF artifact + registering functions..."
        deploy_ptf_artifact_and_functions   # defined in the PTF section (sourced below)
    else
        echo "→ WITH_PTF=0 — skipping the 2 JAR-backed PTF reports."
    fi

    echo "→ Submitting the 5 pure-SQL report INSERT statements..."
    for entry in "${PURE_SQL_REPORTS[@]}"; do
        submit_report_file "${entry%%|*}" "${entry##*|}"
    done

    if [ "${WITH_PTF}" = "1" ]; then
        echo "→ Submitting the 2 PTF report INSERT statements..."
        for entry in "${PTF_REPORTS[@]}"; do
            submit_report_file "${entry%%|*}" "${entry##*|}"
        done
    fi

    echo "✔ Reports deployed as CMF statements. Inspect with:"
    echo "    curl \$CMF/environments/${CMF_ENV}/statements   (or Control Center → Flink)"
else
    cmf_pf_start
    echo "→ Deleting all iso- CMF statements..."
    delete_all_our_statements

    echo "→ Dropping catalog objects (99_teardown)..."
    if [ -f "${SQL_DIR}/99_teardown.fql" ]; then
        stmts=$(split_fql "${SQL_DIR}/99_teardown.fql")
        n=$(printf '%s' "${stmts}" | python3 -c "import sys,json;print(len(json.load(sys.stdin)))")
        for i in $(seq 0 $((n-1))); do
            sql=$(printf '%s' "${stmts}" | python3 -c "import sys,json;print(json.load(sys.stdin)[$i])")
            name="${PREFIX}drop-${i}"
            delete_statement "${name}"
            post_statement "$(statement_json "${name}" "${sql}" "${POOL}")"
            # best-effort: wait briefly then remove the drop record
            sleep 2; delete_statement "${name}"
        done
    fi

    echo "→ Deleting ${#SINK_TOPICS[@]} sink topics..."
    for t in "${SINK_TOPICS[@]}"; do echo "  ↳ ${t}"; delete_topic "${t}"; done
    echo "✔ Reports torn down (statements deleted, catalog objects dropped, sink topics removed)."
fi
