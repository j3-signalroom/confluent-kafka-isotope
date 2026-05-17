#!/usr/bin/env bash
# Deploy (or tear down) the 4 isotope reports as CMF-managed Statements.
#
# Architecture:
#   - CMF (Confluent Manager for Apache Flink) hosts each report as a
#     dedicated Application-mode Flink cluster (1 JM + 1 TM pod per
#     statement). Sink topics are auto-created; Protobuf schemas are
#     auto-registered in SR; Control Center renders the topics natively.
#   - The 5th report (latency_percentiles_flat_1m, UDAF-based) stays on
#     the open-source cp-flink session cluster because CMF disallows
#     user-defined functions. See scripts/deploy-flink-reports.sh.
#
# Usage:
#   scripts/deploy-cmf-reports.sh up      # create catalog/db/pool, ALTER iso-start, submit 4×(DDL,DML)
#   scripts/deploy-cmf-reports.sh down    # cancel statements, drop tables, delete topics + SR subjects
#
# Prereqs:
#   - Minikube + CP up      (`make cp-up`)
#   - CMF installed + env   (`make cmf-install && make cmf-env-create`)
set -euo pipefail

NAMESPACE=confluent
CMF_ENV=dev-local
CMF_CATALOG=iso-cat
CMF_DATABASE=iso-db
CMF_POOL=iso-pool

ACTION="${1:-up}"
if [ "${ACTION}" != "up" ] && [ "${ACTION}" != "down" ]; then
    echo "Usage: $0 {up|down}" >&2
    exit 2
fi

# Parallel arrays — each report has a name, a DDL file, and a DML file.
REPORTS=(latency topology hop-distribution coverage)
DDL_FILES=(
    flink/sql/cmf/10_latency_ddl.sql
    flink/sql/cmf/20_topology_ddl.sql
    flink/sql/cmf/30_hop_distribution_ddl.sql
    flink/sql/cmf/40_coverage_ddl.sql
)
DML_FILES=(
    flink/sql/cmf/10_latency_dml.sql
    flink/sql/cmf/20_topology_dml.sql
    flink/sql/cmf/30_hop_distribution_dml.sql
    flink/sql/cmf/40_coverage_dml.sql
)
SINK_TOPICS=(
    isotope-report-latency-1m
    isotope-report-topology-1m
    isotope-report-hop-distribution-1m
    isotope-report-coverage-1m
)

# Start a port-forward to CMF and ensure it's torn down on exit.
PF_PID=""
start_pf() {
    kubectl port-forward -n "${NAMESPACE}" svc/cmf-service 18080:80 >/dev/null 2>&1 &
    PF_PID=$!
    sleep 2
}
stop_pf() { [ -n "${PF_PID}" ] && kill "${PF_PID}" 2>/dev/null || true; }
trap stop_pf EXIT

CMF_URL="http://localhost:18080/cmf/api/v1"

# POST a JSON manifest. Prints the HTTP code. 200 and 409 are success.
post_json() {
    local url="$1"; local file="$2"
    curl -sS -o /tmp/cmf-resp.json -w "%{http_code}" \
        -X POST "${url}" -H "Content-Type: application/json" -d @"${file}"
}

# Build a Statement payload by reading a .sql file. Strips anything
# after the first ';' to comply with CMF's "only single statement"
# rule — comments at the end of a .sql file are fine but trailing
# DDL/DML in the same file would be silently dropped here.
write_statement_payload() {
    local name="$1"; local sql_file="$2"; local out="$3"
    python3 - <<PY
import json, sys
sql = open("${sql_file}").read()
# Keep only up to the first ';' (single-statement rule).
i = sql.find(';')
single = sql[:i+1] if i >= 0 else sql
payload = {
    "apiVersion": "cmf.confluent.io/v1",
    "kind": "Statement",
    "metadata": {"name": "${name}"},
    "spec": {
        "statement": single,
        "properties": {
            "sql.current-catalog": "${CMF_CATALOG}",
            "sql.current-database": "${CMF_DATABASE}"
        },
        "computePoolName": "${CMF_POOL}",
        "parallelism": 1
    }
}
open("${out}", "w").write(json.dumps(payload))
PY
}

submit_sql() {
    local name="$1"; local sql_file="$2"
    write_statement_payload "${name}" "${sql_file}" /tmp/cmf-stmt.json
    post_json "${CMF_URL}/environments/${CMF_ENV}/statements" /tmp/cmf-stmt.json
}

# True idempotent submit: if a Statement already exists and is RUNNING
# or COMPLETED, leave it alone (return its current phase). Otherwise
# delete any stale FAILED/PENDING leftover and POST fresh. Avoids
# wedging the cluster by mass DELETE+POST of healthy Statements.
ensure_statement() {
    local name="$1"; local sql_file="$2"
    local body
    body=$(curl -sf "${CMF_URL}/environments/${CMF_ENV}/statements/${name}" 2>/dev/null || true)
    if [ -n "${body}" ]; then
        local phase
        phase=$(echo "${body}" | python3 -c "import json,sys; print(json.loads(sys.stdin.read()).get('status',{}).get('phase','?'))" 2>/dev/null || echo "?")
        case "${phase}" in
            RUNNING|COMPLETED)
                # Healthy — leave it alone.
                echo "${phase}"
                return 0 ;;
            *)
                # FAILED/PENDING/CANCELLED — remove the stale row first,
                # then POST fresh below.
                curl -sS -o /dev/null -X DELETE "${CMF_URL}/environments/${CMF_ENV}/statements/${name}" || true
                # Some CMF builds also leave behind a FlinkDeployment;
                # clean that up too to avoid name conflicts on POST.
                kubectl delete flinkdeployment "${name}" -n "${NAMESPACE}" --ignore-not-found 2>/dev/null || true
                ;;
        esac
    fi
    submit_sql "${name}" "${sql_file}" >/dev/null
    echo "submitted"
}

# Wait until a Statement reaches a terminal state (RUNNING, COMPLETED, or FAILED).
wait_statement() {
    local name="$1"; local timeout="${2:-180}"
    local elapsed=0
    while [ ${elapsed} -lt ${timeout} ]; do
        sleep 5; elapsed=$((elapsed + 5))
        local phase
        phase=$(curl -sf "${CMF_URL}/environments/${CMF_ENV}/statements/${name}" 2>/dev/null \
            | python3 -c "import json,sys; r=json.load(sys.stdin); print(r.get('status',{}).get('phase','?'))" 2>/dev/null \
            || echo "?")
        if [ "${phase}" = "RUNNING" ] || [ "${phase}" = "COMPLETED" ] || [ "${phase}" = "FAILED" ]; then
            echo "${phase}"
            return 0
        fi
    done
    echo "TIMEOUT"
}

print_statement() {
    local name="$1"
    local body
    body=$(curl -sf "${CMF_URL}/environments/${CMF_ENV}/statements/${name}" 2>/dev/null) || {
        printf "  %-28s %s\n" "${name}" "(not found)"
        return 0
    }
    echo "${body}" | NAME="${name}" python3 -c "
import json, os, sys
nm = os.environ['NAME']
try:
    r = json.loads(sys.stdin.read())
    s = r.get('status', {})
    detail = (s.get('detail') or '')[:200]
    print(f'  {nm:<28} {s.get(\"phase\",\"?\"):<10}  {detail}')
except Exception as e:
    print(f'  {nm:<28} (couldn\\'t parse response: {e})')
"
}

if [ "${ACTION}" = "up" ]; then
    start_pf

    echo "→ Ensuring CMF KafkaCatalog '${CMF_CATALOG}' …"
    code=$(post_json "${CMF_URL}/catalogs/kafka" k8s/cmf/catalog.json)
    case "${code}" in 200|409) echo "  catalog → ${code}";; *) echo "  ✘ catalog → ${code}"; cat /tmp/cmf-resp.json; exit 1;; esac

    echo "→ Ensuring CMF KafkaDatabase '${CMF_DATABASE}' …"
    code=$(post_json "${CMF_URL}/catalogs/kafka/${CMF_CATALOG}/databases" k8s/cmf/database.json)
    case "${code}" in 200|409) echo "  database → ${code}";; *) echo "  ✘ database → ${code}"; cat /tmp/cmf-resp.json; exit 1;; esac

    echo "→ Ensuring CMF ComputePool '${CMF_POOL}' …"
    code=$(post_json "${CMF_URL}/environments/${CMF_ENV}/compute-pools" k8s/cmf/compute-pool.json)
    case "${code}" in 200|409) echo "  compute-pool → ${code}";; *) echo "  ✘ compute-pool → ${code}"; cat /tmp/cmf-resp.json; exit 1;; esac

    echo "→ Ensuring 'headers' METADATA column on auto-exposed iso-start table …"
    # ALTER ... ADD fails if the column already exists. Probe first; only
    # submit the ALTER when the column is missing. Keeps the script
    # idempotent across re-runs.
    headers_missing=$(curl -sS -X POST "${CMF_URL}/environments/${CMF_ENV}/statements" \
        -H "Content-Type: application/json" \
        -d '{"apiVersion":"cmf.confluent.io/v1","kind":"Statement","metadata":{"name":"_iso_probe_headers"},"spec":{"statement":"DESCRIBE `iso-start`;","properties":{"sql.current-catalog":"iso-cat","sql.current-database":"iso-db"},"computePoolName":"iso-pool","parallelism":1}}' \
        -o /dev/null -w "%{http_code}" 2>/dev/null; \
        sleep 4; \
        curl -sf "${CMF_URL}/environments/${CMF_ENV}/statements/_iso_probe_headers" 2>/dev/null \
        | python3 -c "
import json, sys
r = json.loads(sys.stdin.read())
data = r.get('result', {}).get('results', {}).get('data', [])
rows = [d.get('row', []) for d in data]
has_headers = any(row and row[0] == 'headers' for row in rows)
print('no' if has_headers else 'yes')" 2>/dev/null || echo "yes")
    curl -sS -o /dev/null -X DELETE "${CMF_URL}/environments/${CMF_ENV}/statements/_iso_probe_headers" || true
    if [ "${headers_missing}" = "yes" ]; then
        curl -sS -o /dev/null -X DELETE "${CMF_URL}/environments/${CMF_ENV}/statements/alter-iso-start" || true
        submit_sql "alter-iso-start" "flink/sql/cmf/00_alter_iso_start.sql" >/dev/null
        wait_statement "alter-iso-start" 30 >/dev/null
        print_statement "alter-iso-start"
    else
        echo "  alter-iso-start              already-applied  headers column already present"
    fi

    echo "→ Ensuring 4 sink DDL statements (skipping any already COMPLETED) …"
    for i in "${!REPORTS[@]}"; do
        name="${REPORTS[$i]}-ddl"
        action=$(ensure_statement "${name}" "${DDL_FILES[$i]}")
        if [ "${action}" = "submitted" ]; then
            wait_statement "${name}" 30 >/dev/null
        fi
        print_statement "${name}"
    done

    echo "→ Ensuring 4 INSERT INTO streaming statements (skipping any already RUNNING) …"
    needs_wait=0
    for i in "${!REPORTS[@]}"; do
        name="${REPORTS[$i]}-dml"
        action=$(ensure_statement "${name}" "${DML_FILES[$i]}")
        [ "${action}" = "submitted" ] && needs_wait=1
    done

    echo "→ Waiting for streaming statements to enter RUNNING (up to 5 min each: image pulls + JM/TM pod boot) …"
    failed_any=0
    for i in "${!REPORTS[@]}"; do
        name="${REPORTS[$i]}-dml"
        phase=$(wait_statement "${name}" 300)
        print_statement "${name}"
        [ "${phase}" = "RUNNING" ] || failed_any=1
    done

    if [ ${failed_any} -ne 0 ]; then
        echo ""
        echo "✘ One or more statements did NOT reach RUNNING. Diagnostics:"
        echo "    kubectl get pods -n ${NAMESPACE} | grep -E 'latency-dml|topology-dml|hop-distribution-dml|coverage-dml'"
        echo "    kubectl logs -n ${NAMESPACE} <pod>"
        exit 1
    fi

    echo ""
    echo "✔ 4 CMF Statements running. Each is its own application-mode Flink cluster."
    echo "  Sink topics: isotope-report-{latency,topology,hop-distribution,coverage}-1m"
    echo "  SR subjects: same name with -value suffix, Protobuf-typed."
    echo "  Open Control Center → Topics → pick one to view messages."
    echo ""
    echo "  NOTE: latency_percentiles_flat_1m (UDAF-based) is NOT migrated to CMF."
    echo "  It stays on the cp-flink session cluster — see scripts/deploy-flink-reports.sh."
else
    start_pf

    echo "→ Cancelling 4 CMF report Statements …"
    for r in "${REPORTS[@]}"; do
        for suffix in dml ddl; do
            name="${r}-${suffix}"
            curl -sS -o /dev/null -X DELETE "${CMF_URL}/environments/${CMF_ENV}/statements/${name}" \
                && echo "  ✓ ${name}" || echo "  ✗ ${name}"
        done
    done
    curl -sS -o /dev/null -X DELETE "${CMF_URL}/environments/${CMF_ENV}/statements/alter-iso-start" \
        && echo "  ✓ alter-iso-start" || echo "  ✗ alter-iso-start"

    echo "→ Deleting sink Kafka topics …"
    for t in "${SINK_TOPICS[@]}"; do
        kubectl exec -n "${NAMESPACE}" kafka-0 -- \
            kafka-topics --bootstrap-server localhost:9071 --delete --if-exists --topic "${t}" 2>/dev/null \
            && echo "  ✓ ${t}" || echo "  ✗ ${t}"
    done

    echo "→ Deleting SR subjects …"
    for t in "${SINK_TOPICS[@]}"; do
        kubectl exec -n "${NAMESPACE}" schemaregistry-0 -- bash -c "
            curl -sf -X DELETE 'http://localhost:8081/subjects/${t}-value' -o /dev/null
            curl -sf -X DELETE 'http://localhost:8081/subjects/${t}-value?permanent=true' -o /dev/null
        " 2>/dev/null && echo "  ✓ ${t}-value" || echo "  ✗ ${t}-value"
    done

    echo ""
    echo "✔ CMF reports torn down. Compute pool/catalog/database remain — re-running 'up'"
    echo "  is idempotent. To remove those too, see 'make cmf-resources-down'."
fi
