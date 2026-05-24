#!/usr/bin/env bash
# Deploy (or tear down) all seven Flink reports (pure-SQL + JAR-backed)
# against the CP Flink session cluster running on Minikube.
#
# As of the protobuf-sink refactor, each report is a Kafka sink topic
# (SR-framed Protobuf) populated by a long-lived `INSERT INTO ... SELECT`
# streaming job on the session cluster. Results persist in Kafka across
# Flink SQL Client sessions, so `make flink-sql` users can SELECT from
# them at any time — provided the sink-table DDL is loaded into their
# session, which `make flink-sql` does via the init file
# /tmp/isotope-report-sinks.fql that this script leaves behind.
#
# Usage:
#   scripts/deploy-cp-flink-reports.sh up       # build JAR, upload, create topics, submit jobs
#   scripts/deploy-cp-flink-reports.sh down     # cancel jobs, drop DDL, delete topics
#
# Prereqs:
#   - Minikube + CP up  (`make cp-up`)
#   - Flink session cluster up  (`make flink-up`)
#   - Kafka port-forwards optional for this script, but you'll want
#     `make kafka-pf-up` running for the demo CLI to feed the reports.
set -euo pipefail

NAMESPACE=confluent
JAR_HOST_PATH=ptf/build/libs/isotope-flink-udf.jar
JAR_POD_PATH=/opt/flink/lib/isotope-flink-udf.jar

# Sink topics — all 7 reports run on this session cluster, all written
# as SR-framed Avro (auto-registered on first write).
SINK_TOPICS=(
    isotope_report_latency_1m
    isotope_report_topology_1m
    isotope_report_bipartite_topology_1m
    isotope_report_hop_distribution_1m
    isotope_report_coverage_1m
    isotope_report_stuck_trace_1m
    isotope_report_latency_percentiles_1m
)

# Pipeline names — must match the `SET 'pipeline.name'` values in each
# cp/{10,20,25,30,40,60,70}_*.fql so we can find and cancel the jobs.
JOB_NAMES=(
    isotope_report_latency_1m
    isotope_report_topology_1m
    isotope_report_bipartite_topology_1m
    isotope_report_hop_distribution_1m
    isotope_report_coverage_1m
    isotope_report_stuck_trace_1m
    isotope_report_latency_percentiles_1m
)

ACTION="${1:-up}"
if [ "${ACTION}" != "up" ] && [ "${ACTION}" != "down" ]; then
    echo "Usage: $0 {up|down}" >&2
    exit 2
fi

# Discover the JM pod once.
JM_POD=$(kubectl get pods -n "${NAMESPACE}" -l component=jobmanager \
    --no-headers -o custom-columns=":metadata.name" 2>/dev/null | head -1)
if [ -z "${JM_POD}" ]; then
    echo "✘ No Flink JobManager pod found in '${NAMESPACE}'." >&2
    echo "  Did you run 'make flink-up'?" >&2
    exit 1
fi
echo "→ JobManager pod: ${JM_POD}"

# Discover a Kafka broker pod for kafka-topics admin. CFK creates a
# StatefulSet from the Kafka CR named 'kafka', so the broker pods are
# kafka-0, kafka-1, … We just need any one of them.
KAFKA_POD=$(kubectl get pods -n "${NAMESPACE}" \
    --no-headers -o custom-columns=":metadata.name" 2>/dev/null \
    | grep -E '^kafka-[0-9]+$' | head -1)
if [ -z "${KAFKA_POD}" ]; then
    echo "✘ No Kafka broker pod found in '${NAMESPACE}'." >&2
    echo "  Did you run 'make cp-up'?" >&2
    exit 1
fi
echo "→ Kafka broker pod: ${KAFKA_POD}"

## Copies a file into a pod without requiring `tar` (which the minimal
## Flink/CFK images lack, so `kubectl cp` fails). Streams the bytes
## through `kubectl exec` instead.
copy_to_pod() {
    local local_path="$1"
    local pod="$2"
    local pod_path="$3"
    kubectl exec -i -n "${NAMESPACE}" "${pod}" -- \
        sh -c "cat > ${pod_path}" < "${local_path}"
}

create_topic() {
    local topic="$1"
    kubectl exec -n "${NAMESPACE}" "${KAFKA_POD}" -- \
        kafka-topics --bootstrap-server localhost:9071 \
        --create --if-not-exists \
        --topic "${topic}" \
        --partitions 1 \
        --replication-factor 1 >/dev/null
}

delete_topic() {
    local topic="$1"
    kubectl exec -n "${NAMESPACE}" "${KAFKA_POD}" -- \
        kafka-topics --bootstrap-server localhost:9071 \
        --delete --if-exists \
        --topic "${topic}" >/dev/null 2>&1 || true
}

## Cancels every running Flink job whose `pipeline.name` matches the
## given name. Uses the bundled `flink` CLI rather than the REST API
## directly — `flink list -r` and `flink cancel` are guaranteed present
## in any Flink image, whereas curl/python3/jq are not. Output of
## `flink list -r` looks like:
##   ------------------ Running/Restarting Jobs -------------------
##   <date> <time> : <jobid> : <name> (RUNNING)
cancel_job_by_name() {
    local job_name="$1"
    local jids
    jids=$(kubectl exec -n "${NAMESPACE}" "${JM_POD}" -- \
        /opt/flink/bin/flink list -r 2>/dev/null \
        | awk -F ' : ' -v name="${job_name}" '
            $0 ~ /\(RUNNING\)/ {
                # Strip the trailing " (RUNNING)" from $3 to get the
                # bare job name and compare.
                n = $3
                sub(/ \(RUNNING\)$/, "", n)
                if (n == name) print $2
            }
          ' || true)
    if [ -z "${jids}" ]; then
        echo "  ↳ no running job named '${job_name}'"
        return 0
    fi
    for jid in ${jids}; do
        echo "  ↳ cancelling job ${jid} (${job_name})"
        kubectl exec -n "${NAMESPACE}" "${JM_POD}" -- \
            /opt/flink/bin/flink cancel "${jid}" >/dev/null 2>&1 \
            || echo "    ✘ cancel request failed for ${jid}"
    done
}

if [ "${ACTION}" = "up" ]; then
    # 1. Build the PTF/UDAF JAR if it isn't already there.
    if [ ! -f "${JAR_HOST_PATH}" ]; then
        echo "→ Building PTF shadow JAR..."
        ./gradlew :ptf:shadowJar -q
    fi

    # 2. Copy JAR to the JobManager pod (via cat-over-exec since the
    # image lacks `tar` and so `kubectl cp` doesn't work).
    echo "→ Copying $(basename "${JAR_HOST_PATH}") → ${JM_POD}:${JAR_POD_PATH}"
    copy_to_pod "${JAR_HOST_PATH}" "${JM_POD}" "${JAR_POD_PATH}"

    # 3. Pre-create the sink topics so the SR-framed-protobuf sink can
    # write on first record (avoids relying on broker auto-create, and
    # lets us set partitions/replication explicitly).
    echo "→ Creating ${#SINK_TOPICS[@]} sink topics on ${KAFKA_POD}..."
    for topic in "${SINK_TOPICS[@]}"; do
        echo "  ↳ ${topic}"
        create_topic "${topic}"
    done

    # 4. Concatenate DDL + INSERT files into a single script and apply
    # in one sql-client session. Each `sql-client.sh -f` invocation gets
    # a fresh in-memory catalog, so cross-file references (e.g.
    # `INSERT INTO latency_report_1m` depending on `isotope_raw`,
    # `isotope`, and the sink table) only resolve if they're all in the
    # same session.
    #
    # All 7 reports run on the session cluster.
    UP_FILES=(
        scripts/flink/sql/cp/00_source_table.fql
        scripts/flink/sql/cp/01_register_functions.fql
        scripts/flink/sql/cp/05_isotope_view.fql
        scripts/flink/sql/cp/06_consume_events_view.fql
        scripts/flink/sql/cp/05_report_sinks.fql
        scripts/flink/sql/cp/10_latency_report.fql
        scripts/flink/sql/cp/20_topology_report.fql
        scripts/flink/sql/cp/25_bipartite_topology_report.fql
        scripts/flink/sql/cp/30_hop_distribution.fql
        scripts/flink/sql/cp/40_coverage_report.fql
        scripts/flink/sql/cp/60_stuck_trace_report.fql
        scripts/flink/sql/cp/70_latency_percentiles_report.fql
    )

    # File used by the deploy session itself (source + functions + views
    # + sinks + INSERTs).
    COMBINED=$(mktemp)
    # Sink-only init file for interactive sessions. Includes just the
    # source table + isotope view + register-functions + sink table DDL,
    # so `sql-client.sh -i` users can `SELECT * FROM latency_report_1m`
    # without re-running the streaming INSERTs (which are already on the
    # session cluster doing the work).
    SINKS_INIT=$(mktemp)
    trap 'rm -f "${COMBINED}" "${SINKS_INIT}"' EXIT

    SINKS_INIT_FILES=(
        scripts/flink/sql/cp/00_source_table.fql
        scripts/flink/sql/cp/01_register_functions.fql
        scripts/flink/sql/cp/05_isotope_view.fql
        scripts/flink/sql/cp/06_consume_events_view.fql
        scripts/flink/sql/cp/05_report_sinks.fql
    )
    for f in "${UP_FILES[@]}"; do
        printf -- '-- ===== %s =====\n' "$f" >> "${COMBINED}"
        cat "$f" >> "${COMBINED}"
        printf '\n' >> "${COMBINED}"
    done
    for f in "${SINKS_INIT_FILES[@]}"; do
        printf -- '-- ===== %s =====\n' "$f" >> "${SINKS_INIT}"
        cat "$f" >> "${SINKS_INIT}"
        printf '\n' >> "${SINKS_INIT}"
    done

    echo "→ Applying ${#UP_FILES[@]} FQL files as one session ..."
    copy_to_pod "${COMBINED}"   "${JM_POD}" /tmp/isotope-reports.fql
    copy_to_pod "${SINKS_INIT}" "${JM_POD}" /tmp/isotope-report-sinks.fql
    # Capture the sql-client output so we can scan for [ERROR] markers.
    # sql-client.sh -f exits 0 even when individual statements fail
    # (the gateway reports each error but continues), so we have to
    # detect failures by inspecting both the stderr stream and the
    # actual running-job list afterwards.
    SQL_LOG=$(mktemp)
    trap 'rm -f "${COMBINED}" "${SINKS_INIT}" "${SQL_LOG}"' EXIT
    # Pass the PTF/UDAF JAR via -j so the generated proto message classes
    # (ai.signalroom.kafka.isotope.proto.reports.*) live in the session's
    # user-code classloader. Flink ships -j jars to the TaskManagers as
    # part of each job submission, so Class.forName(messageClassName)
    # works on the TM during sink-writer instantiation. /opt/flink/lib
    # alone is NOT sufficient: TaskManager pods are spawned dynamically
    # by the operator with a fresh emptyDir at /opt/flink/lib that the
    # JM's hot-copied JAR is invisible to.
    kubectl exec -n "${NAMESPACE}" "${JM_POD}" -- \
        bash -lc "/opt/flink/bin/sql-client.sh -j ${JAR_POD_PATH} -f /tmp/isotope-reports.fql" \
        2>&1 | tee "${SQL_LOG}"

    # Verify each expected pipeline.name shows up RUNNING in
    # `flink list -a`. Reasons a job may NOT be running:
    #   - Submission rejected (format/class-not-found at translate time).
    #   - Job submitted but failed during open() — most commonly a
    #     ClassNotFound on the TaskManager (protobuf message class
    #     missing from the user-code classloader, or the format jar
    #     missing from /opt/flink/lib of the dynamically-spawned TM).
    # We poll for up to ~15s so a job that's still INITIALIZING when we
    # first look has a chance to land in RUNNING; conversely, a job that
    # fails fast (the most common failure mode here) is caught after the
    # first short wait. Jobs that appear in the FAILED block of
    # `flink list -a` with the expected pipeline.name are flagged
    # specifically — pointing at a runtime failure rather than a
    # translate-time rejection.
    echo ""
    echo "→ Verifying streaming jobs registered with the session cluster..."
    MISSING=()
    FAILED=()
    for attempt in 1 2 3 4 5; do
        sleep 3
        ALL_JOBS=$(kubectl exec -n "${NAMESPACE}" "${JM_POD}" -- \
            /opt/flink/bin/flink list -a 2>/dev/null || true)
        MISSING=()
        FAILED=()
        for name in "${JOB_NAMES[@]}"; do
            # Match state against the most-recent line bearing this
            # pipeline.name (a single name may appear in RUNNING and in
            # FAILED across retries — last one wins).
            # POSIX-awk only — macOS bundles BSD awk, which doesn't support
            # gawk's match($0, /re/, arr) capture syntax. Strip the trailing
            # " (STATE)" twice: once to extract the bare name, once via a
            # different substitution to extract the bare state.
            #
            # Take FIRST match: `flink list -a` prints Running jobs before
            # Terminated jobs, so the first line bearing a given name is the
            # most-recent (and active) one. If we let later matches overwrite
            # `s`, a job that was previously CANCELED via `flink-reports-down`
            # would shadow this run's RUNNING entry.
            state=$(awk -F ' : ' -v n="${name}" '
                    {
                        line_name = $3
                        sub(/ \([A-Z]+\)$/, "", line_name)
                        if (line_name == n && s == "") {
                            state_str = $3
                            sub(/^.* \(/, "", state_str)
                            sub(/\)$/,    "", state_str)
                            s = state_str
                        }
                    }
                    END { print s }
                  ' <<< "${ALL_JOBS}")
            case "${state}" in
                RUNNING)              ;;  # good
                FAILED)               FAILED+=("${name}") ;;
                "")                   MISSING+=("${name}") ;;
                *)                    MISSING+=("${name} [state=${state}]") ;;
            esac
        done
        # All expected jobs are RUNNING → done early.
        if [ "${#MISSING[@]}" -eq 0 ] && [ "${#FAILED[@]}" -eq 0 ]; then
            break
        fi
    done

    if [ "${#MISSING[@]}" -gt 0 ] || [ "${#FAILED[@]}" -gt 0 ]; then
        echo ""
        if [ "${#FAILED[@]}" -gt 0 ]; then
            echo "✘ ${#FAILED[@]} of ${#JOB_NAMES[@]} streaming jobs FAILED after submission:"
            for f in "${FAILED[@]}"; do
                echo "    ✘ ${f}"
            done
            echo ""
            echo "  Jobs were accepted by the JobMaster but errored during open()."
            echo "  Look at the JobManager pod logs for the root-cause stack:"
            echo "    kubectl logs -n ${NAMESPACE} ${JM_POD} | grep -B1 -A20 'Caused by'"
            echo "  Common causes:"
            echo "    • Class missing on TaskManager (e.g. a protobuf message class)."
            echo "      The deploy script already passes the PTF JAR via 'sql-client.sh -j'"
            echo "      so the user-code classloader sees it. If a class is still missing,"
            echo "      rebuild the JAR (./gradlew :ptf:shadowJar) — the running JAR may"
            echo "      be stale and missing newly-added classes."
            echo "    • UDAF / PTF type-system bug — accumulator or output ROW shape"
            echo "      isn't being inferred correctly during codegen. This is a Java"
            echo "      fix in ptf/src/main/java/..., not a SQL fix."
        fi
        if [ "${#MISSING[@]}" -gt 0 ]; then
            echo ""
            echo "✘ ${#MISSING[@]} of ${#JOB_NAMES[@]} streaming jobs did NOT register at all:"
            for m in "${MISSING[@]}"; do
                echo "    ✘ ${m}"
            done
            echo ""
            echo "  These jobs were rejected before submission. Scan ${SQL_LOG} or the"
            echo "  output above for [ERROR] lines. Common causes:"
            echo "    • value.format jar missing from /opt/flink/lib (init container in"
            echo "      k8s/base/flink-basic-deployment.yaml). Redeploy with"
            echo "      'make flink-delete && make flink-deploy' if you've changed it."
            echo "    • SQL validation error (CAST not allowed, missing column, etc.)."
            echo "    • Upstream DDL (source table / isotope view) failed, so the"
            echo "      sink table that depends on it was never created."
        fi
        exit 1
    fi

    echo ""
    echo "✔ ${#JOB_NAMES[@]} INSERT-INTO streaming jobs verified running on the session cluster."
    echo "  pipeline.name = its sink topic name; check the Flink UI ('make flink-ui')"
    echo "  for live status."
    echo ""
    echo "✔ Reports registered (7/7). Try interactively:"
    echo "    make flink-sql"
    echo "    Flink SQL> SELECT * FROM latency_report_1m;"
    echo "    Flink SQL> SELECT * FROM topology_report_1m;"
    echo "    Flink SQL> SELECT * FROM bipartite_topology_report_1m;"
    echo "    Flink SQL> SELECT * FROM hop_distribution_1m;"
    echo "    Flink SQL> SELECT * FROM coverage_report_1m;"
    echo "    Flink SQL> SELECT * FROM stuck_trace_alerts_1m;"
    echo "    Flink SQL> SELECT * FROM latency_percentiles_flat_1m;"
    echo ""
    echo "Feed traffic via the demo CLI (pipeline-position verbs, run in pipeline order):"
    echo "    ./gradlew :app:run --args=\"place 'hello'\" -q   # terminal A — kick the chain off"
    echo "    ./gradlew :app:run --args=\"enrich\"        -q   # terminal B"
    echo "    ./gradlew :app:run --args=\"fulfill\"       -q   # terminal C"
    echo "    ./gradlew :app:run --args=\"ship\"          -q   # terminal D — terminal consumer (emits marker)"
else
    # 1. Cancel each named streaming job via the Flink REST API.
    echo "→ Cancelling streaming jobs by pipeline.name ..."
    for name in "${JOB_NAMES[@]}"; do
        cancel_job_by_name "${name}"
    done

    # 2. Apply teardown FQL in a fresh sql-client session. The previous
    # session is gone so the catalog is empty — IF EXISTS makes this a
    # safe no-op. We still apply it because (a) a future iteration may
    # use a persistent catalog and (b) it's the documented teardown for
    # the FQL contract.
    echo "→ Applying teardown FQL ..."
    copy_to_pod scripts/flink/sql/cp/99_teardown.fql "${JM_POD}" /tmp/99_teardown.fql
    kubectl exec -n "${NAMESPACE}" "${JM_POD}" -- \
        bash -lc "/opt/flink/bin/sql-client.sh -f /tmp/99_teardown.fql" \
        || echo "→ Teardown FQL hit non-fatal errors (probably already-dropped objects)."

    # 3. Delete the sink Kafka topics so `flink-reports-down` is a full
    # reset. Persisted history is lost — that's the documented contract
    # for the 'delete topics' teardown style.
    echo "→ Deleting ${#SINK_TOPICS[@]} sink topics on ${KAFKA_POD}..."
    for topic in "${SINK_TOPICS[@]}"; do
        echo "  ↳ ${topic}"
        delete_topic "${topic}"
    done

    # Avro+SR auto-registers each sink's schema in SR under
    # `<topic>-value`. Topic deletion alone doesn't reclaim the SR
    # subject — delete soft + hard so the next deploy gets a fresh
    # schema ID rather than colliding with a tombstoned one.
    echo "→ Deleting ${#SINK_TOPICS[@]} SR subjects (soft + hard)..."
    for topic in "${SINK_TOPICS[@]}"; do
        subject="${topic}-value"
        kubectl exec -n "${NAMESPACE}" schemaregistry-0 -- bash -c "
            curl -sf -X DELETE 'http://localhost:8081/subjects/${subject}' -o /dev/null
            curl -sf -X DELETE 'http://localhost:8081/subjects/${subject}?permanent=true' -o /dev/null
        " 2>/dev/null && echo "  ↳ ${subject}" || echo "  ↳ ${subject} (not registered)"
    done

    echo ""
    echo "✔ Jobs cancelled, DDL dropped, sink topics + SR subjects deleted."
fi
