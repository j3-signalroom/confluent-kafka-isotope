#!/usr/bin/env bash
# Query the opt-in Iceberg lake (docs/lake.md) from the host with DuckDB.
#
# WHY DUCKDB ON THE HOST (not Trino/Spark in the cluster)
# minikube is already at its CPU ceiling with the reports and lake applications
# running. DuckDB is a single binary that speaks the Iceberg REST protocol and
# S3 directly, so reading the lake costs the cluster two port-forwards and
# nothing else.
#
# WHAT IT ANSWERS THAT FLINK SQL CANNOT
# `chains` walks every state-provenance version back through its `parents`
# with WITH RECURSIVE — the multi-generation traversal docs/state-provenance.md
# 5.0 leaves to "a relational or graph store fed by the provenance topic".
#
# Usage: scripts/lake-query.sh [chains|hops [TRACE_ID]|latency|shell]
#   chains    (default) each entity's full version chain, newest version first
#   hops      the 20 most recent hops, or every hop of TRACE_ID
#   latency   per-topic hop latency percentiles over the lake's whole history
#   shell     interactive DuckDB with the lake attached as `lake`
#
# Prereqs: make lake-up, and a lake application that has committed at least one
# checkpoint (make cp-flink-reports-up ENABLE_LAKE=true, then drive traffic).
set -euo pipefail

NAMESPACE="${NAMESPACE:-confluent}"
RUSTFS_ACCESS_KEY="${RUSTFS_ACCESS_KEY:-rustfsadmin}"
RUSTFS_SECRET_KEY="${RUSTFS_SECRET_KEY:-rustfsadmin123}"
# Not 8181/9000: keep clear of anything else forwarded on the host.
REST_PORT="${LAKE_REST_PORT:-18181}"
S3_PORT="${LAKE_S3_PORT:-19000}"

MODE="${1:-chains}"
command -v duckdb >/dev/null 2>&1 || {
    echo "✘ duckdb not found. Install it: 'brew install duckdb' (macOS) or see https://duckdb.org/docs/installation." >&2; exit 1; }
kubectl get deployment iceberg-rest -n "${NAMESPACE}" >/dev/null 2>&1 || {
    echo "✘ No Iceberg REST catalog in '${NAMESPACE}'. Run 'make lake-up'." >&2; exit 1; }

PIDS=()
cleanup() { for p in ${PIDS[@]+"${PIDS[@]}"}; do kill "${p}" 2>/dev/null || true; done; }
trap cleanup EXIT
kubectl port-forward -n "${NAMESPACE}" svc/iceberg-rest "${REST_PORT}:8181" >/dev/null 2>&1 & PIDS+=($!)
kubectl port-forward -n "${NAMESPACE}" svc/rustfs "${S3_PORT}:9000" >/dev/null 2>&1 & PIDS+=($!)
for _ in $(seq 1 20); do curl -sf -o /dev/null "http://localhost:${REST_PORT}/v1/config" && break; sleep 1; done

# The catalog hands back s3://isotope-lake/... data-file paths; the secret points
# DuckDB's S3 client at the RustFS port-forward, path-style, like Flink's side.
INIT=$(cat <<SQL
INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;
CREATE SECRET rustfs (TYPE s3, KEY_ID '${RUSTFS_ACCESS_KEY}', SECRET '${RUSTFS_SECRET_KEY}',
    ENDPOINT 'localhost:${S3_PORT}', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1');
ATTACH 's3://isotope-lake/warehouse' AS lake
    (TYPE iceberg, ENDPOINT 'http://localhost:${REST_PORT}', AUTHORIZATION_TYPE 'none');
SQL
)

case "${MODE}" in
chains)
    # DISTINCT first: the provenance topic is at-least-once, and a version_id is
    # content-addressed, so a duplicate row is the same version — never a new one.
    # A leaf is a version no other version names as a parent; the walk starts at
    # each leaf and follows `parents` until a version has none.
    QUERY=$(cat <<'SQL'
WITH RECURSIVE
v AS (SELECT DISTINCT * FROM lake.isotope.state_provenance),
leaf AS (
    SELECT * FROM v
    WHERE NOT EXISTS (SELECT 1 FROM v c WHERE list_contains(c.parents, v.version_id))),
chain AS (
    SELECT version_id AS leaf_id, entity_key, version_id, parents, source_name, emitted_at, 0 AS depth
    FROM leaf
    UNION ALL
    SELECT c.leaf_id, p.entity_key, p.version_id, p.parents, p.source_name, p.emitted_at, c.depth + 1
    FROM chain c JOIN v p ON list_contains(c.parents, p.version_id))
SELECT entity_key,
       max(depth) + 1                                     AS versions,
       string_agg(source_name, ' <- ' ORDER BY depth)     AS lineage_newest_first,
       make_timestamp(max(emitted_at) * 1000)             AS latest
FROM chain
GROUP BY leaf_id, entity_key
ORDER BY latest DESC
LIMIT 20;
SQL
);;
hops)
    if [ -n "${2:-}" ]; then
        QUERY="SELECT * FROM lake.isotope.hops WHERE trace_id = '${2//\'/}' ORDER BY hop_count, event_time;"
    else
        QUERY="SELECT * FROM lake.isotope.hops ORDER BY event_time DESC LIMIT 20;"
    fi;;
latency)
    # The 1-minute latency_percentiles report answers this per window; the lake
    # answers it over everything ever recorded, in one query.
    QUERY=$(cat <<'SQL'
SELECT this_topic,
       count(*)                                   AS hops,
       quantile_cont(latency_ms, 0.50)::INT       AS p50_ms,
       quantile_cont(latency_ms, 0.95)::INT       AS p95_ms,
       quantile_cont(latency_ms, 0.99)::INT       AS p99_ms,
       min(event_time)                            AS first_seen,
       max(event_time)                            AS last_seen
FROM lake.isotope.hops
GROUP BY this_topic
ORDER BY min(hop_count);
SQL
);;
shell)
    INITFILE=$(mktemp); printf '%s\n' "${INIT}" > "${INITFILE}"
    echo "→ Lake attached as 'lake' (tables: lake.isotope.hops, lake.isotope.state_provenance). Ctrl-D to exit."
    duckdb -init "${INITFILE}"; rm -f "${INITFILE}"; exit 0;;
*)
    echo "Usage: $0 [chains|hops [TRACE_ID]|latency|shell]" >&2; exit 2;;
esac

# Setup output (INSTALL/ATTACH result tables) goes to /dev/null; only the
# query's result reaches the terminal.
duckdb -c ".output /dev/null" -c "${INIT}" -c ".output" -c "${QUERY}"
