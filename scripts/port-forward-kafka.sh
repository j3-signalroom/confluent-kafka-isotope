#!/usr/bin/env bash
# Port-forward the CFK-managed Kafka external listener Services AND the
# Schema Registry service to localhost so JUnit integration tests on the
# host can reach the broker and SR.
#
# Kafka: paired with `spec.listeners.external.externalAccess.type: nodePort`
# on the Kafka CR (host: localhost, nodePortOffset: 30092). CFK creates one
# Service per broker plus one bootstrap Service. We forward each NodePort to
# the matching localhost port so the broker's advertised listener
# (`localhost:<port>`) resolves correctly after the first metadata
# round-trip.
#
# Schema Registry: deployed in-cluster on port 8081 by the CFK
# SchemaRegistry CR. We port-forward svc/schemaregistry to localhost:8081
# so KafkaProtobuf{Ser,Deser}ializer (which needs SR at
# `schema.registry.url`) works from the host.
#
# Usage:
#   scripts/port-forward-kafka.sh             # start in background
#   scripts/port-forward-kafka.sh --stop      # kill any running forwards
set -euo pipefail

NAMESPACE=confluent
SR_PORT=8081
SR_SVC=schemaregistry
PID_DIR=/tmp/isotope-kafka-pf
TIMEOUT=30

stop_all() {
    if [ -d "${PID_DIR}" ]; then
        for f in "${PID_DIR}"/*.pid; do
            [ -e "$f" ] || continue
            pid=$(cat "$f" 2>/dev/null || true)
            if [ -n "${pid:-}" ] && kill -0 "${pid}" 2>/dev/null; then
                kill "${pid}" 2>/dev/null || true
            fi
            rm -f "$f"
        done
        rmdir "${PID_DIR}" 2>/dev/null || true
    fi
    echo "✔ All kafka port-forwards stopped."
}

if [ "${1:-}" = "--stop" ]; then
    stop_all
    exit 0
fi

# Find the Services CFK created for the external listener. Naming convention:
# 'kafka-<broker-id>-external' for per-broker, 'kafka-bootstrap' or
# 'kafka-external-bootstrap' for the bootstrap. We match anything in the
# namespace whose name contains 'kafka' and 'external' OR is the bootstrap.
mapfile -t SERVICES < <(
    kubectl get svc -n "${NAMESPACE}" -o json 2>/dev/null \
        | jq -r '.items[]
                 | select(.spec.type == "NodePort")
                 | select(.metadata.name | test("^kafka.*(external|bootstrap)"))
                 | .metadata.name'
)

if [ "${#SERVICES[@]}" -eq 0 ]; then
    echo "✘ No kafka NodePort Services found in namespace '${NAMESPACE}'." >&2
    echo "  Did you re-roll CP after adding the external listener?" >&2
    echo "  Try: make cp-down && make cp-up" >&2
    exit 1
fi

mkdir -p "${PID_DIR}"

for svc in "${SERVICES[@]}"; do
    # The external port on each Service is the NodePort itself (CFK exposes
    # it as the Service's port). Forward localhost:<nodeport> → svc:<nodeport>.
    PORT=$(kubectl get svc -n "${NAMESPACE}" "${svc}" \
        -o jsonpath='{.spec.ports[?(@.name=="external")].nodePort}' 2>/dev/null)
    if [ -z "${PORT}" ]; then
        # Fall back to the first port if the listener isn't named 'external'.
        PORT=$(kubectl get svc -n "${NAMESPACE}" "${svc}" \
            -o jsonpath='{.spec.ports[0].nodePort}' 2>/dev/null)
    fi
    if [ -z "${PORT}" ]; then
        echo "→ Skipping ${svc}: could not determine NodePort." >&2
        continue
    fi

    # Kill anything already on this port (e.g. stale forward).
    lsof -iTCP:"${PORT}" -sTCP:LISTEN -t 2>/dev/null | xargs kill 2>/dev/null || true
    sleep 0.2

    echo "→ Port-forwarding ${PORT} → svc/${svc}:${PORT} ..."
    nohup kubectl port-forward -n "${NAMESPACE}" "svc/${svc}" "${PORT}:${PORT}" \
        >/dev/null 2>&1 &
    disown
    echo $! > "${PID_DIR}/${svc}.pid"

    # Wait for the port to be listening.
    elapsed=0
    while ! lsof -iTCP:"${PORT}" -sTCP:LISTEN -t >/dev/null 2>&1; do
        if [ "${elapsed}" -ge "${TIMEOUT}" ]; then
            echo "✘ Timed out waiting for ${svc} port ${PORT}." >&2
            stop_all
            exit 1
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    echo "✔ ${svc} ready on localhost:${PORT}"
done

# Schema Registry forward — a plain ClusterIP service, not NodePort.
if kubectl get svc -n "${NAMESPACE}" "${SR_SVC}" >/dev/null 2>&1; then
    lsof -iTCP:"${SR_PORT}" -sTCP:LISTEN -t 2>/dev/null | xargs kill 2>/dev/null || true
    sleep 0.2
    echo "→ Port-forwarding ${SR_PORT} → svc/${SR_SVC}:${SR_PORT} ..."
    nohup kubectl port-forward -n "${NAMESPACE}" "svc/${SR_SVC}" "${SR_PORT}:${SR_PORT}" \
        >/dev/null 2>&1 &
    disown
    echo $! > "${PID_DIR}/${SR_SVC}.pid"

    elapsed=0
    while ! lsof -iTCP:"${SR_PORT}" -sTCP:LISTEN -t >/dev/null 2>&1; do
        if [ "${elapsed}" -ge "${TIMEOUT}" ]; then
            echo "✘ Timed out waiting for ${SR_SVC} port ${SR_PORT}." >&2
            stop_all
            exit 1
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    echo "✔ ${SR_SVC} ready on localhost:${SR_PORT}"
else
    echo "→ svc/${SR_SVC} not found in '${NAMESPACE}' — skipping SR forward."
fi

echo ""
echo "Bootstrap server: localhost:30092"
echo "Schema Registry:  http://localhost:${SR_PORT}"
echo "Stop with: scripts/port-forward-kafka.sh --stop"
