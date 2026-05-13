#!/usr/bin/env bash
# Starts kubectl port-forward to the Flink TaskManager in the background,
# waits for port 5005 to be listening, then exits so VS Code can attach.
set -euo pipefail

PORT=5005
NAMESPACE=confluent
LABEL="component=taskmanager"
TIMEOUT=30

# Kill any existing port-forward on this port
lsof -iTCP:${PORT} -sTCP:LISTEN -t 2>/dev/null | xargs kill 2>/dev/null || true
sleep 0.5

# Resolve the TaskManager pod
POD=$(kubectl get pods -n "${NAMESPACE}" -l "${LABEL}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
if [ -z "${POD}" ]; then
    echo "✘ No TaskManager pod found (label=${LABEL}, namespace=${NAMESPACE})." >&2
    echo "  Submit a Flink job first so a TaskManager is running." >&2
    exit 1
fi

echo "→ Port-forwarding ${PORT} → ${POD}:${PORT} ..."
nohup kubectl port-forward -n "${NAMESPACE}" "${POD}" "${PORT}:${PORT}" >/dev/null 2>&1 &
disown
PF_PID=$!

# Wait for the port to be listening
elapsed=0
while ! lsof -iTCP:${PORT} -sTCP:LISTEN -t >/dev/null 2>&1; do
    if ! kill -0 "${PF_PID}" 2>/dev/null; then
        echo "✘ kubectl port-forward exited unexpectedly." >&2
        exit 1
    fi
    if [ "${elapsed}" -ge "${TIMEOUT}" ]; then
        echo "✘ Timed out waiting for port ${PORT} to be ready." >&2
        kill "${PF_PID}" 2>/dev/null || true
        exit 1
    fi
    sleep 1
    elapsed=$((elapsed + 1))
done

echo "✔ Port ${PORT} is ready (pid ${PF_PID})."
