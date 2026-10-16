#!/bin/bash
# Drills against the local Kubernetes deployment (k8s/local):
#   bash loadtest/k8s-drill.sh pg-outage            Postgres crash mid-traffic (incident 001)
#   bash loadtest/k8s-drill.sh k8s-scale-out        KEDA scale-out under live load
#   bash loadtest/k8s-drill.sh k8s-rolling-restart  rolling consumer restart at 1,000 events/s
# Needs kubectl, the deployed namespace "reliant", and k8s/local/secrets.env.
# Port-forwards run in loops: a forward to the postgres pod drops when its container restarts.
set -u
cd "$(dirname "$0")/.."
RUN=$(mktemp); PIDS=$(mktemp) # delete RUN to stop the loops; PIDS lists every kubectl started
fwd() {
  while [ -e "$RUN" ]; do
    kubectl -n reliant port-forward "$1" "$2" >/dev/null 2>&1 & echo $! >> "$PIDS"; wait $!
    sleep 0.5
  done
}
# One forward per ingest pod: a single forward is one pod and one tunnel, too narrow for 1,000/s.
i=0; INGEST_PORTS=""
# Skip pods that are terminating (a recent rollout): a forward to one of them fails every request.
for pod in $(kubectl -n reliant get pods -l app.kubernetes.io/component=ingest -o go-template='{{range .items}}{{if not .metadata.deletionTimestamp}}pod/{{.metadata.name}} {{end}}{{end}}'); do
  fwd "$pod" "$((3200 + i)):3000" & INGEST_PORTS="$INGEST_PORTS,$((3200 + i))"; i=$((i + 1))
done
fwd svc/postgres 15432:5432 &
fwd svc/redis 16379:6379 &
trap 'rm -f "$RUN"; kill $(cat "$PIDS") 2>/dev/null; wait; rm -f "$PIDS"' EXIT
sleep 4
set -a; . k8s/local/secrets.env; set +a
export DB_HOST=localhost DB_PORT=15432 DB_USER=reliant DB_NAME=reliant REDIS_URL=redis://localhost:16379 \
  LT_TARGET=k8s METRICS_PORT=0 LT_INGEST_PORTS="${INGEST_PORTS#,}"
node loadtest/run.js "${1:-pg-outage}"
