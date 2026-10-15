#!/bin/bash
# The Postgres outage drill against the local Kubernetes deployment (k8s/local):
#   bash loadtest/k8s-pg-outage.sh
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
fwd svc/ingest 3200:3000 &
fwd svc/postgres 15432:5432 &
fwd svc/redis 16379:6379 &
trap 'rm -f "$RUN"; kill $(cat "$PIDS") 2>/dev/null; wait; rm -f "$PIDS"' EXIT
sleep 4
set -a; . k8s/local/secrets.env; set +a
export DB_HOST=localhost DB_PORT=15432 DB_USER=reliant DB_NAME=reliant REDIS_URL=redis://localhost:16379 \
  LT_TARGET=k8s METRICS_PORT=0
node loadtest/run.js pg-outage
