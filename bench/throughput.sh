#!/usr/bin/env bash
# Re-measure sustained throughput with the workers running under the operator,
# against the 8.9K records/sec Docker Compose baseline.
#
# The comparison is only meaningful if the two runs are shaped the same, so
# this script pins the parameters the baseline used: 256-byte records,
# concurrency 64, min-idle 5s, one Redis with appendonly/everysec/noeviction.
# What changes is the orchestrator, not the engine.
#
# Prerequisites: a cluster prepared by `make kind-up` with a ProcessingJob
# applied and scaled to WORKERS replicas, plus kubectl and jq on PATH. Writes
# bench/results/throughput.json.
set -euo pipefail

NAMESPACE="${NAMESPACE:-dpe-system}"
JOB="${JOB:-records}"
WORKERS="${WORKERS:-8}"
RATE="${RATE:-12000}"
DURATION="${DURATION:-60}"
SIZE="${SIZE:-256}"
IMAGE="${IMAGE:-dpe:e2e}"
RESULTS_DIR="${RESULTS_DIR:-bench/results}"
BASELINE_RPS="${BASELINE_RPS:-8900}"

for tool in kubectl jq; do
  command -v "$tool" >/dev/null || { echo "bench: $tool is required" >&2; exit 1; }
done
mkdir -p "$RESULTS_DIR"

redis_addr=$(kubectl get processingjob "$JOB" -n "$NAMESPACE" -o jsonpath='{.spec.redisAddr}')
stream=$(kubectl get processingjob "$JOB" -n "$NAMESPACE" -o jsonpath='{.spec.stream}')

# Hold the worker count fixed for the duration: this measures throughput at a
# known width, not the autoscaler's reaction to a spike.
kubectl patch processingjob "$JOB" -n "$NAMESPACE" --type=merge \
  -p "{\"spec\":{\"autoscale\":{\"minReplicas\":$WORKERS,\"maxReplicas\":$WORKERS}}}"
kubectl rollout status "statefulset/${JOB}-worker" -n "$NAMESPACE" --timeout=300s

sink_count() {
  kubectl exec -n "$NAMESPACE" statefulset/dpe-redis -- \
    redis-cli --no-raw HLEN dpe:results | tr -dc '0-9'
}

before=$(sink_count)
started=$(date +%s)

# The producer runs in-cluster so the load generator is not measuring the
# laptop's network path to the API server.
kubectl run "dpe-bench-$(date +%s)" -n "$NAMESPACE" \
  --image="$IMAGE" --image-pull-policy=IfNotPresent --restart=Never --attach --rm \
  --overrides="{\"spec\":{\"containers\":[{\"name\":\"producer\",\"image\":\"$IMAGE\",\"command\":[\"/usr/local/bin/producer\"],\"args\":[\"-redis=$redis_addr\",\"-stream=$stream\",\"-rate=$RATE\",\"-duration=${DURATION}s\",\"-size=$SIZE\"]}]}}"

# Let the cluster finish what the producer queued before reading the sink.
until [[ "$(kubectl get processingjob "$JOB" -n "$NAMESPACE" -o jsonpath='{.status.streamLag}')" == "0" ]]; do
  sleep 1
done
after=$(sink_count)
elapsed=$(( $(date +%s) - started ))
processed=$(( after - before ))
rps=$(( processed / (elapsed > 0 ? elapsed : 1) ))

jq -n \
  --argjson processed "$processed" \
  --argjson elapsed "$elapsed" \
  --argjson rps "$rps" \
  --argjson workers "$WORKERS" \
  --argjson baseline "$BASELINE_RPS" \
  --argjson size "$SIZE" \
  '{measured: true, metric: "throughput_under_operator", unit: "records_per_second",
    workers: $workers, recordSizeBytes: $size,
    recordsProcessed: $processed, elapsedSeconds: $elapsed,
    recordsPerSecond: $rps,
    composeBaselineRecordsPerSecond: $baseline,
    ratioToBaseline: (($rps / $baseline) * 1000 | round / 1000)}' \
  > "$RESULTS_DIR/throughput.json"

echo "wrote $RESULTS_DIR/throughput.json: ${rps} records/sec across ${WORKERS} workers" >&2
