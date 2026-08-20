#!/usr/bin/env bash
# Measure the operator's two latency figures against a running kind cluster:
#
#   reclaim   time from deleting a worker pod mid-flight to the first reclaim
#             log line on a surviving peer
#   scale-up  time from a backlog spike to status.readyReplicas reaching the
#             replica count that backlog calls for
#
# Both are wall-clock measurements taken from outside the cluster, so they
# include everything a user would wait for: the operator's poll interval, the
# API server round trip, image pull (warm, since kind-up loaded the image) and
# container start.
#
# Prerequisites: a cluster prepared by `make kind-up`, plus kubectl, jq and
# grpcurl on PATH. Writes bench/results/reclaim.json and scaleup.json.
#
#   make kind-up
#   kubectl apply -n dpe-system -f examples/processingjob.yaml
#   bench/operator-latency.sh
set -euo pipefail

NAMESPACE="${NAMESPACE:-dpe-system}"
JOB="${JOB:-records}"
RUNS_RECLAIM="${RUNS_RECLAIM:-20}"
RUNS_SCALEUP="${RUNS_SCALEUP:-10}"
RECORDS="${RECORDS:-8000}"
COORDINATOR_PORT="${COORDINATOR_PORT:-9090}"
RESULTS_DIR="${RESULTS_DIR:-bench/results}"

for tool in kubectl jq grpcurl; do
  command -v "$tool" >/dev/null || { echo "bench: $tool is required" >&2; exit 1; }
done

mkdir -p "$RESULTS_DIR"

now_ms() { date +%s%3N; }

# ---------------------------------------------------------------------------

sts() { echo "${JOB}-worker"; }

job_field() {
  kubectl get processingjob "$JOB" -n "$NAMESPACE" -o jsonpath="{.status.$1}" 2>/dev/null || echo 0
}

total_reclaimed() {
  grpcurl -plaintext "localhost:${COORDINATOR_PORT}" engine.v1.Engine/GetClusterStats \
    | jq '[.nodes[]?.reclaimed // 0 | tonumber] | add // 0'
}

stream_lag() {
  grpcurl -plaintext "localhost:${COORDINATOR_PORT}" engine.v1.Engine/GetClusterStats \
    | jq '.streamLag // 0 | tonumber'
}

# submit pushes `count` records through the coordinator, in chunks that stay
# under the server's 10k-per-call limit. Keys carry a per-invocation prefix so
# a later run never collides with an idempotency key an earlier one committed —
# a duplicate key would be deduplicated at the sink and never processed, which
# would quietly measure nothing.
submit() {
  local count="$1" prefix batch sent chunk
  prefix="bench-$(now_ms)-$RANDOM"
  sent=0
  while (( sent < count )); do
    chunk=$(( count - sent ))
    (( chunk > 5000 )) && chunk=5000
    batch=$(jq -nc --argjson n "$chunk" --argjson off "$sent" --arg p "$prefix" \
      '[range($n) | {key: "\($p)-\(. + $off)", payload: ""}]')
    grpcurl -plaintext -d "{\"records\": $batch}" \
      "localhost:${COORDINATOR_PORT}" engine.v1.Engine/SubmitBatch >/dev/null
    sent=$(( sent + chunk ))
  done
}

# percentile <p> <sorted values...>  — nearest-rank, which is honest for the
# small run counts here; do not read a p95 from 20 samples as a tight bound.
percentile() {
  local p="$1"; shift
  local -a v=("$@")
  local n=${#v[@]}
  local idx=$(( (p * n + 99) / 100 ))
  (( idx < 1 )) && idx=1
  (( idx > n )) && idx=n
  echo "${v[$((idx - 1))]}"
}

emit_json() {
  local name="$1" unit="$2"; shift 2
  local -a sorted
  mapfile -t sorted < <(printf '%s\n' "$@" | sort -n)
  jq -n \
    --arg metric "$name" \
    --arg unit "$unit" \
    --argjson samples "$(printf '%s\n' "${sorted[@]}" | jq -sc .)" \
    --argjson p50 "$(percentile 50 "${sorted[@]}")" \
    --argjson p95 "$(percentile 95 "${sorted[@]}")" \
    --arg namespace "$NAMESPACE" \
    --arg job "$JOB" \
    '{measured: true, metric: $metric, unit: $unit, runs: ($samples | length),
      p50: $p50, p95: $p95, min: ($samples | min), max: ($samples | max),
      samples: $samples, namespace: $namespace, processingJob: $job}'
}

# --- reclaim latency -------------------------------------------------------

measure_reclaim() {
  local -a samples=()
  for ((run = 1; run <= RUNS_RECLAIM; run++)); do
    submit "$RECORDS"
    # Wait until records are genuinely in flight; killing an idle pod measures
    # nothing but pod start time.
    until [[ "$(stream_lag)" -gt 0 ]]; do sleep 0.2; done

    local before victim start elapsed
    before=$(total_reclaimed)
    victim=$(kubectl get pods -n "$NAMESPACE" \
      -l "dpe.trungprofile.dev/processing-job=$JOB" \
      -o jsonpath='{.items[0].metadata.name}')

    start=$(now_ms)
    kubectl delete pod "$victim" -n "$NAMESPACE" --grace-period=0 --force >/dev/null 2>&1

    # The entries the victim held stay pending until a peer's lease sweep
    # takes them. That transition is the recovery being timed.
    until [[ "$(total_reclaimed)" -gt "$before" ]]; do sleep 0.05; done
    elapsed=$(( $(now_ms) - start ))

    samples+=("$elapsed")
    echo "reclaim run $run/$RUNS_RECLAIM: ${elapsed}ms (victim $victim)" >&2

    until [[ "$(stream_lag)" -eq 0 ]]; do sleep 0.5; done
    kubectl rollout status "statefulset/$(sts)" -n "$NAMESPACE" --timeout=120s >/dev/null
  done
  emit_json "reclaim_latency" "milliseconds" "${samples[@]}" > "$RESULTS_DIR/reclaim.json"
  echo "wrote $RESULTS_DIR/reclaim.json" >&2
}

# --- scale-up latency ------------------------------------------------------

measure_scaleup() {
  local -a samples=()
  for ((run = 1; run <= RUNS_SCALEUP; run++)); do
    # Start from the floor so every run measures the same transition.
    until [[ "$(job_field replicas)" -eq "$(kubectl get processingjob "$JOB" -n "$NAMESPACE" \
      -o jsonpath='{.spec.autoscale.minReplicas}')" ]]; do sleep 1; done

    local start target elapsed
    start=$(now_ms)
    submit "$RECORDS"

    # Desired replicas is ceil(lag / targetLagPerWorker) clamped to max; wait
    # for status to report that many ready.
    target=$(kubectl get processingjob "$JOB" -n "$NAMESPACE" -o json \
      | jq '[( '"$RECORDS"' / .spec.autoscale.targetLagPerWorker | ceil ), .spec.autoscale.maxReplicas] | min')
    until [[ "$(job_field readyReplicas)" -ge "$target" ]]; do sleep 0.1; done
    elapsed=$(( $(now_ms) - start ))

    samples+=("$elapsed")
    echo "scale-up run $run/$RUNS_SCALEUP: ${elapsed}ms to $target ready replicas" >&2

    until [[ "$(stream_lag)" -eq 0 ]]; do sleep 0.5; done
  done
  emit_json "scaleup_latency" "milliseconds" "${samples[@]}" > "$RESULTS_DIR/scaleup.json"
  echo "wrote $RESULTS_DIR/scaleup.json" >&2
}

case "${1:-all}" in
  reclaim) measure_reclaim ;;
  scaleup) measure_scaleup ;;
  all)     measure_reclaim; measure_scaleup ;;
  *)       echo "usage: $0 [reclaim|scaleup|all]" >&2; exit 2 ;;
esac
