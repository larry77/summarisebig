#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 ]]; then
  cat >&2 <<'USAGE'
Usage:
  ./run-extended-benchmarks.sh DATASET_DIR RESULTS_DIR FUNCTION_FILE [parallel|mapreduce|bootstrap|all]

Environment variables:
  WORKERS_LIST="1 2 4"      Worker counts (default: 1 2 4)
  CHUNK_ROWS=1000000        Rows per materialised chunk (default: 1m)
  TASK_ROWS=100000          Rows per shared-chunk task (default: 100k)
  CPU_REPS=25               Repetitions in deterministic CPU workload
  BOOT_B=10                 Bootstrap replicates per group

Examples:

  ./run-extended-benchmarks.sh \
    data/bench-10m-medium \
    results/bench-10m-medium-parallel \
    ./summarise_big.R \
    parallel

  ./run-extended-benchmarks.sh \
    data/bench-10m-medium \
    results/bench-10m-medium-mapreduce \
    ./summarise_big.R \
    mapreduce

  CPU_REPS=50 WORKERS_LIST="1 2 4 8" \
    ./run-extended-benchmarks.sh \
      data/bench-10m-medium \
      results/bench-10m-medium-cpu50 \
      ./summarise_big.R \
      parallel
USAGE
  exit 2
fi

DATASET_DIR=$1
RESULTS_DIR=$2
FUNCTION_FILE=$3
SUITE=${4:-parallel}

WORKERS_LIST=${WORKERS_LIST:-"1 2 4"}
CHUNK_ROWS=${CHUNK_ROWS:-1000000}
TASK_ROWS=${TASK_ROWS:-100000}
CPU_REPS=${CPU_REPS:-25}
BOOT_B=${BOOT_B:-10}

export CPU_REPS
export BOOT_B

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
FUNCTION_FILE=$(realpath "$FUNCTION_FILE")
DATASET_DIR=$(realpath "$DATASET_DIR")

mkdir -p "$RESULTS_DIR"
RESULTS_DIR=$(realpath "$RESULTS_DIR")

TMP_ROOT="$RESULTS_DIR/tmp"
mkdir -p "$TMP_ROOT"

run_one() {
  local case=$1
  local workers=$2
  local stem="${case}__w${workers}"
  local csv="$RESULTS_DIR/${stem}.csv"
  local metrics="$RESULTS_DIR/${stem}.metrics"
  local log="$RESULTS_DIR/${stem}.log"

  echo "==> $case, workers=$workers"

  python3 "$SCRIPT_DIR/measure-wall.py" \
    --metrics "$metrics" \
    -- \
    Rscript "$SCRIPT_DIR/benchmark-extended.R" \
      "$FUNCTION_FILE" \
      "$DATASET_DIR" \
      "$case" \
      "$workers" \
      "$CHUNK_ROWS" \
      "$TASK_ROWS" \
      "$TMP_ROOT" \
      "$csv" \
      >"$log" 2>&1
}

run_parallel_suite() {
  for w in $WORKERS_LIST; do
    run_one parallel_cpu "$w"
    run_one shared_cpu "$w"

    run_one parallel_quantile "$w"
    run_one shared_quantile "$w"
  done
}

run_mapreduce_suite() {
  run_one mapreduce_slope 1
  run_one mapreduce_skew 1

  for w in $WORKERS_LIST; do
    run_one parallel_slope "$w"
    run_one shared_slope "$w"

    run_one parallel_skew "$w"
    run_one shared_skew "$w"
  done
}

run_bootstrap_suite() {
  for w in $WORKERS_LIST; do
    run_one parallel_boot "$w"
    run_one shared_boot "$w"
  done
}

case "$SUITE" in
  parallel)
    run_parallel_suite
    ;;
  mapreduce)
    run_mapreduce_suite
    ;;
  bootstrap)
    run_bootstrap_suite
    ;;
  all)
    run_parallel_suite
    run_mapreduce_suite
    run_bootstrap_suite
    ;;
  *)
    echo "Suite must be parallel, mapreduce, bootstrap, or all." >&2
    exit 2
    ;;
esac

Rscript "$SCRIPT_DIR/summarize-results.R" "$RESULTS_DIR"

echo
echo "Results: $RESULTS_DIR/benchmark-summary.csv"
