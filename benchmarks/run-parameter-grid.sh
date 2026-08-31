#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 4 ]]; then
  cat >&2 <<'USAGE'
Usage:
  ./run-parameter-grid.sh DATASET_DIR RESULTS_DIR FUNCTION_FILE CASE

CASE must be one of:
  parallel_cpu
  shared_cpu
  parallel_quantile
  shared_quantile

Environment variables:
  WORKERS_LIST="1 2 4"
  CHUNK_ROWS_LIST="500000 1000000 2000000"
  TASK_ROWS_LIST="50000 100000 250000"
  CPU_REPS=25

For parallel_* cases TASK_ROWS_LIST is ignored.
For shared_* cases every valid chunk/task combination is run.
USAGE
  exit 2
fi

DATASET_DIR=$1
RESULTS_DIR=$2
FUNCTION_FILE=$3
CASE=$4

WORKERS_LIST=${WORKERS_LIST:-"1 2 4"}
CHUNK_ROWS_LIST=${CHUNK_ROWS_LIST:-"500000 1000000 2000000"}
TASK_ROWS_LIST=${TASK_ROWS_LIST:-"50000 100000 250000"}
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
  local workers=$1
  local chunk=$2
  local task=$3
  local stem="${CASE}__w${workers}__c${chunk}__t${task}"
  local csv="$RESULTS_DIR/${stem}.csv"
  local metrics="$RESULTS_DIR/${stem}.metrics"
  local log="$RESULTS_DIR/${stem}.log"

  echo "==> $CASE workers=$workers chunk=$chunk task=$task"

  python3 "$SCRIPT_DIR/measure-wall.py" \
    --metrics "$metrics" \
    -- \
    Rscript "$SCRIPT_DIR/benchmark-extended.R" \
      "$FUNCTION_FILE" \
      "$DATASET_DIR" \
      "$CASE" \
      "$workers" \
      "$chunk" \
      "$task" \
      "$TMP_ROOT" \
      "$csv" \
      >"$log" 2>&1
}

case "$CASE" in
  parallel_cpu|parallel_quantile)
    for w in $WORKERS_LIST; do
      for c in $CHUNK_ROWS_LIST; do
        run_one "$w" "$c" 100000
      done
    done
    ;;
  shared_cpu|shared_quantile)
    for w in $WORKERS_LIST; do
      for c in $CHUNK_ROWS_LIST; do
        for t in $TASK_ROWS_LIST; do
          if (( t <= c )); then
            run_one "$w" "$c" "$t"
          fi
        done
      done
    done
    ;;
  *)
    echo "Unsupported CASE: $CASE" >&2
    exit 2
    ;;
esac

Rscript "$SCRIPT_DIR/summarize-results.R" "$RESULTS_DIR"

echo
echo "Results: $RESULTS_DIR/benchmark-summary.csv"
