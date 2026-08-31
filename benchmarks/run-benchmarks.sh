#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 ]]; then
  cat >&2 <<'USAGE'
Usage:
  ./run-benchmarks.sh DATASET_DIR RESULTS_DIR FUNCTION_FILE [core|full]

Environment variables:
  WORKERS_LIST="1 2 4"      Worker counts to test (default: 1 2 4)
  CHUNK_ROWS=2000000        Target rows per materialised chunk
  TASK_ROWS=200000          Target rows per shared-chunk task
  INCLUDE_COLLECT=0         Set to 1 to benchmark collect-all baseline
  SAMPLE_INTERVAL=0.20      Seconds between Linux process-tree memory samples

Examples:
  ./run-benchmarks.sh data/bench-10m results/bench-10m ./summarise_big.R core

  WORKERS_LIST="1 2 4 8" CHUNK_ROWS=5000000 \
    ./run-benchmarks.sh data/bench-50m results/bench-50m ./summarise_big.R full
USAGE
  exit 2
fi

DATASET_DIR=$1
RESULTS_DIR=$2
FUNCTION_FILE=$3
SUITE=${4:-core}

WORKERS_LIST=${WORKERS_LIST:-"1 2 4"}
CHUNK_ROWS=${CHUNK_ROWS:-2000000}
TASK_ROWS=${TASK_ROWS:-200000}
INCLUDE_COLLECT=${INCLUDE_COLLECT:-0}
SAMPLE_INTERVAL=${SAMPLE_INTERVAL:-0.20}

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
FUNCTION_FILE=$(realpath "$FUNCTION_FILE")
DATASET_DIR=$(realpath "$DATASET_DIR")
mkdir -p "$RESULTS_DIR"
RESULTS_DIR=$(realpath "$RESULTS_DIR")
TMP_ROOT="$RESULTS_DIR/tmp"
mkdir -p "$TMP_ROOT"

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required for process-tree PSS measurement." >&2
  exit 2
fi

run_one() {
  local case=$1
  local workers=$2
  local stem="${case}__w${workers}"
  local csv="$RESULTS_DIR/${stem}.csv"
  local metrics="$RESULTS_DIR/${stem}.metrics"
  local log="$RESULTS_DIR/${stem}.log"

  echo "==> $case, workers=$workers"

  python3 "$SCRIPT_DIR/measure-command.py" \
    --metrics "$metrics" \
    --interval "$SAMPLE_INTERVAL" \
    -- \
    Rscript "$SCRIPT_DIR/benchmark-one.R" \
      "$FUNCTION_FILE" "$DATASET_DIR" "$case" "$workers" \
      "$CHUNK_ROWS" "$TASK_ROWS" "$TMP_ROOT" "$csv" \
      >"$log" 2>&1
}

run_one arrow_native 1
run_one sb_auto_native 1

for w in $WORKERS_LIST; do
  run_one parallel_custom_light "$w"
  run_one shared_custom_light "$w"
done

run_one mapreduce_var 1

last_worker=$(for w in $WORKERS_LIST; do echo "$w"; done | tail -n1)

if [[ "$SUITE" == "core" ]]; then
  run_one parallel_var "$last_worker"
  run_one shared_var "$last_worker"
elif [[ "$SUITE" == "full" ]]; then
  for w in $WORKERS_LIST; do
    run_one parallel_var "$w"
    run_one shared_var "$w"
    run_one parallel_custom_heavy "$w"
    run_one shared_custom_heavy "$w"
  done
else
  echo "Suite must be 'core' or 'full'." >&2
  exit 2
fi

if [[ "$INCLUDE_COLLECT" == "1" ]]; then
  run_one collect_custom_light 1
fi

Rscript "$SCRIPT_DIR/summarize-results.R" "$RESULTS_DIR"

echo
echo "Results: $RESULTS_DIR/benchmark-summary.csv"
