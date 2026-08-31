# `summarise_big()` extended performance benchmarks

This is an extension of the original benchmark suite.

The first suite established that:

- native Arrow is very fast;
- MapReduce is already substantially faster than materialising raw groups for variance;
- `parallel_chunks` and `shared_chunk` really do use multiple Mirai workers, as verified separately with PID/timing probes;
- the original cheap custom function is too inexpensive for worker parallelism to amortise its overhead on 1m/10m data.

The extended suite therefore asks harder questions.

## What is new

### 1. Deterministic CPU-heavy custom function

Cases:

```text
parallel_cpu
shared_cpu
```

The function repeats nonlinear vector calculations `CPU_REPS` times per group.  It is deliberately ordinary R code and `.try_arrow = FALSE` is used.

Default:

```text
CPU_REPS=25
```

Increase to 50 or 100 if the workers still have too little useful work.

### 2. Quantile / sort-heavy custom function

Cases:

```text
parallel_quantile
shared_quantile
```

The summary uses transformed 10%, 50%, 90%, and 99% quantiles.

This is a useful counterexample to MapReduce: exact arbitrary quantiles generally cannot be reconstructed from a small fixed vector of sufficient statistics.

### 3. Bootstrap correlation

Cases:

```text
parallel_boot
shared_boot
```

Each group performs `BOOT_B` bootstrap correlations.  This is intentionally computationally expensive and not representable by the current sufficient-statistics MapReduce interface.

Default:

```text
BOOT_B=10
```

Start small.  Increase only after seeing the runtime.

### 4. Regression slope: MapReduce versus raw R

Cases:

```text
mapreduce_slope
parallel_slope
shared_slope
```

All three calculate the same simple-regression slope.

The MapReduce path asks Arrow only for:

```text
n
sum(x)
sum(y)
sum(x^2)
sum(x*y)
```

and calculates the slope in the R finalizer.

The materialised paths fit `lm(y ~ x)` separately inside every complete group.

This is an especially useful demonstration of the package philosophy: an apparently sophisticated custom R summary can sometimes be reduced to a tiny Arrow state.

### 5. Skewness: MapReduce versus raw R

Cases:

```text
mapreduce_skew
parallel_skew
shared_skew
```

MapReduce uses:

```text
n
sum(x)
sum(x^2)
sum(x^3)
```

and reconstructs the central moments in the finalizer.

This tests a less obvious sufficient-statistics calculation than variance.

---

# Recommended datasets

Keep the existing 10m / 10,000-group dataset: it represents **many small groups**.

Then generate two more 10m datasets.

## Medium groups: recommended next benchmark

```bash
Rscript generate-data.R \
  data/bench-10m-medium \
  10000000 \
  1000 \
  1000000 \
  balanced \
  1
```

Average group size: about 10,000 rows.

This is probably the best first dataset for expensive arbitrary R functions.

## Few large groups

```bash
Rscript generate-data.R \
  data/bench-10m-largegroups \
  10000000 \
  100 \
  1000000 \
  balanced \
  1
```

Average group size: about 100,000 rows.

This stresses complete-group materialisation and expensive per-group functions.

## Skewed groups

```bash
Rscript generate-data.R \
  data/bench-10m-skewed \
  10000000 \
  1000 \
  1000000 \
  skewed \
  1
```

For the skewed dataset, start with:

```text
CHUNK_ROWS=2000000
```

because the largest group can be around one million rows.

---

# Run the new suites

Put the current tested `summarise_big.R` in this directory.

## A. Parallel scaling with genuinely expensive work

Recommended first run:

```bash
./run-extended-benchmarks.sh \
  data/bench-10m-medium \
  results/10m-medium-parallel \
  ./summarise_big.R \
  parallel
```

Defaults:

```text
workers:     1 2 4
chunk_rows:  1,000,000
task_rows:     100,000
CPU_REPS:      25
```

If the CPU workload is still too cheap:

```bash
CPU_REPS=50 \
./run-extended-benchmarks.sh \
  data/bench-10m-medium \
  results/10m-medium-cpu50 \
  ./summarise_big.R \
  parallel
```

Or:

```bash
CPU_REPS=100 WORKERS_LIST="1 2 4 8" \
./run-extended-benchmarks.sh \
  data/bench-10m-medium \
  results/10m-medium-cpu100 \
  ./summarise_big.R \
  parallel
```

## B. MapReduce challenge

```bash
./run-extended-benchmarks.sh \
  data/bench-10m-medium \
  results/10m-medium-mapreduce \
  ./summarise_big.R \
  mapreduce
```

This runs:

```text
mapreduce_slope
parallel_slope    (1, 2, 4 workers)
shared_slope      (1, 2, 4 workers)

mapreduce_skew
parallel_skew     (1, 2, 4 workers)
shared_skew       (1, 2, 4 workers)
```

For each statistic, `result_sum` / `result_mean` should be essentially equal across execution strategies, subject to floating-point rounding.

## C. Bootstrap

```bash
BOOT_B=10 \
./run-extended-benchmarks.sh \
  data/bench-10m-medium \
  results/10m-medium-bootstrap \
  ./summarise_big.R \
  bootstrap
```

If that is quick:

```bash
BOOT_B=50 \
./run-extended-benchmarks.sh \
  data/bench-10m-medium \
  results/10m-medium-bootstrap50 \
  ./summarise_big.R \
  bootstrap
```

---

# Parameter grid

Once we have found a workload on which parallelism matters, do not assume our defaults are optimal.

For `parallel_chunks`:

```bash
WORKERS_LIST="1 2 4 8" \
CHUNK_ROWS_LIST="500000 1000000 2000000 5000000" \
CPU_REPS=50 \
./run-parameter-grid.sh \
  data/bench-10m-medium \
  results/grid-parallel-cpu \
  ./summarise_big.R \
  parallel_cpu
```

For `shared_chunk`:

```bash
WORKERS_LIST="1 2 4 8" \
CHUNK_ROWS_LIST="1000000 2000000 5000000" \
TASK_ROWS_LIST="50000 100000 250000 500000" \
CPU_REPS=50 \
./run-parameter-grid.sh \
  data/bench-10m-medium \
  results/grid-shared-cpu \
  ./summarise_big.R \
  shared_cpu
```

This is important because:

```text
.chunk_rows
```

controls materialisation/I/O granularity, while:

```text
.task_rows
```

controls shared-chunk scheduling granularity.

There is no reason to assume one setting is universally optimal.

---

# Most informative experimental sequence

I recommend this order:

1. Generate `bench-10m-medium` (10m rows / 1,000 groups).
2. Run the `mapreduce` suite.
3. Run `parallel` with `CPU_REPS=25`.
4. If parallel workers still lose, rerun with `CPU_REPS=50` or `100`.
5. Run bootstrap only after that.
6. Generate `bench-10m-largegroups`.
7. Repeat the most interesting cases.
8. Only then move to 50m rows.

This separates three effects that were confounded in the original benchmark:

```text
dataset size
group size
per-group computational cost
```

## Timing versus memory

The extended runner deliberately reports end-to-end timing only.

The previous Linux process-tree memory monitor did not see detached Mirai daemon processes (`peak_processes` remained 1), so its multi-worker memory figures should not be treated as authoritative.  We separately verified parallel execution directly using worker PIDs and overlapping task times.

Memory benchmarking should be repaired as a separate exercise rather than mixing unreliable memory measurements into these timing results.
