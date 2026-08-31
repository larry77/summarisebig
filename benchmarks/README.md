# `summarise_big()` performance benchmark suite

This suite benchmarks the **end-to-end** behavior of the Consolidated Function on artificial multi-file Parquet datasets.

It measures two things separately:

1. **Wall-clock time** for the complete operation, including temporary repartitioning used by the materialised strategies.
2. **Peak total PSS for the Linux process tree** (R plus Mirai workers). PSS apportions shared pages across processes, so it is much more informative than summing RSS when comparing `parallel_chunks` with Mori-based `shared_chunk`.

The benchmark runner starts every case in a fresh R process.

## Requirements

R packages required by `summarise_big()` plus `arrow` and `dplyr`, Python 3, and Linux `/proc`.

Keep your tested function in a file such as:

```text
summarise_big.R
```

## 1. Generate Parquet data

Generation is streaming: only `ROWS_PER_FILE` rows are constructed in R at once.

### Small smoke benchmark

```bash
Rscript generate-data.R data/bench-1m 1000000 1000 250000 balanced 1
```

### Useful first benchmark

```bash
Rscript generate-data.R data/bench-10m 10000000 10000 1000000 balanced 1
```

### Larger benchmark

```bash
Rscript generate-data.R data/bench-50m 50000000 20000 1000000 balanced 1
```

### Uneven groups

```bash
Rscript generate-data.R data/bench-20m-skewed 20000000 20000 1000000 skewed 1
```

`balanced` distributes rows almost exactly evenly among groups. `skewed` deliberately creates a long-tailed group-size distribution.

The data contain four columns:

- `grp`
- `row_id`
- `x`
- `y`

and are written as Snappy-compressed Parquet files.

## 2. Record the environment

```bash
Rscript environment-info.R results/environment.csv
lscpu > results/lscpu.txt
free -h > results/memory.txt
```

Storage matters greatly for these benchmarks, so also note whether the Parquet data and temporary directory are on SSD/NVMe/HDD/network storage.

## 3. Run the core suite

```bash
./run-benchmarks.sh \
  data/bench-10m \
  results/bench-10m \
  ./summarise_big.R \
  core
```

Defaults:

```text
workers:    1, 2, 4
chunk_rows: 2,000,000
task_rows:    200,000
```

Override them with environment variables:

```bash
WORKERS_LIST="1 2 4 8" \
CHUNK_ROWS=5000000 \
TASK_ROWS=250000 \
./run-benchmarks.sh data/bench-50m results/bench-50m ./summarise_big.R core
```

## 4. What the core suite runs

### Native Arrow reference

```text
arrow_native
```

Direct Arrow grouped `mean(x)`.

### Automatic Arrow fast path

```text
sb_auto_native
```

The same calculation through `summarise_big()`. This measures the overhead of the wrapper when Arrow can do everything itself.

### Main custom-function benchmark

```text
parallel_custom_light
shared_custom_light
```

Both compute an arbitrary ordinary R function:

```r
custom_light <- function(x, y) {
  mean((x - y)^2 + abs(x), na.rm = TRUE)
}
```

`.try_arrow = FALSE` is used deliberately so these measurements really exercise the requested materialisation strategy.

Each is run for every worker count in `WORKERS_LIST`.

### MapReduce vs raw-row materialisation

```text
mapreduce_var
parallel_var
shared_var
```

All calculate grouped sample variance. `mapreduce_var` uses Arrow sufficient statistics (`n`, `sum(x)`, `sum(x^2)`) followed by an R finalizer. The other two force an ordinary R `var()` over materialised groups.

This is the cleanest direct comparison of the three execution families for the same mathematical problem.

## 5. Full suite

```bash
./run-benchmarks.sh data/bench-10m results/bench-10m-full ./summarise_big.R full
```

The full suite additionally runs all worker counts for the variance benchmark and a more CPU-intensive custom function based on a transformed 90th percentile:

```r
custom_heavy <- function(x, y) {
  z <- (x - y)^2 + sin(x) + log1p(abs(y))
  quantile(z, 0.90, names = FALSE)
}
```

This helps distinguish I/O/repartitioning limits from CPU-parallel scaling.

## 6. Optional collect-everything baseline

Only enable this on a dataset you believe will fit in RAM:

```bash
INCLUDE_COLLECT=1 \
./run-benchmarks.sh data/bench-10m results/bench-10m-collect ./summarise_big.R core
```

This adds:

```text
collect_custom_light
```

which does the naive:

```r
ds |> collect() |> summarise(...)
```

Do **not** enable it merely to prove that an oversized dataset can crash the machine.

## 7. Results

The runner creates:

```text
benchmark-summary.csv
```

Important columns are:

- `wall_sec`: end-to-end elapsed time measured outside R.
- `peak_tree_pss_gb`: peak proportional physical-memory footprint of the complete process tree.
- `r_elapsed_sec`: R's internal elapsed-time cross-check.
- `workers`, `chunk_rows`, `task_rows`.
- `n_groups_returned`, `result_sum`, `result_mean`: lightweight diagnostics useful for spotting obviously inconsistent runs.

Each individual run also gets a `.log`, `.metrics`, and `.csv` file.

## Recommended sequence

1. Run `1m` simply to check that the benchmark machinery works.
2. Run `10m balanced` with workers 1/2/4.
3. Run `20m skewed` to study uneven groups.
4. Move to `50m` or larger only after choosing sensible `chunk_rows` from the smaller runs.
5. Repeat the most interesting cases at least 3 times when you want publication-quality timing numbers; disk cache and other system activity can materially affect a single run.

The benchmark intentionally includes temporary repartitioning in the measured time because that cost is part of the current `summarise_big()` API.
