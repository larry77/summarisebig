# summarisebig

[![R-CMD-check](https://github.com/larry77/summarisebig/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/larry77/summarisebig/actions/workflows/R-CMD-check.yaml)

`summarisebig` is designed for a common problem in large-scale R data
analysis: **how do you perform non-trivial grouped summaries on datasets
that are too large, or simply too inconvenient, to load completely into
R memory?**

[Apache Arrow for R](https://arrow.apache.org/docs/r/) already provides
an excellent foundation for this problem. Arrow can query multi-file and
larger-than-memory datasets, including Parquet datasets, through
familiar `dplyr` verbs without first materializing all rows as an R data
frame. When a grouped summary can be translated completely to Arrow,
this is usually exactly what we want: the heavy computation remains in
Arrow and only the small grouped result is collected into R.

The difficulty begins when the desired statistic is **not fully
expressible in Arrow**. Real analyses often contain custom R functions,
model fitting, order-sensitive calculations, or other operations that
ultimately need ordinary R objects. A simple fallback such as
[`collect()`](https://dplyr.tidyverse.org/reference/compute.html)
followed by
[`dplyr::summarise()`](https://dplyr.tidyverse.org/reference/summarise.html)
can then throw away the main benefit of Arrow by bringing a very large
dataset into R at once.

`summarisebig` tries to bridge that gap. It provides a `dplyr`-like
grouped summary interface and chooses among three increasingly expensive
execution paths:

1.  **Native Arrow.** If Arrow can execute the complete summary, keep
    the data in Arrow and collect only the final grouped result.
2.  **MapReduce.** If the result can be reconstructed from compact
    sufficient statistics, compute those reductions in Arrow and perform
    only the small final calculation in R.
3.  **Bounded R materialization.** If the statistic genuinely needs the
    raw observations, partition the data by complete groups and
    materialize only a bounded part of the dataset at a time.

The third case is where R parallelization becomes useful. `summarisebig`
uses [`futurize`](https://futurize.futureverse.org/) and the Future
ecosystem to run group-safe work concurrently. It offers two
complementary strategies:

- **`parallel_chunks`** lets several workers materialize and process
  different group-safe chunks at the same time. This favors throughput
  when enough RAM is available.
- **`shared_chunk`** materializes one chunk at a time and uses
  [`mori`](https://shikokuchuo.net/mori/) shared memory so workers on
  the same machine can access the same physical data pages instead of
  each receiving a full private copy. This favors tighter memory
  control.

The package therefore does **not** assume that parallelism is
automatically faster. Parallel workers introduce scheduling,
serialization, data-transfer, and I/O overhead. For cheap summaries,
those costs can dominate the calculation and a native Arrow query is
often dramatically preferable. The aim is instead to use the cheapest
execution mechanism that can correctly express the requested statistic.

The central principle is:

> **Keep computation in Arrow as long as possible. Move raw observations
> into R only when the requested statistic genuinely requires them.**

That leads to the following hierarchy:

``` text
Can Arrow compute the whole summary?
        | yes -> Arrow fast path
        | no
        v
Can compact Arrow-computable state reconstruct the answer?
        | yes -> map_reduce
        | no
        v
Does arbitrary R need the raw observations?
        -> parallel_chunks or shared_chunk
```

The package defaults to two workers for the materialized parallel
strategies.

## Installation

The development version is available from GitHub:

``` r

# install.packages("remotes")
remotes::install_github("larry77/summarisebig")
```

## A reproducible example dataset

All examples below use the same small Parquet dataset. The multi-worker
`shared_chunk` example additionally requires the optional `mori`
package.

``` r

library(arrow)
library(dplyr)
library(summarisebig)

dat <- data.frame(
  grp = rep(c("A", "B", "C"), each = 5),
  row_id = rep(1:5, 3),
  x = c(
    1, 2, 4, 8, 10,
    2, 5, 7, 11, 14,
    3, 6, 9, 13, 18
  ),
  y = c(
    2, 4, 5, 9, 12,
    1, 5, 8, 10, 15,
    4, 5, 11, 14, 20
  )
)

path <- tempfile("summarisebig-example-")
write_dataset(dat, path, format = "parquet")
ds <- open_dataset(path)
```

The example dataset is written under R’s temporary directory with
[`tempfile()`](https://rdrr.io/r/base/tempfile.html). These temporary
files are normally removed automatically when the R session ends.

## 1. Prefer the Arrow fast path

If Arrow can translate the complete expression,
[`summarise_big()`](https://larry77.github.io/summarisebig/reference/summarise_big.md)
leaves the large dataset in Arrow and collects only the final grouped
result.

``` r

arrow_result <- summarise_big(
  ds,
  mean_x = mean(x),
  .by = grp
)

arrow_result
```

Even a composed expression can remain on the Arrow fast path if Arrow
can translate all of it:

``` r

summarise_big(
  ds,
  result = mean(x^2 - 5),
  .by = grp
)
```

This is normally the best case. There is no reason to parallelize an R
fallback when Arrow can already execute the complete grouped
calculation.

## 2. MapReduce: reduce in Arrow, finalize in R

Sometimes the final statistic is awkward or impossible to express as one
native Arrow grouped summary, but it can still be reconstructed from a
small fixed set of sufficient statistics.

A useful example is simple linear-regression inference. Fitting
`lm(y ~ x)` in R requires the raw observations, but the slope, its
standard error, and its p-value can be reconstructed from six compact
group-level quantities: `n`, `sum(x)`, `sum(y)`, `sum(x^2)`, `sum(y^2)`,
and `sum(x*y)`.

### How `.finalize` works

With `.strategy = "map_reduce"`, Arrow first computes the expressions
supplied in `.map_reduce`. The resulting small table is collected into R
as a tibble, with one row per group and columns corresponding to the
quantities named in `.map_reduce`.

That tibble is then passed as the **single argument** to `.finalize`. In
the example below,

``` r
.finalize = function(partials) {
  partials |>
    ...
}
```

`function(partials) { ... }` is simply an **anonymous R function**. The
name `partials` has no special meaning: it could just as well be called
`x`, `state`, or anything else. What matters is that the function
receives the compact tibble produced by the Arrow reduction step.

Conceptually,
[`summarise_big()`](https://larry77.github.io/summarisebig/reference/summarise_big.md)
does something like:

``` r

partials <- arrow_reduction |> collect()
result <- .finalize(partials)
```

The finalizer therefore works on the compact table of sufficient
statistics, not on the original raw observations. It must return a data
frame or tibble.

``` r

regression_mr <- summarise_big(
  ds,
  .by = grp,
  .strategy = "map_reduce",
  .map_reduce = list(
    n = ~ dplyr::n(),
    sum_x = ~ sum(x),
    sum_y = ~ sum(y),
    sum_x2 = ~ sum(x * x),
    sum_y2 = ~ sum(y * y),
    sum_xy = ~ sum(x * y)
  ),
  .finalize = function(partials) {
    partials |>
      mutate(
        centered_xx = sum_x2 - sum_x^2 / n,
        centered_yy = sum_y2 - sum_y^2 / n,
        centered_xy = sum_xy - sum_x * sum_y / n,
        slope = centered_xy / centered_xx,
        residual_ss = centered_yy - slope * centered_xy,
        slope_se = sqrt((residual_ss / (n - 2)) / centered_xx),
        t_value = slope / slope_se,
        p_value = 2 * stats::pt(-abs(t_value), df = n - 2)
      ) |>
      select(grp, slope, slope_se, p_value)
  }
)

regression_mr
```

Arrow performs the large-data reduction; only one compact row of
sufficient statistics per group enters ordinary R. The finalizer can
then use ordinary R functions such as
[`stats::pt()`](https://rdrr.io/r/stats/TDist.html) without
materializing the original groups.

This is the kind of problem for which `map_reduce` is intended. The
point is **not** to reimplement statistics that Arrow already supports
natively. For example, Arrow already has variance aggregation, so using
MapReduce merely to reconstruct a variance would normally be
unnecessary. If Arrow can compute the complete requested result
directly, the Arrow fast path remains preferable.

## 3. Arbitrary R functions: `parallel_chunks`

Some functions genuinely require the raw observations of each group.
Consider a normal R function that fits a regression and returns its
slope:

``` r

slope_r <- function(x, y) {
  unname(stats::coef(stats::lm(y ~ x))[[2]])
}
```

`parallel_chunks` keeps statistical groups intact, packs them into
bounded chunks, and allows different workers to materialize and process
different chunks at the same time.

``` r

slope_parallel <- summarise_big(
  ds,
  slope = slope_r(x, y),
  .by = grp,
  .strategy = "parallel_chunks",
  .workers = 2,
  .chunk_rows = 5,
  .try_arrow = FALSE
)

slope_parallel
```

`.try_arrow = FALSE` is used here only to demonstrate the materialized
execution path explicitly.

`parallel_chunks` is the throughput-oriented fallback. Its main cost is
memory: several workers may materialize different chunks simultaneously.

## 4. Shared-memory parallelism: `shared_chunk`

`shared_chunk` is the memory-oriented alternative. It materializes one
disk chunk at a time, makes complete groups contiguous, and divides
those groups into row-balanced tasks. With multiple workers, the
optional `mori` package is used to share the materialized chunk rather
than copying it to every worker.

With `mori` installed:

``` r

slope_shared <- summarise_big(
  ds,
  slope = slope_r(x, y),
  .by = grp,
  .strategy = "shared_chunk",
  .workers = 2,
  .chunk_rows = 10,
  .task_rows = 5,
  .try_arrow = FALSE
)

slope_shared
```

The outer chunk loop is deliberately sequential: only one large
materialized chunk is handled at a time. Parallelism occurs **within**
that shared chunk.

If `mori` is unavailable, `shared_chunk` can still be used with
`.workers = 1`.

## The strategies should not change the answer

Execution strategy is about *how* the calculation is performed, not its
mathematical result.

For a simple mean, we can deliberately force all four routes and compare
them. Materializing a mean is inefficient and is done here only as a
correctness demonstration.

``` r

native <- summarise_big(
  ds,
  mean_x = mean(x),
  .by = grp
) |>
  arrange(grp)

mr <- summarise_big(
  ds,
  .by = grp,
  .strategy = "map_reduce",
  .map_reduce = list(
    n = ~ dplyr::n(),
    sum_x = ~ sum(x)
  ),
  .finalize = function(partials) {
    partials |>
      mutate(mean_x = sum_x / n) |>
      select(grp, mean_x)
  }
) |>
  arrange(grp)

parallel <- summarise_big(
  ds,
  mean_x = mean(x),
  .by = grp,
  .strategy = "parallel_chunks",
  .workers = 2,
  .chunk_rows = 5,
  .try_arrow = FALSE
) |>
  arrange(grp)

shared <- summarise_big(
  ds,
  mean_x = mean(x),
  .by = grp,
  .strategy = "shared_chunk",
  .workers = 1,
  .chunk_rows = 10,
  .task_rows = 5,
  .try_arrow = FALSE
) |>
  arrange(grp)

stopifnot(
  isTRUE(all.equal(native, mr)),
  isTRUE(all.equal(native, parallel)),
  isTRUE(all.equal(native, shared))
)
```

The package test suite also checks invariance to chunk size, task size,
and worker count for deterministic summaries.

## Order-sensitive summaries

Arrow Dataset physical row order should not be treated as meaningful. If
an R summary depends on order, specify it explicitly with `.order_by`.

``` r

ordered_dat <- transform(
  dat,
  txt = paste0("row", row_id)
)

ordered_path <- tempfile("summarisebig-ordered-")
write_dataset(ordered_dat, ordered_path, format = "parquet")
ordered_ds <- open_dataset(ordered_path)

paste_in_order <- function(x) {
  paste(x, collapse = " -> ")
}

summarise_big(
  ordered_ds,
  result = paste_in_order(txt),
  .by = grp,
  .order_by = "row_id",
  .strategy = "parallel_chunks",
  .workers = 2,
  .chunk_rows = 5,
  .try_arrow = FALSE
)
```

## Reproducible performance experiments

Performance depends on hardware, storage, group sizes, and—most
importantly—on what the summary function actually does. Rather than
treating “large data” or “non-trivial functions” as abstract categories,
the examples below show the exact functions being timed.

The snippets are deliberately small enough to run on an ordinary
machine. If a run is too short to measure meaningfully, increase
`rows_per_group` or, for the CPU-heavy example, `cpu_reps`. The timing
numbers reported after the snippets come from the larger development
benchmark suite and are **illustrative, not performance guarantees**.

A fresh benchmark dataset can be created as follows:

``` r

set.seed(1)

n_groups <- 100L
rows_per_group <- 5000L
n <- n_groups * rows_per_group

bench_dat <- data.frame(
  grp = rep(seq_len(n_groups), each = rows_per_group),
  x = stats::rnorm(n)
)

bench_dat$y <- 0.7 * bench_dat$x + stats::rnorm(n)

bench_path <- tempfile("summarisebig-benchmark-")
write_dataset(bench_dat, bench_path, format = "parquet")
bench_ds <- open_dataset(bench_path)
```

A tiny helper keeps both the result and elapsed time:

``` r

time_run <- function(expr) {
  timing <- system.time(value <- force(expr))

  list(
    value = value,
    elapsed = unname(timing[["elapsed"]])
  )
}

same_result <- function(a, b, tolerance = 1e-8) {
  a <- as.data.frame(arrange(a, grp))
  b <- as.data.frame(arrange(b, grp))

  isTRUE(
    all.equal(
      a,
      b,
      tolerance = tolerance,
      check.attributes = FALSE
    )
  )
}
```

### Experiment 1: do not parallelize work Arrow can already do

Start with an ordinary grouped mean. Arrow can compute this directly, so
there is no reason to materialize raw groups merely to use R workers.

``` r

direct_arrow <- time_run(
  bench_ds |>
    summarise(mean_x = mean(x), .by = grp) |>
    collect()
)

summarisebig_arrow <- time_run(
  summarise_big(
    bench_ds,
    mean_x = mean(x),
    .by = grp
  )
)

forced_materialization <- time_run(
  summarise_big(
    bench_ds,
    mean_x = mean(x),
    .by = grp,
    .strategy = "parallel_chunks",
    .workers = 1,
    .chunk_rows = 100000,
    .try_arrow = FALSE
  )
)

stopifnot(
  same_result(direct_arrow$value, summarisebig_arrow$value),
  same_result(direct_arrow$value, forced_materialization$value)
)

data.frame(
  method = c(
    "direct Arrow",
    "summarise_big Arrow fast path",
    "forced R materialization"
  ),
  elapsed = c(
    direct_arrow$elapsed,
    summarisebig_arrow$elapsed,
    forced_materialization$elapsed
  )
)
```

`.try_arrow = FALSE` is used here **only for benchmarking**. In normal
use it would be counterproductive for a summary such as
[`mean()`](https://rdrr.io/r/base/mean.html).

In the development benchmark, direct Arrow took about **1.76 s** and the
[`summarise_big()`](https://larry77.github.io/summarisebig/reference/summarise_big.md)
Arrow fast path about **2.22 s**. The precise difference is
machine-specific; the important point is that both avoid raw-group
materialization entirely.

### Experiment 2: MapReduce versus fitting `lm()` inside every group

The earlier regression example can also be turned into a benchmark. Here
the ordinary-R version actually fits a separate model for every group
and extracts the slope p-value:

``` r

lm_p_value <- function(x, y) {
  fit <- summary(stats::lm(y ~ x))
  unname(fit$coefficients[2, 4])
}
```

The MapReduce version instead asks Arrow only for sufficient statistics
and lets R perform the small final inference calculation:

``` r

map_reduce_lm <- time_run(
  summarise_big(
    bench_ds,
    .by = grp,
    .strategy = "map_reduce",
    .map_reduce = list(
      n = ~ dplyr::n(),
      sum_x = ~ sum(x),
      sum_y = ~ sum(y),
      sum_x2 = ~ sum(x * x),
      sum_y2 = ~ sum(y * y),
      sum_xy = ~ sum(x * y)
    ),
    .finalize = function(partials) {
      partials |>
        mutate(
          centered_xx = sum_x2 - sum_x^2 / n,
          centered_yy = sum_y2 - sum_y^2 / n,
          centered_xy = sum_xy - sum_x * sum_y / n,
          slope = centered_xy / centered_xx,
          residual_ss = centered_yy - slope * centered_xy,
          slope_se = sqrt((residual_ss / (n - 2)) / centered_xx),
          t_value = slope / slope_se,
          result = 2 * stats::pt(-abs(t_value), df = n - 2)
        ) |>
        select(grp, result)
    }
  )
)

raw_lm <- time_run(
  summarise_big(
    bench_ds,
    result = lm_p_value(x, y),
    .by = grp,
    .strategy = "parallel_chunks",
    .workers = 1,
    .chunk_rows = 100000,
    .try_arrow = FALSE
  )
)

stopifnot(
  same_result(map_reduce_lm$value, raw_lm$value, tolerance = 1e-7)
)

data.frame(
  method = c("MapReduce", "per-group lm()"),
  elapsed = c(map_reduce_lm$elapsed, raw_lm$elapsed)
)
```

This is the central reason for the MapReduce route: the R finalizer
receives only one compact row of partial statistics per group rather
than all original observations.

The development suite used the closely related regression **slope**
calculation with the same sufficient-statistics idea. It took about
**2.20 s** with MapReduce versus **6.55 s** for one-worker raw-group
materialization with [`lm()`](https://rdrr.io/r/stats/lm.html). The
exact p-value example above is intentionally more demanding of the R
finalizer than the slope-only benchmark.

### Experiment 3: a cheap custom R function can become slower with more workers

This was the lightweight function used in the development benchmark:

``` r

custom_light <- function(x, y) {
  mean((x - y)^2 + abs(x), na.rm = TRUE)
}
```

We can time the same calculation at different worker counts. The helper
below also verifies that changing the worker count does not change the
result.

``` r

benchmark_workers <- function(fun, strategy) {
  workers <- c(1L, 2L, 4L)

  runs <- lapply(workers, function(w) {
    time_run(
      summarise_big(
        bench_ds,
        result = fun(x, y),
        .by = grp,
        .strategy = strategy,
        .workers = w,
        .chunk_rows = 100000,
        .task_rows = 25000,
        .try_arrow = FALSE
      )
    )
  })

  reference <- runs[[1]]$value

  stopifnot(
    all(vapply(
      runs[-1],
      function(run) same_result(reference, run$value),
      logical(1)
    ))
  )

  data.frame(
    strategy = strategy,
    workers = workers,
    elapsed = vapply(runs, function(run) run$elapsed, numeric(1))
  )
}

benchmark_workers(custom_light, "parallel_chunks")

if (requireNamespace("mori", quietly = TRUE)) {
  benchmark_workers(custom_light, "shared_chunk")
}
```

On the larger development benchmark, the exact same `custom_light()`
function produced:

| Strategy          | 1 worker | 2 workers | 4 workers |
|-------------------|---------:|----------:|----------:|
| `parallel_chunks` |   4.90 s |    6.48 s |    6.49 s |
| `shared_chunk`    |   8.45 s |   11.58 s |   11.77 s |

Nothing is wrong with the parallel machinery here: the useful R work is
simply too cheap. Worker startup, future scheduling, Parquet I/O, task
construction, serialization, and result combination cost more than the
parallelism saves.

### Experiment 4: give the workers enough CPU work and parallelism can pay

To test actual CPU scaling, the development suite used this
deterministic function:

``` r

custom_cpu <- function(x, y, reps = 25L) {
  z <- x - y
  acc <- 0

  for (k in seq_len(reps)) {
    a <- k / (reps + 1)

    acc <- acc + mean(
      sin(z * (1 + a))^2 +
        cos((x + y) / (1 + a))^2 +
        log1p(abs(z * a)),
      na.rm = TRUE
    )
  }

  acc / reps
}
```

You can run exactly the same worker-count experiment:

``` r

cpu_reps <- 25L

custom_cpu_benchmark <- function(x, y) {
  custom_cpu(x, y, reps = cpu_reps)
}

benchmark_workers(custom_cpu_benchmark, "parallel_chunks")

if (requireNamespace("mori", quietly = TRUE)) {
  benchmark_workers(custom_cpu_benchmark, "shared_chunk")
}
```

If these runs are still too short on your machine, increase `cpu_reps`
rather than assuming that adding workers must help.

With this exact function and `reps = 25`, the larger development run
produced:

| Strategy          | 1 worker | 2 workers | 4 workers |
|-------------------|---------:|----------:|----------:|
| `parallel_chunks` |  26.79 s |   17.86 s |   12.48 s |
| `shared_chunk`    |  30.04 s |   24.43 s |   19.51 s |

Here there is enough ordinary-R computation to amortize the parallel
overhead. `parallel_chunks` achieved about a **2.15x** speedup from one
to four workers in that run, while `shared_chunk` achieved about
**1.54x**.

The two strategies optimize different things: `parallel_chunks` is
generally the throughput-oriented choice, while `shared_chunk` is
designed to reduce simultaneous private copies of a materialized chunk.

### Experiment 5: even a substantial raw-data calculation need not scale

Exact quantiles are a useful counterexample because they require the raw
group values and sorting work, but the overall workload can still be
dominated by materialization and coordination costs.

The development benchmark used:

``` r

custom_quantile <- function(x, y) {
  z <- (x - y)^2 + sin(x) + log1p(abs(y))

  qs <- stats::quantile(
    z,
    probs = c(0.10, 0.50, 0.90, 0.99),
    na.rm = TRUE,
    names = FALSE,
    type = 7
  )

  qs[[4L]] - qs[[1L]] + qs[[3L]] - qs[[2L]]
}
```

``` r

benchmark_workers(custom_quantile, "parallel_chunks")

if (requireNamespace("mori", quietly = TRUE)) {
  benchmark_workers(custom_quantile, "shared_chunk")
}
```

The corresponding development timings were:

| Strategy          | 1 worker | 2 workers | 4 workers |
|-------------------|---------:|----------:|----------:|
| `parallel_chunks` |   5.49 s |    6.13 s |    6.33 s |
| `shared_chunk`    |   8.39 s |   13.54 s |   13.35 s |

So the practical rule is not “use more workers for difficult functions”.
It is: **benchmark the actual function, group structure, and storage
layout that matter to your analysis.**

Taken together, the experiments support the package’s execution
hierarchy:

1.  **Native Arrow first** when it can express the complete grouped
    result.
2.  **MapReduce next** when compact Arrow-computable partial statistics
    are sufficient and only a small final calculation needs R.
3.  **Raw-group materialization last**, using `parallel_chunks` or
    `shared_chunk` when the statistic genuinely requires the
    observations themselves.

Parallelism is an optimization *within* the third case, not a substitute
for avoiding materialization in the first place.

## Choosing a strategy

| Situation | Preferred route |
|----|----|
| Arrow translates the complete grouped expression | Arrow fast path |
| Final answer needs R but fixed-size Arrow state is sufficient | `map_reduce` |
| Arbitrary R needs raw rows; throughput is the main concern | `parallel_chunks` |
| Arbitrary R needs raw rows; limiting simultaneous materialization matters | `shared_chunk` |

A complete statistical group is never split by the materialized
strategies. Therefore, if an arbitrary R function requires all
observations in a group, that individual group must fit in memory. If it
does not, the calculation needs a decomposable formulation rather than a
smaller chunk size.

## Future work

One natural extension is a **dictionary of MapReduce recipes** for
common statistics. Many useful calculations can be reconstructed from a
small set of moments or sufficient statistics, but writing the
decomposition by hand is repetitive.

Possible built-in recipes could focus on calculations where Arrow can
perform the reduction efficiently but the complete user-facing statistic
needs a final R step, for example:

- linear-regression inference from sufficient statistics, including
  standard errors and p-values;
- higher-moment or shape statistics when the requested final form is not
  natively translated by Arrow;
- other domain-specific statistics with mergeable fixed-size state.

Recipes should not duplicate native Arrow functionality merely for the
sake of using MapReduce.

A future interface could let users select a known recipe while still
exposing the current `.map_reduce` / `.finalize` mechanism for custom
decompositions. Such a dictionary should complement, not replace, Arrow
translation: if Arrow can execute the complete expression natively, the
Arrow fast path should remain the first choice.

Another possible direction is richer diagnostics explaining *why* a
calculation used Arrow, MapReduce, or materialization, and where time
and memory were spent.

## Documentation and development

The package includes a getting-started vignette:

``` r

vignette("summarisebig", package = "summarisebig")
```

The repository also contains the benchmark scripts used during
development. Large generated benchmark datasets are not included in the
built CRAN source package.
