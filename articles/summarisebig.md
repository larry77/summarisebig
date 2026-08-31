# Getting started with summarisebig

``` r

library(arrow)
#> 
#> Attaching package: 'arrow'
#> The following object is masked from 'package:utils':
#> 
#>     timestamp
library(dplyr)
#> 
#> Attaching package: 'dplyr'
#> The following objects are masked from 'package:stats':
#> 
#>     filter, lag
#> The following objects are masked from 'package:base':
#> 
#>     intersect, setdiff, setequal, union
library(summarisebig)
```

## What problem does `summarisebig` solve?

Arrow is very good at filtering, transforming, grouping, and aggregating
data without first loading an entire dataset into an R data frame. The
difficult case is when a grouped calculation eventually needs ordinary R
code that Arrow cannot execute directly.

[`summarise_big()`](https://larry77.github.io/summarisebig/reference/summarise_big.md)
uses the following hierarchy:

1.  Try to compute the complete grouped summary in Arrow.
2.  If the answer can be reconstructed from compact Arrow-computable
    state, use `.strategy = "map_reduce"` and let R perform only the
    final combination.
3.  If the custom R function genuinely needs raw observations,
    materialize complete groups in bounded chunks with `parallel_chunks`
    or `shared_chunk`.

The examples below use a tiny Parquet dataset so that they are safe to
run in a vignette. The same interface is intended for datasets much
larger than memory.

``` r

d <- data.frame(
  grp = rep(c("A", "B", "C"), each = 4),
  row_id = rep(1:4, 3),
  x = c(1, 2, 4, 8, 10, 12, 14, 18, 3, 6, 9, 15),
  y = c(2, 3, 5, 9, 9, 13, 15, 20, 5, 5, 11, 14),
  txt = c("alpha", "bravo", "charlie", "delta",
          "echo", "foxtrot", "golf", "hotel",
          "india", "juliet", "kilo", "lima")
)

path <- tempfile("summarisebig-vignette-")
arrow::write_dataset(d, path, format = "parquet")
ds <- arrow::open_dataset(path)
```

## 1. Let Arrow do everything when it can

The ordinary strategies first try the entire expression in Arrow. For a
supported grouped summary, raw observations never need to become an R
data frame.

``` r

summarise_big(
  ds,
  result = mean(x),
  .by = grp
) |>
  arrange(grp)
#> # A tibble: 3 × 2
#>   grp   result
#>   <chr>  <dbl>
#> 1 A       3.75
#> 2 B      13.5 
#> 3 C       8.25
```

A fairly complicated expression can still remain entirely in Arrow if
all its parts can be translated by Arrow. Complexity by itself is
therefore not a reason to use MapReduce or materialization.

## 2. Go as far as possible in Arrow, then finalize in R

Sometimes the final statistic is awkward to express as one native Arrow
grouped summary, but it can still be reconstructed from a small fixed
set of sufficient statistics.

A useful example is simple linear-regression inference. Fitting
`lm(y ~ x)` in R requires the raw observations, but the slope, its
standard error, and its p-value can be reconstructed from six compact
group-level quantities: `n`, `sum(x)`, `sum(y)`, `sum(x^2)`, `sum(y^2)`,
and `sum(x*y)`.

``` r

summarise_big(
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
  .finalize = function(state) {
    state |>
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
) |>
  arrange(grp)
#> # A tibble: 3 × 4
#>   grp   slope slope_se p_value
#>   <chr> <dbl>    <dbl>   <dbl>
#> 1 A     1        0     0      
#> 2 B     1.33     0.118 0.00777
#> 3 C     0.829    0.206 0.0566
```

Arrow performs the large-data reduction; only one compact row of
sufficient statistics per group enters ordinary R. The finalizer can
then use ordinary R functions such as
[`stats::pt()`](https://rdrr.io/r/stats/TDist.html) without
materializing the original groups.

`map_reduce` does **not** discover the decomposition automatically. The
user supplies the Arrow-computable building blocks and the R finalizer.
This is often the most attractive route after pure Arrow because the
large-data work remains inside Arrow and only a few numbers per group
cross into R.

The point is not to reimplement statistics that Arrow already supports
natively. For example, Arrow already has variance aggregation, so using
MapReduce merely to reconstruct a variance would normally be
unnecessary.

## 3. Arbitrary R functions: `parallel_chunks`

Some summaries genuinely need the raw values. Here is an intentionally
ordinary R function based on exact quantiles:

``` r

interquartile_span <- function(x) {
  q <- stats::quantile(
    x,
    probs = c(0.25, 0.75),
    names = FALSE,
    type = 7
  )
  q[[2]] - q[[1]]
}
```

We disable the Arrow fast path in this example so that the materialized
route is exercised explicitly. Complete groups are packed into bounded
chunks and separate chunks can be processed by different Mirai workers.

``` r

summarise_big(
  ds,
  result = interquartile_span(x),
  .by = grp,
  .strategy = "parallel_chunks",
  .workers = 2,
  .chunk_rows = 4,
  .try_arrow = FALSE
) |>
  arrange(grp)
#> # A tibble: 3 × 2
#>   grp   result
#>   <chr>  <dbl>
#> 1 A       3.25
#> 2 B       3.5 
#> 3 C       5.25
```

For expensive user-defined R functions this strategy can benefit
substantially from multiple workers. For very cheap functions, parallel
startup and scheduling overhead can dominate, so more workers are not
automatically faster.

## 4. Lower-memory materialization: `shared_chunk`

`shared_chunk` materializes only one disk chunk at a time. It first
sorts the chunk so that each group is contiguous, then divides complete
groups into row-balanced tasks. With multiple workers and the optional
`mori` package, the chunk can be shared rather than copied to every
worker.

The following order-sensitive string summary also demonstrates why
`.order_by` exists: Arrow Dataset physical row order should not be
treated as meaningful.

``` r

initials_in_order <- function(txt) {
  paste0(substr(txt, 1, 1), collapse = "")
}

summarise_big(
  ds,
  result = initials_in_order(txt),
  .by = grp,
  .order_by = "row_id",
  .strategy = "shared_chunk",
  .workers = 1,
  .chunk_rows = 8,
  .task_rows = 4,
  .try_arrow = FALSE
) |>
  arrange(grp)
#>   grp result
#> 1   A   abcd
#> 2   B   efgh
#> 3   C   ijkl
```

Using one worker above keeps the vignette independent of optional
shared-memory support. With `mori` installed, the same call can use
multiple workers:

``` r

summarise_big(
  ds,
  result = initials_in_order(txt),
  .by = grp,
  .order_by = "row_id",
  .strategy = "shared_chunk",
  .workers = 2,
  .chunk_rows = 8,
  .task_rows = 4,
  .try_arrow = FALSE
)
```

## Choosing a strategy

A useful decision rule is:

| Situation | Preferred route |
|----|----|
| Arrow can translate the complete expression | Arrow fast path |
| Final answer needs R but compact Arrow state is sufficient | `map_reduce` |
| Arbitrary R needs raw rows and throughput matters most | `parallel_chunks` |
| Arbitrary R needs raw rows and peak memory matters most | `shared_chunk` |

For the two materialized strategies, a complete statistical group is
never split. Therefore a genuinely opaque R calculation still requires
each individual group to fit in memory. If one group is itself too
large, the calculation must be reformulated in Arrow or as a compact
reduction if that is mathematically possible.
