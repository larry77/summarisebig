# Summarise a large Arrow dataset by group

`summarise_big()` is designed for grouped computations on Arrow datasets
when the result may be expressible in Arrow, reconstructible from
compact Arrow-computable state, or require arbitrary R code on complete
groups.

## Usage

``` r
summarise_big(
  .data,
  ...,
  .by,
  .order_by = NULL,
  .workers = 2L,
  .strategy = c("parallel_chunks", "shared_chunk", "map_reduce"),
  .chunk_rows = 1e+06,
  .task_rows = 1e+05,
  .oversize = c("error", "warning"),
  .map_reduce = NULL,
  .finalize = NULL,
  .seed = TRUE,
  .try_arrow = TRUE,
  .tmp = tempfile("summarise-big-"),
  .keep_tmp = FALSE
)
```

## Arguments

- .data:

  An Arrow Dataset or Arrow object supporting dplyr operations.

- ...:

  Named grouped summary expressions for the ordinary strategies.

- .by:

  A single grouping column.

- .order_by:

  Optional character vector of columns defining deterministic order
  within groups for order-sensitive R summaries.

- .workers:

  Number of local Mirai workers. Parallel package tests should use at
  most two workers. Defaults to 2.

- .strategy:

  One of `"parallel_chunks"`, `"shared_chunk"`, or `"map_reduce"`.

- .chunk_rows:

  Target maximum number of rows per materialized disk chunk. Complete
  groups are never split.

- .task_rows:

  Target rows per worker task within `shared_chunk`. Complete groups are
  never split.

- .oversize:

  What to do when one group exceeds `.chunk_rows`.

- .map_reduce:

  Named list of Arrow-computable grouped reduction expressions, normally
  supplied as one-sided formulas.

- .finalize:

  R function applied to the compact table produced by `.map_reduce`. It
  must return a data frame.

- .seed:

  Passed to
  [`futurize::futurize()`](https://futurize.futureverse.org/reference/futurize.html)
  for parallel random-number handling.

- .try_arrow:

  If `TRUE`, ordinary strategies attempt the complete summary in Arrow
  before materializing any raw groups.

- .tmp:

  Temporary directory used for group-safe Parquet repartitioning.

- .keep_tmp:

  If `TRUE`, retain the temporary dataset after completion.

## Value

A data frame containing grouped summary results.

## Details

The ordinary strategies first attempt the complete summary lazily in
Arrow. If Arrow cannot execute it, `parallel_chunks` materializes
different group-safe chunks independently, while `shared_chunk`
materializes one chunk at a time and can share it between workers with
`mori`.

With `strategy = "map_reduce"`, Arrow computes the expressions supplied
in `.map_reduce`, only that reduced table is collected, and `.finalize`
runs in ordinary R.

## Examples

``` r
tab <- arrow::Table$create(data.frame(
  grp = c("a", "a", "b", "b"),
  x = c(1, 2, 10, 20)
))

# Arrow fast path.
summarise_big(tab, result = mean(x), .by = grp)
#> # A tibble: 2 × 2
#>   grp   result
#>   <chr>  <dbl>
#> 1 a        1.5
#> 2 b       15  

# Arrow reduction followed by an ordinary R finalizer.
summarise_big(
  tab,
  .by = grp,
  .strategy = "map_reduce",
  .map_reduce = list(n = ~ dplyr::n(), sx = ~ sum(x)),
  .finalize = function(d) {
    dplyr::mutate(d, result = sx / n)
  }
)
#> # A tibble: 2 × 4
#>   grp       n    sx result
#>   <chr> <int> <dbl>  <dbl>
#> 1 a         2     3    1.5
#> 2 b         2    30   15  

if (FALSE) { # \dontrun{
# Arbitrary R function on complete groups in a Parquet Dataset.
ds <- arrow::open_dataset("data")
summarise_big(
  ds,
  result = my_custom_function(x, y),
  .by = grp,
  .strategy = "parallel_chunks",
  .workers = 2
)
} # }
```
