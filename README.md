# summarisebig

`summarisebig` provides a `dplyr`-like interface for grouped summaries on
large Arrow datasets, especially when the summary includes ordinary user-defined
R functions.

The guiding rule is simple:

> **Keep computation in Arrow as long as possible, and move raw observations
> into R only when the requested summary genuinely requires them.**

```text
Can Arrow compute the whole summary?
        | yes -> Arrow fast path
        | no
        v
Can compact Arrow-computable state be finalized in R?
        | yes -> map_reduce
        | no
        v
Does arbitrary R need the raw rows?
        -> parallel_chunks or shared_chunk
```

## Basic use

```r
library(arrow)
library(summarisebig)

# ds <- open_dataset("my-parquet-data")

summarise_big(
  ds,
  result = my_custom_function(x, y),
  .by = grp
)
```

The ordinary strategies first try to execute the complete summary in Arrow.
If that succeeds, only the final grouped result is collected.

## MapReduce-style reduction

When the final calculation is not directly Arrow-executable but can be
reconstructed from compact sufficient statistics, let Arrow do the large-data
reduction and use R only for the final combination:

```r
summarise_big(
  ds,
  .by = grp,
  .strategy = "map_reduce",
  .map_reduce = list(
    n = ~ dplyr::n(),
    sx = ~ sum(x),
    sx2 = ~ sum(x * x)
  ),
  .finalize = function(d) {
    dplyr::mutate(
      d,
      variance = (sx2 - sx * sx / n) / (n - 1)
    )
  }
)
```

## When raw rows are unavoidable

- `parallel_chunks` materializes different complete-group chunks in different
  workers. It is the throughput-oriented fallback.
- `shared_chunk` materializes one chunk at a time and, with `mori`, lets several
  workers process tasks from shared memory. It is the memory-oriented fallback.
- `map_reduce` avoids raw-group materialization entirely when a compact Arrow
  reduction is mathematically possible.

The default is two workers. Set `.workers = 1` for fully sequential execution,
or choose a larger value explicitly when the workload and machine justify it.

## Documentation

The **Getting started** vignette contains runnable examples of the Arrow fast
path, MapReduce, `parallel_chunks`, and `shared_chunk`:

```r
vignette("summarisebig", package = "summarisebig")
```

A pkgdown website is prepared in `_pkgdown.yml`. After the GitHub repository is
created, `usethis::use_pkgdown_github_pages()` can configure automatic website
publication.

## Development status

This is the 0.1.0 release candidate. The package has been exercised with
`R CMD check --as-cran`; large benchmark datasets and benchmark scripts are
kept outside the built CRAN source package.
