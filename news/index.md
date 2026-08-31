# Changelog

## summarisebig 0.1.0

### Initial release

- Arrow-first grouped summaries via
  [`summarise_big()`](https://larry77.github.io/summarisebig/reference/summarise_big.md).
- Explicit Arrow reduction plus R finalization with
  `strategy = "map_reduce"`.
- Bounded complete-group materialization via `parallel_chunks` and
  `shared_chunk`.
- Optional Mirai/Futurize parallelism and Mori shared memory.
- User functions referenced inside captured summary quosures are
  identified with
  [`future::getGlobalsAndPackages()`](https://future.futureverse.org/reference/getGlobalsAndPackages.html)
  and supplied explicitly to futurized workers.
- The caller’s Future plan is restored after every exit path.
- Default worker count is 2.
- Added a runnable Getting Started vignette covering all four execution
  paths.
- Added pkgdown configuration and GitHub setup instructions for the
  project website and continuous `R CMD check`.
