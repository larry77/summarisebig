## R CMD check results

Local `R CMD check --as-cran` on Debian trixie / R 4.6.1:

* 0 ERRORs
* 0 WARNINGs
* 1 NOTE: new submission

Package tests use no more than two workers. Large performance benchmarks are
excluded from the built source package and are not run during `R CMD check`.

The package keeps `purrr::map() |> futurize::futurize()`. `furrr` is declared
because it is the backend used by `futurize` for `purrr` transpilation. User
functions referenced inside captured summary quosures are identified with
`future::getGlobalsAndPackages()` and supplied explicitly to workers.
