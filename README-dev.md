# Development notes

Before CRAN submission:

1.  Run `./check-local.sh` and confirm 0 ERRORs and 0 WARNINGs.
2.  Test with the GitHub Actions `check-standard` workflow (Linux,
    macOS, Windows, R-devel, current R, and oldrel).
3.  Explain only unavoidable NOTEs in `cran-comments.md`.
4.  Keep benchmark data and scripts out of the built source package.
5.  Verify the package name against current/historical CRAN and
    Bioconductor before the first submission.

`check-local.sh` deliberately does not call `devtools::test()`
separately. `R CMD check` installs the package and then runs the test
suite in the same installed-package context CRAN uses. This avoids
misleading warnings from Mirai workers trying to load a package that
[`pkgload::load_all()`](https://pkgload.r-lib.org/reference/load_all.html)
has simulated but not actually installed.

## Parallel globals

The package keeps `purrr::map() |> futurize::futurize()`. Before
launching workers,
[`summarise_big()`](https://larry77.github.io/summarisebig/reference/summarise_big.md)
uses
[`future::getGlobalsAndPackages()`](https://future.futureverse.org/reference/getGlobalsAndPackages.html)
on the captured summary expressions and passes the resulting
globals/packages explicitly to `futurize()`. This is needed because
user-defined functions can be hidden inside rlang quosures.

## Website

`_pkgdown.yml` is committed to the repository. After the GitHub remote
exists, follow `GITHUB-SETUP.md` and run
`usethis::use_pkgdown_github_pages()` so that usethis installs the
current recommended deployment workflow and fills in the real
repository/site URLs.
