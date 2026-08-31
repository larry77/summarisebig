# GitHub and pkgdown setup

This file is for repository setup and is excluded from the CRAN source package.

## Recommended route

The package is already prepared for pkgdown through `_pkgdown.yml`.  Once the
local project has been connected to a GitHub repository, let `usethis` install
the current recommended GitHub Actions workflows rather than copying a workflow
that may become stale.

From the package root in R:

```r
install.packages(c("usethis", "pkgdown"))
```

### If this directory is not already a Git repository

```r
usethis::use_git()
```

Commit the files when prompted / after `use_git()` returns.

### Create the public GitHub repository automatically

With GitHub credentials configured for `usethis`/`gh`:

```r
usethis::use_github(private = FALSE, protocol = "https")
```

This creates the GitHub repository, adds it as `origin`, pushes the current
branch, and adds GitHub links to `DESCRIPTION`.

If you prefer to create the repository in the GitHub web UI, create an empty
public repository named `summarisebig` (do not add another README, license, or
.gitignore), then add it as the local `origin`, push `main`, and run:

```r
usethis::use_github_links()
```

### Add continuous R CMD check

```r
usethis::use_github_action("check-standard")
```

This installs the standard r-lib/actions R CMD check workflow, including Linux,
macOS, Windows, R-devel, current R, and oldrel checks.

### Configure and publish the pkgdown website

```r
usethis::use_pkgdown_github_pages()
```

This is the recommended one-command pkgdown/GitHub setup. It configures GitHub
Pages, installs the current pkgdown GitHub Actions workflow, and adds the final
website URL to `_pkgdown.yml`, `DESCRIPTION`, and the GitHub repository.

After it changes files, inspect and commit them:

```bash
git status
git add .
git commit -m "Set up GitHub Actions and pkgdown site"
git push
```

### Preview the site locally

At any time:

```r
pkgdown::build_site()
```

The home page comes from `README.md`, the function reference from `man/`, and
the Getting Started page from `vignettes/summarisebig.Rmd`.

## GitHub Pages URL

For a repository named `summarisebig`, the default project-site URL will be of
the form:

```text
https://GITHUB-USERNAME.github.io/summarisebig/
```

`usethis::use_pkgdown_github_pages()` determines the actual account name from
the repository remote and writes the correct URL automatically.
