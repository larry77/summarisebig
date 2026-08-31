#!/usr/bin/env bash
set -euo pipefail

if ! command -v R >/dev/null 2>&1; then
  echo "R is not on PATH" >&2
  exit 1
fi

if ! command -v Rscript >/dev/null 2>&1; then
  echo "Rscript is not on PATH" >&2
  exit 1
fi

Rscript -e 'if (!requireNamespace("devtools", quietly = TRUE)) stop("Install devtools first"); devtools::document()'
R CMD build .
TARBALL=$(ls -1t summarisebig_*.tar.gz | head -n 1)
R CMD check --as-cran "$TARBALL"
