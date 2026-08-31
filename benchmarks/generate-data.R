args <- commandArgs(trailingOnly = TRUE)

if (length(args) < 4L) {
  stop(
    paste0(
      "Usage: Rscript generate-data.R OUT_DIR N_ROWS N_GROUPS ROWS_PER_FILE ",
      "[balanced|skewed] [SEED]\n\n",
      "Example:\n",
      "  Rscript generate-data.R data/bench-10m 10000000 10000 1000000 balanced 1"
    ),
    call. = FALSE
  )
}

if (!requireNamespace("arrow", quietly = TRUE)) {
  stop("Package `arrow` is required.", call. = FALSE)
}

out_dir       <- args[[1L]]
n_rows        <- as.numeric(args[[2L]])
n_groups      <- as.integer(args[[3L]])
rows_per_file <- as.integer(args[[4L]])
distribution  <- if (length(args) >= 5L) args[[5L]] else "balanced"
seed          <- if (length(args) >= 6L) as.integer(args[[6L]]) else 1L

if (!distribution %in% c("balanced", "skewed")) {
  stop("Distribution must be `balanced` or `skewed`.", call. = FALSE)
}
if (!is.finite(n_rows) || n_rows < 1) stop("N_ROWS must be positive.", call. = FALSE)
if (is.na(n_groups) || n_groups < 1L) stop("N_GROUPS must be positive.", call. = FALSE)
if (is.na(rows_per_file) || rows_per_file < 1L) stop("ROWS_PER_FILE must be positive.", call. = FALSE)

if (dir.exists(out_dir) && length(list.files(out_dir, all.files = TRUE, no.. = TRUE))) {
  stop("Output directory already exists and is not empty: ", out_dir, call. = FALSE)
}
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

set.seed(seed)

n_files <- ceiling(n_rows / rows_per_file)
cat(sprintf("Generating %s rows in %d Parquet file(s)...\n", format(n_rows, big.mark = ",", scientific = FALSE), n_files))

for (i in seq_len(n_files)) {
  first <- (i - 1) * rows_per_file + 1
  last  <- min(i * rows_per_file, n_rows)
  n     <- as.integer(last - first + 1)

  row_id <- first:last

  if (distribution == "balanced") {
    grp <- as.integer(((row_id - 1) %% n_groups) + 1)
  } else {
    # Deliberately uneven group sizes.  The cube produces a long-tailed
    # distribution while keeping generation streaming and inexpensive.
    u <- runif(n)
    grp <- as.integer(pmin(n_groups, 1 + floor(n_groups * u^3)))
  }

  # Numeric columns with both group structure and row-level noise.
  # They are deterministic for a fixed seed and generation configuration.
  x <- sin(row_id * 0.00013) + (grp %% 17L) / 10 + rnorm(n, sd = 0.75)
  y <- cos(row_id * 0.00007) - (grp %% 11L) / 20 + rnorm(n, sd = 0.50)

  d <- data.frame(
    grp = grp,
    row_id = as.numeric(row_id),
    x = x,
    y = y
  )

  path <- file.path(out_dir, sprintf("part-%05d.parquet", i))
  arrow::write_parquet(d, path, compression = "snappy")

  rm(d, grp, x, y, row_id)
  gc(FALSE)

  cat(sprintf("  %d/%d: rows %s-%s\n", i, n_files,
              format(first, big.mark = ","), format(last, big.mark = ",")))
}

metadata <- data.frame(
  n_rows = n_rows,
  n_groups_requested = n_groups,
  rows_per_file = rows_per_file,
  n_files = n_files,
  distribution = distribution,
  seed = seed,
  compression = "snappy",
  generated_at = format(Sys.time(), tz = "UTC", usetz = TRUE)
)

utils::write.csv(metadata, file.path(out_dir, "_benchmark_metadata.csv"), row.names = FALSE)
cat("Done: ", normalizePath(out_dir), "\n", sep = "")
