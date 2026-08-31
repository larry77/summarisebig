args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 1L) stop("Usage: Rscript summarize-results.R RESULTS_DIR", call. = FALSE)
results_dir <- args[[1L]]

`%||%` <- function(x, y) {
  if (is.null(x) || !length(x) || is.na(x)) y else x
}

read_metrics <- function(path) {
  if (!file.exists(path)) return(list())
  x <- readLines(path, warn = FALSE)
  parts <- strsplit(x, "=", fixed = TRUE)
  out <- lapply(parts, function(z) {
    if (length(z) < 2L) return(NA_character_)
    paste(z[-1L], collapse = "=")
  })
  names(out) <- vapply(parts, `[[`, character(1), 1L)
  out
}

csvs <- list.files(
  results_dir,
  pattern = "__w[0-9]+\\.csv$",
  full.names = TRUE
)

if (!length(csvs)) {
  stop("No benchmark result CSV files found in ", results_dir, call. = FALSE)
}

rows <- lapply(csvs, function(csv) {
  d <- utils::read.csv(csv, stringsAsFactors = FALSE)
  metrics <- read_metrics(sub("\\.csv$", ".metrics", csv))

  d$wall_sec <- as.numeric(metrics$wall_sec %||% NA_character_)
  d$peak_tree_pss_kb <- as.numeric(metrics$peak_tree_pss_kb %||% NA_character_)
  d$peak_tree_pss_gb <- d$peak_tree_pss_kb / 1024^2
  d$peak_tree_rss_sum_kb <- as.numeric(metrics$peak_tree_rss_sum_kb %||% NA_character_)
  d$peak_processes <- as.integer(metrics$peak_processes %||% NA_character_)
  d$exit_status <- as.integer(metrics$exit_status %||% NA_character_)
  d
})

all_names <- unique(unlist(lapply(rows, names)))
rows <- lapply(rows, function(d) {
  missing <- setdiff(all_names, names(d))
  for (nm in missing) d[[nm]] <- NA
  d[all_names]
})

result <- do.call(rbind, rows)
result <- result[order(result$case, result$workers), , drop = FALSE]
row.names(result) <- NULL

out_path <- file.path(results_dir, "benchmark-summary.csv")
utils::write.csv(result, out_path, row.names = FALSE)

show <- result[, intersect(
  c("case", "workers", "wall_sec", "peak_tree_pss_gb", "r_elapsed_sec", "n_groups_returned", "result_mean"),
  names(result)
), drop = FALSE]

print(show, row.names = FALSE, digits = 4)
cat("\nWrote: ", normalizePath(out_path), "\n", sep = "")
