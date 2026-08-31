args <- commandArgs(trailingOnly = TRUE)

if (length(args) < 8L) {
  stop(
    paste0(
      "Usage: Rscript benchmark-one.R FUNCTION_FILE DATASET_DIR CASE WORKERS ",
      "CHUNK_ROWS TASK_ROWS TMP_ROOT RESULT_CSV\n"
    ),
    call. = FALSE
  )
}

function_file <- args[[1L]]
dataset_dir  <- args[[2L]]
case          <- args[[3L]]
workers       <- as.integer(args[[4L]])
chunk_rows    <- as.numeric(args[[5L]])
task_rows     <- as.numeric(args[[6L]])
tmp_root      <- args[[7L]]
result_csv    <- args[[8L]]

required <- c("arrow", "dplyr")
missing <- required[!vapply(required, requireNamespace, logical(1), quietly = TRUE)]
if (length(missing)) stop("Missing package(s): ", paste(missing, collapse = ", "), call. = FALSE)

source(function_file, local = .GlobalEnv)
if (!exists("summarise_big", mode = "function")) stop("`summarise_big()` was not found after sourcing the function file.", call. = FALSE)

dir.create(dirname(result_csv), recursive = TRUE, showWarnings = FALSE)
dir.create(tmp_root, recursive = TRUE, showWarnings = FALSE)

# Ignore the metadata CSV when opening the Parquet Dataset.
parquet_files <- list.files(dataset_dir, pattern = "\\.parquet$", full.names = TRUE)
if (!length(parquet_files)) stop("No Parquet files found in: ", dataset_dir, call. = FALSE)
ds <- arrow::open_dataset(parquet_files, format = "parquet")

# Deliberately ordinary R functions.  The benchmark forces the materialised
# strategies with .try_arrow = FALSE, so these workloads cannot accidentally
# become native Arrow computations.
custom_light <- function(x, y) {
  mean((x - y)^2 + abs(x), na.rm = TRUE)
}

custom_heavy <- function(x, y) {
  z <- (x - y)^2 + sin(x) + log1p(abs(y))
  unname(stats::quantile(z, probs = 0.90, na.rm = TRUE, names = FALSE, type = 7))
}

custom_var <- function(x) {
  stats::var(x, na.rm = TRUE)
}

run_case <- function() {
  tmp <- file.path(tmp_root, paste0(case, "-w", workers, "-", Sys.getpid()))

  switch(
    case,

    arrow_native = {
      ds |>
        dplyr::summarise(result = mean(x), .by = grp) |>
        dplyr::collect()
    },

    sb_auto_native = {
      summarise_big(
        ds,
        result = mean(x),
        .by = grp,
        .workers = workers,
        .tmp = tmp
      )
    },

    collect_custom_light = {
      ds |>
        dplyr::collect() |>
        dplyr::summarise(result = custom_light(x, y), .by = grp)
    },

    parallel_custom_light = {
      summarise_big(
        ds,
        result = custom_light(x, y),
        .by = grp,
        .strategy = "parallel_chunks",
        .workers = workers,
        .chunk_rows = chunk_rows,
        .task_rows = task_rows,
        .try_arrow = FALSE,
        .tmp = tmp
      )
    },

    shared_custom_light = {
      summarise_big(
        ds,
        result = custom_light(x, y),
        .by = grp,
        .strategy = "shared_chunk",
        .workers = workers,
        .chunk_rows = chunk_rows,
        .task_rows = task_rows,
        .try_arrow = FALSE,
        .tmp = tmp
      )
    },

    parallel_custom_heavy = {
      summarise_big(
        ds,
        result = custom_heavy(x, y),
        .by = grp,
        .strategy = "parallel_chunks",
        .workers = workers,
        .chunk_rows = chunk_rows,
        .task_rows = task_rows,
        .try_arrow = FALSE,
        .tmp = tmp
      )
    },

    shared_custom_heavy = {
      summarise_big(
        ds,
        result = custom_heavy(x, y),
        .by = grp,
        .strategy = "shared_chunk",
        .workers = workers,
        .chunk_rows = chunk_rows,
        .task_rows = task_rows,
        .try_arrow = FALSE,
        .tmp = tmp
      )
    },

    mapreduce_var = {
      summarise_big(
        ds,
        .by = grp,
        .strategy = "map_reduce",
        .map_reduce = list(
          n = ~ sum(!is.na(x)),
          sx = ~ sum(x, na.rm = TRUE),
          sx2 = ~ sum(x^2, na.rm = TRUE)
        ),
        .finalize = function(d) {
          dplyr::transmute(
            d,
            grp = grp,
            result = (sx2 - sx^2 / n) / (n - 1)
          )
        }
      )
    },

    parallel_var = {
      summarise_big(
        ds,
        result = custom_var(x),
        .by = grp,
        .strategy = "parallel_chunks",
        .workers = workers,
        .chunk_rows = chunk_rows,
        .task_rows = task_rows,
        .try_arrow = FALSE,
        .tmp = tmp
      )
    },

    shared_var = {
      summarise_big(
        ds,
        result = custom_var(x),
        .by = grp,
        .strategy = "shared_chunk",
        .workers = workers,
        .chunk_rows = chunk_rows,
        .task_rows = task_rows,
        .try_arrow = FALSE,
        .tmp = tmp
      )
    },

    stop("Unknown benchmark case: ", case, call. = FALSE)
  )
}

# Timed here as well as externally by /usr/bin/time.  The external timing is
# the primary end-to-end measurement; this value is useful as a cross-check.
gc()
t <- system.time({
  result <- run_case()
})

# Canonicalise group order before producing lightweight result diagnostics.
if ("grp" %in% names(result)) result <- result[order(result$grp), , drop = FALSE]

numeric_result <- if ("result" %in% names(result) && is.numeric(result$result)) result$result else numeric()

out <- data.frame(
  case = case,
  workers = workers,
  chunk_rows = chunk_rows,
  task_rows = task_rows,
  n_groups_returned = nrow(result),
  r_elapsed_sec = unname(t[["elapsed"]]),
  result_sum = if (length(numeric_result)) sum(numeric_result, na.rm = TRUE) else NA_real_,
  result_mean = if (length(numeric_result)) mean(numeric_result, na.rm = TRUE) else NA_real_,
  stringsAsFactors = FALSE
)

utils::write.csv(out, result_csv, row.names = FALSE)
