args <- commandArgs(trailingOnly = TRUE)

if (length(args) < 8L) {
  stop(
    paste0(
      "Usage: Rscript benchmark-extended.R FUNCTION_FILE DATASET_DIR CASE WORKERS ",
      "CHUNK_ROWS TASK_ROWS TMP_ROOT RESULT_CSV\n"
    ),
    call. = FALSE
  )
}

function_file <- args[[1L]]
dataset_dir  <- args[[2L]]
case         <- args[[3L]]
workers      <- as.integer(args[[4L]])
chunk_rows   <- as.numeric(args[[5L]])
task_rows    <- as.numeric(args[[6L]])
tmp_root     <- args[[7L]]
result_csv   <- args[[8L]]

cpu_reps <- as.integer(Sys.getenv("CPU_REPS", "25"))
boot_B   <- as.integer(Sys.getenv("BOOT_B", "10"))

if (is.na(cpu_reps) || cpu_reps < 1L) stop("CPU_REPS must be >= 1.", call. = FALSE)
if (is.na(boot_B) || boot_B < 1L) stop("BOOT_B must be >= 1.", call. = FALSE)

required <- c("arrow", "dplyr")
missing <- required[!vapply(required, requireNamespace, logical(1), quietly = TRUE)]
if (length(missing)) {
  stop("Missing package(s): ", paste(missing, collapse = ", "), call. = FALSE)
}

source(function_file, local = .GlobalEnv)
if (!exists("summarise_big", mode = "function")) {
  stop("`summarise_big()` was not found after sourcing the function file.", call. = FALSE)
}

dir.create(dirname(result_csv), recursive = TRUE, showWarnings = FALSE)
dir.create(tmp_root, recursive = TRUE, showWarnings = FALSE)

parquet_files <- list.files(dataset_dir, pattern = "\\.parquet$", full.names = TRUE)
if (!length(parquet_files)) stop("No Parquet files found in: ", dataset_dir, call. = FALSE)

ds <- arrow::open_dataset(parquet_files, format = "parquet")


# ------------------------------------------------------------------
# Workload 1: CPU-heavy, deterministic, genuinely arbitrary R code.
#
# The purpose is not statistical novelty.  It gives each group enough
# computation that worker scheduling may become worthwhile.
# ------------------------------------------------------------------

custom_cpu <- function(x, y, reps = cpu_reps) {
  z <- x - y
  acc <- 0

  for (k in seq_len(reps)) {
    a <- k / (reps + 1)
    acc <- acc +
      mean(
        sin(z * (1 + a))^2 +
          cos((x + y) / (1 + a))^2 +
          log1p(abs(z * a)),
        na.rm = TRUE
      )
  }

  acc / reps
}


# ------------------------------------------------------------------
# Workload 2: sort/quantile-heavy.
#
# This requires raw group values and is not reconstructible from a
# small fixed set of sufficient statistics.
# ------------------------------------------------------------------

custom_quantile <- function(x, y) {
  z <- (x - y)^2 + sin(x) + log1p(abs(y))

  qs <- stats::quantile(
    z,
    probs = c(0.10, 0.50, 0.90, 0.99),
    na.rm = TRUE,
    names = FALSE,
    type = 7
  )

  # One scalar per group.
  qs[[4L]] - qs[[1L]] + qs[[3L]] - qs[[2L]]
}


# ------------------------------------------------------------------
# Workload 3: bootstrap correlation.
#
# This is intentionally not MapReduce-able in the generic sense.
# A fixed local seed makes the workload deterministic for benchmarking.
# ------------------------------------------------------------------

custom_boot <- function(x, y, B = boot_B) {
  ok <- is.finite(x) & is.finite(y)
  x <- x[ok]
  y <- y[ok]
  n <- length(x)

  if (n < 3L) return(NA_real_)

  set.seed(20260830L + n)

  out <- numeric(B)

  for (b in seq_len(B)) {
    i <- sample.int(n, n, replace = TRUE)
    out[[b]] <- stats::cor(x[i], y[i])
  }

  mean(out)
}


# ------------------------------------------------------------------
# Workload 4: simple linear-regression slope.
#
# This is interesting precisely because it can be calculated either:
#   * from complete raw groups in R, or
#   * from Arrow-computable sufficient statistics.
# ------------------------------------------------------------------

custom_lm_slope <- function(x, y) {
  ok <- is.finite(x) & is.finite(y)
  x <- x[ok]
  y <- y[ok]

  if (length(x) < 2L) return(NA_real_)

  unname(stats::coef(stats::lm(y ~ x))[[2L]])
}


# ------------------------------------------------------------------
# Workload 5: skewness.
#
# Again, the raw-R calculation sees every row, while MapReduce needs
# only n, sum(x), sum(x^2), and sum(x^3).
# Population-moment convention is used on both paths.
# ------------------------------------------------------------------

custom_skew <- function(x) {
  x <- x[is.finite(x)]
  n <- length(x)

  if (n < 2L) return(NA_real_)

  mu <- mean(x)
  m2 <- mean((x - mu)^2)

  if (!is.finite(m2) || m2 <= 0) return(NA_real_)

  m3 <- mean((x - mu)^3)

  m3 / (m2^(3 / 2))
}


run_case <- function() {

  tmp <- file.path(
    tmp_root,
    paste0(case, "-w", workers, "-", Sys.getpid())
  )

  switch(
    case,

    parallel_cpu = {
      summarise_big(
        ds,
        result = custom_cpu(x, y),
        .by = grp,
        .strategy = "parallel_chunks",
        .workers = workers,
        .chunk_rows = chunk_rows,
        .task_rows = task_rows,
        .try_arrow = FALSE,
        .tmp = tmp
      )
    },

    shared_cpu = {
      summarise_big(
        ds,
        result = custom_cpu(x, y),
        .by = grp,
        .strategy = "shared_chunk",
        .workers = workers,
        .chunk_rows = chunk_rows,
        .task_rows = task_rows,
        .try_arrow = FALSE,
        .tmp = tmp
      )
    },

    parallel_quantile = {
      summarise_big(
        ds,
        result = custom_quantile(x, y),
        .by = grp,
        .strategy = "parallel_chunks",
        .workers = workers,
        .chunk_rows = chunk_rows,
        .task_rows = task_rows,
        .try_arrow = FALSE,
        .tmp = tmp
      )
    },

    shared_quantile = {
      summarise_big(
        ds,
        result = custom_quantile(x, y),
        .by = grp,
        .strategy = "shared_chunk",
        .workers = workers,
        .chunk_rows = chunk_rows,
        .task_rows = task_rows,
        .try_arrow = FALSE,
        .tmp = tmp
      )
    },

    parallel_boot = {
      summarise_big(
        ds,
        result = custom_boot(x, y),
        .by = grp,
        .strategy = "parallel_chunks",
        .workers = workers,
        .chunk_rows = chunk_rows,
        .task_rows = task_rows,
        .try_arrow = FALSE,
        .tmp = tmp
      )
    },

    shared_boot = {
      summarise_big(
        ds,
        result = custom_boot(x, y),
        .by = grp,
        .strategy = "shared_chunk",
        .workers = workers,
        .chunk_rows = chunk_rows,
        .task_rows = task_rows,
        .try_arrow = FALSE,
        .tmp = tmp
      )
    },

    mapreduce_slope = {
      summarise_big(
        ds,
        .by = grp,
        .strategy = "map_reduce",
        .map_reduce = list(
          n   = ~ n(),
          sx  = ~ sum(x),
          sy  = ~ sum(y),
          sxx = ~ sum(x^2),
          sxy = ~ sum(x * y)
        ),
        .finalize = function(d) {
          dplyr::transmute(
            d,
            grp = grp,
            result =
              (sxy - sx * sy / n) /
              (sxx - sx^2 / n)
          )
        }
      )
    },

    parallel_slope = {
      summarise_big(
        ds,
        result = custom_lm_slope(x, y),
        .by = grp,
        .strategy = "parallel_chunks",
        .workers = workers,
        .chunk_rows = chunk_rows,
        .task_rows = task_rows,
        .try_arrow = FALSE,
        .tmp = tmp
      )
    },

    shared_slope = {
      summarise_big(
        ds,
        result = custom_lm_slope(x, y),
        .by = grp,
        .strategy = "shared_chunk",
        .workers = workers,
        .chunk_rows = chunk_rows,
        .task_rows = task_rows,
        .try_arrow = FALSE,
        .tmp = tmp
      )
    },

    mapreduce_skew = {
      summarise_big(
        ds,
        .by = grp,
        .strategy = "map_reduce",
        .map_reduce = list(
          n  = ~ n(),
          s1 = ~ sum(x),
          s2 = ~ sum(x^2),
          s3 = ~ sum(x^3)
        ),
        .finalize = function(d) {
          mu <- d$s1 / d$n
          ex2 <- d$s2 / d$n
          ex3 <- d$s3 / d$n

          m2 <- ex2 - mu^2
          m3 <- ex3 - 3 * mu * ex2 + 2 * mu^3

          result <- ifelse(
            is.finite(m2) & m2 > 0,
            m3 / (m2^(3 / 2)),
            NA_real_
          )

          data.frame(
            grp = d$grp,
            result = result
          )
        }
      )
    },

    parallel_skew = {
      summarise_big(
        ds,
        result = custom_skew(x),
        .by = grp,
        .strategy = "parallel_chunks",
        .workers = workers,
        .chunk_rows = chunk_rows,
        .task_rows = task_rows,
        .try_arrow = FALSE,
        .tmp = tmp
      )
    },

    shared_skew = {
      summarise_big(
        ds,
        result = custom_skew(x),
        .by = grp,
        .strategy = "shared_chunk",
        .workers = workers,
        .chunk_rows = chunk_rows,
        .task_rows = task_rows,
        .try_arrow = FALSE,
        .tmp = tmp
      )
    },

    stop("Unknown extended benchmark case: ", case, call. = FALSE)
  )
}


gc()

t <- system.time({
  result <- run_case()
})

if ("grp" %in% names(result)) {
  result <- result[order(result$grp), , drop = FALSE]
}

numeric_result <- if (
  "result" %in% names(result) &&
  is.numeric(result$result)
) {
  result$result
} else {
  numeric()
}

out <- data.frame(
  case = case,
  workers = workers,
  chunk_rows = chunk_rows,
  task_rows = task_rows,
  cpu_reps = cpu_reps,
  boot_B = boot_B,
  n_groups_returned = nrow(result),
  r_elapsed_sec = unname(t[["elapsed"]]),
  result_sum = if (length(numeric_result)) {
    sum(numeric_result, na.rm = TRUE)
  } else {
    NA_real_
  },
  result_mean = if (length(numeric_result)) {
    mean(numeric_result, na.rm = TRUE)
  } else {
    NA_real_
  },
  stringsAsFactors = FALSE
)

utils::write.csv(
  out,
  result_csv,
  row.names = FALSE
)
