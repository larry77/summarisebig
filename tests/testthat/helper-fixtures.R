# Helper fixtures for summarise_big() tests
#
# All fixtures are generated at test time and written as multi-file Parquet
# datasets.  Rows are deliberately shuffled before writing so tests cannot
# accidentally depend on physical Parquet/Dataset row order.

local_parquet_fixture <- function(data, n_files = 3L, env = parent.frame()) {
  stopifnot(is.data.frame(data), nrow(data) > 0L, n_files >= 1L)

  path <- tempfile("summarise-big-fixture-")
  dir.create(path, recursive = TRUE)
  withr::defer(unlink(path, recursive = TRUE, force = TRUE), envir = env)

  # Split into several physical Parquet files to exercise Arrow Dataset logic.
  file_id <- rep(seq_len(n_files), length.out = nrow(data))
  parts <- split(seq_len(nrow(data)), file_id)

  for (i in seq_along(parts)) {
    arrow::write_parquet(
      data[parts[[i]], , drop = FALSE],
      file.path(path, sprintf("part-%02d.parquet", i))
    )
  }

  list(
    data = data,
    path = path,
    ds = arrow::open_dataset(path)
  )
}

make_main_data <- function(seed = 20260830L) {
  sizes <- c(A = 3L, B = 7L, C = 11L, D = 17L, E = 29L)

  grp <- rep(names(sizes), times = sizes)
  row_id <- sequence(unname(sizes))
  n <- length(grp)

  set.seed(seed)
  z <- seq_len(n)
  x <- 0.15 * z + sin(z / 3) + stats::rnorm(n, sd = 0.08)
  y <- 1.75 + 2.4 * x + cos(z / 5) * 0.15 + stats::rnorm(n, sd = 0.03)

  words <- c(
    "alpha", "bravo", "charlie", "delta", "echo", "foxtrot",
    "golf", "hotel", "india", "juliet", "kilo", "lima"
  )

  txt <- paste0(words[(z - 1L) %% length(words) + 1L], "_", row_id)
  date <- as.Date("2026-01-01") + row_id

  out <- data.frame(
    grp = grp,
    row_id = row_id,
    x = x,
    y = y,
    txt = txt,
    date = date,
    stringsAsFactors = FALSE
  )

  # Deliberately destroy group/order contiguity before writing.
  set.seed(seed + 1L)
  out[sample.int(nrow(out)), , drop = FALSE]
}

make_boundary_data <- function(seed = 20260831L) {
  sizes <- c(small = 4L, exact = 10L, over = 11L, large = 23L)

  grp <- rep(names(sizes), times = sizes)
  row_id <- sequence(unname(sizes))
  n <- length(grp)

  set.seed(seed)
  x <- stats::rnorm(n) + seq_len(n) / 100

  out <- data.frame(
    grp = grp,
    row_id = row_id,
    x = x,
    stringsAsFactors = FALSE
  )

  out[sample.int(nrow(out)), , drop = FALSE]
}

make_order_data <- function(seed = 20260832L) {
  sizes <- c(A = 5L, B = 6L, C = 8L)
  grp <- rep(names(sizes), times = sizes)
  row_id <- sequence(unname(sizes))
  n <- length(grp)

  alphabet <- c(
    "alpha", "bravo", "charlie", "delta", "echo", "foxtrot",
    "golf", "hotel", "india", "juliet", "kilo", "lima",
    "mike", "november", "oscar", "papa", "quebec", "romeo", "sierra"
  )

  out <- data.frame(
    grp = grp,
    row_id = row_id,
    txt = alphabet[seq_len(n)],
    stringsAsFactors = FALSE
  )

  set.seed(seed)
  out[sample.int(nrow(out)), , drop = FALSE]
}

canonical <- function(x) {
  x |>
    dplyr::arrange(.data$grp) |>
    as.data.frame()
}

# Deliberately opaque to Arrow: used to force ordinary-R fallback paths.
opaque_numeric_stat <- function(x) {
  mean(log1p(abs(x))) + stats::mad(x) / (stats::sd(x) + 1)
}

opaque_slope <- function(x, y) {
  unname(stats::coef(stats::lm(y ~ x))[2L])
}

initials_in_order <- function(txt) {
  paste0(substr(txt, 1L, 1L), collapse = "")
}


future_plan_signature <- function() {
  class(future::plan())
}


future_plan_is_sequential <- function() {
  inherits(future::plan(), "sequential")
}
