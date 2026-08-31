testthat::test_that("1. native Arrow mean matches in-memory reference and skips temporary partitioning", {
  fx <- local_parquet_fixture(make_main_data())
  tmp <- tempfile("summarise-big-fast-path-")

  expected <- fx$data |>
    dplyr::summarise(result = mean(x), .by = grp) |>
    canonical()

  actual <- summarise_big(
    fx$ds,
    result = mean(x),
    .by = grp,
    .tmp = tmp
  ) |>
    canonical()

  testthat::expect_equal(actual, expected, tolerance = 1e-10)
  testthat::expect_false(dir.exists(tmp))
  testthat::expect_true(future_plan_is_sequential())
})


testthat::test_that("2. composite Arrow expressions are evaluated without materialising groups", {
  fx <- local_parquet_fixture(make_main_data())

  expected <- fx$data |>
    dplyr::summarise(result = mean(x * x - 5), .by = grp) |>
    canonical()

  actual <- summarise_big(
    fx$ds,
    result = mean(x * x - 5),
    .by = grp
  ) |>
    canonical()

  testthat::expect_equal(actual, expected, tolerance = 1e-10)
})


testthat::test_that("3. caller-local summary globals are exported and the Future plan is restored", {
  fx <- local_parquet_fixture(make_main_data())

  # Deliberately define both a function and one of its dependencies inside the
  # test environment.  This reproduces the strict-environment case that is
  # easy to miss when everything happens to live in .GlobalEnv.
  shift <- 0.125
  local_stat <- function(x) {
    mean(log1p(abs(x))) + shift
  }

  original_plan <- future::plan()
  withr::defer(future::plan(original_plan))

  future::plan(future::sequential)
  before <- future_plan_signature()

  expected <- fx$data |>
    dplyr::summarise(result = local_stat(x), .by = grp) |>
    canonical()

  actual <- summarise_big(
    fx$ds,
    result = local_stat(x),
    .by = grp,
    .strategy = "parallel_chunks",
    .workers = 2L,
    .chunk_rows = 30,
    .try_arrow = FALSE
  ) |>
    canonical()

  after <- future_plan_signature()

  testthat::expect_equal(actual, expected, tolerance = 1e-10)
  testthat::expect_identical(after, before)
})

testthat::test_that("4. parallel_chunks matches an arbitrary in-memory R summary", {
  fx <- local_parquet_fixture(make_main_data())
  tmp <- tempfile("summarise-big-parallel-")

  expected <- fx$data |>
    dplyr::summarise(result = opaque_numeric_stat(x), .by = grp) |>
    canonical()

  actual <- summarise_big(
    fx$ds,
    result = opaque_numeric_stat(x),
    .by = grp,
    .strategy = "parallel_chunks",
    .workers = 2L,
    .chunk_rows = 30,
    .try_arrow = FALSE,
    .tmp = tmp
  ) |>
    canonical()

  testthat::expect_equal(actual, expected, tolerance = 1e-10)
  testthat::expect_false(dir.exists(tmp))
  testthat::expect_true(future_plan_is_sequential())
})


testthat::test_that("5. shared_chunk + Mori matches an arbitrary in-memory R summary", {
  testthat::skip_if_not_installed("mori")

  fx <- local_parquet_fixture(make_main_data())

  expected <- fx$data |>
    dplyr::summarise(result = opaque_numeric_stat(x), .by = grp) |>
    canonical()

  actual <- summarise_big(
    fx$ds,
    result = opaque_numeric_stat(x),
    .by = grp,
    .strategy = "shared_chunk",
    .workers = 2L,
    .chunk_rows = 30,
    .task_rows = 8,
    .try_arrow = FALSE
  ) |>
    canonical()

  testthat::expect_equal(actual, expected, tolerance = 1e-10)
  testthat::expect_true(future_plan_is_sequential())
})


testthat::test_that("6. a two-column lm statistic agrees across both materialised strategies", {
  testthat::skip_if_not_installed("mori")

  fx <- local_parquet_fixture(make_main_data())

  expected <- fx$data |>
    dplyr::summarise(slope = opaque_slope(x, y), .by = grp) |>
    canonical()

  parallel <- summarise_big(
    fx$ds,
    slope = opaque_slope(x, y),
    .by = grp,
    .strategy = "parallel_chunks",
    .workers = 1L,
    .chunk_rows = 30,
    .try_arrow = FALSE
  ) |>
    canonical()

  shared <- summarise_big(
    fx$ds,
    slope = opaque_slope(x, y),
    .by = grp,
    .strategy = "shared_chunk",
    .workers = 2L,
    .chunk_rows = 30,
    .task_rows = 10,
    .try_arrow = FALSE
  ) |>
    canonical()

  testthat::expect_equal(parallel, expected, tolerance = 1e-10)
  testthat::expect_equal(shared, expected, tolerance = 1e-10)
  testthat::expect_equal(shared, parallel, tolerance = 1e-10)
})


testthat::test_that("7. order-sensitive string summaries respect .order_by", {
  testthat::skip_if_not_installed("mori")

  fx <- local_parquet_fixture(make_order_data())

  expected <- fx$data |>
    dplyr::arrange(grp, row_id) |>
    dplyr::summarise(result = initials_in_order(txt), .by = grp) |>
    canonical()

  parallel <- summarise_big(
    fx$ds,
    result = initials_in_order(txt),
    .by = grp,
    .order_by = "row_id",
    .strategy = "parallel_chunks",
    .workers = 1L,
    .chunk_rows = 10,
    .try_arrow = FALSE
  ) |>
    canonical()

  shared <- summarise_big(
    fx$ds,
    result = initials_in_order(txt),
    .by = grp,
    .order_by = "row_id",
    .strategy = "shared_chunk",
    .workers = 2L,
    .chunk_rows = 10,
    .task_rows = 5,
    .try_arrow = FALSE
  ) |>
    canonical()

  testthat::expect_identical(parallel, expected)
  testthat::expect_identical(shared, expected)
})


testthat::test_that("8. MapReduce variance matches both R and materialised execution", {
  fx <- local_parquet_fixture(make_main_data())

  expected <- fx$data |>
    dplyr::summarise(result = stats::var(x), .by = grp) |>
    canonical()

  mr <- summarise_big(
    fx$ds,
    .by = grp,
    .strategy = "map_reduce",
    .map_reduce = list(
      n = ~ dplyr::n(),
      sx = ~ sum(x),
      sx2 = ~ sum(x * x)
    ),
    .finalize = function(d) {
      d |>
        dplyr::mutate(result = (sx2 - sx * sx / n) / (n - 1)) |>
        dplyr::select(grp, result)
    }
  ) |>
    canonical()

  materialised <- summarise_big(
    fx$ds,
    result = stats::var(x),
    .by = grp,
    .strategy = "parallel_chunks",
    .workers = 1L,
    .chunk_rows = 30,
    .try_arrow = FALSE
  ) |>
    canonical()

  testthat::expect_equal(mr, expected, tolerance = 1e-8)
  testthat::expect_equal(materialised, expected, tolerance = 1e-10)
})


testthat::test_that("9. MapReduce regression slope matches lm()", {
  fx <- local_parquet_fixture(make_main_data())

  expected <- fx$data |>
    dplyr::summarise(slope = opaque_slope(x, y), .by = grp) |>
    canonical()

  actual <- summarise_big(
    fx$ds,
    .by = grp,
    .strategy = "map_reduce",
    .map_reduce = list(
      n = ~ dplyr::n(),
      sx = ~ sum(x),
      sy = ~ sum(y),
      sxx = ~ sum(x * x),
      sxy = ~ sum(x * y)
    ),
    .finalize = function(d) {
      d |>
        dplyr::mutate(
          slope = (sxy - sx * sy / n) / (sxx - sx * sx / n)
        ) |>
        dplyr::select(grp, slope)
    }
  ) |>
    canonical()

  testthat::expect_equal(actual, expected, tolerance = 1e-8)
})


testthat::test_that("10. .chunk_rows accepts exact boundary and explicitly handles oversized groups", {
  fx <- local_parquet_fixture(make_boundary_data())

  exact_ds <- fx$ds |>
    dplyr::filter(grp == "exact")

  exact_expected <- fx$data |>
    dplyr::filter(grp == "exact") |>
    dplyr::summarise(result = opaque_numeric_stat(x), .by = grp) |>
    canonical()

  exact_actual <- summarise_big(
    exact_ds,
    result = opaque_numeric_stat(x),
    .by = grp,
    .strategy = "parallel_chunks",
    .workers = 1L,
    .chunk_rows = 10,
    .try_arrow = FALSE
  ) |>
    canonical()

  testthat::expect_equal(exact_actual, exact_expected, tolerance = 1e-10)

  over_ds <- fx$ds |>
    dplyr::filter(grp == "over")

  testthat::expect_error(
    summarise_big(
      over_ds,
      result = opaque_numeric_stat(x),
      .by = grp,
      .strategy = "parallel_chunks",
      .workers = 1L,
      .chunk_rows = 10,
      .oversize = "error",
      .try_arrow = FALSE
    ),
    regexp = "larger than.*chunk_rows|cannot.*split"
  )

  over_expected <- fx$data |>
    dplyr::filter(grp == "over") |>
    dplyr::summarise(result = opaque_numeric_stat(x), .by = grp) |>
    canonical()

  over_actual <- NULL

  testthat::expect_warning(
    over_actual <- summarise_big(
      over_ds,
      result = opaque_numeric_stat(x),
      .by = grp,
      .strategy = "parallel_chunks",
      .workers = 1L,
      .chunk_rows = 10,
      .oversize = "warning",
      .try_arrow = FALSE
    ),
    regexp = "larger than.*chunk_rows|materialised in full"
  )

  over_actual <- canonical(over_actual)

  testthat::expect_equal(over_actual, over_expected, tolerance = 1e-10)
})


testthat::test_that("11. .task_rows and worker count do not change deterministic shared-chunk results", {
  testthat::skip_if_not_installed("mori")

  fx <- local_parquet_fixture(make_main_data())

  expected <- fx$data |>
    dplyr::summarise(result = opaque_numeric_stat(x), .by = grp) |>
    canonical()

  one_worker <- summarise_big(
    fx$ds,
    result = opaque_numeric_stat(x),
    .by = grp,
    .strategy = "shared_chunk",
    .workers = 1L,
    .chunk_rows = 30,
    .task_rows = 1,
    .try_arrow = FALSE
  ) |>
    canonical()

  two_workers <- summarise_big(
    fx$ds,
    result = opaque_numeric_stat(x),
    .by = grp,
    .strategy = "shared_chunk",
    .workers = 2L,
    .chunk_rows = 30,
    .task_rows = 8,
    .try_arrow = FALSE
  ) |>
    canonical()

  testthat::expect_equal(one_worker, expected, tolerance = 1e-10)
  testthat::expect_equal(two_workers, expected, tolerance = 1e-10)
  testthat::expect_equal(one_worker, two_workers, tolerance = 1e-10)
  testthat::expect_setequal(two_workers$grp, unique(fx$data$grp))
  testthat::expect_false(anyDuplicated(two_workers$grp) > 0L)
  testthat::expect_true(future_plan_is_sequential())
})


testthat::test_that("12. temporary files and Future workers are cleaned up after an error", {
  fx <- local_parquet_fixture(make_main_data())
  tmp <- tempfile("summarise-big-error-cleanup-")

  deliberate_failure <- function(x) {
    stop("intentional test failure", call. = FALSE)
  }

  testthat::expect_error(
    summarise_big(
      fx$ds,
      result = deliberate_failure(x),
      .by = grp,
      .strategy = "parallel_chunks",
      .workers = 2L,
      .chunk_rows = 30,
      .try_arrow = FALSE,
      .tmp = tmp
    ),
    "intentional test failure"
  )

  testthat::expect_false(dir.exists(tmp))
  testthat::expect_true(future_plan_is_sequential())
})
