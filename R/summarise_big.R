#' Summarise a large Arrow dataset by group
#'
#' `summarise_big()` is designed for grouped computations on Arrow datasets
#' when the result may be expressible in Arrow, reconstructible from compact
#' Arrow-computable state, or require arbitrary R code on complete groups.
#'
#' The ordinary strategies first attempt the complete summary lazily in Arrow.
#' If Arrow cannot execute it, `parallel_chunks` materializes different
#' group-safe chunks independently, while `shared_chunk` materializes one
#' chunk at a time and can share it between workers with `mori`.
#'
#' With `strategy = "map_reduce"`, Arrow computes the expressions supplied in
#' `.map_reduce`, only that reduced table is collected, and `.finalize` runs in
#' ordinary R.
#'
#' @param .data An Arrow Dataset or Arrow object supporting dplyr operations.
#' @param ... Named grouped summary expressions for the ordinary strategies.
#' @param .by A single grouping column.
#' @param .order_by Optional character vector of columns defining deterministic
#'   order within groups for order-sensitive R summaries.
#' @param .workers Number of local Mirai workers. Parallel package tests should
#'   use at most two workers. Defaults to 2.
#' @param .strategy One of `"parallel_chunks"`, `"shared_chunk"`, or
#'   `"map_reduce"`.
#' @param .chunk_rows Target maximum number of rows per materialized disk chunk.
#'   Complete groups are never split.
#' @param .task_rows Target rows per worker task within `shared_chunk`. Complete
#'   groups are never split.
#' @param .oversize What to do when one group exceeds `.chunk_rows`.
#' @param .map_reduce Named list of Arrow-computable grouped reduction
#'   expressions, normally supplied as one-sided formulas.
#' @param .finalize R function applied to the compact table produced by
#'   `.map_reduce`. It must return a data frame.
#' @param .seed Passed to `futurize::futurize()` for parallel random-number
#'   handling.
#' @param .try_arrow If `TRUE`, ordinary strategies attempt the complete summary
#'   in Arrow before materializing any raw groups.
#' @param .tmp Temporary directory used for group-safe Parquet repartitioning.
#' @param .keep_tmp If `TRUE`, retain the temporary dataset after completion.
#'
#' @return A data frame containing grouped summary results.
#' @export
#'
#' @examples
#' tab <- arrow::Table$create(data.frame(
#'   grp = c("a", "a", "b", "b"),
#'   x = c(1, 2, 10, 20)
#' ))
#'
#' # Arrow fast path.
#' summarise_big(tab, result = mean(x), .by = grp)
#'
#' # Arrow reduction followed by an ordinary R finalizer.
#' summarise_big(
#'   tab,
#'   .by = grp,
#'   .strategy = "map_reduce",
#'   .map_reduce = list(n = ~ dplyr::n(), sx = ~ sum(x)),
#'   .finalize = function(d) {
#'     dplyr::mutate(d, result = sx / n)
#'   }
#' )
#'
#' \dontrun{
#' # Arbitrary R function on complete groups in a Parquet Dataset.
#' ds <- arrow::open_dataset("data")
#' summarise_big(
#'   ds,
#'   result = my_custom_function(x, y),
#'   .by = grp,
#'   .strategy = "parallel_chunks",
#'   .workers = 2
#' )
#' }
summarise_big <- function(
    .data,
    ...,
    .by,
    .order_by = NULL,
    .workers = 2L,
    .strategy = c("parallel_chunks", "shared_chunk", "map_reduce"),
    .chunk_rows = 1e6,
    .task_rows = 1e5,
    .oversize = c("error", "warning"),
    .map_reduce = NULL,
    .finalize = NULL,
    .seed = TRUE,
    .try_arrow = TRUE,
    .tmp = tempfile("summarise-big-"),
    .keep_tmp = FALSE
) {
  required <- c(
    "arrow", "dplyr", "purrr", "rlang", "future", "future.mirai", "futurize", "furrr"
  )
  missing <- required[
    !vapply(required, requireNamespace, logical(1), quietly = TRUE)
  ]
  if (length(missing)) {
    stop(
      "Missing required package(s): ", paste(missing, collapse = ", "),
      call. = FALSE
    )
  }

  .strategy <- match.arg(.strategy)
  .oversize <- match.arg(.oversize)
  .workers <- as.integer(.workers)
  .chunk_rows <- as.numeric(.chunk_rows)
  .task_rows <- as.numeric(.task_rows)

  if (length(.workers) != 1L || is.na(.workers) || .workers < 1L) {
    stop("`.workers` must be a single integer >= 1.", call. = FALSE)
  }
  if (
    length(.chunk_rows) != 1L || is.na(.chunk_rows) ||
    !is.finite(.chunk_rows) || .chunk_rows < 1
  ) {
    stop("`.chunk_rows` must be a single positive number.", call. = FALSE)
  }
  if (
    length(.task_rows) != 1L || is.na(.task_rows) ||
    !is.finite(.task_rows) || .task_rows < 1
  ) {
    stop("`.task_rows` must be a single positive number.", call. = FALSE)
  }
  if (.strategy == "shared_chunk" && .task_rows > .chunk_rows) {
    warning(
      "`.task_rows` is larger than `.chunk_rows`. ",
      "This is normally unnecessary in the shared-chunk strategy.",
      call. = FALSE
    )
  }

  if (
    .strategy == "shared_chunk" && .workers > 1L &&
    !requireNamespace("mori", quietly = TRUE)
  ) {
    stop(
      "Package `mori` is required for `.strategy = \"shared_chunk\"` ",
      "when `.workers > 1`.",
      call. = FALSE
    )
  }

  # A package function should never leave the caller's global Future plan
  # changed.  We temporarily select the Mirai backend only when required and
  # restore whatever plan the caller had on every exit path.
  previous_plan <- future::plan()
  on.exit(future::plan(previous_plan), add = TRUE)

  dots <- rlang::enquos(...)
  by <- rlang::ensym(.by)
  by_name <- rlang::as_string(by)
  caller <- rlang::caller_env()
  data_names <- names(.data)

  # `dots` contains quosures. Automatic future-global discovery sees the
  # quosure objects themselves, but in strict environments it may not discover
  # user functions referenced *inside* those quosures. Identify those globals
  # from the original caller now and pass them explicitly to futurize().
  summary_future <- .sb_future_globals_from_dots(
    dots = dots,
    caller = caller,
    data_names = data_names
  )

  if (!by_name %in% data_names) {
    stop("Grouping column `", by_name, "` does not exist.", call. = FALSE)
  }

  internal_bucket <- "summarise_big_internal_bucket"
  if (internal_bucket %in% data_names) {
    stop(
      "Input data already contain the reserved internal column `",
      internal_bucket, "`.",
      call. = FALSE
    )
  }

  if (!is.null(.order_by) && !is.character(.order_by)) {
    stop(
      "`.order_by` must be NULL or a character vector of column names.",
      call. = FALSE
    )
  }
  if (!is.null(.order_by)) {
    missing_order <- setdiff(.order_by, data_names)
    if (length(missing_order)) {
      stop(
        "Ordering column(s) not found: ",
        paste(missing_order, collapse = ", "),
        call. = FALSE
      )
    }
  }

  if (.strategy == "map_reduce") {
    if (length(dots)) {
      stop(
        "Do not supply ordinary `...` summary expressions when ",
        "`.strategy = \"map_reduce\"`. Put Arrow-computable sufficient ",
        "statistics in `.map_reduce` and the final R calculation in `.finalize`.",
        call. = FALSE
      )
    }
    if (is.null(.map_reduce)) {
      stop("`.strategy = \"map_reduce\"` requires `.map_reduce`.", call. = FALSE)
    }
    if (is.null(.finalize)) {
      stop("`.strategy = \"map_reduce\"` requires `.finalize`.", call. = FALSE)
    }
    if (!is.null(.order_by)) {
      stop(
        "`.order_by` is not supported with `.strategy = \"map_reduce\"` ",
        "in this implementation. MapReduce partial statistics must currently ",
        "be order-independent.",
        call. = FALSE
      )
    }

    partials <- .sb_normalise_map_reduce(.map_reduce, caller)
    finalize <- rlang::as_function(.finalize, env = caller)

    state <- tryCatch(
      .data |>
        dplyr::summarise(!!!partials, .by = !!by) |>
        dplyr::collect(),
      error = function(e) {
        stop(
          "MapReduce failed during the Arrow reduction phase. Every ",
          "`.map_reduce` expression must be executable by Arrow. Arrow reported:\n",
          conditionMessage(e),
          call. = FALSE
        )
      }
    )

    result <- finalize(state)
    if (!inherits(result, "data.frame")) {
      stop("`.finalize` must return a data frame or tibble.", call. = FALSE)
    }
    return(result)
  }

  if (!is.null(.map_reduce) || !is.null(.finalize)) {
    stop(
      "`.map_reduce` and `.finalize` are used only with ",
      "`.strategy = \"map_reduce\"`.",
      call. = FALSE
    )
  }

  if (.try_arrow) {
    arrow_result <- tryCatch(
      {
        q <- .data
        if (!is.null(.order_by)) {
          q <- q |>
            dplyr::arrange(
              dplyr::across(dplyr::all_of(c(by_name, .order_by)))
            )
        }
        q |>
          dplyr::summarise(!!!dots, .by = !!by) |>
          dplyr::collect()
      },
      error = function(e) NULL
    )

    if (!is.null(arrow_result)) return(arrow_result)
  }

  groups <- .data |>
    dplyr::summarise(.n = dplyr::n(), .by = !!by) |>
    dplyr::collect()

  if (nrow(groups) == 0L) {
    stop("The dataset contains no groups to process.", call. = FALSE)
  }
  if (anyNA(groups[[by_name]])) {
    stop(
      "`summarise_big()` currently requires non-missing values in grouping ",
      "variable `", by_name, "`.",
      call. = FALSE
    )
  }

  oversized <- groups[
    groups$.n > .chunk_rows,
    ,
    drop = FALSE
  ]

  if (nrow(oversized) > 0L) {
    show_n <- min(5L, nrow(oversized))
    examples <- paste0(
      as.character(oversized[[by_name]][seq_len(show_n)]),
      " (",
      format(
        oversized$.n[seq_len(show_n)],
        big.mark = ",", scientific = FALSE
      ),
      " rows)"
    )

    msg <- paste0(
      nrow(oversized),
      " group(s) are larger than `.chunk_rows = ",
      format(.chunk_rows, big.mark = ",", scientific = FALSE),
      "`.\n\nLargest group: ",
      format(max(oversized$.n), big.mark = ",", scientific = FALSE),
      " rows.\nExamples: ",
      paste(examples, collapse = ", "),
      if (nrow(oversized) > show_n) ", ..." else "",
      "\n\nAn arbitrary group-level R function may require all rows of a ",
      "group simultaneously, so such a group cannot automatically be split."
    )

    if (.oversize == "error") {
      stop(
        msg,
        "\n\nIncrease `.chunk_rows`, use `.oversize = \"warning\"`, or ",
        "reformulate the calculation using `.strategy = \"map_reduce\"` if ",
        "sufficient statistics exist.",
        call. = FALSE
      )
    } else {
      warning(
        msg,
        "\n\nContinuing. Each oversized group will be placed in a chunk by ",
        "itself and materialized in full.",
        call. = FALSE
      )
    }
  }

  groups[[internal_bucket]] <- .sb_pack_groups(groups$.n, .chunk_rows)
  bucket_ids <- sort(unique(groups[[internal_bucket]]))

  if (length(bucket_ids) > 10000L) {
    warning(
      "This configuration produces ",
      format(length(bucket_ids), big.mark = ","),
      " temporary partitions. Consider increasing `.chunk_rows`; very fine ",
      "Parquet partitioning may carry substantial filesystem and metadata overhead.",
      call. = FALSE
    )
  }

  if (
    dir.exists(.tmp) &&
    length(list.files(.tmp, all.files = TRUE, no.. = TRUE)) > 0L
  ) {
    stop(
      "Temporary directory already exists and is not empty:\n", .tmp,
      call. = FALSE
    )
  }

  dir.create(.tmp, recursive = TRUE, showWarnings = FALSE)
  if (!.keep_tmp) {
    on.exit(unlink(.tmp, recursive = TRUE, force = TRUE), add = TRUE)
  }

  bucket_map_df <- groups |>
    dplyr::select(dplyr::all_of(c(by_name, internal_bucket)))
  bucket_map <- arrow::Table$create(bucket_map_df)

  partitioned <- .data |>
    dplyr::ungroup() |>
    dplyr::inner_join(bucket_map, by = by_name)

  arrow::write_dataset(
    partitioned,
    path = .tmp,
    format = "parquet",
    partitioning = internal_bucket,
    hive_style = TRUE,
    existing_data_behavior = "error",
    max_partitions = max(1024L, length(bucket_ids))
  )

  if (.workers > 1L) {
    future::plan(future.mirai::mirai_multisession, workers = .workers)
  } else {
    future::plan(future::sequential)
  }

  if (.strategy == "parallel_chunks") {
    process_bucket <- function(
        b, path, dots, by_name, order_by, internal_bucket
    ) {
      bucket_sym <- rlang::sym(internal_bucket)

      x <- arrow::open_dataset(path, hive_style = TRUE) |>
        dplyr::filter(!!bucket_sym == b) |>
        dplyr::select(-dplyr::all_of(internal_bucket)) |>
        dplyr::collect()

      if (!is.null(order_by)) {
        x <- x |>
          dplyr::arrange(
            dplyr::across(dplyr::all_of(c(by_name, order_by)))
          )
      }

      x |>
        dplyr::summarise(!!!dots, .by = dplyr::all_of(by_name))
    }

    environment(process_bucket) <- baseenv()

    if (.workers > 1L) {
      result <- bucket_ids |>
        purrr::map(
          process_bucket,
          path = .tmp,
          dots = dots,
          by_name = by_name,
          order_by = .order_by,
          internal_bucket = internal_bucket
        ) |>
        futurize::futurize(
          seed = .seed,
          globals = summary_future$globals,
          packages = summary_future$packages
        ) |>
        purrr::list_rbind()
    } else {
      result <- bucket_ids |>
        purrr::map(
          process_bucket,
          path = .tmp,
          dots = dots,
          by_name = by_name,
          order_by = .order_by,
          internal_bucket = internal_bucket
        ) |>
        purrr::list_rbind()
    }

    return(result)
  }

  process_shared_bucket <- function(b) {
    bucket_sym <- rlang::sym(internal_bucket)

    x <- arrow::open_dataset(.tmp, hive_style = TRUE) |>
      dplyr::filter(!!bucket_sym == b) |>
      dplyr::select(-dplyr::all_of(internal_bucket)) |>
      dplyr::collect() |>
      as.data.frame()

    sort_columns <- c(by_name, .order_by)
    ord_args <- c(
      x[sort_columns],
      list(na.last = TRUE, method = "radix")
    )
    ord <- do.call(order, ord_args)
    x <- x[ord, , drop = FALSE]

    g <- x[[by_name]]
    starts <- which(!duplicated(g))
    ends <- c(starts[-1L] - 1L, nrow(x))

    index <- data.frame(
      .group = g[starts],
      .start = starts,
      .end = ends
    )
    index$.n <- index$.end - index$.start + 1
    index$.task <- .sb_pack_groups(index$.n, .task_rows)
    tasks <- split(index, index$.task)

    if (.workers == 1L) {
      result <- tasks |>
        purrr::map(
          function(idx) {
            first <- idx$.start[[1L]]
            last <- idx$.end[[nrow(idx)]]
            local <- x[first:last, , drop = FALSE]
            local |>
              dplyr::summarise(!!!dots, .by = dplyr::all_of(by_name))
          }
        ) |>
        purrr::list_rbind()

      rm(x)
      gc(FALSE)
      return(result)
    }

    x_shared <- mori::share(x)
    rm(x)
    gc(FALSE)

    process_task <- function(idx, x_shared, dots, by_name) {
      first <- idx$.start[[1L]]
      last <- idx$.end[[nrow(idx)]]
      local <- x_shared[first:last, , drop = FALSE]
      local |>
        dplyr::summarise(!!!dots, .by = dplyr::all_of(by_name))
    }

    environment(process_task) <- baseenv()

    result <- tasks |>
      purrr::map(
        process_task,
        x_shared = x_shared,
        dots = dots,
        by_name = by_name
      ) |>
      futurize::futurize(
          seed = .seed,
          globals = summary_future$globals,
          packages = summary_future$packages
        ) |>
      purrr::list_rbind()

    rm(x_shared)
    gc(FALSE)
    result
  }

  bucket_ids |>
    purrr::map(process_shared_bucket) |>
    purrr::list_rbind()
}
