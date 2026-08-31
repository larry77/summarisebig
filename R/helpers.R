.sb_pack_groups <- function(n, target_rows) {
  id <- integer(length(n))
  current <- 1L
  used <- 0

  for (i in seq_along(n)) {
    ni <- n[[i]]

    if (ni > target_rows) {
      if (used > 0) {
        current <- current + 1L
        used <- 0
      }
      id[[i]] <- current
      current <- current + 1L
      used <- 0
      next
    }

    if (used > 0 && used + ni > target_rows) {
      current <- current + 1L
      used <- 0
    }

    id[[i]] <- current
    used <- used + ni
  }

  id
}

.sb_normalise_map_reduce <- function(spec, default_env) {
  if (
    !is.list(spec) ||
    !length(spec) ||
    is.null(names(spec)) ||
    any(names(spec) == "")
  ) {
    stop(
      "`.map_reduce` must be a non-empty named list.\n\n",
      "Recommended form:\n\n",
      ".map_reduce = list(\n",
      "  n  = ~ dplyr::n(),\n",
      "  sx = ~ sum(x)\n",
      ")",
      call. = FALSE
    )
  }

  if (anyDuplicated(names(spec))) {
    stop("Names in `.map_reduce` must be unique.", call. = FALSE)
  }

  out <- vector("list", length(spec))
  names(out) <- names(spec)

  for (i in seq_along(spec)) {
    item <- spec[[i]]

    if (rlang::is_quosure(item)) {
      out[[i]] <- item
      next
    }

    if (inherits(item, "formula")) {
      if (length(item) != 2L) {
        stop(
          "MapReduce formulas must be one-sided formulas, for example `~ sum(x)`.",
          call. = FALSE
        )
      }
      out[[i]] <- rlang::new_quosure(rlang::f_rhs(item), rlang::f_env(item))
      next
    }

    if (is.call(item) || is.symbol(item)) {
      out[[i]] <- rlang::new_quosure(item, default_env)
      next
    }

    stop(
      "Each `.map_reduce` element must be a one-sided formula, quosure, or quoted R expression.",
      call. = FALSE
    )
  }

  out
}

.sb_future_globals_from_dots <- function(dots, caller, data_names = character()) {
  if (!length(dots)) {
    return(list(
      globals = list(),
      packages = character()
    ))
  }

  exprs <- lapply(dots, rlang::quo_get_expr)
  expr <- as.call(c(list(as.name("list")), exprs))

  gp <- future::getGlobalsAndPackages(
    expr,
    envir = caller,
    globals = TRUE
  )

  globals <- as.list(gp$globals)

  # Bare data-column names belong to dplyr's data mask, not to the caller's
  # global environment. If a same-named object happens to exist in the caller,
  # exporting it would be unnecessary and potentially expensive.
  if (length(globals) && length(data_names)) {
    globals <- globals[setdiff(names(globals), data_names)]
  }

  list(
    globals = globals,
    packages = unique(gp$packages)
  )
}

