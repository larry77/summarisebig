pkgs <- c("arrow", "dplyr", "future", "future.mirai", "futurize", "mori")
versions <- vapply(pkgs, function(p) {
  if (requireNamespace(p, quietly = TRUE)) as.character(utils::packageVersion(p)) else NA_character_
}, character(1))

info <- data.frame(
  item = c(
    "timestamp",
    "R.version",
    "platform",
    "OS",
    "machine",
    "logical_cores",
    paste0("package:", pkgs)
  ),
  value = c(
    format(Sys.time(), tz = "UTC", usetz = TRUE),
    R.version.string,
    R.version$platform,
    Sys.info()[["sysname"]],
    Sys.info()[["machine"]],
    as.character(parallel::detectCores(logical = TRUE)),
    versions
  ),
  stringsAsFactors = FALSE
)

print(info, row.names = FALSE)
if (length(commandArgs(trailingOnly = TRUE))) {
  utils::write.csv(info, commandArgs(trailingOnly = TRUE)[[1L]], row.names = FALSE)
}
