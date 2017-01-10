#' Provides WARC paths for commoncrawl.org
#'
#' Provides WARC paths for commoncrawl.org. To be used with
#' \code{spark_read_warc}.
#'
#' @param start The first path to retrieve.
#' @param end The last path to retrieve.
#'
#' @examples
#'
#' cc_warc(1)
#' cc_warc(2, 3)
#'
#' @export
#' @importFrom utils read.table
cc_warc <- function(start, end = start) {
  warcPathsFile <- system.file("samples/sample.warc.paths", package = "sparkwarc")
  warcPaths <- read.table(warcPathsFile)
  paste(warcPaths[seq(start, end), ], collapse = ",")
}
