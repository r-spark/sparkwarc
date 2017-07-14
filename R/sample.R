#' Loads the sample warc file
#'
#' @param filter A regular expression that is applied to each warc entry
#'   efficiently by running native code using \code{Rcpp}.
#'
#' @export
spark_read_warc_sample <- function(filter = "") {
  sample_warc <- normalizePath(system.file("samples/sample.warc.gz", package = "sparkwarc"))

  sparkwarc:::rcpp_read_warc(sample_warc, filter)
}
