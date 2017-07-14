#' Retrieves sample warc path
#'
#' @export
rcpp_read_warc_sample_path <- function() {
  normalizePath(system.file("samples/sample.warc.gz", package = "sparkwarc"))
}

#' Loads the sample warc file in Rcpp
#'
#' @param filter A regular expression that is applied to each warc entry
#'   efficiently by running native code using \code{Rcpp}.
#'
#' @export
rcpp_read_warc_sample <- function(filter = "") {
  sample_warc <- rcpp_read_warc_sample_path()

  sparkwarc:::rcpp_read_warc(sample_warc, filter)
}

#' Loads the sample warc file in Spark
#'
#' @param An active \code{spark_connection}.
#' @param filter A regular expression that is applied to each warc entry
#'   efficiently by running native code using \code{Rcpp}.
#'
#' @export
spark_read_warc_sample <- function(sc, filter = "") {
  sample_warc <- rcpp_read_warc_sample_path()

  spark_read_warc(sc, "sample_warc", sample_warc, overwrite = TRUE, group = TRUE, filter = filter)
}
