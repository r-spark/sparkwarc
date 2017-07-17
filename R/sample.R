#' Retrieves sample warc path
#'
#' @export
spark_warc_sample_path <- function() {
  normalizePath(system.file("samples/sample.warc.gz", package = "sparkwarc"))
}

#' Loads the sample warc file in Rcpp
#'
#' @param filter A regular expression used to filter to each warc entry
#'   efficiently by running native code using \code{Rcpp}.
#' @param include A regular expression used to keep only matching lines
#'   efficiently by running native code using \code{Rcpp}.
#'
#' @export
rcpp_read_warc_sample <- function(filter = "", include = "") {
  sample_warc <- spark_warc_sample_path()

  sparkwarc:::rcpp_read_warc(sample_warc, filter, include)
}

#' Loads the sample warc file in Spark
#'
#' @param An active \code{spark_connection}.
#' @param filter A regular expression used to filter to each warc entry
#'   efficiently by running native code using \code{Rcpp}.
#' @param include A regular expression used to keep only matching lines
#'   efficiently by running native code using \code{Rcpp}.
#'
#' @export
spark_read_warc_sample <- function(sc, filter = "", include = "") {
  sample_warc <- rcpp_read_warc_sample_path()

  spark_read_warc(
    sc,
    "sample_warc",
    sample_warc,
    overwrite = TRUE,
    group = TRUE,
    filter = filter,
    include = include)
}
