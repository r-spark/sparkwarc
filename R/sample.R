#' Loads the sample warc file
#'
#' @export
spark_read_warc_sample <- function() {
  sample_warc <- normalizePath(system.file("samples/sample.warc.gz", package = "sparkwarc"))

  sparkwarc:::rcpp_read_warc(sample_warc, "")
}
