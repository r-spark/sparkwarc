#' Reads a WARC File into Apache Spark
#'
#' Reads a WARC (Web ARChive) file into Apache Spark using sparklyr.
#'
#' @param sc An active \code{spark_connection}.
#' @param path The path to the warc file.
#' @param partitions Number of partitions to use for loaded warc.
#' @param group \code{TRUE} to group by warc segment.
#' @param ... Additional arguments reserved for future use.
#'
#' @examples
#'
#' library(sparklyr)
#' sc <- spark_connect(master = "spark://HOST:PORT")
#' df <- spark_read_warc(
#'   sc,
#'   system.file("samples/sample.warc", package = "sparkwarc")
#' )
#'
#' spark_disconnect(sc)
#'
#' @export
spark_read_warc <- function(sc, path, partitions = 10, group = FALSE, ...) {
  sparklyr::invoke_static(
    sc,
    "SparkWARC.WARC",
    "load",
    spark_context(sc),
    path,
    as.integer(partitions),
    group)
}
