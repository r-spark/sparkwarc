#' Reads a WARC File into Apache Spark
#'
#' Reads a WARC (Web ARChive) file into Apache Spark using sparklyr.
#'
#' @param sc An active \code{spark_connection}.
#' @param path The path to the warc file.
#' @param repartition The number of partitions to use when distributing the
#' table across the Spark cluster. The default (0) can be used to avoid
#' partitioning.
#' @param group \code{TRUE} to group by warc segment. Currently supported
#' only in HDFS and uncompressed files.
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
spark_read_warc <- function(sc, path, repartition = 0L, group = FALSE, ...) {
  df <- sparklyr::invoke_static(
    sc,
    "SparkWARC.WARC",
    "load",
    spark_context(sc),
    path,
    group)

  if (repartition > 0) {
    df <- invoke(df, "repartition", as.integer(repartition))
  }

  df
}
