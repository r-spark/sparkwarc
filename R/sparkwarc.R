#' Reads a WARC File into Apache Spark
#'
#' Reads a WARC (Web ARChive) file into Apache Spark using sparklyr.
#'
#' @param sc An active \code{spark_connection}.
#' @param name The name to assign to the newly generated table.
#' @param path The path to the file. Needs to be accessible from the cluster.
#'   Supports the \samp{"hdfs://"}, \samp{"s3n://"} and \samp{"file://"} protocols.
#' @param repartition The number of partitions used to distribute the
#'   generated table. Use 0 (the default) to avoid partitioning.
#' @param memory Boolean; should the data be loaded eagerly into memory? (That
#'   is, should the table be cached?)
#' @param overwrite Boolean; overwrite the table with the given name if it
#'   already exists?
#' @param group \code{TRUE} to group by warc segment. Currently supported
#'   only in HDFS and uncompressed files.
#' @param ... Additional arguments reserved for future use.
#'
#' @examples
#'
#' library(sparklyr)
#' sc <- spark_connect(master = "spark://HOST:PORT")
#' df <- spark_read_warc(
#'   sc,
#'   system.file("samples/sample.warc", package = "sparkwarc"),
#'   repartition = FALSE,
#'   memory = FALSE,
#'   overwrite = FALSE
#' )
#'
#' spark_disconnect(sc)
#'
#' @export
#' @import DBI
spark_read_warc <- function(sc,
                            name,
                            path,
                            repartition = 0L,
                            memory = TRUE,
                            overwrite = TRUE,
                            group = FALSE,
                            ...) {
  if (overwrite && name %in% dbListTables(sc)) {
    dbRemoveTable(sc, name)
  }

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

  invoke(df, "registerTempTable", name)

  if (memory) {
    dbGetQuery(sc, paste("CACHE TABLE", DBI::dbQuoteIdentifier(sc, name)))
    dbGetQuery(sc, paste("SELECT count(*) FROM", DBI::dbQuoteIdentifier(sc, name)))
  }

  invisible(NULL)
}
