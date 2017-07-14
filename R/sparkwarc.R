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
#' @param parse \code{TRUE} to parse warc into tags, attribute, value, etc.
#' @param filter A regular expression that is applied to each warc entry
#'   efficiently by running native code using \code{Rcpp}.
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
#' @useDynLib sparkwarc, .registration = TRUE
#' @import DBI
spark_read_warc <- function(sc,
                            name,
                            path,
                            repartition = 0L,
                            memory = TRUE,
                            overwrite = TRUE,
                            group = FALSE,
                            parse = FALSE,
                            filter = "",
                            ...) {
  if (overwrite && name %in% dbListTables(sc)) {
    dbRemoveTable(sc, name)
  }

  if (!parse) {
    paths_df <- data.frame(paths = strsplit(path, ",")[[1]])
    paths_tbl <- sdf_copy_to(sc, paths_df, name = "sparkwarc_paths", overwrite = TRUE)

    if (repartition > 0)
      paths_tbl <- sdf_repartition(paths_tbl, repartition)

    df <- spark_apply(paths_tbl, function(df) {
      entries <- apply(df, 1, function(path) {
        if (grepl("s3n://", path)) {
          path <- sub("s3n://", "commoncrawl.s3.amazonaws.com", path)
          temp_warc <- tempfile(fileext = ".warc.gz")
          download.file(url = path, destfile = temp_warc)
          path <- temp_warc
        }

        rcpp_read_warc(path, filter = "")
      })

      if (nrow(df) > 1) do.call("rbind", entries) else data.frame(entries)
    }) %>% spark_dataframe()
  }
  else {
    df <- sparklyr::invoke_static(
      sc,
      "SparkWARC.WARC",
      if (parse) "parse" else "load",
      spark_context(sc),
      path,
      group,
      as.integer(repartition))
  }

  result_tbl <- sdf_register(df, name)

  if (memory) {
    dbGetQuery(sc, paste("CACHE TABLE", DBI::dbQuoteIdentifier(sc, name)))
    dbGetQuery(sc, paste("SELECT count(*) FROM", DBI::dbQuoteIdentifier(sc, name)))
  }

  result_tbl
}
