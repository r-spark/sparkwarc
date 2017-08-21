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
#' @param match_warc include only warc files mathcing this character string.
#' @param match_line include only lines mathcing this character string.
#' @param parser which parser implementation to use? Options are "scala"
#'   or "r" (default).
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
                            match_warc = "",
                            match_line = "",
                            parser = c("r", "scala"),
                            ...) {
  if (overwrite && name %in% dbListTables(sc)) {
    dbRemoveTable(sc, name)
  }

  if (!is.null(parse) && !parser %in% c("r", "scala"))
    stop("Invalid 'parser' value, must be 'r' or 'scala'")

  if (is.null(parser) || parser == "r") {
    paths_df <- data.frame(paths = strsplit(path, ",")[[1]])
    paths_tbl <- sdf_copy_to(sc, paths_df, name = "sparkwarc_paths", overwrite = TRUE)

    if (repartition > 0)
      paths_tbl <- sdf_repartition(paths_tbl, repartition)

    df <- spark_apply(paths_tbl, function(df) {
      entries <- apply(df, 1, function(path) {
        if (grepl("s3n://", path)) {
          path <- sub("s3n://commoncrawl/", "https://commoncrawl.s3.amazonaws.com/", path)
          temp_warc <- tempfile(fileext = ".warc.gz")
          download.file(url = path, destfile = temp_warc)
          path <- temp_warc
        }

        sparkwarc::rcpp_read_warc(path, filter = match_warc, include = match_line)
      })

      if (nrow(df) > 1) do.call("rbind", entries) else data.frame(entries)
    }, names = c("tags", "content")) %>% spark_dataframe()
  }
  else {
    if (nchar(match_warc) > 0) stop("Scala parser does not support 'match_warc'")

    df <- sparklyr::invoke_static(
      sc,
      "SparkWARC.WARC",
      "parse",
      spark_context(sc),
      path,
      match_line,
      as.integer(repartition))
  }

  result_tbl <- sdf_register(df, name)

  if (memory) {
    dbGetQuery(sc, paste("CACHE TABLE", DBI::dbQuoteIdentifier(sc, name)))
    dbGetQuery(sc, paste("SELECT count(*) FROM", DBI::dbQuoteIdentifier(sc, name)))
  }

  result_tbl
}
