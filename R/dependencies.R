spark_dependencies <- function(spark_version, scala_version, ...) {
  sparklyr::spark_dependency(
    jars = c(
      system.file(
        sprintf("java/sparkwarc-%s-%s.jar", spark_version, scala_version),
        package = "sparkwarc"
      )
    ),
    packages = c(
    )
  )
}

#' @import sparklyr
.onLoad <- function(libname, pkgname) {
  sparklyr::register_extension(pkgname)
}

.onUnload <- function(libpath) {
  library.dynam.unload("sparkwarc", libpath)
}
