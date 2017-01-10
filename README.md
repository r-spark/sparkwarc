sparkwarc - WARC files in sparklyr
================

Install
=======

Install [sparkwarc from CRAN](https://cran.r-project.org/package=sparkwarc) or the dev version with:

``` r
devtools::install_github("javierluraschi/sparkwarc")
```

Intro
=====

The following example loads a very small subset of a WARC file from [Common Crawl](http://commoncrawl.org), a nonprofit 501 organization that crawls the web and freely provides its archives and datasets to the public.

``` r
library(sparkwarc)
library(sparklyr)
library(DBI)
library(dplyr)
```

    ## 
    ## Attaching package: 'dplyr'

    ## The following objects are masked from 'package:stats':
    ## 
    ##     filter, lag

    ## The following objects are masked from 'package:base':
    ## 
    ##     intersect, setdiff, setequal, union

``` r
sc <- spark_connect(master = "local", version = "2.0.1")
spark_read_warc(sc, "warc", system.file("samples/sample.warc.gz", package = "sparkwarc"))
```

``` sql
SELECT count(value)
FROM WARC
WHERE length(regexp_extract(value, '<html', 0)) > 0
```

| count(value) |
|:-------------|
| 6            |

``` r
spark_regexp_stats <- function(tbl, regval) {
  tbl %>%
    transmute(language = regexp_extract(value, regval, 1)) %>%
    group_by(language) %>%
    summarize(n = n())
}
```

``` r
regexpLang <- "http-equiv=\"Content-Language\" content=\"(.*)\""
tbl(sc, "warc") %>% spark_regexp_stats(regexpLang)
```

    ## Source:   query [2 x 2]
    ## Database: spark connection master=local[8] app=sparklyr local=TRUE
    ## 
    ##   language     n
    ##      <chr> <dbl>
    ## 1    ru-RU     5
    ## 2           1709

``` r
spark_disconnect(sc)
```

Scale
=====

By [running sparklyr in EMR](https://aws.amazon.com/blogs/big-data/running-sparklyr-rstudios-r-interface-to-spark-on-amazon-emr/), one can configure an EMR cluster and load about **~5GB** of data using:

``` r
sc <- spark_connect(master = "yarn-client")
spark_read_warc(sc, "warc", cc_warc(1, 1))

tbl(sc, "warc") %>% summarize(n = n())
spark_disconnect_all()
```

To read the first 200 files, or about **~1TB** of data, first scale the cluster, consider maximizing resource allocation with the followin EMR config:

    [
      {
        "Classification": "spark",
        "Properties": {
          "maximizeResourceAllocation": "true"
        }
      }
    ]

Followed by loading the `[1, 200]` file range with:

``` r
sc <- spark_connect(master = "yarn-client")
spark_read_warc(sc, "warc", cc_warc(1, 200))

tbl(sc, "warc") %>% summarize(n = n())
spark_disconnect_all()
```

To read the entire crawl, about **~1PB**, a custom script would be needed to load all the WARC files.
