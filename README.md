Read WARC files from CommonCrawl.org in sparklyr
================

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
