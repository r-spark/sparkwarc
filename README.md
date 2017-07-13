sparkwarc - WARC files in sparklyr
================

Install
=======

Install using with:

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

``` r
sc <- spark_connect(master = "local")
```

    ## * Using Spark: 2.1.0

``` r
spark_read_warc(
  sc,
  "warc",
  system.file("samples/sample.warc.gz", package = "sparkwarc"),
  repartition = 8)
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
cc_regex <- function(ops) {
  ops %>%
    filter(regval != "") %>%
    group_by(regval) %>%
    summarize(count = n()) %>%
    arrange(desc(count)) %>%
    head(100)
}

cc_stats <- function(regex) {
  tbl(sc, "warc") %>%
    transmute(regval = regexp_extract(value, regex, 1)) %>%
    cc_regex()
}
```

``` r
cc_stats("http-equiv=\"Content-Language\" content=\"(.*)\"")
```

    ## # Source:     lazy query [?? x 2]
    ## # Database:   spark_connection
    ## # Ordered by: desc(count)
    ##   regval count
    ##    <chr> <dbl>
    ## 1  ru-RU     5

``` r
cc_stats("<script .*src=\".*/(.+)\".*")
```

    ## # Source:     lazy query [?? x 2]
    ## # Database:   spark_connection
    ## # Ordered by: desc(count)
    ##                            regval count
    ##                             <chr> <dbl>
    ## 1                           08.js     5
    ## 2                           ga.js     5
    ## 3 jquery.formtips.1.2.2.packed.js     5
    ## 4   jquery-ui-1.7.2.custom.min.js     5
    ## 5             jquery-1.4.2.min.js     5
    ## 6                        start.js     5
    ## 7           jquery.equalHeight.js     5
    ## 8                      lytebox.js     5
    ## 9                      plusone.js     5

``` r
cc_stats("<([a-zA-Z]+)>")
```

    ## # Source:     lazy query [?? x 2]
    ## # Database:   spark_connection
    ## # Ordered by: desc(count)
    ##      regval count
    ##       <chr> <dbl>
    ##  1       li    53
    ##  2     span    26
    ##  3       th    18
    ##  4        p    17
    ##  5       ul    16
    ##  6       tr    13
    ##  7   strong     7
    ##  8    title     6
    ##  9     body     6
    ## 10     head     6
    ## 11      div     6
    ## 12 noscript     5
    ## 13    table     3
    ## 14       td     3
    ## 15       br     1
    ## 16    style     1

``` r
cc_stats(" ([a-zA-Z]{5,10}) ")
```

    ## # Source:     lazy query [?? x 2]
    ## # Database:   spark_connection
    ## # Ordered by: desc(count)
    ##      regval count
    ##       <chr> <dbl>
    ##  1  counter    10
    ##  2   PUBLIC     6
    ##  3   return     6
    ##  4  Banners     5
    ##  5   widget     5
    ##  6 function     5
    ##  7   Banner     5
    ##  8    solid     2
    ##  9    Nutch     1
    ## 10   Domain     1
    ## 11    visit     1
    ## 12    crawl     1
    ## 13 Registry     1
    ## 14   Parked     1
    ## 15   Format     1
    ## 16 priceUAH     1
    ## 17   domain     1

``` r
cc_stats("<meta .*keywords.*content=\"([^,\"]+).*")
```

    ## # Source:     lazy query [?? x 2]
    ## # Database:   spark_connection
    ## # Ordered by: desc(count)
    ##                               regval count
    ##                                <chr> <dbl>
    ## 1                                Лес     1
    ## 2                           Вип Степ     1
    ## 3                       domain names     1
    ## 4 Регистрация-ликвидация предприятий     1
    ## 5                            Свобода     1
    ## 6                               Foxy     1

``` r
cc_stats("<script .*src=\".*/([^/]+.js)\".*")
```

    ## # Source:     lazy query [?? x 2]
    ## # Database:   spark_connection
    ## # Ordered by: desc(count)
    ##                            regval count
    ##                             <chr> <dbl>
    ## 1 jquery.formtips.1.2.2.packed.js     5
    ## 2                           08.js     5
    ## 3                           ga.js     5
    ## 4           jquery.equalHeight.js     5
    ## 5                      lytebox.js     5
    ## 6                      plusone.js     5
    ## 7   jquery-ui-1.7.2.custom.min.js     5
    ## 8             jquery-1.4.2.min.js     5
    ## 9                        start.js     5

``` r
spark_disconnect(sc)
```

Querying 1GB
============

``` r
warc_big <- normalizePath("~/cc.warc.gz")           # Name a 5GB warc file
if (!file.exists(warc_big))                         # If the file does not exist
  download.file(                                    # download by
    gsub("s3n://commoncrawl/",                      # mapping the S3 bucket url
         "https://commoncrawl.s3.amazonaws.com/",   # into a adownloadable url
         sparkwarc::cc_warc(1)), warc_big)          # from the first archive file
```

``` r
config <- spark_config()
config[["spark.memory.fraction"]] <- "0.9"
config[["spark.executor.memory"]] <- "10G"
config[["sparklyr.shell.driver-memory"]] <- "10G"

sc <- spark_connect(master = "local", config = config)
```

    ## * Using Spark: 2.1.0

``` r
spark_read_warc(
  sc,
  "warc",
  warc_big,
  repartition = 8)
```

df &lt;- data.frame(list(a = list("a,b,c")))

``` sql
SELECT count(value)
FROM WARC
WHERE length(regexp_extract(value, '<([a-z]+)>', 0)) > 0
```

| count(value) |
|:-------------|
| 6336761      |

``` sql
SELECT count(value)
FROM WARC
WHERE length(regexp_extract(value, '<html', 0)) > 0
```

| count(value) |
|:-------------|
| 74519        |

``` r
cc_stats("http-equiv=\"Content-Language\" content=\"([^\"]*)\"")
```

    ## # Source:     lazy query [?? x 2]
    ## # Database:   spark_connection
    ## # Ordered by: desc(count)
    ##    regval count
    ##     <chr> <dbl>
    ##  1     en   533
    ##  2  en-us   323
    ##  3     ru   150
    ##  4     es   127
    ##  5  en-US   105
    ##  6     fr    95
    ##  7     de    92
    ##  8     pl    71
    ##  9     cs    48
    ## 10     ja    45
    ## # ... with 90 more rows

``` r
cc_stats("WARC-Target-URI: http://([^/]+)/.*")
```

    ## # Source:     lazy query [?? x 2]
    ## # Database:   spark_connection
    ## # Ordered by: desc(count)
    ##                        regval count
    ##                         <chr> <dbl>
    ##  1    www.urbandictionary.com   156
    ##  2                 my-shop.ru    69
    ##  3 hfboards.hockeysfuture.com    69
    ##  4      www.greatlakes4x4.com    66
    ##  5        www.opensecrets.org    60
    ##  6         www.summitpost.org    57
    ##  7             brainly.com.br    57
    ##  8         www.mobileread.com    54
    ##  9          www.genealogy.com    54
    ## 10               shop.ccs.com    51
    ## # ... with 90 more rows

``` r
cc_stats("<([a-zA-Z]+)>")
```

    ## # Source:     lazy query [?? x 2]
    ## # Database:   spark_connection
    ## # Ordered by: desc(count)
    ##    regval   count
    ##     <chr>   <dbl>
    ##  1     li 2492324
    ##  2   span  506471
    ##  3     tr  440658
    ##  4      p  432221
    ##  5     td  398106
    ##  6     ul  258962
    ##  7    div  211937
    ##  8 script  198504
    ##  9     br  196993
    ## 10 strong  152675
    ## # ... with 90 more rows

``` r
cc_stats("<meta .*keywords.*content=\"([a-zA-Z0-9]+).*")
```

    ## # Source:     lazy query [?? x 2]
    ## # Database:   spark_connection
    ## # Ordered by: desc(count)
    ##    regval count
    ##     <chr> <dbl>
    ##  1  width   285
    ##  2   http   235
    ##  3   free   110
    ##  4   text   110
    ##  5    The   100
    ##  6  index    91
    ##  7  https    85
    ##  8  SKYPE    59
    ##  9      1    55
    ## 10   news    48
    ## # ... with 90 more rows

``` r
spark_disconnect(sc)
```

Querying 1TB
============

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

To **query ~1PB** for the entire crawl, a custom script would be needed to load all the WARC files.
