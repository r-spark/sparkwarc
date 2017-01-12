package SparkWARC

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import scala.util.matching._
import org.apache.spark.sql.types._

object WARC {
  def load(sc: SparkContext, path: String, group: Boolean) : DataFrame = {
    if (group) sc.hadoopConfiguration.set(
      "textinputformat.record.delimiter", "WARC/1.0"
    )

    val warc = sc.textFile(path)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val df = warc.toDF
    sc.hadoopConfiguration.unset("textinputformat.record.delimiter")

    df
  }

  def parse(sc: SparkContext, path: String, group: Boolean) : DataFrame = {
    val sqlContext = new SQLContext(sc)
    val warc = sc.textFile(path)

    val warcParsed = warc.flatMap(line => {
      val tagsRegex = new Regex("<([a-zA-Z]+) ?([^>]*)>")

      tagsRegex.findAllIn(line).matchData.toList.flatMap(t => {
        val attrRegex = new Regex("[ ]*([a-zA-Z-]+)[ ]*=[ ]*\\\"([^\\\"]*)\\\".*")

        List.concat(
          List(Row.fromSeq(Seq(t.group(1), "", "", t.group(2)))),
          attrRegex.findAllIn(t.group(2)).matchData.toList.map(a => {
            Row.fromSeq(Seq(t.group(1), a.group(1), a.group(2), t.group(2)))
          })
        )
      }).union({
        val warcRegex = new Regex("(WARC-[a-zA-Z]): (.*)")

        warcRegex.findAllIn(line).matchData.toList.map(t => {
          Row.fromSeq(Seq(t.group(1), "", t.group(2), line))
        })
      })
    })

    val warcStruct = StructType(
      StructField("tag", StringType, true) ::
      StructField("attribute", StringType, true) ::
      StructField("value", StringType, true) ::
      StructField("original", StringType, true) :: Nil
    )

    sqlContext.createDataFrame(warcParsed, warcStruct)
  }
}
