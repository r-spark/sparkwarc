package SparkWARC

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import scala.util.matching._
import org.apache.spark.sql.types._

object WARC {
  def parse(sc: SparkContext, path: String, matchLine: String, repartitions: Int) : DataFrame = {
    val sqlContext = new SQLContext(sc)
    val warc = sc.textFile(path)
    val warcRepart = if (repartitions > 0) warc.repartition(repartitions) else warc

    val warcParsed = warcRepart
      .filter(line => line.contains(matchLine))
      .map(line => {
        Row(
          "<[^>]*>".r.findAllIn(line).length,
          line
        )
      })

    val warcStruct = StructType(
      StructField("tags", IntegerType, true) ::
      StructField("content", StringType, true) :: Nil
    )

    sqlContext.createDataFrame(warcParsed, warcStruct)
  }
}
