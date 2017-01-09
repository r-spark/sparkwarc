package SparkWARC

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

object WARC {
  def load(sc: SparkContext, path: String) : DataFrame = {
    val warc = sc.wholeTextFiles(path)
    val warcpg = warc.flatMap(t => t._2.split("WARC/1.0"))

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    warcpg.toDF
  }
}
