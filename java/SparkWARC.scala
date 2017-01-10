package SparkWARC

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

object WARC {
  def load(sc: SparkContext, path: String, partitions: Int, group: Boolean) : DataFrame = {
    if (group) sc.hadoopConfiguration.set(
      "textinputformat.record.delimiter", "WARC/1.0"
    )

    val warc = sc.textFile(path, partitions)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val df = warc.toDF
    sc.hadoopConfiguration.unset("textinputformat.record.delimiter")

    df
  }
}
