package SparkWARC

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

object WARC {
  def load(sc: SparkContext, path: String) : DataFrame = {
    val warc = sc.textFile(path)
    val warcIdx = warc.zipWithIndex.map(_.swap).cache()

    val warcMarksFirst = warcIdx.filter(t => t._2 == "WARC/1.0").map(t => t._1)
    val warcMarksSecond = warcMarksFirst.zipWithIndex.filter(t => t._2 != 0).map(t => t._1)

    val warcMarksFirstIdx = warcMarksFirst.zipWithIndex.map(_.swap)
    val warcMarksSecondIdx = warcMarksSecond.zipWithIndex.map(_.swap)

    val warcJoined = warcMarksFirstIdx.join(warcMarksSecondIdx)

    val warcEntriesMap = warcJoined.flatMap(t => (t._2._1 to t._2._2).map(i => (i, t._1)))

    val warcGrouped = warcIdx.join(warcEntriesMap).map(t => (t._2._2, (t._1, t._2._1))).groupByKey

    val warcPaged = warcGrouped.mapValues(t => t.toList.sortBy(_._1).map(i => i._2).mkString("|")).map(i => i._2)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val df = warcPaged.toDF
    df
  }
}
