package liutao

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.DataFrame

object SearchHelper {
  def getResult(afterCalculateTFIDF: DataFrame, keywords: DataFrame) : DataFrame = {
    val spark  = afterCalculateTFIDF.sparkSession
    val tmp = afterCalculateTFIDF.crossJoin(keywords) //label rawFeatures features keywordNo hit
                                 .filter(row => row.getAs[Vector](2).toSparse.indices.contains(row.getInt(3)))//.groupBy("label").sum("hit")
    val hit = tmp.rdd.map(row => (row.getString(0), row.getAs[Vector](2), row.getInt(3),  row.getAs[Vector](2).apply(row.getInt(3))))
    val result = spark.createDataFrame(hit).toDF("label", "features", "keywordNo", "hit").groupBy("label").sum("hit")
    result.sort(result("sum(hit)").desc, result("label")) // label sum(hit)
  }
}
