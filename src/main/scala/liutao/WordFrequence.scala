package liutao

import org.apache.spark.ml.feature.{HashingTF, IDF}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.util.control.Breaks

object WordFrequence {

  /**
    * 计算TF-IDF值，TF使用HashingTF
    * @param df 语料的分词结果，必须包含label words两列。
    *           label:文档的唯一标识
    *           words:文档包含的单词集合
    * @return 包含TF和TF-IDF特征的结果
    *         label:文档的唯一标识
    *         rawFeatures:TF特征Vector
    *         features:TF-IDF特征Vector
    */
  def calculateHashTFIDF(df: DataFrame): DataFrame = {
    val hashingTF = new HashingTF().setInputCol("words")
                                    .setOutputCol("rawFeatures")
                                    .setNumFeatures(Const.hashBucketNum)

    val dataWithRawFeature = hashingTF.transform(df)

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(dataWithRawFeature)

    idfModel.transform(dataWithRawFeature).select("label", "rawFeatures", "features")
  }

  /**
    * 计算TF-IDF值
    * @param df 语料分词的按词Flat结果，必须包含label word wordCount count四列。
    *           label:文档的唯一标识，因为是按词Flat，故会出现多个
    *           word:文档包含的单词
    *           wordCount:文档所包含的所有的词数
    *           count:文档中对word的计数，初始值可统一设置为1或相应权值
    * @param allWords 所有文档中包含的词及其Id（唯一标识）
    * @return 包含TF和TF-IDF特征的结果
    *         label:文档的唯一标识
    *         rawFeatures:TF特征Vector
    *         features:TF-IDF特征Vector
    */
  def calculateTFIDF(df: DataFrame, allWords: RDD[(String, Int)]): DataFrame = {
    val allWordsDF = df.sparkSession.createDataFrame(allWords).toDF("word", "id")

    val dataWithRawFeature = calculateTF(df, allWordsDF)

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(dataWithRawFeature)

    idfModel.transform(dataWithRawFeature) // label rawFeatures features
  }

  /**
    * 计算TF值
    * @param df 语料分词的按词Flat结果，必须包含label word wordCount count四列。
    *           label:文档的唯一标识，因为是按词Flat，故会出现多个
    *           word:文档包含的单词
    *           wordCount:文档所包含的所有的词数
    *           count:文档中对word的计数，初始值可统一设置为1或相应权值
    * @param allWords 所有文档中包含的词及其Id（唯一标识）
    *                 word: 单词
    *                 id: 单词的标识
    * @return 包含TF特征的结果
    *         label:文档的唯一标识
    *         rawFeatures:TF特征Vector
    */
  def calculateTF(df: DataFrame, allWords: DataFrame): DataFrame = {
    val allWordsCount = allWords.count().toInt
    val wordCount = df.join(allWords, "word")
                      .groupBy("label", "wordsCount", "id")
                      .sum("count")
    val wordFrequence = wordCount.select(wordCount("label"), wordCount("id"), wordCount("sum(count)")/wordCount("wordsCount"))
    val tmp = wordFrequence.rdd.map(x => (x.getString(0), (Array(x.getInt(1)), Array(x.getDouble(2)))))
    val result = tmp.reduceByKey((a, b) => {
      var indices = a._1
      var value = a._2
      for (i <- b._1.indices) {
        val loop = new Breaks
        var found = false
        loop.breakable(
          for (j <- indices.indices) {
            if (b._1(i) < indices(j)) {
              indices = (indices.dropRight(indices.length - j) :+ b._1(i)) ++ indices.drop(j)
              value = (value.dropRight(value.length - j) :+ b._2(i)) ++ value.drop(j)
              found = true
              loop.break()
            }
          }
        )
        if (!found) {
          indices = indices :+ b._1(i)
          value = value :+ b._2(i)
        }
      }
      (indices, value)
    }).map(el => (el._1, Vectors.sparse(allWordsCount, el._2._1, el._2._2)))
    val ret = df.sparkSession.createDataFrame(result).toDF("label", "rawFeatures")
    ret
  }

}
