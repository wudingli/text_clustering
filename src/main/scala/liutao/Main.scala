package liutao

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.JavaConverters._
import scala.io.StdIn

object Main {
  def main(args: Array[String]): Unit = {
    //读取文件
    val dir = new File(Const.filePath)
    val allFiles = FileReader.findFile(dir).toList

    //spark init
    val conf = new SparkConf().setAppName(Const.appName).setMaster(Const.sparkUri)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    val filesRdd = sc.parallelize(allFiles)

    //文件分割为段落的集合 List[String]
    val fileParsed = filesRdd.map(file => (file.getAbsolutePath, HtmlParser.parse(file)))

    //将文件转换为段落
    val beforeWordSplit = fileParsed.flatMap(el => {
      var count: Int = 0
      el._2.map((paragraph) => {
        count += 1
        (el._1 + "/" + count.toString, paragraph)
      })
    })

    //应用IKAnalyzer进行分词
    val afterWordSplit = beforeWordSplit.map(el => (el._1, WordAnalyzer.analyze(el._2)))
                                        .filter(_._2.nonEmpty)

    //计算TF-IDF
    var allWords: RDD[(String, Int)] = null
    var afterCalculateTFIDF : DataFrame = null
    if(Const.useHashTF){
      allWords = afterWordSplit.flatMap(_._2)
                               .distinct()
                               .map(x => (x, HashUtil.hash(x, Const.hashBucketNum)))
      val beforeCalculateTFIDF = spark.createDataFrame(afterWordSplit).toDF("label", "words")
      afterCalculateTFIDF = WordFrequence.calculateHashTFIDF(beforeCalculateTFIDF)
    } else {
      allWords = afterWordSplit.flatMap(_._2)
                               .distinct()
                               .zipWithIndex()
                               .map(el => (el._1, el._2.toInt))
      val flatData = afterWordSplit.map(el => (el._1, el._2, el._2.length)).flatMap(ele => ele._2.map(w => (ele._1, ele._3, w, 1.0)))
      val beforeCalculateTFIDF = spark.createDataFrame(flatData).toDF("label", "wordsCount", "word", "count")
      afterCalculateTFIDF = WordFrequence.calculateTFIDF(beforeCalculateTFIDF, allWords)
    }
    afterCalculateTFIDF.cache()
//    afterCalculateTFIDF.show(false)

    //训练LDA Model
    val ldaModel = new LDA().setK(Const.ldaTopicNum)
                             .setMaxIter(Const.ldaMaxIter)
                             .setOptimizer(Const.ldaOptimizer)
                             .fit(afterCalculateTFIDF)
    //    ldaModel.save("./LDAModel")
    //获得topic-word分布
    val topic = ldaModel.describeTopics()
    topic.cache()
    topic.show(false)

    //将topic-word分布中的word id的集合转换成word集合
    val flatTopic = topic.rdd.flatMap(row => row.getList[Int](1).toArray.map(el => (el.asInstanceOf[Int], row.getInt(0))))
                             .join(allWords.map(_.swap))
                             .map(el => (el._2._1, List((el._2._2, el._1))))
                             .reduceByKey((a, b) => a ++ b)
                             .sortByKey()
//    val saveOptions = Map("header" -> "true", "path" -> (Const.outputPath + "/topic-word"))
    spark.createDataFrame(flatTopic).toDF("topic","words").repartition(1).write.mode(SaveMode.Overwrite).json(Const.outputPath + "/topic-word")
//    flatTopic.repartition(1).saveAsTextFile(Const.outputPath + "/topic-word")
    //    println(ldaModel.logLikelihood(afterCalculateTFIDF))
    //    println(ldaModel.logPerplexity(afterCalculateTFIDF))

    //获得document-topic分布
    val transformed = ldaModel.transform(afterCalculateTFIDF)

    //过滤掉分布值小于Const.topicThreshold的topic
    val clusterResult = transformed.rdd.map(row => (row.getString(0), row.getAs[Vector](3).toArray.zipWithIndex.filter(el => el._1 > Const.topicThreshold).sortBy(_._1).reverse.map(_._2)))

    //显示过滤后的结果
    val docTopics = spark.createDataFrame(clusterResult).toDF("label", "topics")
    docTopics.cache()

//    docTopics.rdd.repartition(1).saveAsTextFile(Const.outputPath + "/document-topic")

    println("请输入关键字：")
    val keywords = StdIn.readLine().split(" ")
                                   .flatMap(el => WordAnalyzer.analyze(el))
                                   .filter(_.nonEmpty)
                                   .map(x => (x, 1.0))
    //获取输入关键字的id
    val keywordCode = sc.parallelize(keywords).join(allWords).map(el => el._2.swap)

    //搜索关键字得到结果，按TF-IDF值降序排列
    //label sum(hit)
    val searchResult = SearchHelper.getResult(afterCalculateTFIDF, spark.createDataFrame(keywordCode).toDF("keywordNo", "hit"))
//    searchResult.show(false)

    //得到与搜索结果主题相关的其他
    val a = searchResult.join(docTopics,"label")  //label sum(hit) topics
                        .withColumnRenamed("label", "hitLabel")
                        .withColumnRenamed("topics","hitTopics")  //hitLabel sum(hit) hitTopics
                        .crossJoin(docTopics) //hitLabel sum(hit) hitTopics label topics
                        .filter(row => (row.getList[Int](2).asScala.toSet & row.getList[Int](4).asScala.toSet).nonEmpty && (!row.getString(0).equals(row.getString(3))))
//    a.show(false)
    val searchResultWithSuggest = a.rdd.map(row => (row.getString(0),(row.getDouble(1), row.getList[Int](2).asScala, Array(row.getString(3)))))
                                       .reduceByKey((a,b) => (a._1, a._2, a._3 ++  b._3))
                                       .map(el => (el._1, el._2._1, el._2._2, el._2._3))

//    val searchResultWithSuggest = a.rdd.map(row => (row.getString(0), row.getDouble(1), row.getList[Int](2), row.getList[Int](2).toArray.map(el => docTopics.filter(row2 => row2 != null && row != null && row2.getList[Int](1).contains(el) && !row2.getString(0).equals(row.getString(0))))))
    val searchResultWithSuggestDf = spark.createDataFrame(searchResultWithSuggest).toDF("label", "hit", "topics", "relevant")
    val searchResultWithSuggestSorted = searchResultWithSuggestDf.sort(searchResultWithSuggestDf("hit").desc, searchResultWithSuggestDf("label"))
    ldaModel.save(Const.outputPath + "/lda-model")
    docTopics.repartition(1).write.mode(SaveMode.Overwrite).json(Const.outputPath + "/document-topic")
    searchResultWithSuggestSorted.repartition(1).write.mode(SaveMode.Overwrite).json(Const.outputPath + "/search-result")
    spark.stop()
  }
}
