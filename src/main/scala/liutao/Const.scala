package liutao

object Const {
  val outputPath = "../output"
  val filePath ="../raw_data" //扫描文件路径
  val defaultCharset = "UTF-8"
  val splitType = "100+" // html：按照html标签分割；n+：n个字符以上，直至遇到stopChar；n：n个字符
  val stopChar = List(' ', '。', '\r', '\n', '!', '！', '?', '？') //与splitType联合使用，当splitType为n+时，判断段落是否结束
  val ikSmart = true //IKAnalyzer 启用smart模式开关
  val sparkUri = "local"
  val appName = "Text Clustering"
  val useHashTF = true  //启用HashingTF开关
  val hashBucketNum : Int = 1 << 18 //HashingTF的numFeatures；当useHashTF为true时生效
  val ldaMaxIter = 100 //LDA最大迭代数
  val ldaOptimizer = "em" //LDA Optimizer
  val ldaTopicNum = 10 //LDA Topic number
  val topicThreshold = 0.1 //Document belong topic when topic distribution over topicThreshold
  val keywordPath = "../keyword"
}
