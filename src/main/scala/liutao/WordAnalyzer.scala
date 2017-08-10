package liutao

import java.io.StringReader

import org.wltea.analyzer.core.{IKSegmenter, Lexeme}

object WordAnalyzer {
//  def main(args: Array[String]): Unit = {
//    val text= "IK Analyzer是一个结合词典分词和文法分词的中文分词开源工具包。它使用了全新的正向迭代最细粒度切分算法。"
//    println(analyze(text))
//  }

  def analyze(text: String): List[String] = {
    val reader = new StringReader(text)
    val analyzer = new IKSegmenter(reader, Const.ikSmart)
    var result:List[String] = Nil
    var lexeme : Lexeme = analyzer.next()
    while (lexeme != null) {
      result = result :+ lexeme.getLexemeText
      lexeme = analyzer.next()
    }
    result
  }
}
