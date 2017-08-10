package liutao

import java.io.File

import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element, TextNode}

import scala.util.control.Breaks
import scala.util.matching.Regex
import scala.util.matching.Regex.Match

object HtmlParser {
//  def main(args: Array[String]): Unit = {
//    val s = parse(new File(Const.filePath + "/榆树诊所.html"))
//  }


  def parse(file: File): List[String] = {
    val firstLoadDoc = Jsoup.parse(file, Const.defaultCharset, "")
    val charset = parseEncode(firstLoadDoc)
    val doc = if (Const.defaultCharset.equals(charset.toUpperCase()) || charset.isEmpty) firstLoadDoc else Jsoup.parse(file, charset, "")
    if (Const.splitType.equals("html")) {
      splitDocByHtmlTag(doc)
    } else {
      val content = doc.text()
      var list: List[String] = Nil
      if (Const.splitType.matches("^[0-9]+$")) {
        val step: Int = Integer.parseInt(Const.splitType)
        var start = 0
        var end = 0
        while (start < content.length) {
          end = start + step
          if (end > content.length) end = content.length
          val str = content.substring(start, end).trim()
          if (!"".equals(str)) {
            list = list :+ str
          }
          start = end
        }
      } else if (Const.splitType.matches("^[0-9]+\\+$")) {
        val step: Int = Integer.parseInt(Const.splitType.substring(0, Const.splitType.length - 1))
        var start = 0
        var end = 0
        while (start < content.length) {
          end = start + step
          if (end > content.length) end = content.length
          else {
            while (end < content.length && !Const.stopChar.contains(content.charAt(end - 1)) && !Const.stopChar.contains(content.charAt(end))) {
              end += 1
            }
          }
          val str = content.substring(start, end).trim()
          if (!"".equals(str)) {
            list = list :+ str
          }
          start = end
        }
      }
      list
    }
  }

  def splitDocByHtmlTag(doc: Document): List[String] = {
    doc.getAllElements
      .toArray()
      .flatMap(_.asInstanceOf[Element].childNodes().toArray())
      .filter(_.isInstanceOf[TextNode])
      .map(_.asInstanceOf[TextNode].text())
      .filter(_.trim().nonEmpty).toList
  }

  def parseEncode(doc: Document): String = {
    val elements = doc.getElementsByTag("meta").iterator()
    val charsetPattern: Regex = ".*charset=([0-9a-zA-Z-]+)(;|$)".r
    var charset = ""
    val loop = new Breaks
    loop.breakable(
      while (elements.hasNext) {
        val ele = elements.next()
        charset = ele.attr("charset")
        if (!"".equals(charset)) {
          loop.break()
        }
        if ("Content-Type".equals(ele.attr("http-equiv"))) {
          val content = ele.attr("content")
          charsetPattern.findFirstMatchIn(content) match {
            case s: Some[Match] => charset = s.get.group(1)
            case None => println("Charset Error:" + content)
          }
          loop.break()
        }
      }
    )
    charset
  }

}
