package liutao

import java.io.File

object FileReader {

  def findFile(dir: File): Iterator[File] = {
    val dirs = dir.listFiles().filter(_.isDirectory)
    val files = dir.listFiles().filter(_.isFile)
    files.toIterator ++ dirs.toIterator.flatMap(findFile)
  }

}
