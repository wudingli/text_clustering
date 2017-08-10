import liutao.Const
import org.apache.spark.ml.linalg.{Matrices, Vectors}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.util.Identifiable

object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(Const.appName).setMaster(Const.sparkUri)
    val sc = SparkContext.getOrCreate(conf)
    val v1 = Vectors.dense(1,2,3)
    val v2 = Vectors.dense(2,3,4)
    println(v1)
    println(v2)

    val matrix = Matrices.zeros(2,3)

    val matrix2 = Matrices.ones(3,3)

    val matrix3 = Matrices.diag(Vectors.dense(2,2,2))
    val matrix4 = Matrices.dense(3,3, Array(1,2,3,4,5,6,7,8,9))


    println(matrix4)
    println(matrix4.toSparseColMajor)
//    val m1 = matrix.map(_ dot(v1))
  }
}
