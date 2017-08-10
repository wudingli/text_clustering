package liutao

import org.apache.spark.unsafe.hash.Murmur3_x86_32.hashUnsafeBytes
import org.apache.spark.unsafe.types.UTF8String

object HashUtil {
  private val seed = 42

  def nonNegativeMod(value: Int, cap: Int): Int = {
    val rawMod = value % cap
    rawMod + (if (rawMod < 0) cap else 0)
  }

  def hash(value: String, cap : Int): Int = {
    val utf8 = UTF8String.fromString(value)
    val hashed = hashUnsafeBytes(utf8.getBaseObject, utf8.getBaseOffset, utf8.numBytes(), seed)
    nonNegativeMod(hashed, cap)
  }
}
