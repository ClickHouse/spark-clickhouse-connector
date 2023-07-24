package xenon.clickhouse.hash

import org.apache.commons.codec.digest.MurmurHash2

// https://github.com/ClickHouse/ClickHouse/blob/v23.5.3.24-stable/src/Functions/FunctionsHashing.h#L519
object Murmurhash2_32 extends HashFunc[Long] {
  override def applyHash(input: Array[Byte]): Long = {
    val h = MurmurHash2.hash32(input, input.length, 0)
    Util.Int32ToUint32(h)
  }
  override def combineHashes(h1: Long, h2: Long): Long =
    Util.Int32ToUint32(Util.int32Impl(h1) ^ h2)
}
