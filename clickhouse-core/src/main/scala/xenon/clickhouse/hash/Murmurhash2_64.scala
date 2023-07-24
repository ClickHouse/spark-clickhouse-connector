package xenon.clickhouse.hash

import org.apache.commons.codec.digest.MurmurHash2

// https://github.com/ClickHouse/ClickHouse/blob/v23.5.3.24-stable/src/Functions/FunctionsHashing.h#L460
object Murmurhash2_64 extends HashFunc[Long] {
  override def applyHash(input: Array[Byte]): Long =
    MurmurHash2.hash64(input, input.length, 0)

  override def combineHashes(h1: Long, h2: Long): Long =
    Util.intHash64Impl(h1) ^ h2
}
