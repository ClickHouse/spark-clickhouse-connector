package xenon.clickhouse.hash

import org.apache.commons.codec.digest.MurmurHash3

// https://github.com/ClickHouse/ClickHouse/blob/v23.5.3.24-stable/src/Functions/FunctionsHashing.h#L543
object Murmurhash3_64 extends HashFunc[Long] {
  override def applyHash(input: Array[Byte]): Long = {
    val hashes = MurmurHash3.hash128x64(input, 0, input.length, 0)
    hashes(0) ^ hashes(1)
  }

  override def combineHashes(h1: Long, h2: Long): Long =
    HashUtils.intHash64Impl(h1) ^ h2
}
