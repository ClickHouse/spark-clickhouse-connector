package xenon.clickhouse.hash

import xenon.clickhouse.hash.cityhash.{CityHash_v1_0_2, UInt128}

// https://github.com/ClickHouse/ClickHouse/blob/v23.5.3.24-stable/src/Functions/FunctionsHashing.h#L694
object CityHash64 extends HashFunc[Long] {
  override def applyHash(input: Array[Byte]): Long =
    CityHash_v1_0_2.CityHash64(input, 0, input.length)

  override def combineHashes(h1: Long, h2: Long): Long =
    CityHash_v1_0_2.Hash128to64(new UInt128(h1, h2))
}
