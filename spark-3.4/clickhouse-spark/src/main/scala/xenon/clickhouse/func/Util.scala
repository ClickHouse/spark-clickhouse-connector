/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package xenon.clickhouse.func

object Util {
  def intHash64Impl(x: Long): Long =
    // https://github.com/ClickHouse/ClickHouse/blob/v23.5.3.24-stable/src/Functions/FunctionsHashing.h#L140
    intHash64(x ^ 0x4cf2d2baae6da887L)

  def intHash64(l: Long): Long = {
    // https://github.com/ClickHouse/ClickHouse/blob/v23.5.3.24-stable/src/Common/HashTable/Hash.h#L26
    var x = l
    x ^= x >>> 33;
    x *= 0xff51afd7ed558ccdL;
    x ^= x >>> 33;
    x *= 0xc4ceb9fe1a85ec53L;
    x ^= x >>> 33;
    x
  }

  def int32Impl(x: Long): Int =
    // https://github.com/ClickHouse/ClickHouse/blob/v23.5.3.24-stable/src/Functions/FunctionsHashing.h#L133
    intHash32(x, 0x75d9543de018bf45L)

  def intHash32(l: Long, salt: Long): Int = {
    // https://github.com/ClickHouse/ClickHouse/blob/v23.5.3.24-stable/src/Common/HashTable/Hash.h#L502
    var x = l

    x ^= salt;
    x = (~x) + (x << 18)
    x = x ^ ((x >>> 31) | (x << 33))
    x = x * 21
    x = x ^ ((x >>> 11) | (x << 53))
    x = x + (x << 6)
    x = x ^ ((x >>> 22) | (x << 42))
    x.toInt
  }

  def toUInt32Range(v: Long): Long = if (v < 0) v + (1L << 32) else v
}
