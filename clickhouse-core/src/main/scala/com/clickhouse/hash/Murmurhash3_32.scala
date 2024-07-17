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

package com.clickhouse.hash

import org.apache.commons.codec.digest.MurmurHash3

// https://github.com/ClickHouse/ClickHouse/blob/v23.5.3.24-stable/src/Functions/FunctionsHashing.h#L519
object Murmurhash3_32 extends HashFunc[Int] {
  override def applyHash(input: Array[Byte]): Int =
    MurmurHash3.hash32x86(input, 0, input.length, 0)

  override def combineHashes(h1: Int, h2: Int): Int =
    HashUtils.int32Impl(HashUtils.toUInt32(h1)) ^ h2
}
