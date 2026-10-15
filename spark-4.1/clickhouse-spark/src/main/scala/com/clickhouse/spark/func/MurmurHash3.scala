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

package com.clickhouse.spark.func

import com.clickhouse.spark.hash.{HashUtils, Murmurhash3_32, Murmurhash3_64}
import com.clickhouse.spark.hash

// https://github.com/ClickHouse/ClickHouse/blob/v23.5.3.24-stable/src/Functions/FunctionsHashing.h#L543
object MurmurHash3_64 extends MultiStringArgsHash {

  override protected def funcName: String = "clickhouse_murmurHash3_64"

  override val ckFuncNames: Array[String] = Array("murmurHash3_64")

  override def applyHash(input: Array[Any]): Long = Murmurhash3_64(input)
}

// https://github.com/ClickHouse/ClickHouse/blob/v23.5.3.24-stable/src/Functions/FunctionsHashing.h#L519
object MurmurHash3_32 extends MultiStringArgsHash {

  override protected def funcName: String = "clickhouse_murmurHash3_32"

  override val ckFuncNames: Array[String] = Array("murmurHash3_32")

  override def applyHash(input: Array[Any]): Long = HashUtils.toUInt32(Murmurhash3_32(input))
}
