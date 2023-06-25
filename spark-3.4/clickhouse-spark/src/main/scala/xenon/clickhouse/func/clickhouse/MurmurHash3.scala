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

package xenon.clickhouse.func.clickhouse

import org.apache.commons.codec.digest.MurmurHash3
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, ScalarFunction, UnboundFunction}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import xenon.clickhouse.func.{ClickhouseEquivFunction, MultiArgsHash, Util}

object MurmurHash3_64 extends MultiArgsHash {
  // https://github.com/ClickHouse/ClickHouse/blob/v23.5.3.24-stable/src/Functions/FunctionsHashing.h#L543

  override protected def funcName: String = "clickhouse_murmurHash3_64"
  override val ckFuncNames: Array[String] = Array("murmurHash3_64")

  override def invokeBase(value: UTF8String): Long = {
    // ignore UInt64 vs Int64
    val data = value.getBytes
    val hashes = MurmurHash3.hash128x64(data, 0, data.length, 0)
    hashes(0) ^ hashes(1)
  }

  override def combineHashes(v1: Long, v2: Long): Long = Util.intHash64Impl(v1) ^ v2
}

object MurmurHash3_32 extends MultiArgsHash {
  // https://github.com/ClickHouse/ClickHouse/blob/v23.5.3.24-stable/src/Functions/FunctionsHashing.h#L519

  override protected def funcName: String = "clickhouse_murmurHash3_32"
  override val ckFuncNames: Array[String] = Array("murmurHash3_32")

  override def invokeBase(value: UTF8String): Long = {
    val data = value.getBytes
    val v = MurmurHash3.hash32x86(data, 0, data.length, 0)
    Util.toUInt32Range(v)
  }

  override def combineHashes(v1: Long, v2: Long): Long = Util.toUInt32Range(Util.int32Impl(v1) ^ v2)
}
