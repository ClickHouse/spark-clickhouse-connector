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

import io.netty.buffer.{ByteBuf, Unpooled}
import org.apache.spark.unsafe.types.UTF8String
import xenon.clickhouse.func.MultiArgsHash
import xenon.clickhouse.func.clickhouse.cityhash.{CityHash_v1_0_2, UInt128}

object CityHash64 extends MultiArgsHash {
  // https://github.com/ClickHouse/ClickHouse/blob/v23.5.3.24-stable/src/Functions/FunctionsHashing.h#L694

  override protected def funcName: String = "clickhouse_cityHash64"
  override val ckFuncNames: Array[String] = Array("cityHash64")

  def convertToByteBuf(array: Array[Byte]): ByteBuf = {
    val byteBuf = Unpooled.buffer(array.length).writeBytes(array)
    byteBuf
  }

  override def invokeBase(value: UTF8String): Long = {
    // ignore UInt64 vs Int64
    val data = value.getBytes
    CityHash_v1_0_2.CityHash64(convertToByteBuf(data), 0, data.length)
  }

  override def combineHashes(v1: Long, v2: Long): Long = CityHash_v1_0_2.Hash128to64(new UInt128(v1, v2))
}
