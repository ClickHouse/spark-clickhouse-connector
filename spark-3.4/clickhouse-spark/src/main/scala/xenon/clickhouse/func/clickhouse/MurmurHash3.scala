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
import xenon.clickhouse.func.ClickhouseEquivFunction

object MurmurHash3_64 extends UnboundFunction with ScalarFunction[Long] with ClickhouseEquivFunction {

  override def name: String = "clickhouse_murmurHash3_64"

  override def canonicalName: String = s"clickhouse.$name"

  override val ckFuncNames: Array[String] = Array("murmurHash3_64")

  override def description: String = s"$name: (value: string) => hash_value: long"

  override def bind(inputType: StructType): BoundFunction = inputType.fields match {
    case Array(StructField(_, StringType, _, _)) => this
    case _ => throw new UnsupportedOperationException(s"Expect 1 STRING argument. $description")
  }

  override def inputTypes: Array[DataType] = Array(StringType)

  override def resultType: DataType = LongType

  override def isResultNullable: Boolean = false

  def invoke(values: UTF8String): Long = {
    // ignore UInt64 vs Int64
    val data = values.getBytes
    val hashes = MurmurHash3.hash128x64(data, 0, data.length, 0)
    hashes(0) ^ hashes(1)
  }
}

object MurmurHash3_32 extends UnboundFunction with ScalarFunction[Long] with ClickhouseEquivFunction {

  override def name: String = "clickhouse_murmurHash3_32"

  override def canonicalName: String = s"clickhouse.$name"

  override val ckFuncNames: Array[String] = Array("murmurHash3_32")

  override def description: String = s"$name: (value: string) => hash_value: long"

  override def bind(inputType: StructType): BoundFunction = inputType.fields match {
    case Array(StructField(_, StringType, _, _)) => this
    case _ => throw new UnsupportedOperationException(s"Expect 1 STRING argument. $description")
  }

  override def inputTypes: Array[DataType] = Array(StringType)

  override def resultType: DataType = LongType

  override def isResultNullable: Boolean = false

  def invoke(values: UTF8String): Long = {
    val data = values.getBytes
    val v = MurmurHash3.hash32x86(data, 0, data.length, 0).toLong
    if (v < 0) v + (1L << 32) else v
  }
}
