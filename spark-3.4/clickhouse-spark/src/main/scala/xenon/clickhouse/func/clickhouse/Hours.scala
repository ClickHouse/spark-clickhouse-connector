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

import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, ScalarFunction, UnboundFunction}
import org.apache.spark.sql.types._
import xenon.clickhouse.func.ClickhouseEquivFunction

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

object Hours extends UnboundFunction with ScalarFunction[Int] with ClickhouseEquivFunction {

  override def name: String = "clickhouse_hours"

  override def canonicalName: String = s"clickhouse.$name"

  override def toString: String = name

  override val ckFuncNames: Array[String] = Array("toHour", "HOUR")

  override def description: String = s"$name: (time: timestamp) => shard_num: int"

  override def bind(inputType: StructType): BoundFunction = inputType.fields match {
    case Array(StructField(_, TimestampType, _, _)) | Array(StructField(_, TimestampNTZType, _, _)) => this
    case _ => throw new UnsupportedOperationException(s"Expect 1 TIMESTAMP argument. $description")
  }

  override def inputTypes: Array[DataType] = Array(TimestampType)

  override def resultType: DataType = IntegerType

  override def isResultNullable: Boolean = false

  def invoke(time: Long): Int = {
    val ts = new Timestamp(time / 1000)
    val formatter: SimpleDateFormat = new SimpleDateFormat("hh")
    formatter.format(ts).toInt
  }
}
