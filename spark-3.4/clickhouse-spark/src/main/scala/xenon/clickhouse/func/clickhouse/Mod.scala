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

object Mod extends UnboundFunction with ScalarFunction[Long] with ClickhouseEquivFunction {

  override def name: String = "sharding_mod"

  override def canonicalName: String = s"clickhouse.$name"

  override def toString: String = name

  // remainder is not a Clickhouse function, but modulo will be parsed to remainder in the connector.
  // Added remainder as a synonym.
  override val ckFuncNames: Array[String] = Array("modulo", "remainder")

  override def description: String = s"$name: (a: long, b: long) => mod: long"

  override def bind(inputType: StructType): BoundFunction = inputType.fields match {
    case Array(a, b) if
          (a match {
            case StructField(_, LongType, _, _) => true
            case StructField(_, IntegerType, _, _) => true
            case StructField(_, ShortType, _, _) => true
            case StructField(_, ByteType, _, _) => true
            case StructField(_, StringType, _, _) => true
            case _ => false
          }) &&
            (b match {
              case StructField(_, LongType, _, _) => true
              case StructField(_, IntegerType, _, _) => true
              case StructField(_, ShortType, _, _) => true
              case StructField(_, ByteType, _, _) => true
              case StructField(_, StringType, _, _) => true
              case _ => false
            }) =>
      this
    case _ => throw new UnsupportedOperationException(s"Expect 2 integer arguments. $description")
  }

  override def inputTypes: Array[DataType] = Array(LongType, LongType)

  override def resultType: DataType = LongType

  override def isResultNullable: Boolean = false

  def invoke(a: Long, b: Long): Long = a % b
}
