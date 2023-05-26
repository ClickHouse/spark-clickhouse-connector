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

import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, ScalarFunction, UnboundFunction}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

abstract class MultiArgsHash extends UnboundFunction with ClickhouseEquivFunction {
  trait Base extends ScalarFunction[Long] {
    // must not be private object, nor do it successors, because spark would compile them
    override def canonicalName: String = s"clickhouse.$name"
    override def resultType: DataType = LongType
    override def toString: String = name
  }

  object Arg1 extends Base {
    override def name: String = s"${funcName}_1"
    override def inputTypes: Array[DataType] = Array.fill(1)(StringType)
    def invoke(value: UTF8String): Long = invokeBase(value)
  }

  object Arg2 extends Base {
    override def name: String = s"${funcName}_2"
    override def inputTypes: Array[DataType] = Array.fill(2)(StringType)
    def invoke(v1: UTF8String, v2: UTF8String): Long = Seq(v1, v2).map(invokeBase).reduce(combineHashes)
  }

  object Arg3 extends Base {
    override def name: String = s"${funcName}_3"
    override def inputTypes: Array[DataType] = Array.fill(3)(StringType)
    def invoke(v1: UTF8String, v2: UTF8String, v3: UTF8String): Long =
      Seq(v1, v2, v3).map(invokeBase).reduce(combineHashes)
  }

  object Arg4 extends Base {
    override def name: String = s"${funcName}_4"
    override def inputTypes: Array[DataType] = Array.fill(4)(StringType)
    def invoke(v1: UTF8String, v2: UTF8String, v3: UTF8String, v4: UTF8String): Long =
      Seq(v1, v2, v3, v4).map(invokeBase).reduce(combineHashes)
  }

  object Arg5 extends Base {
    override def name: String = s"${funcName}_4"
    override def inputTypes: Array[DataType] = Array.fill(5)(StringType)
    def invoke(v1: UTF8String, v2: UTF8String, v3: UTF8String, v4: UTF8String, v5: UTF8String): Long =
      Seq(v1, v2, v3, v4, v5).map(invokeBase).reduce(combineHashes)
  }
  private def isExceptedType(dt: DataType): Boolean =
    dt.isInstanceOf[StringType]

  final override def name: String = funcName
  final override def bind(inputType: StructType): BoundFunction = inputType.fields match {
    case Array(StructField(_, dt, _, _)) if List(dt).forall(isExceptedType) => this.Arg1
    case Array(
          StructField(_, dt1, _, _),
          StructField(_, dt2, _, _)
        ) if List(dt1, dt2).forall(isExceptedType) =>
      this.Arg2
    case Array(
          StructField(_, dt1, _, _),
          StructField(_, dt2, _, _),
          StructField(_, dt3, _, _)
        ) if List(dt1, dt2, dt3).forall(isExceptedType) =>
      this.Arg3
    case Array(
          StructField(_, dt1, _, _),
          StructField(_, dt2, _, _),
          StructField(_, dt3, _, _),
          StructField(_, dt4, _, _)
        ) if List(dt1, dt2, dt3, dt4).forall(isExceptedType) =>
      this.Arg4
    case Array(
          StructField(_, dt1, _, _),
          StructField(_, dt2, _, _),
          StructField(_, dt3, _, _),
          StructField(_, dt4, _, _),
          StructField(_, dt5, _, _)
        ) if List(dt1, dt2, dt3, dt4, dt5).forall(isExceptedType) =>
      this.Arg5
    case _ => throw new UnsupportedOperationException(s"Expect up to 5 STRING argument. $description")
  }

  protected def funcName: String
  override val ckFuncNames: Array[String]
  override def description: String = s"$name: (value: string, ...) => hash_value: long"
  def invokeBase(value: UTF8String): Long
  def combineHashes(v1: Long, v2: Long): Long
}
