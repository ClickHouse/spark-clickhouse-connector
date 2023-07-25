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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, ScalarFunction, UnboundFunction}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

abstract class MultiStringArgsHash extends UnboundFunction with ClickhouseEquivFunction {

  def applyHash(input: Array[Any]): Long

  protected def funcName: String

  override val ckFuncNames: Array[String]

  override def description: String = s"$name: (value: string, ...) => hash_value: long"

  private def isExceptedType(dt: DataType): Boolean =
    dt.isInstanceOf[StringType]

  final override def name: String = funcName

  final override def bind(inputType: StructType): BoundFunction = {
    val inputDataTypes = inputType.fields.map(_.dataType)
    if (inputDataTypes.forall(isExceptedType)) {
      // need to new a ScalarFunction instance for each bind,
      // because we do not know the number of arguments in advance
      new ScalarFunction[Long] {
        override def inputTypes(): Array[DataType] = inputDataTypes
        override def name: String = funcName
        override def canonicalName: String = s"clickhouse.$name"
        override def resultType: DataType = LongType
        override def toString: String = name
        override def produceResult(input: InternalRow): Long = {
          val inputStrings: Array[Any] =
            input.toSeq(Seq.fill(input.numFields)(StringType)).asInstanceOf[Seq[UTF8String]].toArray
              .map(_.getBytes)
          applyHash(inputStrings)
        }
      }
    } else throw new UnsupportedOperationException(s"Expect multiple STRING argument. $description")

  }

}
