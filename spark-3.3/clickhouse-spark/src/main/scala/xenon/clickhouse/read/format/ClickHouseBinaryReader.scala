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

package xenon.clickhouse.read.format

import com.clickhouse.client.{ClickHouseRecord, ClickHouseValue}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import xenon.clickhouse.exception.CHClientException
import xenon.clickhouse.read.{ClickHouseInputPartition, ClickHouseReader, ScanJobDescription}

import scala.collection.JavaConverters._

class ClickHouseBinaryReader(
  scanJob: ScanJobDescription,
  part: ClickHouseInputPartition
) extends ClickHouseReader[ClickHouseRecord](scanJob, part) {

  override val format: String = "RowBinaryWithNamesAndTypes"

  lazy val streamOutput: Iterator[ClickHouseRecord] = resp.records().asScala.iterator

  override def decode(record: ClickHouseRecord): InternalRow = {
    val values: Array[Any] = new Array[Any](record.size)
    var i: Int = 0
    while (i < record.size) {
      values(i) = decodeValue(record.getValue(i), readSchema.fields(i))
      i = i + 1
    }
    new GenericInternalRow(values)
  }

  private def decodeValue(value: ClickHouseValue, structField: StructField): Any = {
    if (value == null || value.isNullOrEmpty && value.isNullable) {
      // should we check `structField.nullable`?
      return null
    }

    structField.dataType match {
      case BooleanType => value.asBoolean
      case ByteType => value.asByte
      case ShortType => value.asShort
      case IntegerType => value.asInteger
      case LongType => value.asLong
      case FloatType => value.asFloat
      case DoubleType => value.asDouble
      case d: DecimalType => Decimal(value.asBigDecimal(d.scale))
      case TimestampType => value.asZonedDateTime.toEpochSecond * 1000 * 1000 // TODO consider scanJob.tz
      case StringType => UTF8String.fromString(value.asString)
      case DateType => value.asDate.toEpochDay.toInt
      case BinaryType => value.asBinary
      case ArrayType(_dataType, _nullable) =>
        // TODO https://github.com/ClickHouse/clickhouse-jdbc/issues/1088
        new GenericArrayData(value.asArray())
      case MapType(StringType, _valueType, _valueNullable) =>
        // TODO https://github.com/ClickHouse/clickhouse-jdbc/issues/1088
        ArrayBasedMapData(value.asMap.asScala)
      case _ =>
        throw CHClientException(s"Unsupported catalyst type ${structField.name}[${structField.dataType}]")
    }
  }
}
