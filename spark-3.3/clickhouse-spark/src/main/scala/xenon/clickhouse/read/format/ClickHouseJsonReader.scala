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

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import xenon.clickhouse.Utils.{dateFmt, dateTimeFmt}
import xenon.clickhouse.exception.CHClientException
import xenon.clickhouse.format.{JSONCompactEachRowWithNamesAndTypesStreamOutput, StreamOutput}
import xenon.clickhouse.read.{ClickHouseInputPartition, ClickHouseReader, ScanJobDescription}

import java.math.{MathContext, RoundingMode => RM}
import java.time.{LocalDate, ZoneOffset, ZonedDateTime}
import scala.collection.JavaConverters._
import scala.math.BigDecimal.RoundingMode

class ClickHouseJsonReader(
  scanJob: ScanJobDescription,
  part: ClickHouseInputPartition
) extends ClickHouseReader[Array[JsonNode]](scanJob, part) {

  override val format: String = "JSONCompactEachRowWithNamesAndTypes"

  lazy val streamOutput: StreamOutput[Array[JsonNode]] =
    JSONCompactEachRowWithNamesAndTypesStreamOutput.deserializeStream(resp.getInputStream)

  override def decode(record: Array[JsonNode]): InternalRow = {
    val values: Array[Any] = new Array[Any](record.length)
    var i: Int = 0
    while (i < record.length) {
      values(i) = decodeValue(record(i), readSchema.fields(i))
      i = i + 1
    }
    new GenericInternalRow(values)
  }

  private def decodeValue(jsonNode: JsonNode, structField: StructField): Any = {
    if (jsonNode == null || jsonNode.isNull) {
      // should we check `structField.nullable`?
      return null
    }

    structField.dataType match {
      case BooleanType => jsonNode.asBoolean
      case ByteType => jsonNode.asInt.byteValue
      case ShortType => jsonNode.asInt.shortValue
      case IntegerType => jsonNode.asInt
      case LongType => jsonNode.asLong
      case FloatType => jsonNode.asDouble.floatValue
      case DoubleType => jsonNode.asDouble
      case d: DecimalType if jsonNode.isBigDecimal =>
        Decimal(jsonNode.decimalValue.setScale(d.scale, RoundingMode.HALF_UP))
      case d: DecimalType =>
        Decimal(BigDecimal(jsonNode.asText, new MathContext(d.scale, RM.HALF_UP)))
      case TimestampType =>
        ZonedDateTime.parse(jsonNode.asText, dateTimeFmt.withZone(scanJob.tz))
          .withZoneSameInstant(ZoneOffset.UTC)
          .toEpochSecond * 1000 * 1000
      case StringType => UTF8String.fromString(jsonNode.asText)
      case DateType => LocalDate.parse(jsonNode.asText, dateFmt).toEpochDay.toInt
      case BinaryType => jsonNode.binaryValue
      case ArrayType(_dataType, _nullable) =>
        val _structField = StructField(s"${structField.name}__array_element__", _dataType, _nullable)
        new GenericArrayData(jsonNode.asScala.map(decodeValue(_, _structField)))
      case MapType(StringType, _valueType, _valueNullable) =>
        val mapData = jsonNode.fields.asScala.map { entry =>
          val _structField = StructField(s"${structField.name}__map_value__", _valueType, _valueNullable)
          UTF8String.fromString(entry.getKey) -> decodeValue(entry.getValue, _structField)
        }.toMap
        ArrayBasedMapData(mapData)
      case _ =>
        throw CHClientException(s"Unsupported catalyst type ${structField.name}[${structField.dataType}]")
    }
  }
}
