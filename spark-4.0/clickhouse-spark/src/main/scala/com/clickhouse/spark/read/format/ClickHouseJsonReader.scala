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

package com.clickhouse.spark.read.format

import com.clickhouse.spark.exception.CHClientException
import com.clickhouse.spark.format.{JSONCompactEachRowWithNamesAndTypesStreamOutput, StreamOutput}
import com.clickhouse.spark.read.{ClickHouseInputPartition, ClickHouseReader, ScanJobDescription}
import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.types.variant.VariantBuilder
import org.apache.spark.unsafe.types.UTF8String
import com.clickhouse.spark.Utils.{dateFmt, dateTimeFmt}

import java.math.{MathContext, RoundingMode => RM}
import java.time.{LocalDate, ZoneOffset, ZonedDateTime}
import java.util.concurrent.TimeUnit
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
    if (readSchema.nonEmpty) {
      var i: Int = 0
      while (i < record.length) {
        values(i) = decodeValue(record(i), readSchema.fields(i))
        i = i + 1
      }
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
        Decimal(jsonNode.decimalValue, d.precision, d.scale)
      case d: DecimalType if jsonNode.isFloat | jsonNode.isDouble =>
        Decimal(BigDecimal(jsonNode.doubleValue, new MathContext(d.precision)), d.precision, d.scale)
      case d: DecimalType if jsonNode.isInt =>
        Decimal(BigDecimal(jsonNode.intValue, new MathContext(d.precision)), d.precision, d.scale)
      case d: DecimalType if jsonNode.isLong =>
        Decimal(BigDecimal(jsonNode.longValue, new MathContext(d.precision)), d.precision, d.scale)
      case d: DecimalType if jsonNode.isBigInteger =>
        Decimal(BigDecimal(jsonNode.bigIntegerValue, new MathContext(d.precision)), d.precision, d.scale)
      case d: DecimalType =>
        Decimal(BigDecimal(jsonNode.textValue, new MathContext(d.precision)), d.precision, d.scale)
      case TimestampType =>
        var _instant =
          ZonedDateTime.parse(jsonNode.asText, dateTimeFmt.withZone(scanJob.tz)).withZoneSameInstant(ZoneOffset.UTC)
        TimeUnit.SECONDS.toMicros(_instant.toEpochSecond) + TimeUnit.NANOSECONDS.toMicros(_instant.getNano())
      case StringType =>
        if (jsonNode.isObject || jsonNode.isArray) {
          UTF8String.fromString(jsonNode.toString)
        } else {
          UTF8String.fromString(jsonNode.asText)
        }
      case VariantType =>
        val variant = VariantBuilder.parseJson(jsonNode.toString, false)
        new org.apache.spark.unsafe.types.VariantVal(variant.getValue, variant.getMetadata)
      case DateType => LocalDate.parse(jsonNode.asText, dateFmt).toEpochDay.toInt
      case BinaryType if jsonNode.isTextual =>
        // ClickHouse JSON format returns FixedString as plain text, not Base64
        jsonNode.asText.getBytes("UTF-8")
      case BinaryType =>
        // True binary data is Base64 encoded in JSON format
        jsonNode.binaryValue
      case ArrayType(_dataType, _nullable) =>
        val _structField = StructField(s"${structField.name}__array_element__", _dataType, _nullable)
        new GenericArrayData(jsonNode.asScala.map(decodeValue(_, _structField)).toArray)
      case MapType(StringType, _valueType, _valueNullable) =>
        val mapData = jsonNode.fields.asScala.map { entry =>
          val _structField = StructField(s"${structField.name}__map_value__", _valueType, _valueNullable)
          UTF8String.fromString(entry.getKey) -> decodeValue(entry.getValue, _structField)
        }.toMap
        ArrayBasedMapData(mapData)
      case struct: StructType =>
        // ClickHouse represents tuples in JSON format based on whether they are named or unnamed:
        // - Array format: [value1, value2, ...] - for unnamed tuples
        // - Object format: {"field1": value1, "field2": value2} - for named tuples
        val fieldValues = if (jsonNode.isArray) {
          if (jsonNode.size() != struct.fields.length) {
            throw CHClientException(
              s"Tuple length mismatch: expected ${struct.fields.length} fields " +
                s"but got ${jsonNode.size()} values for struct ${struct.simpleString}"
            )
          }
          struct.fields.zipWithIndex.map { case (field, idx) =>
            decodeValue(jsonNode.get(idx), field)
          }
        } else if (jsonNode.isObject) {
          struct.fields.map { field =>
            val fieldNode = jsonNode.get(field.name)
            if (fieldNode != null && !fieldNode.isNull) {
              decodeValue(fieldNode, field)
            } else {
              null
            }
          }
        } else {
          throw CHClientException(s"Expected array or object for tuple, got: ${jsonNode.getNodeType}")
        }
        new GenericInternalRow(fieldValues)
      case _ =>
        throw CHClientException(s"Unsupported catalyst type ${structField.name}[${structField.dataType}]")
    }
  }
}
