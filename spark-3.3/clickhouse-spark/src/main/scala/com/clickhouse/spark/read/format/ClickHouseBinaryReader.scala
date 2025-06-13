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

import java.util.Collections
import com.clickhouse.data.value.{
  ClickHouseArrayValue,
  ClickHouseBoolValue,
  ClickHouseDoubleValue,
  ClickHouseFloatValue,
  ClickHouseIntegerValue,
  ClickHouseLongValue,
  ClickHouseMapValue,
  ClickHouseStringValue
}
import com.clickhouse.data.{ClickHouseArraySequence, ClickHouseRecord, ClickHouseValue}
import com.clickhouse.spark.exception.CHClientException
import com.clickhouse.spark.read.{ClickHouseInputPartition, ClickHouseReader, ScanJobDescription}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.time.ZoneOffset
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._

class ClickHouseBinaryReader(
  scanJob: ScanJobDescription,
  part: ClickHouseInputPartition
) extends ClickHouseReader[ClickHouseRecord](scanJob, part) {

  override val format: String = "RowBinaryWithNamesAndTypes"

  lazy val streamOutput: Iterator[ClickHouseRecord] = resp.records().asScala.iterator

  override def decode(record: ClickHouseRecord): InternalRow = {
    val values: Array[Any] = new Array[Any](record.size)
    if (readSchema.nonEmpty) {
      var i: Int = 0
      while (i < record.size) {
        values(i) = decodeValue(record.getValue(i), readSchema.fields(i))
        i = i + 1
      }
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
      case TimestampType =>
        var _instant = value.asZonedDateTime.withZoneSameInstant(ZoneOffset.UTC)
        TimeUnit.SECONDS.toMicros(_instant.toEpochSecond) + TimeUnit.NANOSECONDS.toMicros(_instant.getNano())
      case StringType if value.isInstanceOf[ClickHouseStringValue] => UTF8String.fromBytes(value.asBinary)
      case StringType => UTF8String.fromString(value.asString)
      case DateType => value.asDate.toEpochDay.toInt
      case BinaryType => value.asBinary
      case ArrayType(_dataType, _nullable) =>
        val arrayValue = value.asInstanceOf[ClickHouseArraySequence]
        val convertedArray = Array.tabulate(arrayValue.length) { i =>
          decodeValue(
            arrayValue.getValue(i, createClickHouseValue(null, _dataType)),
            StructField("element", _dataType, _nullable)
          )
        }
        new GenericArrayData(convertedArray)
      case MapType(_keyType, _valueType, _valueNullable) =>
        val convertedMap = value.asMap().asScala.map { case (rawKey, rawValue) =>
          val decodedKey = decodeValue(createClickHouseValue(rawKey, _keyType), StructField("key", _keyType, false))
          val decodedValue =
            decodeValue(createClickHouseValue(rawValue, _valueType), StructField("value", _valueType, _valueNullable))
          (decodedKey, decodedValue)
        }
        ArrayBasedMapData(convertedMap)

      case _ =>
        throw CHClientException(s"Unsupported catalyst type ${structField.name}[${structField.dataType}]")
    }
  }

  private def createClickHouseValue(rawValue: Any, dataType: DataType): ClickHouseValue = {
    val isNull = rawValue == null

    dataType match {
      case StringType =>
        if (isNull) ClickHouseStringValue.ofNull()
        else ClickHouseStringValue.of(rawValue.toString)

      case IntegerType =>
        if (isNull) ClickHouseIntegerValue.ofNull()
        else ClickHouseIntegerValue.of(rawValue.asInstanceOf[Int])

      case LongType =>
        if (isNull) ClickHouseLongValue.ofNull()
        else ClickHouseLongValue.of(rawValue.asInstanceOf[Long])

      case DoubleType =>
        if (isNull) ClickHouseDoubleValue.ofNull()
        else ClickHouseDoubleValue.of(rawValue.asInstanceOf[Double])

      case FloatType =>
        if (isNull) ClickHouseFloatValue.ofNull()
        else ClickHouseFloatValue.of(rawValue.asInstanceOf[Float])

      case BooleanType =>
        if (isNull) ClickHouseBoolValue.ofNull()
        else ClickHouseBoolValue.of(rawValue.asInstanceOf[Boolean])

      case _: ArrayType =>
        if (isNull) ClickHouseArrayValue.ofEmpty()
        else ClickHouseArrayValue.of(rawValue.asInstanceOf[Array[Object]])

      case _: MapType =>
        if (isNull) ClickHouseMapValue.ofEmpty(classOf[Object], classOf[Object])
        else ClickHouseMapValue.of(
          rawValue.asInstanceOf[java.util.Map[Object, Object]],
          classOf[Object],
          classOf[Object]
        )

      case _ =>
        if (isNull) ClickHouseStringValue.ofNull()
        else ClickHouseStringValue.of(rawValue.toString)
    }
  }

}
