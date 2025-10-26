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

import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader
import com.clickhouse.client.api.data_formats.{ClickHouseBinaryFormatReader, RowBinaryWithNamesAndTypesFormatReader}
import com.clickhouse.client.api.query.{GenericRecord, Records}

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

import java.io.InputStream
import java.time.{LocalDate, ZoneOffset, ZonedDateTime}
import java.util
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._

class ClickHouseBinaryReader(
  scanJob: ScanJobDescription,
  part: ClickHouseInputPartition
) extends ClickHouseReader[GenericRecord](scanJob, part) {

  override val format: String = "RowBinaryWithNamesAndTypes"

  lazy val streamOutput: Iterator[GenericRecord] = {
    val inputString: InputStream = resp.getInputStream
    val cbfr: ClickHouseBinaryFormatReader = new RowBinaryWithNamesAndTypesFormatReader(
      inputString,
      resp.getSettings,
      new BinaryStreamReader.DefaultByteBufferAllocator
    )
    val r = new Records(resp, cbfr)
    r.asScala.iterator
  }

  override def decode(record: GenericRecord): InternalRow = {
    val size = record.getSchema.getColumns.size()
    val values: Array[Any] = new Array[Any](size)
    if (readSchema.nonEmpty) {
      var i: Int = 0
      while (i < size) {
        val v: Object = record.getObject(i + 1)
        values(i) = decodeValue(v, readSchema.fields(i))
        i = i + 1
      }
    }
    new GenericInternalRow(values)
  }

  private def decodeValue(value: Object, structField: StructField): Any = {
    if (value == null) {
      // should we check `structField.nullable`?
      return null
    }

    structField.dataType match {
      case BooleanType => value.asInstanceOf[Boolean]
      case ByteType => value.asInstanceOf[Byte]
      case ShortType => value.asInstanceOf[Short]
//        case IntegerType if value.getClass.toString.equals("class java.lang.Long") =>
      case IntegerType if value.isInstanceOf[java.lang.Long] =>
        val v: Integer = Integer.valueOf(value.asInstanceOf[Long].toInt)
        v.intValue()
      case IntegerType =>
        value.asInstanceOf[Integer].intValue()
      case LongType if value.isInstanceOf[java.math.BigInteger] =>
        value.asInstanceOf[java.math.BigInteger].longValue()
      case LongType =>
        value.asInstanceOf[Long]
      case FloatType => value.asInstanceOf[Float]
      case DoubleType => value.asInstanceOf[Double]
      case d: DecimalType =>
        val dec = value.asInstanceOf[BigDecimal]
        Decimal(dec.setScale(d.scale))
      case TimestampType =>
        var _instant = value.asInstanceOf[ZonedDateTime].withZoneSameInstant(ZoneOffset.UTC)
        TimeUnit.SECONDS.toMicros(_instant.toEpochSecond) + TimeUnit.NANOSECONDS.toMicros(_instant.getNano())
      case StringType => UTF8String.fromString(value.asInstanceOf[String])
      case DateType => value.asInstanceOf[LocalDate].toEpochDay.toInt
      case BinaryType => value.asInstanceOf[String].getBytes
      case ArrayType(_dataType, _nullable) =>
        val arrayValue = value.asInstanceOf[Seq[Object]]
        val convertedArray = Array.tabulate(arrayValue.length) { i =>
          decodeValue(
            arrayValue(i),
            StructField("element", _dataType, _nullable)
          )
        }
        new GenericArrayData(convertedArray)
      case MapType(_keyType, _valueType, _valueNullable) =>
        val convertedMap =
          value.asInstanceOf[util.LinkedHashMap[Object, Object]].asScala.map { case (rawKey, rawValue) =>
            val decodedKey = decodeValue(rawKey, StructField("key", _keyType, false))
            val decodedValue =
              decodeValue(rawValue, StructField("value", _valueType, _valueNullable))
            (decodedKey, decodedValue)
          }
        ArrayBasedMapData(convertedMap)
      case _ =>
        throw CHClientException(s"Unsupported catalyst type ${structField.name}[${structField.dataType}]")
    }
  }

}
