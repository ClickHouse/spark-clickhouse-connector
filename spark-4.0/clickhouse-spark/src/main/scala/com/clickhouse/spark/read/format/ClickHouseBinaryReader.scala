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
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.types.variant.VariantBuilder
import org.apache.spark.unsafe.types.UTF8String

import java.io.InputStream
import java.time.{LocalDate, ZoneOffset, ZonedDateTime}
import java.util

object ClickHouseBinaryReader {
  private val jsonMapper = new ObjectMapper()
  private val jsonFactory = new com.fasterxml.jackson.core.JsonFactory()

  private def buildVariantFromValue(value: Any): org.apache.spark.unsafe.types.VariantVal = {
    val jsonString = jsonMapper.writeValueAsString(value)
    val parser = jsonFactory.createParser(jsonString)
    parser.nextToken()
    val variant = VariantBuilder.parseJson(parser, false)
    new org.apache.spark.unsafe.types.VariantVal(variant.getValue, variant.getMetadata)
  }
}

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
        // Java client returns BigInteger for Int256/UInt256, BigDecimal for Decimal types
        val dec: BigDecimal = value match {
          case bi: java.math.BigInteger => BigDecimal(bi)
          case bd: java.math.BigDecimal => BigDecimal(bd)
        }
        Decimal(dec.setScale(d.scale))
      case TimestampType =>
        var _instant = value.asInstanceOf[ZonedDateTime].withZoneSameInstant(ZoneOffset.UTC)
        TimeUnit.SECONDS.toMicros(_instant.toEpochSecond) + TimeUnit.NANOSECONDS.toMicros(_instant.getNano())
      case StringType =>
        val strValue = value match {
          case uuid: java.util.UUID => uuid.toString
          case inet: java.net.InetAddress => inet.getHostAddress
          case s: String => s
          case enumValue: BinaryStreamReader.EnumValue => enumValue.toString
          case _ => value.toString
        }
        UTF8String.fromString(strValue)
      case VariantType =>
        ClickHouseBinaryReader.buildVariantFromValue(value)
      case DateType =>
        val localDate = value match {
          case ld: LocalDate => ld
          case zdt: ZonedDateTime => zdt.toLocalDate
          case _ => value.asInstanceOf[LocalDate]
        }
        localDate.toEpochDay.toInt
      case BinaryType => value.asInstanceOf[String].getBytes
      case ArrayType(_dataType, _nullable) =>
        // Java client returns BinaryStreamReader.ArrayValue for arrays
        val arrayVal = value.asInstanceOf[BinaryStreamReader.ArrayValue]
        val arrayValue = arrayVal.getArrayOfObjects().toSeq.asInstanceOf[Seq[Object]]
        val convertedArray = Array.tabulate(arrayValue.length) { i =>
          decodeValue(
            arrayValue(i),
            StructField("element", _dataType, _nullable)
          )
        }
        new GenericArrayData(convertedArray)
      case MapType(_keyType, _valueType, _valueNullable) =>
        // Java client returns util.Map (LinkedHashMap or EmptyMap)
        val javaMap = value.asInstanceOf[util.Map[Object, Object]]
        val convertedMap =
          javaMap.asScala.map { case (rawKey, rawValue) =>
            val decodedKey = decodeValue(rawKey, StructField("key", _keyType, false))
            val decodedValue =
              decodeValue(rawValue, StructField("value", _valueType, _valueNullable))
            (decodedKey, decodedValue)
          }
        ArrayBasedMapData(convertedMap)
      case struct: StructType =>
        // ClickHouse Java client can return tuples as either:
        // - Array[Object] for unnamed tuples
        // - util.List[Object] for named tuples
        val fieldValues = value match {
          case arr: Array[Object] =>
            if (arr.length != struct.fields.length) {
              throw CHClientException(
                s"Tuple length mismatch: expected ${struct.fields.length} fields " +
                  s"but got ${arr.length} values for struct ${struct.simpleString}"
              )
            }
            struct.fields.zip(arr).map { case (field, rawValue) =>
              decodeValue(rawValue, field)
            }
          case list: util.List[_] =>
            if (list.size() != struct.fields.length) {
              throw CHClientException(
                s"Tuple length mismatch: expected ${struct.fields.length} fields " +
                  s"but got ${list.size()} values for struct ${struct.simpleString}"
              )
            }
            struct.fields.zipWithIndex.map { case (field, idx) =>
              decodeValue(list.get(idx).asInstanceOf[Object], field)
            }
          case _ =>
            throw CHClientException(s"Unexpected tuple type: ${value.getClass}, expected Array or List")
        }
        new GenericInternalRow(fieldValues)
      case _ =>
        throw CHClientException(s"Unsupported catalyst type ${structField.name}[${structField.dataType}]")
    }
  }

}
