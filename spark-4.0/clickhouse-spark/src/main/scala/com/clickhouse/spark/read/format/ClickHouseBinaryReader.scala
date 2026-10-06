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
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf.READ_JSON_AS
import org.apache.spark.sql.types._
import org.apache.spark.types.variant.VariantBuilder
import org.apache.spark.unsafe.types.UTF8String

import java.io.InputStream
import java.time.{LocalDate, ZonedDateTime}
import java.util

object ClickHouseBinaryReader extends SQLConfHelper {
  private val jsonMapper = new ObjectMapper()
  private val jsonFactory = new com.fasterxml.jackson.core.JsonFactory()

  private def buildVariantFromValue(value: Any): org.apache.spark.unsafe.types.VariantVal = {
    if (value == null) {
      return null
    }
    val jsonString = jsonMapper.writeValueAsString(value)
    val parser = jsonFactory.createParser(jsonString)
    parser.nextToken()
    val variant = VariantBuilder.parseJson(parser, false)
    new org.apache.spark.unsafe.types.VariantVal(variant.getValue, variant.getMetadata)
  }

  private def serializeToJsonString(value: Any): String =
    jsonMapper.writeValueAsString(value)
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

  private val converters: Array[AnyRef => Any] = readSchema.fields.map(f => converter(f.name, f.dataType))

  // Reused for every row: Spark copies each row into an UnsafeRow before calling next()
  private val values: Array[Any] = new Array[Any](converters.length)
  private val row: InternalRow = new GenericInternalRow(values)

  override def decode(record: GenericRecord): InternalRow = {
    var i: Int = 0
    while (i < converters.length) {
      values(i) = convertValue(record.getObject(i + 1), converters(i))
      i = i + 1
    }
    row
  }

  // should we check the field's nullability?
  private def convertValue(value: AnyRef, convert: AnyRef => Any): Any =
    if (value == null) null else convert(value)

  private def converter(name: String, dataType: DataType): AnyRef => Any = dataType match {
    case BooleanType => _.asInstanceOf[java.lang.Boolean]
    case ByteType => _.asInstanceOf[java.lang.Byte]
    case ShortType => _.asInstanceOf[java.lang.Short]
    case IntegerType => {
      case v: java.lang.Long => v.intValue
      case v => v.asInstanceOf[Integer]
    }
    case LongType => {
      case v: java.math.BigInteger => v.longValue
      case v => v.asInstanceOf[java.lang.Long]
    }
    case FloatType => _.asInstanceOf[java.lang.Float]
    case DoubleType => _.asInstanceOf[java.lang.Double]
    case d: DecimalType if d.precision <= Decimal.MAX_LONG_DIGITS => {
      case bd: java.math.BigDecimal if bd.scale == d.scale && bd.precision <= d.precision =>
        // scaleByPowerOfTen avoids the BigInteger that unscaledValue allocates
        Decimal.createUnsafe(bd.scaleByPowerOfTen(bd.scale).longValueExact, d.precision, d.scale)
      case v => toDecimal(v, d)
    }
    case d: DecimalType => toDecimal(_, d)
    case TimestampType => value =>
        // epoch second and nano do not depend on the zone
        val dateTime = value.asInstanceOf[ZonedDateTime]
        TimeUnit.SECONDS.toMicros(dateTime.toEpochSecond) + TimeUnit.NANOSECONDS.toMicros(dateTime.getNano)
    case StringType => value =>
        val strValue = value match {
          case _: util.Map[_, _] | _: util.List[_] => ClickHouseBinaryReader.serializeToJsonString(value)
          case uuid: java.util.UUID => uuid.toString
          case inet: java.net.InetAddress => inet.getHostAddress
          case s: String => s
          case enumValue: BinaryStreamReader.EnumValue => enumValue.toString
          case _ => value.toString
        }
        UTF8String.fromString(strValue)
    case VariantType => ClickHouseBinaryReader.buildVariantFromValue(_)
    case DateType => value =>
        val localDate = value match {
          case ld: LocalDate => ld
          case zdt: ZonedDateTime => zdt.toLocalDate
          case _ => value.asInstanceOf[LocalDate]
        }
        localDate.toEpochDay.toInt
    case BinaryType => _.asInstanceOf[String].getBytes
    case ArrayType(elementType, _) =>
      val convertElement = converter("element", elementType)
      value =>
        // Java client returns BinaryStreamReader.ArrayValue for arrays
        val elements = value.asInstanceOf[BinaryStreamReader.ArrayValue].getArrayOfObjects()
        new GenericArrayData(elements.map(convertValue(_, convertElement)))
    case MapType(keyType, valueType, _) =>
      val convertKey = converter("key", keyType)
      val convertMapValue = converter("value", valueType)
      value =>
        // Java client returns util.Map (LinkedHashMap or EmptyMap)
        val javaMap = value.asInstanceOf[util.Map[AnyRef, AnyRef]]
        val convertedMap = javaMap.asScala.map { case (rawKey, rawValue) =>
          (convertValue(rawKey, convertKey), convertValue(rawValue, convertMapValue))
        }
        ArrayBasedMapData(convertedMap)
    case struct: StructType =>
      val fieldConverters = struct.fields.map(field => converter(field.name, field.dataType))
      value =>
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
            Array.tabulate[Any](arr.length)(i => convertValue(arr(i), fieldConverters(i)))
          case list: util.List[_] =>
            if (list.size() != struct.fields.length) {
              throw CHClientException(
                s"Tuple length mismatch: expected ${struct.fields.length} fields " +
                  s"but got ${list.size()} values for struct ${struct.simpleString}"
              )
            }
            Array.tabulate[Any](list.size())(i => convertValue(list.get(i).asInstanceOf[AnyRef], fieldConverters(i)))
          case _ =>
            throw CHClientException(s"Unexpected tuple type: ${value.getClass}, expected Array or List")
        }
        new GenericInternalRow(fieldValues)
    case _ =>
      _ => throw CHClientException(s"Unsupported catalyst type $name[$dataType]")
  }

  private def toDecimal(value: AnyRef, d: DecimalType): Decimal = {
    // Java client returns BigInteger for Int256/UInt256, BigDecimal for Decimal types
    val dec: BigDecimal = value match {
      case bi: java.math.BigInteger => BigDecimal(bi)
      case bd: java.math.BigDecimal => BigDecimal(bd)
    }
    Decimal(dec.setScale(d.scale))
  }
}
