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

import com.clickhouse.client.api.query.QuerySettings
import com.clickhouse.data.{ClickHouseColumn, ClickHouseDataType => CH}
import com.clickhouse.spark.format.{RowBinaryInput, StartOfDayDates}
import com.clickhouse.spark.format.RowBinaryInput.Unreadable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BaseGenericInternalRow, GenericInternalRow, UnsafeArrayData}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.io.InputStream
import java.math.{BigDecimal => JBigDecimal, BigInteger}
import java.net.InetAddress
import java.nio.charset.{Charset, StandardCharsets}
import java.util.{Arrays, TimeZone, UUID}
import scala.collection.JavaConverters._

/** Decodes RowBinaryWithNamesAndTypes into the rows client-v2 gives today, handing over to it where it can't. */
final private[format] class RowBinaryDecoder(stream: InputStream, settings: QuerySettings, schema: StructType) {
  import RowBinaryDecoder.ValueDecoder

  private val input = new RowBinaryInput(stream)
  // client-v2 reads a row ahead, so the row being returned and the one after it are both held
  private val rows = Array.fill(2)(new DecodedRow(schema.fields.map(_.dataType)))
  private var decoders: Array[ValueDecoder] = _
  private var header = Array.emptyByteArray
  private var started = false
  private var current = 0
  private var pending = -1
  private var handOver = false

  /** Once true, client-v2 reads the rest from [[clientInput]]. */
  def handedOver: Boolean = handOver

  /** Row of the last [[next]] that returned true. */
  def row: InternalRow = rows(current)

  /** May hand over while still returning a row, when the row after it can't be read. */
  def next(): Boolean = {
    if (!started) {
      started = true
      if (!open()) {
        handOver = true
        return false
      }
      pending = readRow(0)
    }
    if (pending < 0) return false
    current = pending
    pending = readRow(1 - current)
    true
  }

  /** The header, the row client-v2 continues from, then the rest of the stream. */
  def clientInput: InputStream = input.replay(header)

  private def open(): Boolean = {
    val opened = for {
      columns <- input.readHeader(schema.length) if columns.nonEmpty
      timeZone <- StartOfDayDates.clientTimeZone(settings)
      built <- RowBinaryDecoder.decoders(columns, schema, timeZone)
    } yield built
    opened.foreach { built =>
      decoders = built
      header = input.markedBytes()
    }
    opened.isDefined
  }

  // Returns `target`, or -1 at the end of the data or on hand-over
  private def readRow(target: Int): Int = {
    input.mark()
    if (!input.hasMore) {
      handOver = input.failed
      return -1
    }
    val row = rows(target)
    try {
      var i = 0
      while (i < decoders.length) {
        decoders(i).decodeInto(input, row, i)
        i += 1
      }
      target
    } catch {
      case Unreadable =>
        handOver = true
        -1
    }
  }
}

private[format] object RowBinaryDecoder {

  private[format] def decoders(
    columns: Array[ClickHouseColumn],
    schema: StructType,
    timeZone: TimeZone
  ): Option[Array[ValueDecoder]] = {
    val context = new Context(timeZone)
    val decoders = columns.indices.map(i => decoderFor(columns(i), schema.fields(i).dataType, context))
    if (decoders.forall(_.isDefined)) Some(decoders.map(_.get).toArray) else None
  }

  final private class Context(timeZone: TimeZone) {
    lazy val dates = new StartOfDayDates(timeZone.toZoneId)
    val utf8IsDefault: Boolean = Charset.defaultCharset == StandardCharsets.UTF_8
  }

  // Pairs of header type and the Spark type SchemaUtils maps it to; anything else stays with client-v2
  private def decoderFor(column: ClickHouseColumn, dataType: DataType, context: Context): Option[ValueDecoder] = {
    val decoder: Option[ValueDecoder] = (column.getDataType, dataType) match {
      case (CH.Bool, BooleanType) => Some(BoolDecoder)
      case (CH.Int8, ByteType) => Some(Int8Decoder)
      case (CH.UInt8, ShortType) => Some(UInt8Decoder)
      case (CH.Int16, ShortType) => Some(Int16Decoder)
      case (CH.UInt16, IntegerType) => Some(UInt16Decoder)
      // UInt32 as IntegerType is `_shard_num`, UInt64 as LongType `_part_index`: both keep the raw bits
      case (CH.Int32 | CH.UInt32, IntegerType) => Some(Int32Decoder)
      case (CH.UInt32, LongType) => Some(UInt32Decoder)
      case (CH.Int64 | CH.UInt64, LongType) => Some(Int64Decoder)
      case (CH.UInt64, d: DecimalType) => Some(new BigIntegerDecoder(8, true, d))
      case (CH.Int128, d: DecimalType) => Some(new BigIntegerDecoder(16, false, d))
      case (CH.UInt128, d: DecimalType) => Some(new BigIntegerDecoder(16, true, d))
      case (CH.Int256, d: DecimalType) => Some(new BigIntegerDecoder(32, false, d))
      case (CH.UInt256, d: DecimalType) => Some(new BigIntegerDecoder(32, true, d))
      case (CH.Float32, FloatType) => Some(Float32Decoder)
      case (CH.Float64, DoubleType) => Some(Float64Decoder)
      case (CH.Decimal | CH.Decimal32 | CH.Decimal64 | CH.Decimal128, d: DecimalType)
          if column.getScale == d.scale =>
        decimalDecoder(column, d)
      case (CH.Date, DateType) => Some(new DateDecoder(context.dates))
      case (CH.Date32, DateType) => Some(new Date32Decoder(context.dates))
      case (CH.DateTime | CH.DateTime32, TimestampType) => Some(DateTimeDecoder)
      case (CH.DateTime64, TimestampType) if column.getScale >= 0 && column.getScale <= 9 =>
        Some(new DateTime64Decoder(column.getScale))
      case (CH.String, StringType) => Some(StringDecoder)
      case (CH.FixedString, BinaryType | StringType) if column.getPrecision > 0 =>
        Some(new FixedStringDecoder(column.getPrecision, dataType == BinaryType, context.utf8IsDefault))
      case (CH.UUID, StringType) => Some(UuidDecoder)
      case (CH.IPv4, StringType) => Some(IPv4Decoder)
      case (CH.IPv6, StringType) => Some(IPv6Decoder)
      case (CH.Enum8 | CH.Enum16, StringType) => EnumDecoder(column)
      case (CH.Nothing, NullType) if column.isNullable => Some(NothingDecoder)
      case (CH.Array, ArrayType(elementType, _)) if !itemClassVaries(column) =>
        val element = column.getNestedColumns.get(0)
        // client-v2 reads nested arrays without a null flag
        if (element.getDataType == CH.Array && element.isNullable) None
        else decoderFor(element, elementType, context).map(new ArrayDecoder(_))
      case (CH.Map, MapType(keyType, valueType, _)) =>
        for {
          key <- keyDecoderFor(column.getKeyInfo, keyType, context)
          value <- decoderFor(column.getValueInfo, valueType, context)
        } yield new MapDecoder(key, value)
      case (CH.Tuple, StructType(fields)) if column.getNestedColumns.size == fields.length =>
        val nested = column.getNestedColumns.asScala
        val decoders = nested.indices.map(i => decoderFor(nested(i), fields(i).dataType, context))
        if (decoders.forall(_.isDefined)) Some(new TupleDecoder(decoders.map(_.get).toArray)) else None
      case _ => None
    }
    if (column.isNullable) decoder.map(new NullableDecoder(_)) else decoder
  }

  // client-v2 types an array by its first item, so items of these types fail it when their Java class changes
  private def itemClassVaries(array: ClickHouseColumn): Boolean = {
    val item = array.getArrayBaseColumn
    !item.isNullable && (item.getDataType == CH.Map || item.getDataType == CH.IPv6)
  }

  private def keyDecoderFor(column: ClickHouseColumn, dataType: DataType, context: Context): Option[ValueDecoder] =
    column.getDataType match {
      case _ if column.isNullable => None
      case CH.Array | CH.Map | CH.Tuple => None
      // byte[] keys hash by identity in the Spark-side map
      case CH.FixedString if dataType == BinaryType => None
      case _ => decoderFor(column, dataType, context)
    }

  // client-v2 picks the width by precision: 4 bytes up to 9 digits, 8 up to 18, 16 up to 38
  private def decimalDecoder(column: ClickHouseColumn, d: DecimalType): Option[ValueDecoder] = {
    val precision = column.getDataType match {
      case CH.Decimal => column.getPrecision
      case other => other.getMaxPrecision
    }
    if (precision <= CH.Decimal32.getMaxScale) Some(new DecimalDecoder(4, column.getScale, d))
    else if (precision <= CH.Decimal64.getMaxScale) Some(new DecimalDecoder(8, column.getScale, d))
    else if (precision <= CH.Decimal128.getMaxScale) Some(new Decimal128Decoder(column.getScale, d))
    else None
  }

  // UTF8String.fromString(new String(bytes, UTF_8)), as decoded today, without a String for ASCII
  private def utf8(in: RowBinaryInput, offset: Int, length: Int): UTF8String =
    if (in.isAscii(offset, length)) {
      UTF8String.fromBytes(Arrays.copyOfRange(in.buffer, offset, offset + length))
    } else {
      UTF8String.fromString(new String(in.buffer, offset, length, StandardCharsets.UTF_8))
    }

  // Big-endian copy of client-v2's little-endian bytes
  private def bigInteger(in: RowBinaryInput, length: Int, unsigned: Boolean): BigInteger = {
    val offset = in.take(length)
    val buf = in.buffer
    val bytes = new Array[Byte](length)
    var i = 0
    while (i < length) {
      bytes(i) = buf(offset + length - 1 - i)
      i += 1
    }
    if (unsigned) new BigInteger(1, bytes) else new BigInteger(bytes)
  }

  private def pow10(n: Int): Long = {
    var r = 1L
    var i = 0
    while (i < n) {
      r *= 10
      i += 1
    }
    r
  }

  abstract private[format] class ValueDecoder {

    /** Smallest encoded size of a value, to check a length before allocating for it. */
    def minBytes: Int

    /** Reads a value as a Spark internal value, or null. */
    def decode(in: RowBinaryInput): Any

    def decodeInto(in: RowBinaryInput, row: DecodedRow, ordinal: Int): Unit = row.update(ordinal, decode(in))

    def decodeArray(in: RowBinaryInput, n: Int): ArrayData = boxedArray(this, in, n)

    /** Reads a map key as an object that compares like the key client-v2 builds. */
    def decodeKey(in: RowBinaryInput): AnyRef = decode(in).asInstanceOf[AnyRef]

    /** Spark value of a key from [[decodeKey]]. */
    def keyValue(key: AnyRef): Any = key
  }

  private object BoolDecoder extends ValueDecoder {
    def minBytes: Int = 1
    private def read(in: RowBinaryInput): Boolean = in.readByte() == 1
    def decode(in: RowBinaryInput): Any = read(in)
    override def decodeInto(in: RowBinaryInput, row: DecodedRow, ordinal: Int): Unit =
      row.setBoolean(ordinal, read(in))
    override def decodeArray(in: RowBinaryInput, n: Int): ArrayData = {
      val values = new Array[Boolean](n)
      var i = 0
      while (i < n) {
        values(i) = read(in)
        i += 1
      }
      UnsafeArrayData.fromPrimitiveArray(values)
    }
  }

  private object Int8Decoder extends ValueDecoder {
    def minBytes: Int = 1
    def decode(in: RowBinaryInput): Any = in.readByte()
    override def decodeInto(in: RowBinaryInput, row: DecodedRow, ordinal: Int): Unit =
      row.setByte(ordinal, in.readByte())
    override def decodeArray(in: RowBinaryInput, n: Int): ArrayData = {
      val values = new Array[Byte](n)
      var i = 0
      while (i < n) {
        values(i) = in.readByte()
        i += 1
      }
      UnsafeArrayData.fromPrimitiveArray(values)
    }
  }

  abstract private class ShortDecoder extends ValueDecoder {
    protected def read(in: RowBinaryInput): Short
    def decode(in: RowBinaryInput): Any = read(in)
    override def decodeInto(in: RowBinaryInput, row: DecodedRow, ordinal: Int): Unit =
      row.setShort(ordinal, read(in))
    override def decodeArray(in: RowBinaryInput, n: Int): ArrayData = {
      val values = new Array[Short](n)
      var i = 0
      while (i < n) {
        values(i) = read(in)
        i += 1
      }
      UnsafeArrayData.fromPrimitiveArray(values)
    }
  }

  private object UInt8Decoder extends ShortDecoder {
    def minBytes: Int = 1
    protected def read(in: RowBinaryInput): Short = (in.readByte() & 0xff).toShort
  }

  private object Int16Decoder extends ShortDecoder {
    def minBytes: Int = 2
    protected def read(in: RowBinaryInput): Short = in.readShortLE()
  }

  abstract private class IntDecoder extends ValueDecoder {
    protected def read(in: RowBinaryInput): Int
    def decode(in: RowBinaryInput): Any = read(in)
    override def decodeInto(in: RowBinaryInput, row: DecodedRow, ordinal: Int): Unit =
      row.setInt(ordinal, read(in))
    override def decodeArray(in: RowBinaryInput, n: Int): ArrayData = {
      val values = new Array[Int](n)
      var i = 0
      while (i < n) {
        values(i) = read(in)
        i += 1
      }
      UnsafeArrayData.fromPrimitiveArray(values)
    }
  }

  private object UInt16Decoder extends IntDecoder {
    def minBytes: Int = 2
    protected def read(in: RowBinaryInput): Int = in.readShortLE() & 0xffff
  }

  private object Int32Decoder extends IntDecoder {
    def minBytes: Int = 4
    protected def read(in: RowBinaryInput): Int = in.readIntLE()
  }

  final private class DateDecoder(dates: StartOfDayDates) extends IntDecoder {
    def minBytes: Int = 2
    protected def read(in: RowBinaryInput): Int = dates.epochDay(in.readShortLE() & 0xffff)
  }

  final private class Date32Decoder(dates: StartOfDayDates) extends IntDecoder {
    def minBytes: Int = 4
    protected def read(in: RowBinaryInput): Int = dates.epochDay(in.readIntLE())
  }

  abstract private class LongDecoder extends ValueDecoder {
    def minBytes: Int = 8
    protected def read(in: RowBinaryInput): Long
    def decode(in: RowBinaryInput): Any = read(in)
    override def decodeInto(in: RowBinaryInput, row: DecodedRow, ordinal: Int): Unit =
      row.setLong(ordinal, read(in))
    override def decodeArray(in: RowBinaryInput, n: Int): ArrayData = {
      val values = new Array[Long](n)
      var i = 0
      while (i < n) {
        values(i) = read(in)
        i += 1
      }
      UnsafeArrayData.fromPrimitiveArray(values)
    }
  }

  private object UInt32Decoder extends LongDecoder {
    override def minBytes: Int = 4
    protected def read(in: RowBinaryInput): Long = in.readIntLE() & 0xffffffffL
  }

  private object Int64Decoder extends LongDecoder {
    protected def read(in: RowBinaryInput): Long = in.readLongLE()
  }

  private object DateTimeDecoder extends LongDecoder {
    override def minBytes: Int = 4
    protected def read(in: RowBinaryInput): Long = (in.readIntLE() & 0xffffffffL) * 1000000L
  }

  final private class DateTime64Decoder(scale: Int) extends LongDecoder {
    private val factor = pow10(math.abs(6 - scale))
    // client-v2 goes through Instant and ZonedDateTime, which beyond this can fail or saturate
    private val maxTicks = if (scale > 6) Long.MaxValue else 9000000000000L * pow10(scale)

    private def micros(ticks: Long): Long =
      if (scale > 6) Math.floorDiv(ticks, factor)
      else if (ticks >= -maxTicks && ticks <= maxTicks) ticks * factor
      else throw Unreadable

    protected def read(in: RowBinaryInput): Long = micros(in.readLongLE())

    // client-v2's keys keep nanoseconds, so keys that differ below a microsecond aren't merged
    override def decodeKey(in: RowBinaryInput): AnyRef =
      if (scale > 6) java.lang.Long.valueOf(in.readLongLE()) else super.decodeKey(in)

    override def keyValue(key: AnyRef): Any =
      if (scale > 6) micros(key.asInstanceOf[java.lang.Long].longValue) else key
  }

  private object Float32Decoder extends ValueDecoder {
    def minBytes: Int = 4
    private def read(in: RowBinaryInput): Float = java.lang.Float.intBitsToFloat(in.readIntLE())
    def decode(in: RowBinaryInput): Any = read(in)
    override def decodeInto(in: RowBinaryInput, row: DecodedRow, ordinal: Int): Unit =
      row.setFloat(ordinal, read(in))
    override def decodeArray(in: RowBinaryInput, n: Int): ArrayData = {
      val values = new Array[Float](n)
      var i = 0
      while (i < n) {
        values(i) = read(in)
        i += 1
      }
      UnsafeArrayData.fromPrimitiveArray(values)
    }
  }

  private object Float64Decoder extends ValueDecoder {
    def minBytes: Int = 8
    private def read(in: RowBinaryInput): Double = java.lang.Double.longBitsToDouble(in.readLongLE())
    def decode(in: RowBinaryInput): Any = read(in)
    override def decodeInto(in: RowBinaryInput, row: DecodedRow, ordinal: Int): Unit =
      row.setDouble(ordinal, read(in))
    override def decodeArray(in: RowBinaryInput, n: Int): ArrayData = {
      val values = new Array[Double](n)
      var i = 0
      while (i < n) {
        values(i) = read(in)
        i += 1
      }
      UnsafeArrayData.fromPrimitiveArray(values)
    }
  }

  // client-v2's BigInteger becomes the Decimal through today's toDecimal, rounding included
  final private class BigIntegerDecoder(length: Int, unsigned: Boolean, d: DecimalType) extends ValueDecoder {
    def minBytes: Int = length
    def decode(in: RowBinaryInput): Any = ClickHouseBinaryReader.toDecimal(bigInteger(in, length, unsigned), d)
    override def decodeKey(in: RowBinaryInput): AnyRef = bigInteger(in, length, unsigned)
    override def keyValue(key: AnyRef): Any = ClickHouseBinaryReader.toDecimal(key, d)
  }

  final private class DecimalDecoder(width: Int, scale: Int, d: DecimalType) extends ValueDecoder {
    def minBytes: Int = width
    // the unsafe Decimal equals today's only when the unscaled value fits the precision
    private val fast = d.precision <= Decimal.MAX_LONG_DIGITS
    private val bound = if (fast) pow10(d.precision) else 0L

    def decode(in: RowBinaryInput): Any = {
      val unscaled = if (width == 4) in.readIntLE().toLong else in.readLongLE()
      if (fast && unscaled > -bound && unscaled < bound) Decimal.createUnsafe(unscaled, d.precision, d.scale)
      else ClickHouseBinaryReader.toDecimal(JBigDecimal.valueOf(unscaled, scale), d)
    }
  }

  final private class Decimal128Decoder(scale: Int, d: DecimalType) extends ValueDecoder {
    def minBytes: Int = 16
    def decode(in: RowBinaryInput): Any =
      ClickHouseBinaryReader.toDecimal(new JBigDecimal(bigInteger(in, 16, false), scale), d)
  }

  private object StringDecoder extends ValueDecoder {
    def minBytes: Int = 1
    def decode(in: RowBinaryInput): Any = {
      val length = in.readVarInt()
      if (length < 0) throw Unreadable
      if (length == 0) UTF8String.EMPTY_UTF8 else utf8(in, in.take(length), length)
    }
  }

  final private class FixedStringDecoder(length: Int, binary: Boolean, utf8IsDefault: Boolean)
      extends ValueDecoder {
    def minBytes: Int = length
    def decode(in: RowBinaryInput): Any = {
      val offset = in.take(length)
      // today: new String(bytes, UTF_8), then getBytes with the default charset or UTF8String.fromString
      if (!binary) utf8(in, offset, length)
      else if (utf8IsDefault && in.isAscii(offset, length)) Arrays.copyOfRange(in.buffer, offset, offset + length)
      else new String(in.buffer, offset, length, StandardCharsets.UTF_8).getBytes
    }
  }

  private object UuidDecoder extends ValueDecoder {
    def minBytes: Int = 16
    def decode(in: RowBinaryInput): Any = {
      val msb = in.readLongLE()
      val lsb = in.readLongLE()
      UTF8String.fromString(new UUID(msb, lsb).toString)
    }
  }

  private object IPv4Decoder extends ValueDecoder {
    private val octets = Array.tabulate(256)(i => UTF8String.fromString(i.toString))
    private val dot = UTF8String.fromString(".")
    def minBytes: Int = 4
    // Inet4Address.getHostAddress of client-v2's reversed bytes
    def decode(in: RowBinaryInput): Any = {
      val v = in.readIntLE()
      UTF8String.concat(
        octets(v >>> 24),
        dot,
        octets((v >> 16) & 0xff),
        dot,
        octets((v >> 8) & 0xff),
        dot,
        octets(v & 0xff)
      )
    }
  }

  private object IPv6Decoder extends ValueDecoder {
    def minBytes: Int = 16
    // IPv4-mapped addresses come back as Inet4Address
    def decode(in: RowBinaryInput): Any = {
      val offset = in.take(16)
      val address = InetAddress.getByAddress(Arrays.copyOfRange(in.buffer, offset, offset + 16))
      UTF8String.fromString(address.getHostAddress)
    }
  }

  private object EnumDecoder {
    private val Unknown = UTF8String.fromString("<unknown>")

    // Same lookup as ClickHouseEnum.nameNullable: the first constant with the value
    def apply(column: ClickHouseColumn): Option[ValueDecoder] = {
      val values = column.getEnumConstants.getValues
      val names = column.getEnumConstants.getNames
      val min = if (values.isEmpty) 0 else values.min
      val size = if (values.isEmpty) 0L else values.max.toLong - min + 1
      if (size > 65536) None
      else {
        val table = new Array[UTF8String](size.toInt)
        values.indices.foreach { i =>
          if (table(values(i) - min) == null) table(values(i) - min) = UTF8String.fromString(names(i))
        }
        Some(new EnumDecoder(column.getDataType == CH.Enum16, min, table))
      }
    }
  }

  final private class EnumDecoder(enum16: Boolean, min: Int, names: Array[UTF8String]) extends ValueDecoder {
    def minBytes: Int = if (enum16) 2 else 1
    def decode(in: RowBinaryInput): Any = {
      val i = (if (enum16) in.readShortLE().toInt else in.readByte().toInt) - min
      if (i >= 0 && i < names.length && names(i) != null) names(i) else EnumDecoder.Unknown
    }
    // client-v2's EnumValue keys compare by identity, so equal names aren't merged before the Spark map
    override def decodeKey(in: RowBinaryInput): AnyRef = new IdentityKey(decode(in))
    override def keyValue(key: AnyRef): Any = key.asInstanceOf[IdentityKey].value
  }

  final private class IdentityKey(val value: Any)

  private object NothingDecoder extends ValueDecoder {
    def minBytes: Int = 0
    def decode(in: RowBinaryInput): Any = null
  }

  final private class NullableDecoder(value: ValueDecoder) extends ValueDecoder {
    def minBytes: Int = 1
    def decode(in: RowBinaryInput): Any = if (in.readByte() == 1) null else value.decode(in)
    override def decodeInto(in: RowBinaryInput, row: DecodedRow, ordinal: Int): Unit =
      if (in.readByte() == 1) row.setNullAt(ordinal) else value.decodeInto(in, row, ordinal)
  }

  private def boxedArray(element: ValueDecoder, in: RowBinaryInput, n: Int): ArrayData = {
    val values = new Array[Any](n)
    var i = 0
    while (i < n) {
      values(i) = element.decode(in)
      i += 1
    }
    new GenericArrayData(values)
  }

  // Checks that `n` values of at least `size` bytes are buffered before allocating for them
  private def requireValues(in: RowBinaryInput, n: Int, size: Int): Unit = {
    if (n < 0) throw Unreadable
    val bytes = n.toLong * size
    if (bytes > Int.MaxValue) throw Unreadable
    in.require(bytes.toInt)
  }

  final private class ArrayDecoder(element: ValueDecoder) extends ValueDecoder {
    def minBytes: Int = 1
    def decode(in: RowBinaryInput): Any = {
      val n = in.readVarInt()
      requireValues(in, n, element.minBytes)
      // UnsafeArrayData can't take ~2^28 8-byte values; boxed as today, the projection then fails as today
      if (n > (1 << 27)) boxedArray(element, in, n) else element.decodeArray(in, n)
    }
  }

  final private class MapDecoder(key: ValueDecoder, value: ValueDecoder) extends ValueDecoder {
    def minBytes: Int = 1
    def decode(in: RowBinaryInput): Any = {
      val n = in.readVarInt()
      requireValues(in, n, key.minBytes + value.minBytes)
      if (n == 0) ArrayBasedMapData(Array.empty[Any], Array.empty[Any])
      else {
        // client-v2 collects entries in a LinkedHashMap, then ClickHouseBinaryReader builds the Spark map from it
        val entries = new java.util.LinkedHashMap[AnyRef, AnyRef](n)
        var i = 0
        while (i < n) {
          val k = key.decodeKey(in)
          entries.put(k, value.decode(in).asInstanceOf[AnyRef])
          i += 1
        }
        val converted = entries.asScala.map { case (k, v) => (key.keyValue(k), v) }
        ArrayBasedMapData(converted)
      }
    }
  }

  final private class TupleDecoder(fields: Array[ValueDecoder]) extends ValueDecoder {
    val minBytes: Int = fields.map(_.minBytes).sum
    def decode(in: RowBinaryInput): Any = {
      val values = new Array[Any](fields.length)
      var i = 0
      while (i < fields.length) {
        values(i) = fields(i).decode(in)
        i += 1
      }
      new GenericInternalRow(values)
    }
  }
}

/** Reused row the decoders fill: arrays, as SpecificInternalRow's object per field made speed vary by heap layout. */
final private[format] class DecodedRow(types: Array[DataType]) extends BaseGenericInternalRow {
  private val primitive = types.map {
    case BooleanType | ByteType | ShortType | IntegerType | DateType | LongType | TimestampType | FloatType |
        DoubleType =>
      true
    case _ => false
  }
  private val nulls = new Array[Boolean](types.length)
  private val bits = new Array[Long](types.length)
  private val objects = new Array[AnyRef](types.length)

  override def numFields: Int = types.length

  // Boxed like SpecificInternalRow's values, for get(ordinal, dataType), copy, equals and toString
  override protected def genericGet(ordinal: Int): Any =
    if (nulls(ordinal)) null
    else types(ordinal) match {
      case BooleanType => getBoolean(ordinal)
      case ByteType => getByte(ordinal)
      case ShortType => getShort(ordinal)
      case IntegerType | DateType => getInt(ordinal)
      case LongType | TimestampType => getLong(ordinal)
      case FloatType => getFloat(ordinal)
      case DoubleType => getDouble(ordinal)
      case _ => objects(ordinal)
    }

  // primitive getters then read 0, as on the GenericInternalRow decoded today
  override def setNullAt(ordinal: Int): Unit = {
    nulls(ordinal) = true
    bits(ordinal) = 0
    objects(ordinal) = null
  }

  override def update(ordinal: Int, value: Any): Unit =
    if (value == null) setNullAt(ordinal)
    else if (primitive(ordinal)) types(ordinal) match {
      case BooleanType => setBoolean(ordinal, value.asInstanceOf[Boolean])
      case ByteType => setByte(ordinal, value.asInstanceOf[Byte])
      case ShortType => setShort(ordinal, value.asInstanceOf[Short])
      case IntegerType | DateType => setInt(ordinal, value.asInstanceOf[Int])
      case LongType | TimestampType => setLong(ordinal, value.asInstanceOf[Long])
      case FloatType => setFloat(ordinal, value.asInstanceOf[Float])
      case DoubleType => setDouble(ordinal, value.asInstanceOf[Double])
    }
    else {
      nulls(ordinal) = false
      objects(ordinal) = value.asInstanceOf[AnyRef]
    }

  private def setBits(ordinal: Int, value: Long): Unit = {
    nulls(ordinal) = false
    bits(ordinal) = value
  }

  override def setBoolean(ordinal: Int, value: Boolean): Unit = setBits(ordinal, if (value) 1L else 0L)
  override def setByte(ordinal: Int, value: Byte): Unit = setBits(ordinal, value)
  override def setShort(ordinal: Int, value: Short): Unit = setBits(ordinal, value)
  override def setInt(ordinal: Int, value: Int): Unit = setBits(ordinal, value)
  override def setLong(ordinal: Int, value: Long): Unit = setBits(ordinal, value)
  override def setFloat(ordinal: Int, value: Float): Unit = setBits(ordinal, java.lang.Float.floatToRawIntBits(value))
  override def setDouble(ordinal: Int, value: Double): Unit =
    setBits(ordinal, java.lang.Double.doubleToRawLongBits(value))

  override def isNullAt(ordinal: Int): Boolean = nulls(ordinal)
  override def getBoolean(ordinal: Int): Boolean = bits(ordinal) != 0
  override def getByte(ordinal: Int): Byte = bits(ordinal).toByte
  override def getShort(ordinal: Int): Short = bits(ordinal).toShort
  override def getInt(ordinal: Int): Int = bits(ordinal).toInt
  override def getLong(ordinal: Int): Long = bits(ordinal)
  override def getFloat(ordinal: Int): Float = java.lang.Float.intBitsToFloat(bits(ordinal).toInt)
  override def getDouble(ordinal: Int): Double = java.lang.Double.longBitsToDouble(bits(ordinal))
  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = objects(ordinal).asInstanceOf[Decimal]
  override def getUTF8String(ordinal: Int): UTF8String = objects(ordinal).asInstanceOf[UTF8String]
  override def getBinary(ordinal: Int): Array[Byte] = objects(ordinal).asInstanceOf[Array[Byte]]
  override def getArray(ordinal: Int): ArrayData = objects(ordinal).asInstanceOf[ArrayData]
  override def getMap(ordinal: Int): MapData = objects(ordinal).asInstanceOf[MapData]
  override def getStruct(ordinal: Int, numFields: Int): InternalRow = objects(ordinal).asInstanceOf[InternalRow]
}
