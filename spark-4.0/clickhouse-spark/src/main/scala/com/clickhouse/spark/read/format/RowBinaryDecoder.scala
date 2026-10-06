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

import com.clickhouse.data.{ClickHouseColumn, ClickHouseDataType => CH, ClickHouseEnum}
import com.clickhouse.spark.format.RowBinaryInput
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, SpecificInternalRow, UnsafeArrayData}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import java.net.InetAddress
import java.nio.charset.StandardCharsets.UTF_8
import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Decodes RowBinaryWithNamesAndTypes straight into Spark rows, without client-v2's per-value objects.
 * Each supported type decodes to the same value as the client-based path in [[ClickHouseBinaryReader]],
 * except that map entries keep ClickHouse's order and duplicate keys.
 */
private[format] object RowBinaryDecoder {

  /**
   * Reads the header and returns the rows, decoded into one reused row, or None if the stream is empty
   * or has a column this decoder does not support. In the None case the stream can still be replayed.
   */
  def rows(in: RowBinaryInput, schema: StructType): Option[Iterator[InternalRow]] =
    if (schema.isEmpty || !in.hasMore) None
    else {
      val n = in.readVarInt()
      val names = Seq.fill(n)(readString(in))
      val types = Seq.fill(n)(readString(in))
      Try(names.zip(types).map { case (name, chType) => ClickHouseColumn.of(name, chType) }).toOption
        .filter(_.size == schema.length)
        .map(_.zip(schema.fields).flatMap { case (column, field) => decoder(column, field.dataType) })
        .filter(_.size == schema.length)
        .map { decoders =>
          in.release()
          new RowIterator(in, decoders.toArray, new SpecificInternalRow(schema))
        }
    }

  // Java client returns BigInteger for Int128/Int256 and UInt64, BigDecimal for Decimal types
  def toDecimal(value: AnyRef, d: DecimalType): Decimal = {
    val dec: BigDecimal = value match {
      case bi: java.math.BigInteger => BigDecimal(bi)
      case bd: java.math.BigDecimal => BigDecimal(bd)
    }
    Decimal(dec.setScale(d.scale))
  }

  // Reused for every row: Spark copies each row into an UnsafeRow before calling next()
  final private class RowIterator(in: RowBinaryInput, decoders: Array[ValueDecoder], row: SpecificInternalRow)
      extends Iterator[InternalRow] {
    override def hasNext: Boolean = in.hasMore

    override def next(): InternalRow = {
      var i = 0
      while (i < decoders.length) {
        decoders(i).write(in, row, i)
        i += 1
      }
      row
    }
  }

  private def decoder(column: ClickHouseColumn, dataType: DataType): Option[ValueDecoder] = {
    val valueDecoder: Option[ValueDecoder] = (column.getDataType, dataType) match {
      case (CH.Bool, BooleanType) => Some(BoolDecoder)
      case (CH.Int8, ByteType) => Some(Int8Decoder)
      case (CH.UInt8, ShortType) => Some(UInt8Decoder)
      case (CH.Int16, ShortType) => Some(Int16Decoder)
      case (CH.UInt16, IntegerType) | (CH.Date, DateType) => Some(UInt16Decoder)
      case (CH.Int32, IntegerType) | (CH.Date32, DateType) => Some(Int32Decoder)
      case (CH.UInt32, LongType) => Some(UInt32Decoder)
      case (CH.Int64, LongType) => Some(Int64Decoder)
      case (CH.UInt64, d: DecimalType) => Some(new BigIntegerDecoder(8, unsigned = true, d))
      case (CH.Int128, d: DecimalType) => Some(new BigIntegerDecoder(16, unsigned = false, d))
      case (CH.UInt128, d: DecimalType) => Some(new BigIntegerDecoder(16, unsigned = true, d))
      case (CH.Int256, d: DecimalType) => Some(new BigIntegerDecoder(32, unsigned = false, d))
      case (CH.UInt256, d: DecimalType) => Some(new BigIntegerDecoder(32, unsigned = true, d))
      case (CH.Float32, FloatType) => Some(Float32Decoder)
      case (CH.Float64, DoubleType) => Some(Float64Decoder)
      case (CH.Decimal | CH.Decimal32 | CH.Decimal64 | CH.Decimal128, d: DecimalType) =>
        Some(new DecimalDecoder(decimalByteWidth(column), column.getScale, d))
      case (CH.DateTime | CH.DateTime32, TimestampType) => Some(DateTime32Decoder)
      case (CH.DateTime64, TimestampType) => Some(new DateTime64Decoder(column.getScale))
      case (CH.String, StringType) => Some(StringDecoder)
      case (CH.FixedString, BinaryType) => Some(new FixedStringDecoder(column.getPrecision, binary = true))
      case (CH.FixedString, StringType) => Some(new FixedStringDecoder(column.getPrecision, binary = false))
      case (CH.UUID, StringType) => Some(UUIDDecoder)
      case (CH.IPv4, StringType) => Some(IPv4Decoder)
      case (CH.IPv6, StringType) => Some(IPv6Decoder)
      case (CH.Enum8, StringType) => Some(new Enum8Decoder(column.getEnumConstants))
      case (CH.Enum16, StringType) => Some(new Enum16Decoder(column.getEnumConstants))
      case (CH.Nothing, NullType) => Some(NothingDecoder)
      case (CH.Array, ArrayType(elementType, _)) =>
        decoder(column.getNestedColumns.get(0), elementType).map(new ArrayDecoder(_))
      case (CH.Map, MapType(keyType, valueType, _)) =>
        for {
          k <- decoder(column.getKeyInfo, keyType)
          v <- decoder(column.getValueInfo, valueType)
        } yield new MapDecoder(k, v)
      case (CH.Tuple, struct: StructType) if column.getNestedColumns.size == struct.length =>
        val fields = column.getNestedColumns.asScala.zip(struct.fields).flatMap { case (c, f) =>
          decoder(c, f.dataType)
        }
        if (fields.size == struct.length) Some(new TupleDecoder(fields.toArray)) else None
      case _ => None
    }
    valueDecoder.map(d => if (column.isNullable) new NullableDecoder(d) else d)
  }

  // Storage width follows the precision, as in the client
  private def decimalByteWidth(column: ClickHouseColumn): Int = column.getDataType match {
    case CH.Decimal32 => 4
    case CH.Decimal64 => 8
    case CH.Decimal128 => 16
    case _ =>
      val p = column.getPrecision
      if (p <= 9) 4 else if (p <= 18) 8 else if (p <= 38) 16 else 32
  }

  private val PowersOf10: Array[Long] = Array.iterate(1L, 19)(_ * 10)

  private def readString(in: RowBinaryInput): String = new String(in.readBytes(in.readVarInt()), UTF_8)

  private def readBigInteger(in: RowBinaryInput, byteWidth: Int, unsigned: Boolean): java.math.BigInteger = {
    val bytes = in.readBytes(byteWidth)
    var i = 0
    var j = bytes.length - 1
    while (i < j) {
      val b = bytes(i)
      bytes(i) = bytes(j)
      bytes(j) = b
      i += 1
      j -= 1
    }
    if (unsigned) new java.math.BigInteger(1, bytes) else new java.math.BigInteger(bytes)
  }

  // The client-based path decodes to java.lang.String, which replaces malformed UTF-8; ASCII round-trips as is
  private def toUTF8String(bytes: Array[Byte]): UTF8String =
    if (isAscii(bytes)) UTF8String.fromBytes(bytes) else UTF8String.fromString(new String(bytes, UTF_8))

  private def isAscii(bytes: Array[Byte]): Boolean = {
    var i = 0
    while (i < bytes.length) {
      if (bytes(i) < 0) return false
      i += 1
    }
    true
  }

  private def enumName(constants: ClickHouseEnum, value: Int): UTF8String = {
    val name = constants.nameNullable(value)
    UTF8String.fromString(if (name == null) "<unknown>" else name)
  }

  /** Decodes one value; `write` stores it in a row slot, unboxed where the slot is primitive. */
  abstract class ValueDecoder {
    def read(in: RowBinaryInput): Any

    def write(in: RowBinaryInput, row: InternalRow, ordinal: Int): Unit = row.update(ordinal, read(in))

    def readArray(in: RowBinaryInput, length: Int): ArrayData = {
      val values = new Array[Any](length)
      var i = 0
      while (i < length) {
        values(i) = read(in)
        i += 1
      }
      new GenericArrayData(values)
    }
  }

  private object BoolDecoder extends ValueDecoder {
    // like the client, only 1 is true
    def read(in: RowBinaryInput): Any = in.readByte() == 1

    override def write(in: RowBinaryInput, row: InternalRow, ordinal: Int): Unit =
      row.setBoolean(ordinal, in.readByte() == 1)

    override def readArray(in: RowBinaryInput, length: Int): ArrayData = {
      val values = new Array[Boolean](length)
      var i = 0
      while (i < length) {
        values(i) = in.readByte() == 1
        i += 1
      }
      UnsafeArrayData.fromPrimitiveArray(values)
    }
  }

  private object Int8Decoder extends ValueDecoder {
    def read(in: RowBinaryInput): Any = in.readByte()

    override def write(in: RowBinaryInput, row: InternalRow, ordinal: Int): Unit = row.setByte(ordinal, in.readByte())

    override def readArray(in: RowBinaryInput, length: Int): ArrayData = {
      val values = new Array[Byte](length)
      var i = 0
      while (i < length) {
        values(i) = in.readByte()
        i += 1
      }
      UnsafeArrayData.fromPrimitiveArray(values)
    }
  }

  abstract private class ShortDecoder extends ValueDecoder {
    def readShort(in: RowBinaryInput): Short

    final def read(in: RowBinaryInput): Any = readShort(in)

    final override def write(in: RowBinaryInput, row: InternalRow, ordinal: Int): Unit =
      row.setShort(ordinal, readShort(in))

    final override def readArray(in: RowBinaryInput, length: Int): ArrayData = {
      val values = new Array[Short](length)
      var i = 0
      while (i < length) {
        values(i) = readShort(in)
        i += 1
      }
      UnsafeArrayData.fromPrimitiveArray(values)
    }
  }

  private object UInt8Decoder extends ShortDecoder {
    def readShort(in: RowBinaryInput): Short = (in.readByte() & 0xff).toShort
  }

  private object Int16Decoder extends ShortDecoder {
    def readShort(in: RowBinaryInput): Short = in.readShort()
  }

  abstract private class IntDecoder extends ValueDecoder {
    def readInt(in: RowBinaryInput): Int

    final def read(in: RowBinaryInput): Any = readInt(in)

    final override def write(in: RowBinaryInput, row: InternalRow, ordinal: Int): Unit =
      row.setInt(ordinal, readInt(in))

    final override def readArray(in: RowBinaryInput, length: Int): ArrayData = {
      val values = new Array[Int](length)
      var i = 0
      while (i < length) {
        values(i) = readInt(in)
        i += 1
      }
      UnsafeArrayData.fromPrimitiveArray(values)
    }
  }

  // UInt16, and Date as days since epoch
  private object UInt16Decoder extends IntDecoder {
    def readInt(in: RowBinaryInput): Int = in.readShort() & 0xffff
  }

  // Int32, and Date32 as days since epoch
  private object Int32Decoder extends IntDecoder {
    def readInt(in: RowBinaryInput): Int = in.readInt()
  }

  abstract private class LongDecoder extends ValueDecoder {
    def readLong(in: RowBinaryInput): Long

    final def read(in: RowBinaryInput): Any = readLong(in)

    final override def write(in: RowBinaryInput, row: InternalRow, ordinal: Int): Unit =
      row.setLong(ordinal, readLong(in))

    final override def readArray(in: RowBinaryInput, length: Int): ArrayData = {
      val values = new Array[Long](length)
      var i = 0
      while (i < length) {
        values(i) = readLong(in)
        i += 1
      }
      UnsafeArrayData.fromPrimitiveArray(values)
    }
  }

  private object UInt32Decoder extends LongDecoder {
    def readLong(in: RowBinaryInput): Long = in.readInt() & 0xffffffffL
  }

  private object Int64Decoder extends LongDecoder {
    def readLong(in: RowBinaryInput): Long = in.readLong()
  }

  // Seconds since epoch to micros
  private object DateTime32Decoder extends LongDecoder {
    def readLong(in: RowBinaryInput): Long = (in.readInt() & 0xffffffffL) * 1000000L
  }

  // Ticks of 10^-scale seconds to micros, rounding down like the client-based path
  final private class DateTime64Decoder(scale: Int) extends LongDecoder {
    private val factor = PowersOf10(math.abs(6 - scale))

    def readLong(in: RowBinaryInput): Long = {
      val ticks = in.readLong()
      if (scale <= 6) ticks * factor else Math.floorDiv(ticks, factor)
    }
  }

  private object Float32Decoder extends ValueDecoder {
    def read(in: RowBinaryInput): Any = in.readFloat()

    override def write(in: RowBinaryInput, row: InternalRow, ordinal: Int): Unit = row.setFloat(ordinal, in.readFloat())

    override def readArray(in: RowBinaryInput, length: Int): ArrayData = {
      val values = new Array[Float](length)
      var i = 0
      while (i < length) {
        values(i) = in.readFloat()
        i += 1
      }
      UnsafeArrayData.fromPrimitiveArray(values)
    }
  }

  private object Float64Decoder extends ValueDecoder {
    def read(in: RowBinaryInput): Any = in.readDouble()

    override def write(in: RowBinaryInput, row: InternalRow, ordinal: Int): Unit =
      row.setDouble(ordinal, in.readDouble())

    override def readArray(in: RowBinaryInput, length: Int): ArrayData = {
      val values = new Array[Double](length)
      var i = 0
      while (i < length) {
        values(i) = in.readDouble()
        i += 1
      }
      UnsafeArrayData.fromPrimitiveArray(values)
    }
  }

  // Values that fit the Spark type's precision take the compact path, the rest match the client-based path
  final private class DecimalDecoder(byteWidth: Int, chScale: Int, d: DecimalType) extends ValueDecoder {
    private val compact = d.precision <= Decimal.MAX_LONG_DIGITS && d.scale == chScale
    private val bound = PowersOf10(math.min(d.precision, Decimal.MAX_LONG_DIGITS))

    def read(in: RowBinaryInput): Any =
      if (byteWidth == 4) fromUnscaled(in.readInt())
      else if (byteWidth == 8) fromUnscaled(in.readLong())
      else toDecimal(new java.math.BigDecimal(readBigInteger(in, byteWidth, unsigned = false), chScale), d)

    private def fromUnscaled(unscaled: Long): Decimal =
      if (compact && unscaled > -bound && unscaled < bound) Decimal.createUnsafe(unscaled, d.precision, d.scale)
      else toDecimal(java.math.BigDecimal.valueOf(unscaled, chScale), d)
  }

  final private class BigIntegerDecoder(byteWidth: Int, unsigned: Boolean, d: DecimalType) extends ValueDecoder {
    def read(in: RowBinaryInput): Any = toDecimal(readBigInteger(in, byteWidth, unsigned), d)
  }

  private object StringDecoder extends ValueDecoder {
    def read(in: RowBinaryInput): Any = toUTF8String(in.readBytes(in.readVarInt()))
  }

  final private class FixedStringDecoder(length: Int, binary: Boolean) extends ValueDecoder {
    def read(in: RowBinaryInput): Any = {
      val bytes = in.readBytes(length)
      if (!binary) toUTF8String(bytes)
      else if (isAscii(bytes)) bytes
      else new String(bytes, UTF_8).getBytes
    }
  }

  private object UUIDDecoder extends ValueDecoder {
    def read(in: RowBinaryInput): Any = UTF8String.fromString(new java.util.UUID(in.readLong(), in.readLong()).toString)
  }

  private object IPv4Decoder extends ValueDecoder {
    def read(in: RowBinaryInput): Any = {
      val v = in.readInt()
      val address = Array((v >>> 24).toByte, (v >>> 16).toByte, (v >>> 8).toByte, v.toByte)
      UTF8String.fromString(InetAddress.getByAddress(address).getHostAddress)
    }
  }

  private object IPv6Decoder extends ValueDecoder {
    def read(in: RowBinaryInput): Any = UTF8String.fromString(InetAddress.getByAddress(in.readBytes(16)).getHostAddress)
  }

  final private class Enum8Decoder(constants: ClickHouseEnum) extends ValueDecoder {
    def read(in: RowBinaryInput): Any = enumName(constants, in.readByte())
  }

  final private class Enum16Decoder(constants: ClickHouseEnum) extends ValueDecoder {
    def read(in: RowBinaryInput): Any = enumName(constants, in.readShort())
  }

  private object NothingDecoder extends ValueDecoder {
    def read(in: RowBinaryInput): Any = null
  }

  final private class NullableDecoder(inner: ValueDecoder) extends ValueDecoder {
    def read(in: RowBinaryInput): Any = if (in.readByte() == 1) null else inner.read(in)

    override def write(in: RowBinaryInput, row: InternalRow, ordinal: Int): Unit =
      if (in.readByte() == 1) row.setNullAt(ordinal) else inner.write(in, row, ordinal)
  }

  final private class ArrayDecoder(element: ValueDecoder) extends ValueDecoder {
    def read(in: RowBinaryInput): Any = element.readArray(in, in.readVarInt())
  }

  final private class MapDecoder(key: ValueDecoder, value: ValueDecoder) extends ValueDecoder {
    def read(in: RowBinaryInput): Any = {
      val length = in.readVarInt()
      val keys = new Array[Any](length)
      val values = new Array[Any](length)
      var i = 0
      while (i < length) {
        keys(i) = key.read(in)
        values(i) = value.read(in)
        i += 1
      }
      new ArrayBasedMapData(new GenericArrayData(keys), new GenericArrayData(values))
    }
  }

  final private class TupleDecoder(fields: Array[ValueDecoder]) extends ValueDecoder {
    def read(in: RowBinaryInput): Any = {
      val values = new Array[Any](fields.length)
      var i = 0
      while (i < fields.length) {
        values(i) = fields(i).read(in)
        i += 1
      }
      new GenericInternalRow(values)
    }
  }
}
