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

import com.clickhouse.client.api.data_formats.RowBinaryWithNamesAndTypesFormatReader
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader
import com.clickhouse.client.api.query.{QueryResponse, Records}
import com.clickhouse.data.ClickHouseColumn
import com.clickhouse.spark.format.RowBinaryTestData._
import com.clickhouse.spark.read.{ClickHouseInputPartition, ScanJobDescription}
import com.clickhouse.spark.spec.{MergeTreeEngineSpec, NoPartitionSpec, NodeSpec, TableSpec}
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.clickhouse.{ReadOptions, SchemaUtils}
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

import java.io.{ByteArrayInputStream, IOException, InputStream}
import java.math.{BigDecimal => JBigDecimal, BigInteger}
import java.net.{Inet4Address, InetAddress}
import java.nio.charset.StandardCharsets
import java.time.{Instant, LocalDate, LocalDateTime, Period, ZoneId}
import java.util.{Arrays, TimeZone, UUID}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.control.NonFatal

/**
 * Reads RowBinaryWithNamesAndTypes bytes through [[ClickHouseBinaryReader]] and the way `main` does (client-v2's
 * reader, then [[ClickHouseBinaryReader.decode]]), and checks that Spark gets byte-equal UnsafeRows, or the same
 * exception after the same rows.
 */
class RowBinaryDecoderSuite extends AnyFunSuite {

  private case class Outcome(rows: Seq[UnsafeRow], error: Option[String])

  private val apia = "Pacific/Apia"

  private def schemaOf(columns: Seq[(String, String)]): StructType = StructType(columns.map { case (name, chType) =>
    val (dataType, nullable) = SchemaUtils.fromClickHouseType(ClickHouseColumn.of(name, chType)).get
    StructField(name, dataType, nullable)
  })

  private val table = TableSpec(
    database = "db",
    name = "tbl",
    uuid = "",
    engine = "MergeTree",
    is_temporary = false,
    data_paths = Nil,
    metadata_path = "",
    metadata_modification_time = LocalDateTime.of(2026, 1, 1, 0, 0),
    dependencies_database = Nil,
    dependencies_table = Nil,
    create_table_query = "",
    engine_full = "MergeTree",
    partition_key = "",
    sorting_key = "",
    primary_key = "",
    sampling_key = "",
    storage_policy = "",
    total_rows = None,
    total_bytes = None,
    lifetime_rows = None,
    lifetime_bytes = None
  )

  // The reader Spark creates for a scan, with `response` instead of a query
  private def reader(response: QueryResponse, schema: StructType): ClickHouseBinaryReader = {
    val scanJob = ScanJobDescription(
      node = NodeSpec("127.0.0.1"),
      tz = ZoneId.of("UTC"),
      tableSpec = table,
      tableEngineSpec = MergeTreeEngineSpec(engine_clause = "MergeTree"),
      cluster = None,
      localTableSpec = None,
      localTableEngineSpec = None,
      readOptions = new ReadOptions(new java.util.HashMap[String, String]()),
      readSchema = schema
    )
    val partition = ClickHouseInputPartition(table, NoPartitionSpec, filterByPartitionId = false, NodeSpec("127.0.0.1"))
    new ClickHouseBinaryReader(scanJob, partition) {
      override lazy val resp: QueryResponse = response
    }
  }

  private def describe(e: Throwable): String =
    Iterator.iterate(e)(_.getCause).takeWhile(_ != null).map(t => s"${t.getClass.getName}: ${t.getMessage}")
      .mkString(" <- ")

  // Spark copies each scanned row into an UnsafeRow before asking for the next
  private def collect(schema: StructType)(read: (UnsafeProjection, ArrayBuffer[UnsafeRow]) => Unit): Outcome = {
    val projection = UnsafeProjection.create(schema)
    val rows = ArrayBuffer.empty[UnsafeRow]
    val error =
      try {
        read(projection, rows)
        None
      } catch {
        case NonFatal(e) => Some(describe(e))
      }
    Outcome(rows.toList, error)
  }

  private def readDirect(response: QueryResponse, schema: StructType): (Outcome, Boolean) = {
    val r = reader(response, schema)
    val outcome = collect(schema)((projection, rows) => while (r.next()) rows += projection(r.get).copy())
    (outcome, r.rowDecoder.handedOver)
  }

  // main's streamOutput, next and get
  private def readLikeMain(response: QueryResponse, schema: StructType): Outcome = {
    val r = reader(response, schema)
    collect(schema) { (projection, rows) =>
      val records = new Records(
        response,
        new RowBinaryWithNamesAndTypesFormatReader(
          response.getInputStream,
          response.getSettings,
          new BinaryStreamReader.DefaultByteBufferAllocator
        )
      ).asScala.iterator
      while (records.hasNext) rows += projection(r.decode(records.next())).copy()
    }
  }

  private def variants(bytes: Array[Byte]): Seq[(String, () => InputStream)] = Seq(
    "plain" -> (() => new ByteArrayInputStream(bytes)),
    "one byte per read" -> (() => new Trickle(new ByteArrayInputStream(bytes), 1)),
    "lz4" -> (() => unlz4(new ByteArrayInputStream(lz4(bytes))))
  )

  /** Checks both paths agree on each stream; `handsOver` is None where it depends on where the stream breaks. */
  private def assertSame(
    streams: Seq[(String, () => InputStream)],
    schema: StructType,
    handsOver: Option[Boolean],
    serverTimeZone: String = "UTC",
    settings: Map[String, AnyRef] = clientDefaults
  ): Outcome =
    streams.map { case (label, stream) =>
      val (direct, handedOver) = readDirect(response(stream(), serverTimeZone, settings), schema)
      val main = readLikeMain(response(stream(), serverTimeZone, settings), schema)
      assert(direct === main, label)
      handsOver.foreach(expected => assert(handedOver === expected, label))
      direct
    }.head

  private def check(
    columns: Seq[(String, String)],
    rows: Seq[Seq[Any]],
    schema: Option[StructType] = None,
    handsOver: Boolean = false,
    serverTimeZone: String = "UTC",
    settings: Map[String, AnyRef] = clientDefaults
  ): Outcome = {
    val readSchema = schema.getOrElse(schemaOf(columns))
    val outcome =
      assertSame(variants(encode(columns, rows)), readSchema, Some(handsOver), serverTimeZone, settings)
    if (!handsOver) {
      assert(outcome.error.isEmpty)
      assert(outcome.rows.size === rows.size)
    }
    outcome
  }

  private def bigInt(s: String): BigInteger = new BigInteger(s)

  private def dec(s: String): JBigDecimal = new JBigDecimal(s)

  // Raw bytes of a non-null value in a Nullable column
  private def notNull(raw: Raw): Raw = Raw(0.toByte +: raw.bytes)

  private def le(value: BigInteger, width: Int): Raw = {
    val bytes = Array.fill[Byte](width)(if (value.signum < 0) -1 else 0)
    value.toByteArray.reverse.take(width).zipWithIndex.foreach { case (b, i) => bytes(i) = b }
    Raw(bytes)
  }

  private def ip4(s: String): Inet4Address = InetAddress.getByName(s).asInstanceOf[Inet4Address]

  private def ip6(s: String): Raw = Raw(InetAddress.getByName(s).getAddress match {
    case v4 if v4.length == 4 => Array.fill[Byte](10)(0) ++ Array[Byte](-1, -1) ++ v4
    case v6 => v6
  })

  private def date(s: String): LocalDate = LocalDate.parse(s)

  test("integers at their limits") {
    val columns = Seq(
      "i8" -> "Int8",
      "i16" -> "Int16",
      "i32" -> "Int32",
      "i64" -> "Int64",
      "u8" -> "UInt8",
      "u16" -> "UInt16",
      "u32" -> "UInt32",
      "u64" -> "UInt64",
      "b" -> "Bool"
    )
    check(
      columns,
      Seq(
        Seq(Byte.MinValue.toInt, Short.MinValue.toInt, Int.MinValue, Long.MinValue, 0, 0, 0L, 0L, false),
        Seq(Byte.MaxValue.toInt, Short.MaxValue.toInt, Int.MaxValue, Long.MaxValue, 255, 65535, 4294967295L, -1L, true),
        Seq(-1, -1, -1, -1L, 1, 1, 1L, Long.MaxValue, Raw(Array[Byte](2))),
        Seq(0, 0, 0, 0L, 128, 32768, 2147483648L, Long.MinValue, Raw(Array[Byte](-1)))
      )
    )
  }

  test("128- and 256-bit integers, including values today's decimal conversion rounds or nulls") {
    val columns = Seq("i128" -> "Int128", "u128" -> "UInt128", "i256" -> "Int256", "u256" -> "UInt256")
    def max(bits: Int) = BigInteger.ONE.shiftLeft(bits).subtract(BigInteger.ONE)
    val digits34 = bigInt("9" * 34)
    val digits35 = bigInt("12345678901234567890123456789012345")
    val digits38 = bigInt("9" * 38)
    val digits39 = digits38.add(BigInteger.ONE)
    check(
      columns,
      Seq(
        Seq(BigInteger.ZERO, BigInteger.ZERO, BigInteger.ZERO, BigInteger.ZERO),
        Seq(max(127), max(128), max(255), max(256)),
        Seq(
          max(127).negate.subtract(BigInteger.ONE),
          BigInteger.ONE,
          max(255).negate.subtract(BigInteger.ONE),
          BigInteger.ONE
        ),
        Seq(digits34, digits34, digits34.negate, digits34),
        Seq(digits35.negate, digits35, digits35, digits35),
        Seq(digits38, digits38, digits38.negate, digits38),
        Seq(digits39, digits39, digits39.negate, digits39)
      )
    )
  }

  test("UInt64 as Decimal(20, 0)") {
    check(
      Seq("u" -> "UInt64", "n" -> "Nullable(UInt64)"),
      Seq(Seq(0L, null), Seq(1L, 2L), Seq(Long.MaxValue, Long.MinValue), Seq(-1L, -2L))
    )
  }

  test("floats, including NaN payloads, infinities and signed zeros") {
    val floats = Seq(0f, -0f, 1.5f, Float.MinPositiveValue, Float.MaxValue, Float.NegativeInfinity)
    val doubles = Seq(0d, -0d, -2.25d, Double.MinPositiveValue, Double.MaxValue, Double.PositiveInfinity)
    val rows = floats.zip(doubles).map { case (f, d) => Seq[Any](f, d, f) } ++ Seq(
      Seq(int32LE(0x7fc00001), int64LE(0x7ff8000000000001L), notNull(int32LE(0xff800001))),
      Seq(int32LE(0x7f800001), int64LE(0xfff0000000000001L), null)
    )
    check(Seq("f" -> "Float32", "d" -> "Float64", "n" -> "Nullable(Float32)"), rows)
  }

  test("decimals at their precision limits, negative, and too wide for their precision") {
    val columns = Seq(
      "d9" -> "Decimal(9, 2)",
      "d18" -> "Decimal(18, 4)",
      "d38" -> "Decimal(38, 10)",
      "d32" -> "Decimal32(3)",
      "d64" -> "Decimal64(5)",
      "d128" -> "Decimal128(0)",
      "d1" -> "Decimal(1, 0)",
      "d10" -> "Nullable(Decimal(10, 10))"
    )
    val max38 = "9" * 28 + "." + "9" * 10
    check(
      columns,
      Seq(
        Seq(dec("0"), dec("0"), dec("0"), dec("0"), dec("0"), dec("0"), dec("0"), null),
        Seq(
          dec("9999999.99"),
          dec("99999999999999.9999"),
          dec(max38),
          dec("999999.999"),
          dec("9999999999999.99999"),
          dec("9" * 38),
          dec("9"),
          dec("0.9999999999")
        ),
        Seq(
          dec("-9999999.99"),
          dec("-99999999999999.9999"),
          dec("-" + max38),
          dec("-999999.999"),
          dec("-9999999999999.99999"),
          dec("-" + "9" * 38),
          dec("-9"),
          dec("-0.0000000001")
        ),
        Seq(
          dec("0.01"),
          dec("-0.0001"),
          dec("0.0000000001"),
          dec("0.001"),
          dec("0.00001"),
          dec("1"),
          dec("1"),
          dec("0.0000000001")
        ),
        Seq(
          int32LE(Int.MaxValue),
          int64LE(Long.MaxValue),
          le(BigInteger.ONE.shiftLeft(127).subtract(BigInteger.ONE), 16),
          int32LE(1000000000),
          int64LE(Long.MinValue),
          le(bigInt("1" + "0" * 38), 16),
          int32LE(10),
          notNull(int64LE(10000000000L))
        )
      )
    )
  }

  test("Date and Date32 around 2011-12-30 in Pacific/Apia, the day it skipped") {
    val days = Seq("2011-12-28", "2011-12-29", "2011-12-30", "2011-12-31", "2012-01-01")
    val columns = Seq("d" -> "Date", "d32" -> "Date32", "n" -> "Nullable(Date32)")
    val rows = days.map(d => Seq(date(d), date(d), date(d))) ++ Seq(
      Seq(date("1970-01-01"), date("1900-01-01"), null),
      Seq(LocalDate.ofEpochDay(65535), date("2299-12-31"), date("1969-12-31")),
      Seq(Raw(Array[Byte](-1, -1)), int32LE(Int.MinValue), notNull(int32LE(Int.MaxValue)))
    )
    for (timeZone <- Seq(apia, "UTC", "America/Sao_Paulo")) {
      val outcome = check(columns, rows, serverTimeZone = timeZone)
      if (timeZone == apia) {
        assert(outcome.rows(2).getInt(0) === outcome.rows(3).getInt(0))
        assert(outcome.rows(2).getInt(1) === date("2011-12-31").toEpochDay)
      }
    }
    // the use_time_zone client option instead of the server's
    check(columns, rows, settings = useTimeZone(apia))
  }

  test("DateTime and DateTime64 at every scale, before and after 1970") {
    val instants = Seq(
      "1970-01-01T00:00:00Z",
      "1969-12-31T23:59:59.999999999Z",
      "1900-01-01T00:00:00.000000001Z",
      "2262-04-11T23:47:16.854775807Z",
      "2024-02-29T12:34:56.789012345Z",
      "1960-06-15T08:09:10.5Z"
    )
      .map(Instant.parse)
    for (scale <- 0 to 9) {
      val columns =
        Seq("t" -> "DateTime", "t64" -> s"DateTime64($scale)", "z" -> s"Nullable(DateTime64($scale, 'Asia/Tokyo'))")
      val rows = instants.map { i =>
        Seq(Instant.ofEpochSecond(math.max(i.getEpochSecond, 0L) % 4294967296L), i, i)
      } ++ Seq(
        Seq(int32LE(-1), int64LE(-1), null),
        Seq(int32LE(Int.MaxValue), int64LE(1), notNull(int64LE(-123456789L)))
      )
      check(columns, rows)
    }
  }

  test("DateTime64 values client-v2 can't convert without failing or saturating stay with client-v2") {
    val columns = Seq("id" -> "Int32", "t" -> "Nullable(DateTime64(0))")
    for (scale <- Seq(0, 3, 6, 7, 9); ticks <- Seq(Long.MinValue, Long.MaxValue, 9000000000000L * 1000000 + 1)) {
      val typed = columns.map { case (n, t) => n -> t.replace("(0)", s"($scale)") }
      val rows = (1 to 5).map(i => Seq(i, notNull(int64LE(if (i == 3) ticks else i.toLong))))
      val beyondMicros = scale <= 6 && BigInt(ticks).abs > BigInt(9000000000000L) * BigInt(10).pow(scale)
      check(typed, rows, handsOver = beyondMicros)
    }
  }

  test("strings: ASCII, non-ASCII, invalid UTF-8, empty and longer than the read buffer") {
    val invalid = Seq(
      Array(0xff, 0xfe),
      Array(0xc3),
      Array(0xe2, 0x82),
      Array(0xed, 0xa0, 0x80),
      Array(0xc0, 0xaf),
      Array(0xf4, 0x90, 0x80, 0x80),
      Array(0x6f, 0x6b, 0x80, 0xc3, 0xa9)
    ).map(_.map(_.toByte))
    val long = new Random(5).alphanumeric.take(200000).mkString
    val columns = Seq("s" -> "String", "lc" -> "LowCardinality(Nullable(String))", "n" -> "Nullable(String)")
    val rows = Seq(
      Seq("", "", ""),
      Seq("a", null, null),
      Seq("héllo wörld", "日本語", "😀 emoji 𝄞"),
      Seq(long, long + "é", "x")
    ) ++ invalid.map(b => Seq(stringBytes(b), notNull(stringBytes(b)), notNull(stringBytes(b))))
    check(columns, rows)
  }

  test("FixedString as binary and as string") {
    val columns = Seq("f" -> "FixedString(4)", "n" -> "Nullable(FixedString(4))")
    val rows = Seq(
      Seq("abcd", null),
      Seq("ab", "é"),
      Seq(Raw(Array(0xff, 0x00, 0x41, 0xc3).map(_.toByte)), notNull(Raw("€x".getBytes(StandardCharsets.UTF_8))))
    )
    for (dataType <- Seq(BinaryType, StringType))
      check(columns, rows, schema = Some(StructType(columns.map(c => StructField(c._1, dataType)))))
  }

  test("UUID, IPv4 and IPv6, including IPv4-mapped IPv6") {
    val columns = Seq("u" -> "UUID", "v4" -> "IPv4", "v6" -> "IPv6", "n6" -> "Nullable(IPv6)")
    val random = new Random(1)
    val rows = Seq(
      Seq(new UUID(0, 0), ip4("0.0.0.0"), ip6("::"), null),
      Seq(new UUID(-1, -1), ip4("255.255.255.255"), ip6("::1"), notNull(ip6("::ffff:1.2.3.4"))),
      Seq(
        UUID.fromString("123e4567-e89b-12d3-a456-426614174000"),
        ip4("1.2.3.4"),
        ip6("::ffff:1.2.3.4"),
        notNull(ip6("2001:db8::1"))
      ),
      Seq(
        new UUID(Long.MinValue, Long.MaxValue),
        ip4("10.0.0.255"),
        ip6("fe80::1:2:3:4"),
        notNull(ip6("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"))
      )
    ) ++ (1 to 50).map { _ =>
      val mapped = "::ffff:" + Seq.fill(4)(random.nextInt(256)).mkString(".")
      Seq(
        new UUID(random.nextLong(), random.nextLong()),
        int32LE(random.nextInt()),
        Raw(Array.fill(16)(random.nextInt(256).toByte)),
        notNull(ip6(mapped))
      )
    }
    check(columns, rows)
  }

  test("enums, including values without a name") {
    val columns = Seq(
      "e8" -> "Enum8('a' = 1, 'b' = -128, 'c' = 127, 'é' = 2)",
      "e16" -> "Enum16('x' = -32768, 'y' = 32767, 'z' = 1000)",
      "n" -> "Nullable(Enum8('a' = 1))"
    )
    val rows = Seq(
      Seq("a", "x", "a"),
      Seq("b", "y", null),
      Seq("c", "z", Raw(Array[Byte](0, 5))),
      Seq("é", 0, null),
      Seq(5, -1, null),
      Seq(-127, 32766, null)
    )
    check(columns, rows)
  }

  test("Nullable(Nothing)") {
    check(
      Seq("id" -> "Int32", "n" -> "Nullable(Nothing)"),
      Seq(Seq(1, null), Seq(2, Raw(Array[Byte](0))), Seq(3, null))
    )
  }

  test("arrays: empty, nested, nullable items and every primitive item type") {
    val columns = Seq(
      "i32" -> "Array(Int32)",
      "ni32" -> "Array(Nullable(Int32))",
      "nested" -> "Array(Array(String))",
      "deep" -> "Array(Array(Array(Int8)))",
      "b" -> "Array(Bool)",
      "u8" -> "Array(UInt8)",
      "u16" -> "Array(UInt16)",
      "u32" -> "Array(UInt32)",
      "f" -> "Array(Float32)",
      "d" -> "Array(Float64)",
      "date" -> "Array(Date)",
      "t" -> "Array(DateTime64(9))",
      "dec" -> "Array(Decimal(18, 2))",
      "tuple" -> "Array(Tuple(Int32, Nullable(String)))",
      "lc" -> "Array(LowCardinality(String))",
      "e" -> "Array(Enum8('a' = 1, 'b' = 2))",
      "nothing" -> "Array(Nullable(Nothing))"
    )
    val empty = Seq.fill(columns.size)(Seq.empty)
    val full = Seq(
      Seq(Int.MinValue, 0, Int.MaxValue),
      Seq(null, 1, null),
      Seq(Seq("a", ""), Seq.empty, Seq("ü")),
      Seq(Seq(Seq(1, -1), Seq.empty), Seq.empty),
      Seq(true, false, Raw(Array[Byte](7))),
      Seq(0, 255),
      Seq(0, 65535),
      Seq(0L, 4294967295L),
      Seq(1.5f, int32LE(0x7fc00001), -0f),
      Seq(Double.NaN, -0d, Double.MaxValue),
      Seq(date("2011-12-30"), date("1970-01-01")),
      Seq(Instant.parse("1969-12-31T23:59:59.999999999Z"), Instant.parse("2024-01-01T00:00:00Z")),
      Seq(dec("1.25"), dec("-99999999999999.99")),
      Seq(Seq(1, null), Seq(2, "two")),
      Seq("x", "y", "x"),
      Seq("a", "b", 3),
      Seq(null, null)
    )
    check(columns, Seq(empty, full, full, empty), serverTimeZone = apia)
  }

  test("maps keep client-v2's duplicate handling and today's entry order") {
    val many = Entries((1 to 300).map(i => s"key$i" -> i) ++ (1 to 300 by 7).map(i => s"key$i" -> -i): _*)
    check(
      Seq("m" -> "Map(String, Int32)", "n" -> "Map(String, Nullable(String))"),
      Seq(
        Seq(Entries(), Entries()),
        Seq(Entries("a" -> 1, "b" -> 2, "a" -> 3), Entries("x" -> null, "x" -> "y", "z" -> null)),
        Seq(many, Entries("only" -> "one")),
        Seq(Entries("b" -> 1, "a" -> 2, "c" -> 3, "a" -> 4, "b" -> 5), Entries())
      )
    )
  }

  test("map keys of every kind ClickHouse allows, merged as client-v2 merges them") {
    val i256 = bigInt("12345678901234567890123456789012345678")
    val cases: Seq[(String, Entries)] = Seq(
      "Int32" -> Entries(1 -> "a", 2 -> "b", 1 -> "c"),
      "UInt64" -> Entries(-1L -> "max", 0L -> "zero", -1L -> "again"),
      "Int256" -> Entries(i256 -> "a", i256.add(BigInteger.ONE) -> "b", i256 -> "c"),
      "Float64" -> Entries(Double.NaN -> "nan", int64LE(0x7ff8000000000001L) -> "nan2", 0d -> "zero", -0d -> "neg"),
      "Float32" -> Entries(Float.NaN -> "nan", 0f -> "zero", -0f -> "neg", 1f -> "one"),
      "Decimal(10, 2)" -> Entries(dec("1.10") -> "a", dec("1.1") -> "b", int64LE(Long.MaxValue) -> "c"),
      "Bool" -> Entries(true -> "t", false -> "f", Raw(Array[Byte](2)) -> "two"),
      "Date" -> Entries(date("2011-12-30") -> "skipped", date("2011-12-31") -> "next", date("2011-12-29") -> "before"),
      "Date32" -> Entries(date("2011-12-30") -> "skipped", date("2011-12-31") -> "next"),
      "DateTime" -> Entries(Instant.ofEpochSecond(1) -> "a", Instant.ofEpochSecond(1) -> "b"),
      "DateTime64(9)" -> Entries(int64LE(1) -> "1ns", int64LE(2) -> "2ns", int64LE(1000) -> "1us", int64LE(1) -> "x"),
      "DateTime64(3)" -> Entries(int64LE(1) -> "a", int64LE(1) -> "b", int64LE(-1) -> "c"),
      "UUID" -> Entries(new UUID(1, 2) -> "a", new UUID(1, 2) -> "b", new UUID(2, 1) -> "c"),
      "IPv4" -> Entries(ip4("1.2.3.4") -> "a", ip4("4.3.2.1") -> "b", ip4("1.2.3.4") -> "c"),
      "IPv6" -> Entries(ip6("::1") -> "a", ip6("::ffff:1.2.3.4") -> "b", ip6("::1") -> "c"),
      "Enum8('a' = 1, 'b' = 2)" -> Entries("a" -> "1", "b" -> "2", "a" -> "3", 9 -> "unknown", 8 -> "unknown too"),
      "LowCardinality(String)" -> Entries("x" -> "1", "y" -> "2", "x" -> "3"),
      // both keys decode to the same String
      "FixedString(2)" -> Entries(
        Raw(Array(0xff, 0x41).map(_.toByte)) -> "a",
        Raw(Array(0xfe, 0x41).map(_.toByte)) -> "b"
      )
    )
    for ((keyType, entries) <- cases) {
      val keySparkType =
        if (keyType.startsWith("FixedString")) StringType else schemaOf(Seq("k" -> keyType)).head.dataType
      val schema = StructType(Seq(StructField("m", MapType(keySparkType, StringType, valueContainsNull = false))))
      check(
        Seq("m" -> s"Map($keyType, String)"),
        Seq(Seq(entries), Seq(Entries())),
        Some(schema),
        serverTimeZone = apia
      )
    }
  }

  test("maps nested in maps, arrays and tuples, and nullable tuples") {
    check(
      Seq(
        "mm" -> "Map(String, Map(String, Int32))",
        "ma" -> "Map(LowCardinality(String), Array(Nullable(Int32)))",
        "tm" -> "Tuple(a Map(Int32, String), b Array(Int64))",
        "nt" -> "Nullable(Tuple(Int32, String))"
      ),
      Seq(
        Seq(
          Entries("a" -> Entries("x" -> 1, "x" -> 2), "a" -> Entries()),
          Entries("k" -> Seq(1, null)),
          Seq(Entries(1 -> "a", 1 -> "b"), Seq(1L)),
          null
        ),
        Seq(Entries(), Entries(), Seq(Entries(), Seq.empty), Seq(1, "one"))
      )
    )
  }

  test("tuples, named and unnamed, and nested") {
    check(
      Seq(
        "t" -> "Tuple(Int32, String)",
        "named" -> "Tuple(a Int32, b Nullable(String), c Array(UInt8))",
        "nested" -> "Tuple(x Tuple(y Int64, z Decimal(9, 3)), w Map(String, UUID))"
      ),
      Seq(
        Seq(Seq(1, "a"), Seq(2, null, Seq(1, 2)), Seq(Seq(3L, dec("1.5")), Entries("u" -> new UUID(1, 1)))),
        Seq(Seq(-1, ""), Seq(0, "b", Seq.empty), Seq(Seq(Long.MinValue, dec("-999999.999")), Entries()))
      )
    )
  }

  test("metadata columns keep the raw bits: _part_index UInt64 as Long, _shard_num UInt32 as Int") {
    check(
      Seq("_part_index" -> "UInt64", "_shard_num" -> "UInt32"),
      Seq(Seq(0L, 0L), Seq(-1L, 4294967295L), Seq(Long.MaxValue, 2147483648L)),
      schema = Some(StructType(Seq(StructField("_part_index", LongType), StructField("_shard_num", IntegerType))))
    )
  }

  test("client-v2 reads Array(Map) and Array(IPv6) itself, failing on mixed items as today") {
    val columns = Seq("am" -> "Array(Map(String, Int32))", "a6" -> "Array(IPv6)")
    check(columns, Seq(Seq(Seq(Entries("a" -> 1), Entries("b" -> 2)), Seq(ip6("::1"), ip6("::2")))), handsOver = true)
    val mixedMaps = check(columns, Seq(Seq(Seq(Entries("a" -> 1), Entries()), Seq(ip6("::1")))), handsOver = true)
    assert(mixedMaps.error.isDefined)
    val mixedIps = check(columns, Seq(Seq(Seq.empty, Seq(ip6("::1"), ip6("::ffff:1.2.3.4")))), handsOver = true)
    assert(mixedIps.error.isDefined)
  }

  test("types and pairings it doesn't decode stay with client-v2, failing or not as today") {
    // intervals fail in today's decode
    val interval = check(Seq("i" -> "IntervalDay"), Seq(Seq(Period.ofDays(3))), handsOver = true)
    assert(interval.error.exists(_.contains("Unsupported catalyst type")))
    val unparseable = assertSame(
      variants(header(Seq("x" -> "NotAType")) ++ Array[Byte](1)),
      StructType(Seq(StructField("x", IntegerType))),
      Some(true)
    )
    assert(unparseable.error.isDefined)
    // SELECT 1 when every column is pruned
    check(Seq("1" -> "UInt8"), Seq(Seq(1), Seq(1)), schema = Some(new StructType()), handsOver = true)
    // pairings SchemaUtils doesn't produce
    check(Seq("u" -> "UInt8"), Seq(Seq(1)), Some(StructType(Seq(StructField("u", BooleanType)))), handsOver = true)
    check(
      Seq("d" -> "Decimal(10, 2)"),
      Seq(Seq(dec("1.25"))),
      Some(StructType(Seq(StructField("d", DecimalType(10, 4))))),
      handsOver = true
    )
    val noTimeZone = check(Seq("i" -> "Int32"), Seq(Seq(1)), settings = Map.empty, handsOver = true)
    assert(noTimeZone.error.exists(_.contains("Time zone is not set")))
    assertSame(variants(Array.emptyByteArray), StructType(Seq(StructField("i", IntegerType))), Some(true))
  }

  private val mixedColumns = Seq(
    "id" -> "UInt64",
    "s" -> "Nullable(String)",
    "a" -> "Array(Int32)",
    "m" -> "Map(String, Int64)",
    "d" -> "Decimal(18, 4)",
    "t" -> "DateTime64(3)"
  )

  private def mixedRows(n: Int, seed: Long = 3): Seq[Seq[Any]] = {
    val random = new Random(seed)
    (0 until n).map { i =>
      Seq(
        i.toLong,
        if (i % 5 == 0) null else random.alphanumeric.take(random.nextInt(20)).mkString,
        Seq.fill(random.nextInt(4))(random.nextInt()),
        Entries(Seq.fill(random.nextInt(3))(random.nextInt(3).toString -> random.nextLong()): _*),
        JBigDecimal.valueOf(random.nextLong() % 100000000000000L, 4),
        Instant.ofEpochMilli(random.nextLong() % 4000000000000L)
      )
    }
  }

  test("a stream that ends inside a row: a clean end in the first column, an error after it") {
    val bytes = encode(mixedColumns, mixedRows(6))
    val endings = (0 to bytes.length).map { cut =>
      val truncated = Arrays.copyOf(bytes, cut)
      val outcome = assertSame(variants(truncated).take(2), schemaOf(mixedColumns), None)
      outcome.error.getOrElse(s"${outcome.rows.size} rows")
    }
    assert(endings.exists(_.contains("Failed to read next row")))
    assert(endings.exists(_.endsWith(" rows")))
  }

  test("a compressed stream cut or failing at any byte ends or fails after the same rows") {
    val compressed = lz4(encode(mixedColumns, mixedRows(30)), blockSize = 256)
    val failure = new IOException("connection reset")
    for (cut <- 0 to compressed.length) {
      val bytes = Arrays.copyOf(compressed, cut)
      val streams = Seq(
        "cut" -> (() => unlz4(new ByteArrayInputStream(bytes))),
        "failing" -> (() => unlz4(new FailingStream(bytes, failure)))
      )
      assertSame(streams, schemaOf(mixedColumns), None)
    }
  }

  test("a stream that fails instead of ending fails after the same rows") {
    val bytes = encode(mixedColumns, mixedRows(4))
    for (cut <- 0 to bytes.length) {
      val failure = new IOException("connection reset")
      val stream = () => new FailingStream(Arrays.copyOf(bytes, cut), failure)
      val outcome = assertSame(Seq("failing" -> stream), schemaOf(mixedColumns), Some(true))
      assert(outcome.error.exists(_.contains("connection reset")), s"cut at $cut")
    }
  }

  test("negative and oversized lengths stay with client-v2") {
    val columns = Seq("id" -> "Int32", "s" -> "String", "a" -> "Array(Int32)", "m" -> "Map(String, Int32)")
    val negative = Raw(Array(0xff, 0xff, 0xff, 0xff, 0x0f).map(_.toByte))
    val oversized = Raw(Array(0x80, 0x80, 0x80, 0x01).map(_.toByte)) // 2 MiB, more than is left
    for (bad <- Seq(negative, oversized); column <- 1 to 3) {
      val row = Seq(1, "s", Seq(1), Entries("k" -> 1)).updated(column, bad)
      check(columns, Seq(Seq(0, "", Seq.empty, Entries()), row, Seq(2, "", Seq.empty, Entries())), handsOver = true)
    }
  }

  test("many rows of mixed types") {
    val outcome = check(mixedColumns, mixedRows(20000, seed = 11))
    assert(outcome.rows.size === 20000)
  }

  test("a reused row clears a column's null flag when its next value isn't null, for every type") {
    val columns = Seq(
      "b" -> "Nullable(Bool)",
      "i8" -> "Nullable(Int8)",
      "u8" -> "Nullable(UInt8)",
      "i16" -> "Nullable(Int16)",
      "u16" -> "Nullable(UInt16)",
      "i32" -> "Nullable(Int32)",
      "u32" -> "Nullable(UInt32)",
      "i64" -> "Nullable(Int64)",
      "u64" -> "Nullable(UInt64)",
      "i128" -> "Nullable(Int128)",
      "f32" -> "Nullable(Float32)",
      "f64" -> "Nullable(Float64)",
      "d9" -> "Nullable(Decimal(9, 2))",
      "d38" -> "Nullable(Decimal(38, 10))",
      "date" -> "Nullable(Date)",
      "date32" -> "Nullable(Date32)",
      "dt" -> "Nullable(DateTime)",
      "dt64" -> "Nullable(DateTime64(6))",
      "s" -> "Nullable(String)",
      "lc" -> "LowCardinality(Nullable(String))",
      "fs" -> "Nullable(FixedString(2))",
      "uuid" -> "Nullable(UUID)",
      "v4" -> "Nullable(IPv4)",
      "v6" -> "Nullable(IPv6)",
      "e" -> "Nullable(Enum8('a' = 1))",
      "t" -> "Nullable(Tuple(Int32, String))"
    )
    val values = Seq[Any](
      true,
      1,
      1,
      1,
      1,
      1,
      1L,
      1L,
      1L,
      BigInteger.ONE,
      1.5f,
      2.5d,
      dec("1.25"),
      dec("1.5"),
      date("2024-01-01"),
      date("1900-01-01"),
      Instant.ofEpochSecond(1),
      Instant.ofEpochSecond(1, 1000),
      "x",
      "y",
      "ab",
      new UUID(1, 2),
      ip4("1.2.3.4"),
      notNull(ip6("::1")),
      "a",
      Seq(1, "one")
    )
    // the decoder fills its two rows in turn, so each gets null, a value, null, a value
    val nulls = Seq(true, true, false, false, true, true, false, false)
    val outcome = check(columns, nulls.map(isNull => if (isNull) Seq.fill(columns.size)(null) else values))
    for ((isNull, r) <- nulls.zipWithIndex; c <- columns.indices)
      assert(outcome.rows(r).isNullAt(c) === isNull, s"row $r, ${columns(c)._1}")
  }

  test("values taken from a returned row stay intact while later rows are read") {
    val columns = Seq(
      "s" -> "String",
      "n" -> "Nullable(String)",
      "lc" -> "LowCardinality(String)",
      "fb" -> "FixedString(3)",
      "fs" -> "FixedString(3)",
      "as" -> "Array(String)",
      "an" -> "Array(Nullable(String))",
      "aa" -> "Array(Array(String))",
      "m" -> "Map(String, String)",
      "ma" -> "Map(String, Array(String))",
      "t" -> "Tuple(String, FixedString(2))",
      "u" -> "UUID",
      "v4" -> "IPv4",
      "v6" -> "IPv6",
      "e" -> "Enum8('a' = 1, 'b' = 2)",
      "i128" -> "Int128",
      "d" -> "Decimal(38, 10)"
    )
    val schema = StructType(schemaOf(columns).map(f => if (f.name == "fs") f.copy(dataType = StringType) else f))
    val random = new Random(7)
    def text(): String = random.alphanumeric.take(random.nextInt(30)).mkString + (if (random.nextBoolean()) "é" else "")
    def bytes(n: Int): Raw = Raw(Array.fill(n)(random.nextInt(256).toByte))
    // rows 500, 1500 and 2500 outgrow the read buffer
    val rows = (0 until 3000).map { i =>
      Seq(
        if (i % 1000 == 500) "x" * 150000 else text(),
        if (i % 3 == 0) null else text(),
        text(),
        bytes(3),
        bytes(3),
        Seq.fill(random.nextInt(4))(text()),
        Seq.fill(random.nextInt(4))(if (random.nextBoolean()) null else text()),
        Seq.fill(random.nextInt(3))(Seq.fill(random.nextInt(3))(text())),
        Entries(Seq.fill(random.nextInt(3))(text() -> text()): _*),
        Entries(Seq.fill(random.nextInt(3))(text() -> Seq.fill(random.nextInt(3))(text())): _*),
        Seq(text(), bytes(2)),
        new UUID(random.nextLong(), random.nextLong()),
        int32LE(random.nextInt()),
        bytes(16),
        if (random.nextBoolean()) "a" else "b",
        BigInteger.valueOf(random.nextLong()).shiftLeft(random.nextInt(60)),
        JBigDecimal.valueOf(random.nextLong(), 10)
      )
    }
    for ((label, stream) <- variants(encode(columns, rows))) {
      val r = reader(response(stream()), schema)
      // the values themselves, not copies, as an operator holding them would keep them
      val held = ArrayBuffer.empty[Array[Any]]
      while (r.next()) {
        val row = r.get
        held += Array.tabulate(schema.length)(i => if (row.isNullAt(i)) null else row.get(i, schema(i).dataType))
      }
      assert(!r.rowDecoder.handedOver, label)
      val projection = UnsafeProjection.create(schema)
      val main = readLikeMain(response(stream()), schema)
      assert(main.error.isEmpty, label)
      assert(held.map(values => projection(new GenericInternalRow(values)).copy()).toList === main.rows, label)
    }
  }

  test("a row handed to client-v2 mid-stream is neither skipped nor repeated, wherever it falls") {
    val columns = Seq(
      "id" -> "Int32",
      "s" -> "String",
      "t" -> "Nullable(DateTime64(3))",
      "a" -> "Array(String)",
      "m" -> "Map(String, Int64)"
    )
    val n = 3000
    val random = new Random(9)
    def text(max: Int): String = random.alphanumeric.take(random.nextInt(max)).mkString
    val base = (0 until n).map { i =>
      Seq[Any](
        i,
        text(60),
        if (i % 4 == 0) null else Instant.ofEpochMilli(random.nextLong() % 4000000000000L),
        Seq.fill(random.nextInt(4))(text(10)),
        Entries(Seq.fill(random.nextInt(3))(text(3) -> random.nextLong()): _*)
      )
    }
    // client-v2 converts it; past the microseconds Spark holds, so the decoder hands over
    val beyondMicros = notNull(int64LE(9000000000000L * 1000 + 1))
    // the value after other columns of its row, or first in it
    val layouts = Seq(Seq(0, 1, 2, 3, 4), Seq(2, 0, 1, 3, 4))
    val badRows = Seq(0, 1, 2, 3, 517, 1499, 1500, 2222, n - 2, n - 1)
    for (layout <- layouts; bad <- badRows) {
      val rows = base.zipWithIndex.map { case (row, i) =>
        layout.map(if (i == bad) row.updated(2, beyondMicros) else row)
      }
      val outcome = check(layout.map(columns), rows, handsOver = true)
      assert(outcome.error.isEmpty, s"bad row $bad")
      assert(outcome.rows.map(_.getInt(layout.indexOf(0))) === (0 until n), s"bad row $bad")
    }
  }

  test("a stream that breaks anywhere, compressed or not, fails instead of ending") {
    val rows = mixedRows(3000, seed = 5)
    val bytes = encode(mixedColumns, rows)
    val compressed = lz4(bytes)
    val random = new Random(13)
    val boundaries = (0 to rows.size by 97).map(k => encode(mixedColumns, rows.take(k)).length)
    // every byte of the header and first rows, some row boundaries, random points through the rest, and the end
    def cuts(length: Int): Seq[Int] =
      ((0 to 300) ++ boundaries.filter(_ <= length) ++ Seq.fill(150)(random.nextInt(length + 1)) :+ length).distinct
    val failure = new IOException("connection reset")
    def assertFails(label: String, stream: () => InputStream): Unit = {
      val outcome = assertSame(Seq(label -> stream), schemaOf(mixedColumns), None)
      assert(outcome.error.exists(_.contains("connection reset")), label)
    }
    for (cut <- cuts(bytes.length)) {
      val head = Arrays.copyOf(bytes, cut)
      assertFails(s"plain, cut at $cut", () => new FailingStream(head, failure))
      assertFails(s"one byte per read, cut at $cut", () => new Trickle(new FailingStream(head, failure), 1))
    }
    for (cut <- cuts(compressed.length)) {
      val head = Arrays.copyOf(compressed, cut)
      assertFails(s"lz4, cut at $cut", () => unlz4(new FailingStream(head, failure)))
    }
  }

  test("a column of a type it doesn't decode sends the whole scan to client-v2") {
    val types = Seq(
      "JSON",
      "IntervalYear",
      "IntervalMonth",
      "IntervalDay",
      "IntervalHour",
      "IntervalMinute",
      "IntervalSecond",
      "Array(Nothing)",
      "Array(IPv6)",
      "Array(Array(IPv6))",
      "Array(Map(String, Int32))",
      "Map(FixedString(2), String)",
      "Map(String, Array(IPv6))",
      "Tuple(a Int32, b Array(Map(String, Int32)))"
    )
    for (chType <- types) {
      val columns = Seq("id" -> "Int32", "c" -> chType)
      val parsed = columns.map { case (name, t) => ClickHouseColumn.of(name, t) }.toArray
      assert(RowBinaryDecoder.decoders(parsed, schemaOf(columns), TimeZone.getTimeZone("UTC")).isEmpty, chType)
    }
  }

  test("DecodedRow reads back like SpecificInternalRow") {
    val types = Array[DataType](
      BooleanType,
      ByteType,
      ShortType,
      IntegerType,
      DateType,
      LongType,
      TimestampType,
      FloatType,
      DoubleType,
      StringType,
      DecimalType(10, 2),
      BinaryType,
      NullType,
      ArrayType(IntegerType)
    )
    val values = Seq[Any](
      true,
      -1.toByte,
      Short.MinValue,
      Int.MaxValue,
      -719162,
      Long.MinValue,
      1L,
      java.lang.Float.intBitsToFloat(0x7fc00001),
      -0d,
      org.apache.spark.unsafe.types.UTF8String.fromString("é"),
      org.apache.spark.sql.types.Decimal(12345L, 10, 2),
      Array[Byte](1, 2),
      null,
      org.apache.spark.sql.catalyst.util.ArrayData.toArrayData(Array(1, 2))
    )
    val decoded = new DecodedRow(types)
    val specific = new org.apache.spark.sql.catalyst.expressions.SpecificInternalRow(types.toSeq)
    val projection = UnsafeProjection.create(types)
    for (nullAt <- -1 until types.length) {
      types.indices.foreach { i =>
        val value = if (i == nullAt) null else values(i)
        decoded.update(i, value)
        specific.update(i, value)
      }
      types.indices.foreach { i =>
        assert(decoded.isNullAt(i) === specific.isNullAt(i))
        assert(java.util.Objects.equals(decoded.get(i, types(i)), specific.get(i, types(i))), s"field $i")
      }
      assert(projection(decoded).copy() === projection(specific).copy())
      assert(decoded.copy() === specific.copy())
      assert(decoded.toString === specific.toString)
    }
    decoded.setFloat(7, 1.5f)
    decoded.setDouble(8, Double.NaN)
    assert(decoded.getFloat(7) === 1.5f && decoded.getDouble(8).isNaN && !decoded.isNullAt(8))
  }
}
