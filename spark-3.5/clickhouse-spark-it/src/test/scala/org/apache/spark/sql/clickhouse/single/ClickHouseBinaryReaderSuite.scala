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

package org.apache.spark.sql.clickhouse.single

import com.clickhouse.spark.base.{ClickHouseCloudMixIn, ClickHouseSingleMixIn}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.tags.Cloud

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._

@Cloud
class ClickHouseCloudBinaryReaderSuite extends ClickHouseBinaryReaderSuite with ClickHouseCloudMixIn

class ClickHouseSingleBinaryReaderSuite extends ClickHouseBinaryReaderSuite with ClickHouseSingleMixIn

/**
 * Test suite for ClickHouse Binary Reader.
 * Uses binary format for reading data from ClickHouse.
 * All test cases are inherited from ClickHouseReaderTestBase.
 */
abstract class ClickHouseBinaryReaderSuite extends ClickHouseReaderTestBase {

  // Override to use binary format for reading
  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.clickhouse.read.format", "binary")
    .set("spark.clickhouse.write.format", "arrow")
    // the same server with session_timezone Pacific/Apia, which ClickHouse reports as the response's time zone
    .setAll(super.sparkConf.getAllWithPrefix("spark.sql.catalog.clickhouse.").map { case (k, v) =>
      s"spark.sql.catalog.clickhouse_apia.$k" -> v
    })
    .set("spark.sql.catalog.clickhouse_apia", "com.clickhouse.spark.ClickHouseCatalog")
    .set("spark.sql.catalog.clickhouse_apia.option.clickhouse_setting_session_timezone", "Pacific/Apia")

  // All tests are inherited from ClickHouseReaderTestBase
  // Additional binary-specific tests can be added here if needed

  test("binary: map entries come back deduplicated and ordered as before") {
    withKVTable("test_db", "test_map_order", valueColDef = "Map(String, Int32)") { (db, tbl) =>
      val maps = Seq(
        Seq("a" -> 1, "b" -> 2, "a" -> 3),
        (1 to 40).map(i => s"k$i" -> i) ++ Seq("k7" -> -7, "k1" -> -1),
        Seq.empty
      )
      val values = maps.zipWithIndex.map { case (entries, i) =>
        s"($i, map(${entries.map { case (k, v) => s"'$k', $v" }.mkString(", ")}))"
      }
      runClickHouseSQL(s"INSERT INTO $db.$tbl VALUES ${values.mkString(", ")}")

      // client-v2 keeps a duplicate key's first position and last value; the Spark map is then built from a
      // Scala map of the decoded entries, whose iteration order is what Spark gets
      def expected(entries: Seq[(String, Int)]): Seq[(String, Int)] = {
        val javaMap = new java.util.LinkedHashMap[AnyRef, AnyRef]()
        entries.foreach { case (k, v) => javaMap.put(UTF8String.fromString(k), Int.box(v)) }
        javaMap.asScala.map { case (k, v) => (k, v) }.toSeq.map { case (k, v) => (k.toString, v.asInstanceOf[Int]) }
      }
      val result = spark.sql(s"SELECT key, map_entries(value) FROM $db.$tbl ORDER BY key").collect()
      assert(result.map(_.getSeq[Row](1).map(e => (e.getString(0), e.getInt(1)))).toSeq === maps.map(expected))
    }
  }

  test("binary: Date and Date32 read in the server time zone, where 2011-12-30 in Pacific/Apia is 12-31") {
    withKVTable("test_db", "test_date_apia", valueColDef = "Tuple(d Date, d32 Date32)") { (db, tbl) =>
      runClickHouseSQL(
        s"""INSERT INTO $db.$tbl VALUES
           |(1, ('2011-12-29', '2011-12-29')),
           |(2, ('2011-12-30', '2011-12-30')),
           |(3, ('2011-12-31', '2011-12-31')),
           |(4, ('1970-01-01', '1900-01-01'))
           |""".stripMargin
      )
      def read(catalog: String): Seq[(String, String)] =
        spark.sql(s"SELECT CAST(value.d AS STRING), CAST(value.d32 AS STRING) FROM $catalog.$db.$tbl ORDER BY key")
          .collect().map(r => (r.getString(0), r.getString(1))).toSeq
      assert(read("clickhouse_apia") === Seq(
        ("2011-12-29", "2011-12-29"),
        ("2011-12-31", "2011-12-31"),
        ("2011-12-31", "2011-12-31"),
        ("1970-01-01", "1900-01-01")
      ))
      assert(read("clickhouse").map(_._1) === Seq("2011-12-29", "2011-12-30", "2011-12-31", "1970-01-01"))
    }
  }

  test("binary: DateTime64 ticks become microseconds, rounding down below a microsecond and before 1970") {
    withKVTable(
      "test_db",
      "test_datetime64_ticks",
      valueColDef = "Tuple(ms DateTime64(3, 'UTC'), us DateTime64(6, 'UTC'), ns DateTime64(9, 'UTC'))"
    ) { (db, tbl) =>
      val ticks = Seq((-1L, -1L, -1L), (1L, 1L, 1999L), (-2208988800000L, -2208988800000000L, -2208988799999999999L))
      val values = ticks.zipWithIndex.map { case ((ms, us, ns), i) =>
        s"($i, (fromUnixTimestamp64Milli($ms), fromUnixTimestamp64Micro($us), fromUnixTimestamp64Nano($ns)))"
      }
      runClickHouseSQL(s"INSERT INTO $db.$tbl VALUES ${values.mkString(", ")}")
      val result = spark.sql(
        s"SELECT unix_micros(value.ms), unix_micros(value.us), unix_micros(value.ns) FROM $db.$tbl ORDER BY key"
      ).collect().map(r => (r.getLong(0), r.getLong(1), r.getLong(2))).toSeq
      assert(result === Seq(
        (-1000L, -1L, -1L),
        (1000L, 1L, 1L),
        (-2208988800000000L, -2208988800000000L, -2208988800000000L)
      ))
    }
  }

  test("binary: invalid UTF-8 reads with replacement characters, as client-v2 decodes it") {
    withKVTable("test_db", "test_invalid_utf8", valueColDef = "String") { (db, tbl) =>
      val hex = Seq("FF41", "C328", "E282", "EDA080", "68C3A96C6C6F", "")
      runClickHouseSQL(s"INSERT INTO $db.$tbl VALUES ${hex.zipWithIndex.map { case (h, i) => s"($i, unhex('$h'))" }
          .mkString(", ")}")
      val expected =
        hex.map(h => new String(h.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray, StandardCharsets.UTF_8))
      val result = spark.sql(s"SELECT value FROM $db.$tbl ORDER BY key").collect().map(_.getString(0)).toSeq
      assert(result === expected)
    }
  }

  test("binary: 128- and 256-bit integers read as before, with every digit") {
    withKVTable("test_db", "test_wide_ints", valueColDef = "Tuple(i Int128, u UInt256)") { (db, tbl) =>
      runClickHouseSQL(
        s"""INSERT INTO $db.$tbl VALUES
           |(1, (-170141183460469231731687303715884105728, 0)),
           |(2, (12345678901234567890123456789012345, 12345678901234567890123456789012346)),
           |(3, (9999999999999999999999999999999999, 99999999999999999999999999999999999999)),
           |(4, (1234567890123456789012345678901234, 115792089237316195423570985008687907853269984665640564039457584007913129639935))
           |""".stripMargin
      )
      val strings = spark.sql(s"SELECT CAST(value.i AS STRING), CAST(value.u AS STRING) FROM $db.$tbl ORDER BY key")
        .collect().map(r => (r.getString(0), r.getString(1))).toSeq
      assert(strings === Seq(
        ("-170141183460469231731687303715884105728", "0"),
        ("12345678901234567890123456789012345", "12345678901234567890123456789012346"),
        ("9999999999999999999999999999999999", "99999999999999999999999999999999999999"),
        (
          "1234567890123456789012345678901234",
          "115792089237316195423570985008687907853269984665640564039457584007913129639935"
        )
      ))
    }
  }

  test("binary: _part_index reads as ClickHouse reports it") {
    withKVTable("test_db", "test_part_index", valueColDef = "String") { (db, tbl) =>
      runClickHouseSQL(s"INSERT INTO $db.$tbl VALUES (1, 'a')")
      runClickHouseSQL(s"INSERT INTO $db.$tbl VALUES (2, 'b')")
      val mapper = new ObjectMapper()
      val expected = runClickHouseSQL(s"SELECT key, _part_index FROM $db.$tbl ORDER BY key").collect().map { r =>
        val node = mapper.readTree(r.getString(0))
        (node.get("key").asInt, node.get("_part_index").asText.toLong)
      }.toSeq
      val result = spark.sql(s"SELECT key, _part_index FROM $db.$tbl ORDER BY key").collect()
        .map(r => (r.getInt(0), r.getLong(1))).toSeq
      assert(result === expected)
    }
  }

  test("binary: arrays of maps still read through client-v2") {
    withKVTable("test_db", "test_array_of_maps", valueColDef = "Array(Map(String, Int32))") { (db, tbl) =>
      runClickHouseSQL(s"INSERT INTO $db.$tbl VALUES (1, [map('a', 1, 'b', 2), map('c', 3)]), (2, [])")
      val result = spark.sql(s"SELECT key, value FROM $db.$tbl ORDER BY key").collect()
      assert(result(0).getSeq[Map[String, Int]](1) === Seq(Map("a" -> 1, "b" -> 2), Map("c" -> 3)))
      assert(result(1).getSeq[Map[String, Int]](1).isEmpty)
    }
  }

  test("binary: interval columns still fail to decode, as before") {
    val db = if (useSuiteLevelDatabase) testDatabaseName else "test_db"
    val view = "test_interval_view"
    try {
      if (!useSuiteLevelDatabase) runClickHouseSQL(s"CREATE DATABASE IF NOT EXISTS `$db`")
      runClickHouseSQL(s"CREATE VIEW `$db`.`$view` AS SELECT number AS key, toIntervalDay(number) AS d FROM numbers(3)")
      if (isCloud) Thread.sleep(1000)
      val e = intercept[Exception](spark.sql(s"SELECT key, d FROM $db.$view").collect())
      assert(Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null).exists(
        _.getMessage.contains("Unsupported catalyst type d[DayTimeIntervalType(0,0)]")
      ))
    } finally {
      runClickHouseSQL(s"DROP VIEW IF EXISTS `$db`.`$view`")
      if (!useSuiteLevelDatabase) runClickHouseSQL(s"DROP DATABASE IF EXISTS `$db`")
    }
  }

  test("binary: JSON columns still read through client-v2, together with the other columns") {
    withKVTable("test_db", "test_json_type", valueColDef = "JSON") { (db, tbl) =>
      runClickHouseSQL(s"""INSERT INTO $db.$tbl VALUES (1, '{"name": "Alice", "age": 30}'), (2, '{}')""")
      val result = spark.sql(s"SELECT key, value FROM $db.$tbl ORDER BY key").collect()
      // client-v2 returns a java.util.Map, which today's decode turns into its toString
      assert(result.map(r => (r.getInt(0), r.getString(1))).toSeq === Seq((1, "{name=Alice, age=30}"), (2, "{}")))
    }
  }
}
