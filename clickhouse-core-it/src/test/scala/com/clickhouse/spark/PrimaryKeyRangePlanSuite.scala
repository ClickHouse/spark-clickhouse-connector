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

package com.clickhouse.spark

import com.clickhouse.spark.base.{ClickHouseCloudMixIn, ClickHouseProvider, ClickHouseSingleMixIn}
import com.clickhouse.spark.client.NodeClient
import com.clickhouse.spark.expr.PushedFilter
import com.clickhouse.spark.spec.{NotSplittable, PrimaryIndexSplitSettings, PrimaryKeyRange, TableSpec}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.tags.Cloud

import java.time.LocalDateTime
import java.util.UUID

@Cloud
class ClickHouseCloudPrimaryKeyRangePlanSuite extends PrimaryKeyRangePlanSuite with ClickHouseCloudMixIn

class ClickHouseSinglePrimaryKeyRangePlanSuite extends PrimaryKeyRangePlanSuite with ClickHouseSingleMixIn

abstract class PrimaryKeyRangePlanSuite extends AnyFunSuite with ClickHouseProvider {

  private val granule = 64
  private val rows = 20000
  private val noCutPoint = "the primary index yields no cut point within the filters"

  Seq(
    "Int32" -> s"toInt32(number) - ${rows / 2}",
    "Int64" -> "-9223372036854775808 + number",
    "UInt64" -> "18446744073709551615 - number",
    "Date" -> "toDate('2000-01-01') + number",
    "Date32" -> "toDate32('1900-01-01') + number",
    "DateTime" -> "toDateTime('2024-01-01 00:00:00', 'UTC') + number * 3600",
    // span the hour the column's time zone repeats when DST ends
    "DateTime('America/New_York')" -> "toDateTime('2024-11-03 04:30:00', 'UTC') + number",
    "DateTime64(6, 'America/New_York')" ->
      "toDateTime64('2024-11-03 04:30:00', 6, 'UTC') + toIntervalMicrosecond(number * 337123)",
    // every byte value, including quotes, backslashes and bytes that are not UTF-8
    "String" -> "concat(char(number % 256), toString(number))",
    "FixedString(4)" -> "toFixedString(reinterpretAsString(toUInt32(number * 2654435761)), 4)",
    "UUID" -> "generateUUIDv4(number)",
    "LowCardinality(String)" -> "toString(number % 5000)"
  ).foreach { case (keyType, values) =>
    test(s"ranges of a $keyType key hold about as many rows each and cover every row once") {
      withTable(mergeTree(s"(k $keyType, v UInt64)", "ORDER BY k"), s"SELECT $values, number FROM numbers($rows)") {
        table =>
          val ranges = table.ranges(8)
          assert(ranges.length === 8)
          assert(ranges.forall(_.key == Seq("`k`")))
          table.assertCovers(ranges)
          ranges.foreach(range => assert(math.abs(table.count(range.filterExpr) - rows / 8) <= 2 * granule, range))
      }
    }
  }

  test("ranges cover every row once when the key changes in a high-cardinality prefix") {
    withTable(
      mergeTree("(tenant LowCardinality(String), ts DateTime, id UInt64)", "ORDER BY (tenant, ts, id)"),
      s"SELECT ['a', 'b', 'c'][1 + number % 3], toDateTime('2024-01-01 00:00:00') + number * 60, number " +
        s"FROM numbers($rows)"
    ) { table =>
      val ranges = table.ranges(8)
      assert(ranges.length === 8)
      assert(ranges.forall(_.key == Seq("`tenant`", "`ts`", "`id`")))
      table.assertCovers(ranges)
      // three tenants, yet every range holds an eighth of the rows
      ranges.foreach(range => assert(math.abs(table.count(range.filterExpr) - rows / 8) <= 2 * granule, range))
    }
  }

  test("ranges are cut within the rows the key filters keep") {
    withTable(
      mergeTree("(tenant String, ts DateTime)", "PARTITION BY toYYYYMM(ts) ORDER BY (tenant, ts)"),
      s"SELECT ['a', 'b'][1 + number % 2], toDateTime('2024-01-01 00:00:00') + number * 400 FROM numbers($rows)"
    ) { table =>
      val lastMonth = "`ts` >= '2024-03-01 00:00:00'"
      val ranges =
        table.ranges(4, PushedFilter.Compare("ts", ">=", "'2024-03-01 00:00:00'"), PushedFilter.Compare("v", ">", "0"))
      assert(ranges.length === 4)
      table.assertCovers(ranges)
      val inLastMonth = table.count(lastMonth)
      ranges.foreach { range =>
        assert(math.abs(table.count(s"(${range.filterExpr}) AND $lastMonth") - inLastMonth / 4) <= 2 * granule, range)
      }
    }
  }

  test("filters on key columns past a high-cardinality prefix still leave ranges to cut") {
    // ClickHouse 25.3+ does not load `ts` here, and mergeTreeIndex reports it as its default value
    withTable(
      mergeTree("(id UInt64, ts DateTime)", "ORDER BY (id, ts)"),
      s"SELECT number, toDateTime('2024-01-01 00:00:00') + number FROM numbers($rows)"
    ) { table =>
      val ranges = table.ranges(4, PushedFilter.Compare("ts", ">=", "'2024-01-01 01:00:00'"))
      assert(ranges.length === 4)
      table.assertCovers(ranges)
    }
  }

  test("a key expression is cut on, in parentheses") {
    withTable(
      mergeTree("(tenant String, ts DateTime)", "ORDER BY (tenant, toStartOfHour(ts))"),
      s"SELECT toString(number % 100), toDateTime('2024-01-01 00:00:00') + number FROM numbers($rows)"
    ) { table =>
      val ranges = table.ranges(4)
      assert(ranges.length === 4)
      assert(ranges.forall(_.key == Seq("`tenant`", "(toStartOfHour(ts))")))
      table.assertCovers(ranges)
    }
  }

  test("a key expression over a column that needs quoting is spelled as ClickHouse formats it") {
    withTable(
      mergeTree("(`event time` DateTime, id UInt64)", "ORDER BY (toStartOfHour(`event time`), id)"),
      s"SELECT toDateTime('2024-01-01 00:00:00') + number, number FROM numbers($rows)"
    ) { table =>
      val ranges = table.ranges(4, PushedFilter.Compare("event time", ">=", "'2024-01-01 04:00:00'"))
      assert(ranges.length === 4)
      assert(ranges.forall(_.key == Seq("(toStartOfHour(`event time`))", "`id`")))
      table.assertCovers(ranges)
    }
  }

  test("a filter on the column of a time function in the key places the cuts within the rows it keeps") {
    // a row a second for five and a half hours; the filter keeps the last hour and a half
    withTable(
      mergeTree("(ts DateTime, id UInt64)", "ORDER BY (toStartOfHour(ts), id)"),
      s"SELECT toDateTime('2024-01-01 00:00:00') + number, number FROM numbers($rows)"
    ) { table =>
      val lastHours = "`ts` >= '2024-01-01 04:00:00'"
      val ranges = table.ranges(4, PushedFilter.Compare("ts", ">=", "'2024-01-01 04:00:00'"))
      assert(ranges.length === 4)
      assert(ranges.forall(_.key == Seq("(toStartOfHour(ts))", "`id`")))
      table.assertCovers(ranges)
      val kept = table.count(lastHours)
      ranges.foreach { range =>
        assert(math.abs(table.count(s"(${range.filterExpr}) AND $lastHours") - kept / 4) <= 2 * granule, range)
      }
    }
  }

  test("a table smaller than the requested ranges gets fewer, none smaller than a granule") {
    withTable(mergeTree("(k UInt64)", "ORDER BY k"), s"SELECT number FROM numbers(${granule * 3})") { table =>
      val ranges = table.ranges(8)
      assert(ranges.length === 3)
      table.assertCovers(ranges)
      ranges.foreach(range => assert(table.count(range.filterExpr) === granule))
    }
  }

  test("a range is cut per target rows the key filters keep, no fewer than the least, no more than the maximum") {
    withTable(mergeTree("(k UInt64)", "ORDER BY k"), s"SELECT number FROM numbers($rows)") { table =>
      val target = rows / 10
      val settings = PrimaryIndexSplitSettings(atLeast = 4, targetRowsPerTask = target, maxParallelism = None)
      val ranges = table.ranges(settings)
      assert(ranges.length === 10)
      table.assertCovers(ranges)
      ranges.foreach(range => assert(math.abs(table.count(range.filterExpr) - target) <= 2 * granule, range))
      // the filter keeps half the rows
      val upperHalf = PushedFilter.Compare("k", ">=", (rows / 2).toString)
      assert(table.ranges(settings, upperHalf).length === 5)
      assert(table.ranges(settings.copy(atLeast = 16)).length === 16)
      // the maximum caps the ten the rows give, and the least when that is more
      val capped = table.ranges(settings.copy(maxParallelism = Some(6)))
      assert(capped.length === 6)
      table.assertCovers(capped)
      assert(table.ranges(settings.copy(atLeast = 16, maxParallelism = Some(6))).length === 6)
    }
  }

  test("a smallest key spanning several granules does not leave the first range empty") {
    withTable(
      mergeTree("(k UInt64)", "ORDER BY k"),
      s"SELECT if(number < ${rows / 3}, 0, number) FROM numbers($rows)"
    ) {
      table =>
        val ranges = table.ranges(8)
        table.assertCovers(ranges)
        ranges.foreach(range => assert(table.count(range.filterExpr) > 0, range))
    }
  }

  test("cut values too long for a scan query are skipped, leaving the table unsplit") {
    withTable(
      mergeTree("(k String)", "ORDER BY k"),
      s"SELECT concat(repeat('x', 2000), toString(number)) FROM numbers($rows)"
    ) { table =>
      assert(table.plan(8) === Left(NotSplittable(noCutPoint)))
    }
  }

  test("an empty table is not split") {
    withTable(mergeTree("(k UInt64)", "ORDER BY k"), "SELECT number FROM numbers(0)") { table =>
      assert(table.plan(8) === Left(NotSplittable(noCutPoint)))
    }
  }

  test("tables without a key we can cut on are not split, saying why") {
    val noSplitColumn =
      "the primary key does not start with an integer, date, date time, string or UUID column or expression"
    Seq(
      mergeTree("(k UInt64)", "ORDER BY tuple()") -> "the table has no primary key",
      mergeTree("(k Nullable(UInt64))", "ORDER BY k", "allow_nullable_key = 1") -> noSplitColumn,
      mergeTree("(k Float64)", "ORDER BY k") -> noSplitColumn,
      mergeTree("(k UInt64)", "ORDER BY toFloat64(k)") -> noSplitColumn,
      "(k UInt64) ENGINE = Memory" -> "the Memory engine has no primary index"
    ).foreach { case (ddl, reason) =>
      withTable(ddl, "SELECT 1") { table =>
        assert(table.plan(8) === Left(NotSplittable(reason)), ddl)
      }
    }
  }

  private case class Table(name: String)(implicit client: NodeClient) {

    private def records(sql: String) = client.syncQueryAndCheckOutputJSONEachRow(sql).records

    lazy val spec: TableSpec = {
      val row =
        records(s"SELECT engine, primary_key FROM system.tables WHERE database = 'default' AND name = '$name'").head
      TableSpec(
        database = "default",
        name = name,
        uuid = "",
        engine = row.get("engine").asText,
        is_temporary = false,
        data_paths = Nil,
        metadata_path = "",
        metadata_modification_time = LocalDateTime.of(2026, 1, 1, 0, 0),
        dependencies_database = Nil,
        dependencies_table = Nil,
        create_table_query = "",
        engine_full = "",
        partition_key = "",
        sorting_key = "",
        primary_key = row.get("primary_key").asText,
        sampling_key = "",
        storage_policy = "",
        total_rows = None,
        total_bytes = None,
        lifetime_rows = None,
        lifetime_bytes = None
      )
    }

    private lazy val columns: Map[String, String] =
      records(s"SELECT name, type FROM system.columns WHERE database = 'default' AND table = '$name'")
        .map(row => row.get("name").asText -> row.get("type").asText).toMap

    // `slices` as the least, with a target no table reaches: that many, data permitting
    private def sized(slices: Int): PrimaryIndexSplitSettings =
      PrimaryIndexSplitSettings(atLeast = slices, targetRowsPerTask = Long.MaxValue, maxParallelism = None)

    def plan(slices: Int, filters: PushedFilter*): Either[NotSplittable, Seq[PrimaryKeyRange]] =
      plan(sized(slices), filters: _*)

    def plan(settings: PrimaryIndexSplitSettings, filters: PushedFilter*): Either[NotSplittable, Seq[PrimaryKeyRange]] =
      PrimaryKeyRange.plan(spec, columns, filters, settings)

    // the planned ranges of a table that must be splittable
    def ranges(slices: Int, filters: PushedFilter*): Seq[PrimaryKeyRange] =
      ranges(sized(slices), filters: _*)

    def ranges(settings: PrimaryIndexSplitSettings, filters: PushedFilter*): Seq[PrimaryKeyRange] =
      plan(settings, filters: _*).fold(why => fail(s"Not split: $why"), identity)

    def count(where: String): Long = records(s"SELECT count() AS c FROM `default`.`$name` WHERE $where")
      .head.get("c").asLong

    // every row in exactly one range, checked row by row and through the primary index
    def assertCovers(ranges: Seq[PrimaryKeyRange]): Unit = {
      assert(count(ranges.map(range => s"(${range.filterExpr})").mkString("", " + ", " != 1")) === 0)
      assert(ranges.map(range => count(range.filterExpr)).sum === count("1=1"))
    }
  }

  private def mergeTree(columns: String, clauses: String, settings: String*): String =
    s"$columns ENGINE = MergeTree $clauses SETTINGS ${(s"index_granularity = $granule" +: settings).mkString(", ")}"

  /** Creates `default.<random name>` as `ddl`, fills it from `select` and drops it after `f`. */
  private def withTable(ddl: String, select: String)(f: Table => Unit): Unit = withNodeClient() { implicit client =>
    val name = s"pk_range_${UUID.randomUUID().toString.replace("-", "")}"
    client.syncQueryAndCheckOutputJSONEachRow(s"CREATE TABLE `default`.`$name` $ddl")
    if (isCloud) Thread.sleep(1000)
    try {
      client.syncQueryAndCheckOutputJSONEachRow(s"INSERT INTO `default`.`$name` $select")
      f(Table(name))
    } finally client.syncQueryAndCheckOutputJSONEachRow(s"DROP TABLE IF EXISTS `default`.`$name`")
  }
}
