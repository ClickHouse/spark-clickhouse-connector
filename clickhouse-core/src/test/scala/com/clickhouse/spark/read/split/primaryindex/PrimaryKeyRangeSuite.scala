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

package com.clickhouse.spark.read.split.primaryindex

import com.clickhouse.spark.expr.PushedFilter
import com.clickhouse.spark.read.split.primaryindex.KeyType._
import com.clickhouse.spark.read.split.primaryindex.PrimaryKeyRangePlanner.{cutsQuery, splittableKeyPrefix}
import com.clickhouse.spark.spec.TableSpec
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDateTime

class PrimaryKeyRangeSuite extends AnyFunSuite {

  test("a single-entry range compares the entry with each bound") {
    assert(PrimaryKeyRange(Seq("`id`"), None, None).filterExpr === "1=1")
    assert(PrimaryKeyRange(Seq("`id`"), Some(Seq("10")), None).filterExpr === "(`id` >= 10)")
    assert(PrimaryKeyRange(Seq("`id`"), None, Some(Seq("20"))).filterExpr === "(`id` < 20)")
    assert(PrimaryKeyRange(Seq("`id`"), Some(Seq("10")), Some(Seq("20"))).filterExpr ===
      "(`id` >= 10) AND (`id` < 20)")
  }

  test("a multi-entry range compares the entries lexicographically, expressions in parentheses") {
    assert(PrimaryKeyRange(Seq("`a`", "`b`"), Some(Seq("1", "2")), None).filterExpr ===
      "(`a` > 1 OR (`a` = 1 AND `b` >= 2))")
    assert(PrimaryKeyRange(Seq("`a`", "`b`", "`c`"), Some(Seq("1", "2", "3")), Some(Seq("4", "5", "6"))).filterExpr ===
      "(`a` > 1 OR (`a` = 1 AND (`b` > 2 OR (`b` = 2 AND `c` >= 3)))) AND " +
      "(`a` < 4 OR (`a` = 4 AND (`b` < 5 OR (`b` = 5 AND `c` < 6))))")
    val hour = "toDateTime('2024-01-05 10:00:00', 'UTC')"
    assert(PrimaryKeyRange(Seq("(toStartOfHour(ts))", "`id`"), Some(Seq(hour, "7")), None).filterExpr ===
      s"((toStartOfHour(ts)) > $hour OR ((toStartOfHour(ts)) = $hour AND `id` >= 7))")
  }

  test("ranges are cut on integer, date, date time, string and UUID keys") {
    Seq("Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64").foreach { t =>
      assert(KeyType.of(t) === Some(IntegerKey), t)
    }
    Seq("Date", "Date32", "UUID").foreach(t => assert(KeyType.of(t) === Some(StringFormKey), t))
    assert(KeyType.of("DateTime") === Some(DateTimeKey(None)))
    assert(KeyType.of("DateTime('Asia/Kolkata')") === Some(DateTimeKey(None)))
    assert(KeyType.of("DateTime64(3)") === Some(DateTimeKey(Some(3))))
    assert(KeyType.of("DateTime64(6, 'Asia/Kolkata')") === Some(DateTimeKey(Some(6))))
    Seq("String", "FixedString(16)", "LowCardinality(String)", "LowCardinality(FixedString(2))").foreach { t =>
      assert(KeyType.of(t) === Some(BytesKey), t)
    }
    assert(KeyType.of("LowCardinality(UInt32)") === Some(IntegerKey))
  }

  test("ranges are not cut on types with values outside the order or inexact literals") {
    Seq(
      "Nullable(Int32)",
      "LowCardinality(Nullable(String))",
      "Float32",
      "Float64",
      "Int128",
      "UInt256",
      "Decimal(10, 2)",
      "Bool",
      "Enum8('a' = 1)",
      "Array(Int32)",
      "Tuple(Int32, String)"
    ).foreach(t => assert(KeyType.of(t) === None, t))
  }

  test("cut values are written back as exact literals") {
    assert(IntegerKey.literal("-42") === "-42")
    assert(IntegerKey.literal("18446744073709551615") === "18446744073709551615")
    intercept[IllegalArgumentException](IntegerKey.literal("1 OR 1=1"))
    assert(StringFormKey.literal("2024-01-01") === "'2024-01-01'")
    assert(StringFormKey.literal("61f0c404-5cb3-11e7-907b-a6006ad3dba0") === "'61f0c404-5cb3-11e7-907b-a6006ad3dba0'")
    assert(DateTimeKey(None).literal("2024-01-01 10:00:00") === "toDateTime('2024-01-01 10:00:00', 'UTC')")
    assert(DateTimeKey(Some(3)).literal("2024-01-01 10:00:00.123") ===
      "toDateTime64('2024-01-01 10:00:00.123', 3, 'UTC')")
  }

  test("string cut values keep every byte") {
    def hex(bytes: Int*): String = bytes.map(b => f"$b%02X").mkString
    assert(BytesKey.literal(hex('a', 'b')) === "'ab'")
    assert(BytesKey.literal("") === "''")
    assert(BytesKey.literal(hex('i', 't', '\'', 's', '\\')) === "'it\\'s\\\\'")
    // é in UTF-8, a zero byte, and a byte that is not UTF-8 at all
    assert(BytesKey.literal(hex(0xc3, 0xa9, 0x00, 0xff)) === "'\\xC3\\xA9\\x00\\xFF'")
    intercept[IllegalArgumentException](BytesKey.literal("ABC"))
    intercept[IllegalArgumentException](BytesKey.literal("0G"))
  }

  private val index = Seq("part_name" -> "String", "mark_number" -> "UInt64", "rows_in_granule" -> "UInt64")
  private val columns = Map(
    "tenant" -> "String",
    "ts" -> "DateTime",
    "id" -> "UInt64",
    "score" -> "Float64",
    "event time" -> "DateTime",
    "s" -> "String"
  )
  private val hourOfTs = Monotonic("ts", "DateTime", "toStartOfHour(", ")")

  test("the split key is the primary key prefix of supported types, columns and expressions alike") {
    val key = Seq("tenant", "ts", "id")
    assert(splittableKeyPrefix(
      index ++ Seq("tenant" -> "String", "ts" -> "DateTime", "id" -> "UInt64"),
      key,
      columns
    ) ===
      Seq(
        ColumnEntry("tenant", BytesKey),
        ColumnEntry("ts", DateTimeKey(None)),
        ColumnEntry("id", IntegerKey)
      ))
    val byHour = Seq("toStartOfHour(ts)", "id")
    assert(splittableKeyPrefix(index ++ Seq("toStartOfHour(ts)" -> "DateTime", "id" -> "UInt64"), byHour, columns) ===
      Seq(
        MonotonicEntry("toStartOfHour(ts)", "(toStartOfHour(ts))", DateTimeKey(None), hourOfTs),
        ColumnEntry("id", IntegerKey)
      ))
    assert(splittableKeyPrefix(index ++ Seq("cityHash64(id)" -> "UInt64"), Seq("cityHash64(id)"), columns) ===
      Seq(OpaqueEntry("cityHash64(id)", "(cityHash64(id))", IntegerKey)))
    assert(splittableKeyPrefix(
      index ++ Seq("id" -> "UInt64", "score" -> "Float64", "ts" -> "DateTime"),
      key,
      columns
    ) ===
      Seq(ColumnEntry("id", IntegerKey)))
    assert(splittableKeyPrefix(index ++ Seq("score" -> "Nullable(Int32)"), Seq("score"), columns).isEmpty)
    assert(splittableKeyPrefix(index, Nil, columns).isEmpty)
  }

  test("an expression is spelled as ClickHouse formats the primary key, the index naming it unquoted") {
    // mergeTreeIndex calls the column `toStartOfHour(event time)`; only the primary key's spelling is valid SQL
    val entries = splittableKeyPrefix(
      index ++ Seq("toStartOfHour(event time)" -> "DateTime", "id" -> "UInt64"),
      Seq("toStartOfHour(`event time`)", "id"),
      columns
    )
    assert(entries.map(_.sql) === Seq("(toStartOfHour(`event time`))", "`id`"))
    // an operator that binds looser than a comparison stays one operand of a range bound
    val flags = splittableKeyPrefix(index ++ Seq("and(a, b)" -> "UInt8"), Seq("a AND b"), columns)
    assert(PrimaryKeyRange(flags.map(_.sql), Some(Seq("1")), None).filterExpr === "((a AND b) >= 1)")
    assert(entries.head.identifier === "`toStartOfHour(event time)`")
    assert(entries.head === MonotonicEntry(
      "toStartOfHour(event time)",
      "(toStartOfHour(`event time`))",
      DateTimeKey(None),
      Monotonic("event time", "DateTime", "toStartOfHour(", ")")
    ))
    // a String-typed function result follows string order, so a filter on its column can't place the cuts
    assert(splittableKeyPrefix(index ++ Seq("toString(ts)" -> "String"), Seq("toString(ts)"), columns) ===
      Seq(OpaqueEntry("toString(ts)", "(toString(ts))", BytesKey)))
  }

  test("a monotonic time function of one column is recognised with its constant arguments") {
    assert(Monotonic.of("toStartOfHour(ts)", columns) === Some(hourOfTs))
    assert(Monotonic.of("toStartOfHour(`ts`)", columns) === Some(hourOfTs))
    assert(Monotonic.of("toYYYYMM(ts)", columns) === Some(Monotonic("ts", "DateTime", "toYYYYMM(", ")")))
    assert(Monotonic.of("date_trunc('hour', ts, 'UTC')", columns) ===
      Some(Monotonic("ts", "DateTime", "date_trunc('hour', ", ", 'UTC')")))
    assert(Monotonic.of("toStartOfInterval(ts, toIntervalHour(1))", columns) ===
      Some(Monotonic("ts", "DateTime", "toStartOfInterval(", ", toIntervalHour(1))")))
    assert(Monotonic.of("toStartOfDay(ts, 'Europe/Paris')", columns) ===
      Some(Monotonic("ts", "DateTime", "toStartOfDay(", ", 'Europe/Paris')")))
    // periodic, not a call, two columns, no column, an unknown column, unbalanced
    assert(Monotonic.of("toHour(ts)", columns) === None)
    assert(Monotonic.of("toStartOfHour(ts) + 1", columns) === None)
    assert(Monotonic.of("plus(toStartOfHour(ts), 1)", columns) === None)
    assert(Monotonic.of("toStartOfInterval(ts, id)", columns) === None)
    assert(Monotonic.of("toStartOfHour(now())", columns) === None)
    assert(Monotonic.of("toStartOfHour(other)", columns) === None)
    assert(Monotonic.of("toStartOfHour(ts))", columns) === None)
    // a String column orders as text, not as time; month-level shifts of a DateTime are not monotonic either
    assert(Monotonic.of("toDateTime(s)", columns) === None)
    assert(Monotonic.of("addMonths(ts, 1)", columns) === None)
    assert(Monotonic.of("addDays(ts, 1)", columns) === Some(Monotonic("ts", "DateTime", "addDays(", ", 1)")))
    assert(hourOfTs("'2024-01-05 00:00:00'") === "toStartOfHour(CAST('2024-01-05 00:00:00' AS DateTime))")
  }

  test("cut points are read from the index granules the key filters keep") {
    val key = Seq(
      ColumnEntry("tenant", BytesKey),
      ColumnEntry("ts", DateTimeKey(None))
    )
    val filters = Seq(
      PushedFilter.Compare("tenant", "=", "'a'"),
      PushedFilter.Compare("ts", ">=", "'2024-02-01 00:00:00'"),
      PushedFilter.Other("(`tenant` = 'b') OR (`ts` < '2024-01-01 00:00:00')", Set("tenant", "ts")),
      PushedFilter.Compare("value", ">", "1"),
      PushedFilter.Other("`tenant` = `value`", Set("tenant", "value"))
    )
    assert(cutsQuery(tableSpec, key, filters, settings) ===
      """SELECT hex(`tenant`), toString(`ts`, 'UTC')
        |FROM (
        |  SELECT `tenant`, `ts`,
        |    sum(rows_in_granule) OVER (ORDER BY `tenant`, `ts` ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        |      - rows_in_granule AS _rows_before,
        |    intDiv(_rows_before * greatest(6, toUInt64(ceil(sum(rows_in_granule) OVER () / 4000000))), sum(rows_in_granule) OVER ()) AS _slice
        |  FROM mergeTreeIndex('db', 'tbl')
        |  WHERE rows_in_granule > 0 AND ((`tenant` = 'a')) AND ((`ts` >= '2024-02-01 00:00:00') OR `ts` = defaultValueOfArgumentType(`ts`)) AND (((`tenant` = 'b') OR (`ts` < '2024-01-01 00:00:00')) OR `ts` = defaultValueOfArgumentType(`ts`))
        |)
        |ORDER BY _rows_before
        |LIMIT 1 BY _slice
        |""".stripMargin)
    // a maximum caps what the rows the filters keep give
    assert(cutsQuery(tableSpec, key, filters, settings.copy(maxParallelism = Some(4))).contains(
      "intDiv(_rows_before * least(4, greatest(6, toUInt64(ceil(sum(rows_in_granule) OVER () / 4000000)))), " +
        "sum(rows_in_granule) OVER ()) AS _slice"
    ))
  }

  test("a filter on the column of a monotonic time function places the cuts, strict comparisons relaxed") {
    val key = Seq(
      MonotonicEntry("toStartOfHour(ts)", "toStartOfHour(ts)", DateTimeKey(None), hourOfTs),
      ColumnEntry("id", IntegerKey)
    )
    val filters = Seq(
      PushedFilter.Compare("ts", ">=", "'2024-01-05 00:00:00'"),
      PushedFilter.Compare("ts", "<", "'2024-01-06 00:00:00'"),
      PushedFilter.In("ts", Seq("'2024-01-07 00:00:00'")),
      PushedFilter.Compare("id", ">", "5"),
      PushedFilter.Other("`ts` IS NOT NULL", Set("ts"))
    )
    assert(cutsQuery(tableSpec, key, filters, settings) ===
      """SELECT toString(`toStartOfHour(ts)`, 'UTC'), toString(`id`)
        |FROM (
        |  SELECT `toStartOfHour(ts)`, `id`,
        |    sum(rows_in_granule) OVER (ORDER BY `toStartOfHour(ts)`, `id` ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        |      - rows_in_granule AS _rows_before,
        |    intDiv(_rows_before * greatest(6, toUInt64(ceil(sum(rows_in_granule) OVER () / 4000000))), sum(rows_in_granule) OVER ()) AS _slice
        |  FROM mergeTreeIndex('db', 'tbl')
        |  WHERE rows_in_granule > 0 AND ((`toStartOfHour(ts)` >= toStartOfHour(CAST('2024-01-05 00:00:00' AS DateTime)))) AND ((`toStartOfHour(ts)` <= toStartOfHour(CAST('2024-01-06 00:00:00' AS DateTime)))) AND ((`toStartOfHour(ts)` IN (toStartOfHour(CAST('2024-01-07 00:00:00' AS DateTime))))) AND ((`id` > 5) OR `id` = defaultValueOfArgumentType(`id`))
        |)
        |ORDER BY _rows_before
        |LIMIT 1 BY _slice
        |""".stripMargin)
  }

  private val settings = PrimaryIndexSplitSettings(atLeast = 6, targetRowsPerTask = 4000000, maxParallelism = None)

  private val tableSpec = TableSpec(
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
    engine_full = "MergeTree ORDER BY (tenant, ts)",
    partition_key = "",
    sorting_key = "tenant, ts",
    primary_key = "tenant, ts",
    sampling_key = "",
    storage_policy = "",
    total_rows = None,
    total_bytes = None,
    lifetime_rows = None,
    lifetime_bytes = None
  )
}
