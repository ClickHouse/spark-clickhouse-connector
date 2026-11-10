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

package com.clickhouse.spark.spec

import com.clickhouse.spark.expr.PushedFilter
import com.clickhouse.spark.spec.PrimaryKeyRange._
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDateTime

class PrimaryKeyRangeSuite extends AnyFunSuite {

  test("a single-column range compares the column with each bound") {
    assert(PrimaryKeyRange(Seq("id"), None, None).filterExpr === "1=1")
    assert(PrimaryKeyRange(Seq("id"), Some(Seq("10")), None).filterExpr === "(`id` >= 10)")
    assert(PrimaryKeyRange(Seq("id"), None, Some(Seq("20"))).filterExpr === "(`id` < 20)")
    assert(PrimaryKeyRange(Seq("id"), Some(Seq("10")), Some(Seq("20"))).filterExpr === "(`id` >= 10) AND (`id` < 20)")
  }

  test("a multi-column range compares the columns lexicographically") {
    assert(PrimaryKeyRange(Seq("a", "b"), Some(Seq("1", "2")), None).filterExpr ===
      "(`a` > 1 OR (`a` = 1 AND `b` >= 2))")
    assert(PrimaryKeyRange(Seq("a", "b", "c"), Some(Seq("1", "2", "3")), Some(Seq("4", "5", "6"))).filterExpr ===
      "(`a` > 1 OR (`a` = 1 AND (`b` > 2 OR (`b` = 2 AND `c` >= 3)))) AND " +
      "(`a` < 4 OR (`a` = 4 AND (`b` < 5 OR (`b` = 5 AND `c` < 6))))")
  }

  test("ranges are cut on integer, date, date time, string and UUID keys") {
    Seq("Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64").foreach { t =>
      assert(keyType(t) === Some(IntegerKey), t)
    }
    Seq("Date", "Date32", "UUID").foreach(t => assert(keyType(t) === Some(StringFormKey), t))
    assert(keyType("DateTime") === Some(DateTimeKey(None)))
    assert(keyType("DateTime('Asia/Kolkata')") === Some(DateTimeKey(None)))
    assert(keyType("DateTime64(3)") === Some(DateTimeKey(Some(3))))
    assert(keyType("DateTime64(6, 'Asia/Kolkata')") === Some(DateTimeKey(Some(6))))
    Seq("String", "FixedString(16)", "LowCardinality(String)", "LowCardinality(FixedString(2))").foreach { t =>
      assert(keyType(t) === Some(BytesKey), t)
    }
    assert(keyType("LowCardinality(UInt32)") === Some(IntegerKey))
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
    ).foreach(t => assert(keyType(t) === None, t))
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

  test("the split key is the primary key prefix up to the first expression or unsupported type") {
    val index = Seq("part_name" -> "String", "mark_number" -> "UInt64", "rows_in_granule" -> "UInt64")
    val columns = Set("tenant", "ts", "id", "score")
    assert(splitColumns(index ++ Seq("tenant" -> "String", "ts" -> "DateTime", "id" -> "UInt64"), columns) ===
      Seq("tenant" -> BytesKey, "ts" -> DateTimeKey(None), "id" -> IntegerKey))
    assert(splitColumns(index ++ Seq("tenant" -> "String", "toStartOfHour(ts)" -> "DateTime"), columns) ===
      Seq("tenant" -> BytesKey))
    assert(splitColumns(index ++ Seq("id" -> "UInt64", "score" -> "Float64", "ts" -> "DateTime"), columns) ===
      Seq("id" -> IntegerKey))
    assert(splitColumns(index ++ Seq("toStartOfHour(ts)" -> "DateTime", "id" -> "UInt64"), columns).isEmpty)
    assert(splitColumns(index ++ Seq("score" -> "Nullable(Int32)"), columns).isEmpty)
    assert(splitColumns(index, columns).isEmpty)
  }

  test("cut points are read from the index granules the key filters keep") {
    val key = Seq("tenant" -> BytesKey, "ts" -> DateTimeKey(None))
    val filters = Seq(
      PushedFilter("`tenant` = 'a'", Set("tenant")),
      PushedFilter("`ts` >= '2024-02-01 00:00:00'", Set("ts")),
      PushedFilter("(`tenant` = 'b') OR (`ts` < '2024-01-01 00:00:00')", Set("tenant", "ts")),
      PushedFilter("`value` > 1", Set("value")),
      PushedFilter("`tenant` = `value`", Set("tenant", "value"))
    )
    assert(cutsQuery(tableSpec, key, filters, 4) ===
      """SELECT hex(`tenant`), toString(`ts`, 'UTC')
        |FROM (
        |  SELECT `tenant`, `ts`,
        |    sum(rows_in_granule) OVER (ORDER BY `tenant`, `ts` ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        |      - rows_in_granule AS _rows_before,
        |    intDiv(_rows_before * 4, sum(rows_in_granule) OVER ()) AS _slice
        |  FROM mergeTreeIndex('db', 'tbl')
        |  WHERE rows_in_granule > 0 AND ((`tenant` = 'a')) AND ((`ts` >= '2024-02-01 00:00:00') OR `ts` = defaultValueOfArgumentType(`ts`)) AND (((`tenant` = 'b') OR (`ts` < '2024-01-01 00:00:00')) OR `ts` = defaultValueOfArgumentType(`ts`))
        |)
        |ORDER BY _rows_before
        |LIMIT 1 BY _slice
        |""".stripMargin)
  }

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
