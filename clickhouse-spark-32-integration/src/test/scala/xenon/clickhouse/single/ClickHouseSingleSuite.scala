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

package xenon.clickhouse.single

import java.sql.Timestamp

import org.apache.spark.sql.types._
import org.apache.spark.sql.QueryTest._
import org.apache.spark.sql.Row
import xenon.clickhouse.{BaseSparkSuite, Logging}
import xenon.clickhouse.base.ClickHouseSingleMixIn

class ClickHouseSingleSuite extends BaseSparkSuite
    with ClickHouseSingleMixIn
    with SparkClickHouseSingleMixin
    with SparkClickHouseSingleTestHelper
    with Logging {

  import spark.implicits._

  test("clickhouse command runner") {
    checkAnswer(
      runClickHouseSQL("SELECT visibleWidth(NULL)"),
      Row("""{"visibleWidth(NULL)":"4"}""") :: Nil
    )
  }

  test("clickhouse catalog") {
    try {
      spark.sql("CREATE DATABASE db_t1")
      spark.sql("CREATE DATABASE db_t2")
      checkAnswer(
        spark.sql("SHOW DATABASES LIKE 'db_t*'"),
        Row("db_t1") :: Row("db_t2") :: Nil
      )
      spark.sql("USE system")
      checkAnswer(
        spark.sql("SELECT current_database()"),
        Row("system") :: Nil
      )
      assert(spark.sql("SHOW tables").where($"tableName" === "contributors").count === 1)
    } finally {
      runClickHouseSQL(s"DROP DATABASE IF EXISTS db_t1")
      runClickHouseSQL(s"DROP DATABASE IF EXISTS db_t2")
    }
  }

  test("clickhouse partition") {
    val db = "db_part"
    val tbl = "tbl_part"

    // DROP + PURGE
    withSimpleTable(db, tbl, true) {
      checkAnswer(
        spark.sql(s"SHOW PARTITIONS $db.$tbl"),
        Seq(Row("m=1"), Row("m=2"))
      )
      checkAnswer(
        spark.sql(s"SHOW PARTITIONS $db.$tbl PARTITION(m = 2)"),
        Seq(Row("m=2"))
      )

      spark.sql(s"ALTER TABLE $db.$tbl DROP PARTITION(m = 2)")
      checkAnswer(
        spark.sql(s"SHOW PARTITIONS $db.$tbl"),
        Seq(Row("m=1"))
      )

      spark.sql(s"ALTER TABLE $db.$tbl DROP PARTITION(m = 1) PURGE")
      checkAnswer(
        spark.sql(s"SHOW PARTITIONS $db.$tbl"),
        Seq()
      )
    }

    // DROP + TRUNCATE
    withSimpleTable(db, tbl, true) {
      checkAnswer(
        spark.sql(s"SHOW PARTITIONS $db.$tbl"),
        Seq(Row("m=1"), Row("m=2"))
      )
      checkAnswer(
        spark.sql(s"SHOW PARTITIONS $db.$tbl PARTITION(m = 2)"),
        Seq(Row("m=2"))
      )

      spark.sql(s"ALTER TABLE $db.$tbl DROP PARTITION(m = 2)")
      checkAnswer(
        spark.sql(s"SHOW PARTITIONS $db.$tbl"),
        Seq(Row("m=1"))
      )

      spark.sql(s"TRUNCATE TABLE $db.$tbl PARTITION(m = 1)")
      checkAnswer(
        spark.sql(s"SHOW PARTITIONS $db.$tbl"),
        Seq()
      )
    }
  }

  test("clickhouse multi part columns") {
    val db = "db_multi_part_col"
    val tbl = "tbl_multi_part_col"
    val schema =
      StructType(
        StructField("id", LongType, false) ::
          StructField("value", StringType, false) ::
          StructField("part_1", StringType, false) ::
          StructField("part_2", IntegerType, false) :: Nil
      )
    withTable(db, tbl, schema, partKeys = Seq("part_1", "part_2")) {
      spark.sql(
        s"""INSERT INTO `$db`.`$tbl`
           |VALUES
           |  (11L, 'one_one', '1', 1),
           |  (12L, 'one_two', '1', 2) AS tab(id, value, part_1, part_2)
           |""".stripMargin
      )

      spark.createDataFrame(Seq(
        (21L, "two_one", "2", 1),
        (22L, "two_two", "2", 2)
      ))
        .toDF("id", "value", "part_1", "part_2")
        .writeTo(s"$db.$tbl").append

      checkAnswer(
        spark.table(s"$db.$tbl").orderBy($"id"),
        Row(11L, "one_one", "1", 1) ::
          Row(12L, "one_two", "1", 2) ::
          Row(21L, "two_one", "2", 1) ::
          Row(22L, "two_two", "2", 2) :: Nil
      )

      checkAnswer(
        spark.sql(s"SHOW PARTITIONS $db.$tbl"),
        Seq(
          Row("part_1=1/part_2=1"),
          Row("part_1=1/part_2=2"),
          Row("part_1=2/part_2=1"),
          Row("part_1=2/part_2=2")
        )
      )
    }
  }

  test("clickhouse multi sort columns") {
    val db = "db_multi_sort_col"
    val tbl = "tbl_multi_sort_col"
    val schema =
      StructType(
        StructField("id", LongType, false) ::
          StructField("value", StringType, false) ::
          StructField("sort_2", StringType, false) ::
          StructField("sort_3", IntegerType, false) :: Nil
      )
    withTable(db, tbl, schema, sortKeys = Seq("sort_2", "sort_3")) {
      spark.sql(
        s"""INSERT INTO `$db`.`$tbl`
           |VALUES
           |  (11L, 'one_one', '1', 1),
           |  (12L, 'one_two', '1', 2) AS tab(id, value, sort_2, sort_3)
           |""".stripMargin
      )

      spark.createDataFrame(Seq(
        (21L, "two_one", "2", 1),
        (22L, "two_two", "2", 2)
      ))
        .toDF("id", "value", "sort_2", "sort_3")
        .writeTo(s"$db.$tbl").append

      checkAnswer(
        spark.table(s"$db.$tbl").orderBy($"id"),
        Row(11L, "one_one", "1", 1) ::
          Row(12L, "one_two", "1", 2) ::
          Row(21L, "two_one", "2", 1) ::
          Row(22L, "two_two", "2", 2) :: Nil
      )
    }
  }

  test("clickhouse truncate table") {
    withClickHouseSingleIdTable("db_trunc", "tbl_trunc") { (db, tbl) =>
      spark.range(10).toDF("id").writeTo(s"$db.$tbl").append
      assert(spark.table(s"$db.$tbl").count == 10)
      spark.sql(s"TRUNCATE TABLE $db.$tbl")
      assert(spark.table(s"$db.$tbl").count == 0)
    }
  }

  test("clickhouse write then read") {
    val db = "db_rw"
    val tbl = "tbl_rw"

    withSimpleTable(db, tbl, true) {
      val tblSchema = spark.table(s"$db.$tbl").schema
      assert(tblSchema == StructType(
        StructField("id", DataTypes.LongType, false) ::
          StructField("value", DataTypes.StringType, true) ::
          StructField("create_time", DataTypes.TimestampType, false) ::
          StructField("m", DataTypes.IntegerType, false) :: Nil
      ))

      checkAnswer(
        spark.table(s"$db.$tbl").sort("m"),
        Seq(
          Row(1L, "1", Timestamp.valueOf("2021-01-01 10:10:10"), 1),
          Row(2L, "2", Timestamp.valueOf("2022-02-02 10:10:10"), 2)
        )
      )

      checkAnswer(
        spark.table(s"$db.$tbl").filter($"id" > 1),
        Row(2L, "2", Timestamp.valueOf("2022-02-02 10:10:10"), 2) :: Nil
      )

      assert(spark.table(s"$db.$tbl").filter($"id" > 1).count === 1)

      // infiniteLoop()
    }
  }

  test("clickhouse metadata column") {
    val db = "db_metadata_col"
    val tbl = "tbl_metadata_col"

    withSimpleTable(db, tbl, true) {
      checkAnswer(
        spark.sql(s"SELECT m, _partition_id FROM $db.$tbl ORDER BY m"),
        Seq(
          Row(1, "1"),
          Row(2, "2")
        )
      )
    }
  }

  test("push down aggregation") {
    val db = "db_agg_col"
    val tbl = "tbl_agg_col"

    withSimpleTable(db, tbl, true) {
      checkAnswer(
        spark.sql(s"SELECT COUNT(id) FROM $db.$tbl"),
        Seq(Row(2))
      )

      checkAnswer(
        spark.sql(s"SELECT MIN(id) FROM $db.$tbl"),
        Seq(Row(1))
      )

      checkAnswer(
        spark.sql(s"SELECT MAX(id) FROM $db.$tbl"),
        Seq(Row(2))
      )

      checkAnswer(
        spark.sql(s"SELECT m, COUNT(DISTINCT id) FROM $db.$tbl GROUP BY m"),
        Seq(
          Row(1, 1),
          Row(2, 1)
        )
      )

      checkAnswer(
        spark.sql(s"SELECT m, SUM(DISTINCT id) FROM $db.$tbl GROUP BY m"),
        Seq(
          Row(1, 1),
          Row(2, 2)
        )
      )
    }
  }

  test("create or replace table") {
    autoCleanupTable("db_cor", "tbl_cor") { (db, tbl) =>
      def createOrReplaceTable(): Unit = spark.sql(
        s"""CREATE TABLE IF NOT EXISTS `$db`.`$tbl` (
           |  id Long NOT NULL
           |) USING ClickHouse
           |TBLPROPERTIES (
           |  engine = 'MergeTree()',
           |  order_by = '(id)',
           |  settings.index_granularity = 8192
           |)
           |""".stripMargin
      )
      createOrReplaceTable()
      createOrReplaceTable()
    }
  }
}
