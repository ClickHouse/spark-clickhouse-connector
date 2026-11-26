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
import com.clickhouse.spark.read.{ClickHouseBatchScan, ClickHouseInputPartition, ClickHouseReader}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.types._
import org.scalatest.tags.Cloud

@Cloud
class ClickHouseCloudGenericSuite extends ClickHouseGenericSuite with ClickHouseCloudMixIn

class ClickHouseSingleGenericSuite extends ClickHouseGenericSuite with ClickHouseSingleMixIn

abstract class ClickHouseGenericSuite extends SparkClickHouseSingleTest {

  import testImplicits._

  test("clickhouse command runner") {
    // Pin the JSON formatting of UInt64 (visibleWidth returns UInt64). ClickHouse
    // flipped the default of `output_format_json_quote_64bit_integers` from 1 to 0
    // around 25.7+, which changes the output from `"4"` to bare `4`.
    checkAnswer(
      runClickHouseSQL(
        "SELECT visibleWidth(NULL) SETTINGS output_format_json_quote_64bit_integers=1"
      ),
      Row("""{"visibleWidth(NULL)":"4"}""") :: Nil
    )
  }

  test("clickhouse catalog") {
    val prefix = if (useSuiteLevelDatabase) s"${testDatabaseName}_cat" else "db_t"
    val db1 = s"${prefix}1"
    val db2 = s"${prefix}2"
    withDatabase(db1, db2) {
      spark.sql(s"CREATE DATABASE $db1")
      spark.sql(s"CREATE DATABASE $db2")
      checkAnswer(
        spark.sql(s"SHOW DATABASES LIKE '${prefix}*'"),
        Row(db1) :: Row(db2) :: Nil
      )
      spark.sql("USE system")
      checkAnswer(
        spark.sql("SELECT current_database()"),
        Row("system") :: Nil
      )
      assert(spark.sql("SHOW tables").where($"tableName" === "contributors").count === 1)
    }
  }

  test("clickhouse system table") {
    checkAnswer(
      spark.sql("SELECT time_zone FROM `system`.`time_zones` WHERE time_zone = 'Asia/Shanghai'"),
      Row("Asia/Shanghai") :: Nil
    )
  }

  test("clickhouse partition") {
    val db = "db_part"

    // DROP + PURGE
    withSimpleTable(db, "tbl_part_purge", true) { (actualDb: String, actualTbl: String) =>
      eventuallyOnCloud {
        checkAnswer(
          spark.sql(s"SHOW PARTITIONS $actualDb.$actualTbl"),
          Seq(Row("m=1"), Row("m=2"))
        )
      }
      eventuallyOnCloud {
        checkAnswer(
          spark.sql(s"SHOW PARTITIONS $actualDb.$actualTbl PARTITION(m = 2)"),
          Seq(Row("m=2"))
        )
      }

      eventuallyOnCloud {
        spark.sql(s"ALTER TABLE $actualDb.$actualTbl DROP PARTITION(m = 2)")
      }
      eventuallyOnCloud {
        checkAnswer(
          spark.sql(s"SHOW PARTITIONS $actualDb.$actualTbl"),
          Seq(Row("m=1"))
        )
      }

      eventuallyOnCloud {
        spark.sql(s"ALTER TABLE $actualDb.$actualTbl DROP PARTITION(m = 1) PURGE")
      }
      eventuallyOnCloud {
        checkAnswer(
          spark.sql(s"SHOW PARTITIONS $actualDb.$actualTbl"),
          Seq()
        )
      }
    }

    // DROP + TRUNCATE
    withSimpleTable(db, "tbl_part_truncate", true) { (actualDb: String, actualTbl: String) =>
      eventuallyOnCloud {
        checkAnswer(
          spark.sql(s"SHOW PARTITIONS $actualDb.$actualTbl"),
          Seq(Row("m=1"), Row("m=2"))
        )
      }
      eventuallyOnCloud {
        checkAnswer(
          spark.sql(s"SHOW PARTITIONS $actualDb.$actualTbl PARTITION(m = 2)"),
          Seq(Row("m=2"))
        )
      }

      eventuallyOnCloud {
        spark.sql(s"ALTER TABLE $actualDb.$actualTbl DROP PARTITION(m = 2)")
      }
      eventuallyOnCloud {
        checkAnswer(
          spark.sql(s"SHOW PARTITIONS $actualDb.$actualTbl"),
          Seq(Row("m=1"))
        )
      }

      eventuallyOnCloud {
        spark.sql(s"TRUNCATE TABLE $actualDb.$actualTbl PARTITION(m = 1)")
      }
      eventuallyOnCloud {
        checkAnswer(
          spark.sql(s"SHOW PARTITIONS $actualDb.$actualTbl"),
          Seq()
        )
      }
    }
  }

  test("clickhouse partition (date type)") {
    val db = "db_part_date"
    val tbl = "tbl_part_date"
    val schema =
      StructType(
        StructField("id", LongType, false) ::
          StructField("date", DateType, false) :: Nil
      )
    withTable(db, tbl, schema, partKeys = Seq("date")) { (actualDb, actualTbl) =>
      spark.sql(
        s"""INSERT INTO `$actualDb`.`$actualTbl`
           |VALUES
           |  (11L, "2022-04-11"),
           |  (12L, "2022-04-12") AS tab(id, date)
           |""".stripMargin
      )
      spark.createDataFrame(Seq(
        (21L, date("2022-04-21")),
        (22L, date("2022-04-22"))
      ))
        .toDF("id", "date")
        .writeTo(s"$actualDb.$actualTbl").append

      checkAnswer(
        spark.table(s"$actualDb.$actualTbl").orderBy($"id"),
        Row(11L, date("2022-04-11")) ::
          Row(12L, date("2022-04-12")) ::
          Row(21L, date("2022-04-21")) ::
          Row(22L, date("2022-04-22")) :: Nil
      )

      eventuallyOnCloud {
        checkAnswer(
          spark.sql(s"SHOW PARTITIONS $actualDb.$actualTbl"),
          Seq(
            Row("date=2022-04-11"),
            Row("date=2022-04-12"),
            Row("date=2022-04-21"),
            Row("date=2022-04-22")
          )
        )
      }
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
    withTable(db, tbl, schema, partKeys = Seq("part_1", "part_2")) { (actualDb, actualTbl) =>
      spark.sql(
        s"""INSERT INTO `$actualDb`.`$actualTbl`
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
        .writeTo(s"$actualDb.$actualTbl").append

      checkAnswer(
        spark.table(s"$actualDb.$actualTbl").orderBy($"id"),
        Row(11L, "one_one", "1", 1) ::
          Row(12L, "one_two", "1", 2) ::
          Row(21L, "two_one", "2", 1) ::
          Row(22L, "two_two", "2", 2) :: Nil
      )

      eventuallyOnCloud {
        checkAnswer(
          spark.sql(s"SHOW PARTITIONS $actualDb.$actualTbl"),
          Seq(
            Row("part_1=1/part_2=1"),
            Row("part_1=1/part_2=2"),
            Row("part_1=2/part_2=1"),
            Row("part_1=2/part_2=2")
          )
        )
      }
    }
  }

  test("clickhouse multi part columns (date type)") {
    val db = "db_mul_part_date"
    val tbl = "tbl_mul_part_date"
    val schema =
      StructType(
        StructField("id", LongType, false) ::
          StructField("part_1", DateType, false) ::
          StructField("part_2", IntegerType, false) :: Nil
      )
    withTable(db, tbl, schema, partKeys = Seq("part_1", "part_2")) { (actualDb, actualTbl) =>
      spark.sql(
        s"""INSERT INTO `$actualDb`.`$actualTbl`
           |VALUES
           |  (11L, "2022-04-11", 1),
           |  (12L, "2022-04-12", 2) AS tab(id, part_1, part_2)
           |""".stripMargin
      )
      spark.createDataFrame(Seq(
        (21L, "2022-04-21", 1),
        (22L, "2022-04-22", 2)
      )).toDF("id", "part_1", "part_2")
        .writeTo(s"$actualDb.$actualTbl").append

      checkAnswer(
        spark.table(s"$actualDb.$actualTbl").orderBy($"id"),
        Row(11L, date("2022-04-11"), 1) ::
          Row(12L, date("2022-04-12"), 2) ::
          Row(21L, date("2022-04-21"), 1) ::
          Row(22L, date("2022-04-22"), 2) :: Nil
      )

      eventuallyOnCloud {
        checkAnswer(
          spark.sql(s"SHOW PARTITIONS $actualDb.$actualTbl"),
          Seq(
            Row("part_1=2022-04-11/part_2=1"),
            Row("part_1=2022-04-12/part_2=2"),
            Row("part_1=2022-04-21/part_2=1"),
            Row("part_1=2022-04-22/part_2=2")
          )
        )
      }
    }
  }

  // TODO remove this hack version
  test("clickhouse partition toYYYYMMDD(toDate(col))") {
    val db = if (useSuiteLevelDatabase) testDatabaseName else "db_part_toYYYYMMDD_toDate"
    val tbl = "tbl_part_toYYYYMMDD_toDate"
    try {
      if (!useSuiteLevelDatabase) runClickHouseSQL(s"CREATE DATABASE IF NOT EXISTS `$db`")
      runClickHouseSQL(
        s"""CREATE TABLE IF NOT EXISTS `$db`.`$tbl` (
           |    `id` Int64,
           |    `dt` String
           |) ENGINE = MergeTree
           |PARTITION BY toYYYYMMDD(toDate(dt))
           |ORDER BY (id)
           |""".stripMargin
      )
      spark.createDataFrame(Seq(
        (1L, "2022-06-06"),
        (2L, "2022-06-07")
      )).toDF("id", "dt")
        .writeTo(s"$db.$tbl").append
      eventuallyOnCloud {
        checkAnswer(
          spark.sql(s"SHOW PARTITIONS $db.$tbl"),
          Seq(
            Row("dt=20220606"),
            Row("dt=20220607")
          )
        )
      }
      checkAnswer(
        spark.table(s"$db.$tbl").orderBy($"id"),
        Seq(
          Row(1L, "2022-06-06"),
          Row(2L, "2022-06-07")
        )
      )
    } finally
      if (useSuiteLevelDatabase) dropTableWithRetry(db, tbl)
      else {
        runClickHouseSQL(s"DROP TABLE IF EXISTS `$db`.`$tbl`")
        runClickHouseSQL(s"DROP DATABASE IF EXISTS `$db`")
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
    withTable(db, tbl, schema, sortKeys = Seq("sort_2", "sort_3")) { (actualDb, actualTbl) =>
      spark.sql(
        s"""INSERT INTO `$actualDb`.`$actualTbl`
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
        .writeTo(s"$actualDb.$actualTbl").append

      checkAnswer(
        spark.table(s"$actualDb.$actualTbl").orderBy($"id"),
        Row(11L, "one_one", "1", 1) ::
          Row(12L, "one_two", "1", 2) ::
          Row(21L, "two_one", "2", 1) ::
          Row(22L, "two_two", "2", 2) :: Nil
      )
    }
  }

  test("clickhouse truncate table") {
    val schema = StructType(StructField("id", LongType, nullable = false) :: Nil)
    withTable("db_trunc", "tbl_trunc", schema) { (actualDb: String, actualTbl: String) =>
      spark.range(10).toDF("id").writeTo(s"$actualDb.$actualTbl").append
      assert(spark.table(s"$actualDb.$actualTbl").count == 10)
      spark.sql(s"TRUNCATE TABLE $actualDb.$actualTbl")
      assert(spark.table(s"$actualDb.$actualTbl").count == 0)
    }
  }

  test("clickhouse delete") {
    val schema = StructType(StructField("id", LongType, nullable = false) :: Nil)
    withTable("db_del", "tbl_db_del", schema) { (actualDb: String, actualTbl: String) =>
      spark.range(10).toDF("id").writeTo(s"$actualDb.$actualTbl").append
      eventuallyOnCloud(assert(spark.table(s"$actualDb.$actualTbl").count == 10))
      spark.sql(s"DELETE FROM $actualDb.$actualTbl WHERE id < 5")
      eventuallyOnCloud(assert(spark.table(s"$actualDb.$actualTbl").count == 5))
    }
  }

  test("clickhouse write then read") {
    val db = "db_rw"
    val tbl = "tbl_rw"

    withSimpleTable(db, tbl, true) { (actualDb: String, actualTbl: String) =>
      val tblSchema = spark.table(s"$actualDb.$actualTbl").schema
      assert(tblSchema == StructType(
        StructField("id", DataTypes.LongType, false) ::
          StructField("value", DataTypes.StringType, true) ::
          StructField("create_time", DataTypes.TimestampType, false) ::
          StructField("m", DataTypes.IntegerType, false) :: Nil
      ))

      checkAnswer(
        spark.table(s"$actualDb.$actualTbl").sort("m"),
        Seq(
          Row(1L, "1", timestamp("2021-01-01T10:10:10Z"), 1),
          Row(2L, "2", timestamp("2022-02-02T10:10:10Z"), 2)
        )
      )

      checkAnswer(
        spark.table(s"$actualDb.$actualTbl").filter($"id" > 1),
        Row(2L, "2", timestamp("2022-02-02T10:10:10Z"), 2) :: Nil
      )

      assert(spark.table(s"$actualDb.$actualTbl").filter($"id" > 1).count === 1)

    // infiniteLoop()
    }
  }

  test("clickhouse metadata column") {
    val db = "db_metadata_col"
    val tbl = "tbl_metadata_col"

    withSimpleTable(db, tbl, true) { (actualDb: String, actualTbl: String) =>
      checkAnswer(
        spark.sql(s"SELECT m, _partition_id FROM $actualDb.$actualTbl ORDER BY m"),
        Seq(
          Row(1, "1"),
          Row(2, "2")
        )
      )
    }
  }

  test("push down limit") {
    checkAnswer(
      spark.sql(s"SELECT zero FROM system.zeros LIMIT 2"),
      Seq(Row(0), Row(0))
    )
  }

  test("push down top N") {
    val db = "db_topn"
    // Avoid CREATE TABLE collisions across parallel Spark profiles in CI.
    val tbl = s"tbl_topn_${System.nanoTime()}"
    // Sort column is named `interval` -- a ClickHouse reserved keyword. this is to check the back-quoting fix.
    val schema = StructType(
      StructField("id", LongType, nullable = false) ::
        StructField("interval", LongType, nullable = true) :: Nil
    )
    withTable(db, tbl, schema) { (actualDb, actualTbl) =>
      runClickHouseSQL(
        s"""INSERT INTO `$actualDb`.`$actualTbl` VALUES
           |  (1, 10),
           |  (2, 20),
           |  (3, 30),
           |  (4, 40),
           |  (5, NULL)
           |""".stripMargin
      ).collect()

      // Tag the query so we can pull it from system.query_log by log_comment.
      val topNLogTag = java.util.UUID.randomUUID().toString
      withSQLConf(READ_SETTINGS_KEY -> readSettingsWith(s"log_comment='$topNLogTag'")) {
        checkAnswer(
          spark.sql(s"SELECT interval FROM $actualDb.$actualTbl ORDER BY interval DESC NULLS LAST LIMIT 2"),
          Seq(Row(40L), Row(30L))
        )
      }

      runClickHouseSQL(
        if (isCloud) "SYSTEM FLUSH LOGS ON CLUSTER 'default'"
        else "SYSTEM FLUSH LOGS"
      ).collect()
      val queryLogRef =
        if (isCloud) "clusterAllReplicas('default', system, query_log)"
        else "system.query_log"
      val recentQueries = runClickHouseSQL(
        s"""SELECT query FROM $queryLogRef
           |WHERE type = 'QueryFinish'
           |  AND log_comment = '$topNLogTag'
           |ORDER BY event_time DESC
           |LIMIT 5""".stripMargin
      ).collect().map(_.getString(0))
      val pushedTopN = recentQueries.exists { q =>
        q.contains("ORDER BY `interval` DESC NULLS LAST") && q.contains("LIMIT 2")
      }
      assert(
        pushedTopN,
        "Pushed query should include 'ORDER BY `interval` DESC NULLS LAST' and 'LIMIT 2'. " +
          s"Recent queries: ${recentQueries.mkString("; ")}"
      )

      checkAnswer(
        spark.sql(s"SELECT interval FROM $actualDb.$actualTbl ORDER BY interval ASC NULLS FIRST LIMIT 2"),
        Seq(Row(null), Row(10L))
      )

      withSQLConf("spark.clickhouse.read.pushdown.topN" -> "false") {
        checkAnswer(
          spark.sql(s"SELECT interval FROM $actualDb.$actualTbl ORDER BY interval DESC NULLS LAST LIMIT 2"),
          Seq(Row(40L), Row(30L))
        )
      }
    }
  }

  test("push down aggregation") {
    val db = "db_agg_col"
    val tbl = "tbl_agg_col"

    withSimpleTable(db, tbl, true) { (actualDb: String, actualTbl: String) =>
      checkAnswer(
        spark.sql(s"SELECT COUNT(id) FROM $actualDb.$actualTbl"),
        Seq(Row(2))
      )

      checkAnswer(
        spark.sql(s"SELECT MIN(id) FROM $actualDb.$actualTbl"),
        Seq(Row(1))
      )

      checkAnswer(
        spark.sql(s"SELECT MAX(id) FROM $actualDb.$actualTbl"),
        Seq(Row(2))
      )

      checkAnswer(
        spark.sql(s"SELECT m, COUNT(DISTINCT id) FROM $actualDb.$actualTbl GROUP BY m"),
        Seq(
          Row(1, 1),
          Row(2, 1)
        )
      )

      checkAnswer(
        spark.sql(s"SELECT m, SUM(DISTINCT id) FROM $actualDb.$actualTbl GROUP BY m"),
        Seq(
          Row(1, 1),
          Row(2, 2)
        )
      )
    }
  }

  test("create or replace table") {
    val db = if (useSuiteLevelDatabase) testDatabaseName else "db_cor"
    val tbl = "tbl_cor"
    try {
      if (!useSuiteLevelDatabase) runClickHouseSQL(s"CREATE DATABASE IF NOT EXISTS `$db`")
      def createOrReplaceTable(): Unit = spark.sql(
        s"""CREATE OR REPLACE TABLE `$db`.`$tbl` (
           |  id Long NOT NULL
           |) USING ClickHouse
           |TBLPROPERTIES (
           |  engine = 'MergeTree()',
           |  order_by = 'id',
           |  settings.index_granularity = 8192
           |)
           |""".stripMargin
      )
      createOrReplaceTable()
      createOrReplaceTable()
    } finally
      if (useSuiteLevelDatabase) dropTableWithRetry(db, tbl)
      else {
        runClickHouseSQL(s"DROP TABLE IF EXISTS `$db`.`$tbl`")
        runClickHouseSQL(s"DROP DATABASE IF EXISTS `$db`")
      }
  }

  /** The tasks `df` is planned into; a later `df.collect()` runs exactly these tasks. */
  private def plannedPartitions(df: DataFrame): Seq[ClickHouseInputPartition] =
    df.queryExecution.sparkPlan.collectFirst { case s: BatchScanExec => s.scan }
      .getOrElse(fail(s"no ClickHouse scan in ${df.queryExecution.sparkPlan}"))
      .asInstanceOf[ClickHouseBatchScan]
      .inputPartitions
      .toSeq

  private def withPartitionRangeTable(dbName: String, tbl: String, ddl: String)(f: (String, String) => Unit): Unit = {
    val db = if (useSuiteLevelDatabase) testDatabaseName else dbName
    try {
      if (!useSuiteLevelDatabase) runClickHouseSQL(s"CREATE DATABASE IF NOT EXISTS `$db`")
      runClickHouseSQL(s"CREATE TABLE `$db`.`$tbl` $ddl")
      f(db, tbl)
    } finally
      if (useSuiteLevelDatabase) dropTableWithRetry(db, tbl)
      else {
        runClickHouseSQL(s"DROP TABLE IF EXISTS `$db`.`$tbl`")
        runClickHouseSQL(s"DROP DATABASE IF EXISTS `$db`")
      }
  }

  /**
   * Plans `SELECT id` over `db.tbl`, checks the planned tasks, runs `insertAfterPlanning`, then reads with the
   * tasks planned before the insert and expects ids `1..expectedRows`, each once. Collecting the same
   * Dataset is what makes this a race test: a new Dataset would list `system.parts` again.
   */
  private def checkReadAfterConcurrentInsert(db: String, tbl: String, expectedRows: Long)(
    checkPlanned: Seq[ClickHouseInputPartition] => Unit
  )(insertAfterPlanning: => Unit): Unit = {
    var df: DataFrame = null
    eventuallyOnCloud {
      df = spark.table(s"$db.$tbl").select($"id")
      checkPlanned(plannedPartitions(df))
    }
    insertAfterPlanning
    eventuallyOnCloud {
      // a known count, not a live one: a lagging Cloud replica must not make the check pass vacuously
      val ids = df.collect().map(_.getLong(0)).toSeq
      assert(ids.length === expectedRows, ids.sorted)
      assert(ids.distinct.length === ids.length, ids.sorted)
      assert(ids.sorted === (1L to expectedRows).toList)
    }
  }

  test("a read planned before an insert reads Int and NULL partitions created after planning") {
    withPartitionRangeTable(
      "db_part_range_int",
      "tbl_part_range_int",
      "(id Int64, k Nullable(Int32)) ENGINE = MergeTree PARTITION BY k ORDER BY id SETTINGS allow_nullable_key = 1"
    ) { (db, tbl) =>
      runClickHouseSQL(s"INSERT INTO `$db`.`$tbl` VALUES (1, 20), (2, 40)")
      checkReadAfterConcurrentInsert(db, tbl, expectedRows = 7) { planned =>
        assert(planned.map(_.partFilterExpr) === Seq("_partition_id < '40'", "_partition_id >= '40'"))
      } {
        // below, between and above the listed ids, a NULL key, and the already listed partition 20
        runClickHouseSQL(s"INSERT INTO `$db`.`$tbl` VALUES (3, 10), (4, 30), (5, 50), (6, NULL), (7, 20)")
      }
    }
  }

  test("a read planned before an insert reads String partitions created after planning") {
    withPartitionRangeTable(
      "db_part_range_str",
      "tbl_part_range_str",
      "(id Int64, s String) ENGINE = MergeTree PARTITION BY s ORDER BY id"
    ) { (db, tbl) =>
      runClickHouseSQL(s"INSERT INTO `$db`.`$tbl` VALUES (1, 'a'), (2, 'b'), (3, 'c')")
      checkReadAfterConcurrentInsert(db, tbl, expectedRows = 23) { planned =>
        assert(planned.length === 3)
        assert(
          planned.head.partitionIdRange.exists(_.lower.isEmpty) && planned.last.partitionIdRange.exists(_.upper.isEmpty)
        )
        assert(planned.forall(_.filterByPartitionId))
      } {
        // String partition ids are hashes, so these land anywhere between and around the listed ones
        val values = (Seq("o''k") ++ (1 to 19).map(i => s"v$i")).zipWithIndex
          .map { case (s, i) => s"(${i + 4}, '$s')" }
        runClickHouseSQL(s"INSERT INTO `$db`.`$tbl` VALUES ${values.mkString(", ")}")
      }
    }
  }

  test("a read planned over a single partition reads partitions created after planning") {
    withPartitionRangeTable(
      "db_part_range_one",
      "tbl_part_range_one",
      "(id Int64, k Int32) ENGINE = MergeTree PARTITION BY k ORDER BY id"
    ) { (db, tbl) =>
      runClickHouseSQL(s"INSERT INTO `$db`.`$tbl` VALUES (1, 1)")
      checkReadAfterConcurrentInsert(db, tbl, expectedRows = 2) { planned =>
        assert(planned.map(_.partFilterExpr) === Seq("1=1"))
      } {
        runClickHouseSQL(s"INSERT INTO `$db`.`$tbl` VALUES (2, 2)")
      }
    }
  }

  /** The SQL the first ClickHouse scan task of `df` would send, as the reader builds it. */
  private def scanQuery(df: DataFrame): String = {
    val scan = df.queryExecution.sparkPlan.collectFirst { case s: BatchScanExec => s.scan }
      .getOrElse(fail(s"no ClickHouse scan in ${df.queryExecution.sparkPlan}"))
      .asInstanceOf[ClickHouseBatchScan]
    val reader = scan.createReader(scan.planInputPartitions.head).asInstanceOf[ClickHouseReader[_]]
    try reader.scanQuery
    finally reader.close()
  }

  test("a pushed-down aggregate over no matching rows is not the empty-set value") {
    // a task matching nothing returns min()=0 for a non-nullable column, which Spark folds in
    withSimpleTable("db_agg_empty", "tbl_agg", writeData = true) { (db, tbl) =>
      val min = spark.sql(s"SELECT MIN(id) FROM $db.$tbl WHERE id > 999")
      checkAnswer(min, Seq(Row(null)))
      // the plain form is pushed, and HAVING drops the empty task's row
      val minQuery = scanQuery(min)
      assert(minQuery.contains("SELECT MIN(`id`)") && minQuery.contains("HAVING count() > 0"), minQuery)
      checkAnswer(spark.sql(s"SELECT COUNT(*) FROM $db.$tbl WHERE id > 999"), Seq(Row(0L)))
    }
  }

  test("a pushed-down aggregate over an empty table keeps SQL semantics") {
    withSimpleTable("db_agg_empty", "tbl_none", writeData = false) { (db, tbl) =>
      // COUNT of no rows is 0, but MIN and SUM of no rows are NULL, as in Spark without pushdown
      checkAnswer(spark.sql(s"SELECT COUNT(*) FROM $db.$tbl"), Seq(Row(0L)))
      checkAnswer(spark.sql(s"SELECT MIN(id) FROM $db.$tbl"), Seq(Row(null)))
      checkAnswer(spark.sql(s"SELECT SUM(id) FROM $db.$tbl"), Seq(Row(null)))
    }
  }

  test("a pushed-down GROUP BY aggregate is unaffected") {
    withSimpleTable("db_agg_empty", "tbl_grp", writeData = true) { (db, tbl) =>
      checkAnswer(spark.sql(s"SELECT m, COUNT(*) FROM $db.$tbl GROUP BY m ORDER BY m"), Seq(Row(1, 1L), Row(2, 1L)))
    }
  }

  test("a pushed-down aggregate keeps a legitimately zero result") {
    // the NULL form only changes the empty-set answer, so a genuine 0 is returned as 0
    withSimpleTable("db_agg_zero", "tbl_zero", writeData = false) { (db, tbl) =>
      spark.createDataFrame(Seq(
        (0L, "zero", java.sql.Timestamp.valueOf("2021-01-01 10:10:10"), 1),
        (5L, "five", java.sql.Timestamp.valueOf("2021-01-01 10:10:10"), 2)
      )).toDF("id", "value", "create_time", "m").writeTo(s"$db.$tbl").append()
      checkAnswer(spark.sql(s"SELECT MIN(id) FROM $db.$tbl"), Seq(Row(0L)))
      checkAnswer(spark.sql(s"SELECT SUM(id) FROM $db.$tbl WHERE id = 0"), Seq(Row(0L)))
    }
  }

  test("a pushed-down aggregate over no matching rows survives a filter") {
    // collect() reads NULL in a non-nullable slot as 0 and cannot see this corruption; a filter can
    withSimpleTable("db_agg_filter", "tbl_filter", writeData = true) { (db, tbl) =>
      val counted = spark.sql(s"SELECT COUNT(*) AS c FROM $db.$tbl WHERE id > 999")
      assert(counted.filter("c = 0").count() === 1L)
      assert(!counted.collect().head.isNullAt(0))

      // COUNT and MIN need different empty-set answers out of the same task
      val countMin = spark.sql(s"SELECT COUNT(*), MIN(id) FROM $db.$tbl WHERE id > 999")
      checkAnswer(countMin, Seq(Row(0L, null)))
      // the empty task keeps its row for COUNT, so MIN is pushed in its NULL-if-empty form
      val countMinQuery = scanQuery(countMin)
      assert(countMinQuery.contains("COUNT(*), if(count() = 0, NULL, MIN(`id`))"), countMinQuery)
      assert(!countMinQuery.contains("HAVING"), countMinQuery)
    }
  }

  test("MIN and MAX of an Array or Tuple column ignore a task that matches no rows") {
    // the plain form returns [] or a tuple of defaults over no rows, and Spark folds that in
    val schema = StructType(
      StructField("id", LongType, nullable = false) ::
        StructField("m", IntegerType, nullable = false) ::
        StructField("tags", ArrayType(LongType, containsNull = false), nullable = false) ::
        StructField(
          "pair",
          StructType(
            StructField("a", IntegerType, nullable = false) :: StructField("b", StringType, nullable = false) :: Nil
          ),
          nullable = false
        ) :: Nil
    )
    // the columns the ClickHouse scan returns: compiled aggregates when the aggregation was pushed
    def scannedColumns(df: org.apache.spark.sql.DataFrame): Seq[String] =
      df.queryExecution.sparkPlan.collect { case scan: BatchScanExec =>
        scan.scan.readSchema().fieldNames.toSeq
      }.flatten
    withTable("db_agg_container", "tbl_agg_container", schema, partKeys = Seq("m")) { (db, tbl) =>
      runClickHouseSQL(s"INSERT INTO `$db`.`$tbl` VALUES (1, 1, [3, 4], (5, 'x')), (2, 2, [1, 2], (1, 'y'))")
      val minMax = "MIN(tags), MAX(tags), MIN(pair), MAX(pair)"
      // pushed with HAVING count() > 0, so the empty task returns no row instead of the defaults
      for (filter <- Seq("m = 1", "id = 1")) {
        val df = spark.sql(s"SELECT $minMax FROM `$db`.`$tbl` WHERE $filter")
        checkAnswer(df, Seq(Row(Seq(3L, 4L), Seq(3L, 4L), Row(5, "x"), Row(5, "x"))))
        assert(scannedColumns(df).containsSlice(Seq("MIN(`tags`)", "MAX(`tags`)", "MIN(`pair`)", "MAX(`pair`)")))
        assert(scanQuery(df).contains("HAVING count() > 0"), scanQuery(df))
      }
      // every task is empty, so Spark merges no rows at all
      checkAnswer(spark.sql(s"SELECT $minMax FROM `$db`.`$tbl` WHERE id > 999"), Seq(Row(null, null, null, null)))

      // a COUNT needs the empty task's 0 row, and a container has no NULL form, so it is not pushed
      val counted = spark.sql(s"SELECT COUNT(*), MIN(tags), MAX(pair) FROM `$db`.`$tbl` WHERE id = 1")
      checkAnswer(counted, Seq(Row(1L, Seq(3L, 4L), Row(5, "x"))))
      val scanned = scannedColumns(counted)
      assert(scanned.contains("tags"), s"expected the bare column to be scanned, got $scanned")
      assert(!scanned.exists(_.contains("(")), s"pushed with no correct empty-set form: $scanned")

      // with GROUP BY no group is empty, so the plain form is pushed and is exact
      val grouped = spark.sql(s"SELECT m, MIN(tags), MAX(pair) FROM `$db`.`$tbl` GROUP BY m")
      checkAnswer(grouped, Seq(Row(1, Seq(3L, 4L), Row(5, "x")), Row(2, Seq(1L, 2L), Row(1, "y"))))
      assert(scannedColumns(grouped).containsSlice(Seq("MIN(`tags`)", "MAX(`pair`)")))
    }
  }

  test("cache table") {
    val db = "cache_db"
    val tbl = "cache_tbl"

    withSimpleTable(db, tbl, true) { (actualDb: String, actualTbl: String) =>
      try {
        spark.sql(s"CACHE TABLE $actualDb.$actualTbl")
        val cachedPlan = spark.sql(s"SELECT * FROM $actualDb.$actualTbl").queryExecution.commandExecuted
          .find(node => spark.sharedState.cacheManager.lookupCachedData(node).isDefined)
        assert(cachedPlan.isDefined)
      } finally
        spark.sql(s"UNCACHE TABLE $actualDb.$actualTbl")
    }
  }

  test("runtime filter") {
    val db = "runtime_db"
    val tbl = "runtime_tbl"

    withSimpleTable(db, tbl, true) { (actualDb: String, actualTbl: String) =>
      spark.sql("set spark.clickhouse.read.runtimeFilter.enabled=false")
      checkAnswer(
        spark.sql(s"SELECT id FROM $actualDb.$actualTbl " +
          s"WHERE id IN (" +
          s"  SELECT id FROM $actualDb.$actualTbl " +
          s"  WHERE DATE_FORMAT(create_time, 'yyyy-MM-dd') between '2021-01-01' and '2022-01-01'" +
          s")"),
        Row(1)
      )

      spark.sql("set spark.clickhouse.read.runtimeFilter.enabled=true")
      val df = spark.sql(s"SELECT id FROM $actualDb.$actualTbl " +
        s"WHERE id IN (" +
        s"  SELECT id FROM $actualDb.$actualTbl " +
        s"  WHERE DATE_FORMAT(create_time, 'yyyy-MM-dd') between '2021-01-01' and '2022-01-01'" +
        s")")
      checkAnswer(df, Row(1))
      val runtimeFilterExists = df.queryExecution.sparkPlan.exists {
        case BatchScanExec(_, _, runtimeFilters, _) if runtimeFilters.nonEmpty => true
        case _ => false
      }
      assert(runtimeFilterExists)
    }
  }
}
