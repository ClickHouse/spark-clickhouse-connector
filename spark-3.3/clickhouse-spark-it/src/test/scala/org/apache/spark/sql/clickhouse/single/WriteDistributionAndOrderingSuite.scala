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
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{AnalysisException, Row, SaveMode}
import org.scalatest.tags.Cloud

@Cloud
class ClickHouseCloudsWriteDistributionAndOrderingSuite
    extends WriteDistributionAndOrderingSuite with ClickHouseCloudMixIn

class ClickHouseSinglesWriteDistributionAndOrderingSuite
    extends WriteDistributionAndOrderingSuite with ClickHouseSingleMixIn

abstract class WriteDistributionAndOrderingSuite extends SparkClickHouseSingleTest {

  import testImplicits._

  private lazy val db = if (useSuiteLevelDatabase) testDatabaseName else "db_distribution_and_ordering"
  private val tbl = "tbl_distribution_and_ordering"

  private def write(): Unit = {
    spark.range(3)
      .toDF("id")
      .withColumn("id", $"id".cast(StringType))
      .withColumn("load_date", lit(date("2022-05-27")))
      .writeTo(s"$db.$tbl")
      .append
    Thread.sleep(1000)
  }

  private def check(): Unit = checkAnswer(
    spark.sql(s"SELECT id, load_date FROM $db.$tbl"),
    Seq(
      Row("0", date("2022-05-27")),
      Row("1", date("2022-05-27")),
      Row("2", date("2022-05-27"))
    )
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (!useSuiteLevelDatabase) {
      sql(s"CREATE DATABASE IF NOT EXISTS `$db`")
    }
    runClickHouseSQL(
      s"""CREATE TABLE `$db`.`$tbl` (
         |  `id` String,
         |  `load_date` Date
         |) ENGINE = MergeTree
         |ORDER BY load_date
         |PARTITION BY xxHash64(id)
         |""".stripMargin
    )
  }

  override def afterAll(): Unit =
    try
      if (useSuiteLevelDatabase) {
        dropTableWithRetry(db, tbl)
      } else {
        sql(s"DROP TABLE IF EXISTS `$db`.`$tbl`")
        sql(s"DROP DATABASE IF EXISTS `$db`")
      }
    finally
      super.afterAll()

  override protected def beforeEach(): Unit = {
    sql(s"TRUNCATE TABLE `$db`.`$tbl`")
    super.beforeEach()
  }

  test("write to table with PARTITION BY tuple() succeeds") {
    val db = if (useSuiteLevelDatabase) testDatabaseName else "db_tuple_partition"
    val tbl = "tbl_tuple_partition"

    try {
      if (!useSuiteLevelDatabase) {
        sql(s"CREATE DATABASE IF NOT EXISTS `$db`")
      }

      runClickHouseSQL(
        s"""CREATE TABLE `$db`.`$tbl` (
           |  `id` String,
           |  `value` String
           |) ENGINE = MergeTree()
           |ORDER BY id
           |PARTITION BY tuple()
           |""".stripMargin
      )

      val df = spark.createDataFrame(Seq(
        ("1", "a"),
        ("2", "b"),
        ("3", "c")
      )).toDF("id", "value")

      val options = cmdRunnerOptions ++ Map("database" -> db, "table" -> tbl)

      df.write
        .format("clickhouse")
        .mode(SaveMode.Append)
        .options(options)
        .save()

      val result = spark.read
        .format("clickhouse")
        .options(options)
        .load()

      assert(result.count() == 3)
    } finally
      if (useSuiteLevelDatabase) {
        dropTableWithRetry(db, tbl)
      } else {
        sql(s"DROP TABLE IF EXISTS `$db`.`$tbl`")
        sql(s"DROP DATABASE IF EXISTS `$db`")
      }
  }

  test("write to table with unsupported PARTITION BY substring() succeeds with warning") {
    val db = if (useSuiteLevelDatabase) testDatabaseName else "db_substring_partition"
    val tbl = "tbl_substring_partition"

    try {
      if (!useSuiteLevelDatabase) {
        sql(s"CREATE DATABASE IF NOT EXISTS `$db`")
      }

      runClickHouseSQL(
        s"""CREATE TABLE `$db`.`$tbl` (
           |  `id` String,
           |  `value` String
           |) ENGINE = MergeTree()
           |ORDER BY id
           |PARTITION BY substring(id, 1, 1)
           |""".stripMargin
      )

      val df = spark.createDataFrame(Seq(
        ("abc", "1"),
        ("def", "2"),
        ("ghi", "3")
      )).toDF("id", "value")

      val options = cmdRunnerOptions ++ Map("database" -> db, "table" -> tbl)

      // With default ignoreUnsupportedTransform=true, write succeeds with warning
      df.write
        .format("clickhouse")
        .mode(SaveMode.Append)
        .options(options)
        .save()

      val result = spark.read
        .format("clickhouse")
        .options(options)
        .load()

      assert(result.count() == 3)
    } finally
      if (useSuiteLevelDatabase) {
        dropTableWithRetry(db, tbl)
      } else {
        sql(s"DROP TABLE IF EXISTS `$db`.`$tbl`")
        sql(s"DROP DATABASE IF EXISTS `$db`")
      }
  }

  test("write to table with unsupported PARTITION BY substring() fails when ignoreUnsupportedTransform=false") {
    val db = if (useSuiteLevelDatabase) testDatabaseName else "db_substring_partition_fail"
    val tbl = "tbl_substring_partition_fail"

    try {
      if (!useSuiteLevelDatabase) {
        sql(s"CREATE DATABASE IF NOT EXISTS `$db`")
      }

      runClickHouseSQL(
        s"""CREATE TABLE `$db`.`$tbl` (
           |  `id` String,
           |  `value` String
           |) ENGINE = MergeTree()
           |ORDER BY id
           |PARTITION BY substring(id, 1, 1)
           |""".stripMargin
      )

      val df = spark.createDataFrame(Seq(
        ("abc", "1"),
        ("def", "2")
      )).toDF("id", "value")

      val options = cmdRunnerOptions ++ Map("database" -> db, "table" -> tbl)

      withSQLConf(IGNORE_UNSUPPORTED_TRANSFORM.key -> "false") {
        val ex = intercept[Exception] {
          df.write
            .format("clickhouse")
            .mode(SaveMode.Append)
            .options(options)
            .save()
        }
        assert(ex.getMessage.contains("substring") || ex.getCause.getMessage.contains("substring"))
      }
    } finally
      if (useSuiteLevelDatabase) {
        dropTableWithRetry(db, tbl)
      } else {
        sql(s"DROP TABLE IF EXISTS `$db`.`$tbl`")
        sql(s"DROP DATABASE IF EXISTS `$db`")
      }
  }
}
