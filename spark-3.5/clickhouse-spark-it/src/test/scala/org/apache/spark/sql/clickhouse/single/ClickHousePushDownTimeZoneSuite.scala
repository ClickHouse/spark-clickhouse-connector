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
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr
import org.scalatest.tags.Cloud

import java.util.{TimeZone, UUID}

@Cloud
class ClickHouseCloudPushDownTimeZoneSuite extends ClickHousePushDownTimeZoneSuite with ClickHouseCloudMixIn

class ClickHouseSinglePushDownTimeZoneSuite extends ClickHousePushDownTimeZoneSuite with ClickHouseSingleMixIn

/**
 * Pushed-down timestamp filters must mean the same instant ClickHouse compares, whatever the JVM default zone (issue
 * #607) and whatever the column's own zone: they are sent as UTC-typed literals. The JVM and session zone is
 * America/Los_Angeles, the server zone differs.
 */
abstract class ClickHousePushDownTimeZoneSuite extends SparkClickHouseSingleTest {

  private val jvmZone = "America/Los_Angeles"
  private var originalZone: TimeZone = _

  override def beforeAll(): Unit = {
    originalZone = TimeZone.getDefault
    TimeZone.setDefault(TimeZone.getTimeZone(jvmZone))
    super.beforeAll()
  }

  override def afterAll(): Unit =
    try super.afterAll()
    finally TimeZone.setDefault(originalZone)

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.session.timeZone", jvmZone)

  // one row per minute from 2024-01-01 00:00:00 UTC to 2024-01-07 23:59:00 UTC
  private val rowCount = 7 * 24 * 60

  private def withMinuteTable(f: (String, String) => Unit): Unit = withMinuteTable("DateTime")(f)

  private def withMinuteTable(columnType: String)(f: (String, String) => Unit): Unit = {
    val db = if (useSuiteLevelDatabase) testDatabaseName else "db_pushdown_tz"
    val tbl = s"tbl_pushdown_tz_${System.nanoTime()}"
    try {
      if (!useSuiteLevelDatabase) runClickHouseSQL(s"CREATE DATABASE IF NOT EXISTS `$db`")
      runClickHouseSQL(s"CREATE TABLE `$db`.`$tbl` (ts $columnType, id UInt64) ENGINE = MergeTree() ORDER BY id")
      runClickHouseSQL(
        s"""INSERT INTO `$db`.`$tbl`
           |SELECT toDateTime('2024-01-01 00:00:00', 'UTC') + number * 60, number FROM numbers($rowCount)
           |""".stripMargin
      )
      f(db, tbl)
    } finally
      if (useSuiteLevelDatabase) dropTableWithRetry(db, tbl)
      else {
        runClickHouseSQL(s"DROP TABLE IF EXISTS `$db`.`$tbl`")
        runClickHouseSQL(s"DROP DATABASE IF EXISTS `$db`")
      }
  }

  /** Counts `df` with its read tagged, and returns the count plus the SQL ClickHouse received for it. */
  private def countWithPushedSql(df: => DataFrame): (Long, Seq[String]) = {
    val tag = UUID.randomUUID().toString
    var count = 0L
    withSQLConf(READ_SETTINGS_KEY -> readSettingsWith(s"log_comment='$tag'")) { count = df.count() }
    runClickHouseSQL(if (isCloud) "SYSTEM FLUSH LOGS ON CLUSTER 'default'" else "SYSTEM FLUSH LOGS").collect()
    val queryLog = if (isCloud) "clusterAllReplicas('default', system, query_log)" else "system.query_log"
    val queries = runClickHouseSQL(
      s"SELECT DISTINCT query FROM $queryLog WHERE type = 'QueryFinish' AND log_comment = '$tag'"
    ).collect().map(_.getString(0)).filter(_.contains("WHERE")).toSeq
      .map(_.replace("\\n", " ").replaceAll("\\s+", " "))
    (count, queries)
  }

  private def assertPushedMatchesSpark(
    db: String,
    tbl: String,
    condition: String,
    expected: Long,
    pushed: String => Boolean,
    checkInSpark: Boolean = true
  ): Unit =
    Seq("false", "true").foreach { java8Api =>
      withSQLConf("spark.sql.datetime.java8API.enabled" -> java8Api) {
        // through the connector: a filter that compiles is pushed to ClickHouse and not re-checked by Spark
        val (viaConnector, sql) = countWithPushedSql(spark.table(s"`$db`.`$tbl`").filter(condition))
        info(s"java8API=$java8Api connector=$viaConnector SQL: ${sql.mkString(" | ")}")
        // the same predicate on an expression Spark cannot push, so Spark evaluates it
        lazy val inSpark =
          spark.table(s"`$db`.`$tbl`").where(expr(condition.replaceFirst("^ts ", "ts + INTERVAL 0 SECOND ")))
            .count()
        assert(viaConnector === expected, s"java8API=$java8Api pushed SQL: ${sql.mkString(" | ")}")
        if (checkInSpark) assert(inSpark === expected, s"java8API=$java8Api")
        assert(sql.nonEmpty && sql.forall(pushed), s"java8API=$java8Api pushed SQL: ${sql.mkString(" | ")}")
      }
    }

  test("pushed-down timestamp filter uses the catalog time zone") {
    withMinuteTable { (db, tbl) =>
      // '2024-01-05 00:00:00' America/Los_Angeles is 2024-01-05 08:00:00 UTC: 3 days minus 8 hours of rows remain
      eventuallyOnCloud(assertPushedMatchesSpark(
        db,
        tbl,
        "ts >= '2024-01-05 00:00:00'",
        3 * 1440 - 8 * 60,
        _.contains("`ts` >= toDateTime64(")
      ))
      eventuallyOnCloud(assertPushedMatchesSpark(
        db,
        tbl,
        "ts IN (timestamp'2024-01-05 00:00:00', timestamp'2024-01-05 00:01:00')",
        2,
        _.contains("`ts` IN (toDateTime(")
      ))
    }
  }

  test("pushed-down sub-second timestamp filter on a DateTime column") {
    withMinuteTable { (db, tbl) =>
      // 2024-01-05 08:00:00.5 UTC: the row at 08:00:00 is excluded
      eventuallyOnCloud(assertPushedMatchesSpark(
        db,
        tbl,
        "ts > '2024-01-05 00:00:00.5'",
        3 * 1440 - 8 * 60 - 1,
        _.contains("`ts` > toDateTime64(")
      ))
      eventuallyOnCloud(assertPushedMatchesSpark(
        db,
        tbl,
        "ts <= '2024-01-05 00:00:00.5'",
        4 * 1440 + 8 * 60 + 1,
        _.contains("`ts` <= toDateTime64(")
      ))
      // IN with a sub-second value is left to Spark; only 08:01:00 UTC matches
      eventuallyOnCloud(assertPushedMatchesSpark(
        db,
        tbl,
        "ts IN (timestamp'2024-01-05 00:00:00.5', timestamp'2024-01-05 00:01:00')",
        1,
        !_.contains(" IN (")
      ))
    }
  }

  // A column with its own zone: ClickHouse reads a plain-text literal in that zone, a typed UTC literal as UTC.
  // One row per minute from 2024-01-01 00:00:00 UTC, so 2024-01-05 05:00:00 UTC is 14:00 in Tokyo. `expected` is
  // what ClickHouse holds; the Spark-side cross-check is off here because the read path does not yet apply the
  // column's own zone to the values it returns.
  test("pushed-down timestamp filters on a DateTime('Asia/Tokyo') column") {
    withMinuteTable("DateTime('Asia/Tokyo')") { (db, tbl) =>
      eventuallyOnCloud(assertPushedMatchesSpark(
        db,
        tbl,
        "ts >= timestamp'2024-01-05 05:00:00Z'",
        3 * 1440 - 5 * 60,
        _.contains("`ts` >= toDateTime64("),
        checkInSpark = false
      ))
      eventuallyOnCloud(assertPushedMatchesSpark(
        db,
        tbl,
        "ts >= timestamp'2024-01-05 05:00:00.5Z'",
        3 * 1440 - 5 * 60 - 1,
        _.contains("`ts` >= toDateTime64("),
        checkInSpark = false
      ))
      eventuallyOnCloud(assertPushedMatchesSpark(
        db,
        tbl,
        "ts IN (timestamp'2024-01-05 00:00:00Z', timestamp'2024-01-05 00:01:00Z')",
        2,
        _.contains("`ts` IN (toDateTime("),
        checkInSpark = false
      ))
    }
  }

  test("pushed-down timestamp filters on a DateTime64(3) column") {
    withMinuteTable("DateTime64(3)") { (db, tbl) =>
      eventuallyOnCloud(assertPushedMatchesSpark(
        db,
        tbl,
        "ts > timestamp'2024-01-05 05:00:00.5Z'",
        3 * 1440 - 5 * 60 - 1,
        _.contains("`ts` > toDateTime64(")
      ))
      eventuallyOnCloud(assertPushedMatchesSpark(
        db,
        tbl,
        "ts IN (timestamp'2024-01-05 00:00:00Z', timestamp'2024-01-05 00:01:00Z')",
        2,
        _.contains("`ts` IN (toDateTime(")
      ))
      // pre-1970: every row is later
      eventuallyOnCloud(assertPushedMatchesSpark(
        db,
        tbl,
        "ts > timestamp'1969-12-31 23:00:00Z'",
        rowCount,
        _.contains("`ts` > toDateTime64('1969-12-31 23:00:00', 6, 'UTC')")
      ))
    }
  }
}
