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
 * Pushed-down timestamp filters must be rendered in the catalog time zone (`spark.clickhouse.tz`, `server` here),
 * not the JVM default zone, see issue #607. The JVM and session zone is America/Los_Angeles, the server zone differs.
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

  private def withMinuteTable(f: (String, String) => Unit): Unit = {
    val db = if (useSuiteLevelDatabase) testDatabaseName else "db_pushdown_tz"
    val tbl = s"tbl_pushdown_tz_${System.nanoTime()}"
    try {
      if (!useSuiteLevelDatabase) runClickHouseSQL(s"CREATE DATABASE IF NOT EXISTS `$db`")
      runClickHouseSQL(s"CREATE TABLE `$db`.`$tbl` (ts DateTime, id UInt64) ENGINE = MergeTree() ORDER BY id")
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
    pushed: String => Boolean
  ): Unit =
    Seq("false", "true").foreach { java8Api =>
      withSQLConf("spark.sql.datetime.java8API.enabled" -> java8Api) {
        // through the connector: a filter that compiles is pushed to ClickHouse and not re-checked by Spark
        val (viaConnector, sql) = countWithPushedSql(spark.table(s"`$db`.`$tbl`").filter(condition))
        info(s"java8API=$java8Api connector=$viaConnector SQL: ${sql.mkString(" | ")}")
        // the same predicate on an expression Spark cannot push, so Spark evaluates it
        val inSpark =
          spark.table(s"`$db`.`$tbl`").where(expr(condition.replaceFirst("^ts ", "ts + INTERVAL 0 SECOND ")))
            .count()
        assert(viaConnector === expected, s"java8API=$java8Api pushed SQL: ${sql.mkString(" | ")}")
        assert(inSpark === expected, s"java8API=$java8Api")
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
        _.contains("`ts` >= '")
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
}
