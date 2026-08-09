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
import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.LogEvent
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.spark.sql.Row
import org.scalatest.concurrent.Eventually._
import org.scalatest.tags.Cloud
import org.scalatest.time.SpanSugar._

import scala.collection.mutable.ArrayBuffer

@Cloud
class ClickHouseCloudReadWarningSuite extends ClickHouseReadWarningSuite with ClickHouseCloudMixIn

class ClickHouseSingleReadWarningSuite extends ClickHouseReadWarningSuite with ClickHouseSingleMixIn

abstract class ClickHouseReadWarningSuite extends SparkClickHouseSingleTest {

  private val engineUtilsLogger = "com.clickhouse.spark.spec.TableEngineUtils"
  private val batchScanLogger = "com.clickhouse.spark.read.ClickHouseBatchScan"

  // Cloud's multi-replica compute can serve reads from a replica whose part / metadata cache
  // has not yet caught up with a recent write or DDL. Retry the assertion until reads converge.
  private def eventuallyOnCloud(body: => Unit): Unit =
    if (isCloud) eventually(timeout(15.seconds), interval(500.millis))(body) else body

  test("reading a view does not warn about unknown table engine") {
    withKVTable("db_read_warning", "tbl_view_src", valueColDef = "String") { (db, tbl) =>
      insertKV(db, tbl, 1 -> "a", 2 -> "b")
      withView(db, s"${tbl}_v", s"SELECT key, value FROM `$db`.`$tbl`") { view =>
        eventuallyOnCloud {
          val (rows, warnings) = captureWarnings(engineUtilsLogger)(readSortedByKey(db, view))
          assert(rows === Seq(Row(1, "a"), Row(2, "b")))
          assert(!warnings.exists(_.contains("Unknown table engine")))
        }
      }
    }
  }

  // a materialized view is deliberately not folded into the view special-case: its empty
  // engine_full is unparseable, so reading it must keep emitting the unknown-engine warning
  test("reading a materialized view warns about unknown table engine") {
    withKVTable("db_read_warning", "tbl_mv_src", valueColDef = "String") { (db, tbl) =>
      withMaterializedView(db, s"${tbl}_mv", s"SELECT key, value FROM `$db`.`$tbl`") { mv =>
        eventuallyOnCloud {
          val (rowCount, warnings) = captureWarnings(engineUtilsLogger)(readRowCount(db, mv))
          assert(rowCount === 0)
          assert(warnings.exists(_.contains(s"Unknown table engine for table $db.$mv")))
        }
      }
    }
  }

  test("reading a table with a supported engine does not warn about unknown table engine") {
    withKVTable("db_read_warning", "tbl_supported_engine", valueColDef = "String") { (db, tbl) =>
      insertKV(db, tbl, 1 -> "a", 2 -> "b")
      eventuallyOnCloud {
        val (rowCount, warnings) = captureWarnings(engineUtilsLogger)(readRowCount(db, tbl))
        assert(rowCount === 2)
        assert(!warnings.exists(_.contains("Unknown table engine")))
      }
    }
  }

  test("reading a view warns about single partition read") {
    withKVTable("db_read_warning", "tbl_view_single_part_src", valueColDef = "String") { (db, tbl) =>
      insertKV(db, tbl, 1 -> "a")
      withView(db, s"${tbl}_v", s"SELECT key, value FROM `$db`.`$tbl`") { view =>
        eventuallyOnCloud {
          val (rowCount, warnings) = captureWarnings(batchScanLogger)(readRowCount(db, view))
          assert(rowCount === 1)
          assert(warnings.exists(_.contains(singlePartitionWarning(db, view))))
        }
      }
    }
  }

  test("reading an unpartitioned table warns about single partition read") {
    withKVTable("db_read_warning", "tbl_single_part", valueColDef = "String") { (db, tbl) =>
      insertKV(db, tbl, 1 -> "a", 2 -> "b")
      eventuallyOnCloud {
        val (rowCount, warnings) = captureWarnings(batchScanLogger)(readRowCount(db, tbl))
        assert(rowCount === 2)
        assert(warnings.exists(_.contains(singlePartitionWarning(db, tbl))))
      }
    }
  }

  test("reading a table with multiple partitions does not warn about single partition read") {
    withSimpleTable("db_read_warning", "tbl_multi_part", writeData = true) { (db, tbl) =>
      eventuallyOnCloud {
        val (rowCount, warnings) = captureWarnings(batchScanLogger)(readRowCount(db, tbl))
        assert(rowCount === 2)
        assert(!warnings.exists(_.contains("as a single partition")))
      }
    }
  }

  /** Creates a ClickHouse view over `select` for the duration of `f`. */
  private def withView(db: String, view: String, select: String)(f: String => Unit): Unit = {
    runClickHouseSQL(s"CREATE VIEW `$db`.`$view` AS $select")
    try f(view)
    finally runClickHouseSQL(s"DROP VIEW IF EXISTS `$db`.`$view`")
  }

  /** Creates a ClickHouse materialized view over `select` for the duration of `f`. */
  private def withMaterializedView(db: String, mv: String, select: String)(f: String => Unit): Unit = {
    runClickHouseSQL(s"CREATE MATERIALIZED VIEW `$db`.`$mv` ENGINE = MergeTree() ORDER BY key AS $select")
    try f(mv)
    finally runClickHouseSQL(s"DROP VIEW IF EXISTS `$db`.`$mv`")
  }

  private def insertKV(db: String, tbl: String, rows: (Int, String)*): Unit =
    runClickHouseSQL(s"INSERT INTO `$db`.`$tbl` VALUES " +
      rows.map { case (k, v) => s"($k, '$v')" }.mkString(", "))

  /** Reads the table through the connector and returns the row count. */
  private def readRowCount(db: String, tbl: String): Long =
    spark.table(s"$db.$tbl").count()

  /** Reads the KV table through the connector and returns its rows ordered by key. */
  private def readSortedByKey(db: String, tbl: String): Seq[Row] =
    spark.sql(s"SELECT key, value FROM `$db`.`$tbl` ORDER BY key").collect().toSeq

  private def singlePartitionWarning(db: String, tbl: String): String =
    s"Reading $db.$tbl as a single partition"

  /** Runs `f` and returns its result with the WARN messages the logger emitted while it ran. */
  private def captureWarnings[A](loggerName: String)(f: => A): (A, Seq[String]) = {
    val warnings = ArrayBuffer.empty[String]
    val appender: AbstractAppender =
      new AbstractAppender("read-warning-capture", null, null, false, Property.EMPTY_ARRAY) {
        override def append(event: LogEvent): Unit =
          if (event.getLevel == Level.WARN && event.getLoggerName == loggerName)
            warnings += event.getMessage.getFormattedMessage
      }
    appender.start()
    // attach to the nearest configured LoggerConfig: Logger.addAppender would register a permanent
    // config for `loggerName` that black-holes its logging once the appender is removed
    val loggerConfig = LogManager.getLogger(loggerName).asInstanceOf[org.apache.logging.log4j.core.Logger]
      .getContext.getConfiguration.getLoggerConfig(loggerName)
    loggerConfig.addAppender(appender, Level.WARN, null)
    val result =
      try f
      finally {
        loggerConfig.removeAppender(appender.getName)
        appender.stop()
      }
    (result, warnings.toSeq)
  }
}
