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
import org.scalatest.tags.Cloud

import scala.collection.mutable.ArrayBuffer

@Cloud
class ClickHouseCloudReadWarningSuite extends ClickHouseReadWarningSuite with ClickHouseCloudMixIn

class ClickHouseSingleReadWarningSuite extends ClickHouseReadWarningSuite with ClickHouseSingleMixIn

abstract class ClickHouseReadWarningSuite extends SparkClickHouseSingleTest {

  private val engineUtilsLogger = "com.clickhouse.spark.spec.TableEngineUtils"
  private val batchScanLogger = "com.clickhouse.spark.read.ClickHouseBatchScan"

  test("reading a view does not warn about unknown table engine") {
    withKVTable("db_read_warning", "tbl_view_src", valueColDef = "String") { (db, tbl) =>
      runClickHouseSQL(s"INSERT INTO `$db`.`$tbl` VALUES (1, 'a'), (2, 'b')")
      withView(db, s"${tbl}_v", s"SELECT key, value FROM `$db`.`$tbl`") { view =>
        val warnings = captureWarnings(engineUtilsLogger) {
          checkAnswer(
            spark.sql(s"SELECT key, value FROM `$db`.`$view` ORDER BY key"),
            Row(1, "a") :: Row(2, "b") :: Nil
          )
        }
        assert(!warnings.exists(_.contains("Unknown table engine")))
      }
    }
  }

  test("reading a view warns about single partition read") {
    withKVTable("db_read_warning", "tbl_view_single_part_src", valueColDef = "String") { (db, tbl) =>
      runClickHouseSQL(s"INSERT INTO `$db`.`$tbl` VALUES (1, 'a')")
      withView(db, s"${tbl}_v", s"SELECT key, value FROM `$db`.`$tbl`") { view =>
        val warnings = captureWarnings(batchScanLogger) {
          assert(spark.sql(s"SELECT key, value FROM `$db`.`$view`").count() === 1)
        }
        assert(warnings.exists(_.contains(s"Reading $db.$view as a single partition")))
      }
    }
  }

  test("reading an unpartitioned table warns about single partition read") {
    withKVTable("db_read_warning", "tbl_single_part", valueColDef = "String") { (db, tbl) =>
      runClickHouseSQL(s"INSERT INTO `$db`.`$tbl` VALUES (1, 'a'), (2, 'b')")
      val warnings = captureWarnings(batchScanLogger) {
        assert(spark.table(s"$db.$tbl").count() === 2)
      }
      assert(warnings.exists(_.contains(s"Reading $db.$tbl as a single partition")))
    }
  }

  test("reading a table with multiple partitions does not warn about single partition read") {
    withSimpleTable("db_read_warning", "tbl_multi_part", writeData = true) { (db, tbl) =>
      val warnings = captureWarnings(batchScanLogger) {
        assert(spark.table(s"$db.$tbl").count() === 2)
      }
      assert(!warnings.exists(_.contains("as a single partition")))
    }
  }

  private def withView(db: String, view: String, select: String)(f: String => Unit): Unit = {
    runClickHouseSQL(s"CREATE VIEW `$db`.`$view` AS $select")
    if (isCloud) Thread.sleep(1000)
    try f(view)
    finally runClickHouseSQL(s"DROP VIEW IF EXISTS `$db`.`$view`")
  }

  private def captureWarnings(loggerName: String)(f: => Unit): Seq[String] = {
    val warnings = ArrayBuffer.empty[String]
    val appender: AbstractAppender =
      new AbstractAppender("read-warning-capture", null, null, false, Property.EMPTY_ARRAY) {
        override def append(event: LogEvent): Unit =
          if (event.getLevel == Level.WARN) warnings += event.getMessage.getFormattedMessage
      }
    appender.start()
    val logger = LogManager.getLogger(loggerName).asInstanceOf[org.apache.logging.log4j.core.Logger]
    logger.addAppender(appender)
    try f
    finally {
      logger.removeAppender(appender)
      appender.stop()
    }
    warnings.toSeq
  }
}
