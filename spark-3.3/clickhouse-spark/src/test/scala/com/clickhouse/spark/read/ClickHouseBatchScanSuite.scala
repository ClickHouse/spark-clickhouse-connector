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

package com.clickhouse.spark.read

import com.clickhouse.spark.spec.{DistributedEngineSpec, NodeSpec, TableSpec}
import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.LogEvent
import org.apache.logging.log4j.core.appender.AbstractAppender
import org.apache.logging.log4j.core.config.Property
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf.{
  READ_DISTRIBUTED_CONVERT_LOCAL,
  READ_DISTRIBUTED_USE_CLUSTER_NODES
}
import org.apache.spark.sql.clickhouse.ReadOptions
import org.scalatest.funsuite.AnyFunSuite

import java.time.{LocalDateTime, ZoneId}
import scala.collection.mutable.ArrayBuffer

class ClickHouseBatchScanSuite extends AnyFunSuite {

  test("planning a single input partition warns about read performance") {
    val scan = new ClickHouseBatchScan(scanJob())
    val (partitions, warnings) = captureWarnings(scan.inputPartitions)
    assert(partitions.length === 1)
    assert(warnings.exists(_.contains("Reading db.dist as a single partition")))
  }

  /** Runs `f` and returns its result with the WARN messages the scan logger emitted while it ran. */
  private def captureWarnings[A](f: => A): (A, Seq[String]) = {
    val loggerName = classOf[ClickHouseBatchScan].getName
    val warnings = ArrayBuffer.empty[String]
    val appender: AbstractAppender =
      new AbstractAppender("batch-scan-warning-capture", null, null, false, Property.EMPTY_ARRAY) {
        override def append(event: LogEvent): Unit =
          if (event.getLevel == Level.WARN && event.getLoggerName == loggerName)
            warnings += event.getMessage.getFormattedMessage
      }
    appender.start()
    // attach to the nearest configured LoggerConfig: Logger.addAppender would register a permanent
    // config for the scan logger that black-holes its logging once the appender is removed
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

  // a Distributed table scan without convertDistributedToLocal plans exactly one partition without any I/O
  private def scanJob(): ScanJobDescription = {
    val readOptions = new java.util.HashMap[String, String]()
    readOptions.put(READ_DISTRIBUTED_CONVERT_LOCAL.key, "false")
    readOptions.put(READ_DISTRIBUTED_USE_CLUSTER_NODES.key, "false")
    ScanJobDescription(
      node = NodeSpec("127.0.0.1"),
      tz = ZoneId.of("UTC"),
      tableSpec = TableSpec(
        database = "db",
        name = "dist",
        uuid = "",
        engine = "Distributed",
        is_temporary = false,
        data_paths = Nil,
        metadata_path = "",
        metadata_modification_time = LocalDateTime.of(2026, 1, 1, 0, 0),
        dependencies_database = Nil,
        dependencies_table = Nil,
        create_table_query = "",
        engine_full = "Distributed('single', 'db', 'local')",
        partition_key = "",
        sorting_key = "",
        primary_key = "",
        sampling_key = "",
        storage_policy = "",
        total_rows = None,
        total_bytes = None,
        lifetime_rows = None,
        lifetime_bytes = None
      ),
      tableEngineSpec = DistributedEngineSpec(
        engine_clause = "Distributed('single', 'db', 'local')",
        cluster = "single",
        local_db = "db",
        local_table = "local"
      ),
      cluster = None,
      localTableSpec = None,
      localTableEngineSpec = None,
      readOptions = new ReadOptions(readOptions)
    )
  }
}
