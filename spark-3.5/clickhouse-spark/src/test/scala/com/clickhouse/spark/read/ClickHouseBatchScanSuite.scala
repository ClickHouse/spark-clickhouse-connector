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

import com.clickhouse.spark.Log4j2CaptureHelper
import com.clickhouse.spark.func.StaticFunctionRegistry
import com.clickhouse.spark.ClickHouseTable
import com.clickhouse.spark.spec.{DistributedEngineSpec, MergeTreeEngineSpec, NodeSpec, TableSpec}
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf.{
  READ_DISTRIBUTED_CONVERT_LOCAL,
  READ_DISTRIBUTED_USE_CLUSTER_NODES
}
import org.apache.spark.sql.clickhouse.ReadOptions
import org.scalatest.funsuite.AnyFunSuite

import java.time.{LocalDateTime, ZoneId}

class ClickHouseBatchScanSuite extends AnyFunSuite with Log4j2CaptureHelper {

  test("planning a single input partition warns about read performance") {
    val scan = new ClickHouseBatchScan(scanJob())
    val (partitions, warnings) =
      captureWarnings(classOf[ClickHouseBatchScan].getName)(scan.inputPartitions)
    assert(partitions.length === 1)
    assert(warnings.exists(_.contains("Reading db.dist as a single partition")))
  }

  test("table identity ignores volatile runtime statistics") {
    def table(rows: Option[Long]): ClickHouseTable = ClickHouseTable(
      node = NodeSpec("127.0.0.1"),
      cluster = None,
      tz = ZoneId.of("UTC"),
      spec = tableSpec().copy(total_rows = rows, total_bytes = rows, lifetime_rows = rows),
      engineSpec = MergeTreeEngineSpec(engine_clause = "MergeTree"),
      functionRegistry = StaticFunctionRegistry
    )
    // two catalog loads of the same table must stay equal while data commits, or plan reuse breaks
    assert(table(Some(0L)) === table(Some(2L)))
    assert(table(Some(0L)).hashCode === table(Some(2L)).hashCode)
    assert(table(None) === table(Some(2L)))
  }

  private def tableSpec(): TableSpec = TableSpec(
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
  )

  // a Distributed table scan without convertDistributedToLocal plans exactly one partition without any I/O
  private def scanJob(): ScanJobDescription = {
    val readOptions = new java.util.HashMap[String, String]()
    readOptions.put(READ_DISTRIBUTED_CONVERT_LOCAL.key, "false")
    readOptions.put(READ_DISTRIBUTED_USE_CLUSTER_NODES.key, "false")
    ScanJobDescription(
      node = NodeSpec("127.0.0.1"),
      tz = ZoneId.of("UTC"),
      tableSpec = tableSpec(),
      tableEngineSpec = DistributedEngineSpec(
        engine_clause = "Distributed('single', 'db', 'local')",
        cluster = "single",
        local_db = "db",
        local_table = "local"
      ),
      cluster = None,
      localTableSpec = None,
      localTableEngineSpec = None,
      readOptions = new ReadOptions(readOptions),
      functionRegistry = StaticFunctionRegistry
    )
  }
}
