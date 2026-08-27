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
import com.clickhouse.spark.spec.{
  ClusterSpec,
  DistributedEngineSpec,
  MergeTreeEngineSpec,
  NodeSpec,
  ReplicaSpec,
  ShardSpec,
  TableEngineSpec,
  TableSpec
}
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
    def table(rows: Option[Long]): ClickHouseTable =
      clickHouseTable(spec = tableSpec().copy(total_rows = rows, total_bytes = rows, lifetime_rows = rows))
    // two loads either side of a write must stay equal, or plan reuse breaks
    assert(table(Some(0L)) === table(Some(2L)))
    assert(table(Some(0L)).hashCode === table(Some(2L)).hashCode)
    assert(table(None) === table(Some(2L)))
  }

  test("table identity ignores replica-local metadata") {
    // replica-local: whichever replica served the load
    def table(suffix: String): ClickHouseTable = clickHouseTable(spec =
      tableSpec().copy(
        data_paths = List(s"store/$suffix"),
        metadata_path = s"metadata/$suffix",
        metadata_modification_time = LocalDateTime.of(2026, 1, 1, 0, 0).plusSeconds(suffix.length.toLong)
      )
    )
    assert(table("a") === table("bb"))
    assert(table("a").hashCode === table("bb").hashCode)
  }

  test("table identity holds across separately built cluster specs") {
    // a catalog is built per getTable, so two loads carry different ClusterSpec instances
    def cluster(): ClusterSpec =
      ClusterSpec("single", Array(ShardSpec(1, 1, Array(ReplicaSpec(1, NodeSpec("127.0.0.1"))))))
    assert(cluster() === cluster())
    assert(cluster().hashCode === cluster().hashCode)

    // queryClusterSpecs groups an unordered `system.clusters` result, so array order is not stable
    val r1 = ReplicaSpec(1, NodeSpec("10.0.0.1"))
    val r2 = ReplicaSpec(2, NodeSpec("10.0.0.2"))
    val shard1 = (replicas: Array[ReplicaSpec]) => ShardSpec(1, 1, replicas)
    val shard2 = ShardSpec(2, 1, Array(ReplicaSpec(1, NodeSpec("10.0.0.3"))))
    val a = ClusterSpec("c", Array(shard1(Array(r1, r2)), shard2))
    val b = ClusterSpec("c", Array(shard2, shard1(Array(r2, r1))))
    assert(a === b)
    assert(a.hashCode === b.hashCode)
    // a genuinely different topology must still differ: same hosts, swapped replica numbers
    val swapped = ClusterSpec(
      "c",
      Array(shard1(Array(ReplicaSpec(1, NodeSpec("10.0.0.2")), ReplicaSpec(2, NodeSpec("10.0.0.1")))), shard2)
    )
    assert(a !== swapped)

    def table(): ClickHouseTable = clickHouseTable(
      cluster = Some(cluster()),
      engineSpec = DistributedEngineSpec(
        engine_clause = "Distributed('single', 'db', 'local')",
        cluster = "single",
        local_db = "db",
        local_table = "local"
      )
    )
    assert(table() === table())
    assert(table().hashCode === table().hashCode)
  }

  test("table identity still distinguishes different tables") {
    assert(clickHouseTable(spec = tableSpec().copy(name = "other")) !== clickHouseTable())
    assert(clickHouseTable(spec = tableSpec().copy(uuid = "other")) !== clickHouseTable())
    assert(clickHouseTable(spec = tableSpec().copy(database = "other")) !== clickHouseTable())
    assert(clickHouseTable(node = NodeSpec("10.0.0.1")) !== clickHouseTable())
  }

  private def clickHouseTable(
    node: NodeSpec = NodeSpec("127.0.0.1"),
    cluster: Option[ClusterSpec] = None,
    spec: TableSpec = tableSpec(),
    engineSpec: TableEngineSpec = MergeTreeEngineSpec(engine_clause = "MergeTree")
  ): ClickHouseTable = ClickHouseTable(
    node = node,
    cluster = cluster,
    tz = ZoneId.of("UTC"),
    spec = spec,
    engineSpec = engineSpec,
    functionRegistry = StaticFunctionRegistry
  )

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
