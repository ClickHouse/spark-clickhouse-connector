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
import com.clickhouse.spark.telemetry.UserAnalyticsFactory
import com.clickhouse.spark.ClickHouseTable
import com.clickhouse.spark.spec.{
  ClusterSpec,
  DistributedEngineSpec,
  MergeTreeEngineSpec,
  NoPartitionSpec,
  NodeSpec,
  PartitionSpec,
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
    val scan = new ClickHouseBatchScan(scanJob(), UserAnalyticsFactory.createCapture())
    val (partitions, warnings) =
      captureWarnings(classOf[ClickHouseBatchScan].getName)(scan.inputPartitions)
    assert(partitions.length === 1)
    assert(warnings.exists(_.contains("Reading db.dist as a single partition")))
    assert(partitions.head.partFilterExpr === "1=1")
  }

  private val node = NodeSpec("127.0.0.1")
  private def ps(id: String): PartitionSpec = PartitionSpec(id, id, 1, 1)
  private def plan(ids: Seq[String], split: Boolean = true): Seq[ClickHouseInputPartition] =
    ClickHouseInputPartition.plan(tableSpec(), ids.map(ps), split, node)

  test("splitByPartitionId plans _partition_id ranges between sorted distinct listed ids") {
    // listed out of id order with a duplicate: String order puts "10" before "3"
    val parts = plan(Seq("3", "1", "10", "1"))
    assert(parts.map(_.partFilterExpr) === Seq(
      "_partition_id < '10'",
      "_partition_id >= '10' AND _partition_id < '3'",
      "_partition_id >= '3'"
    ))
    assert(parts.map(_.partition.partition_id) === Seq("1", "10", "3"))
    assert(parts.forall(_.filterByPartitionId))
  }

  test("partition id ranges are open at both ends and adjacent ranges share a bound") {
    val parts = plan(Seq("a", "b", "c", "d"))
    assert(parts.head.partitionIdLowerBound.isEmpty)
    assert(parts.last.partitionIdUpperBound.isEmpty)
    parts.zip(parts.tail).foreach { case (prev, next) =>
      assert(prev.partitionIdUpperBound.isDefined)
      assert(prev.partitionIdUpperBound === next.partitionIdLowerBound)
    }
  }

  test("a single listed partition id plans one unbounded task") {
    val parts = plan(Seq("202105"))
    assert(parts.map(_.partFilterExpr) === Seq("1=1"))
    assert(parts.head.partitionIdLowerBound.isEmpty && parts.head.partitionIdUpperBound.isEmpty)
  }

  test("partition id bounds are SQL-escaped") {
    assert(plan(Seq("a", "a'b")).map(_.partFilterExpr) === Seq("_partition_id < 'a''b'", "_partition_id >= 'a''b'"))
  }

  test("partition id ranges cover every possible partition id exactly once") {
    val listed = Seq("20", "40", "6d2c8d1f0f4f8e0e9b1b4c5d6e7f8a9b", "a1")
    val parts = plan(listed)
    def matches(p: ClickHouseInputPartition, id: String): Boolean =
      p.partitionIdLowerBound.forall(id >= _) && p.partitionIdUpperBound.forall(id < _)
    // listed ids, ids below / between / above them (partitions created after planning)
    (Seq("", "10", "30", "zz") ++ listed).foreach { id =>
      assert(parts.count(matches(_, id)) === 1, s"partition id '$id'")
    }
  }

  test("without splitByPartitionId every listed spec filters on the partition key as before") {
    val parts = ClickHouseInputPartition.plan(
      tableSpec().copy(partition_key = "m"),
      Seq(ps("1"), ps("2"), ps("2")),
      splitByPartitionId = false,
      node
    )
    assert(parts.map(_.partFilterExpr) === Seq("m = 1", "m = 2", "m = 2"))
    assert(parts.forall(p => p.partitionIdLowerBound.isEmpty && p.partitionIdUpperBound.isEmpty))
  }

  test("an unpartitioned or empty table plans one task reading the whole table") {
    Seq(true, false).foreach { split =>
      val parts = ClickHouseInputPartition.plan(tableSpec(), Seq(NoPartitionSpec), split, node)
      assert(parts.map(_.partFilterExpr) === Seq("1=1"), s"splitByPartitionId=$split")
    }
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
    engineSpec = engineSpec
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
      readOptions = new ReadOptions(readOptions)
    )
  }
}
