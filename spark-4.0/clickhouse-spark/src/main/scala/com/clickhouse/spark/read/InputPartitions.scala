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

import com.clickhouse.spark.SQLHelper
import com.clickhouse.spark.spec.{NoPartitionSpec, NodeSpec, Nodes, PartitionSpec, TableSpec}
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.read.partitioning.Partitioning
import com.clickhouse.spark.spec._

case class ClickHousePartitioning(inputParts: Array[ClickHouseInputPartition]) extends Partitioning {

  override def numPartitions(): Int = inputParts.length

}

/**
 * One scan task of a ClickHouse read.
 *
 * With `filterByPartitionId` (`spark.clickhouse.read.splitByPartitionId=true`) a task does not read a single
 * listed partition id but the half-open range `[partitionIdLowerBound, partitionIdUpperBound)` of `_partition_id`
 * values; `None` leaves that side unbounded. [[ClickHouseInputPartition.plan]] builds the bounds from the sorted,
 * distinct partition ids of one `system.parts` listing, so neighbouring tasks share a bound, the first task has no
 * lower bound and the last no upper bound. Every row has exactly one `_partition_id`, so the tasks together read
 * every row exactly once, including rows of partitions created between planning and reading.
 *
 * `partition` is a listed partition inside the range (the first task's range also reaches below it); the
 * `splitByPartitionId=false` path still filters on `partition_key = partition_value`, so it does not cover partitions
 * created after planning. Build tasks through [[ClickHouseInputPartition.plan]]: a task constructed directly with
 * `filterByPartitionId` and no bounds reads the whole table.
 */
case class ClickHouseInputPartition(
  table: TableSpec,
  partition: PartitionSpec,
  filterByPartitionId: Boolean,
  candidateNodes: Nodes, // try to use them only when preferredNode unavailable
  preferredNode: Option[NodeSpec] = None, // TODO assigned by ScanBuilder in Spark Driver side
  partitionIdLowerBound: Option[String] = None, // inclusive; None = unbounded
  partitionIdUpperBound: Option[String] = None // exclusive; None = unbounded
) extends InputPartition {

  override def preferredLocations(): Array[String] = preferredNode match {
    case Some(preferred) => Array(preferred.host)
    case None => candidateNodes.nodes.map(_.host)
  }

  def partFilterExpr: String = partition match {
    case NoPartitionSpec => "1=1"
    case _ if filterByPartitionId =>
      val conds =
        partitionIdLowerBound.map(id => s"_partition_id >= '${ClickHouseInputPartition.escapeSql(id)}'").toSeq ++
          partitionIdUpperBound.map(id => s"_partition_id < '${ClickHouseInputPartition.escapeSql(id)}'")
      if (conds.isEmpty) "1=1" else conds.mkString(" AND ")
    case PartitionSpec(partitionValue, _, _, _) =>
      s"${table.partition_key} = ${compilePartitionFilterValue(partitionValue)}"
  }

  // TODO improve and test
  def compilePartitionFilterValue(partitionValue: String): String =
    (partitionValue.contains("-"), partitionValue.contains("(")) match {
      // quote when partition by a single Date Type column to avoid illegal types of arguments (Date, Int64)
      case (true, false) => s"'$partitionValue'"
      // Date type column is quoted if there are multi partition columns
      case _ => s"$partitionValue"
    }
}

object ClickHouseInputPartition extends SQLHelper {

  /**
   * Builds the scan tasks for the partitions of one table (or one shard's local table) listed from `system.parts`.
   *
   * With `splitByPartitionId`, there is one task per distinct listed partition id, and the tasks read consecutive
   * `_partition_id` ranges whose bounds are the neighbouring listed ids (first task unbounded below, last unbounded
   * above), so a partition created after the listing still falls into exactly one task. Ids are ordered with
   * `String` ordering; ClickHouse compares `String` bytewise, which agrees for ASCII, and partition ids are ASCII
   * (decimal numbers, dates such as `202105`, hex hashes, `all`).
   *
   * Without `splitByPartitionId`, or when the table is listed as unpartitioned / empty ([[NoPartitionSpec]]),
   * this is one task per listed spec, as before.
   */
  def plan(
    table: TableSpec,
    specs: Seq[PartitionSpec],
    splitByPartitionId: Boolean,
    nodes: Nodes
  ): Seq[ClickHouseInputPartition] =
    if (!splitByPartitionId || specs.exists(_ eq NoPartitionSpec)) {
      specs.map(ClickHouseInputPartition(table, _, splitByPartitionId, nodes))
    } else {
      // the listing is ordered by (partition, partition_id): dedupe and re-order by id
      val firstById = specs.groupBy(_.partition_id).map { case (id, ss) => id -> ss.head }
      val ids = firstById.keys.toSeq.sorted
      ids.indices.map { i =>
        ClickHouseInputPartition(
          table,
          firstById(ids(i)),
          filterByPartitionId = true,
          nodes,
          preferredNode = None,
          partitionIdLowerBound = if (i == 0) None else Some(ids(i)),
          partitionIdUpperBound = if (i == ids.length - 1) None else Some(ids(i + 1))
        )
      }
    }
}
