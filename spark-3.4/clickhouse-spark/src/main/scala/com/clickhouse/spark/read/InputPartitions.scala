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

import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.read.partitioning.Partitioning
import com.clickhouse.spark.spec._

case class ClickHousePartitioning(inputParts: Array[ClickHouseInputPartition]) extends Partitioning {

  override def numPartitions(): Int = inputParts.length

}

/**
 * One scan task of a ClickHouse read.
 *
 * With `filterByPartitionId` (`spark.clickhouse.read.splitByPartitionId=true`), [[ClickHouseInputPartition.plan]]
 * gives each task a [[PartitionIdRange]] from [[PartitionIdRange.cover]] over the distinct partition ids of one
 * `system.parts` listing. Every row has exactly one `_partition_id`, so the tasks together read every row exactly
 * once, including rows of partitions the listing missed. Only a range unbounded on both sides reads the whole table;
 * a task built without a range reads exactly its listed `partition_id`.
 *
 * `partition` is a listed partition inside the range (the first task's range also reaches below it); the
 * `splitByPartitionId=false` path still filters on `partition_key = partition_value`, so it does not cover partitions
 * missing from the listing.
 */
case class ClickHouseInputPartition(
  table: TableSpec,
  partition: PartitionSpec,
  filterByPartitionId: Boolean,
  candidateNodes: Nodes, // try to use them only when preferredNode unavailable
  preferredNode: Option[NodeSpec] = None, // TODO assigned by ScanBuilder in Spark Driver side
  partitionIdRange: Option[PartitionIdRange] = None
) extends InputPartition {

  override def preferredLocations(): Array[String] = preferredNode match {
    case Some(preferred) => Array(preferred.host)
    case None => candidateNodes.nodes.map(_.host)
  }

  def partFilterExpr: String = partition match {
    case NoPartitionSpec => "1=1"
    case _ if filterByPartitionId =>
      partitionIdRange match {
        case Some(range) => range.filterExpr.getOrElse("1=1")
        case None => s"_partition_id = ${PartitionIdRange.quote(partition.partition_id)}"
      }
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

object ClickHouseInputPartition {

  /**
   * Builds the scan tasks for the partitions of one table (or one shard's local table) listed from `system.parts`.
   *
   * With `splitByPartitionId`, there is one task per distinct listed partition id, each reading its
   * [[PartitionIdRange]] from [[PartitionIdRange.cover]], so a partition missing from the listing still falls into
   * exactly one task.
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
      val ids = firstById.keys.toVector.sorted
      // the i-th range contains the i-th id
      ids.zip(PartitionIdRange.cover(ids)).map { case (id, range) =>
        ClickHouseInputPartition(
          table,
          firstById(id),
          filterByPartitionId = true,
          nodes,
          preferredNode = None,
          partitionIdRange = Some(range)
        )
      }
    }
}
