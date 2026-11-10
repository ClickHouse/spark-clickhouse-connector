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
import com.clickhouse.spark.exception.CHClientException
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf._
import com.clickhouse.spark.spec._

case class ClickHousePartitioning(inputParts: Array[ClickHouseInputPartition]) extends Partitioning {

  override def numPartitions(): Int = inputParts.length

}

case class ClickHouseInputPartition(
  table: TableSpec,
  partition: PartitionSpec,
  filterByPartitionId: Boolean,
  candidateNodes: Nodes, // try to use them only when preferredNode unavailable
  preferredNode: Option[NodeSpec] = None, // TODO assigned by ScanBuilder in Spark Driver side
  keyRange: Option[PrimaryKeyRange] = None
) extends InputPartition {

  override def preferredLocations(): Array[String] = preferredNode match {
    case Some(preferred) => Array(preferred.host)
    case None => candidateNodes.nodes.map(_.host)
  }

  def partFilterExpr: String = {
    val partitionFilterExpr = partition match {
      case NoPartitionSpec => "1=1"
      case PartitionSpec(_, partitionId, _, _) if filterByPartitionId =>
        s"_partition_id = '$partitionId'"
      case PartitionSpec(partitionValue, _, _, _) =>
        s"${table.partition_key} = ${compilePartitionFilterValue(partitionValue)}"
    }
    keyRange.fold(partitionFilterExpr)(range => s"$partitionFilterExpr AND ${range.filterExpr}")
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

/** How a read is split into tasks, once the strategy, the engine and the primary index have been weighed. */
sealed trait TaskSplit

object TaskSplit {

  /** The whole table in one task; a Distributed table through the Distributed engine. */
  case object OneTask extends TaskSplit

  /** One task per ClickHouse partition, listed shard by shard for a Distributed table. */
  case object PartitionSplit extends TaskSplit

  /** One task per range of the primary key. */
  case class PrimaryIndexSplit(ranges: Seq[PrimaryKeyRange]) extends TaskSplit

  /**
   * Decides the split from the configured `strategy`. Each strategy falls back to one task on its own, never to
   * another split. Pure: `primaryKeyRanges` is the one costly input and is only evaluated when `primaryIndexSplit`
   * gets that far; None there falls back to one task.
   */
  def resolve(
    strategy: String,
    globalAggregatePushed: Boolean,
    engine: TableEngineSpec,
    convertDistributedToLocal: Boolean,
    useClusterNodesForDistributed: Boolean,
    primaryKeyRanges: => Option[Seq[PrimaryKeyRange]]
  ): TaskSplit = {
    val distributed = engine.isInstanceOf[DistributedEngineSpec]
    if (distributed && useClusterNodesForDistributed && !convertDistributedToLocal) {
      throw CHClientException(s"${READ_DISTRIBUTED_USE_CLUSTER_NODES.key} is not supported yet.")
    }
    // a global aggregate is always one task: an empty task returns a row of ClickHouse's defaults (not NULL) that
    // Spark would merge in
    strategy match {
      case TASK_SPLIT_STRATEGY_ONE_TASK => OneTask
      case TASK_SPLIT_STRATEGY_PARTITION =>
        if (globalAggregatePushed) OneTask
        else if (distributed && !convertDistributedToLocal) OneTask // partitions can't be listed through the engine
        else PartitionSplit // shard by shard for a Distributed table
      case TASK_SPLIT_STRATEGY_PRIMARY_INDEX =>
        if (globalAggregatePushed) OneTask
        else if (distributed) OneTask // no primary index across shards
        else primaryKeyRanges.map(PrimaryIndexSplit).getOrElse(OneTask)
    }
  }
}
