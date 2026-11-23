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

import com.clickhouse.spark.{ClickHouseHelper, Utils}
import com.clickhouse.spark.client.NodeClient
import com.clickhouse.spark.read.split.partition.TablePartition
import com.clickhouse.spark.read.split.primaryindex.{
  NotSplittable,
  PrimaryIndexSplitSettings,
  PrimaryKeyRangePlanner,
  TableKeyRanges
}
import com.clickhouse.spark.spec.{DistributedEngineSpec, Nodes, TableSpec}
import org.apache.spark.sql.clickhouse.ClickHouseSQLConf.READ_PRIMARY_INDEX_SPLIT_MAX_PARALLELISM

/** Resolves the split of a read on the driver, querying ClickHouse for what its strategy needs. */
private[read] class TaskSplitPlanner(scanJob: ScanJobDescription) extends ClickHouseHelper {

  private val queryTimeoutMs: Long = scanJob.readOptions.clientQueryTimeout

  def plan(): TaskSplit = TaskSplit.resolve(
    strategy = scanJob.readOptions.taskSplitStrategy,
    globalAggregatePushed = scanJob.globalAggregatePushed,
    primaryIndexSplitMaxParallelism = scanJob.readOptions.primaryIndexSplitMaxParallelism,
    engine = scanJob.tableEngineSpec,
    convertDistributedToLocal = scanJob.readOptions.convertDistributedToLocal,
    partitions = listPartitions(),
    primaryKeyRanges = planPrimaryKeyRanges()
  )

  // a table a split reads, on `nodes`
  private case class SplitTable(table: TableSpec, nodes: Nodes)

  // the table itself; for a Distributed table every shard's local table
  private def splitTables: Seq[SplitTable] = scanJob.tableEngineSpec match {
    case _: DistributedEngineSpec =>
      scanJob.cluster.get.shards.toSeq.map(shard => SplitTable(scanJob.localTableSpec.get, shard))
    case _ => Seq(SplitTable(scanJob.tableSpec, scanJob.node))
  }

  // the ClickHouse partitions of every split table, listed on its first node
  private def listPartitions(): Seq[TablePartition] = splitTables.flatMap { case SplitTable(table, nodes) =>
    Utils.tryWithResource(NodeClient(nodes.nodes.head, queryTimeoutMs)) { implicit nodeClient: NodeClient =>
      queryPartitionSpec(table.database, table.name).map(TablePartition(table, nodes, _))
    }
  }

  // the key ranges of every split table, planned on its first node; a shard left with one task is not planned
  private def planPrimaryKeyRanges(): Seq[TableKeyRanges] = {
    val tables = splitTables
    val settings = PrimaryIndexSplitSettings(
      maxParallelism = scanJob.readOptions.primaryIndexSplitMaxParallelism,
      targetRowsPerTask = scanJob.readOptions.primaryIndexSplitTargetRowsPerTask,
      defaultParallelism = scanJob.defaultParallelism,
      tables = tables.length
    )
    tables.map { case SplitTable(table, nodes) =>
      val ranges =
        if (settings.maxParallelism.contains(1))
          Left(NotSplittable(s"${READ_PRIMARY_INDEX_SPLIT_MAX_PARALLELISM.key} leaves one task per shard"))
        else Utils.tryWithResource(NodeClient(nodes.nodes.head, queryTimeoutMs)) { implicit nodeClient: NodeClient =>
          PrimaryKeyRangePlanner.plan(table, scanJob.tableColumns, scanJob.filters, settings)
        }
      TableKeyRanges(table, nodes, ranges)
    }
  }
}
